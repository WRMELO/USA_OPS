"""Data adapters for US market sources (Polygon, FRED)."""
from __future__ import annotations

import time
from datetime import date, datetime
from io import StringIO
from typing import Any, Callable
from urllib.request import urlopen

import pandas as pd
from polygon import RESTClient


class FredAdapter:
    """FRED public CSV series adapter."""

    BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="

    SERIES = {
        "VIXCLS": "vix_close",
        "DTWEXBGS": "usd_index_broad",
        "DGS10": "ust_10y_yield",
        "DGS2": "ust_2y_yield",
        "DFF": "fed_funds_rate",
        "BAMLH0A0HYM2": "hy_oas",
        "BAMLC0A0CM": "ig_oas",
    }

    def __init__(self, timeout_seconds: float = 30.0, max_retries: int = 5) -> None:
        self.timeout = timeout_seconds
        self.max_retries = max_retries

    def fetch_series(self, series_id: str, alias: str) -> pd.DataFrame:
        url = f"{self.BASE_URL}{series_id}"
        for attempt in range(1, self.max_retries + 1):
            try:
                with urlopen(url, timeout=self.timeout) as resp:
                    csv_text = resp.read().decode("utf-8")
                df = pd.read_csv(StringIO(csv_text))
                date_col = "DATE" if "DATE" in df.columns else "observation_date"
                value_col = series_id if series_id in df.columns else alias
                out = df.rename(columns={date_col: "date", value_col: alias})
                out["date"] = pd.to_datetime(out["date"], errors="coerce")
                out[alias] = pd.to_numeric(out[alias], errors="coerce")
                return out[["date", alias]].dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            except Exception:
                if attempt == self.max_retries:
                    raise
                wait_s = min(2**attempt, 60)
                print(f"[FRED] Attempt {attempt}/{self.max_retries} failed for {series_id}; retrying in {wait_s}s...")
                time.sleep(float(wait_s))
        return pd.DataFrame(columns=["date", alias])

    def fetch_all(self) -> dict[str, pd.DataFrame]:
        return {alias: self.fetch_series(series_id, alias) for series_id, alias in self.SERIES.items()}


class PolygonAdapter:
    """Polygon.io market data adapter with exponential retry."""

    def __init__(self, api_key: str, timeout_seconds: float = 20.0, max_retries: int = 5) -> None:
        if not api_key:
            raise ValueError("POLYGON_API_KEY not found in environment.")
        self.client = RESTClient(api_key=api_key, trace=False, connect_timeout=timeout_seconds, read_timeout=timeout_seconds)
        self.max_retries = max_retries

    def _with_retry(self, fn: Callable[[], Any], label: str) -> Any:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt == self.max_retries:
                    break
                wait_s = min(2**attempt, 60)
                print(f"[POLYGON] Attempt {attempt}/{self.max_retries} failed for {label}; retrying in {wait_s}s...")
                time.sleep(float(wait_s))
        raise RuntimeError(f"Polygon request failed for {label}") from last_exc

    @staticmethod
    def _to_date(value: Any) -> pd.Timestamp:
        if isinstance(value, (datetime, date)):
            return pd.Timestamp(value).normalize()
        return pd.to_datetime(value, errors="coerce").normalize()

    def get_ohlcv(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        label = f"aggs:{ticker}:{start}:{end}"

        def _call() -> list[Any]:
            return list(
                self.client.list_aggs(
                    ticker=ticker,
                    multiplier=1,
                    timespan="day",
                    from_=start,
                    to=end,
                    adjusted=True,
                    sort="asc",
                    limit=50000,
                )
            )

        rows = self._with_retry(_call, label)
        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        out = []
        for row in rows:
            ts_ms = getattr(row, "timestamp", None)
            if ts_ms is None:
                ts_ms = row.get("timestamp") if isinstance(row, dict) else None
            dt = pd.to_datetime(ts_ms, unit="ms", utc=True, errors="coerce")
            out.append(
                {
                    "date": dt.tz_convert(None).normalize() if pd.notna(dt) else pd.NaT,
                    "open": getattr(row, "open", None) if not isinstance(row, dict) else row.get("open"),
                    "high": getattr(row, "high", None) if not isinstance(row, dict) else row.get("high"),
                    "low": getattr(row, "low", None) if not isinstance(row, dict) else row.get("low"),
                    "close": getattr(row, "close", None) if not isinstance(row, dict) else row.get("close"),
                    "volume": getattr(row, "volume", None) if not isinstance(row, dict) else row.get("volume"),
                }
            )
        df = pd.DataFrame(out)
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df[["date", "open", "high", "low", "close", "volume"]]

    def get_dividends(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        label = f"dividends:{ticker}:{start}:{end}"

        def _call() -> list[Any]:
            return list(
                self.client.list_dividends(
                    ticker=ticker,
                    ex_dividend_date_gte=start,
                    ex_dividend_date_lte=end,
                    sort="ex_dividend_date",
                    order="asc",
                    limit=1000,
                )
            )

        rows = self._with_retry(_call, label)
        if not rows:
            return pd.DataFrame(columns=["date", "amount"])
        out = []
        for row in rows:
            ex_date = getattr(row, "ex_dividend_date", None)
            if ex_date is None and isinstance(row, dict):
                ex_date = row.get("ex_dividend_date")
            amount = getattr(row, "cash_amount", None) if not isinstance(row, dict) else row.get("cash_amount")
            out.append({"date": self._to_date(ex_date), "amount": amount})
        df = pd.DataFrame(out)
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df[["date", "amount"]]

    def get_splits(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        label = f"splits:{ticker}:{start}:{end}"

        def _call() -> list[Any]:
            return list(
                self.client.list_splits(
                    ticker=ticker,
                    execution_date_gte=start,
                    execution_date_lte=end,
                    sort="execution_date",
                    order="asc",
                    limit=1000,
                )
            )

        rows = self._with_retry(_call, label)
        if not rows:
            return pd.DataFrame(columns=["date", "split_from", "split_to"])
        out = []
        for row in rows:
            exec_date = getattr(row, "execution_date", None)
            if exec_date is None and isinstance(row, dict):
                exec_date = row.get("execution_date")
            split_from = getattr(row, "split_from", None) if not isinstance(row, dict) else row.get("split_from")
            split_to = getattr(row, "split_to", None) if not isinstance(row, dict) else row.get("split_to")
            out.append({"date": self._to_date(exec_date), "split_from": split_from, "split_to": split_to})
        df = pd.DataFrame(out)
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df[["date", "split_from", "split_to"]]
