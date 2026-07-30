"""Step 02 — ingest prices US via EODHD local base."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from lib.eodhd_source_us import load_incremental_rows_from_eodhd
from lib.trading_calendar import prev_session

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "ssot" / "us_market_data_raw.parquet"
EODHD_BASE_PATH = Path("/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/data/eodhd_raw_us.parquet")
EXCHANGE = "XNYS"


def _normalize_raw(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out = out.dropna(subset=["date", "ticker"]).reset_index(drop=True)
    return out


def _assert_eodhd_fresh(target_end: date) -> None:
    expected = prev_session(target_end, exchange=EXCHANGE)
    if not EODHD_BASE_PATH.exists():
        raise RuntimeError(f"Base EODHD US ausente: {EODHD_BASE_PATH}")
    base_dates = pd.read_parquet(EODHD_BASE_PATH, columns=["date"]).copy()
    base_dates["date"] = pd.to_datetime(base_dates["date"], errors="coerce").dt.normalize()
    if base_dates["date"].dropna().empty:
        raise RuntimeError(f"Base EODHD US sem datas validas: {EODHD_BASE_PATH}")
    max_date = pd.Timestamp(base_dates["date"].max()).date()
    if max_date < expected:
        raise RuntimeError(
            "Base EODHD US defasada para o Step 02: "
            f"max_date={max_date.isoformat()} < expected_prev_session={expected.isoformat()}."
        )


def run(end_date: date | None = None) -> dict:
    target_end = end_date or date.today()
    _assert_eodhd_fresh(target_end)

    if not RAW_PATH.exists():
        raise RuntimeError(f"Raw oficial ausente para ingest incremental: {RAW_PATH}")

    current_raw = _normalize_raw(pd.read_parquet(RAW_PATH))
    if current_raw.empty:
        raise RuntimeError(f"Raw oficial sem pares validos de date/ticker: {RAW_PATH}")

    tickers = sorted(current_raw["ticker"].dropna().unique().tolist())
    last_by_ticker = current_raw.groupby("ticker", as_index=True)["date"].max()
    ticker_last_dates = {ticker: ts.date() for ticker, ts in last_by_ticker.items()}

    incremental = load_incremental_rows_from_eodhd(
        tickers=tickers,
        ticker_last_dates=ticker_last_dates,
        end_date=target_end,
    )
    if incremental.empty:
        return {
            "status": "ok",
            "end_date": str(target_end),
            "raw_path": "data/ssot/us_market_data_raw.parquet",
            "added_rows": 0,
        }

    official_cols = list(current_raw.columns)
    incremental = incremental.copy()
    for col in official_cols:
        if col not in incremental.columns:
            incremental[col] = pd.NA
    incremental = incremental[official_cols]
    incremental["date"] = pd.to_datetime(incremental["date"], errors="coerce").dt.normalize()
    incremental["ticker"] = incremental["ticker"].astype(str).str.upper().str.strip()
    if "ingested_at" in incremental.columns:
        incremental["ingested_at"] = pd.to_datetime(incremental["ingested_at"], errors="coerce", utc=True)

    merged = pd.concat([current_raw[official_cols], incremental], ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.normalize()
    merged["ticker"] = merged["ticker"].astype(str).str.upper().str.strip()
    merged = merged.dropna(subset=["date", "ticker"])
    if "ingested_at" in merged.columns:
        merged["ingested_at"] = pd.to_datetime(merged["ingested_at"], errors="coerce", utc=True)
        merged = merged.sort_values(["ticker", "date", "ingested_at"], na_position="last")
    else:
        merged = merged.sort_values(["ticker", "date"])
    merged = merged.drop_duplicates(subset=["date", "ticker"], keep="last").reset_index(drop=True)

    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(RAW_PATH, index=False)
    return {
        "status": "ok",
        "end_date": str(target_end),
        "raw_path": "data/ssot/us_market_data_raw.parquet",
        "added_rows": int(len(incremental)),
    }
