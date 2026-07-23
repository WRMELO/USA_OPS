"""Helper de leitura para sugerir preco-sombra (F-16/F-17) via SSOT operacional.

Le exclusivamente `data/ssot/operational_window.parquet` e retorna o
close_operational do pregao fechado imediatamente anterior a `--exec-date`.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from lib.trading_calendar import prev_session  # noqa: E402

DEFAULT_WINDOW_PATH = ROOT / "data" / "ssot" / "operational_window.parquet"


def _parse_iso_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Data invalida: {raw!r}. Use YYYY-MM-DD.") from exc


def _resolve_path(raw: str | None) -> Path:
    if not raw:
        return DEFAULT_WINDOW_PATH
    p = Path(raw)
    if not p.is_absolute():
        p = (ROOT / p).resolve()
    return p


def lookup_close(ticker: str, exec_date: date, window_path: Path) -> dict[str, Any]:
    market_day = prev_session(exec_date)
    normalized_ticker = ticker.upper().strip()
    result: dict[str, Any] = {
        "ticker": normalized_ticker,
        "exec_date": exec_date.isoformat(),
        "market_day": market_day.isoformat(),
        "close": None,
        "found": False,
    }

    if not window_path.exists():
        return result

    try:
        frame = pd.read_parquet(window_path, columns=["date", "ticker", "close_operational"])
    except Exception:
        return result

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    match = frame[(frame["ticker"] == normalized_ticker) & (frame["date"] == market_day)]
    if match.empty:
        return result

    close_value = match.iloc[0]["close_operational"]
    if close_value is None or pd.isna(close_value):
        return result

    result["close"] = float(close_value)
    result["found"] = True
    return result


def resolve_marking_prices(
    tickers: set[str], market_day: date, window_path: Path
) -> tuple[dict[str, float], list[str]]:
    normalized = {str(tk).upper().strip() for tk in tickers if str(tk).strip()}
    if not normalized:
        return {}, []
    if not window_path.exists():
        return {}, sorted(normalized)

    try:
        frame = pd.read_parquet(window_path, columns=["date", "ticker", "close_operational"])
    except Exception:
        return {}, sorted(normalized)

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["ticker"] = frame["ticker"].astype(str).str.upper().str.strip()
    scoped = frame[
        (frame["ticker"].isin(normalized))
        & (frame["date"] <= market_day)
        & (~pd.isna(frame["close_operational"]))
    ]
    if scoped.empty:
        return {}, sorted(normalized)

    latest = scoped.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).tail(1)
    prices = {
        str(row["ticker"]).upper().strip(): float(row["close_operational"])
        for _, row in latest.iterrows()
    }
    missing = sorted(normalized - set(prices.keys()))
    return prices, missing


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sugere preco-sombra (close_operational do pregao anterior) para um ticker."
    )
    parser.add_argument("--ticker", type=str, required=True)
    parser.add_argument("--exec-date", type=_parse_iso_date, required=True, help="Data YYYY-MM-DD.")
    parser.add_argument(
        "--window-path",
        type=str,
        default=None,
        help="Path do operational_window.parquet (default: data/ssot/operational_window.parquet).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = lookup_close(args.ticker, args.exec_date, _resolve_path(args.window_path))
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
