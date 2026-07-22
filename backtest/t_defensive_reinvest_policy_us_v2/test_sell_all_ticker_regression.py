"""Regressao read-only: venda integral compartilhada vs wrapper exato da V2."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.t_defensive_reinvest_policy_us_v2.run_t_defensive_reinvest_policy_us_v2 import (  # noqa: E402
    OUT_DIR,
    _sell_all_shares_exact,
    rb,
)


def _shares(lots: list[rb.Lot], ticker: str) -> int:
    return int(sum(int(lot.shares) for lot in lots if lot.ticker == ticker))


def main() -> None:
    ticker = "FIXTURE"
    day = pd.Timestamp("2026-01-02")
    lots = [
        rb.Lot(ticker=ticker, buy_date=pd.Timestamp("2025-01-02"), shares=33, buy_price=9.0),
        rb.Lot(ticker=ticker, buy_date=pd.Timestamp("2025-02-03"), shares=33, buy_price=10.0),
        rb.Lot(ticker=ticker, buy_date=pd.Timestamp("2025-03-03"), shares=33, buy_price=11.0),
    ]
    prices = pd.Series({ticker: 10.37})

    shared_pending: dict[pd.Timestamp, float] = {}
    shared_lots, _, _, shared_sold = rb.sell_all_ticker(
        ticker=ticker,
        lots=lots,
        price_row=prices,
        friction=0.00025,
        trading_dates=[day],
        i=0,
        settlement_days=0,
        pending_cash=shared_pending,
    )
    shared_remaining = _shares(shared_lots, ticker)

    exact_pending: dict[pd.Timestamp, float] = {}
    exact_lots, _, _, exact_sold = _sell_all_shares_exact(
        ticker=ticker,
        lots=lots,
        price_row=prices,
        friction=0.00025,
        pending_cash=exact_pending,
        exec_day=day,
    )
    exact_remaining = _shares(exact_lots, ticker)
    exact_status = "PASS" if exact_sold == 99 and exact_remaining == 0 else "FAIL"
    shared_status = "PASS" if shared_sold == 99 and shared_remaining == 0 else "FAIL"
    payload = {
        "fixture": "3 lotes x 33 acoes; preco=10.37",
        "shared_helper": {
            "status": shared_status,
            "sold_shares": int(shared_sold),
            "remaining_shares": int(shared_remaining),
        },
        "v2_exact_wrapper": {
            "status": exact_status,
            "sold_shares": int(exact_sold),
            "remaining_shares": int(exact_remaining),
        },
        "recommend_separate_historical_impact_task": bool(shared_status == "FAIL"),
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "shared_sell_all_regression.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if exact_status != "PASS":
        raise SystemExit("FAIL: wrapper exato da V2 deixou saldo.")


if __name__ == "__main__":
    main()
