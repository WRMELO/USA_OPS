from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


def _load_backtest_module():
    root = Path(__file__).resolve().parents[1]
    script = root / "backtest" / "run_backtest_variants_us.py"
    module_name = "run_backtest_variants_us_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_sell_ticker_fifo_sells_full_lot_without_dust_on_float_floor_edge():
    backtest = _load_backtest_module()
    lots = [backtest.Lot(ticker="AAA", buy_date=pd.Timestamp("2026-07-01"), shares=99, buy_price=0.08)]
    price_row = pd.Series({"AAA": 0.1})
    trading_dates = [pd.Timestamp("2026-07-23"), pd.Timestamp("2026-07-24")]
    pending_cash: dict[pd.Timestamp, float] = {}

    updated_lots, proceeds_liq, _, sold_shares = backtest.sell_ticker_fifo(
        ticker="AAA",
        target_value_to_sell=9.9,
        lots=lots,
        price_row=price_row,
        friction=0.0,
        trading_dates=trading_dates,
        i=0,
        settlement_days=1,
        pending_cash=pending_cash,
    )

    assert sold_shares == 99
    assert updated_lots == []
    assert abs(proceeds_liq - 9.9) < 1e-9
    assert abs(pending_cash[trading_dates[1]] - 9.9) < 1e-9


def test_sell_all_ticker_uses_exact_full_liquidation_without_dust():
    backtest = _load_backtest_module()
    lots = [backtest.Lot(ticker="AAA", buy_date=pd.Timestamp("2026-07-01"), shares=99, buy_price=0.08)]
    price_row = pd.Series({"AAA": 0.1})
    trading_dates = [pd.Timestamp("2026-07-23"), pd.Timestamp("2026-07-24")]
    pending_cash: dict[pd.Timestamp, float] = {}

    updated_lots, proceeds_liq, _, sold_shares = backtest.sell_all_ticker(
        ticker="AAA",
        lots=lots,
        price_row=price_row,
        friction=0.0,
        trading_dates=trading_dates,
        i=0,
        settlement_days=1,
        pending_cash=pending_cash,
    )

    assert sold_shares == 99
    assert updated_lots == []
    assert abs(proceeds_liq - 9.9) < 1e-9
    assert abs(pending_cash[trading_dates[1]] - 9.9) < 1e-9
