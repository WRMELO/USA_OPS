from __future__ import annotations

import numpy as np
import pandas as pd

from lib.band_exp_gate import compute_bandexp_ret62_gate


def _rows_for_ticker(
    ticker: str,
    dates: pd.DatetimeIndex,
    widths: list[float],
    *,
    violate_last: bool = False,
    nan_spc: bool = False,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for i, dt in enumerate(dates):
        width = float(widths[i])
        if nan_spc:
            i_ucl = np.nan
            i_lcl = np.nan
            i_value = 0.0
        else:
            i_lcl = -1.0
            i_ucl = i_lcl + width
            i_value = float(i_ucl + 0.01) if (violate_last and i == len(dates) - 1) else 0.0
        rows.append(
            {
                "date": dt,
                "ticker": ticker,
                "i_value": i_value,
                "i_ucl": i_ucl,
                "i_lcl": i_lcl,
                "mr_value": 0.1,
                "mr_ucl": 1.0,
                "xbar_value": 0.0,
                "xbar_ucl": 0.5,
                "xbar_lcl": -0.5,
                "r_value": 0.1,
                "r_ucl": 1.0,
            }
        )
    return rows


def test_compute_bandexp_ret62_gate_core_conditions():
    dates = pd.date_range("2026-01-01", periods=30, freq="D")
    as_of = dates[-1]

    w_true = [1.0 + 0.40 * i for i in range(len(dates))]
    w_lowret = [1.0 + 0.35 * i for i in range(len(dates))]
    w_viol = [1.0 + 0.15 * i for i in range(len(dates))]
    w_lowmono = [3.0 + (0.3 if i % 2 == 0 else -0.3) for i in range(len(dates))]
    w_base1 = [3.5 for _ in dates]
    w_base2 = [4.0 - 0.05 * i for i in range(len(dates))]
    w_short = [1.0 + 0.5 * i for i in range(10)]
    short_dates = dates[-10:]
    w_nanspc = [2.0 + 0.1 * i for i in range(len(dates))]

    canonical_rows: list[dict[str, object]] = []
    canonical_rows += _rows_for_ticker("GATE_TRUE", dates, w_true)
    canonical_rows += _rows_for_ticker("LOWRET", dates, w_lowret)
    canonical_rows += _rows_for_ticker("VIOL", dates, w_viol, violate_last=True)
    canonical_rows += _rows_for_ticker("LOWMONO", dates, w_lowmono)
    canonical_rows += _rows_for_ticker("BASE1", dates, w_base1)
    canonical_rows += _rows_for_ticker("BASE2", dates, w_base2)
    canonical_rows += _rows_for_ticker("SHORT", short_dates, w_short)
    canonical_rows += _rows_for_ticker("NANSPC", dates, w_nanspc, nan_spc=True)
    canonical = pd.DataFrame(canonical_rows)

    scores = pd.DataFrame(
        [
            {"date": as_of, "ticker": "GATE_TRUE", "ret_62": 1.20},
            {"date": as_of, "ticker": "LOWRET", "ret_62": 0.90},
            {"date": as_of, "ticker": "VIOL", "ret_62": 1.30},
            {"date": as_of, "ticker": "LOWMONO", "ret_62": 1.30},
            {"date": as_of, "ticker": "BASE1", "ret_62": 1.30},
            {"date": as_of, "ticker": "BASE2", "ret_62": 1.30},
            {"date": as_of, "ticker": "SHORT", "ret_62": 1.30},
            {"date": as_of, "ticker": "NANSPC", "ret_62": 1.30},
        ]
    )

    out = compute_bandexp_ret62_gate(canonical=canonical, scores=scores, as_of_date=as_of)

    assert bool(out.loc["GATE_TRUE", "flag_bandexp"]) is True
    assert bool(out.loc["GATE_TRUE", "gate_bandexp_ret62"]) is True

    assert bool(out.loc["LOWRET", "flag_bandexp"]) is True
    assert bool(out.loc["LOWRET", "gate_bandexp_ret62"]) is False

    assert bool(out.loc["VIOL", "flag_bandexp"]) is False
    assert bool(out.loc["VIOL", "gate_bandexp_ret62"]) is False

    assert bool(out.loc["LOWMONO", "flag_bandexp"]) is False
    assert bool(out.loc["LOWMONO", "gate_bandexp_ret62"]) is False

    assert bool(out.loc["SHORT", "flag_bandexp"]) is False
    assert bool(out.loc["SHORT", "gate_bandexp_ret62"]) is False

    assert bool(out.loc["NANSPC", "flag_bandexp"]) is False
    assert bool(out.loc["NANSPC", "gate_bandexp_ret62"]) is False
