from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd


def _load_decide_module():
    root = Path(__file__).resolve().parents[1]
    script = root / "pipeline" / "09_decide.py"
    module_name = "decide09_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _rows_for_ticker(ticker: str, dates: pd.DatetimeIndex, widths: list[float]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for i, dt in enumerate(dates):
        width = float(widths[i])
        i_lcl = -1.0
        i_ucl = i_lcl + width
        rows.append(
            {
                "date": dt,
                "ticker": ticker,
                "market_cap": 1_000_000_000.0,
                "i_value": 0.0,
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


def test_09_decide_applies_bandexp_ret62_veto_with_substitution(tmp_path, monkeypatch):
    decide = _load_decide_module()
    monkeypatch.setenv("USA_OPS_CANONICAL_PATH", "")
    monkeypatch.setattr(decide, "ROOT", tmp_path)
    monkeypatch.setattr(decide, "OUT_DIR", tmp_path / "data" / "daily")
    monkeypatch.setattr(decide, "LAST_REBALANCE_PATH", decide.OUT_DIR / "last_rebalance.json")

    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "features").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "ssot").mkdir(parents=True, exist_ok=True)

    winner = {
        "winner_config_snapshot": {
            "top_n": 3,
            "buffer_k": 1,
            "rebalance_cadence": 10,
            "rebalance_anchor_date": "2099-01-01",
            "rebalance_phase_offset": 0,
            "max_weight_cap": 0.06,
            "min_market_cap": 300000000.0,
        }
    }
    (tmp_path / "config" / "winner_us.json").write_text(
        json.dumps(winner, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    tickers = ["GATE_HI", "RET_ONLY", "BAND_ONLY", "ALT1", "ALT2"]
    dates_all = pd.date_range("2026-01-01", periods=31, freq="D")
    prev_dt = dates_all[-2]

    rank_map = {"GATE_HI": 1, "RET_ONLY": 2, "BAND_ONLY": 3, "ALT1": 4, "ALT2": 5}
    ret_prev = {"GATE_HI": 1.20, "RET_ONLY": 1.10, "BAND_ONLY": 0.90, "ALT1": 0.20, "ALT2": 0.20}

    score_rows: list[dict[str, object]] = []
    for dt in dates_all:
        for tk in tickers:
            ret_62 = ret_prev[tk] if pd.Timestamp(dt) == prev_dt else 0.20
            score_rows.append(
                {
                    "date": dt,
                    "ticker": tk,
                    "m3_rank": rank_map[tk],
                    "score_m3": float(100.0 - rank_map[tk]),
                    "ret_62": ret_62,
                }
            )
    scores_df = pd.DataFrame(score_rows)
    scores_df.to_parquet(tmp_path / "data" / "features" / "scores_m3_us.parquet", index=False)

    hist_dates = dates_all[:-1]
    canonical_rows: list[dict[str, object]] = []
    canonical_rows += _rows_for_ticker("GATE_HI", hist_dates, [1.0 + 0.40 * i for i in range(len(hist_dates))])
    canonical_rows += _rows_for_ticker("BAND_ONLY", hist_dates, [1.0 + 0.35 * i for i in range(len(hist_dates))])
    canonical_rows += _rows_for_ticker("RET_ONLY", hist_dates, [3.5 for _ in hist_dates])
    canonical_rows += _rows_for_ticker("ALT1", hist_dates, [3.0 for _ in hist_dates])
    canonical_rows += _rows_for_ticker("ALT2", hist_dates, [4.0 - 0.05 * i for i in range(len(hist_dates))])
    canonical_df = pd.DataFrame(canonical_rows)
    canonical_df.to_parquet(tmp_path / "data" / "ssot" / "canonical_us.parquet", index=False)

    out = decide.run(dry_run=True)

    selected = out["selected_tickers"]
    assert out["ranking_schema_version"] == 3
    assert "GATE_HI" not in selected
    assert selected == ["RET_ONLY", "BAND_ONLY", "ALT1"]

    veto_events = out.get("bandexp_ret62_veto_events", [])
    assert len(veto_events) == 1
    assert veto_events[0]["ticker"] == "GATE_HI"
    assert int(veto_events[0]["m3_rank"]) == 1

    # ret_62 alto isolado não veta sem BandExp.
    assert "RET_ONLY" in selected

    # BandExp isolado não veta sem ret_62 >= 1.00.
    assert "BAND_ONLY" in selected
