from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd


def _load_analise_module():
    root = Path(__file__).resolve().parents[1]
    script = root / "pipeline" / "analise_us.py"
    module_name = "analise_us_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _write_fixture(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_payload = {
        "winner_config_snapshot": {
            "top_n": 20,
            "rebalance_cadence": 10,
            "rebalance_anchor_date": "2026-07-01",
            "max_weight_cap": 0.06,
        }
    }
    (config_dir / "winner_us.json").write_text(
        json.dumps(config_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    daily_dir = tmp_path / "data" / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    decision_payload = {
        "scores_reference_date_d_minus_1": "2026-07-15",
        "action": "HOLD",
        "is_rebalance_day": False,
        "selected_tickers": ["AAA", "BBB", "CCC"],
        "target_weights": {"AAA": 0.05, "BBB": 0.05, "CCC": 0.05},
        "operational_ranking": [
            {"rank": 3, "m3_rank": 1, "ticker": "AAA", "score_m3": 9.5, "target_weight": 0.05, "bucket": "LISTA"},
            {"rank": 1, "m3_rank": 2, "ticker": "BBB", "score_m3": 8.1, "target_weight": 0.05, "bucket": "LISTA"},
            {"rank": 2, "m3_rank": 3, "ticker": "CCC", "score_m3": 7.2, "target_weight": 0.05, "bucket": "LISTA"},
        ],
    }
    (daily_dir / "decision_2026-07-16.json").write_text(
        json.dumps(decision_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    ssot_dir = tmp_path / "data" / "ssot"
    ssot_dir.mkdir(parents=True, exist_ok=True)
    opw = pd.DataFrame(
        [
            {
                "date": "2026-07-15",
                "ticker": "AAA",
                "close_operational": 10.0,
                "i_value": 0.01,
                "i_ucl": 0.05,
                "i_lcl": -0.05,
                "mr_value": 0.01,
                "mr_ucl": 0.06,
                "xbar_value": 0.01,
                "xbar_ucl": 0.05,
                "xbar_lcl": -0.05,
                "r_value": 0.01,
                "r_ucl": 0.06,
            },
            {
                "date": "2026-07-15",
                "ticker": "BBB",
                "close_operational": 11.0,
                "i_value": 0.01,
                "i_ucl": 0.05,
                "i_lcl": -0.05,
                "mr_value": 0.01,
                "mr_ucl": 0.06,
                "xbar_value": 0.01,
                "xbar_ucl": 0.05,
                "xbar_lcl": -0.05,
                "r_value": 0.01,
                "r_ucl": 0.06,
            },
            {
                "date": "2026-07-15",
                "ticker": "CCC",
                "close_operational": 12.0,
                "i_value": 0.01,
                "i_ucl": 0.05,
                "i_lcl": -0.05,
                "mr_value": 0.01,
                "mr_ucl": 0.06,
                "xbar_value": 0.01,
                "xbar_ucl": 0.05,
                "xbar_lcl": -0.05,
                "r_value": 0.01,
                "r_ucl": 0.06,
            },
        ]
    )
    opw["date"] = pd.to_datetime(opw["date"])
    opw.to_parquet(ssot_dir / "operational_window.parquet", index=False)

    real_test_dir = tmp_path / "data" / "live_real_test"
    real_test_dir.mkdir(parents=True, exist_ok=True)
    (real_test_dir / "ledger_real.jsonl").write_text(
        json.dumps({"type": "APORTE", "exec_date": "2026-07-16", "amount": 20008.72}) + "\n",
        encoding="utf-8",
    )


def test_real_test_active_detection(tmp_path, monkeypatch):
    analise = _load_analise_module()
    monkeypatch.setattr(analise, "ROOT", tmp_path)
    ledger_path = tmp_path / "data" / "live_real_test" / "ledger_real.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps({"type": "APORTE", "exec_date": "2026-07-16", "amount": 20008.72}) + "\n",
        encoding="utf-8",
    )
    assert analise._real_test_active(ledger_path) is True


def test_real_test_inactive_without_aporte(tmp_path, monkeypatch):
    analise = _load_analise_module()
    monkeypatch.setattr(analise, "ROOT", tmp_path)
    ledger_path = tmp_path / "data" / "live_real_test" / "ledger_real.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps({"type": "BUY", "exec_date": "2026-07-16", "amount": 100.0}) + "\n",
        encoding="utf-8",
    )
    assert analise._real_test_active(ledger_path) is False


def test_load_real_ledger_doc_reads_cash_and_zeroed_positions(tmp_path):
    analise = _load_analise_module()
    ledger_path = tmp_path / "ledger_real.jsonl"
    ledger_path.write_text(
        json.dumps({"type": "APORTE", "exec_date": "2026-07-16", "amount": 20008.72}) + "\n",
        encoding="utf-8",
    )

    original_ledger_path = analise._ledger_mod.LEDGER_PATH
    real_doc = analise._load_real_ledger_doc(ledger_path, date(2026, 7, 16))

    assert real_doc["positions_snapshot"] == []
    assert abs(float(real_doc["cash_free"]) - 20008.72) < 1e-9
    assert abs(float(real_doc["cash_accounting"]) - 0.0) < 1e-9
    assert analise._ledger_mod.LEDGER_PATH == original_ledger_path


def test_build_context_real_test_mode_uses_real_ledger(tmp_path, monkeypatch):
    analise = _load_analise_module()
    _write_fixture(tmp_path)
    monkeypatch.setattr(analise, "ROOT", tmp_path)

    ctx = analise.build_context(date(2026, 7, 15))

    assert ctx["real_test"]["active"] is True
    assert ctx["real_test"]["exec_day"] == "2026-07-16"
    assert ctx["holdings"] == []
    assert abs(float(ctx["cash"]["cash_free"]) - 20008.72) < 1e-9
    assert abs(float(ctx["cash"]["cash_accounting"]) - 0.0) < 1e-9
    assert [c["ticker"] for c in ctx["candidates"]] == ["AAA", "BBB", "CCC"]


def test_build_context_dry_run_mode_when_no_real_ledger(tmp_path, monkeypatch):
    analise = _load_analise_module()
    _write_fixture(tmp_path)
    (tmp_path / "data" / "live_real_test" / "ledger_real.jsonl").unlink()
    monkeypatch.setattr(analise, "ROOT", tmp_path)

    ctx = analise.build_context(date(2026, 7, 15))

    assert ctx["real_test"]["active"] is False
    assert abs(float(ctx["cash"]["cash_free"]) - 0.0) < 1e-9


def test_normalize_positions_keeps_fractional_qty_and_weighted_cost():
    analise = _load_analise_module()
    out = analise._normalize_positions(
        [
            {"ticker": "HNGE", "qtd": 11.554936, "preco_compra": 86.54, "data_compra": "2026-07-17"},
            {"ticker": "HNGE", "qtd": 2.0, "preco_compra": 81.21, "data_compra": "2026-07-22"},
        ]
    )
    assert len(out) == 1
    row = out[0]
    assert row["ticker"] == "HNGE"
    assert abs(float(row["qty"]) - 13.554936) < 1e-9
    expected_avg = ((11.554936 * 86.54) + (2.0 * 81.21)) / 13.554936
    assert abs(float(row["avg_cost"]) - round(expected_avg, 4)) < 1e-9
    assert row["purchase_date"] == "2026-07-17"


def test_build_context_real_test_keeps_fractional_qty_in_holdings(tmp_path, monkeypatch):
    analise = _load_analise_module()
    _write_fixture(tmp_path)
    monkeypatch.setattr(analise, "ROOT", tmp_path)

    ledger_path = tmp_path / "data" / "live_real_test" / "ledger_real.jsonl"
    previous_path = analise._ledger_mod.LEDGER_PATH
    try:
        ledger_path.write_text("", encoding="utf-8")
        analise._ledger_mod.LEDGER_PATH = ledger_path
        aporte = analise._ledger_mod.create_event(analise._ledger_mod.EventType.APORTE, date(2026, 7, 16), 20008.72)
        analise._ledger_mod.append_event(aporte)
        buy = analise._ledger_mod.create_event(
            analise._ledger_mod.EventType.BUY,
            date(2026, 7, 16),
            12.0,
            ticker="AAA",
            qtd=1.5,
            price=8.0,
            settle_date=date(2026, 7, 16),
        )
        analise._ledger_mod.append_event(buy)
    finally:
        analise._ledger_mod.LEDGER_PATH = previous_path

    ctx = analise.build_context(date(2026, 7, 15))
    assert ctx["real_test"]["active"] is True
    row = next(item for item in ctx["holdings"] if item["ticker"] == "AAA")
    assert abs(float(row["qty"]) - 1.5) < 1e-9
