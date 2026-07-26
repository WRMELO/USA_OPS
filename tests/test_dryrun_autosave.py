from __future__ import annotations

import json
from datetime import date

import pipeline.dryrun_autosave as dryrun_autosave


def test_autosave_pending_days_is_idempotent_when_day_already_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(dryrun_autosave, "ROOT", tmp_path)
    real_dir = tmp_path / "data" / "real"
    real_dir.mkdir(parents=True, exist_ok=True)
    (real_dir / "2026-07-15.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(dryrun_autosave.painel_diario, "get_d_minus_1", lambda _d: date(2026, 7, 15))
    monkeypatch.setattr(dryrun_autosave, "_load_trading_days", lambda _d: [date(2026, 7, 15)])

    calls = []
    monkeypatch.setattr(
        dryrun_autosave.servidor,
        "apply_boletim_operations",
        lambda payload: calls.append(payload) or {"ok": True, "paths": []},
    )

    out = dryrun_autosave.autosave_pending_days(as_of=date(2026, 7, 16))
    assert out == []
    assert calls == []


def test_autosave_pending_days_catchup_logs_and_skips_second_run(tmp_path, monkeypatch):
    monkeypatch.setattr(dryrun_autosave, "ROOT", tmp_path)
    real_dir = tmp_path / "data" / "real"
    real_dir.mkdir(parents=True, exist_ok=True)
    (real_dir / "2026-07-14.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(dryrun_autosave.painel_diario, "get_d_minus_1", lambda _d: date(2026, 7, 16))
    monkeypatch.setattr(
        dryrun_autosave,
        "_load_trading_days",
        lambda _d: [date(2026, 7, 14), date(2026, 7, 15), date(2026, 7, 16)],
    )
    monkeypatch.setattr(
        dryrun_autosave,
        "_market_day_to_exec_day",
        lambda market_day: date(2026, 7, 16) if market_day == date(2026, 7, 15) else date(2026, 7, 17),
    )

    def _fake_compute(exec_day: date):
        if exec_day == date(2026, 7, 16):
            return {
                "exec_day": "2026-07-16",
                "market_day": "2026-07-15",
                "trade_day": "2026-07-16",
                "operations": [{"type": "VENDA", "ticker": "OLD", "qtd": 10, "preco": 10.0}],
                "cash_transfers": [{"value": 100.0, "note": "SELL-OLD"}],
            }
        return {
            "exec_day": "2026-07-17",
            "market_day": "2026-07-16",
            "trade_day": "2026-07-17",
            "operations": [],
            "cash_transfers": [],
        }

    monkeypatch.setattr(dryrun_autosave.painel_diario, "compute_dryrun_autosave_operations", _fake_compute)

    applied_payloads = []

    def _fake_apply(payload):
        applied_payloads.append(payload)
        out_real = real_dir / f"{payload['market_day']}.json"
        out_real.write_text(json.dumps(payload), encoding="utf-8")
        return {"ok": True, "paths": [f"data/real/{payload['market_day']}.json"]}

    monkeypatch.setattr(dryrun_autosave.servidor, "apply_boletim_operations", _fake_apply)

    first = dryrun_autosave.autosave_pending_days(as_of=date(2026, 7, 17))
    assert [row["market_day"] for row in first] == ["2026-07-15", "2026-07-16"]
    assert [x["market_day"] for x in applied_payloads] == ["2026-07-15", "2026-07-16"]
    assert applied_payloads[0]["cash_transfers"] == [{"value": 100.0, "note": "SELL-OLD"}]
    assert applied_payloads[1]["cash_transfers"] == []

    log_path = tmp_path / "data" / "daily" / "autosave_log.jsonl"
    assert log_path.exists()
    lines = [json.loads(x) for x in log_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) == 2
    assert [x["market_day"] for x in lines] == ["2026-07-15", "2026-07-16"]

    second = dryrun_autosave.autosave_pending_days(as_of=date(2026, 7, 17))
    assert second == []
