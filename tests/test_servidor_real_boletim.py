from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_servidor_module():
    root = Path(__file__).resolve().parents[1]
    script = root / "pipeline" / "servidor.py"
    module_name = "servidor_test_module"
    spec = importlib.util.spec_from_file_location(module_name, script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_real_test_active_without_ledger(tmp_path, monkeypatch):
    servidor = _load_servidor_module()
    monkeypatch.setattr(servidor, "ROOT", tmp_path)
    assert servidor._real_test_active() is False


def test_real_test_active_with_aporte(tmp_path, monkeypatch):
    servidor = _load_servidor_module()
    monkeypatch.setattr(servidor, "ROOT", tmp_path)

    ledger_path = tmp_path / "data" / "live_real_test" / "ledger_real.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps({"type": "APORTE", "amount": 20008.72, "exec_date": "2026-07-16"}) + "\n",
        encoding="utf-8",
    )
    assert servidor._real_test_active() is True


def test_real_test_active_without_aporte(tmp_path, monkeypatch):
    servidor = _load_servidor_module()
    monkeypatch.setattr(servidor, "ROOT", tmp_path)

    ledger_path = tmp_path / "data" / "live_real_test" / "ledger_real.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps({"type": "BUY", "amount": 100.0, "exec_date": "2026-07-16"}) + "\n",
        encoding="utf-8",
    )
    assert servidor._real_test_active() is False


def test_apply_boletim_operations_persists_payload_and_paths(tmp_path, monkeypatch):
    servidor = _load_servidor_module()
    monkeypatch.setattr(servidor, "ROOT", tmp_path)

    appended_events = []
    monkeypatch.setattr(servidor, "create_event", lambda event_type, **kwargs: {"type": event_type, **kwargs})
    monkeypatch.setattr(servidor, "is_duplicate", lambda _ev: False)
    monkeypatch.setattr(servidor, "append_event", lambda ev: appended_events.append(ev))
    monkeypatch.setattr(servidor, "pending_settlements", lambda _day: [])
    monkeypatch.setattr(servidor, "compute_cash", lambda _day: {"cash_free": 125.5, "cash_accounting": 20.0})
    monkeypatch.setattr(servidor, "export_snapshot", lambda _day: [{"ticker": "MRVI", "qtd": 10}])

    payload = {
        "exec_day": "2026-07-16",
        "market_day": "2026-07-15",
        "trade_day": "2026-07-16",
        "operations": [{"type": "COMPRA", "ticker": "MRVI", "qtd": 10, "preco": 7.0}],
        "cash_movements": [{"type": "APORTE", "value": 20008.72}],
        "cash_transfers": [],
    }
    out = servidor.apply_boletim_operations(payload)
    assert out["ok"] is True
    assert len(out["paths"]) == 2

    out_cycle = tmp_path / out["paths"][0]
    out_real = tmp_path / out["paths"][1]
    assert out_cycle.exists()
    assert out_real.exists()

    saved = json.loads(out_real.read_text(encoding="utf-8"))
    assert saved["market_day"] == "2026-07-15"
    assert saved["exec_day"] == "2026-07-16"
    assert saved["cash_free"] == 125.5
    assert saved["cash_accounting"] == 20.0
    assert len(saved["operations"]) == 1
    assert len(appended_events) == 2
