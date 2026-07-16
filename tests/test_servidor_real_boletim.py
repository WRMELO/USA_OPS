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
