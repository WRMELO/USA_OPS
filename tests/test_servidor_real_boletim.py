from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
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


def test_real_boletim_payload_path_resolution(tmp_path, monkeypatch):
    servidor = _load_servidor_module()
    monkeypatch.setattr(servidor, "ROOT", tmp_path)
    today = date(2026, 7, 16)

    assert servidor._real_boletim_payload_path(today) is None

    real_dir = tmp_path / "data" / "live_real_test"
    real_dir.mkdir(parents=True, exist_ok=True)
    abertura_path = real_dir / f"abertura_{today.isoformat()}.json"
    fechamento_path = real_dir / f"{today.isoformat()}.json"

    abertura_path.write_text("{}", encoding="utf-8")
    assert servidor._real_boletim_payload_path(today) == (abertura_path, "abertura")

    abertura_path.unlink()
    fechamento_path.write_text("{}", encoding="utf-8")
    assert servidor._real_boletim_payload_path(today) == (fechamento_path, "fechamento")

    abertura_path.write_text("{}", encoding="utf-8")
    assert servidor._real_boletim_payload_path(today) == (abertura_path, "abertura")


def test_render_real_boletim_html_contains_expected_markers():
    servidor = _load_servidor_module()
    payload = {
        "cash_free": 20008.72,
        "cash_accounting": 0.0,
        "top_operational": [],
        "positions_snapshot": [],
    }
    html = servidor._render_real_boletim_html(payload, date(2026, 7, 16), "abertura")
    assert "LIVE-REAL-TEST" in html
    assert "Nenhuma posicao registrada ainda" in html


def test_render_real_boletim_html_shows_m3_rank_column():
    servidor = _load_servidor_module()
    payload = {
        "cash_free": 20008.72,
        "cash_accounting": 0.0,
        "top_operational": [
            {
                "rank": 1,
                "m3_rank": 45,
                "ticker": "FCEL",
                "score_m3": 2.51,
                "target_weight": 0.05,
                "close_d1": 20.25,
            }
        ],
        "positions_snapshot": [],
    }
    html = servidor._render_real_boletim_html(payload, date(2026, 7, 16), "abertura")
    assert "M3 Rank" in html
    assert ">45<" in html
