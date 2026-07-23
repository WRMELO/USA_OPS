from __future__ import annotations

import io
import importlib.util
import json
import sys
import types
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


def _capture_handler_class(monkeypatch, servidor, *, override_day: date):
    captured: dict[str, object] = {}

    class DummyHTTPServer:
        def __init__(self, _addr, handler_cls):
            captured["handler_cls"] = handler_cls

        def serve_forever(self):
            return

        def server_close(self):
            return

    import http.server as http_server

    monkeypatch.setattr(http_server, "ThreadingHTTPServer", DummyHTTPServer)
    servidor.serve(host="127.0.0.1", port=0, auto_open=False, override_date=override_day)
    return captured["handler_cls"]


def _make_handler_instance(handler_cls, *, path: str, form_body: str):
    handler = object.__new__(handler_cls)
    payload = form_body.encode("utf-8")
    handler.path = path
    handler.headers = {"Content-Length": str(len(payload))}
    handler.rfile = io.BytesIO(payload)
    calls: dict[str, object] = {}
    handler._redirect = types.MethodType(lambda self, target: calls.setdefault("redirect", target), handler)
    handler._respond_html = types.MethodType(
        lambda self, html, code=200: calls.setdefault("respond_html", {"html": html, "code": code}),
        handler,
    )
    handler._respond_json = types.MethodType(
        lambda self, payload, code=200: calls.setdefault("respond_json", {"payload": payload, "code": code}),
        handler,
    )
    return handler, calls


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


def test_painel_liquidar_route_success_redirects(monkeypatch):
    servidor = _load_servidor_module()
    handler_cls = _capture_handler_class(monkeypatch, servidor, override_day=date(2026, 7, 23))
    calls_confirm: dict[str, object] = {}
    monkeypatch.setattr(
        servidor.real_boletim_web,
        "confirm_settlement",
        lambda exec_day, ledger_dir, sell_id, amount=None: calls_confirm.update(
            {"exec_day": exec_day, "ledger_dir": ledger_dir, "sell_id": sell_id, "amount": amount}
        ),
    )
    handler, calls = _make_handler_instance(
        handler_cls,
        path="/painel/liquidar",
        form_body="exec_day=2026-07-23&sell_id=SELL123&amount=42.50",
    )

    handler_cls.do_POST(handler)

    assert calls.get("redirect") == "/painel"
    assert calls_confirm["exec_day"] == date(2026, 7, 23)
    assert calls_confirm["sell_id"] == "SELL123"
    assert abs(float(calls_confirm["amount"]) - 42.5) < 1e-9


def test_painel_liquidar_route_requires_sell_id(monkeypatch):
    servidor = _load_servidor_module()
    handler_cls = _capture_handler_class(monkeypatch, servidor, override_day=date(2026, 7, 23))
    handler, calls = _make_handler_instance(
        handler_cls,
        path="/painel/liquidar",
        form_body="exec_day=2026-07-23&sell_id=&amount=10.00",
    )

    handler_cls.do_POST(handler)

    response = calls.get("respond_html")
    assert isinstance(response, dict)
    assert response["code"] == 400
    assert "sell_id obrigatorio" in str(response["html"])


def test_painel_liquidar_route_blocks_historic_exec_day(monkeypatch):
    servidor = _load_servidor_module()
    handler_cls = _capture_handler_class(monkeypatch, servidor, override_day=date(2026, 7, 23))
    handler, calls = _make_handler_instance(
        handler_cls,
        path="/painel/liquidar",
        form_body="exec_day=2026-07-22&sell_id=SELL123&amount=10.00",
    )

    handler_cls.do_POST(handler)

    response = calls.get("respond_html")
    assert isinstance(response, dict)
    assert response["code"] == 403
    assert "Somente o painel do dia atual pode confirmar liquidacao." in str(response["html"])


def test_painel_encerrar_route_returns_409_when_close_day_is_blocked(monkeypatch):
    servidor = _load_servidor_module()
    handler_cls = _capture_handler_class(monkeypatch, servidor, override_day=date(2026, 7, 23))
    monkeypatch.setattr(
        servidor.real_boletim_web,
        "close_day",
        lambda exec_day, ledger_dir, caixa_real=None: {
            "exec_day": exec_day.isoformat(),
            "error": "MISSING_PRICE_SSOT",
            "message": "Encerramento bloqueado: sem preco SSOT para RLJ.",
        },
    )
    handler, calls = _make_handler_instance(
        handler_cls,
        path="/painel/encerrar",
        form_body="exec_day=2026-07-23&confirmar=sim&caixa_real=915.16",
    )

    handler_cls.do_POST(handler)

    response = calls.get("respond_html")
    assert isinstance(response, dict)
    assert response["code"] == 409
    assert "Encerramento bloqueado" in str(response["html"])
    assert "redirect" not in calls
