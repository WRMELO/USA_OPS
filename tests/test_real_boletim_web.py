from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

import pipeline.ledger as ledger
import pipeline.real_boletim_web as real_boletim_web


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


def test_add_operation_roundtrip_persists_only_draft(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 16)

    real_boletim_web.add_operation(
        exec_day,
        tipo="COMPRA",
        ticker="MRVI",
        qtd=10,
        preco=7.0,
        corretagem=2.5,
        preco_sombra=6.9,
    )

    payload = real_boletim_web.load_draft(exec_day)
    assert len(payload["operations"]) == 1
    assert payload["operations"][0]["ticker"] == "MRVI"
    assert not (tmp_path / "drafts" / "ledger_real.jsonl").exists()


def test_remove_operation_keeps_remaining_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 16)

    draft = real_boletim_web.add_operation(
        exec_day,
        tipo="COMPRA",
        ticker="MRVI",
        qtd=10,
        preco=7.0,
        corretagem=2.5,
        preco_sombra=6.9,
    )
    draft = real_boletim_web.add_operation(
        exec_day,
        tipo="VENDA",
        ticker="HPP",
        qtd=2,
        preco=16.0,
        corretagem=1.2,
        preco_sombra=15.8,
        liquidacao="EM_LIQUIDACAO",
    )
    op_ids = [op["id"] for op in draft["operations"]]
    assert len(op_ids) == 2

    updated = real_boletim_web.remove_operation(exec_day, op_ids[0])
    assert len(updated["operations"]) == 1
    assert updated["operations"][0]["id"] == op_ids[1]


def test_apply_draft_to_ledger_creates_buy_fee_and_shadow_and_preserves_ssot(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 16)
    ledger_dir = tmp_path / "live_real"

    ssot_path = real_boletim_web.ROOT / "data" / "ssot" / "ledger.jsonl"
    ssot_before = _count_lines(ssot_path)

    operations = [
        {
            "id": "op-1",
            "type": "COMPRA",
            "ticker": "MRVI",
            "qtd": 10,
            "preco": 7.0,
            "corretagem": 2.5,
            "preco_sombra": 6.8,
        }
    ]

    result = real_boletim_web.apply_draft_to_ledger(exec_day, ledger_dir, operations)
    assert len(result["events_created"]) >= 3

    real_ledger_path = ledger_dir / "ledger_real.jsonl"
    shadow_ledger_path = ledger_dir / "ledger_shadow.jsonl"
    assert real_ledger_path.exists()
    assert shadow_ledger_path.exists()

    real_events = [
        json.loads(line)
        for line in real_ledger_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    shadow_events = [
        json.loads(line)
        for line in shadow_ledger_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    buys = [ev for ev in real_events if ev.get("type") == "BUY"]
    fees = [ev for ev in real_events if ev.get("type") == "FEE"]
    assert len(buys) == 1
    assert len(fees) == 1
    assert float(fees[0]["amount"]) == 2.5
    assert fees[0]["ref_id"] == buys[0]["id"]
    assert any(ev.get("type") == "BUY" for ev in shadow_events)

    expected_real_path = ledger_dir / "ledger_real.jsonl"
    assert real_boletim_web.ledger_mod.LEDGER_PATH == expected_real_path

    ssot_after = _count_lines(ssot_path)
    assert ssot_after == ssot_before


def test_add_operation_venda_requires_liquidacao(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 16)
    with pytest.raises(ValueError):
        real_boletim_web.add_operation(
            exec_day,
            tipo="VENDA",
            ticker="PENG",
            qtd=3,
            preco=55.6,
            corretagem=2.5,
        )


def test_apply_draft_venda_ja_no_caixa_creates_settlement_and_zeros_accounting(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 21)
    ledger_dir = tmp_path / "live_real"
    operations = [
        {
            "id": "op-v1",
            "type": "VENDA",
            "ticker": "PENG",
            "qtd": 16.734846,
            "preco": 55.6,
            "corretagem": 0.0,
            "preco_sombra": 0.0,
            "liquidacao": "JA_NO_CAIXA",
        }
    ]

    real_boletim_web.apply_draft_to_ledger(exec_day, ledger_dir, operations)
    real_events = [
        json.loads(line)
        for line in (ledger_dir / "ledger_real.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    sells = [ev for ev in real_events if ev.get("type") == "SELL"]
    settlements = [ev for ev in real_events if ev.get("type") == "SETTLEMENT"]
    assert len(sells) == 1
    assert len(settlements) == 1
    assert settlements[0]["ref_id"] == sells[0]["id"]
    assert "liquidacao=JA_NO_CAIXA" in str(sells[0].get("reason", ""))

    prev = ledger.LEDGER_PATH
    try:
        ledger.LEDGER_PATH = ledger_dir / "ledger_real.jsonl"
        cash = ledger.compute_cash(exec_day)
    finally:
        ledger.LEDGER_PATH = prev
    assert abs(float(cash["cash_accounting"])) < 0.01


def test_apply_draft_venda_em_liquidacao_keeps_accounting(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 21)
    ledger_dir = tmp_path / "live_real"
    operations = [
        {
            "id": "op-v2",
            "type": "VENDA",
            "ticker": "PENG",
            "qtd": 16.734846,
            "preco": 55.6,
            "corretagem": 0.0,
            "preco_sombra": 0.0,
            "liquidacao": "EM_LIQUIDACAO",
        }
    ]

    real_boletim_web.apply_draft_to_ledger(exec_day, ledger_dir, operations)
    real_events = [
        json.loads(line)
        for line in (ledger_dir / "ledger_real.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    sells = [ev for ev in real_events if ev.get("type") == "SELL"]
    settlements = [ev for ev in real_events if ev.get("type") == "SETTLEMENT"]
    assert len(sells) == 1
    assert settlements == []

    prev = ledger.LEDGER_PATH
    try:
        ledger.LEDGER_PATH = ledger_dir / "ledger_real.jsonl"
        cash = ledger.compute_cash(exec_day)
    finally:
        ledger.LEDGER_PATH = prev
    assert abs(float(cash["cash_accounting"]) - float(sells[0]["amount"])) < 0.02


def test_close_day_generates_artifacts_and_archives_draft(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 16)
    ledger_dir = tmp_path / "live_real"

    real_boletim_web.add_operation(
        exec_day,
        tipo="COMPRA",
        ticker="MRVI",
        qtd=5,
        preco=7.0,
        corretagem=2.5,
        preco_sombra=6.9,
    )
    draft_file = real_boletim_web.draft_path(exec_day)
    assert draft_file.exists()

    out = real_boletim_web.close_day(exec_day, ledger_dir)
    assert Path(out["boletim_path"]).exists()
    assert Path(out["friction_report_path"]).exists()
    assert out["archived_draft_path"] is not None
    assert Path(out["archived_draft_path"]).exists()
    assert not draft_file.exists()


def test_close_stale_drafts_closes_only_open_stale_files(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    today = date(2026, 7, 17)
    stale_day = date(2026, 7, 16)
    current_day = date(2026, 7, 17)
    old_archived_day = date(2026, 7, 15)
    ledger_dir = tmp_path / "live_real"

    real_boletim_web.add_operation(
        stale_day,
        tipo="COMPRA",
        ticker="MRVI",
        qtd=3,
        preco=7.0,
        corretagem=2.5,
        preco_sombra=6.9,
    )
    real_boletim_web.add_operation(
        current_day,
        tipo="COMPRA",
        ticker="HPP",
        qtd=2,
        preco=15.0,
        corretagem=1.0,
        preco_sombra=14.8,
    )

    archived_path = real_boletim_web.DRAFT_DIR / f"draft_{old_archived_day.isoformat()}_encerrado_101010.json"
    archived_path.parent.mkdir(parents=True, exist_ok=True)
    archived_path.write_text("{}", encoding="utf-8")

    result = real_boletim_web.close_stale_drafts(today, ledger_dir)
    assert any(row.get("exec_day") == stale_day.isoformat() and row.get("auto_closed") is True for row in result)
    assert not real_boletim_web.draft_path(stale_day).exists()
    assert real_boletim_web.draft_path(current_day).exists()
    assert archived_path.exists()
    assert any(real_boletim_web.DRAFT_DIR.glob(f"draft_{stale_day.isoformat()}_encerrado_*.json"))
    assert (ledger_dir / f"{stale_day.isoformat()}.json").exists()
    assert (ledger_dir / f"friction_report_{stale_day.isoformat()}.json").exists()


def test_suggested_defensive_sells_uses_grave_threshold_for_heat():
    view = {
        "forno": {},
        "holdings": [
            {
                "ticker": "AAA",
                "heat_pct": -20.0,
                "spc_status": "ESTAVEL",
                "drawdown_pct": -5.0,
                "qty": 1.0,
                "close_d1": 10.0,
            },
            {
                "ticker": "BBB",
                "heat_pct": -33.0,
                "spc_status": "ESTAVEL",
                "drawdown_pct": -5.0,
                "qty": 2.0,
                "close_d1": 20.0,
            },
            {
                "ticker": "CCC",
                "heat_pct": -7.0,
                "spc_status": "ATENCAO",
                "drawdown_pct": -5.0,
                "qty": 3.0,
                "close_d1": 30.0,
            },
        ],
        "held_set": ["AAA", "BBB", "CCC"],
    }
    out = real_boletim_web._suggested_defensive_sells(view)
    defensive = out.get("defensive", [])
    tickers = [row.get("ticker") for row in defensive if isinstance(row, dict)]
    assert "AAA" not in tickers
    assert "BBB" in tickers
    assert "CCC" not in tickers


def test_confirm_settlement_creates_append_only_event(tmp_path):
    ledger_dir = tmp_path / "live_real"
    ledger_dir.mkdir(parents=True, exist_ok=True)
    exec_day = date(2026, 7, 23)
    sell_amount = 240.0
    prev = ledger.LEDGER_PATH
    try:
        ledger.LEDGER_PATH = ledger_dir / "ledger_real.jsonl"
        sell = ledger.create_event(
            ledger.EventType.SELL,
            exec_day,
            sell_amount,
            ticker="PENG",
            qtd=4.0,
            price=60.0,
            settle_date=exec_day,
        )
        ledger.append_event(sell)
        before = ledger.compute_cash(exec_day)
    finally:
        ledger.LEDGER_PATH = prev

    result = real_boletim_web.confirm_settlement(exec_day, ledger_dir, sell_id=sell.id)
    assert result["ok"] is True
    assert abs(float(result["amount"]) - sell_amount) < 1e-6

    prev = ledger.LEDGER_PATH
    try:
        ledger.LEDGER_PATH = ledger_dir / "ledger_real.jsonl"
        events = ledger.read_all_events()
        settlements = [ev for ev in events if ev.type == ledger.EventType.SETTLEMENT and ev.ref_id == sell.id]
        after = ledger.compute_cash(exec_day)
    finally:
        ledger.LEDGER_PATH = prev
    assert len(settlements) == 1
    assert abs(float(before["cash_accounting"]) - sell_amount) < 1e-6
    assert abs(float(after["cash_accounting"])) < 1e-6


def test_confirm_settlement_rejects_amount_above_pending(tmp_path):
    ledger_dir = tmp_path / "live_real"
    ledger_dir.mkdir(parents=True, exist_ok=True)
    exec_day = date(2026, 7, 23)
    prev = ledger.LEDGER_PATH
    try:
        ledger.LEDGER_PATH = ledger_dir / "ledger_real.jsonl"
        sell = ledger.create_event(
            ledger.EventType.SELL,
            exec_day,
            100.0,
            ticker="PENG",
            qtd=2.0,
            price=50.0,
            settle_date=exec_day,
        )
        ledger.append_event(sell)
    finally:
        ledger.LEDGER_PATH = prev

    with pytest.raises(ValueError):
        real_boletim_web.confirm_settlement(exec_day, ledger_dir, sell_id=sell.id, amount=200.0)


def test_confirm_settlement_rejects_unknown_sell_id(tmp_path):
    ledger_dir = tmp_path / "live_real"
    ledger_dir.mkdir(parents=True, exist_ok=True)
    exec_day = date(2026, 7, 23)
    with pytest.raises(ValueError):
        real_boletim_web.confirm_settlement(exec_day, ledger_dir, sell_id="SELL_INEXISTENTE")


def test_confirm_settlement_rejects_duplicate_confirmation(tmp_path):
    ledger_dir = tmp_path / "live_real"
    ledger_dir.mkdir(parents=True, exist_ok=True)
    exec_day = date(2026, 7, 23)
    prev = ledger.LEDGER_PATH
    try:
        ledger.LEDGER_PATH = ledger_dir / "ledger_real.jsonl"
        sell = ledger.create_event(
            ledger.EventType.SELL,
            exec_day,
            120.0,
            ticker="AAA",
            qtd=3.0,
            price=40.0,
            settle_date=exec_day,
        )
        ledger.append_event(sell)
    finally:
        ledger.LEDGER_PATH = prev

    first = real_boletim_web.confirm_settlement(exec_day, ledger_dir, sell_id=sell.id, amount=120.0)
    assert first["ok"] is True
    with pytest.raises(ValueError):
        real_boletim_web.confirm_settlement(exec_day, ledger_dir, sell_id=sell.id, amount=120.0)


def test_load_live_view_projects_draft_into_balancete_dfc(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 23)
    ledger_dir = tmp_path / "live_real"
    ledger_dir.mkdir(parents=True, exist_ok=True)

    prev = ledger.LEDGER_PATH
    try:
        ledger.LEDGER_PATH = ledger_dir / "ledger_real.jsonl"
        aporte = ledger.create_event(ledger.EventType.APORTE, exec_day, 1000.0)
        ledger.append_event(aporte)
        buy = ledger.create_event(
            ledger.EventType.BUY,
            exec_day,
            500.0,
            ticker="AAA",
            qtd=5.0,
            price=100.0,
            settle_date=exec_day,
        )
        ledger.append_event(buy)
    finally:
        ledger.LEDGER_PATH = prev

    real_boletim_web.add_operation(
        exec_day,
        tipo="COMPRA",
        ticker="AAA",
        qtd=1.0,
        preco=100.0,
        corretagem=2.5,
        preco_sombra=0.0,
    )

    view = real_boletim_web.load_live_view(exec_day, ledger_dir)
    assert abs(float(view["cash_free"]) - 500.0) < 1e-6
    assert abs(float(view["cash_free_projetado"]) - 397.5) < 1e-6
    assert len(view["positions"]) == 1
    assert len(view["positions_projetado"]) == 2
    assert abs(float(view["positions"][0]["qtd"]) - 5.0) < 1e-6
    assert abs(sum(float(row["qtd"]) for row in view["positions_projetado"]) - 6.0) < 1e-6
    assert "operations_book_projetado" in view
    assert "AAA" in view["operations_book_projetado"]
    assert abs(float(view["operations_book_projetado"]["AAA"]["qtd_liquida"]) - 6.0) < 1e-6


def test_render_live_html_has_new_layout_and_pending_liquidations_block():
    view = {
        "today": "2026-07-23",
        "market_day": "2026-07-22",
        "cash_free": 500.0,
        "cash_accounting": 100.0,
        "cash_free_projetado": 450.0,
        "cash_accounting_projetado": 120.0,
        "carteira_d1_valor": 1000.0,
        "carteira_projetada_valor": 980.0,
        "caixa_real_informado": 430.0,
        "friccao_balanco_real": 70.0,
        "positions": [],
        "positions_projetado": [],
        "held_set": [],
        "top_operational": [],
        "target_weights": {},
        "operations_book": {},
        "operations_book_projetado": {},
        "pending_settlements": [
            {
                "sell_id": "SELL123",
                "sale_date": "2026-07-22",
                "ticker": "PENG",
                "valor_venda": 200.0,
                "ja_transferido": 50.0,
                "pendente": 150.0,
            }
        ],
        "forno": {},
        "draft": {"operations": []},
        "closed_boletim_exists": False,
        "base1_series": [
            {"date": "2026-07-23", "nav": 1550.0, "base1": 1.0, "daily_var_pct": 0.0, "cagr_expect": 1.0}
        ],
        "corretagem_dia": 2.5,
        "corretagem_total": 10.0,
        "capital_em_uso": 1000.0,
        "sparklines_tickers": [],
    }
    html = real_boletim_web.render_live_html(view)
    assert "Liquidacoes pendentes de confirmacao" in html
    assert "Carteira fechada (ledger)" in html
    assert "Sugestao corrente do motor" not in html
    assert html.index("Adicionar operacao") < html.index("Rascunho operacional (persistente)")
    assert html.index("Encerramento definitivo do dia") > html.index("Mapa de custos")


def test_render_live_html_orders_by_m3_rank():
    view = {
        "today": "2026-07-16",
        "market_day": "2026-07-15",
        "cash_free": 18018.34,
        "cash_accounting": 0.0,
        "positions": [],
        "held_set": [],
        "top_operational": [
            {"ticker": "ZZZZ", "m3_rank": 9, "score_m3": 1.0, "close_d1": 10.0},
            {"ticker": "AAAA", "m3_rank": 1, "score_m3": 2.0, "close_d1": 20.0},
        ],
        "target_weights": {"ZZZZ": 0.03, "AAAA": 0.05},
        "operations_book": {
            "MRVI": {
                "ticker": "MRVI",
                "compras": [{"date": "2026-07-16", "qtd": 10, "preco": 7.0}],
                "vendas": [],
                "qtd_liquida": 10,
                "custo_medio": 7.0,
                "investido": 70.0,
                "realizado": 0.0,
                "close_d1": 7.2,
                "nao_realizado": 2.0,
            }
        },
        "forno": {},
        "draft": {"operations": []},
        "closed_boletim_exists": False,
    }
    html = real_boletim_web.render_live_html(view)
    assert "Carteira real" in html
    assert "Livro de operacoes" in html
    assert "Top-20 operacional" in html
    assert "M3 Rank" in html
    assert html.index("AAAA") < html.index("ZZZZ")


def test_add_operation_with_valor_investido_calculates_fractional_qty(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 16)

    real_boletim_web.add_operation(
        exec_day,
        tipo="COMPRA",
        ticker="MRVI",
        preco=6.9986,
        corretagem=2.5,
        valor_investido=1000.0,
        preco_sombra=6.9,
    )

    payload = real_boletim_web.load_draft(exec_day)
    row = payload["operations"][0]
    assert abs(float(row["qtd"]) - 142.88572) < 1e-4
    assert abs(float(row["valor_investido_informado"]) - 1000.0) < 1e-9


def test_render_live_html_shows_fractional_quantities():
    view = {
        "today": "2026-07-16",
        "market_day": "2026-07-15",
        "cash_free": 18018.34,
        "cash_accounting": 0.0,
        "positions": [
            {
                "ticker": "MRVI",
                "data_compra": "2026-07-16",
                "qtd": 142.88572,
                "preco_compra": 6.9986,
                "close_d1": 7.09,
                "heat_pct": 1.29,
            }
        ],
        "held_set": ["MRVI"],
        "top_operational": [],
        "target_weights": {},
        "operations_book": {
            "MRVI": {
                "ticker": "MRVI",
                "compras": [{"date": "2026-07-16", "qtd": 142.88572, "preco": 6.9986}],
                "vendas": [],
                "qtd_liquida": 142.88572,
                "custo_medio": 6.9986,
                "investido": 1000.0,
                "realizado": 0.0,
                "close_d1": 7.09,
                "nao_realizado": 12.86,
            }
        },
        "forno": {},
        "draft": {
            "operations": [
                {
                    "id": "op-frac",
                    "type": "COMPRA",
                    "ticker": "MRVI",
                    "qtd": 0.88572,
                    "preco": 6.9986,
                    "corretagem": 2.5,
                    "preco_sombra": 6.9,
                }
            ]
        },
        "closed_boletim_exists": False,
    }
    html = real_boletim_web.render_live_html(view)
    assert "142.88572" in html
    assert "0.88572" in html


def test_render_live_html_uses_lot_heat_and_lot_unrealized_values():
    view = {
        "today": "2026-07-23",
        "market_day": "2026-07-22",
        "cash_free": 500.0,
        "cash_accounting": 0.0,
        "positions": [
            {
                "ticker": "HNGE",
                "data_compra": "2026-07-17",
                "qtd": 11.554936,
                "preco_compra": 86.54,
                "close_d1": 79.08,
                "heat_pct": -8.62,
            },
            {
                "ticker": "HNGE",
                "data_compra": "2026-07-22",
                "qtd": 2.0,
                "preco_compra": 81.21,
                "close_d1": 79.08,
                "heat_pct": -2.62,
            },
        ],
        "held_set": ["HNGE"],
        "top_operational": [],
        "target_weights": {},
        "operations_book": {
            "HNGE": {
                "ticker": "HNGE",
                "compras": [
                    {"date": "2026-07-17", "qtd": 11.554936, "preco": 86.54},
                    {"date": "2026-07-22", "qtd": 2.0, "preco": 81.21},
                ],
                "vendas": [],
                "qtd_liquida": 13.554936,
                "custo_medio": 85.76,
                "investido": 1162.4,
                "realizado": 0.0,
                "close_d1": 79.08,
                "nao_realizado": -90.5,
            }
        },
        "forno": {},
        "draft": {"operations": []},
        "closed_boletim_exists": False,
    }
    html = real_boletim_web.render_live_html(view)
    assert "-8.62%" in html
    assert "-2.62%" in html
    assert "$ -86.20" in html
    assert "$ -4.26" in html


def test_close_day_records_caixa_real_informado_and_computes_friccao(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 18)
    ledger_dir = tmp_path / "live_real"

    out = real_boletim_web.close_day(exec_day, ledger_dir, caixa_real=950.0)
    assert Path(out["boletim_path"]).exists()

    boletim = json.loads(Path(out["boletim_path"]).read_text(encoding="utf-8"))
    assert abs(float(boletim["caixa_livre_real_informado"]) - 950.0) < 1e-6
    assert boletim["friccao_balanco_real"] is not None

    view = real_boletim_web.load_live_view(exec_day, ledger_dir)
    assert abs(float(view["caixa_real_informado"]) - 950.0) < 1e-6
    assert view["caixa_real_informado_date"] == exec_day.isoformat()
    assert view["friccao_balanco_real"] == round(float(view["cash_free"]) - 950.0, 2)


def test_close_day_without_caixa_real_leaves_it_pending(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 18)
    ledger_dir = tmp_path / "live_real"

    real_boletim_web.close_day(exec_day, ledger_dir)
    view = real_boletim_web.load_live_view(exec_day, ledger_dir)
    assert view["caixa_real_informado"] is None
    assert view["friccao_balanco_real"] is None


def test_load_live_view_exposes_base1_bridge_keys(tmp_path, monkeypatch):
    monkeypatch.setattr(real_boletim_web, "DRAFT_DIR", tmp_path / "drafts")
    exec_day = date(2026, 7, 18)
    ledger_dir = tmp_path / "live_real"

    real_boletim_web.close_day(exec_day, ledger_dir)
    view = real_boletim_web.load_live_view(exec_day, ledger_dir)

    assert "base1_series" in view
    assert "corretagem_dia" in view
    assert "corretagem_total" in view
    assert "capital_em_uso" in view
    assert "carteira_d1_valor" in view
    assert "sparklines_tickers" in view
    assert isinstance(view["base1_series"], list)
    assert isinstance(view["corretagem_dia"], float)
    assert isinstance(view["sparklines_tickers"], list)
    assert isinstance(view["corretagem_total"], float)
    assert isinstance(view["capital_em_uso"], float)


def test_render_live_html_has_bridge_spark_and_no_cdn(monkeypatch):
    monkeypatch.setattr(
        real_boletim_web,
        "_load_sparklines",
        lambda tickers, as_of, lookback=62: {"AAAA": "<svg class='spark'></svg>"},
    )
    view = {
        "today": "2026-07-16",
        "market_day": "2026-07-15",
        "cash_free": 961.22,
        "cash_accounting": 0.0,
        "caixa_real_informado": 950.0,
        "friccao_balanco_real": 11.22,
        "positions": [
            {
                "ticker": "MRVI",
                "data_compra": "2026-07-16",
                "qtd": 142.88572,
                "preco_compra": 6.9986,
                "close_d1": 7.09,
                "heat_pct": 1.29,
            }
        ],
        "held_set": ["MRVI"],
        "top_operational": [{"ticker": "AAAA", "m3_rank": 1, "score_m3": 2.0, "close_d1": 20.0}],
        "target_weights": {"AAAA": 0.05},
        "operations_book": {
            "MRVI": {
                "ticker": "MRVI",
                "compras": [{"date": "2026-07-16", "qtd": 142.88572, "preco": 6.9986}],
                "vendas": [],
                "qtd_liquida": 142.88572,
                "custo_medio": 6.9986,
                "investido": 1000.0,
                "realizado": 0.0,
                "close_d1": 7.09,
                "nao_realizado": 12.86,
            }
        },
        "forno": {},
        "draft": {"operations": []},
        "closed_boletim_exists": False,
        "base1_series": [
            {"date": "2026-07-16", "nav": 1000.0, "base1": 1.0, "daily_var_pct": 0.0, "cagr_expect": 1.0},
            {
                "date": "2026-07-17",
                "nav": 1010.0,
                "base1": 1.01,
                "daily_var_pct": 1.0,
                "cagr_expect": 1.001,
            },
        ],
        "corretagem_total": 47.5,
        "capital_em_uso": 20008.72,
        "carteira_d1_valor": 1000.0,
        "sparklines_tickers": ["AAAA"],
    }
    html = real_boletim_web.render_live_html(view)
    assert "Balancete simplificado" in html
    assert "DFC simplificado" in html
    assert "class='spark'" in html
    assert "QTY_FIXES" not in html
    assert "cdn." not in html
    assert '<script src="http' not in html
    assert "<script src='http" not in html

