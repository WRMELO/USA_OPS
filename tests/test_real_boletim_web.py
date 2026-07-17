from __future__ import annotations

import json
from datetime import date
from pathlib import Path

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
    assert "Livro de operacoes por ativo" in html
    assert "Top-20 operacional (m3_rank)" in html
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

