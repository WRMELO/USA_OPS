from __future__ import annotations

from datetime import UTC, date, datetime

import pipeline.ledger as ledger
from pipeline.ledger import EventType, LedgerEvent


def _append(ev: LedgerEvent) -> None:
    ledger.append_event(ev)


def test_compute_positions_cash_and_pending(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    _append(
        LedgerEvent(
            id="E1",
            type=EventType.APORTE,
            exec_date=date(2026, 1, 2),
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _append(
        LedgerEvent(
            id="E2",
            type=EventType.BUY,
            exec_date=date(2026, 1, 2),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=10,
            price=50.0,
            amount=500.0,
            settle_date=date(2026, 1, 3),
        )
    )
    _append(
        LedgerEvent(
            id="E3",
            type=EventType.SELL,
            exec_date=date(2026, 1, 3),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=4,
            price=60.0,
            amount=240.0,
            settle_date=date(2026, 1, 4),
        )
    )

    pos = ledger.compute_positions(date(2026, 1, 3))
    assert "ABC" in pos
    assert sum(int(l["qtd"]) for l in pos["ABC"]) == 6

    cash_d3 = ledger.compute_cash(date(2026, 1, 3))
    assert abs(cash_d3["cash_free"] - 500.0) < 1e-9  # 1000 - 500
    assert abs(cash_d3["cash_accounting"] - 240.0) < 1e-9

    pending = ledger.pending_settlements(date(2026, 1, 4))
    assert len(pending) == 1
    assert pending[0]["sell_id"] == "E3"

    _append(
        LedgerEvent(
            id="E4",
            type=EventType.SETTLEMENT,
            exec_date=date(2026, 1, 4),
            created_at=datetime.now(tz=UTC),
            amount=240.0,
            ref_id="E3",
            settle_date=date(2026, 1, 4),
        )
    )
    cash_d4 = ledger.compute_cash(date(2026, 1, 4))
    assert abs(cash_d4["cash_free"] - 740.0) < 1e-9
    assert abs(cash_d4["cash_accounting"]) < 1e-9


def test_unmatched_settlement_reduces_accounting(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    _append(
        LedgerEvent(
            id="A1",
            type=EventType.APORTE,
            exec_date=date(2026, 1, 2),
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _append(
        LedgerEvent(
            id="S1",
            type=EventType.SELL,
            exec_date=date(2026, 1, 3),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=5,
            price=20.0,
            amount=100.0,
            settle_date=date(2026, 1, 4),
        )
    )
    _append(
        LedgerEvent(
            id="T1",
            type=EventType.SETTLEMENT,
            exec_date=date(2026, 1, 4),
            created_at=datetime.now(tz=UTC),
            amount=100.0,
            ref_id=None,
            reason="manual-transfer",
            settle_date=date(2026, 1, 4),
        )
    )

    cash_d4 = ledger.compute_cash(date(2026, 1, 4))
    assert abs(cash_d4["cash_free"] - 1100.0) < 1e-9
    assert abs(cash_d4["cash_accounting"]) < 1e-9
    assert ledger.pending_settlements(date(2026, 1, 4)) == []


def test_duplicate_event_not_appended(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    ev = LedgerEvent(
        id="D1",
        type=EventType.BUY,
        exec_date=date(2026, 1, 2),
        created_at=datetime.now(tz=UTC),
        ticker="ABC",
        qtd=10,
        price=10.0,
        amount=100.0,
        settle_date=date(2026, 1, 3),
    )
    _append(ev)
    assert ledger.is_duplicate(ev) is True


def test_sells_in_settlement_and_reconciliation(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    _append(
        LedgerEvent(
            id="R1",
            type=EventType.APORTE,
            exec_date=date(2026, 1, 2),
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _append(
        LedgerEvent(
            id="R2",
            type=EventType.SELL,
            exec_date=date(2026, 1, 3),
            created_at=datetime.now(tz=UTC),
            ticker="XYZ",
            qtd=5,
            price=40.0,
            amount=200.0,
            settle_date=date(2026, 1, 5),
        )
    )
    _append(
        LedgerEvent(
            id="R3",
            type=EventType.SELL,
            exec_date=date(2026, 1, 3),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=3,
            price=50.0,
            amount=150.0,
            settle_date=date(2026, 1, 4),
        )
    )

    cash_d1 = ledger.compute_cash(date(2026, 1, 3))
    assert abs(cash_d1["cash_accounting"] - 350.0) < 1e-9

    as_of = date(2026, 1, 4)
    pending = ledger.pending_settlements(as_of)
    in_settlement = ledger.sells_in_settlement(as_of)

    assert any(p["sell_id"] == "R3" for p in pending)
    assert any(s["sell_id"] == "R2" for s in in_settlement)
    assert not any(p["sell_id"] == "R2" for p in pending)
    assert not any(s["sell_id"] == "R3" for s in in_settlement)

    recon = sum(p["pendente"] for p in pending) + sum(s["pendente"] for s in in_settlement)
    assert abs(recon - cash_d1["cash_accounting"]) < 0.02


def test_build_operations_book_single_buy(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    _append(
        LedgerEvent(
            id="B1",
            type=EventType.BUY,
            exec_date=date(2026, 1, 2),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=10,
            price=10.0,
            amount=100.0,
            settle_date=date(2026, 1, 3),
        )
    )

    book = ledger.build_operations_book(date(2026, 1, 2))
    row = book["ABC"]
    assert row["qtd_liquida"] == 10
    assert abs(row["investido"] - 100.0) < 1e-9
    assert abs(row["custo_medio"] - 10.0) < 1e-9
    assert abs(row["realizado"]) < 1e-9
    assert len(row["compras"]) == 1
    assert row["vendas"] == []


def test_build_operations_book_partial_sell(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    _append(
        LedgerEvent(
            id="B2",
            type=EventType.BUY,
            exec_date=date(2026, 1, 2),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=10,
            price=10.0,
            amount=100.0,
            settle_date=date(2026, 1, 3),
        )
    )
    _append(
        LedgerEvent(
            id="S2",
            type=EventType.SELL,
            exec_date=date(2026, 1, 3),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=4,
            price=12.0,
            amount=48.0,
            settle_date=date(2026, 1, 4),
        )
    )

    book = ledger.build_operations_book(date(2026, 1, 3))
    row = book["ABC"]
    assert row["qtd_liquida"] == 6
    assert abs(row["investido"] - 60.0) < 1e-9
    assert abs(row["custo_medio"] - 10.0) < 1e-9
    assert abs(row["realizado"] - 8.0) < 1e-9
    assert len(row["compras"]) == 1
    assert len(row["vendas"]) == 1


def test_build_operations_book_fifo_multiple_buys(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    _append(
        LedgerEvent(
            id="B3A",
            type=EventType.BUY,
            exec_date=date(2026, 1, 2),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=5,
            price=10.0,
            amount=50.0,
            settle_date=date(2026, 1, 3),
        )
    )
    _append(
        LedgerEvent(
            id="B3B",
            type=EventType.BUY,
            exec_date=date(2026, 1, 3),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=5,
            price=12.0,
            amount=60.0,
            settle_date=date(2026, 1, 4),
        )
    )
    _append(
        LedgerEvent(
            id="S3",
            type=EventType.SELL,
            exec_date=date(2026, 1, 4),
            created_at=datetime.now(tz=UTC),
            ticker="ABC",
            qtd=8,
            price=15.0,
            amount=120.0,
            settle_date=date(2026, 1, 5),
        )
    )

    book = ledger.build_operations_book(date(2026, 1, 4))
    row = book["ABC"]
    assert row["qtd_liquida"] == 2
    assert abs(row["investido"] - 24.0) < 1e-9
    assert abs(row["custo_medio"] - 12.0) < 1e-9
    assert abs(row["realizado"] - 34.0) < 1e-9
    assert len(row["compras"]) == 2
    assert len(row["vendas"]) == 1


def test_qtd_from_invested_matches_broker_fractional_examples():
    mrvi_qtd = ledger.qtd_from_invested(1000.0, 6.9986)
    hpp_qtd = ledger.qtd_from_invested(1000.0, 15.9872)
    assert abs(mrvi_qtd - 142.88572) < 1e-4
    assert abs(hpp_qtd - 62.55004003) < 1e-4


def test_fractional_compute_positions_and_operations_book(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    _append(
        LedgerEvent(
            id="FB1",
            type=EventType.BUY,
            exec_date=date(2026, 7, 16),
            created_at=datetime.now(tz=UTC),
            ticker="MRVI",
            qtd=142.88572,
            price=6.9986,
            amount=1000.0,
            settle_date=date(2026, 7, 17),
        )
    )
    _append(
        LedgerEvent(
            id="FS1",
            type=EventType.SELL,
            exec_date=date(2026, 7, 17),
            created_at=datetime.now(tz=UTC),
            ticker="MRVI",
            qtd=20.12345,
            price=7.2,
            amount=144.88884,
            settle_date=date(2026, 7, 18),
        )
    )

    pos = ledger.compute_positions(date(2026, 7, 17))
    assert "MRVI" in pos
    qtd_restante = sum(float(l["qtd"]) for l in pos["MRVI"])
    assert abs(qtd_restante - (142.88572 - 20.12345)) < 1e-6

    book = ledger.build_operations_book(date(2026, 7, 17))
    row = book["MRVI"]
    assert abs(float(row["qtd_liquida"]) - (142.88572 - 20.12345)) < 1e-6
    assert abs(float(row["custo_medio"]) - 6.9986) < 1e-4


def test_is_duplicate_with_fractional_qty_epsilon(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    base = LedgerEvent(
        id="DQ1",
        type=EventType.BUY,
        exec_date=date(2026, 7, 16),
        created_at=datetime.now(tz=UTC),
        ticker="HPP",
        qtd=62.55004003,
        price=15.9872,
        amount=1000.0,
        settle_date=date(2026, 7, 17),
    )
    _append(base)

    within_eps = LedgerEvent(
        id="DQ2",
        type=EventType.BUY,
        exec_date=date(2026, 7, 16),
        created_at=datetime.now(tz=UTC),
        ticker="HPP",
        qtd=62.55004003 + 5e-7,
        price=15.9872,
        amount=1000.0,
        settle_date=date(2026, 7, 17),
    )
    assert ledger.is_duplicate(within_eps) is True

    outside_eps = LedgerEvent(
        id="DQ3",
        type=EventType.BUY,
        exec_date=date(2026, 7, 16),
        created_at=datetime.now(tz=UTC),
        ticker="HPP",
        qtd=62.55004003 + 2e-5,
        price=15.9872,
        amount=1000.0,
        settle_date=date(2026, 7, 17),
    )
    assert ledger.is_duplicate(outside_eps) is False

