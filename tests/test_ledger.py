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

