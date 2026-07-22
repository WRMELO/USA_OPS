from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

import pipeline.ledger as ledger
from pipeline.ledger import EventType, LedgerEvent


def _append(ev: LedgerEvent) -> None:
    ledger.append_event(ev)


def _write_real_boletim(
    base_dir: Path,
    exec_day: date,
    *,
    market_day: date | None = None,
    snapshot: list[dict[str, float]] | None = None,
    cash_free: float = 0.0,
    cash_accounting: float = 0.0,
) -> None:
    payload = {
        "exec_day": exec_day.isoformat(),
        "market_day": (market_day or exec_day).isoformat(),
        "positions_snapshot": snapshot or [],
        "cash_free": float(cash_free),
        "cash_accounting": float(cash_accounting),
    }
    (base_dir / f"{exec_day.isoformat()}.json").write_text(
        json.dumps(payload, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _write_price_window(path: Path, rows: list[tuple[date, str, float]]) -> None:
    df = pd.DataFrame(
        [{"date": d.isoformat(), "ticker": t, "close_operational": px} for d, t, px in rows]
    )
    df.to_parquet(path, index=False)


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


def test_compute_cash_honors_settlement_even_with_future_settle_date(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"
    day = date(2026, 1, 3)

    _append(
        LedgerEvent(
            id="AP2",
            type=EventType.APORTE,
            exec_date=day,
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _append(
        LedgerEvent(
            id="S2",
            type=EventType.SELL,
            exec_date=day,
            created_at=datetime.now(tz=UTC),
            ticker="PENG",
            qtd=10.0,
            price=20.0,
            amount=200.0,
            settle_date=date(2026, 1, 4),
        )
    )
    _append(
        LedgerEvent(
            id="T2",
            type=EventType.SETTLEMENT,
            exec_date=day,
            created_at=datetime.now(tz=UTC),
            amount=200.0,
            ref_id="S2",
            settle_date=day,
            reason="liquidacao=JA_NO_CAIXA",
        )
    )

    cash = ledger.compute_cash(day)
    assert abs(cash["cash_free"] - 1200.0) < 1e-9
    assert abs(cash["cash_accounting"]) < 1e-9


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


def test_total_fees_and_capital_em_uso(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"

    _append(
        LedgerEvent(
            id="CF1",
            type=EventType.APORTE,
            exec_date=date(2026, 7, 16),
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _append(
        LedgerEvent(
            id="CF2",
            type=EventType.RETIRADA,
            exec_date=date(2026, 7, 17),
            created_at=datetime.now(tz=UTC),
            amount=200.0,
        )
    )
    _append(
        LedgerEvent(
            id="CF3",
            type=EventType.FEE,
            exec_date=date(2026, 7, 16),
            created_at=datetime.now(tz=UTC),
            amount=2.5,
        )
    )
    _append(
        LedgerEvent(
            id="CF4",
            type=EventType.FEE,
            exec_date=date(2026, 7, 18),
            created_at=datetime.now(tz=UTC),
            amount=1.0,
        )
    )

    assert abs(ledger.total_fees(date(2026, 7, 17)) - 2.5) < 1e-9
    assert abs(ledger.total_fees(date(2026, 7, 18)) - 3.5) < 1e-9
    assert abs(ledger.capital_em_uso(date(2026, 7, 17)) - 800.0) < 1e-9


def test_build_real_base1_series_stable_prices(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"
    window = tmp_path / "opw.parquet"
    day1 = date(2026, 7, 16)
    day2 = date(2026, 7, 17)

    _append(
        LedgerEvent(
            id="B1A",
            type=EventType.APORTE,
            exec_date=day1,
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _write_real_boletim(
        tmp_path,
        day1,
        snapshot=[{"ticker": "AAA", "qtd": 10.0, "preco_compra": 100.0}],
    )
    _write_real_boletim(
        tmp_path,
        day2,
        snapshot=[{"ticker": "AAA", "qtd": 10.0, "preco_compra": 100.0}],
    )
    _write_price_window(window, [(day1, "AAA", 100.0), (day2, "AAA", 100.0)])

    series = ledger.build_real_base1_series(day2, price_window_path=window)
    assert len(series) == 2
    assert abs(series[0]["base1"] - 1.0) < 1e-9
    assert abs(series[1]["base1"] - 1.0) < 1e-9
    assert abs(series[1]["daily_var_pct"]) < 1e-9


def test_build_real_base1_series_price_gain_without_new_flow(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"
    window = tmp_path / "opw.parquet"
    day1 = date(2026, 7, 16)
    day2 = date(2026, 7, 17)

    _append(
        LedgerEvent(
            id="B2A",
            type=EventType.APORTE,
            exec_date=day1,
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _write_real_boletim(
        tmp_path,
        day1,
        snapshot=[{"ticker": "AAA", "qtd": 10.0, "preco_compra": 100.0}],
    )
    _write_real_boletim(
        tmp_path,
        day2,
        snapshot=[{"ticker": "AAA", "qtd": 10.0, "preco_compra": 100.0}],
    )
    _write_price_window(window, [(day1, "AAA", 100.0), (day2, "AAA", 110.0)])

    series = ledger.build_real_base1_series(day2, price_window_path=window)
    assert len(series) == 2
    assert abs(series[1]["base1"] - 1.1) < 1e-6
    assert abs(series[1]["daily_var_pct"] - 10.0) < 1e-6


def test_build_real_base1_series_second_aporte_preserves_gain(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"
    window = tmp_path / "opw.parquet"
    day1 = date(2026, 7, 16)
    day2 = date(2026, 7, 17)
    day3 = date(2026, 7, 18)

    _append(
        LedgerEvent(
            id="B3A",
            type=EventType.APORTE,
            exec_date=day1,
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _append(
        LedgerEvent(
            id="B3B",
            type=EventType.APORTE,
            exec_date=day3,
            created_at=datetime.now(tz=UTC),
            amount=500.0,
        )
    )
    _write_real_boletim(
        tmp_path,
        day1,
        snapshot=[{"ticker": "AAA", "qtd": 10.0, "preco_compra": 100.0}],
    )
    _write_real_boletim(
        tmp_path,
        day2,
        snapshot=[{"ticker": "AAA", "qtd": 10.0, "preco_compra": 100.0}],
    )
    _write_real_boletim(
        tmp_path,
        day3,
        snapshot=[{"ticker": "AAA", "qtd": 14.54545455, "preco_compra": 110.0}],
    )
    _write_price_window(
        window,
        [
            (day1, "AAA", 100.0),
            (day2, "AAA", 110.0),
            (day3, "AAA", 110.0),
        ],
    )

    series = ledger.build_real_base1_series(day3, price_window_path=window)
    assert len(series) == 3
    assert abs(series[1]["base1"] - 1.1) < 1e-6
    assert abs(series[2]["base1"] - series[1]["base1"]) < 1e-6
    assert abs(series[2]["nav"] - 1600.0) < 1e-3


def test_build_real_base1_series_keeps_fractional_qty(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"
    window = tmp_path / "opw.parquet"
    day1 = date(2026, 7, 16)

    _append(
        LedgerEvent(
            id="B4A",
            type=EventType.APORTE,
            exec_date=day1,
            created_at=datetime.now(tz=UTC),
            amount=100.0,
        )
    )
    _write_real_boletim(
        tmp_path,
        day1,
        snapshot=[{"ticker": "AAA", "qtd": 0.5, "preco_compra": 200.0}],
    )
    _write_price_window(window, [(day1, "AAA", 200.0)])

    series = ledger.build_real_base1_series(day1, price_window_path=window)
    assert len(series) == 1
    assert abs(series[0]["nav"] - 100.0) < 1e-9
    assert abs(series[0]["base1"] - 1.0) < 1e-9


def test_build_real_base1_series_appends_live_point(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"
    window = tmp_path / "opw.parquet"
    day1 = date(2026, 7, 16)
    day2 = date(2026, 7, 17)

    _append(
        LedgerEvent(
            id="B5A",
            type=EventType.APORTE,
            exec_date=day1,
            created_at=datetime.now(tz=UTC),
            amount=1000.0,
        )
    )
    _write_real_boletim(
        tmp_path,
        day1,
        snapshot=[{"ticker": "AAA", "qtd": 10.0, "preco_compra": 100.0}],
    )
    _write_price_window(window, [(day1, "AAA", 100.0), (day2, "AAA", 105.0)])

    series = ledger.build_real_base1_series(
        day2,
        live_snapshot=[{"ticker": "AAA", "qtd": 10.0, "preco_compra": 100.0}],
        live_cash_free=0.0,
        live_cash_accounting=0.0,
        price_window_path=window,
    )
    assert len(series) == 2
    assert series[-1]["date"] == day2.isoformat()
    assert abs(series[-1]["base1"] - 1.05) < 1e-6


def test_recon_adjust_updates_positions_without_cash_impact(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"
    day = date(2026, 7, 21)

    _append(
        LedgerEvent(
            id="AP1",
            type=EventType.APORTE,
            exec_date=day,
            created_at=datetime.now(tz=UTC),
            amount=1200.0,
        )
    )
    _append(
        LedgerEvent(
            id="BUY1",
            type=EventType.BUY,
            exec_date=day,
            created_at=datetime.now(tz=UTC),
            ticker="AAA",
            qtd=10.0,
            price=100.0,
            amount=1000.0,
            settle_date=day,
        )
    )
    _append(
        LedgerEvent(
            id="FEE1",
            type=EventType.FEE,
            exec_date=day,
            created_at=datetime.now(tz=UTC),
            ticker="AAA",
            amount=2.5,
            ref_id="BUY1",
        )
    )

    cash_before = ledger.compute_cash(day)

    _append(
        LedgerEvent(
            id="ADJ1",
            type=EventType.RECON_ADJUST,
            exec_date=day,
            created_at=datetime.now(tz=UTC),
            ticker="AAA",
            qtd=12.5,
            price=80.0,
            amount=0.0,
            ref_id="BUY1",
            reason="ajuste de reconciliacao",
        )
    )

    pos = ledger.compute_positions(day)
    assert "AAA" in pos
    assert len(pos["AAA"]) == 1
    assert abs(float(pos["AAA"][0]["qtd"]) - 12.5) < 1e-6
    assert abs(float(pos["AAA"][0]["buy_price"]) - 80.0) < 1e-6

    book = ledger.build_operations_book(day)
    row = book["AAA"]
    assert abs(float(row["qtd_liquida"]) - 12.5) < 1e-6
    assert abs(float(row["custo_medio"]) - 80.0) < 1e-6
    assert abs(float(row["investido"]) - 1000.0) < 0.01

    cash_after = ledger.compute_cash(day)
    assert abs(float(cash_before["cash_free"]) - float(cash_after["cash_free"])) < 1e-9
    assert abs(float(cash_before["cash_accounting"]) - float(cash_after["cash_accounting"])) < 1e-9
    assert abs(ledger.total_fees(day) - 2.5) < 1e-9


def test_create_event_recon_adjust_forces_zero_amount(tmp_path):
    ledger.LEDGER_PATH = tmp_path / "ledger.jsonl"
    day = date(2026, 7, 21)

    ev = ledger.create_event(
        EventType.RECON_ADJUST,
        day,
        999.99,
        ticker="AAA",
        qtd=1.0,
        price=10.0,
        ref_id="BUY1",
        reason="teste",
    )
    assert abs(float(ev.amount)) < 1e-12

    _append(ev)
    saved = ledger.read_all_events()[-1]
    assert saved.type == EventType.RECON_ADJUST
    assert abs(float(saved.amount)) < 1e-12

