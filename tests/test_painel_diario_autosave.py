from __future__ import annotations

from datetime import date

import pipeline.painel_diario as painel_diario


def test_build_rebalance_buy_suggestions_respects_rank_and_cash(monkeypatch):
    monkeypatch.setattr(painel_diario, "_read_json", lambda _path: {"winner_config_snapshot": {"top_n": 20}})
    decision = {
        "is_rebalance_day": True,
        "selected_tickers": ["AAA", "BBB", "CCC"],
        "target_weights": {"AAA": 0.5, "BBB": 0.3, "CCC": 0.2},
        "operational_ranking": [{"ticker": "AAA"}, {"ticker": "BBB"}, {"ticker": "CCC"}],
    }
    prices = {"AAA": 10.0, "BBB": 20.0, "CCC": 5.0}

    buys_full = painel_diario._build_rebalance_buy_suggestions(
        decision=decision,
        holdings_qty={},
        prices_d1=prices,
        total_ativo=1000.0,
        cash_available=1000.0,
    )
    assert [(b["ticker"], b["qty_buy"]) for b in buys_full] == [("AAA", 50), ("BBB", 15), ("CCC", 40)]

    buys_partial = painel_diario._build_rebalance_buy_suggestions(
        decision=decision,
        holdings_qty={},
        prices_d1=prices,
        total_ativo=1000.0,
        cash_available=560.0,
    )
    assert [(b["ticker"], b["qty_buy"]) for b in buys_partial] == [("AAA", 50), ("BBB", 3)]

    buys_missing_price = painel_diario._build_rebalance_buy_suggestions(
        decision=decision,
        holdings_qty={},
        prices_d1={"AAA": 10.0, "BBB": 0.0, "CCC": 5.0},
        total_ativo=1000.0,
        cash_available=1000.0,
    )
    assert all(b["ticker"] != "BBB" for b in buys_missing_price)


def test_compute_dryrun_autosave_operations_returns_hold_on_non_rebalance(monkeypatch):
    monkeypatch.setattr(painel_diario, "_read_json", lambda _path: {"is_rebalance_day": False, "portfolio": []})
    monkeypatch.setattr(
        painel_diario,
        "_build_tables_and_cards",
        lambda _exec_day: (
            "",
            {
                "prices_d1": {},
                "holdings_qty": {},
                "cash_free": 100.0,
                "cash_acc": 0.0,
                "total_ativo": 100.0,
            },
            [],
        ),
    )
    monkeypatch.setattr(painel_diario, "get_d_minus_1", lambda _day: date(2026, 7, 15))
    monkeypatch.setattr(painel_diario, "_resolve_trade_day", lambda _day: date(2026, 7, 16))

    out = painel_diario.compute_dryrun_autosave_operations(date(2026, 7, 16))
    assert out["market_day"] == "2026-07-15"
    assert out["trade_day"] == "2026-07-16"
    assert out["operations"] == []


def test_compute_dryrun_autosave_operations_builds_sell_then_buy(monkeypatch):
    decision = {
        "is_rebalance_day": True,
        "selected_tickers": ["AAA", "BBB"],
        "target_weights": {"AAA": 0.6, "BBB": 0.4},
        "operational_ranking": [{"ticker": "AAA"}, {"ticker": "BBB"}],
        "portfolio": [{"ticker": "AAA"}, {"ticker": "BBB"}],
    }
    monkeypatch.setattr(painel_diario, "_read_json", lambda _path: decision)
    monkeypatch.setattr(
        painel_diario,
        "_build_tables_and_cards",
        lambda _exec_day: (
            "",
            {
                "prices_d1": {"OLD": 10.0},
                "holdings_qty": {"OLD": 10},
                "cash_free": 100.0,
                "cash_acc": 50.0,
                "total_ativo": 1000.0,
            },
            [],
        ),
    )
    monkeypatch.setattr(painel_diario, "get_d_minus_1", lambda _day: date(2026, 7, 15))
    monkeypatch.setattr(painel_diario, "_resolve_trade_day", lambda _day: date(2026, 7, 16))
    monkeypatch.setattr(painel_diario, "get_latest_prices", lambda _tickers, as_of_day: {"AAA": 10.0, "BBB": 20.0, "OLD": 10.0})
    monkeypatch.setattr(
        painel_diario,
        "_build_rebalance_sell_suggestions",
        lambda **kwargs: [{"ticker": "OLD", "qty_sell": 10, "close_d1": 10.0, "sell_pct": 100.0}],
    )

    def _fake_buys(*, holdings_qty, cash_available, **kwargs):
        assert holdings_qty["OLD"] == 0
        assert cash_available == 250.0
        return [
            {"ticker": "AAA", "qty_buy": 5, "close_d1": 10.0},
            {"ticker": "BBB", "qty_buy": 2, "close_d1": 20.0},
        ]

    monkeypatch.setattr(painel_diario, "_build_rebalance_buy_suggestions", _fake_buys)

    out = painel_diario.compute_dryrun_autosave_operations(date(2026, 7, 16))
    assert out["operations"] == [
        {"type": "VENDA", "ticker": "OLD", "qtd": 10, "preco": 10.0},
        {"type": "COMPRA", "ticker": "AAA", "qtd": 5, "preco": 10.0},
        {"type": "COMPRA", "ticker": "BBB", "qtd": 2, "preco": 20.0},
    ]
