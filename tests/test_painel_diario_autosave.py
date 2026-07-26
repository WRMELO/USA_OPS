from __future__ import annotations

import json
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


def test_compute_dryrun_autosave_operations_returns_hold_on_non_rebalance(tmp_path, monkeypatch):
    monkeypatch.setattr(painel_diario, "ROOT", tmp_path)
    def _fake_read_json(path):
        if str(path).endswith("decision_2026-07-16.json"):
            return {"is_rebalance_day": False, "portfolio": []}
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return {}

    monkeypatch.setattr(painel_diario, "_read_json", _fake_read_json)
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
                "pending_sales": [],
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
    assert out["cash_transfers"] == []


def test_compute_dryrun_autosave_operations_rebalance_day_builds_sell_and_freezes_plan(tmp_path, monkeypatch):
    monkeypatch.setattr(painel_diario, "ROOT", tmp_path)
    decision = {
        "is_rebalance_day": True,
        "selected_tickers": ["AAA", "BBB"],
        "target_weights": {"AAA": 0.6, "BBB": 0.4},
        "operational_ranking": [{"ticker": "AAA"}, {"ticker": "BBB"}],
        "portfolio": [{"ticker": "AAA"}, {"ticker": "BBB"}],
    }
    def _fake_read_json(path):
        if str(path).endswith("decision_2026-07-16.json"):
            return decision
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return {}

    monkeypatch.setattr(painel_diario, "_read_json", _fake_read_json)
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
                "pending_sales": [],
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

    out = painel_diario.compute_dryrun_autosave_operations(date(2026, 7, 16))
    assert out["operations"] == [
        {"type": "VENDA", "ticker": "OLD", "qtd": 10, "preco": 10.0},
    ]
    assert out["cash_transfers"] == []

    plan_path = tmp_path / "data" / "daily" / "pending_rebalance_buy.json"
    assert plan_path.exists()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan["source_decision_exec_day"] == "2026-07-16"
    assert plan["source_market_day"] == "2026-07-15"
    assert plan["selected_tickers"] == ["AAA", "BBB"]
    assert plan["target_weights"] == {"AAA": 0.6, "BBB": 0.4}


def test_compute_dryrun_autosave_operations_executes_deferred_buy_with_transfers(tmp_path, monkeypatch):
    monkeypatch.setattr(painel_diario, "ROOT", tmp_path)
    plan_dir = tmp_path / "data" / "daily"
    plan_dir.mkdir(parents=True, exist_ok=True)
    plan_path = plan_dir / "pending_rebalance_buy.json"
    plan_payload = {
        "source_decision_exec_day": "2026-07-16",
        "source_market_day": "2026-07-15",
        "selected_tickers": ["AAA", "BBB"],
        "target_weights": {"AAA": 0.6, "BBB": 0.4},
        "operational_ranking": [{"ticker": "AAA"}, {"ticker": "BBB"}],
        "total_ativo_ref": 1000.0,
        "created_at": "2026-07-16T12:00:00+00:00",
    }
    plan_path.write_text(json.dumps(plan_payload), encoding="utf-8")

    def _fake_read_json(path):
        if str(path).endswith("decision_2026-07-17.json"):
            return {"is_rebalance_day": False, "portfolio": []}
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return {}

    monkeypatch.setattr(painel_diario, "_read_json", _fake_read_json)
    monkeypatch.setattr(
        painel_diario,
        "_build_tables_and_cards",
        lambda _exec_day: (
            "",
            {
                "prices_d1": {"AAA": 10.0, "BBB": 20.0},
                "holdings_qty": {"AAA": 0, "BBB": 0},
                "cash_free": 10.0,
                "cash_acc": 0.0,
                "total_ativo": 1000.0,
                "pending_sales": [{"ref": "SELL-1", "pendente": 50.4}],
            },
            [],
        ),
    )
    monkeypatch.setattr(painel_diario, "get_d_minus_1", lambda _day: date(2026, 7, 16))
    monkeypatch.setattr(painel_diario, "_resolve_trade_day", lambda _day: date(2026, 7, 17))
    monkeypatch.setattr(painel_diario, "get_latest_prices", lambda _tickers, as_of_day: {"AAA": 10.0, "BBB": 20.0})

    buy_calls: list[float] = []

    def _fake_buys(*, decision, cash_available, **kwargs):
        buy_calls.append(cash_available)
        assert decision["selected_tickers"] == ["AAA", "BBB"]
        if cash_available >= painel_diario._UNLIMITED_CASH_PROBE:
            return []
        return [{"ticker": "AAA", "qty_buy": 2, "close_d1": 10.0}]

    monkeypatch.setattr(painel_diario, "_build_rebalance_buy_suggestions", _fake_buys)

    out = painel_diario.compute_dryrun_autosave_operations(date(2026, 7, 17))
    assert out["cash_transfers"] == [{"value": 50.4, "note": "SELL-1"}]
    assert out["operations"] == [{"type": "COMPRA", "ticker": "AAA", "qtd": 2, "preco": 10.0}]
    assert buy_calls[0] == 60.4
    assert buy_calls[-1] == painel_diario._UNLIMITED_CASH_PROBE
    assert not plan_path.exists()


def test_compute_dryrun_autosave_operations_generates_cash_transfer_only(tmp_path, monkeypatch):
    monkeypatch.setattr(painel_diario, "ROOT", tmp_path)
    def _fake_read_json(path):
        if str(path).endswith("decision_2026-07-17.json"):
            return {"is_rebalance_day": False, "portfolio": []}
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return {}

    monkeypatch.setattr(painel_diario, "_read_json", _fake_read_json)
    monkeypatch.setattr(
        painel_diario,
        "_build_tables_and_cards",
        lambda _exec_day: (
            "",
            {
                "prices_d1": {},
                "holdings_qty": {},
                "cash_free": 0.0,
                "cash_acc": 0.0,
                "total_ativo": 0.0,
                "pending_sales": [
                    {"ref": "SELL-OK", "pendente": 100.0},
                    {"ref": "SELL-SMALL", "pendente": 0.1},
                ],
            },
            [],
        ),
    )
    monkeypatch.setattr(painel_diario, "get_d_minus_1", lambda _day: date(2026, 7, 16))
    monkeypatch.setattr(painel_diario, "_resolve_trade_day", lambda _day: date(2026, 7, 17))
    monkeypatch.setattr(painel_diario, "get_latest_prices", lambda _tickers, as_of_day: {})

    out = painel_diario.compute_dryrun_autosave_operations(date(2026, 7, 17))
    assert out["operations"] == []
    assert out["cash_transfers"] == [{"value": 100.0, "note": "SELL-OK"}]


def test_compute_dryrun_autosave_operations_blocks_overlap_rebalance(tmp_path, monkeypatch):
    monkeypatch.setattr(painel_diario, "ROOT", tmp_path)
    plan_dir = tmp_path / "data" / "daily"
    plan_dir.mkdir(parents=True, exist_ok=True)
    (plan_dir / "pending_rebalance_buy.json").write_text(
        json.dumps({"source_decision_exec_day": "2026-07-16", "selected_tickers": ["AAA"], "target_weights": {"AAA": 1.0}}),
        encoding="utf-8",
    )
    def _fake_read_json(path):
        if str(path).endswith("decision_2026-07-18.json"):
            return {"is_rebalance_day": True, "selected_tickers": ["AAA"], "target_weights": {"AAA": 1.0}}
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return {}

    monkeypatch.setattr(painel_diario, "_read_json", _fake_read_json)
    monkeypatch.setattr(
        painel_diario,
        "_build_tables_and_cards",
        lambda _exec_day: ("", {"prices_d1": {}, "holdings_qty": {}, "cash_free": 0.0, "cash_acc": 0.0, "total_ativo": 0.0, "pending_sales": []}, []),
    )
    monkeypatch.setattr(painel_diario, "get_d_minus_1", lambda _day: date(2026, 7, 17))
    monkeypatch.setattr(painel_diario, "_resolve_trade_day", lambda _day: date(2026, 7, 18))
    monkeypatch.setattr(painel_diario, "get_latest_prices", lambda _tickers, as_of_day: {})

    try:
        painel_diario.compute_dryrun_autosave_operations(date(2026, 7, 18))
    except RuntimeError as exc:
        assert "plano de compra pendente" in str(exc)
    else:
        raise AssertionError("Era esperado RuntimeError por sobreposicao de rebalance.")
