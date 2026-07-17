from __future__ import annotations

import json
from datetime import date

from pipeline import analise_us
from pipeline import run_daily


def test_refresh_contexto_analista_us_writes_latest_eligible_market_day(tmp_path, monkeypatch):
    monkeypatch.setattr(run_daily, "ROOT", tmp_path)
    monkeypatch.setattr(
        analise_us,
        "_load_trading_days_us",
        lambda: [date(2026, 7, 14), date(2026, 7, 15), date(2026, 7, 16)],
    )

    called: dict[str, date] = {}

    def _fake_build_context(market_day: date):
        called["market_day"] = market_day
        return {
            "market_day": market_day.isoformat(),
            "holdings": [{"ticker": "MRVI"}],
        }

    monkeypatch.setattr(analise_us, "build_context", _fake_build_context)

    ctx = run_daily.refresh_contexto_analista_us(date(2026, 7, 15))
    assert ctx is not None
    assert called["market_day"] == date(2026, 7, 15)

    out_path = tmp_path / "data" / "ssot" / "contexto_analista_us.json"
    assert out_path.exists()
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["market_day"] == "2026-07-15"
    assert payload["holdings"][0]["ticker"] == "MRVI"


def test_refresh_contexto_analista_us_returns_none_when_no_eligible_day(tmp_path, monkeypatch):
    monkeypatch.setattr(run_daily, "ROOT", tmp_path)
    monkeypatch.setattr(analise_us, "_load_trading_days_us", lambda: [date(2026, 7, 16)])

    def _unexpected_build_context(_: date):
        raise AssertionError("build_context nao deveria ser chamado sem dia elegivel")

    monkeypatch.setattr(analise_us, "build_context", _unexpected_build_context)

    result = run_daily.refresh_contexto_analista_us(date(2026, 7, 15))
    assert result is None
    assert not (tmp_path / "data" / "ssot" / "contexto_analista_us.json").exists()
