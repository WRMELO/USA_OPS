from __future__ import annotations

import hashlib
import importlib.util
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from lib.eodhd_source_us import OUTPUT_COLUMNS


def _load_step_module():
    module_path = Path(__file__).resolve().parents[1] / "pipeline" / "02_ingest_prices_us.py"
    spec = importlib.util.spec_from_file_location("step02_eodhd_test_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _raw_schema_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-07-29",
                "ticker": "AAA",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 1000,
                "dividend_rate": 0.0,
                "split_from": pd.NA,
                "split_to": pd.NA,
                "source": "legacy",
                "ingested_at": "2026-07-29T22:00:00+00:00",
            }
        ]
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_merge_dedup_keep_last_and_schema(tmp_path, monkeypatch):
    mod = _load_step_module()
    raw_path = tmp_path / "us_market_data_raw.parquet"
    eodhd_base_path = tmp_path / "eodhd_raw_us.parquet"
    _raw_schema_frame().to_parquet(raw_path, index=False)
    pd.DataFrame([{"date": "2026-07-29"}]).to_parquet(eodhd_base_path, index=False)

    monkeypatch.setattr(mod, "RAW_PATH", raw_path)
    monkeypatch.setattr(mod, "EODHD_BASE_PATH", eodhd_base_path)
    monkeypatch.setattr(mod, "prev_session", lambda _target, exchange="XNYS": date(2026, 7, 29))

    def fake_loader(*, tickers, ticker_last_dates, end_date):
        assert tickers == ["AAA"]
        assert ticker_last_dates["AAA"] == date(2026, 7, 29)
        assert end_date == date(2026, 7, 30)
        return pd.DataFrame(
            [
                {
                    "date": "2026-07-30",
                    "ticker": "AAA",
                    "open": 11.0,
                    "high": 12.0,
                    "low": 10.5,
                    "close": 11.4,
                    "volume": 1100,
                    "dividend_rate": 0.0,
                    "split_from": pd.NA,
                    "split_to": pd.NA,
                    "source": "eodhd_local_base_v1",
                    "ingested_at": "2026-07-30T10:00:00+00:00",
                },
                {
                    "date": "2026-07-30",
                    "ticker": "AAA",
                    "open": 11.0,
                    "high": 12.0,
                    "low": 10.5,
                    "close": 11.8,
                    "volume": 1200,
                    "dividend_rate": 0.0,
                    "split_from": pd.NA,
                    "split_to": pd.NA,
                    "source": "eodhd_local_base_v1",
                    "ingested_at": "2026-07-30T10:05:00+00:00",
                },
            ]
        )

    monkeypatch.setattr(mod, "load_incremental_rows_from_eodhd", fake_loader)
    result = mod.run(end_date=date(2026, 7, 30))

    assert result["status"] == "ok"
    assert result["added_rows"] == 2
    out = pd.read_parquet(raw_path)
    assert list(out.columns) == OUTPUT_COLUMNS
    assert len(out) == 2
    row = out[(pd.to_datetime(out["date"]) == pd.Timestamp("2026-07-30")) & (out["ticker"] == "AAA")]
    assert len(row) == 1
    assert abs(float(row.iloc[0]["close"]) - 11.8) < 1e-12


def test_freshness_guard_raises_runtime_error(tmp_path, monkeypatch):
    mod = _load_step_module()
    raw_path = tmp_path / "us_market_data_raw.parquet"
    eodhd_base_path = tmp_path / "eodhd_raw_us.parquet"
    _raw_schema_frame().to_parquet(raw_path, index=False)
    pd.DataFrame([{"date": "2026-07-28"}]).to_parquet(eodhd_base_path, index=False)

    monkeypatch.setattr(mod, "RAW_PATH", raw_path)
    monkeypatch.setattr(mod, "EODHD_BASE_PATH", eodhd_base_path)
    monkeypatch.setattr(mod, "prev_session", lambda _target, exchange="XNYS": date(2026, 7, 29))

    with pytest.raises(RuntimeError, match="Base EODHD US defasada"):
        mod.run(end_date=date(2026, 7, 30))


def test_noop_does_not_rewrite_raw_file(tmp_path, monkeypatch):
    mod = _load_step_module()
    raw_path = tmp_path / "us_market_data_raw.parquet"
    eodhd_base_path = tmp_path / "eodhd_raw_us.parquet"
    _raw_schema_frame().to_parquet(raw_path, index=False)
    pd.DataFrame([{"date": "2026-07-29"}]).to_parquet(eodhd_base_path, index=False)

    monkeypatch.setattr(mod, "RAW_PATH", raw_path)
    monkeypatch.setattr(mod, "EODHD_BASE_PATH", eodhd_base_path)
    monkeypatch.setattr(mod, "prev_session", lambda _target, exchange="XNYS": date(2026, 7, 29))
    monkeypatch.setattr(
        mod,
        "load_incremental_rows_from_eodhd",
        lambda **kwargs: pd.DataFrame(columns=OUTPUT_COLUMNS),
    )

    before_hash = _sha256(raw_path)
    before_mtime = raw_path.stat().st_mtime_ns
    result = mod.run(end_date=date(2026, 7, 30))
    after_hash = _sha256(raw_path)
    after_mtime = raw_path.stat().st_mtime_ns

    assert result["status"] == "ok"
    assert result["added_rows"] == 0
    assert before_hash == after_hash
    assert before_mtime == after_mtime
