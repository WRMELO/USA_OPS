from __future__ import annotations

from datetime import date

import pandas as pd

from lib.eodhd_source_us import OUTPUT_COLUMNS, load_incremental_rows_from_eodhd


def test_schema_dedup_dividend_agg_and_split_mapping(tmp_path, monkeypatch):
    base = pd.DataFrame(
        [
            {"ticker": "AAA", "date": "2026-07-28", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1000},
            {"ticker": "AAA", "date": "2026-07-28", "open": 10.1, "high": 11.2, "low": 9.1, "close": 10.9, "volume": 1100},
            {"ticker": "AAA", "date": "2026-07-29", "open": 11.0, "high": 12.0, "low": 10.0, "close": 11.5, "volume": 1200},
            {"ticker": "BBB", "date": "2026-07-29", "open": 20.0, "high": 21.0, "low": 19.0, "close": 20.5, "volume": 1300},
        ]
    )
    div = pd.DataFrame(
        [
            {"ticker": "AAA", "date": "2026-07-28", "dividend_rate": 0.1, "dividend_label": "Q"},
            {"ticker": "AAA", "date": "2026-07-28", "dividend_rate": 0.2, "dividend_label": "Q"},
            {"ticker": "BBB", "date": "2026-07-29", "dividend_rate": 0.0, "dividend_label": "N"},
        ]
    )
    splits = pd.DataFrame(
        [
            {"ticker": "AAA", "date": "2026-07-29", "split_old": 2.0, "split_new": 1.0},
            {"ticker": "BBB", "date": "2026-07-29", "split_old": 3.0, "split_new": 2.0},
        ]
    )

    base_path = tmp_path / "raw.parquet"
    div_path = tmp_path / "div.parquet"
    split_path = tmp_path / "splits.parquet"
    base.to_parquet(base_path, index=False)
    div.to_parquet(div_path, index=False)
    splits.to_parquet(split_path, index=False)

    monkeypatch.setenv("EODHD_BASE_US_PATH", str(base_path))
    monkeypatch.setenv("EODHD_DIV_US_PATH", str(div_path))
    monkeypatch.setenv("EODHD_SPLITS_US_PATH", str(split_path))

    out = load_incremental_rows_from_eodhd(
        tickers=["AAA", "BBB"],
        ticker_last_dates={},
        end_date=date(2026, 7, 29),
    )

    assert list(out.columns) == OUTPUT_COLUMNS
    assert len(out) == 3

    aaa_0728 = out[(out["ticker"] == "AAA") & (out["date"] == pd.Timestamp("2026-07-28"))]
    assert len(aaa_0728) == 1
    assert float(aaa_0728.iloc[0]["close"]) == 10.9
    assert abs(float(aaa_0728.iloc[0]["dividend_rate"]) - 0.3) < 1e-12

    aaa_0729 = out[(out["ticker"] == "AAA") & (out["date"] == pd.Timestamp("2026-07-29"))].iloc[0]
    assert float(aaa_0729["split_from"]) == 2.0
    assert float(aaa_0729["split_to"]) == 1.0
    assert aaa_0729["source"] == "eodhd_local_base_v1"
    assert "T" in str(aaa_0729["ingested_at"])


def test_tail_only_and_session_filter(tmp_path, monkeypatch):
    base = pd.DataFrame(
        [
            {"ticker": "AAA", "date": "2026-07-26", "open": 9.0, "high": 9.0, "low": 9.0, "close": 9.0, "volume": 900},
            {"ticker": "AAA", "date": "2026-07-27", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1000},
            {"ticker": "AAA", "date": "2026-07-28", "open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0, "volume": 1100},
            {"ticker": "BBB", "date": "2026-07-28", "open": 12.0, "high": 12.0, "low": 12.0, "close": 12.0, "volume": 1200},
        ]
    )

    base_path = tmp_path / "raw.parquet"
    base.to_parquet(base_path, index=False)

    monkeypatch.setenv("EODHD_BASE_US_PATH", str(base_path))
    monkeypatch.setenv("EODHD_DIV_US_PATH", str(tmp_path / "missing_div.parquet"))
    monkeypatch.setenv("EODHD_SPLITS_US_PATH", str(tmp_path / "missing_splits.parquet"))

    out = load_incremental_rows_from_eodhd(
        tickers=["AAA"],
        ticker_last_dates={"AAA": date(2026, 7, 27)},
        end_date=date(2026, 7, 28),
    )

    assert len(out) == 1
    row = out.iloc[0]
    assert row["ticker"] == "AAA"
    assert row["date"] == pd.Timestamp("2026-07-28")
    assert float(row["close"]) == 11.0
