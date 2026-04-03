"""Step 05 — build macro expanded features with FRED fallback resilience."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
IN_OPERATIONAL_WINDOW = ROOT / "data" / "ssot" / "operational_window.parquet"
OUT_MACRO = ROOT / "data" / "ssot" / "macro_us.parquet"
OUT_FEATURES = ROOT / "data" / "features" / "macro_features_us.parquet"

BASE_SERIES = [
    "vix_close",
    "usd_index_broad",
    "ust_10y_yield",
    "ust_2y_yield",
    "fed_funds_rate",
    "hy_oas",
    "ig_oas",
]

SERIES_IDS = {
    "vix_close": "VIXCLS",
    "usd_index_broad": "DTWEXBGS",
    "ust_10y_yield": "DGS10",
    "ust_2y_yield": "DGS2",
    "fed_funds_rate": "DFF",
    "hy_oas": "BAMLH0A0HYM2",
    "ig_oas": "BAMLC0A0CM",
}


def _load_calendar(end_date: date | None) -> pd.DataFrame:
    if not IN_OPERATIONAL_WINDOW.exists():
        raise RuntimeError(f"Missing operational window: {IN_OPERATIONAL_WINDOW}")
    cal = pd.read_parquet(IN_OPERATIONAL_WINDOW, columns=["date"]).copy()
    cal["date"] = pd.to_datetime(cal["date"], errors="coerce").dt.normalize()
    cal = cal.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last").sort_values("date")
    if end_date is not None:
        cal = cal[cal["date"] <= pd.Timestamp(end_date)].copy()
    cal = cal.reset_index(drop=True)
    if cal.empty:
        raise RuntimeError("Operational calendar is empty after end_date filter.")
    return cal


def _load_existing_macro(end_date: date | None) -> pd.DataFrame:
    if not OUT_MACRO.exists():
        return pd.DataFrame(columns=["date"] + BASE_SERIES)
    existing = pd.read_parquet(OUT_MACRO).copy()
    if "date" not in existing.columns:
        return pd.DataFrame(columns=["date"] + BASE_SERIES)
    existing["date"] = pd.to_datetime(existing["date"], errors="coerce").dt.normalize()
    for col in BASE_SERIES:
        if col not in existing.columns:
            existing[col] = pd.NA
        existing[col] = pd.to_numeric(existing[col], errors="coerce")
    existing = existing[["date"] + BASE_SERIES]
    existing = existing.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last").sort_values("date")
    if end_date is not None:
        existing = existing[existing["date"] <= pd.Timestamp(end_date)].copy()
    return existing.reset_index(drop=True)


def _build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    out = pd.DataFrame({"date": df["date"]})
    feature_cols: list[str] = []
    for alias in BASE_SERIES:
        level_col = f"feature_{alias}_level"
        diff_col = f"feature_{alias}_diff_1d"
        pct_col = f"feature_{alias}_pct_1d"
        values = pd.to_numeric(df[alias], errors="coerce")
        out[level_col] = values
        out[diff_col] = values.diff(1)
        out[pct_col] = values.pct_change(1)
        feature_cols.extend([level_col, diff_col, pct_col])
    out[feature_cols] = out[feature_cols].shift(1)
    cutoff = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize("UTC")
    out["feature_timestamp_cutoff"] = (
        cutoff - pd.Timedelta(days=1) + pd.Timedelta(hours=23, minutes=59, seconds=59)
    )
    return out, feature_cols


def run(end_date: date | None = None) -> dict[str, Any]:
    from lib.adapters import FredAdapter

    calendar = _load_calendar(end_date=end_date)
    existing = _load_existing_macro(end_date=end_date)

    fred = FredAdapter(timeout_seconds=10.0, max_retries=2)
    fetched_by_alias: dict[str, pd.DataFrame] = {}
    fred_status: dict[str, str] = {}
    fred_errors: dict[str, str] = {}

    for alias in BASE_SERIES:
        series_id = SERIES_IDS[alias]
        try:
            series_df = fred.fetch_series(series_id, alias)
            if series_df.empty:
                fred_status[alias] = "fallback"
            else:
                series_df = series_df.copy()
                series_df["date"] = pd.to_datetime(series_df["date"], errors="coerce").dt.normalize()
                series_df[alias] = pd.to_numeric(series_df[alias], errors="coerce")
                series_df = (
                    series_df.dropna(subset=["date"])
                    .drop_duplicates(subset=["date"], keep="last")
                    .sort_values("date")
                    .reset_index(drop=True)
                )
                fetched_by_alias[alias] = series_df[["date", alias]]
                fred_status[alias] = "fresh"
        except Exception as exc:  # noqa: BLE001
            fred_status[alias] = "fallback"
            fred_errors[alias] = f"{type(exc).__name__}: {exc}"

    fallback_aliases = [a for a in BASE_SERIES if fred_status.get(a) != "fresh"]
    if fallback_aliases and existing.empty:
        raise RuntimeError(
            "FRED unavailable for aliases without local fallback: "
            + ", ".join(sorted(fallback_aliases))
        )

    merged = calendar.copy()
    for alias in BASE_SERIES:
        if fred_status.get(alias) == "fresh":
            source = fetched_by_alias[alias]
        else:
            source = existing[["date", alias]].copy() if alias in existing.columns else pd.DataFrame(columns=["date", alias])
        merged = merged.merge(source, on="date", how="left")

    if not existing.empty:
        ex = existing[["date"] + BASE_SERIES].copy()
        ex = ex.rename(columns={c: f"{c}_existing" for c in BASE_SERIES})
        merged = merged.merge(ex, on="date", how="left")
        for alias in BASE_SERIES:
            merged[alias] = pd.to_numeric(merged[alias], errors="coerce").combine_first(
                pd.to_numeric(merged[f"{alias}_existing"], errors="coerce")
            )
            merged = merged.drop(columns=[f"{alias}_existing"])

    merged = merged.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    merged[BASE_SERIES] = merged[BASE_SERIES].apply(pd.to_numeric, errors="coerce")
    merged[BASE_SERIES] = merged[BASE_SERIES].ffill()

    macro_us = merged[["date"] + BASE_SERIES].copy()
    if macro_us.empty:
        raise RuntimeError("macro_us is empty after merge+ffill.")

    last_row = macro_us.iloc[-1]
    missing_last = [c for c in BASE_SERIES if pd.isna(last_row[c])]
    if missing_last:
        raise RuntimeError(f"macro_us last row has nulls after ffill: {missing_last}")

    features, feature_cols = _build_features(macro_us)

    target_end = end_date if end_date is not None else pd.to_datetime(calendar["date"]).max().date()
    min_acceptable = target_end - timedelta(days=2)
    date_max = pd.to_datetime(features["date"], errors="coerce").max().date()
    gate_d2 = date_max >= min_acceptable
    if not gate_d2:
        raise RuntimeError(
            f"Gate FAIL D-2: features date_max={date_max} < min_acceptable={min_acceptable} (end_date={target_end})"
        )

    OUT_MACRO.parent.mkdir(parents=True, exist_ok=True)
    OUT_FEATURES.parent.mkdir(parents=True, exist_ok=True)
    macro_us.to_parquet(OUT_MACRO, index=False)
    features.to_parquet(OUT_FEATURES, index=False)

    payload = {
        "status": "ok",
        "end_date": str(end_date) if end_date else None,
        "macro_features_path": str(OUT_FEATURES.relative_to(ROOT)),
        "macro_us_path": str(OUT_MACRO.relative_to(ROOT)),
        "fred_status": fred_status,
        "fred_errors": fred_errors,
        "rows": {"macro_us": len(macro_us), "features": len(features)},
        "coverage": {"date_min": str(macro_us["date"].min().date()), "date_max": str(date_max)},
        "gate_d2": gate_d2,
        "feature_columns_count": len(feature_cols),
    }
    print("[05] Macro expanded built with fallback resilience")
    print(json.dumps(payload, ensure_ascii=False))
    return payload


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", type=str, default=None)
    args = parser.parse_args()
    end = date.fromisoformat(args.end_date) if args.end_date else None
    run(end_date=end)
