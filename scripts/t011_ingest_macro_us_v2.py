#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


BASE_SERIES = [
    "vix_close",
    "usd_index_broad",
    "ust_10y_yield",
    "ust_2y_yield",
    "fed_funds_rate",
    "hy_oas",
    "ig_oas",
]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="T-011v2: macro US (outer merge -> ffill -> filter) + features shift(1)."
    )
    parser.add_argument("--workspace", required=True)
    parser.add_argument(
        "--in-trading-calendar",
        default="data/ssot/us_market_data_raw.parquet",
    )
    parser.add_argument(
        "--out-macro-ssot",
        default="data/ssot/macro_us.parquet",
    )
    parser.add_argument(
        "--out-macro-features",
        default="data/features/macro_features_us.parquet",
    )
    parser.add_argument(
        "--out-report",
        default="data/ssot/t011v2_macro_report.json",
    )
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--max-retries", type=int, default=5)
    return parser.parse_args()


def _build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    out = pd.DataFrame({"date": df["date"]})
    feature_cols: list[str] = []
    for alias in BASE_SERIES:
        level_col = f"feature_{alias}_level"
        diff_col = f"feature_{alias}_diff_1d"
        pct_col = f"feature_{alias}_pct_1d"

        out[level_col] = pd.to_numeric(df[alias], errors="coerce")
        out[diff_col] = pd.to_numeric(df[alias], errors="coerce").diff(1)
        out[pct_col] = pd.to_numeric(df[alias], errors="coerce").pct_change(1)
        feature_cols.extend([level_col, diff_col, pct_col])

    out[feature_cols] = out[feature_cols].shift(1)
    cutoff = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize("UTC")
    out["feature_timestamp_cutoff"] = (
        cutoff - pd.Timedelta(days=1) + pd.Timedelta(hours=23, minutes=59, seconds=59)
    )
    return out, feature_cols


def main() -> None:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    sys.path.insert(0, str(workspace))
    in_calendar = workspace / args.in_trading_calendar
    out_macro = workspace / args.out_macro_ssot
    out_features = workspace / args.out_macro_features
    out_report = workspace / args.out_report

    if not in_calendar.exists():
        raise FileNotFoundError(f"Input ausente: {in_calendar}")

    raw = pd.read_parquet(in_calendar, columns=["date"]).copy()
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.normalize()
    calendar = pd.DataFrame({"date": sorted(raw["date"].dropna().unique().tolist())})
    if calendar.empty:
        raise RuntimeError("Calendário de pregões vazio.")

    from lib.adapters import FredAdapter

    fred = FredAdapter(timeout_seconds=args.timeout_seconds, max_retries=args.max_retries)
    fetched = fred.fetch_all()

    merged = calendar.copy()
    for alias in BASE_SERIES:
        s = fetched[alias].copy()
        s["date"] = pd.to_datetime(s["date"], errors="coerce").dt.normalize()
        s[alias] = pd.to_numeric(s[alias], errors="coerce")
        s = s.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
        merged = merged.merge(s[["date", alias]], on="date", how="outer")

    merged = merged.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    missing_before = {c: int(merged[c].isna().sum()) for c in BASE_SERIES}
    merged[BASE_SERIES] = merged[BASE_SERIES].ffill()
    missing_after = {c: int(merged[c].isna().sum()) for c in BASE_SERIES}

    trading_dates_set = set(calendar["date"].tolist())
    macro_us = merged[merged["date"].isin(trading_dates_set)].copy()
    macro_us = macro_us.sort_values("date").reset_index(drop=True)
    macro_us = macro_us[["date"] + BASE_SERIES]

    if macro_us.empty:
        raise RuntimeError("macro_us vazio após filter para calendário de pregões.")

    features, feature_cols = _build_features(macro_us)

    macro_last = macro_us.iloc[-1]
    gate_last_row_no_nulls = bool(pd.notna(macro_last[BASE_SERIES]).all())

    first_feature_row = features.iloc[0]
    gate_shift1_first_row_nulls = bool(first_feature_row[feature_cols].isna().all())

    date_max_calendar = pd.to_datetime(calendar["date"]).max()
    date_max_features = pd.to_datetime(features["date"]).max()
    gate_coverage_d_minus_1 = bool(date_max_features >= (date_max_calendar - pd.Timedelta(days=1)))

    out_macro.parent.mkdir(parents=True, exist_ok=True)
    out_features.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    macro_us.to_parquet(out_macro, index=False)
    features.to_parquet(out_features, index=False)

    report = {
        "task_id": "T-011v2",
        "decision_ref": "D-007",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "trading_calendar_path": str(in_calendar),
            "fred_series": BASE_SERIES,
            "sha256_inputs": {
                "trading_calendar_source": _sha256(in_calendar),
            },
        },
        "counts": {
            "calendar_dates": int(calendar["date"].nunique()),
            "outer_merged_dates": int(merged["date"].nunique()),
            "macro_rows": int(len(macro_us)),
            "features_rows": int(len(features)),
            "features_cols_count": int(len(features.columns)),
        },
        "missing": {
            "before_ffill": missing_before,
            "after_ffill": missing_after,
        },
        "coverage": {
            "calendar_date_min": str(pd.to_datetime(calendar["date"]).min().date()),
            "calendar_date_max": str(date_max_calendar.date()),
            "features_date_max": str(date_max_features.date()),
        },
        "gates": {
            "macro_last_row_no_nulls": gate_last_row_no_nulls,
            "features_shift1_first_row_all_nulls": gate_shift1_first_row_nulls,
            "features_date_max_gte_calendar_date_max_minus_1d": gate_coverage_d_minus_1,
        },
        "outputs": {
            "macro_us_path": str(out_macro),
            "macro_features_path": str(out_features),
            "macro_us_sha256": _sha256(out_macro),
            "macro_features_sha256": _sha256(out_features),
        },
        "sample": {
            "macro_head": macro_us.head(3).to_dict(orient="records"),
            "features_head": features.head(3).to_dict(orient="records"),
        },
    }
    out_report.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    if not gate_last_row_no_nulls:
        raise RuntimeError("Gate FAIL: última linha do macro_us contém nulos nas séries base.")
    if not gate_shift1_first_row_nulls:
        raise RuntimeError("Gate FAIL: primeira linha de feature_* não está toda nula após shift(1).")
    if not gate_coverage_d_minus_1:
        raise RuntimeError("Gate FAIL: cobertura de features abaixo de D-1.")

    print("T-011v2 PASS")
    print(
        json.dumps(
            {
                "macro_rows": len(macro_us),
                "features_rows": len(features),
                "features_cols_count": len(features.columns),
                "gate_macro_last_row_no_nulls": gate_last_row_no_nulls,
                "gate_features_shift1_first_row_all_nulls": gate_shift1_first_row_nulls,
                "gate_features_date_max_gte_calendar_date_max_minus_1d": gate_coverage_d_minus_1,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
