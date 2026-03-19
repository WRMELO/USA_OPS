#!/usr/bin/env python3
"""T-013: Feature engineering US (macro + SPC/M3 cross-section + equity proxy)."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-013: build US features dataset.")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--in-macro-features", default="data/features/macro_features_us.parquet")
    parser.add_argument("--in-scores", default="data/features/scores_m3_us.parquet")
    parser.add_argument(
        "--in-canonical",
        default=os.getenv("USA_OPS_CANONICAL_PATH", "data/ssot/canonical_us.parquet"),
    )
    parser.add_argument("--out-dataset", default="data/features/dataset_us.parquet")
    parser.add_argument("--out-feature-guard", default="config/feature_guard_us.json")
    parser.add_argument("--out-report", default="data/features/t013_features_report.json")
    return parser.parse_args()


def _compute_spc_feature(canonical: pd.DataFrame) -> pd.Series:
    valid = canonical["xbar_value"].notna() & canonical["xbar_ucl"].notna()
    flagged = valid & (canonical["xbar_value"] > canonical["xbar_ucl"])
    by_day_valid = valid.groupby(canonical["date"]).sum()
    by_day_flagged = flagged.groupby(canonical["date"]).sum()
    frac = by_day_flagged / by_day_valid.replace(0, np.nan)
    frac.name = "spc_xbar_special_frac"
    return frac


def _compute_m3_top_decile(scores: pd.DataFrame) -> pd.Series:
    def _day_frac(group: pd.DataFrame) -> float:
        ranks = pd.to_numeric(group["m3_rank"], errors="coerce").dropna()
        n = int(len(ranks))
        if n <= 0:
            return np.nan
        top_n = max(1, int(np.ceil(n * 0.10)))
        return float((ranks <= top_n).mean())

    frac = scores.groupby("date", as_index=True).apply(_day_frac)
    frac.name = "m3_frac_top_decile"
    return frac


def _build_equity_proxy_features(scores: pd.DataFrame, canonical: pd.DataFrame, base_dates: pd.DatetimeIndex) -> pd.DataFrame:
    scores = scores.copy()
    scores["date"] = pd.to_datetime(scores["date"], errors="coerce").dt.normalize()
    scores["m3_rank"] = pd.to_numeric(scores["m3_rank"], errors="coerce")
    scores = scores.dropna(subset=["date", "ticker", "m3_rank"])

    # Portfolio do dia D vem do ranking de D-1 (anti-lookahead).
    top10_by_date: dict[pd.Timestamp, list[str]] = {}
    for day, grp in scores.groupby("date"):
        picks = grp.sort_values("m3_rank").head(10)["ticker"].astype(str).tolist()
        top10_by_date[pd.Timestamp(day)] = picks

    px = canonical[["date", "ticker", "close_operational"]].copy()
    px["date"] = pd.to_datetime(px["date"], errors="coerce").dt.normalize()
    px["close_operational"] = pd.to_numeric(px["close_operational"], errors="coerce")
    px = px.dropna(subset=["date", "ticker"]).drop_duplicates(subset=["date", "ticker"], keep="last")
    px_wide = px.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="last").sort_index()
    ret_1d = px_wide.pct_change(1)

    dates = ret_1d.index.tolist()
    eq_ret = pd.Series(np.nan, index=ret_1d.index, dtype="float64", name="equity_proxy_ret_1d")
    for idx in range(1, len(dates)):
        day = pd.Timestamp(dates[idx])
        prev_day = pd.Timestamp(dates[idx - 1])
        picks = top10_by_date.get(prev_day, [])
        if not picks:
            continue
        available = [t for t in picks if t in ret_1d.columns]
        if not available:
            continue
        v = pd.to_numeric(ret_1d.loc[day, available], errors="coerce").dropna()
        if v.empty:
            continue
        eq_ret.loc[day] = float(v.mean())

    eq_index = pd.Series(np.nan, index=eq_ret.index, dtype="float64", name="equity_proxy_index")
    first_valid = eq_ret.first_valid_index()
    if first_valid is not None:
        cum = (1.0 + eq_ret.loc[first_valid:].fillna(0.0)).cumprod()
        eq_index.loc[first_valid:] = cum.values

    out = pd.DataFrame(index=base_dates)
    out["equity_ret_1d"] = eq_ret.reindex(base_dates)
    out["equity_ret_5d"] = eq_index.pct_change(5)
    out["equity_ret_21d"] = eq_index.pct_change(21)
    out["equity_mom_63d"] = (eq_index / eq_index.shift(63)) - 1.0
    out["equity_vol_21d"] = eq_ret.rolling(21, min_periods=21).std(ddof=0)
    out["equity_vol_63d"] = eq_ret.rolling(63, min_periods=63).std(ddof=0)
    rolling_max_252 = eq_index.rolling(252, min_periods=252).max()
    out["equity_dd_252d"] = (eq_index / rolling_max_252) - 1.0

    # Equidade excessiva vs retorno diario aproximado do Fed Funds (a partir do level em % aa).
    # Usa nivel macro (D-1) e aplica shift(1) final junto das demais features nao-macro.
    return out


def _build_stationary_macro_derivatives(out: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Build stationary macro derivatives from already shift(1) macro base."""
    df = out.copy()

    vix_level = pd.to_numeric(df["feature_vix_close_level"], errors="coerce")
    usd_level = pd.to_numeric(df["feature_usd_index_broad_level"], errors="coerce")
    ust10 = pd.to_numeric(df["feature_ust_10y_yield_level"], errors="coerce")
    ust2 = pd.to_numeric(df["feature_ust_2y_yield_level"], errors="coerce")
    ff = pd.to_numeric(df["feature_fed_funds_rate_level"], errors="coerce")
    hy = pd.to_numeric(df["feature_hy_oas_level"], errors="coerce")
    ig = pd.to_numeric(df["feature_ig_oas_level"], errors="coerce")

    spread = ust10 - ust2
    vix_ret_1d = vix_level.pct_change(1)
    usd_ret_1d = usd_level.pct_change(1)

    df["feature_ust_10y_2y_spread"] = spread
    df["feature_ust_spread_delta_1d"] = spread.diff(1)
    df["feature_ust_spread_delta_5d"] = spread.diff(5)
    df["feature_vix_ret_1d"] = vix_ret_1d
    df["feature_vix_ret_5d"] = vix_level.pct_change(5)
    df["feature_vix_ret_21d"] = vix_level.pct_change(21)
    df["feature_vix_vol_21d"] = vix_ret_1d.rolling(21, min_periods=21).std(ddof=0)
    df["feature_ust_10y_delta_5d"] = ust10.diff(5)
    df["feature_ust_2y_delta_5d"] = ust2.diff(5)
    df["feature_fed_funds_rate_delta_5d"] = ff.diff(5)
    df["feature_hy_oas_delta_5d"] = hy.diff(5)
    df["feature_ig_oas_delta_5d"] = ig.diff(5)
    df["feature_usd_index_broad_ret_5d"] = usd_level.pct_change(5)
    df["feature_usd_index_broad_ret_21d"] = usd_level.pct_change(21)
    df["feature_usd_index_broad_vol_21d"] = usd_ret_1d.rolling(21, min_periods=21).std(ddof=0)

    added_cols = [
        "feature_ust_10y_2y_spread",
        "feature_ust_spread_delta_1d",
        "feature_ust_spread_delta_5d",
        "feature_vix_ret_1d",
        "feature_vix_ret_5d",
        "feature_vix_ret_21d",
        "feature_vix_vol_21d",
        "feature_ust_10y_delta_5d",
        "feature_ust_2y_delta_5d",
        "feature_fed_funds_rate_delta_5d",
        "feature_hy_oas_delta_5d",
        "feature_ig_oas_delta_5d",
        "feature_usd_index_broad_ret_5d",
        "feature_usd_index_broad_ret_21d",
        "feature_usd_index_broad_vol_21d",
    ]
    return df, added_cols


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    in_macro = workspace / args.in_macro_features
    in_scores = workspace / args.in_scores
    in_canonical = workspace / args.in_canonical
    out_dataset = workspace / args.out_dataset
    out_guard = workspace / args.out_feature_guard
    out_report = workspace / args.out_report

    for p in [in_macro, in_scores, in_canonical]:
        if not p.exists():
            raise FileNotFoundError(f"Input ausente: {p}")

    macro = pd.read_parquet(in_macro).copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

    macro_level_cols = sorted(
        [
            c
            for c in macro.columns
            if c.startswith("feature_")
            and c != "feature_timestamp_cutoff"
            and c.endswith("_level")
        ]
    )
    macro_feature_cols = sorted(
        [
            c
            for c in macro.columns
            if c.startswith("feature_")
            and c != "feature_timestamp_cutoff"
            and not c.endswith("_level")
        ]
    )
    if not macro_feature_cols:
        raise RuntimeError("Input macro_features_us.parquet sem colunas feature_*")

    scores = pd.read_parquet(in_scores, columns=["date", "ticker", "m3_rank"]).copy()
    scores["date"] = pd.to_datetime(scores["date"], errors="coerce").dt.normalize()
    scores = scores.dropna(subset=["date"]).drop_duplicates(subset=["date", "ticker"], keep="last")

    canonical = pd.read_parquet(in_canonical, columns=["date", "ticker", "close_operational", "xbar_value", "xbar_ucl"]).copy()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["date"]).drop_duplicates(subset=["date", "ticker"], keep="last")

    base = macro[["date", "feature_timestamp_cutoff"] + macro_level_cols + macro_feature_cols].copy()
    base = base.sort_values("date").reset_index(drop=True)
    base_dates = pd.DatetimeIndex(base["date"])

    spc_frac = _compute_spc_feature(canonical).reindex(base_dates)
    m3_frac = _compute_m3_top_decile(scores).reindex(base_dates)
    eq = _build_equity_proxy_features(scores, canonical, base_dates)

    out = base.set_index("date")
    out["spc_xbar_special_frac"] = spc_frac
    out["m3_frac_top_decile"] = m3_frac
    for col in eq.columns:
        out[col] = eq[col]
    out, added_stationary_cols = _build_stationary_macro_derivatives(out)

    ff_level = pd.to_numeric(out["feature_fed_funds_rate_level"], errors="coerce")
    ff_daily = ((1.0 + (ff_level / 100.0)).pow(1.0 / 252.0) - 1.0).where((1.0 + (ff_level / 100.0)) > 0)
    out["equity_vs_ff_21d"] = (
        pd.to_numeric(out["equity_ret_1d"], errors="coerce").rolling(21, min_periods=21).sum()
        - ff_daily.rolling(21, min_periods=21).sum()
    )

    non_macro_cols = [
        "spc_xbar_special_frac",
        "m3_frac_top_decile",
        "equity_ret_5d",
        "equity_ret_21d",
        "equity_mom_63d",
        "equity_vol_21d",
        "equity_vol_63d",
        "equity_dd_252d",
        "equity_vs_ff_21d",
    ]
    out[non_macro_cols] = out[non_macro_cols].shift(1)

    out = out.reset_index().rename(columns={"index": "date"})
    if "equity_ret_1d" in out.columns:
        out = out.drop(columns=["equity_ret_1d"])
    out = out.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

    required_features = macro_feature_cols + added_stationary_cols + non_macro_cols
    # De-duplicate while preserving order.
    required_features = list(dict.fromkeys(required_features))
    guard_payload = {
        "task_id": "T-013",
        "decision_ref": "D-002, D-009, D-010, D-012",
        "features_required": required_features,
    }
    out_guard.parent.mkdir(parents=True, exist_ok=True)
    out_guard.write_text(json.dumps(guard_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    missing_guard_cols = [c for c in required_features if c not in out.columns]
    if missing_guard_cols:
        raise RuntimeError(f"Feature guard FAIL: colunas ausentes {missing_guard_cols}")

    # Export only approved features (no *_level in final dataset).
    out = out[["date", "feature_timestamp_cutoff"] + required_features].copy()
    gate_no_dup_date = int(out.duplicated(subset=["date"]).sum()) == 0
    first_row = out.iloc[0] if not out.empty else pd.Series(dtype="object")
    gate_shift1_non_macro = bool(first_row[non_macro_cols].isna().all()) if not out.empty else False
    level_features_in_guard = [c for c in required_features if c.endswith("_level")]
    gate_no_level_features = len(level_features_in_guard) == 0

    null_rate = {c: float(pd.to_numeric(out[c], errors="coerce").isna().mean()) for c in required_features}

    out_dataset.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_dataset, index=False)

    report = {
        "task_id": "T-013",
        "decision_ref": "D-002, D-009, D-010, D-012",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "macro_features_path": str(in_macro),
            "scores_path": str(in_scores),
            "canonical_path": str(in_canonical),
            "sha256_inputs": {
                "macro_features_us": _sha256(in_macro),
                "scores_m3_us": _sha256(in_scores),
                "canonical_us": _sha256(in_canonical),
                "script": _sha256(Path(__file__).resolve()),
                "feature_guard": _sha256(out_guard),
            },
        },
        "counts": {
            "rows_dataset": int(len(out)),
            "dates_dataset": int(out["date"].nunique()),
            "features_required_count": int(len(required_features)),
            "macro_feature_count": int(len(macro_feature_cols)),
            "non_macro_feature_count": int(len(non_macro_cols)),
                "added_stationary_feature_count": int(len(added_stationary_cols)),
        },
        "gates": {
            "feature_guard_no_missing_columns": len(missing_guard_cols) == 0,
            "zero_duplicates_date": gate_no_dup_date,
            "shift1_non_macro_first_row_all_null": gate_shift1_non_macro,
            "feature_guard_has_zero_level_features": gate_no_level_features,
        },
        "added_stationary_features": added_stationary_cols,
        "level_features_in_guard": level_features_in_guard,
        "null_rate_by_feature": null_rate,
        "sample": {
            "dataset_head": out.head(5).to_dict(orient="records"),
            "features_required_head": required_features[:20],
        },
        "outputs": {
            "dataset_us_path": str(out_dataset),
            "feature_guard_path": str(out_guard),
            "sha256_outputs": {
                "dataset_us": _sha256(out_dataset),
                "feature_guard_us": _sha256(out_guard),
            },
        },
    }
    out_report.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    if missing_guard_cols:
        raise RuntimeError(f"Gate FAIL: feature guard com colunas ausentes {missing_guard_cols}")
    if not gate_no_dup_date:
        raise RuntimeError("Gate FAIL: dataset com duplicatas por date")
    if not gate_shift1_non_macro:
        raise RuntimeError("Gate FAIL: primeira linha de features nao-macro nao esta toda nula apos shift(1)")
    if not gate_no_level_features:
        raise RuntimeError(f"Gate FAIL: feature_guard contém _level: {level_features_in_guard}")

    print("T-013 PASS")
    print(
        json.dumps(
            {
                "rows_dataset": int(len(out)),
                "dates_dataset": int(out["date"].nunique()),
                "features_required_count": int(len(required_features)),
                "gate_feature_guard_no_missing_columns": len(missing_guard_cols) == 0,
                "gate_zero_duplicates_date": gate_no_dup_date,
                "gate_shift1_non_macro_first_row_all_null": gate_shift1_non_macro,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
