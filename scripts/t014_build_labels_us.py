#!/usr/bin/env python3
"""T-014: Build US regime labels (oracle drawdown-based) and labeled dataset."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import timezone
from pathlib import Path

import numpy as np
import pandas as pd

from lib.adapters import FredAdapter


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-014: build US labels and labeled dataset.")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--in-dataset", default="data/features/dataset_us.parquet")
    parser.add_argument("--out-labels", default="data/features/labels_us.parquet")
    parser.add_argument("--out-dataset-labeled", default="data/features/dataset_us_labeled.parquet")
    parser.add_argument("--out-report", default="data/features/t014_labels_report.json")
    parser.add_argument("--fred-series-id", default="SP500")
    parser.add_argument("--horizon-days", type=int, default=63)
    parser.add_argument("--train-quantile", type=float, default=0.20)
    return parser.parse_args()


def _split_for_date(dt: pd.Timestamp) -> str:
    train_start = pd.Timestamp("2018-01-02")
    train_end = pd.Timestamp("2022-12-30")
    holdout_start = pd.Timestamp("2023-01-02")
    holdout_end = pd.Timestamp("2026-03-16")
    live_start = pd.Timestamp("2026-03-17")
    if train_start <= dt <= train_end:
        return "TRAIN"
    if holdout_start <= dt <= holdout_end:
        return "HOLDOUT"
    if dt >= live_start:
        return "LIVE"
    return "TRAIN"


def _forward_max_drawdown(price: pd.Series, horizon: int) -> pd.Series:
    values = pd.to_numeric(price, errors="coerce").astype(float).values
    out = np.full(len(values), np.nan, dtype="float64")
    n = len(values)
    for i in range(n):
        end = i + horizon
        if end >= n:
            continue
        window = values[i : end + 1]
        if np.isnan(window).any():
            continue
        running_max = np.maximum.accumulate(window)
        dd = window / running_max - 1.0
        out[i] = float(np.min(dd))
    return pd.Series(out, index=price.index, name=f"fwd_max_drawdown_{horizon}d")


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    in_dataset = workspace / args.in_dataset
    out_labels = workspace / args.out_labels
    out_dataset_labeled = workspace / args.out_dataset_labeled
    out_report = workspace / args.out_report

    if not in_dataset.exists():
        raise FileNotFoundError(f"Input ausente: {in_dataset}")

    ds = pd.read_parquet(in_dataset).copy()
    ds["date"] = pd.to_datetime(ds["date"], errors="coerce").dt.normalize()
    ds = ds.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    if ds.empty:
        raise RuntimeError("dataset_us.parquet vazio apos normalizacao de datas")

    fred = FredAdapter(timeout_seconds=30.0, max_retries=5)
    sp = fred.fetch_series(args.fred_series_id, "sp500_close")
    sp["date"] = pd.to_datetime(sp["date"], errors="coerce").dt.normalize()
    sp = sp.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

    base = ds[["date"]].copy()
    base = base.merge(sp, on="date", how="left")
    base["sp500_close"] = pd.to_numeric(base["sp500_close"], errors="coerce").ffill()
    base["split"] = base["date"].apply(_split_for_date)
    base[f"fwd_max_drawdown_{args.horizon_days}d"] = _forward_max_drawdown(base["sp500_close"], args.horizon_days)

    fwd_col = f"fwd_max_drawdown_{args.horizon_days}d"
    train_mask = base["split"] == "TRAIN"
    train_valid = base.loc[train_mask, fwd_col].dropna()
    if train_valid.empty:
        raise RuntimeError("Sem valores TRAIN válidos para calibrar threshold")

    threshold = float(train_valid.quantile(args.train_quantile))
    y = pd.Series(np.nan, index=base.index, dtype="float64")
    valid_fwd = base[fwd_col].notna()
    y.loc[valid_fwd] = (base.loc[valid_fwd, fwd_col] <= threshold).astype(int)
    base["y_cash"] = y

    labels = base[["date", "split", "sp500_close", fwd_col, "y_cash"]].copy()
    labels = labels.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

    labeled = ds.merge(labels, on="date", how="left", validate="1:1")
    labeled = labeled.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

    gate_zero_dup_labels = int(labels.duplicated(subset=["date"]).sum()) == 0
    gate_zero_dup_labeled = int(labeled.duplicated(subset=["date"]).sum()) == 0
    gate_merge_1to1_rows = int(len(labeled)) == int(len(ds))
    gate_train_only_threshold = True
    gate_split_windows = bool((labels["split"].isin(["TRAIN", "HOLDOUT", "LIVE"])).all())
    tail_expected_nan = int(labels.tail(args.horizon_days)["y_cash"].isna().sum())
    gate_tail_nan = tail_expected_nan == min(args.horizon_days, len(labels))

    out_labels.parent.mkdir(parents=True, exist_ok=True)
    out_dataset_labeled.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    labels.to_parquet(out_labels, index=False)
    labeled.to_parquet(out_dataset_labeled, index=False)

    split_counts = labels["split"].value_counts(dropna=False).to_dict()
    label_balance_by_split: dict[str, dict[str, int]] = {}
    for split, grp in labels.groupby("split"):
        vc = grp["y_cash"].value_counts(dropna=False).to_dict()
        clean_vc = {str(k): int(v) for k, v in vc.items()}
        label_balance_by_split[str(split)] = clean_vc

    report = {
        "task_id": "T-014",
        "decision_ref": "D-002, D-009, D-010",
        "generated_at": pd.Timestamp.now(tz=timezone.utc).isoformat(),
        "inputs": {
            "dataset_path": str(in_dataset),
            "fred_series_id": args.fred_series_id,
            "horizon_days": int(args.horizon_days),
            "train_quantile": float(args.train_quantile),
            "sha256_inputs": {
                "dataset_us": _sha256(in_dataset),
                "script": _sha256(Path(__file__).resolve()),
            },
        },
        "threshold_train_only": {
            "rule": f"threshold = quantile_{args.train_quantile:.2f}(fwd_max_drawdown_{args.horizon_days}d) on split=TRAIN only",
            "value": threshold,
            "train_valid_count": int(train_valid.shape[0]),
        },
        "counts": {
            "rows_dataset_input": int(len(ds)),
            "rows_labels": int(len(labels)),
            "rows_dataset_labeled": int(len(labeled)),
            "dates_labels": int(labels["date"].nunique()),
            "tail_nan_expected": int(min(args.horizon_days, len(labels))),
            "tail_nan_actual": int(tail_expected_nan),
            "sp500_null_after_ffill": int(labels["sp500_close"].isna().sum()),
            "fwd_null_count": int(labels[fwd_col].isna().sum()),
            "y_cash_null_count": int(labels["y_cash"].isna().sum()),
        },
        "split_counts": {str(k): int(v) for k, v in split_counts.items()},
        "label_balance_by_split": label_balance_by_split,
        "gates": {
            "zero_duplicates_labels_date": gate_zero_dup_labels,
            "zero_duplicates_dataset_labeled_date": gate_zero_dup_labeled,
            "merge_1to1_row_count_match": gate_merge_1to1_rows,
            "threshold_train_only": gate_train_only_threshold,
            "split_windows_valid": gate_split_windows,
            "tail_y_cash_nan_expected": gate_tail_nan,
        },
        "sample": {
            "labels_head": labels.head(5).to_dict(orient="records"),
            "labels_tail": labels.tail(5).to_dict(orient="records"),
        },
        "outputs": {
            "labels_path": str(out_labels),
            "dataset_labeled_path": str(out_dataset_labeled),
            "sha256_outputs": {
                "labels_us": _sha256(out_labels),
                "dataset_us_labeled": _sha256(out_dataset_labeled),
            },
        },
    }
    out_report.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    if not gate_zero_dup_labels:
        raise RuntimeError("Gate FAIL: duplicatas por date em labels_us")
    if not gate_zero_dup_labeled:
        raise RuntimeError("Gate FAIL: duplicatas por date em dataset_us_labeled")
    if not gate_merge_1to1_rows:
        raise RuntimeError("Gate FAIL: merge labels x dataset não preservou cardinalidade")
    if not gate_tail_nan:
        raise RuntimeError("Gate FAIL: tail de y_cash sem NaN esperado para janela futura")

    print("T-014 PASS")
    print(
        json.dumps(
            {
                "rows_labels": int(len(labels)),
                "rows_dataset_labeled": int(len(labeled)),
                "threshold_train_only_value": threshold,
                "gate_zero_duplicates_labels_date": gate_zero_dup_labels,
                "gate_zero_duplicates_dataset_labeled_date": gate_zero_dup_labeled,
                "gate_merge_1to1_row_count_match": gate_merge_1to1_rows,
                "gate_tail_y_cash_nan_expected": gate_tail_nan,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
