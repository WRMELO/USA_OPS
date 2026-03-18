#!/usr/bin/env python3
"""T-025v2: Orchestrate stationary-feature retrain and trigger recalibration."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import timezone
from pathlib import Path

import pandas as pd


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _run_step(command: list[str], label: str, workspace: Path) -> None:
    print(f"[T-025v2] Running {label}: {' '.join(command)}")
    env = dict(**{"PYTHONPATH": str(workspace)})
    # Preserve existing PYTHONPATH if present.
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        env["PYTHONPATH"] = f"{str(workspace)}:{existing}"
    # Merge full environment.
    merged_env = dict(os.environ)
    merged_env.update(env)
    subprocess.run(command, check=True, cwd=str(workspace), env=merged_env)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-025v2 stationary-feature retrain orchestration.")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--python-env", default="")
    parser.add_argument("--out-report", default="data/features/t025v2_report.json")
    return parser.parse_args()


def _proba_stats(pred: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    out: dict[str, dict[str, float | int]] = {}
    for split, grp in pred.groupby(pred["split"].astype(str).str.upper()):
        s = pd.to_numeric(grp["y_proba_cash"], errors="coerce").dropna()
        out[split] = {
            "count": int(len(s)),
            "min": float(s.min()) if len(s) else 0.0,
            "median": float(s.median()) if len(s) else 0.0,
            "p90": float(s.quantile(0.9)) if len(s) else 0.0,
            "max": float(s.max()) if len(s) else 0.0,
            "mean": float(s.mean()) if len(s) else 0.0,
        }
    return out


def _proba_stats_by_year(pred: pd.DataFrame) -> dict[str, dict[str, dict[str, float | int]]]:
    pred = pred.copy()
    pred["year"] = pd.to_datetime(pred["date"], errors="coerce").dt.year
    out: dict[str, dict[str, dict[str, float | int]]] = {}
    for split, split_df in pred.groupby(pred["split"].astype(str).str.upper()):
        out[split] = {}
        for year, grp in split_df.groupby("year"):
            s = pd.to_numeric(grp["y_proba_cash"], errors="coerce").dropna()
            out[split][str(int(year))] = {
                "count": int(len(s)),
                "min": float(s.min()) if len(s) else 0.0,
                "median": float(s.median()) if len(s) else 0.0,
                "p90": float(s.quantile(0.9)) if len(s) else 0.0,
                "max": float(s.max()) if len(s) else 0.0,
                "mean": float(s.mean()) if len(s) else 0.0,
            }
    return out


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    py = Path(args.python_env) if args.python_env else (workspace / ".venv/bin/python")
    out_report = workspace / args.out_report

    if not py.exists():
        raise FileNotFoundError(f"Python env ausente: {py}")

    inputs = {
        "macro_features": workspace / "data/features/macro_features_us.parquet",
        "scores": workspace / "data/features/scores_m3_us.parquet",
        "canonical": workspace / "data/ssot/canonical_us.parquet",
        "labels": workspace / "data/features/labels_us.parquet",
        "script_t013": workspace / "scripts/t013_build_features_us.py",
        "script_t014": workspace / "scripts/t014_build_labels_us.py",
        "script_t025": workspace / "scripts/t025_train_xgboost_us.py",
        "script_t026": workspace / "scripts/t026_ablate_threshold_hysteresis_us.py",
    }
    for name, path in inputs.items():
        if not path.exists():
            raise FileNotFoundError(f"Input ausente ({name}): {path}")

    # Step chain: T-013 -> T-014 -> T-025 -> T-026
    _run_step([str(py), str(inputs["script_t013"]), "--workspace", str(workspace)], "T-013", workspace)
    _run_step([str(py), str(inputs["script_t014"]), "--workspace", str(workspace)], "T-014", workspace)
    _run_step([str(py), str(inputs["script_t025"]), "--workspace", str(workspace)], "T-025", workspace)
    _run_step([str(py), str(inputs["script_t026"]), "--workspace", str(workspace)], "T-026", workspace)

    outputs = {
        "feature_guard": workspace / "config/feature_guard_us.json",
        "dataset_us": workspace / "data/features/dataset_us.parquet",
        "dataset_us_labeled": workspace / "data/features/dataset_us_labeled.parquet",
        "xgb_model": workspace / "data/models/xgb_us.ubj",
        "predictions_us": workspace / "data/features/predictions_us.parquet",
        "ml_model_us": workspace / "config/ml_model_us.json",
        "ml_trigger_us": workspace / "config/ml_trigger_us.json",
        "t013_report": workspace / "data/features/t013_features_report.json",
        "t014_report": workspace / "data/features/t014_labels_report.json",
        "t025_report": workspace / "data/features/t025_xgboost_report.json",
        "t026_report": workspace / "data/features/t026_trigger_ablation_report.json",
    }
    for name, path in outputs.items():
        if not path.exists():
            raise FileNotFoundError(f"Output ausente ({name}): {path}")

    guard = json.loads(outputs["feature_guard"].read_text(encoding="utf-8"))
    features_required = list(guard.get("features_required") or [])
    level_features = [c for c in features_required if c.endswith("_level")]

    required_added = [
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
    missing_added = [c for c in required_added if c not in features_required]

    ds = pd.read_parquet(outputs["dataset_us"])
    ds_labeled = pd.read_parquet(outputs["dataset_us_labeled"])
    pred = pd.read_parquet(outputs["predictions_us"])
    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.normalize()

    gates = {
        "feature_guard_zero_level_features": len(level_features) == 0,
        "feature_guard_has_all_required_added_features": len(missing_added) == 0,
        "dataset_labeled_cardinality_matches_dataset": len(ds_labeled) == len(ds),
        "predictions_columns_exact": list(pred.columns) == ["date", "split", "y_cash", "y_proba_cash", "y_pred_cash"],
        "outputs_written": True,
    }

    report = {
        "task_id": "T-025v2",
        "decision_ref": "D-022",
        "generated_at": pd.Timestamp.now(tz=timezone.utc).isoformat(),
        "inputs": {
            "paths": {k: str(v) for k, v in inputs.items()},
            "sha256_inputs": {k: _sha256(v) for k, v in inputs.items()},
        },
        "outputs": {
            "paths": {k: str(v) for k, v in outputs.items()},
            "sha256_outputs": {k: _sha256(v) for k, v in outputs.items()},
        },
        "features": {
            "count": len(features_required),
            "level_features_count": len(level_features),
            "level_features": level_features,
            "required_added_features": required_added,
            "missing_added_features": missing_added,
        },
        "stats": {
            "dataset_rows": int(len(ds)),
            "dataset_labeled_rows": int(len(ds_labeled)),
            "predictions_rows": int(len(pred)),
            "y_proba_by_split": _proba_stats(pred),
            "y_proba_by_split_year": _proba_stats_by_year(pred),
        },
        "gates": gates,
    }

    out_report.parent.mkdir(parents=True, exist_ok=True)
    out_report.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    failing = [k for k, v in gates.items() if not bool(v)]
    if failing:
        raise RuntimeError(f"Gates FAIL: {failing}")

    print("T-025v2 PASS")
    print(
        json.dumps(
            {
                "dataset_rows": int(len(ds)),
                "predictions_rows": int(len(pred)),
                "features_count": len(features_required),
                "level_features_count": len(level_features),
                "missing_added_features_count": len(missing_added),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
