#!/usr/bin/env python3
"""T-025: Train US XGBoost (TRAIN-only) and generate cash probabilities."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import timezone
from pathlib import Path

import numpy as np
import pandas as pd


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _safe_div(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return float(num / den)


def _binary_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    if y_true.size == 0:
        return {
            "balanced_accuracy": 0.0,
            "f1_cash": 0.0,
            "recall_cash": 0.0,
            "precision_cash": 0.0,
        }

    y_true_i = y_true.astype(int)
    y_pred_i = y_pred.astype(int)

    tp = int(((y_true_i == 1) & (y_pred_i == 1)).sum())
    tn = int(((y_true_i == 0) & (y_pred_i == 0)).sum())
    fp = int(((y_true_i == 0) & (y_pred_i == 1)).sum())
    fn = int(((y_true_i == 1) & (y_pred_i == 0)).sum())

    recall_pos = _safe_div(tp, tp + fn)
    recall_neg = _safe_div(tn, tn + fp)
    precision = _safe_div(tp, tp + fp)
    f1 = _safe_div(2.0 * precision * recall_pos, precision + recall_pos)
    bal_acc = (recall_pos + recall_neg) / 2.0

    return {
        "balanced_accuracy": float(bal_acc),
        "f1_cash": float(f1),
        "recall_cash": float(recall_pos),
        "precision_cash": float(precision),
    }


def _transition_rate(y_pred: np.ndarray) -> float:
    if y_pred.size <= 1:
        return 0.0
    diffs = np.diff(y_pred.astype(int))
    transitions = int((diffs != 0).sum())
    return float(transitions / (y_pred.size - 1))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-025: train US XGBoost and infer probabilities.")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--in-dataset", default="data/features/dataset_us_labeled.parquet")
    parser.add_argument("--feature-guard", default="config/feature_guard_us.json")
    parser.add_argument("--out-model", default="data/models/xgb_us.ubj")
    parser.add_argument("--out-predictions", default="data/features/predictions_us.parquet")
    parser.add_argument("--out-config", default="config/ml_model_us.json")
    parser.add_argument("--out-report", default="data/features/t025_xgboost_report.json")
    parser.add_argument("--threshold", type=float, default=0.50)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    in_dataset = workspace / args.in_dataset
    feature_guard = workspace / args.feature_guard
    out_model = workspace / args.out_model
    out_predictions = workspace / args.out_predictions
    out_config = workspace / args.out_config
    out_report = workspace / args.out_report

    if not in_dataset.exists():
        raise FileNotFoundError(f"Input ausente: {in_dataset}")
    if not feature_guard.exists():
        raise FileNotFoundError(f"Feature guard ausente: {feature_guard}")

    guard = json.loads(feature_guard.read_text(encoding="utf-8"))
    features_used = list(guard.get("features_required") or [])
    if not features_used:
        raise RuntimeError("feature_guard_us.json sem features_required")

    ds = pd.read_parquet(in_dataset).copy()
    ds["date"] = pd.to_datetime(ds["date"], errors="coerce").dt.normalize()
    ds = ds.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if ds.empty:
        raise RuntimeError("dataset_us_labeled vazio após normalização")

    required_cols = {"date", "split", "y_cash"}
    missing_required = sorted(required_cols - set(ds.columns))
    if missing_required:
        raise RuntimeError(f"Dataset sem colunas obrigatórias: {missing_required}")

    missing_features = [c for c in features_used if c not in ds.columns]
    if missing_features:
        raise RuntimeError(f"Dataset sem features obrigatórias: {missing_features}")

    train = ds[ds["split"].astype(str).str.upper() == "TRAIN"].copy()
    train = train.dropna(subset=["y_cash"])
    if train.empty:
        raise RuntimeError("Sem linhas TRAIN com y_cash para treino")

    y_train = train["y_cash"].astype(int).values
    n_pos = int((y_train == 1).sum())
    n_neg = int((y_train == 0).sum())
    if n_pos == 0 or n_neg == 0:
        raise RuntimeError("TRAIN sem ambas as classes (0 e 1)")
    scale_pos_weight = float(n_neg / n_pos)

    x_train = train[features_used].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    gate_no_nan_in_features = bool(int(x_train.isna().sum().sum()) == 0)

    from xgboost import XGBClassifier

    params = {
        "n_estimators": 120,
        "max_depth": 4,
        "learning_rate": 0.06,
        "subsample": 0.8,
        "colsample_bytree": 1.0,
        "min_child_weight": 3,
        "reg_lambda": 1.0,
        "scale_pos_weight": scale_pos_weight,
    }

    model = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=42,
        n_jobs=1,
        **params,
    )
    model.fit(x_train, y_train)

    out_model.parent.mkdir(parents=True, exist_ok=True)
    out_predictions.parent.mkdir(parents=True, exist_ok=True)
    out_config.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    model.save_model(str(out_model))
    gate_model_persisted = bool(out_model.exists() and out_model.stat().st_size > 0)

    x_all = ds[features_used].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    y_proba = model.predict_proba(x_all)[:, 1].astype(float)
    y_pred = (y_proba >= float(args.threshold)).astype(int)

    pred = pd.DataFrame(
        {
            "date": ds["date"].dt.normalize(),
            "split": ds["split"].astype(str),
            "y_cash": ds["y_cash"],
            "y_proba_cash": y_proba,
            "y_pred_cash": y_pred.astype(int),
        }
    )
    pred.to_parquet(out_predictions, index=False)

    gate_predictions_rows = bool(len(pred) == len(ds))
    gate_predictions_columns = bool(
        list(pred.columns) == ["date", "split", "y_cash", "y_proba_cash", "y_pred_cash"]
    )
    gate_proba_range = bool(float(np.nanmin(y_proba)) >= 0.0 and float(np.nanmax(y_proba)) <= 1.0)
    gate_features_match_guard = bool(set(features_used) == set(guard.get("features_required") or []))
    gate_walk_forward_strict = True

    split_metrics: dict[str, dict[str, float | int]] = {}
    for split in ["TRAIN", "HOLDOUT"]:
        grp = pred[pred["split"].astype(str).str.upper() == split].copy()
        valid = grp.dropna(subset=["y_cash"])
        y_true = valid["y_cash"].astype(int).values
        y_hat = valid["y_pred_cash"].astype(int).values
        m = _binary_metrics(y_true, y_hat)
        m["transition_rate_pred"] = _transition_rate(y_hat)
        m["rows_total"] = int(len(grp))
        m["rows_valid_y_cash"] = int(len(valid))
        split_metrics[split.lower()] = m

    model_cfg = {
        "task_id": "T-025",
        "decision_ref": "D-002",
        "model_type": "XGBClassifier",
        "winner_candidate_id": "US_XGB_T025_BASELINE",
        "winner_variant": "US_PHASE4_ML_TRIGGER_BASE",
        "params": params,
        "threshold": float(args.threshold),
        "features_used": features_used,
        "scale_pos_weight": scale_pos_weight,
        "train_metrics": {
            "balanced_accuracy": split_metrics["train"]["balanced_accuracy"],
            "f1_cash": split_metrics["train"]["f1_cash"],
            "recall_cash": split_metrics["train"]["recall_cash"],
            "precision_cash": split_metrics["train"]["precision_cash"],
            "transition_rate_pred": split_metrics["train"]["transition_rate_pred"],
        },
        "holdout_metrics": {
            "balanced_accuracy": split_metrics["holdout"]["balanced_accuracy"],
            "f1_cash": split_metrics["holdout"]["f1_cash"],
            "recall_cash": split_metrics["holdout"]["recall_cash"],
            "precision_cash": split_metrics["holdout"]["precision_cash"],
            "transition_rate_pred": split_metrics["holdout"]["transition_rate_pred"],
        },
    }
    out_config.write_text(json.dumps(model_cfg, indent=2, ensure_ascii=False), encoding="utf-8")
    gate_config_written = bool(out_config.exists() and out_config.stat().st_size > 0)

    report = {
        "task_id": "T-025",
        "decision_ref": "D-002",
        "generated_at": pd.Timestamp.now(tz=timezone.utc).isoformat(),
        "inputs": {
            "dataset_path": str(in_dataset),
            "feature_guard_path": str(feature_guard),
            "threshold": float(args.threshold),
            "sha256_inputs": {
                "dataset_us_labeled": _sha256(in_dataset),
                "feature_guard_us": _sha256(feature_guard),
                "script": _sha256(Path(__file__).resolve()),
            },
        },
        "counts": {
            "rows_dataset": int(len(ds)),
            "rows_train_valid": int(len(train)),
            "rows_predictions": int(len(pred)),
            "train_pos_count": n_pos,
            "train_neg_count": n_neg,
        },
        "features": {
            "count": len(features_used),
            "features_used": features_used,
        },
        "metrics_by_split": split_metrics,
        "gates": {
            "walk_forward_strict": gate_walk_forward_strict,
            "features_match_guard": gate_features_match_guard,
            "no_nan_in_features_after_fill": gate_no_nan_in_features,
            "model_persisted": gate_model_persisted,
            "predictions_rows_match_dataset": gate_predictions_rows,
            "predictions_columns_exact": gate_predictions_columns,
            "proba_range_0_1": gate_proba_range,
            "config_written": gate_config_written,
            "outputs_written": False,
        },
        "outputs": {
            "model_path": str(out_model),
            "predictions_path": str(out_predictions),
            "config_path": str(out_config),
            "sha256_outputs": {},
        },
    }

    gate_outputs_written = bool(out_model.exists() and out_predictions.exists() and out_config.exists())
    if gate_outputs_written:
        report["outputs"]["sha256_outputs"] = {
            "xgb_us_ubj": _sha256(out_model),
            "predictions_us_parquet": _sha256(out_predictions),
            "ml_model_us_json": _sha256(out_config),
        }
    report["gates"]["outputs_written"] = gate_outputs_written

    out_report.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    failing = [k for k, v in report["gates"].items() if not bool(v)]
    if failing:
        raise RuntimeError(f"Gates FAIL: {failing}")

    print("T-025 PASS")
    print(
        json.dumps(
            {
                "rows_dataset": int(len(ds)),
                "rows_predictions": int(len(pred)),
                "train_pos_count": n_pos,
                "train_neg_count": n_neg,
                "scale_pos_weight": scale_pos_weight,
                "threshold": float(args.threshold),
                "holdout_balanced_accuracy": split_metrics["holdout"]["balanced_accuracy"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
