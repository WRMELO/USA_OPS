#!/usr/bin/env python3
"""T-026: Ablate threshold + hysteresis for US ML trigger."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.engine import apply_hysteresis


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
    changes = np.diff(y_pred.astype(int))
    return float((changes != 0).sum() / (y_pred.size - 1))


def _evaluate_split(df: pd.DataFrame, pred_col: str) -> dict[str, float | int]:
    valid = df.dropna(subset=["y_cash"]).copy()
    y_true = valid["y_cash"].astype(int).values
    y_pred = valid[pred_col].astype(int).values
    metrics = _binary_metrics(y_true, y_pred)
    metrics["transition_rate"] = _transition_rate(y_pred)
    metrics["rows_total"] = int(len(df))
    metrics["rows_valid_y_cash"] = int(len(valid))
    return metrics


def _build_thr_grid() -> list[float]:
    base = [i / 100.0 for i in range(1, 21)]  # 0.01..0.20
    base.append(0.22)  # BR baseline
    # Unique and sorted with stable float representation.
    return sorted({round(x, 2) for x in base})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-026: ablate thr/h_in/h_out with TRAIN-only selection.")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--in-predictions", default="data/features/predictions_us.parquet")
    parser.add_argument("--in-ml-config", default="config/ml_model_us.json")
    parser.add_argument("--out-summary-csv", default="data/features/t026_trigger_ablation_summary.csv")
    parser.add_argument("--out-summary-json", default="data/features/t026_trigger_ablation_summary.json")
    parser.add_argument("--out-trigger-config", default="config/ml_trigger_us.json")
    parser.add_argument("--out-report", default="data/features/t026_trigger_ablation_report.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    in_predictions = workspace / args.in_predictions
    in_ml_config = workspace / args.in_ml_config
    out_summary_csv = workspace / args.out_summary_csv
    out_summary_json = workspace / args.out_summary_json
    out_trigger_config = workspace / args.out_trigger_config
    out_report = workspace / args.out_report

    if not in_predictions.exists():
        raise FileNotFoundError(f"Input ausente: {in_predictions}")
    if not in_ml_config.exists():
        raise FileNotFoundError(f"Input ausente: {in_ml_config}")

    pred = pd.read_parquet(in_predictions).copy()
    required = {"date", "split", "y_cash", "y_proba_cash"}
    missing = sorted(required - set(pred.columns))
    if missing:
        raise RuntimeError(f"predictions_us sem colunas obrigatórias: {missing}")

    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.normalize()
    pred = pred.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if pred.empty:
        raise RuntimeError("predictions_us vazio após normalização")

    thr_grid = _build_thr_grid()
    h_in_grid = [1, 2, 3, 4, 5, 6]
    h_out_grid = [1, 2, 3, 4, 5, 6]

    rows: list[dict[str, float | int | str]] = []
    for thr in thr_grid:
        state = apply_hysteresis(pred["y_proba_cash"], thr=float(thr), h_in=1, h_out=1)
        # We compute once for base shape validation; below loop recomputes exact h_in/h_out.
        if len(state) != len(pred):
            raise RuntimeError("Falha interna: state_cash com tamanho divergente")

        for h_in in h_in_grid:
            for h_out in h_out_grid:
                state_cash = apply_hysteresis(pred["y_proba_cash"], thr=float(thr), h_in=int(h_in), h_out=int(h_out))
                tmp = pred.copy()
                tmp["state_cash"] = state_cash.astype(int)

                train_df = tmp[tmp["split"].astype(str).str.upper() == "TRAIN"].copy()
                holdout_df = tmp[tmp["split"].astype(str).str.upper() == "HOLDOUT"].copy()

                m_train = _evaluate_split(train_df, "state_cash")
                m_hold = _evaluate_split(holdout_df, "state_cash")

                rows.append(
                    {
                        "thr": float(thr),
                        "h_in": int(h_in),
                        "h_out": int(h_out),
                        "train_balanced_accuracy": float(m_train["balanced_accuracy"]),
                        "train_f1_cash": float(m_train["f1_cash"]),
                        "train_recall_cash": float(m_train["recall_cash"]),
                        "train_precision_cash": float(m_train["precision_cash"]),
                        "train_transition_rate": float(m_train["transition_rate"]),
                        "train_rows_total": int(m_train["rows_total"]),
                        "train_rows_valid_y_cash": int(m_train["rows_valid_y_cash"]),
                        "holdout_balanced_accuracy": float(m_hold["balanced_accuracy"]),
                        "holdout_f1_cash": float(m_hold["f1_cash"]),
                        "holdout_recall_cash": float(m_hold["recall_cash"]),
                        "holdout_precision_cash": float(m_hold["precision_cash"]),
                        "holdout_transition_rate": float(m_hold["transition_rate"]),
                        "holdout_rows_total": int(m_hold["rows_total"]),
                        "holdout_rows_valid_y_cash": int(m_hold["rows_valid_y_cash"]),
                    }
                )

    summary = pd.DataFrame(rows)
    if summary.empty:
        raise RuntimeError("Ablação vazia: nenhuma combinação gerada")

    # TRAIN-only selection rule.
    summary = summary.sort_values(
        by=[
            "train_balanced_accuracy",
            "train_transition_rate",
            "train_f1_cash",
            "thr",
            "h_in",
            "h_out",
        ],
        ascending=[False, True, False, True, True, True],
    ).reset_index(drop=True)
    summary["rank_train"] = np.arange(1, len(summary) + 1, dtype=int)

    best = summary.iloc[0].to_dict()

    baseline_mask = (
        (summary["thr"] == 0.22) & (summary["h_in"] == 3) & (summary["h_out"] == 2)
    )
    gate_baseline_present = bool(int(baseline_mask.sum()) == 1)
    baseline_row = summary[baseline_mask].iloc[0].to_dict() if gate_baseline_present else {}

    out_summary_csv.parent.mkdir(parents=True, exist_ok=True)
    out_summary_json.parent.mkdir(parents=True, exist_ok=True)
    out_trigger_config.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    summary.to_csv(out_summary_csv, index=False)
    out_summary_json.write_text(
        json.dumps(
            {
                "task_id": "T-026",
                "decision_ref": "D-002",
                "selection_rule": "TRAIN-only: sort by train_balanced_accuracy desc, train_transition_rate asc, train_f1_cash desc, thr asc, h_in asc, h_out asc",
                "grid_size": int(len(summary)),
                "rows": summary.to_dict(orient="records"),
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    trigger_cfg = {
        "task_id": "T-026",
        "decision_ref": "D-002",
        "selected_params": {
            "thr": float(best["thr"]),
            "h_in": int(best["h_in"]),
            "h_out": int(best["h_out"]),
        },
        "selection_rule": "TRAIN-only: sort by train_balanced_accuracy desc, train_transition_rate asc, train_f1_cash desc, thr asc, h_in asc, h_out asc",
        "baseline_br": {
            "thr": 0.22,
            "h_in": 3,
            "h_out": 2,
        },
        "selected_metrics": {
            "train": {
                "balanced_accuracy": float(best["train_balanced_accuracy"]),
                "f1_cash": float(best["train_f1_cash"]),
                "recall_cash": float(best["train_recall_cash"]),
                "precision_cash": float(best["train_precision_cash"]),
                "transition_rate": float(best["train_transition_rate"]),
                "rows_valid_y_cash": int(best["train_rows_valid_y_cash"]),
            },
            "holdout": {
                "balanced_accuracy": float(best["holdout_balanced_accuracy"]),
                "f1_cash": float(best["holdout_f1_cash"]),
                "recall_cash": float(best["holdout_recall_cash"]),
                "precision_cash": float(best["holdout_precision_cash"]),
                "transition_rate": float(best["holdout_transition_rate"]),
                "rows_valid_y_cash": int(best["holdout_rows_valid_y_cash"]),
            },
        },
        "baseline_metrics": {
            "train": {
                "balanced_accuracy": float(baseline_row.get("train_balanced_accuracy", 0.0)),
                "f1_cash": float(baseline_row.get("train_f1_cash", 0.0)),
                "transition_rate": float(baseline_row.get("train_transition_rate", 0.0)),
            },
            "holdout": {
                "balanced_accuracy": float(baseline_row.get("holdout_balanced_accuracy", 0.0)),
                "f1_cash": float(baseline_row.get("holdout_f1_cash", 0.0)),
                "transition_rate": float(baseline_row.get("holdout_transition_rate", 0.0)),
            },
        },
    }
    out_trigger_config.write_text(json.dumps(trigger_cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    gate_train_only_selection = True
    gate_summary_written = bool(out_summary_csv.exists() and out_summary_json.exists())
    gate_trigger_config_written = bool(out_trigger_config.exists() and out_trigger_config.stat().st_size > 0)
    gate_grid_nonempty = bool(len(summary) > 0)

    report = {
        "task_id": "T-026",
        "decision_ref": "D-002",
        "generated_at": pd.Timestamp.now(tz=timezone.utc).isoformat(),
        "inputs": {
            "predictions_path": str(in_predictions),
            "ml_model_path": str(in_ml_config),
            "sha256_inputs": {
                "predictions_us": _sha256(in_predictions),
                "ml_model_us": _sha256(in_ml_config),
                "script": _sha256(Path(__file__).resolve()),
            },
        },
        "grid": {
            "thr_values": thr_grid,
            "h_in_values": h_in_grid,
            "h_out_values": h_out_grid,
            "grid_size": int(len(summary)),
        },
        "selection": {
            "rule": "TRAIN-only: sort by train_balanced_accuracy desc, train_transition_rate asc, train_f1_cash desc, thr asc, h_in asc, h_out asc",
            "best_params": {
                "thr": float(best["thr"]),
                "h_in": int(best["h_in"]),
                "h_out": int(best["h_out"]),
            },
            "best_rank_train": int(best["rank_train"]),
        },
        "baseline_br": {
            "params": {"thr": 0.22, "h_in": 3, "h_out": 2},
            "present_in_grid": gate_baseline_present,
        },
        "top10_train": summary.head(10).to_dict(orient="records"),
        "gates": {
            "baseline_br_present_in_grid": gate_baseline_present,
            "train_only_selection_rule": gate_train_only_selection,
            "summary_written": gate_summary_written,
            "trigger_config_written": gate_trigger_config_written,
            "grid_nonempty": gate_grid_nonempty,
            "outputs_written": False,
        },
        "outputs": {
            "summary_csv": str(out_summary_csv),
            "summary_json": str(out_summary_json),
            "trigger_config": str(out_trigger_config),
            "sha256_outputs": {},
        },
    }

    gate_outputs_written = bool(
        out_summary_csv.exists() and out_summary_json.exists() and out_trigger_config.exists()
    )
    if gate_outputs_written:
        report["outputs"]["sha256_outputs"] = {
            "t026_summary_csv": _sha256(out_summary_csv),
            "t026_summary_json": _sha256(out_summary_json),
            "ml_trigger_us_json": _sha256(out_trigger_config),
        }
    report["gates"]["outputs_written"] = gate_outputs_written

    out_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    failing = [k for k, v in report["gates"].items() if not bool(v)]
    if failing:
        raise RuntimeError(f"Gates FAIL: {failing}")

    print("T-026 PASS")
    print(
        json.dumps(
            {
                "grid_size": int(len(summary)),
                "best_params": {
                    "thr": float(best["thr"]),
                    "h_in": int(best["h_in"]),
                    "h_out": int(best["h_out"]),
                },
                "best_train_balanced_accuracy": float(best["train_balanced_accuracy"]),
                "best_holdout_balanced_accuracy": float(best["holdout_balanced_accuracy"]),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
