"""Step 11 — reconciliação de métricas (curva operacional vs winner)."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _curve_metrics(curve: pd.DataFrame) -> tuple[float, float]:
    if curve.empty or len(curve) < 2:
        return 0.0, 0.0
    eq = pd.to_numeric(curve["equity"], errors="coerce")
    eq = eq.replace([np.inf, -np.inf], np.nan).dropna()
    if len(eq) < 2:
        return 0.0, 0.0
    running_max = eq.cummax().replace(0.0, np.nan)
    dd = (eq / running_max) - 1.0
    mdd = float(dd.min()) if dd.notna().any() else 0.0
    n_years = max(len(eq) / 252.0, 1.0 / 252.0)
    cagr = float((eq.iloc[-1] / eq.iloc[0]) ** (1.0 / n_years) - 1.0) if eq.iloc[0] > 0 else 0.0
    return cagr, mdd


def _annualized_sharpe(curve: pd.DataFrame) -> float:
    eq = pd.to_numeric(curve["equity"], errors="coerce")
    rets = eq.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    if rets.empty:
        return 0.0
    sigma = float(rets.std(ddof=0))
    if sigma <= 0:
        return 0.0
    return float((float(rets.mean()) / sigma) * np.sqrt(252.0))


def run() -> dict:
    winner_path = ROOT / "config" / "winner_us.json"
    curve_path = ROOT / "data" / "daily" / "winner_curve_us.parquet"
    out_path = ROOT / "logs" / "metrics_reconciliation.json"
    if not winner_path.exists() or not curve_path.exists():
        raise FileNotFoundError("Inputs ausentes para reconciliação.")

    winner = json.loads(winner_path.read_text(encoding="utf-8"))
    curve = pd.read_parquet(curve_path).copy()
    curve["date"] = pd.to_datetime(curve["date"], errors="coerce").dt.normalize()
    curve = curve.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    cagr, mdd = _curve_metrics(curve)
    sharpe = _annualized_sharpe(curve)
    holdout_ref = winner.get("holdout_metrics", {})
    cagr_ref = float(holdout_ref.get("cagr_pct", 0.0))
    mdd_ref = float(holdout_ref.get("mdd_pct", 0.0))

    cagr_abs_diff = abs((cagr * 100.0) - cagr_ref)
    mdd_abs_diff = abs((mdd * 100.0) - mdd_ref)
    tol_pct_points = 2.0

    # Fallback robusto: reconciliar com a curva canônica apontada no winner JSON.
    winner_curve_rel = winner.get("winner_curve_path")
    winner_curve_metrics = None
    winner_curve_match = False
    winner_curve_diff = {"cagr_pct_points": None, "mdd_pct_points": None}
    if winner_curve_rel:
        winner_curve_path = ROOT / str(winner_curve_rel)
        if winner_curve_path.exists():
            wcurve = pd.read_csv(winner_curve_path)
            wcurve["date"] = pd.to_datetime(wcurve["date"], errors="coerce").dt.normalize()
            wcurve = wcurve.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            wcagr, wmdd = _curve_metrics(wcurve)
            winner_curve_metrics = {
                "cagr_pct": float(wcagr * 100.0),
                "mdd_pct": float(wmdd * 100.0),
                "days": int(len(wcurve)),
            }
            winner_curve_diff = {
                "cagr_pct_points": abs(float(cagr * 100.0) - float(wcagr * 100.0)),
                "mdd_pct_points": abs(float(mdd * 100.0) - float(wmdd * 100.0)),
            }
            winner_curve_match = (
                winner_curve_diff["cagr_pct_points"] <= tol_pct_points
                and winner_curve_diff["mdd_pct_points"] <= tol_pct_points
            )

    status = "PASS" if ((cagr_abs_diff <= tol_pct_points and mdd_abs_diff <= tol_pct_points) or winner_curve_match) else "FAIL"

    payload = {
        "task_id": "T-029",
        "step": "11_reconcile_metrics",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "tolerance_pct_points": tol_pct_points,
        "operational_metrics": {
            "equity_final": float(curve["equity"].iloc[-1]) if not curve.empty else 0.0,
            "cagr_pct": float(cagr * 100.0),
            "mdd_pct": float(mdd * 100.0),
            "sharpe": float(sharpe),
            "days": int(len(curve)),
        },
        "winner_reference_holdout": {
            "cagr_pct": cagr_ref,
            "mdd_pct": mdd_ref,
        },
        "winner_reference_curve_metrics": winner_curve_metrics,
        "diffs_abs": {
            "cagr_pct_points": float(cagr_abs_diff),
            "mdd_pct_points": float(mdd_abs_diff),
        },
        "diffs_vs_winner_curve_abs": winner_curve_diff,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload
