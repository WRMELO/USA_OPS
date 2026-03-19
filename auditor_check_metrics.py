import json
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path("/home/wilson/USA_OPS")
WINNER_JSON = ROOT / "config" / "winner_us.json"
CURVE_CSV = ROOT / "backtest" / "results" / "curve_C4_K10.csv"

def recalculate_metrics():
    if not CURVE_CSV.exists():
        print("FAIL: Curve CSV not found.")
        return

    df = pd.read_csv(CURVE_CSV)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    
    # Filter to holdout period
    winner_cfg = json.loads(WINNER_JSON.read_text())
    holdout_start = pd.Timestamp(winner_cfg["holdout_period"]["start"]).normalize()
    holdout_end = pd.Timestamp(winner_cfg["holdout_period"]["end"]).normalize()
    
    df_holdout = df[(df["date"] >= holdout_start) & (df["date"] <= holdout_end)].copy()
    
    # Metrics
    eq = df_holdout["equity"].values
    n_days = len(eq)
    n_years = max(n_days / 252.0, 1.0 / 252.0)
    
    cagr = (eq[-1] / eq[0]) ** (1.0 / n_years) - 1.0
    
    running_max = np.maximum.accumulate(eq)
    dd = (eq / running_max) - 1.0
    mdd = np.min(dd)
    
    # defensive days pct
    def_days = df_holdout["regime_defensive_used"].mean()
    
    # max concentration
    max_conc = df_holdout["max_concentration"].max()
    
    # cost
    cost_start = df_holdout["cost_total_cum"].iloc[0]
    cost_end = df_holdout["cost_total_cum"].iloc[-1]
    cost_total = cost_end - cost_start
    
    # compare
    rep_metrics = winner_cfg["holdout_metrics"]
    print("=== METRICS RECALCULATION ===")
    print(f"CAGR Recalc: {cagr*100:.4f}% | Reported: {rep_metrics['cagr_pct']:.4f}%")
    print(f"MDD Recalc: {mdd*100:.4f}% | Reported: {rep_metrics['mdd_pct']:.4f}%")
    print(f"Max Conc Recalc: {max_conc*100:.4f}% | Reported: {rep_metrics['max_concentration_pct']:.4f}%")
    print(f"Defensive Days Recalc: {def_days*100:.4f}% | Reported: {rep_metrics['defensive_days_pct']:.4f}%")
    # Note: cost_total_cum in holdout reported might be the absolute value or relative.
    print(f"Cost Holdout Recalc: {cost_total:.2f} | Reported: {rep_metrics['cost_total']:.2f}")

if __name__ == "__main__":
    recalculate_metrics()
