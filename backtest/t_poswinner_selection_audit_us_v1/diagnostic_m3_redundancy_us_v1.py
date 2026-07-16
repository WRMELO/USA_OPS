"""Diagnostic: cross-sectional redundancy between z_m0 and z_ret.

Read-only study script for T-SDC-POSWINNER-SELECTION-AUDIT-US-V1.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.run_backtest_variants_us import load_inputs  # noqa: E402

TASK_ID = "T-SDC-POSWINNER-SELECTION-AUDIT-US-V1"
MIN_TICKERS_PER_DAY = 30
IN_CRITERION = (
    ROOT
    / "backtest"
    / "t_poswinner_selection_audit_us_v1"
    / "decision_criterion_poswinner_selection_audit_us_v1.json"
)
IN_MANIFEST = ROOT / "backtest" / "research_dataset_us" / "manifest.json"
OUT_DIR = ROOT / "backtest" / "t_poswinner_selection_audit_us_v1" / "results"
OUT_JSON = OUT_DIR / "diagnostic_z_m0_z_ret_correlation.json"


def _load_json(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"arquivo nao encontrado: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_stat(values: pd.Series, fn: str) -> float:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return float("nan")
    if fn == "mean":
        return float(np.mean(arr))
    if fn == "median":
        return float(np.median(arr))
    if fn == "p10":
        return float(np.quantile(arr, 0.10))
    if fn == "p90":
        return float(np.quantile(arr, 0.90))
    raise ValueError(f"fn desconhecida: {fn}")


def main() -> None:
    criterion = _load_json(IN_CRITERION)
    manifest = _load_json(IN_MANIFEST)

    _, _, scores = load_inputs()
    required_cols = {"date", "ticker", "z_m0", "z_ret"}
    missing = sorted(required_cols - set(scores.columns))
    if missing:
        raise RuntimeError(f"scores sem colunas obrigatorias: {missing}")

    scores = scores.copy()
    scores["date"] = pd.to_datetime(scores["date"], errors="coerce").dt.normalize()
    scores["ticker"] = scores["ticker"].astype(str).str.upper().str.strip()
    scores["z_m0"] = pd.to_numeric(scores["z_m0"], errors="coerce")
    scores["z_ret"] = pd.to_numeric(scores["z_ret"], errors="coerce")
    scores = scores.dropna(subset=["date", "ticker", "z_m0", "z_ret"])

    rows: list[dict[str, float | int | str]] = []
    for day, g in scores.groupby("date", sort=True):
        g2 = g[["z_m0", "z_ret"]].dropna()
        n_tickers = int(len(g2))
        if n_tickers < MIN_TICKERS_PER_DAY:
            continue
        corr = float(g2["z_m0"].corr(g2["z_ret"], method="pearson"))
        if not np.isfinite(corr):
            continue
        rows.append(
            {
                "date": pd.Timestamp(day).date().isoformat(),
                "n_tickers": n_tickers,
                "corr_z_m0_z_ret": corr,
            }
        )

    daily_df = pd.DataFrame(rows)
    if daily_df.empty:
        raise RuntimeError("nenhum dia elegivel para diagnostico de correlacao")

    corr_series = pd.to_numeric(daily_df["corr_z_m0_z_ret"], errors="coerce")
    corr_arr = corr_series.to_numpy(dtype=float)
    corr_arr = corr_arr[np.isfinite(corr_arr)]
    frac_ge_095 = float(np.mean(corr_arr >= 0.95)) if corr_arr.size else float("nan")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "task_id": TASK_ID,
        "criterion_file": str(IN_CRITERION.relative_to(ROOT)),
        "dataset": {
            "name": "research_dataset_us",
            "freeze_asof_manifest": manifest.get("freeze_asof"),
            "loader_env_var": "US_RESEARCH_DATASET_DIR",
        },
        "guardrails": {
            "min_tickers_per_day": MIN_TICKERS_PER_DAY,
            "registered_before_execution": bool(criterion.get("registered_before_execution", False)),
        },
        "summary": {
            "n_days": int(len(daily_df)),
            "date_min": str(daily_df["date"].min()),
            "date_max": str(daily_df["date"].max()),
            "mean_corr": _safe_stat(corr_series, "mean"),
            "median_corr": _safe_stat(corr_series, "median"),
            "p10_corr": _safe_stat(corr_series, "p10"),
            "p90_corr": _safe_stat(corr_series, "p90"),
            "frac_days_corr_ge_095": frac_ge_095,
        },
        "daily_correlation_series": daily_df.to_dict(orient="records"),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {OUT_JSON.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
