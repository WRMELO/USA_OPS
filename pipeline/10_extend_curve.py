"""Step 10 — estende curva operacional do winner."""
from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _load_last_decision(target_date: date) -> dict:
    p = ROOT / "data" / "daily" / f"decision_{target_date}.json"
    if not p.exists():
        raise FileNotFoundError(f"Decision file ausente: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _portfolio_return(canonical: pd.DataFrame, day_prev: pd.Timestamp, day_now: pd.Timestamp, weights: dict[str, float]) -> float:
    if not weights:
        return 0.0
    rows_prev = canonical[canonical["date"] == day_prev][["ticker", "close_raw"]].drop_duplicates("ticker")
    rows_now = canonical[canonical["date"] == day_now][["ticker", "close_raw"]].drop_duplicates("ticker")
    px_prev = rows_prev.set_index("ticker")["close_raw"].to_dict()
    px_now = rows_now.set_index("ticker")["close_raw"].to_dict()
    rets = []
    for t, w in weights.items():
        p0 = float(px_prev.get(t, np.nan))
        p1 = float(px_now.get(t, np.nan))
        if np.isfinite(p0) and np.isfinite(p1) and p0 > 0:
            rets.append(float(w) * ((p1 / p0) - 1.0))
    return float(sum(rets)) if rets else 0.0


def run(target_date: date | None = None) -> pd.DataFrame:
    if target_date is None:
        raise ValueError("target_date é obrigatório para estender curva.")
    target_ts = pd.Timestamp(target_date).normalize()

    winner_curve_path = ROOT / "backtest" / "results" / "curve_C4_K10.csv"
    canonical_path = ROOT / "data" / "ssot" / "canonical_us.parquet"
    out_path = ROOT / "data" / "daily" / "winner_curve_us.parquet"
    if not winner_curve_path.exists():
        raise FileNotFoundError(f"Input ausente: {winner_curve_path}")
    if not canonical_path.exists():
        raise FileNotFoundError(f"Input ausente: {canonical_path}")

    base = pd.read_csv(winner_curve_path)
    base["date"] = pd.to_datetime(base["date"], errors="coerce").dt.normalize()
    base = base.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    base = base[["date", "equity"]].copy()

    if out_path.exists():
        curve = pd.read_parquet(out_path).copy()
        curve["date"] = pd.to_datetime(curve["date"], errors="coerce").dt.normalize()
        curve = curve.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    else:
        curve = base.copy()

    if (curve["date"] == target_ts).any():
        return curve

    last_dt = pd.Timestamp(curve["date"].max()).normalize()
    if target_ts <= last_dt:
        return curve

    canonical = pd.read_parquet(canonical_path, columns=["date", "ticker", "close_raw"]).copy()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["close_raw"] = pd.to_numeric(canonical["close_raw"], errors="coerce")
    canonical = canonical.dropna(subset=["date", "ticker", "close_raw"])

    decision = _load_last_decision(target_date=target_date)
    weights = {str(k).upper().strip(): float(v) for k, v in decision.get("target_weights", {}).items() if float(v) > 0}
    if not weights:
        ret = 0.0
    else:
        ret = _portfolio_return(canonical=canonical, day_prev=last_dt, day_now=target_ts, weights=weights)

    last_equity = float(curve["equity"].iloc[-1])
    equity_new = float(last_equity * (1.0 + ret))
    new_row = pd.DataFrame(
        [
            {
                "date": target_ts,
                "equity": equity_new,
                "ret_1d": ret,
                "source": "pipeline_step10",
                "generated_at": datetime.now(tz=UTC).isoformat(),
            }
        ]
    )
    if "ret_1d" not in curve.columns:
        curve["ret_1d"] = np.nan
    if "source" not in curve.columns:
        curve["source"] = "backtest_seed"
    if "generated_at" not in curve.columns:
        curve["generated_at"] = pd.NaT

    curve = pd.concat([curve, new_row], ignore_index=True)
    curve = curve.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    base_eq = float(curve["equity"].iloc[0]) if float(curve["equity"].iloc[0]) > 0 else 1.0
    curve["equity_base100"] = (curve["equity"].astype(float) / base_eq) * 100.0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    curve.to_parquet(out_path, index=False)
    return curve
