"""Step 10 — estende curva operacional do winner.

Sempre recalcula os últimos RECALC_WINDOW dias da curva (D-037),
corrigindo automaticamente linhas gravadas com dados incompletos
(ex.: pipeline rodou antes da ingestão diária).
"""
from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RECALC_WINDOW = 5


def _load_decision_safe(target_date: date) -> dict | None:
    """Carrega decisão para um pregão.

    Tenta primeiro a data exata do pregão, depois os 3 dias civis seguintes
    (exec_day pode ser D+1 civil, ex: pregão sexta → decisão sábado/segunda).
    """
    from datetime import timedelta
    for offset in range(4):
        d = target_date + timedelta(days=offset)
        p = ROOT / "data" / "daily" / f"decision_{d}.json"
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    return None


def _portfolio_return(
    opw: pd.DataFrame,
    day_prev: pd.Timestamp,
    day_now: pd.Timestamp,
    weights: dict[str, float],
) -> float:
    if not weights:
        return 0.0
    rows_prev = opw[opw["date"] == day_prev][["ticker", "close_raw"]].drop_duplicates("ticker")
    rows_now = opw[opw["date"] == day_now][["ticker", "close_raw"]].drop_duplicates("ticker")
    px_prev = rows_prev.set_index("ticker")["close_raw"].to_dict()
    px_now = rows_now.set_index("ticker")["close_raw"].to_dict()
    rets = []
    for t, w in weights.items():
        p0 = float(px_prev.get(t, np.nan))
        p1 = float(px_now.get(t, np.nan))
        if np.isfinite(p0) and np.isfinite(p1) and p0 > 0:
            rets.append(float(w) * ((p1 / p0) - 1.0))
    return float(sum(rets)) if rets else 0.0


def _resolve_trading_day(
    trading_days: list, ref: pd.Timestamp, direction: str = "le",
) -> pd.Timestamp | None:
    if direction == "le":
        cands = [d for d in trading_days if d <= ref]
        return cands[-1] if cands else None
    cands = [d for d in trading_days if d >= ref]
    return cands[0] if cands else None


def run(target_date: date | None = None) -> pd.DataFrame:
    if target_date is None:
        raise ValueError("target_date é obrigatório para estender curva.")
    target_ts = pd.Timestamp(target_date).normalize()

    winner_curve_path = ROOT / "backtest" / "results" / "curve_C4_K10.csv"
    opw_path = ROOT / "data" / "ssot" / "operational_window.parquet"
    out_path = ROOT / "data" / "daily" / "winner_curve_us.parquet"
    if not winner_curve_path.exists():
        raise FileNotFoundError(f"Input ausente: {winner_curve_path}")
    if not opw_path.exists():
        raise FileNotFoundError(f"Input ausente: {opw_path}")

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

    opw = pd.read_parquet(opw_path, columns=["date", "ticker", "close_raw"]).copy()
    opw["date"] = pd.to_datetime(opw["date"], errors="coerce").dt.normalize()
    opw["ticker"] = opw["ticker"].astype(str).str.upper().str.strip()
    opw["close_raw"] = pd.to_numeric(opw["close_raw"], errors="coerce")
    opw = opw.dropna(subset=["date", "ticker", "close_raw"])
    trading_days = sorted(opw["date"].unique())

    if "ret_1d" not in curve.columns:
        curve["ret_1d"] = np.nan
    if "source" not in curve.columns:
        curve["source"] = "backtest_seed"
    if "generated_at" not in curve.columns:
        curve["generated_at"] = pd.NaT

    dates_to_compute = _dates_to_recalc(curve, target_ts, RECALC_WINDOW, trading_days)
    if not dates_to_compute:
        td_set = set(trading_days)
        spurious = curve[~curve["date"].isin(td_set)]
        if not spurious.empty:
            curve = curve[curve["date"].isin(td_set)].copy()
            curve = curve.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
            base_eq = float(curve["equity"].iloc[0]) if float(curve["equity"].iloc[0]) > 0 else 1.0
            curve["equity_base100"] = (curve["equity"].astype(float) / base_eq) * 100.0
            out_path.parent.mkdir(parents=True, exist_ok=True)
            curve.to_parquet(out_path, index=False)
        return curve

    td_set = set(trading_days)
    curve = curve[~curve["date"].isin(dates_to_compute)].copy()
    curve = curve[curve["date"].isin(td_set) | (curve["source"] == "backtest_seed")].copy()

    for dt in sorted(dates_to_compute):
        curve = curve.sort_values("date").reset_index(drop=True)
        if curve.empty:
            continue

        prev_curve_dt = pd.Timestamp(curve["date"].max()).normalize()
        prev_td = _resolve_trading_day(trading_days, prev_curve_dt, "le")
        now_td = _resolve_trading_day(trading_days, dt, "le")

        decision = _load_decision_safe(target_date=dt.date() if hasattr(dt, "date") else dt)
        if decision is None:
            ret = 0.0
        else:
            weights = {
                str(k).upper().strip(): float(v)
                for k, v in decision.get("target_weights", {}).items()
                if float(v) > 0
            }
            if not weights or prev_td is None or now_td is None or prev_td == now_td:
                ret = 0.0
            else:
                ret = _portfolio_return(opw, day_prev=prev_td, day_now=now_td, weights=weights)

        last_equity = float(curve["equity"].iloc[-1])
        equity_new = float(last_equity * (1.0 + ret))
        new_row = pd.DataFrame(
            [
                {
                    "date": dt,
                    "equity": equity_new,
                    "ret_1d": ret,
                    "source": "pipeline_step10",
                    "generated_at": datetime.now(tz=UTC).isoformat(),
                }
            ]
        )
        curve = pd.concat([curve, new_row], ignore_index=True)

    curve = curve.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    base_eq = float(curve["equity"].iloc[0]) if float(curve["equity"].iloc[0]) > 0 else 1.0
    curve["equity_base100"] = (curve["equity"].astype(float) / base_eq) * 100.0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    curve.to_parquet(out_path, index=False)
    return curve


def _dates_to_recalc(
    curve: pd.DataFrame,
    target_ts: pd.Timestamp,
    window: int,
    trading_days: list[pd.Timestamp],
) -> list[pd.Timestamp]:
    """Retorna as datas que precisam de (re)cálculo.

    Todas as datas retornadas são pregões reais (presentes em trading_days).
    Inclui o pregão real <= target_ts (se ainda não existe na curva) e até
    `window` datas mais recentes já presentes na curva para corrigir dados
    incompletos (D-037).  Datas na curva que não são pregões reais são
    descartadas (D-038).
    """
    td_set = set(trading_days)
    existing = sorted(d for d in curve["date"].unique() if d in td_set)
    recalc = [d for d in existing if d <= target_ts][-window:]

    resolved = _resolve_trading_day(trading_days, target_ts, "le")
    if resolved is not None and resolved not in set(existing):
        recalc.append(resolved)
    return sorted(set(recalc))
# test
