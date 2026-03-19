"""Step 09 — decisão diária C4 puro."""
from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "daily"


def _load_winner() -> dict:
    p = ROOT / "config" / "winner_us.json"
    if not p.exists():
        raise FileNotFoundError(f"Input ausente: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _select_c2_target(scores_day: pd.DataFrame, holdings: set[str], top_n: int, buffer_k: int) -> list[str]:
    ranked = scores_day.sort_values(["m3_rank", "ticker"]).copy()
    top = ranked.head(top_n)["ticker"].astype(str).tolist()
    rank_map = ranked.set_index("ticker")["m3_rank"].to_dict()
    kept = sorted([t for t in holdings if float(rank_map.get(t, np.inf)) <= float(buffer_k)])
    target = kept[:]
    for t in top:
        if t not in target:
            target.append(t)
        if len(target) >= top_n:
            break
    return target[:top_n]


def _compute_defensive_actions(canonical_day: pd.DataFrame, holdings: list[str]) -> list[dict]:
    if canonical_day.empty or not holdings:
        return []
    subset = canonical_day[canonical_day["ticker"].isin(holdings)].copy()
    actions: list[dict] = []
    for _, row in subset.iterrows():
        iv = float(pd.to_numeric(row.get("i_value"), errors="coerce"))
        iu = float(pd.to_numeric(row.get("i_ucl"), errors="coerce"))
        il = float(pd.to_numeric(row.get("i_lcl"), errors="coerce"))
        mv = float(pd.to_numeric(row.get("mr_value"), errors="coerce"))
        mu = float(pd.to_numeric(row.get("mr_ucl"), errors="coerce"))
        xv = float(pd.to_numeric(row.get("xbar_value"), errors="coerce"))
        xu = float(pd.to_numeric(row.get("xbar_ucl"), errors="coerce"))
        xl = float(pd.to_numeric(row.get("xbar_lcl"), errors="coerce"))
        rv = float(pd.to_numeric(row.get("r_value"), errors="coerce"))
        ru = float(pd.to_numeric(row.get("r_ucl"), errors="coerce"))

        any_rule = bool(
            (np.isfinite(iv) and np.isfinite(iu) and iv > iu)
            or (np.isfinite(iv) and np.isfinite(il) and iv < il)
            or (np.isfinite(mv) and np.isfinite(mu) and mv > mu)
            or (np.isfinite(xv) and np.isfinite(xu) and xv > xu)
            or (np.isfinite(xv) and np.isfinite(xl) and xv < xl)
            or (np.isfinite(rv) and np.isfinite(ru) and rv > ru)
        )
        if not any_rule:
            continue
        score = 4
        sell_pct = 0.25
        if (np.isfinite(iv) and np.isfinite(il) and iv < il) or (np.isfinite(mv) and np.isfinite(mu) and mv > mu):
            score = 5
            sell_pct = 0.50
        if np.isfinite(iv) and np.isfinite(il) and iv < (il - abs(il) * 0.2):
            score = 6
            sell_pct = 1.0
        actions.append(
            {
                "ticker": str(row["ticker"]),
                "score": int(score),
                "sell_pct": float(sell_pct),
            }
        )
    return sorted(actions, key=lambda x: (-x["score"], x["ticker"]))[:5]


def run(target_date: date | None = None) -> dict:
    winner = _load_winner()
    cfg = winner["winner_config_snapshot"]
    top_n = int(cfg["top_n"])
    buffer_k = int(cfg["buffer_k"])
    cadence = int(cfg["rebalance_cadence"])
    max_weight_cap = float(cfg["max_weight_cap"])
    min_market_cap = float(cfg["min_market_cap"])

    scores_path = ROOT / "data" / "features" / "scores_m3_us.parquet"
    canonical_path = ROOT / "data" / "ssot" / "canonical_us.parquet"
    if not scores_path.exists() or not canonical_path.exists():
        raise FileNotFoundError("Inputs ausentes para decisão (scores/canonical).")

    scores = pd.read_parquet(scores_path, columns=["date", "ticker", "m3_rank", "score_m3"]).copy()
    scores["date"] = pd.to_datetime(scores["date"], errors="coerce").dt.normalize()
    scores["ticker"] = scores["ticker"].astype(str).str.upper().str.strip()
    scores["m3_rank"] = pd.to_numeric(scores["m3_rank"], errors="coerce")
    scores = scores.dropna(subset=["date", "ticker", "m3_rank"]).drop_duplicates(subset=["date", "ticker"], keep="last")

    canonical = pd.read_parquet(
        canonical_path,
        columns=[
            "date",
            "ticker",
            "market_cap",
            "i_value",
            "i_ucl",
            "i_lcl",
            "mr_value",
            "mr_ucl",
            "xbar_value",
            "xbar_ucl",
            "xbar_lcl",
            "r_value",
            "r_ucl",
        ],
    ).copy()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["market_cap"] = pd.to_numeric(canonical["market_cap"], errors="coerce")
    canonical = canonical.dropna(subset=["date", "ticker"])

    target_dt = pd.Timestamp(target_date).normalize() if target_date else pd.Timestamp(scores["date"].max()).normalize()
    prev_dt = pd.Timestamp(scores[scores["date"] < target_dt]["date"].max()).normalize()
    if pd.isna(prev_dt):
        raise RuntimeError("Sem data D-1 de score para decidir.")

    day_scores = scores[scores["date"] == prev_dt].copy()
    day_caps = canonical[canonical["date"] == prev_dt][["ticker", "market_cap"]].drop_duplicates("ticker")
    day_scores = day_scores.merge(day_caps, on="ticker", how="left")
    day_scores = day_scores[day_scores["market_cap"] >= min_market_cap].copy()
    day_scores = day_scores.sort_values(["m3_rank", "ticker"]).reset_index(drop=True)
    if day_scores.empty:
        raise RuntimeError(f"Sem tickers elegíveis por min_market_cap em {prev_dt.date()}.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    prev_decisions = sorted(OUT_DIR.glob("decision_*.json"))
    previous_holdings: list[str] = []
    if prev_decisions:
        last_payload = json.loads(prev_decisions[-1].read_text(encoding="utf-8"))
        previous_holdings = [str(x).upper().strip() for x in last_payload.get("selected_tickers", []) if str(x).strip()]

    trading_days = sorted(scores["date"].dropna().unique().tolist())
    idx_map = {pd.Timestamp(d).normalize(): i for i, d in enumerate(trading_days)}
    day_idx = int(idx_map.get(prev_dt, 0))
    is_rebalance_day = (day_idx % max(cadence, 1)) == 0

    if is_rebalance_day:
        selected = _select_c2_target(day_scores, set(previous_holdings), top_n=top_n, buffer_k=buffer_k)
    else:
        selected = sorted(list(set(previous_holdings)))[:top_n]
        if not selected:
            selected = day_scores.head(top_n)["ticker"].astype(str).tolist()

    if not selected:
        selected = day_scores.head(top_n)["ticker"].astype(str).tolist()

    # C4 com k_damp=0 no winner atual => pesos iguais com cap como guardrail.
    eq_w = 1.0 / float(max(len(selected), 1))
    weights = {t: float(min(eq_w, max_weight_cap)) for t in selected}
    s = float(sum(weights.values()))
    if s > 0:
        weights = {k: float(v / s) for k, v in weights.items()}

    defensive_actions = _compute_defensive_actions(
        canonical_day=canonical[canonical["date"] == prev_dt].copy(),
        holdings=selected,
    )
    action = "REBALANCE" if is_rebalance_day else "HOLD"
    if defensive_actions:
        action = "DEFENSIVE_SELL"

    payload = {
        "task_id": "T-029",
        "step": "09_decide",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "target_date": str(target_dt.date()),
        "scores_reference_date_d_minus_1": str(prev_dt.date()),
        "winner_config_snapshot": cfg,
        "action": action,
        "is_rebalance_day": bool(is_rebalance_day),
        "selected_tickers": selected,
        "target_weights": weights,
        "portfolio": [{"ticker": t, "target_weight": float(weights[t])} for t in selected],
        "defensive_actions": defensive_actions,
        "selected_count": int(len(selected)),
    }
    out_path = OUT_DIR / f"decision_{target_dt.date()}.json"
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload
