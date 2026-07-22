"""Backtest T-SDC-DEFENSIVE-REINVEST-POLICY-US-V1.

Estudo read-only pre-registrado (R-041 / R-048), com liquidacao D0:
- Zero-A: C4 puro sem venda defensiva adicional.
- Zero-B: venda defensiva apenas por SPC ativo; reinveste no Top-20 congelado.
- A1: venda por SPC ou cruzamento MEDIO->GRAVE; reinveste no Top-20 congelado.
- A2: mesmo gatilho do A1; completa existentes ate limite e compra Top-20 diario.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import backtest.run_backtest_variants_us as rb  # noqa: E402
import backtest.t_band_exp_entry_us_v1.run_t_band_exp_entry_us_v1 as prev  # noqa: E402
import backtest.t_bandexp_r037_materiality_entry_us_v1.run_t_bandexp_r037_materiality_entry_us_v1 as prevr037  # noqa: E402

TASK_ID = "T-SDC-DEFENSIVE-REINVEST-POLICY-US-V1"
ARMS = ["Zero-A", "Zero-B", "A1", "A2"]
PAIR_DEFS = [
    ("Zero-A", "Zero-B"),
    ("Zero-A", "A1"),
    ("Zero-A", "A2"),
    ("A1", "A2"),
]

DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2"
IN_CANONICAL = DATASET_DIR / "canonical_us.parquet"
IN_MANIFEST = DATASET_DIR / "manifest.json"
IN_WINNER = ROOT / "config" / "winner_us.json"
IN_BLACKLIST = ROOT / "data" / "ssot" / "blacklist_us.json"
IN_DECISION_CRITERION = (
    ROOT
    / "backtest"
    / "t_defensive_reinvest_policy_us_v1"
    / "decision_criterion_defensive_reinvest_policy_us_v1.json"
)
OUT_DIR = ROOT / "backtest" / "t_defensive_reinvest_policy_us_v1" / "results"

MEDIO_THRESHOLD_US = -12.85
GRAVE_THRESHOLD_US = -32.42


def _is_finite(v: Any) -> bool:
    return bool(np.isfinite(v))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        out = float(v)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _load_winner_snapshot_full(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"winner_us.json nao encontrado: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    snap = payload.get("winner_config_snapshot", payload)
    return {
        "top_n": int(snap.get("top_n", 20)),
        "rebalance_cadence": int(snap.get("rebalance_cadence", 10)),
        "rebalance_anchor_date": str(snap.get("rebalance_anchor_date", "2026-04-16")),
        "buffer_k": int(snap.get("buffer_k", 10)),
        "k_damp": float(snap.get("k_damp", 0.0)),
        "max_weight_cap": float(snap.get("max_weight_cap", 0.06)),
        "min_market_cap": float(snap.get("min_market_cap", 300_000_000.0)),
        "friction_one_way_bps": float(snap.get("friction_one_way_bps", 2.5)),
        "base_capital": float(snap.get("base_capital", 100_000.0)),
    }


def _normalize_scores_for_day(
    scores_day: pd.DataFrame | None,
    eligible: set[str],
    blacklist: set[str],
) -> pd.DataFrame:
    if scores_day is None or scores_day.empty:
        return pd.DataFrame()
    view = scores_day.copy()
    if "m3_rank" not in view.columns:
        return pd.DataFrame()
    view.index = view.index.astype(str).str.upper().str.strip()
    view["m3_rank"] = pd.to_numeric(view["m3_rank"], errors="coerce")
    view = view.dropna(subset=["m3_rank"])
    if "score_m3" in view.columns:
        view["score_m3"] = pd.to_numeric(view["score_m3"], errors="coerce")
    else:
        view["score_m3"] = np.nan
    if "ret_62" in view.columns:
        view["ret_62"] = pd.to_numeric(view["ret_62"], errors="coerce")
    else:
        view["ret_62"] = np.nan
    if eligible:
        view = view[view.index.isin(eligible)]
    if blacklist:
        view = view[~view.index.isin(blacklist)]
    return view.sort_values(["m3_rank", "score_m3"], ascending=[True, False])


def _ticker_shares(lots: list[rb.Lot], ticker: str) -> int:
    return int(sum(int(l.shares) for l in lots if l.ticker == ticker))


def _ticker_avg_cost(lots: list[rb.Lot], ticker: str) -> float:
    shares = 0
    invested = 0.0
    for lot in lots:
        if lot.ticker != ticker or lot.shares <= 0:
            continue
        shares += int(lot.shares)
        invested += float(lot.shares) * float(lot.buy_price)
    if shares <= 0:
        return 0.0
    return float(invested / shares)


def _heat_pct(close_price: float, avg_cost: float) -> float:
    if avg_cost <= 0 or close_price <= 0:
        return float("nan")
    return float((close_price / avg_cost - 1.0) * 100.0)


def _heat_class_us(heat: float) -> str:
    if not _is_finite(heat):
        return "N/A"
    if heat >= 0.0:
        return "GANHO"
    if heat > MEDIO_THRESHOLD_US:
        return "LEVE"
    if heat > GRAVE_THRESHOLD_US:
        return "MEDIO"
    return "GRAVE"


def _cross_medium_to_grave(heat_prev2: float, heat_prev1: float) -> bool:
    return bool(
        _heat_class_us(heat_prev2) == "MEDIO"
        and _heat_class_us(heat_prev1) == "GRAVE"
    )


def _consume_same_day_settlement(
    current_day: pd.Timestamp,
    pending_cash: dict[pd.Timestamp, float],
) -> float:
    return float(pending_cash.pop(current_day, 0.0))


def _buy_ticker_for_value(
    *,
    ticker: str,
    desired_value: float,
    lots: list[rb.Lot],
    price_row: pd.Series,
    cash_free: float,
    friction: float,
    exec_day: pd.Timestamp,
) -> tuple[list[rb.Lot], float, float, float, int]:
    if desired_value <= 0 or cash_free <= 0:
        return lots, cash_free, 0.0, 0.0, 0
    px = _safe_float(price_row.get(ticker, np.nan), np.nan)
    if not _is_finite(px) or px <= 0:
        return lots, cash_free, 0.0, 0.0, 0

    max_afford = float(cash_free / (1.0 + friction))
    gross_target = float(min(desired_value, max_afford))
    if gross_target <= 0:
        return lots, cash_free, 0.0, 0.0, 0

    shares_to_buy = int(gross_target // px)
    if shares_to_buy <= 0:
        return lots, cash_free, 0.0, 0.0, 0

    gross = float(shares_to_buy * px)
    cost = float(gross * friction)
    outflow = float(gross + cost)
    if outflow > cash_free + 1e-12:
        return lots, cash_free, 0.0, 0.0, 0

    new_lots = lots[:]
    new_lots.append(
        rb.Lot(
            ticker=str(ticker).upper().strip(),
            buy_date=exec_day,
            shares=int(shares_to_buy),
            buy_price=float(px),
        )
    )
    return new_lots, float(cash_free - outflow), gross, cost, int(shares_to_buy)


def _rebalance_to_target(
    *,
    target: list[str],
    target_weights: dict[str, float],
    lots: list[rb.Lot],
    cash_free: float,
    pending_cash: dict[pd.Timestamp, float],
    price_row: pd.Series,
    trading_days: list[pd.Timestamp],
    day_idx: int,
    exec_day: pd.Timestamp,
    friction: float,
    settlement_days: int,
    blocked: set[str],
) -> tuple[list[rb.Lot], float, dict[pd.Timestamp, float], float, float, float, int]:
    gross_sells = 0.0
    gross_buys = 0.0
    trade_cost = 0.0
    defensive_sales = 0

    held = set(rb.split_lots_by_ticker(lots).keys())
    target_set = set(target)
    to_sell = sorted([tk for tk in held if tk not in target_set])
    for tk in to_sell:
        lots, proceeds, cost, sold_shares = rb.sell_all_ticker(
            ticker=tk,
            lots=lots,
            price_row=price_row,
            friction=friction,
            trading_dates=trading_days,
            i=day_idx,
            settlement_days=settlement_days,
            pending_cash=pending_cash,
        )
        if sold_shares <= 0:
            continue
        trade_cost += float(cost)
        gross_sells += float(proceeds + cost)
        if settlement_days == 0:
            cash_free += _consume_same_day_settlement(exec_day, pending_cash)

    if target:
        weight_map = {tk: float(target_weights.get(tk, 0.0)) for tk in target}
        total_w = float(sum(max(0.0, w) for w in weight_map.values()))
        if total_w <= 0.0:
            eq = 1.0 / float(len(target))
            weight_map = {tk: eq for tk in target}
        else:
            weight_map = {tk: float(max(0.0, w) / total_w) for tk, w in weight_map.items()}

        equity_now = float(
            cash_free + sum(float(v) for v in pending_cash.values()) + rb.lots_market_value(lots, price_row)
        )
        for tk in target:
            if tk in blocked:
                continue
            current_val = rb.ticker_value(lots, tk, price_row)
            desired_val = max(0.0, equity_now * float(weight_map.get(tk, 0.0)) - current_val)
            if desired_val <= 0:
                continue
            lots, cash_free, gross, cost, bought = _buy_ticker_for_value(
                ticker=tk,
                desired_value=desired_val,
                lots=lots,
                price_row=price_row,
                cash_free=cash_free,
                friction=friction,
                exec_day=exec_day,
            )
            if bought <= 0:
                continue
            gross_buys += gross
            trade_cost += cost

    return lots, cash_free, pending_cash, gross_sells, gross_buys, trade_cost, defensive_sales


def _reinvest_frozen_policy(
    *,
    frozen_target: list[str],
    frozen_weights: dict[str, float],
    lots: list[rb.Lot],
    cash_free: float,
    price_row: pd.Series,
    friction: float,
    exec_day: pd.Timestamp,
    blocked: set[str],
) -> tuple[list[rb.Lot], float, float, float]:
    gross_buys = 0.0
    trade_cost = 0.0
    if cash_free <= 0 or not frozen_target:
        return lots, cash_free, gross_buys, trade_cost

    eligible = [tk for tk in frozen_target if tk not in blocked]
    if not eligible:
        return lots, cash_free, gross_buys, trade_cost

    weights = {tk: float(frozen_weights.get(tk, 0.0)) for tk in eligible}
    total_w = float(sum(max(0.0, w) for w in weights.values()))
    if total_w <= 0.0:
        eq = 1.0 / float(len(eligible))
        weights = {tk: eq for tk in eligible}
    else:
        weights = {tk: float(max(0.0, w) / total_w) for tk, w in weights.items()}

    equity_now = float(cash_free + rb.lots_market_value(lots, price_row))
    deficits: list[tuple[str, float]] = []
    for tk in eligible:
        current_val = rb.ticker_value(lots, tk, price_row)
        desired_val = max(0.0, equity_now * float(weights[tk]) - current_val)
        if desired_val > 0:
            deficits.append((tk, desired_val))

    deficits.sort(key=lambda x: x[1], reverse=True)
    for tk, desired_val in deficits:
        lots, cash_free, gross, cost, bought = _buy_ticker_for_value(
            ticker=tk,
            desired_value=desired_val,
            lots=lots,
            price_row=price_row,
            cash_free=cash_free,
            friction=friction,
            exec_day=exec_day,
        )
        if bought <= 0:
            continue
        gross_buys += gross
        trade_cost += cost
        if cash_free <= 0:
            break

    return lots, cash_free, gross_buys, trade_cost


def _reinvest_a2_policy(
    *,
    top_n: int,
    max_weight_cap: float,
    prev_scores: pd.DataFrame,
    lots: list[rb.Lot],
    cash_free: float,
    price_row: pd.Series,
    friction: float,
    exec_day: pd.Timestamp,
    blocked: set[str],
) -> tuple[list[rb.Lot], float, float, float]:
    gross_buys = 0.0
    trade_cost = 0.0
    if cash_free <= 0:
        return lots, cash_free, gross_buys, trade_cost
    if prev_scores.empty:
        return lots, cash_free, gross_buys, trade_cost

    limit_weight = float(max_weight_cap if max_weight_cap > 0 else (1.0 / max(top_n, 1)))
    limit_weight = float(min(max(limit_weight, 0.0), 1.0))
    if limit_weight <= 0:
        return lots, cash_free, gross_buys, trade_cost

    equity_now = float(cash_free + rb.lots_market_value(lots, price_row))
    if equity_now <= 0:
        return lots, cash_free, gross_buys, trade_cost

    held = set(rb.split_lots_by_ticker(lots).keys())
    rank_map = pd.to_numeric(prev_scores.get("m3_rank"), errors="coerce").to_dict()
    held_sorted = sorted(
        list(held),
        key=lambda tk: float(rank_map.get(tk, np.inf)),
    )

    for tk in held_sorted:
        if tk in blocked:
            continue
        current_val = rb.ticker_value(lots, tk, price_row)
        desired_val = max(0.0, equity_now * limit_weight - current_val)
        if desired_val <= 0:
            continue
        lots, cash_free, gross, cost, bought = _buy_ticker_for_value(
            ticker=tk,
            desired_value=desired_val,
            lots=lots,
            price_row=price_row,
            cash_free=cash_free,
            friction=friction,
            exec_day=exec_day,
        )
        if bought <= 0:
            continue
        gross_buys += gross
        trade_cost += cost
        if cash_free <= 0:
            return lots, cash_free, gross_buys, trade_cost

    daily_top20 = rb._select_top_n(prev_scores, top_n=top_n, quarantine=set())
    for tk in daily_top20:
        if tk in blocked:
            continue
        held_now = set(rb.split_lots_by_ticker(lots).keys())
        if tk not in held_now and len(held_now) >= top_n:
            continue
        current_val = rb.ticker_value(lots, tk, price_row)
        desired_val = max(0.0, equity_now * limit_weight - current_val)
        if desired_val <= 0:
            continue
        lots, cash_free, gross, cost, bought = _buy_ticker_for_value(
            ticker=tk,
            desired_value=desired_val,
            lots=lots,
            price_row=price_row,
            cash_free=cash_free,
            friction=friction,
            exec_day=exec_day,
        )
        if bought <= 0:
            continue
        gross_buys += gross
        trade_cost += cost
        if cash_free <= 0:
            break

    return lots, cash_free, gross_buys, trade_cost


def _simulate_arm(
    *,
    arm_name: str,
    trading_days: list[pd.Timestamp],
    day_to_idx: dict[pd.Timestamp, int],
    px_exec_wide: pd.DataFrame,
    cash_log_daily: pd.Series,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    mc_eligible_by_day: dict[pd.Timestamp, set[str]],
    spc_blocked_by_day: dict[pd.Timestamp, set[str]],
    cfg: dict[str, Any],
    blacklist: set[str],
    holdout_end: pd.Timestamp,
) -> pd.DataFrame:
    top_n = int(cfg["top_n"])
    cadence = int(cfg["rebalance_cadence"])
    buffer_k = int(cfg["buffer_k"])
    k_damp = float(cfg["k_damp"])
    max_weight_cap = float(cfg["max_weight_cap"])
    friction = float(cfg["friction_one_way_bps"]) / 10_000.0

    lots: list[rb.Lot] = []
    pending_cash: dict[pd.Timestamp, float] = {}
    cash_free = float(cfg["base_capital"])
    defensive_blocked: set[str] = set()

    frozen_target: list[str] = []
    frozen_weights: dict[str, float] = {}

    rows: list[dict[str, Any]] = []
    prev_equity: float | None = None
    cycle_id = -1

    anchor_date = pd.Timestamp(cfg["rebalance_anchor_date"]).normalize()
    anchor_idx = day_to_idx.get(anchor_date)
    if anchor_idx is None:
        pos = int(
            np.searchsorted(
                np.array(trading_days, dtype="datetime64[ns]"),
                np.datetime64(anchor_date),
            )
        )
        anchor_idx = 0 if pos >= len(trading_days) else pos

    for i, d in enumerate(trading_days):
        matured = float(pending_cash.pop(d, 0.0))
        if matured > 0:
            cash_free += matured

        price_row = px_exec_wide.loc[d]
        d_prev = trading_days[i - 1] if i > 0 else None
        d_prev2 = trading_days[i - 2] if i > 1 else None

        gross_sells_today = 0.0
        gross_buys_today = 0.0
        costs_today = 0.0
        defensive_sells_today = 0
        trigger_spc_today = 0
        trigger_medio_grave_today = 0

        is_rebalance_day = bool(i >= anchor_idx and ((i - anchor_idx) % max(cadence, 1) == 0))
        if is_rebalance_day:
            cycle_id += 1
            defensive_blocked = set()
            if d_prev is not None:
                prev_scores = _normalize_scores_for_day(
                    scores_day=scores_by_day.get(d_prev),
                    eligible=mc_eligible_by_day.get(d_prev, set()),
                    blacklist=blacklist,
                )
                held = set(rb.split_lots_by_ticker(lots).keys())
                if not prev_scores.empty:
                    frozen_target = rb._select_c2_target(
                        prev_scores,
                        holdings=held,
                        top_n=top_n,
                        buffer_k=buffer_k,
                        quarantine=set(),
                    )
                    frozen_weights = rb.compute_target_weights(
                        prev_scores,
                        frozen_target,
                        k_damp=k_damp,
                        max_weight_cap=max_weight_cap,
                    )
                elif held:
                    frozen_target = sorted(list(held))[:top_n]
                    eq = 1.0 / float(max(len(frozen_target), 1))
                    frozen_weights = {tk: eq for tk in frozen_target}

            lots, cash_free, pending_cash, gross_s, gross_b, cost_r, _ = _rebalance_to_target(
                target=frozen_target,
                target_weights=frozen_weights,
                lots=lots,
                cash_free=cash_free,
                pending_cash=pending_cash,
                price_row=price_row,
                trading_days=trading_days,
                day_idx=i,
                exec_day=d,
                friction=friction,
                settlement_days=0,
                blocked=set(),
            )
            gross_sells_today += gross_s
            gross_buys_today += gross_b
            costs_today += cost_r

        if arm_name != "Zero-A" and d_prev is not None:
            held_now = sorted(set(rb.split_lots_by_ticker(lots).keys()))
            spc_set = set(spc_blocked_by_day.get(d_prev, set()))

            trigger_tickers: dict[str, set[str]] = {}
            for tk in held_now:
                if tk in defensive_blocked:
                    continue
                reasons: set[str] = set()
                if tk in spc_set:
                    reasons.add("SPC")
                if arm_name in {"A1", "A2"} and d_prev2 is not None:
                    avg_cost = _ticker_avg_cost(lots, tk)
                    close_prev = _safe_float(px_exec_wide.at[d_prev, tk], np.nan) if tk in px_exec_wide.columns else np.nan
                    close_prev2 = _safe_float(px_exec_wide.at[d_prev2, tk], np.nan) if tk in px_exec_wide.columns else np.nan
                    heat_prev = _heat_pct(close_prev, avg_cost)
                    heat_prev2 = _heat_pct(close_prev2, avg_cost)
                    if _cross_medium_to_grave(heat_prev2, heat_prev):
                        reasons.add("MEDIO->GRAVE")
                if reasons:
                    trigger_tickers[tk] = reasons

            for tk, reasons in trigger_tickers.items():
                lots, proceeds, cost, sold_shares = rb.sell_all_ticker(
                    ticker=tk,
                    lots=lots,
                    price_row=price_row,
                    friction=friction,
                    trading_dates=trading_days,
                    i=i,
                    settlement_days=0,
                    pending_cash=pending_cash,
                )
                if sold_shares <= 0:
                    continue
                cash_free += _consume_same_day_settlement(d, pending_cash)
                defensive_blocked.add(tk)
                defensive_sells_today += 1
                if "SPC" in reasons:
                    trigger_spc_today += 1
                if "MEDIO->GRAVE" in reasons:
                    trigger_medio_grave_today += 1
                costs_today += float(cost)
                gross_sells_today += float(proceeds + cost)

            if defensive_sells_today > 0 or cash_free > 0:
                prev_scores_reinvest = _normalize_scores_for_day(
                    scores_day=scores_by_day.get(d_prev),
                    eligible=mc_eligible_by_day.get(d_prev, set()),
                    blacklist=blacklist,
                )
                if arm_name in {"Zero-B", "A1"}:
                    lots, cash_free, gross_b, cost_b = _reinvest_frozen_policy(
                        frozen_target=frozen_target,
                        frozen_weights=frozen_weights,
                        lots=lots,
                        cash_free=cash_free,
                        price_row=price_row,
                        friction=friction,
                        exec_day=d,
                        blocked=defensive_blocked,
                    )
                    gross_buys_today += gross_b
                    costs_today += cost_b
                elif arm_name == "A2":
                    lots, cash_free, gross_b, cost_b = _reinvest_a2_policy(
                        top_n=top_n,
                        max_weight_cap=max_weight_cap,
                        prev_scores=prev_scores_reinvest,
                        lots=lots,
                        cash_free=cash_free,
                        price_row=price_row,
                        friction=friction,
                        exec_day=d,
                        blocked=defensive_blocked,
                    )
                    gross_buys_today += gross_b
                    costs_today += cost_b

        cash_log = float(cash_log_daily.get(d, 0.0))
        cash_ret = float(np.expm1(cash_log))
        if cash_free > 0:
            cash_free *= (1.0 + cash_ret)

        holdings_value = rb.lots_market_value(lots, price_row)
        by_ticker = rb.split_lots_by_ticker(lots)
        n_tickers = int(len(by_ticker))
        equity_end = float(cash_free + sum(float(v) for v in pending_cash.values()) + holdings_value)

        if prev_equity is not None and prev_equity > 0 and equity_end > 0:
            log_ret_equity = float(np.log(equity_end / prev_equity))
        else:
            log_ret_equity = float("nan")
        prev_equity = equity_end

        turnover_gross_pct = (
            float((gross_sells_today + gross_buys_today) / equity_end)
            if equity_end > 0
            else float("nan")
        )
        defensive_sell_rate = float(defensive_sells_today / max(top_n, 1))
        cash_idle_gt5 = bool(equity_end > 0 and (cash_free / equity_end) > 0.05)

        split = prev._to_split(d, holdout_end=holdout_end)
        if split == "OTHER":
            continue

        rows.append(
            {
                "date": d,
                "arm": arm_name,
                "split": split,
                "is_holdout": int(split in {"HOLDOUT", "SW1", "SW2"}),
                "cycle_id": int(cycle_id),
                "equity": equity_end,
                "cash_free": float(cash_free),
                "cash_pending": float(sum(float(v) for v in pending_cash.values())),
                "holdings_value": float(holdings_value),
                "n_tickers": n_tickers,
                "log_ret_equity": log_ret_equity,
                "turnover_gross_pct": turnover_gross_pct,
                "defensive_sells_today": int(defensive_sells_today),
                "defensive_sell_rate": defensive_sell_rate,
                "trigger_spc_today": int(trigger_spc_today),
                "trigger_medio_grave_today": int(trigger_medio_grave_today),
                "cash_idle_gt5": int(cash_idle_gt5),
                "trade_cost_today": float(costs_today),
                "gross_sells_today": float(gross_sells_today),
                "gross_buys_today": float(gross_buys_today),
                "is_rebalance_day": int(is_rebalance_day),
            }
        )

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    return out


def _subset_df(df: pd.DataFrame, subset: str) -> pd.DataFrame:
    if subset == "TRAIN":
        return df[df["split"] == "TRAIN"].copy()
    if subset == "HOLDOUT":
        return df[df["is_holdout"] == 1].copy()
    if subset in {"SW1", "SW2"}:
        return df[df["split"] == subset].copy()
    raise ValueError(f"Subset desconhecido: {subset}")


def _nanmean(values: pd.Series | np.ndarray) -> float:
    if isinstance(values, np.ndarray):
        arr = pd.to_numeric(pd.Series(values), errors="coerce").to_numpy(dtype=float)
    else:
        arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if arr.size == 0 or np.isnan(arr).all():
        return float("nan")
    return float(np.nanmean(arr))


def _summarize_split_arm(curve: pd.DataFrame, subset: str, arm: str) -> dict[str, Any]:
    sub = _subset_df(curve[curve["arm"] == arm].copy(), subset=subset)
    rets = pd.to_numeric(sub["log_ret_equity"], errors="coerce").dropna()
    cvar5 = prev._cvar(rets.to_numpy(dtype=float), 0.05) if len(rets) > 0 else float("nan")
    sharpe = (
        prev._portfolio_sharpe(rets, pd.Series(np.ones(len(rets), dtype=float)))
        if len(rets) > 1
        else float("nan")
    )
    n_cycles = int(sub["cycle_id"].nunique()) if not sub.empty else 0
    defensive_total = int(pd.to_numeric(sub["defensive_sells_today"], errors="coerce").fillna(0).sum())
    defensive_per_cycle = float(defensive_total / n_cycles) if n_cycles > 0 else float("nan")
    cash_idle_days_pct = float(_nanmean(sub["cash_idle_gt5"]) * 100.0) if not sub.empty else float("nan")
    avg_tickers = float(_nanmean(sub["n_tickers"])) if not sub.empty else float("nan")
    turnover_mean_pct = (
        float(_nanmean(sub["turnover_gross_pct"]) * 100.0)
        if not sub.empty
        else float("nan")
    )
    mean_veto_rate = float(_nanmean(sub["defensive_sell_rate"])) if not sub.empty else float("nan")

    return {
        "arm": arm,
        "split": subset,
        "n_days": int(len(sub)),
        "n_cycles": n_cycles,
        "equity_final": float(sub["equity"].iloc[-1]) if len(sub) > 0 else float("nan"),
        "mean_cvar5": cvar5,
        "mean_sharpe_cost_adj": sharpe,
        "mean_veto_rate": mean_veto_rate,
        "avg_tickers": avg_tickers,
        "cash_idle_days_pct": cash_idle_days_pct,
        "turnover_mean_pct": turnover_mean_pct,
        "defensive_sells_total": defensive_total,
        "defensive_sells_per_cycle": defensive_per_cycle,
    }


def _paired_frame(curves: pd.DataFrame, baseline: str, arm: str) -> pd.DataFrame:
    b = curves[curves["arm"] == baseline][["date", "split", "is_holdout", "log_ret_equity"]].copy()
    a = curves[curves["arm"] == arm][["date", "log_ret_equity", "defensive_sell_rate"]].copy()
    b = b.rename(columns={"log_ret_equity": "ret_base"})
    a = a.rename(columns={"log_ret_equity": "ret_arm", "defensive_sell_rate": "veto_rate_arm"})
    m = b.merge(a, on="date", how="inner")
    m["ret_base"] = pd.to_numeric(m["ret_base"], errors="coerce")
    m["ret_arm"] = pd.to_numeric(m["ret_arm"], errors="coerce")
    m["veto_rate_arm"] = pd.to_numeric(m["veto_rate_arm"], errors="coerce")
    m = m.dropna(subset=["ret_base", "ret_arm"])
    return m.sort_values("date").reset_index(drop=True)


def _bootstrap_pack(arr: np.ndarray) -> dict[str, Any]:
    arr = np.asarray(arr, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return {
            "delta_mean": float("nan"),
            "ic95": [float("nan"), float("nan")],
            "mass_same_sign_pct": float("nan"),
        }
    mean = float(np.nanmean(arr))
    lo = float(np.nanpercentile(arr, 2.5))
    hi = float(np.nanpercentile(arr, 97.5))
    if mean > 0:
        mass = float(np.mean(arr > 0) * 100.0)
    elif mean < 0:
        mass = float(np.mean(arr < 0) * 100.0)
    else:
        mass = 50.0
    return {"delta_mean": mean, "ic95": [lo, hi], "mass_same_sign_pct": mass}


def _bootstrap_pair_stats(
    pair_df: pd.DataFrame,
    subset: str,
    n_resamples: int,
    seed: int,
) -> dict[str, Any]:
    sub = _subset_df(pair_df, subset=subset)
    if sub.empty:
        return {
            "subset": subset,
            "n_dates": 0,
            "n_pairs": 0,
            "delta_cvar5": _bootstrap_pack(np.array([], dtype=float)),
            "delta_sharpe_cost_adj": _bootstrap_pack(np.array([], dtype=float)),
            "mean_veto_rate_arm": float("nan"),
        }

    rng = np.random.default_rng(seed)
    dates = np.array(sorted(sub["date"].dropna().unique()))
    by_day = {d: sub[sub["date"] == d].copy() for d in dates}

    deltas_cvar: list[float] = []
    deltas_sharpe: list[float] = []
    veto_rates: list[float] = []

    for _ in range(n_resamples):
        sampled_days = rng.choice(dates, size=len(dates), replace=True)
        boot = pd.concat([by_day[d] for d in sampled_days], ignore_index=True)
        base_ret = pd.to_numeric(boot["ret_base"], errors="coerce").to_numpy(dtype=float)
        arm_ret = pd.to_numeric(boot["ret_arm"], errors="coerce").to_numpy(dtype=float)
        if base_ret.size == 0 or arm_ret.size == 0:
            continue

        cvar_base = prev._cvar(base_ret, 0.05)
        cvar_arm = prev._cvar(arm_ret, 0.05)
        if _is_finite(cvar_base) and _is_finite(cvar_arm):
            deltas_cvar.append(float(cvar_arm - cvar_base))

        s_base = prev._portfolio_sharpe(pd.Series(base_ret), pd.Series(np.ones(len(base_ret), dtype=float)))
        s_arm = prev._portfolio_sharpe(pd.Series(arm_ret), pd.Series(np.ones(len(arm_ret), dtype=float)))
        if _is_finite(s_base) and _is_finite(s_arm):
            deltas_sharpe.append(float(s_arm - s_base))

        veto_rates.append(float(_nanmean(boot["veto_rate_arm"])))

    return {
        "subset": subset,
        "n_dates": int(len(dates)),
        "n_pairs": int(len(sub)),
        "delta_cvar5": _bootstrap_pack(np.asarray(deltas_cvar, dtype=float)),
        "delta_sharpe_cost_adj": _bootstrap_pack(np.asarray(deltas_sharpe, dtype=float)),
        "mean_veto_rate_arm": float(_nanmean(np.asarray(veto_rates, dtype=float))),
    }


def _favorable_ic(ic95: list[float]) -> bool:
    if len(ic95) != 2:
        return False
    lo, hi = ic95
    return bool(_is_finite(lo) and _is_finite(hi) and lo > 0.0 and hi > 0.0)


def _delta_favorable(v: float) -> bool:
    return bool(_is_finite(v) and v > 0.0)


def _sanitize_label(name: str) -> str:
    out = "".join(ch if ch.isalnum() else "_" for ch in str(name).upper()).strip("_")
    while "__" in out:
        out = out.replace("__", "_")
    return out or "ARM"


def main() -> None:
    if not IN_DECISION_CRITERION.exists():
        raise RuntimeError(f"Criterio pre-registrado nao encontrado: {IN_DECISION_CRITERION}")
    with IN_DECISION_CRITERION.open("r", encoding="utf-8") as fp:
        decision_criterion = json.load(fp)

    holdout_end, manifest = prev._load_holdout_end_from_manifest(IN_MANIFEST)
    if bool(decision_criterion.get("dataset", {}).get("required_hash_verification", False)):
        prevr037._verify_manifest_hashes(manifest, DATASET_DIR)
        print("Hashes conferidos com sucesso contra manifest.json.")

    cfg = _load_winner_snapshot_full(IN_WINNER)
    top_n = int(cfg["top_n"])
    cadence = int(cfg["rebalance_cadence"])
    min_market_cap = float(cfg["min_market_cap"])

    required_cols = [
        "ticker",
        "date",
        "close_operational",
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
    ]
    canonical = pd.read_parquet(IN_CANONICAL, columns=required_cols).copy()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_operational"]).copy()
    canonical = canonical[canonical["date"] <= holdout_end].copy()

    blacklist = prev._load_blacklist(IN_BLACKLIST)
    if blacklist:
        canonical = canonical[~canonical["ticker"].isin(blacklist)].copy()
    print(f"Tickers excluidos por blacklist: {len(blacklist)}")

    spc_blocked_by_day = prev._build_spc_blocked_by_day(canonical)
    print(f"SPC blocked map carregado para {len(spc_blocked_by_day)} pregoes.")

    canonical["market_cap"] = pd.to_numeric(canonical["market_cap"], errors="coerce")
    mc_eligible_by_day: dict[pd.Timestamp, set[str]] = {}
    for _dt, _grp in canonical.groupby("date"):
        mc_eligible_by_day[_dt] = set(_grp.loc[_grp["market_cap"] >= min_market_cap, "ticker"].dropna())

    px_exec_wide = (
        canonical.pivot_table(
            index="date",
            columns="ticker",
            values="close_operational",
            aggfunc="first",
        )
        .sort_index()
        .ffill()
    )
    trading_days = list(px_exec_wide.index)
    if not trading_days:
        raise RuntimeError("Nenhum pregao encontrado no canonical filtrado.")
    day_to_idx = {d: i for i, d in enumerate(trading_days)}

    scores_by_day = prev._compute_scores_by_day(px_exec_wide, holdout_end=holdout_end)
    print(f"Scores computados para {len(scores_by_day)} pregoes.")

    macro = pd.read_parquet(DATASET_DIR / "macro_us.parquet").copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date", "fed_funds_rate"]).sort_values("date")
    cash_log_daily = rb.build_cash_log_daily(macro)

    curves: list[pd.DataFrame] = []
    for arm in ARMS:
        print(f"Executando arm {arm} ...")
        arm_curve = _simulate_arm(
            arm_name=arm,
            trading_days=trading_days,
            day_to_idx=day_to_idx,
            px_exec_wide=px_exec_wide,
            cash_log_daily=cash_log_daily,
            scores_by_day=scores_by_day,
            mc_eligible_by_day=mc_eligible_by_day,
            spc_blocked_by_day=spc_blocked_by_day,
            cfg=cfg,
            blacklist=blacklist,
            holdout_end=holdout_end,
        )
        if arm_curve.empty:
            raise RuntimeError(f"Nenhuma observacao gerada para arm={arm}")
        curves.append(arm_curve)

    curves_df = pd.concat(curves, ignore_index=True)
    curves_df = curves_df.sort_values(["arm", "date"]).reset_index(drop=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    curves_df.to_csv(OUT_DIR / "observations_defensive_reinvest_policy_us_v1.csv", index=False)

    summary_by_split: dict[str, list[dict[str, Any]]] = {}
    summary_lookup: dict[str, dict[str, dict[str, Any]]] = {}
    for subset in ("TRAIN", "HOLDOUT", "SW1", "SW2"):
        rows = [_summarize_split_arm(curves_df, subset=subset, arm=arm) for arm in ARMS]
        frame = pd.DataFrame(rows)
        frame.to_csv(OUT_DIR / f"summary_{subset}_defensive_reinvest_policy_us_v1.csv", index=False)
        summary_by_split[subset] = rows
        summary_lookup[subset] = {str(r["arm"]): r for r in rows}

    bcfg = decision_criterion.get("bootstrap", {})
    n_resamples = int(bcfg.get("n_resamples", 2000))
    seed = int(bcfg.get("seed", 42))
    max_veto_rate = float(decision_criterion.get("max_veto_rate", 0.35))

    bootstrap_payload: dict[str, Any] = {
        "task_id": TASK_ID,
        "method": "cluster por dia",
        "n_resamples": n_resamples,
        "seed": seed,
        "pairs": {},
    }
    verdict_pairs: dict[str, Any] = {}

    for baseline, arm in PAIR_DEFS:
        pair_key = f"{baseline}_vs_{arm}"
        pair_df = _paired_frame(curves_df, baseline=baseline, arm=arm)
        bs_holdout = _bootstrap_pair_stats(pair_df, "HOLDOUT", n_resamples=n_resamples, seed=seed)
        bs_sw1 = _bootstrap_pair_stats(pair_df, "SW1", n_resamples=n_resamples, seed=seed)
        bs_sw2 = _bootstrap_pair_stats(pair_df, "SW2", n_resamples=n_resamples, seed=seed)

        bootstrap_payload["pairs"][pair_key] = {
            "baseline": baseline,
            "arm": arm,
            "splits": {
                "HOLDOUT": bs_holdout,
                "SW1": bs_sw1,
                "SW2": bs_sw2,
            },
        }

        b_hold = summary_lookup["HOLDOUT"][baseline]
        a_hold = summary_lookup["HOLDOUT"][arm]
        b_sw1 = summary_lookup["SW1"][baseline]
        a_sw1 = summary_lookup["SW1"][arm]
        b_sw2 = summary_lookup["SW2"][baseline]
        a_sw2 = summary_lookup["SW2"][arm]

        delta_hold_cvar5 = float(a_hold["mean_cvar5"] - b_hold["mean_cvar5"])
        delta_hold_sharpe = float(a_hold["mean_sharpe_cost_adj"] - b_hold["mean_sharpe_cost_adj"])
        delta_sw1_cvar5 = float(a_sw1["mean_cvar5"] - b_sw1["mean_cvar5"])
        delta_sw1_sharpe = float(a_sw1["mean_sharpe_cost_adj"] - b_sw1["mean_sharpe_cost_adj"])
        delta_sw2_cvar5 = float(a_sw2["mean_cvar5"] - b_sw2["mean_cvar5"])
        delta_sw2_sharpe = float(a_sw2["mean_sharpe_cost_adj"] - b_sw2["mean_sharpe_cost_adj"])

        veto_hold_ok = bool(_is_finite(a_hold["mean_veto_rate"]) and a_hold["mean_veto_rate"] <= max_veto_rate)
        veto_sw1_ok = bool(_is_finite(a_sw1["mean_veto_rate"]) and a_sw1["mean_veto_rate"] <= max_veto_rate)
        veto_sw2_ok = bool(_is_finite(a_sw2["mean_veto_rate"]) and a_sw2["mean_veto_rate"] <= max_veto_rate)

        domina_forte = bool(
            _favorable_ic(bs_holdout["delta_cvar5"]["ic95"])
            and _favorable_ic(bs_holdout["delta_sharpe_cost_adj"]["ic95"])
            and _favorable_ic(bs_sw1["delta_cvar5"]["ic95"])
            and _favorable_ic(bs_sw1["delta_sharpe_cost_adj"]["ic95"])
            and _favorable_ic(bs_sw2["delta_cvar5"]["ic95"])
            and _favorable_ic(bs_sw2["delta_sharpe_cost_adj"]["ic95"])
            and veto_hold_ok
            and veto_sw1_ok
            and veto_sw2_ok
        )
        holdout_mass_ok = bool(
            _is_finite(bs_holdout["delta_cvar5"]["mass_same_sign_pct"])
            and _is_finite(bs_holdout["delta_sharpe_cost_adj"]["mass_same_sign_pct"])
            and bs_holdout["delta_cvar5"]["mass_same_sign_pct"] >= 90.0
            and bs_holdout["delta_sharpe_cost_adj"]["mass_same_sign_pct"] >= 90.0
        )
        direction_ok = bool(
            _delta_favorable(delta_hold_cvar5)
            and _delta_favorable(delta_hold_sharpe)
            and _delta_favorable(delta_sw1_cvar5)
            and _delta_favorable(delta_sw1_sharpe)
            and _delta_favorable(delta_sw2_cvar5)
            and _delta_favorable(delta_sw2_sharpe)
        )
        materiality_ok = bool(
            (abs(delta_hold_sharpe) >= 0.30) or (abs(delta_hold_cvar5) >= 0.02)
        )
        favorecido = bool(
            (not domina_forte)
            and holdout_mass_ok
            and direction_ok
            and materiality_ok
            and veto_hold_ok
        )
        if domina_forte:
            verdict = "DOMINA_FORTE"
        elif favorecido:
            verdict = f"FAVORECIDO_{_sanitize_label(arm)}"
        else:
            verdict = "INCONCLUSIVO"

        verdict_pairs[pair_key] = {
            "baseline": baseline,
            "arm": arm,
            "final_verdict": verdict,
            "deltas": {
                "HOLDOUT": {
                    "delta_cvar5": delta_hold_cvar5,
                    "delta_sharpe_cost_adj": delta_hold_sharpe,
                    "mean_veto_rate_arm": a_hold["mean_veto_rate"],
                },
                "SW1": {
                    "delta_cvar5": delta_sw1_cvar5,
                    "delta_sharpe_cost_adj": delta_sw1_sharpe,
                    "mean_veto_rate_arm": a_sw1["mean_veto_rate"],
                },
                "SW2": {
                    "delta_cvar5": delta_sw2_cvar5,
                    "delta_sharpe_cost_adj": delta_sw2_sharpe,
                    "mean_veto_rate_arm": a_sw2["mean_veto_rate"],
                },
            },
            "bootstrap": {
                "HOLDOUT": bs_holdout,
                "SW1": bs_sw1,
                "SW2": bs_sw2,
            },
            "checks": {
                "domina_forte_conditions": {
                    "holdout_ic_cvar5_favoravel": _favorable_ic(bs_holdout["delta_cvar5"]["ic95"]),
                    "holdout_ic_sharpe_favoravel": _favorable_ic(bs_holdout["delta_sharpe_cost_adj"]["ic95"]),
                    "sw1_ic_cvar5_favoravel": _favorable_ic(bs_sw1["delta_cvar5"]["ic95"]),
                    "sw1_ic_sharpe_favoravel": _favorable_ic(bs_sw1["delta_sharpe_cost_adj"]["ic95"]),
                    "sw2_ic_cvar5_favoravel": _favorable_ic(bs_sw2["delta_cvar5"]["ic95"]),
                    "sw2_ic_sharpe_favoravel": _favorable_ic(bs_sw2["delta_sharpe_cost_adj"]["ic95"]),
                    "veto_holdout_ok": veto_hold_ok,
                    "veto_sw1_ok": veto_sw1_ok,
                    "veto_sw2_ok": veto_sw2_ok,
                },
                "favorecido_conditions": {
                    "holdout_mass_ok": holdout_mass_ok,
                    "direction_ok_holdout_sw1_sw2": direction_ok,
                    "materiality_ok": materiality_ok,
                    "veto_holdout_ok": veto_hold_ok,
                },
            },
        }

    with (OUT_DIR / "bootstrap_stats_defensive_reinvest_policy_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(bootstrap_payload, fp, ensure_ascii=False, indent=2)

    verdict_payload = {
        "task_id": TASK_ID,
        "criteria_file": str(IN_DECISION_CRITERION.relative_to(ROOT)),
        "dataset_manifest": str(IN_MANIFEST.relative_to(ROOT)),
        "freeze_asof": str(manifest.get("freeze_asof")),
        "arms": ARMS,
        "pairs": verdict_pairs,
        "summary_by_split": summary_by_split,
        "thresholds": {
            "max_veto_rate": max_veto_rate,
            "materiality_sharpe_abs_min": 0.30,
            "materiality_cvar5_abs_min": 0.02,
            "bootstrap_mass_min_pct": 90.0,
            "medium_threshold_us": MEDIO_THRESHOLD_US,
            "grave_threshold_us": GRAVE_THRESHOLD_US,
        },
    }
    with (OUT_DIR / "verdict_defensive_reinvest_policy_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(verdict_payload, fp, ensure_ascii=False, indent=2)

    print(f"{TASK_ID} concluido.")
    print(f"freeze_asof={manifest.get('freeze_asof')}")
    print(f"top_n={top_n} cadence={cadence}")
    print(f"rows_observations={len(curves_df)}")
    for subset in ("TRAIN", "HOLDOUT", "SW1", "SW2"):
        print(f"rows_summary_{subset}={len(summary_by_split[subset])}")
    for pair_key, payload in verdict_pairs.items():
        print(f"{pair_key}={payload['final_verdict']}")


if __name__ == "__main__":
    main()
