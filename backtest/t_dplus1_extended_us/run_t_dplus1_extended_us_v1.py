"""T-DPLUS1-EXTENDED-US-V1.

Experimento offline/read-only para comparar:
- ARM_V0: metodo atual operacional (A1D no D+1; ranking vivo em D+2..D+9).
- ARM_V1: lista travada do rebalance estendida por todo o intervalo inter-rebalance.

O ARM_0 usa o motor produtivo C4 por construcao para diagnosticar paridade de baseline.
"""

from __future__ import annotations

from pathlib import Path
import json
import sys
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.run_backtest_variants_us import (  # noqa: E402
    Lot,
    _apply_split_adjustment,
    _band_from_z,
    _persist_points,
    _select_c2_target,
    _to_bool,
    apply_min_market_cap_filter,
    build_cash_log_daily,
    build_market_cap_wide,
    build_scores_by_day,
    compute_target_weights,
    load_blacklist,
    load_inputs,
    lots_market_value,
    run_variant,
    split_lots_by_ticker,
    ticker_value,
)
from backtest.t_exec_completion_us.run_t_exec_completion_us_v2 import (  # noqa: E402
    HOLDOUT_END,
    HOLDOUT_START,
    IN_BLACKLIST,
    IN_WINNER,
    OUT_DIR as _V2_OUT_DIR,
    SPC_COLS,
    SW_RECENT_START,
    _compute_metrics_dual_window,
    _load_winner_cfg,
    _pending_total,
    _prepare_wides,
    _sell_all_ticker_source,
    _sell_ticker_fifo_source,
    _spc_instavel,
)

OUT_DIR = ROOT / "backtest" / "t_dplus1_extended_us" / "results"

# Pre-registro: criterios fixos antes da execucao (nao alterar pos-run).
VERDICT_CRITERIA_V1: dict[str, Any] = {
    "holdout_start": "2023-01-02",
    "holdout_end": "2026-03-16",
    "sw_recent_start": "2024-07-01",
    "sw_recent_end": "2026-03-16",
    "primary_comparison": "ARM_V1 - ARM_V0",
    "delta_cagr_threshold_holdout_pct": 1.0,
    "delta_mdd_threshold_holdout_pct": -5.0,
    "delta_cagr_threshold_recent_pct": 0.0,
    "delta_mdd_threshold_recent_pct": -7.5,
    "verdict_MELHORA_V1": "V1 supera V0 em HOLDOUT e SW_RECENT sem degradacao severa de MDD",
    "verdict_MELHORA_V0": "V0 supera V1 em HOLDOUT e SW_RECENT sem degradacao severa de MDD",
    "verdict_NEUTRO": "|delta_cagr_holdout(V1-V0)| <= 1.0 sem degradacao severa de MDD",
    "verdict_INCONCLUSIVO": "demais casos",
    "_note": "Criterios pre-registrados antes da execucao — NAO ALTERAR pos-run",
}


def _empty_stats() -> dict[str, int]:
    return {
        "n_inter_rebalance_buy_days": 0,
        "n_skipped_r001": 0,
        "n_live_ranking_buys": 0,
        "n_locked_buys": 0,
        "n_defensive_sell_25": 0,
        "n_defensive_sell_50": 0,
        "n_defensive_sell_100": 0,
        "n_quarantine_entries": 0,
    }


def _buy_to_target(
    ticker: str,
    target_value: float,
    lots: list[Lot],
    price_row: pd.Series,
    cash_free: float,
    total_cost: float,
    friction: float,
    buy_date: pd.Timestamp,
) -> tuple[list[Lot], float, float, bool]:
    """Compra ticker ate target_value, limitado por cash_free."""
    px = float(price_row.get(ticker, np.nan))
    if (not np.isfinite(px)) or px <= 0 or cash_free <= 0:
        return lots, cash_free, total_cost, False
    current_val = ticker_value(lots, ticker, price_row)
    desired_val = max(0.0, float(target_value) - current_val)
    if desired_val <= 0:
        return lots, cash_free, total_cost, False
    max_afford = cash_free / (1.0 + friction)
    buy_val = min(desired_val, max_afford)
    if buy_val <= 0:
        return lots, cash_free, total_cost, False
    shares_to_buy = int(buy_val // px)
    if shares_to_buy <= 0:
        return lots, cash_free, total_cost, False
    gross = shares_to_buy * px
    cost = gross * friction
    outflow = gross + cost
    if outflow > cash_free + 1e-12:
        return lots, cash_free, total_cost, False
    cash_free -= outflow
    total_cost += cost
    lots.append(Lot(ticker=ticker, buy_date=buy_date, shares=shares_to_buy, buy_price=px))
    return lots, cash_free, total_cost, True


def _locked_queue(
    locked_list: list[str],
    weights: dict[str, float],
    lots: list[Lot],
    price_row: pd.Series,
    equity_now: float,
) -> list[str]:
    pending: list[str] = []
    rest: list[str] = []
    for tk in locked_list:
        wt = float(weights.get(tk, 0.0))
        if wt <= 0:
            continue
        target_val = equity_now * wt
        current_val = ticker_value(lots, tk, price_row)
        if target_val - current_val > 1e-9:
            pending.append(tk)
        else:
            rest.append(tk)
    rest = sorted(rest, key=lambda t: float(weights.get(t, 0.0)), reverse=True)
    return list(dict.fromkeys(pending + rest))


def _live_ranking_queue(
    scores_day: pd.DataFrame | None,
    held: set[str],
    quarantine: set[str],
    top_n: int,
) -> list[str]:
    if scores_day is None or scores_day.empty:
        return []
    ranked = scores_day.sort_values(["m3_rank", "ticker"] if "ticker" in scores_day.columns else ["m3_rank"])
    out: list[str] = []
    for tk in ranked.index.astype(str).tolist():
        if tk in held or tk in quarantine:
            continue
        out.append(tk)
        if len(out) >= top_n:
            break
    return out


def _classify_global_verdict(delta: dict[str, float]) -> str:
    cagr_h = float(delta["delta_cagr_holdout_v1_minus_v0"])
    mdd_h = float(delta["delta_mdd_holdout_v1_minus_v0"])
    cagr_r = float(delta["delta_cagr_recent_v1_minus_v0"])
    mdd_r = float(delta["delta_mdd_recent_v1_minus_v0"])

    v1_pass = (
        cagr_h > float(VERDICT_CRITERIA_V1["delta_cagr_threshold_holdout_pct"])
        and mdd_h > float(VERDICT_CRITERIA_V1["delta_mdd_threshold_holdout_pct"])
        and cagr_r > float(VERDICT_CRITERIA_V1["delta_cagr_threshold_recent_pct"])
        and mdd_r > float(VERDICT_CRITERIA_V1["delta_mdd_threshold_recent_pct"])
    )
    v0_pass = (
        -cagr_h > float(VERDICT_CRITERIA_V1["delta_cagr_threshold_holdout_pct"])
        and -mdd_h > float(VERDICT_CRITERIA_V1["delta_mdd_threshold_holdout_pct"])
        and -cagr_r > float(VERDICT_CRITERIA_V1["delta_cagr_threshold_recent_pct"])
        and -mdd_r > float(VERDICT_CRITERIA_V1["delta_mdd_threshold_recent_pct"])
    )
    neutral = (
        abs(cagr_h) <= float(VERDICT_CRITERIA_V1["delta_cagr_threshold_holdout_pct"])
        and mdd_h > float(VERDICT_CRITERIA_V1["delta_mdd_threshold_holdout_pct"])
        and -mdd_h > float(VERDICT_CRITERIA_V1["delta_mdd_threshold_holdout_pct"])
    )
    if v1_pass:
        return "MELHORA_V1"
    if v0_pass:
        return "MELHORA_V0"
    if neutral:
        return "NEUTRO"
    return "INCONCLUSIVO"


def run_arm_extended(
    arm_name: str,
    policy: str,
    px_exec_wide: pd.DataFrame,
    split_event_wide: pd.DataFrame,
    i_wide: pd.DataFrame,
    z_wide: pd.DataFrame,
    any_rule_wide: pd.DataFrame,
    strong_rule_wide: pd.DataFrame,
    spc_wide: dict[str, pd.DataFrame],
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    cash_log_daily: pd.Series,
    cfg: Any,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if policy not in {"V0_current", "V1_locked_extended"}:
        raise ValueError(f"policy invalida: {policy}")

    friction = cfg.friction_one_way_bps / 10_000.0
    rebalance_cadence = max(int(cfg.rebalance_cadence), 1)
    trading_dates = list(px_exec_wide.index.intersection(cash_log_daily.index).sort_values())
    if len(trading_dates) < 30:
        raise RuntimeError("Poucas datas de interseccao para simular.")

    cash_free = float(cfg.base_capital)
    pending_cash: dict[pd.Timestamp, dict[str, float]] = {}
    lots: list[Lot] = []
    rows: list[dict[str, float | int | str]] = []
    total_cost = 0.0
    quarantine: set[str] = set()
    stats = _empty_stats()

    regime_hist: list[float] = []
    defensive_state = False
    in_streak = 0
    out_streak = 0
    locked_completion: dict[str, Any] | None = None

    for i, d in enumerate(trading_dates):
        matured_sources = pending_cash.pop(d, {})
        matured_total = float(sum(matured_sources.values()))
        if matured_total > 0:
            cash_free += matured_total

        split_row = split_event_wide.loc[d] if d in split_event_wide.index else pd.Series(dtype=float)
        lots = _apply_split_adjustment(lots, split_row, d, arm_name, [])

        price_row = px_exec_wide.loc[d]
        prev_d = trading_dates[i - 1] if i > 0 else d
        prev2_d = trading_dates[i - 2] if i > 1 else prev_d
        prev3_d = trading_dates[i - 3] if i > 2 else prev2_d
        prev_scores = scores_by_day.get(prev_d)
        held = set(split_lots_by_ticker(lots).keys())

        # Camada defensiva identica ao harness V2.
        candidates: list[tuple[str, int, float]] = []
        if defensive_state and held:
            for tk in held:
                z_prev = float(z_wide.at[prev_d, tk]) if (prev_d in z_wide.index and tk in z_wide.columns) else np.nan
                z_prev2 = (
                    float(z_wide.at[prev2_d, tk]) if (prev2_d in z_wide.index and tk in z_wide.columns) else np.nan
                )
                z_prev3 = (
                    float(z_wide.at[prev3_d, tk]) if (prev3_d in z_wide.index and tk in z_wide.columns) else np.nan
                )
                if not np.isfinite(z_prev):
                    continue
                band = _band_from_z(z_prev)
                persist = _persist_points(z_prev, z_prev2, z_prev3)
                any_rule = (
                    _to_bool(any_rule_wide.at[prev_d, tk])
                    if (prev_d in any_rule_wide.index and tk in any_rule_wide.columns)
                    else False
                )
                strong_rule = (
                    _to_bool(strong_rule_wide.at[prev_d, tk])
                    if (prev_d in strong_rule_wide.index and tk in strong_rule_wide.columns)
                    else False
                )
                evidence = (1 if any_rule else 0) + (2 if strong_rule else 0)
                score = int(min(6, band + persist + evidence))
                if z_prev < 0 and score >= 4:
                    candidates.append((tk, score, z_prev))

            candidates = sorted(candidates, key=lambda x: (-x[1], x[2]))[:5]
            cand_set = {t for t, _, _ in candidates}
            for tk in list(quarantine):
                any_rule = (
                    _to_bool(any_rule_wide.at[prev_d, tk])
                    if (prev_d in any_rule_wide.index and tk in any_rule_wide.columns)
                    else False
                )
                strong_rule = (
                    _to_bool(strong_rule_wide.at[prev_d, tk])
                    if (prev_d in strong_rule_wide.index and tk in strong_rule_wide.columns)
                    else False
                )
                in_control = not (any_rule or strong_rule)
                if in_control and tk not in cand_set:
                    quarantine.remove(tk)

            for tk, score, _ in candidates:
                if score >= 6:
                    pct = 1.0
                    stats["n_defensive_sell_100"] += 1
                elif score == 5:
                    pct = 0.50
                    stats["n_defensive_sell_50"] += 1
                else:
                    pct = 0.25
                    stats["n_defensive_sell_25"] += 1
                current_val = ticker_value(lots, tk, price_row)
                target_sell = current_val * pct
                lots, _, cost, sold_shares = _sell_ticker_fifo_source(
                    ticker=tk,
                    target_value_to_sell=target_sell,
                    lots=lots,
                    price_row=price_row,
                    friction=friction,
                    trading_dates=trading_dates,
                    i=i,
                    settlement_days=cfg.settlement_days,
                    pending_cash=pending_cash,
                    source="defensive",
                )
                if sold_shares > 0:
                    total_cost += cost
                    quarantine.add(tk)
                    stats["n_quarantine_entries"] += 1

        held = set(split_lots_by_ticker(lots).keys())
        is_rebalance_day = (i % rebalance_cadence) == 0
        if is_rebalance_day:
            target = _select_c2_target(prev_scores, held, cfg.top_n, cfg.buffer_k, quarantine=quarantine)
            target_set = set(target)
            to_sell = sorted([t for t in held if t not in target_set])
            for tk in to_sell:
                lots, _, cost, sold_shares = _sell_all_ticker_source(
                    ticker=tk,
                    lots=lots,
                    price_row=price_row,
                    friction=friction,
                    trading_dates=trading_dates,
                    i=i,
                    settlement_days=cfg.settlement_days,
                    pending_cash=pending_cash,
                    source="rebalance",
                )
                if sold_shares > 0:
                    total_cost += cost
        else:
            target = sorted(list(held))

        if is_rebalance_day and target:
            equity_now_trim = cash_free + _pending_total(pending_cash) + lots_market_value(lots, price_row)
            if equity_now_trim > 0 and cfg.max_weight_cap < 1.0:
                cap_val = float(equity_now_trim * cfg.max_weight_cap)
                shared = sorted(list(set(held).intersection(set(target))))
                for tk in shared:
                    current_val = ticker_value(lots, tk, price_row)
                    if current_val <= cap_val + 1e-12:
                        continue
                    target_sell = max(0.0, current_val - cap_val)
                    lots, _, cost, sold_shares = _sell_ticker_fifo_source(
                        ticker=tk,
                        target_value_to_sell=target_sell,
                        lots=lots,
                        price_row=price_row,
                        friction=friction,
                        trading_dates=trading_dates,
                        i=i,
                        settlement_days=cfg.settlement_days,
                        pending_cash=pending_cash,
                        source="rebalance",
                    )
                    if sold_shares > 0:
                        total_cost += cost

        if is_rebalance_day and target:
            equity_now = cash_free + _pending_total(pending_cash) + lots_market_value(lots, price_row)
            c4_weights = compute_target_weights(prev_scores, target, cfg.k_damp, cfg.max_weight_cap)
            for tk in target:
                if tk in quarantine:
                    continue
                wt = float(c4_weights.get(tk, 0.0))
                if wt <= 0:
                    continue
                target_val = max(0.0, equity_now * wt)
                lots, cash_free, total_cost, _ = _buy_to_target(
                    ticker=tk,
                    target_value=target_val,
                    lots=lots,
                    price_row=price_row,
                    cash_free=cash_free,
                    total_cost=total_cost,
                    friction=friction,
                    buy_date=d,
                )

            locked_completion = {
                "origin_i": i,
                "locked_list": list(target),
                "weights": dict(c4_weights),
            }

        if locked_completion and (not is_rebalance_day):
            age = i - int(locked_completion["origin_i"])
            if 1 <= age <= rebalance_cadence - 1 and cash_free > 0:
                equity_now = cash_free + _pending_total(pending_cash) + lots_market_value(lots, price_row)
                bought_today = False
                if policy == "V1_locked_extended" or (policy == "V0_current" and age == 1):
                    queue = _locked_queue(
                        locked_list=list(locked_completion["locked_list"]),
                        weights=dict(locked_completion["weights"]),
                        lots=lots,
                        price_row=price_row,
                        equity_now=equity_now,
                    )
                    for tk in queue:
                        if cash_free <= 0:
                            break
                        if tk in quarantine:
                            continue
                        if _spc_instavel(tk, prev_d, spc_wide):
                            stats["n_skipped_r001"] += 1
                            continue
                        wt = float(locked_completion["weights"].get(tk, 0.0))
                        if wt <= 0:
                            continue
                        target_val = equity_now * wt
                        lots, cash_free, total_cost, bought = _buy_to_target(
                            ticker=tk,
                            target_value=target_val,
                            lots=lots,
                            price_row=price_row,
                            cash_free=cash_free,
                            total_cost=total_cost,
                            friction=friction,
                            buy_date=d,
                        )
                        if bought:
                            bought_today = True
                            stats["n_locked_buys"] += 1
                elif policy == "V0_current" and age >= 2:
                    held_now = set(split_lots_by_ticker(lots).keys())
                    queue = _live_ranking_queue(prev_scores, held_now, quarantine, cfg.top_n)
                    for tk in queue:
                        if cash_free <= 0:
                            break
                        if _spc_instavel(tk, prev_d, spc_wide):
                            stats["n_skipped_r001"] += 1
                            continue
                        target_val = equity_now * float(cfg.max_weight_cap)
                        lots, cash_free, total_cost, bought = _buy_to_target(
                            ticker=tk,
                            target_value=target_val,
                            lots=lots,
                            price_row=price_row,
                            cash_free=cash_free,
                            total_cost=total_cost,
                            friction=friction,
                            buy_date=d,
                        )
                        if bought:
                            bought_today = True
                            stats["n_live_ranking_buys"] += 1
                if bought_today:
                    stats["n_inter_rebalance_buy_days"] += 1

        cash_log = float(cash_log_daily.get(d, 0.0))
        cash_ret = float(np.expm1(cash_log))
        if cash_free > 0:
            cash_free *= 1.0 + cash_ret

        held = set(split_lots_by_ticker(lots).keys())
        proxy_ret = np.nan
        if held and d in i_wide.index:
            vals = i_wide.loc[d, list(held)] if len(held) > 0 else pd.Series(dtype=float)
            vals_num = pd.to_numeric(vals, errors="coerce")
            if vals_num.notna().any():
                proxy_ret = float(vals_num.mean())
        regime_hist.append(proxy_ret if np.isfinite(proxy_ret) else 0.0)
        if len(regime_hist) >= 4:
            y = np.array(regime_hist[-4:], dtype=float)
            x = np.arange(4, dtype=float)
            slope = float(np.polyfit(x, y, 1)[0])
        else:
            slope = 0.0
        if slope < 0:
            in_streak += 1
            out_streak = 0
        elif slope > 0:
            out_streak += 1
            in_streak = 0
        else:
            in_streak = 0
            out_streak = 0
        if not defensive_state and in_streak >= 2:
            defensive_state = True
        elif defensive_state and out_streak >= 3:
            defensive_state = False

        holdings_value = lots_market_value(lots, price_row)
        by_ticker = split_lots_by_ticker(lots)
        conc_vals = [ticker_value(lots, tk, price_row) for tk in by_ticker] if holdings_value > 0 else []
        equity_end = cash_free + _pending_total(pending_cash) + holdings_value
        max_conc = (max(conc_vals) / equity_end) if conc_vals and equity_end > 0 else 0.0
        rows.append(
            {
                "date": d,
                "variant": arm_name,
                "equity": float(equity_end),
                "cash_free": float(cash_free),
                "cash_pending": float(_pending_total(pending_cash)),
                "n_tickers": int(len(by_ticker)),
                "max_concentration": float(max_conc),
                "cost_total_cum": float(total_cost),
                "ret_cash": float(cash_ret),
                "regime_defensive_used": int(defensive_state),
                "rebalance_cadence": int(rebalance_cadence),
                "is_rebalance_day": int(is_rebalance_day),
                "inter_rebalance_age": int(i - int(locked_completion["origin_i"])) if locked_completion else -1,
            }
        )

    curve = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    if not curve.empty:
        base = float(curve["equity"].iloc[0]) if float(curve["equity"].iloc[0]) > 0 else 1.0
        curve["equity_base100"] = (curve["equity"].astype(float) / base) * 100.0
    else:
        curve["equity_base100"] = pd.Series(dtype="float64")
    return curve, stats


def _delta_metrics(a: dict[str, float | int], b: dict[str, float | int], prefix: str) -> dict[str, float]:
    return {
        f"delta_cagr_holdout_{prefix}": round(float(a["cagr_holdout"]) - float(b["cagr_holdout"]), 6),
        f"delta_mdd_holdout_{prefix}": round(float(a["mdd_holdout"]) - float(b["mdd_holdout"]), 6),
        f"delta_cagr_recent_{prefix}": round(float(a["cagr_recent"]) - float(b["cagr_recent"]), 6),
        f"delta_mdd_recent_{prefix}": round(float(a["mdd_recent"]) - float(b["mdd_recent"]), 6),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    cfg, min_market_cap = _load_winner_cfg()

    canonical, macro, scores = load_inputs()
    blacklist = load_blacklist(IN_BLACKLIST)
    cash_log_daily = build_cash_log_daily(macro)
    scores_by_day = build_scores_by_day(scores=scores, blacklist=blacklist)
    market_cap_wide = build_market_cap_wide(canonical)
    scores_by_day, median_pre_filter, median_post_filter = apply_min_market_cap_filter(
        scores_by_day=scores_by_day,
        market_cap_wide=market_cap_wide,
        min_market_cap=min_market_cap,
    )
    px_exec_wide, split_event_wide, i_wide, z_wide, any_rule_wide, strong_rule_wide, spc_wide = _prepare_wides(
        canonical
    )

    curve_arm0, _, _, _ = run_variant(
        variant="C4",
        px_exec_wide=px_exec_wide,
        split_event_wide=split_event_wide,
        i_wide=i_wide,
        z_wide=z_wide,
        any_rule_wide=any_rule_wide,
        strong_rule_wide=strong_rule_wide,
        scores_by_day=scores_by_day,
        cash_log_daily=cash_log_daily,
        cfg=cfg,
    )
    curve_arm0 = curve_arm0.copy()
    curve_arm0["variant"] = "ARM_0"

    curve_v0, stats_v0 = run_arm_extended(
        arm_name="ARM_V0",
        policy="V0_current",
        px_exec_wide=px_exec_wide,
        split_event_wide=split_event_wide,
        i_wide=i_wide,
        z_wide=z_wide,
        any_rule_wide=any_rule_wide,
        strong_rule_wide=strong_rule_wide,
        spc_wide=spc_wide,
        scores_by_day=scores_by_day,
        cash_log_daily=cash_log_daily,
        cfg=cfg,
    )
    curve_v1, stats_v1 = run_arm_extended(
        arm_name="ARM_V1",
        policy="V1_locked_extended",
        px_exec_wide=px_exec_wide,
        split_event_wide=split_event_wide,
        i_wide=i_wide,
        z_wide=z_wide,
        any_rule_wide=any_rule_wide,
        strong_rule_wide=strong_rule_wide,
        spc_wide=spc_wide,
        scores_by_day=scores_by_day,
        cash_log_daily=cash_log_daily,
        cfg=cfg,
    )

    curves = {"ARM_0": curve_arm0, "ARM_V0": curve_v0, "ARM_V1": curve_v1}
    for arm, curve in curves.items():
        curve.to_csv(OUT_DIR / f"curve_v1_{arm}.csv", index=False)

    metrics_by_arm = {arm: _compute_metrics_dual_window(curve) for arm, curve in curves.items()}
    delta_v1_minus_v0 = {
        "delta_cagr_holdout_v1_minus_v0": round(
            float(metrics_by_arm["ARM_V1"]["cagr_holdout"]) - float(metrics_by_arm["ARM_V0"]["cagr_holdout"]), 6
        ),
        "delta_mdd_holdout_v1_minus_v0": round(
            float(metrics_by_arm["ARM_V1"]["mdd_holdout"]) - float(metrics_by_arm["ARM_V0"]["mdd_holdout"]), 6
        ),
        "delta_cagr_recent_v1_minus_v0": round(
            float(metrics_by_arm["ARM_V1"]["cagr_recent"]) - float(metrics_by_arm["ARM_V0"]["cagr_recent"]), 6
        ),
        "delta_mdd_recent_v1_minus_v0": round(
            float(metrics_by_arm["ARM_V1"]["mdd_recent"]) - float(metrics_by_arm["ARM_V0"]["mdd_recent"]), 6
        ),
    }
    global_verdict = _classify_global_verdict(delta_v1_minus_v0)

    winner_payload = json.loads(IN_WINNER.read_text(encoding="utf-8"))
    winner_cagr = float(winner_payload.get("holdout_metrics", {}).get("cagr_pct", 42.1353))
    winner_mdd = float(winner_payload.get("holdout_metrics", {}).get("mdd_pct", -40.1213))
    arm0_cagr = float(metrics_by_arm["ARM_0"]["cagr_holdout"])
    arm0_mdd = float(metrics_by_arm["ARM_0"]["mdd_holdout"])
    baseline_gap_diagnostic = {
        "arm0_cagr_holdout_pct": round(arm0_cagr, 6),
        "winner_cagr_pct": round(winner_cagr, 6),
        "delta_cagr_pct": round(arm0_cagr - winner_cagr, 6),
        "arm0_mdd_holdout_pct": round(arm0_mdd, 6),
        "winner_mdd_pct": round(winner_mdd, 6),
        "delta_mdd_pct": round(arm0_mdd - winner_mdd, 6),
    }

    arms: list[dict[str, Any]] = []
    for arm in ["ARM_0", "ARM_V0", "ARM_V1"]:
        m = metrics_by_arm[arm]
        row: dict[str, Any] = {
            "arm": arm,
            "policy": {
                "ARM_0": "C4_produtivo_sem_completude_inter_rebalance",
                "ARM_V0": "A1D_D+1_e_ranking_vivo_D+2_ate_D+9",
                "ARM_V1": "lista_travada_estendida_D+1_ate_D+9",
            }[arm],
            "cagr_holdout_pct": round(float(m["cagr_holdout"]), 6),
            "mdd_holdout_pct": round(float(m["mdd_holdout"]), 6),
            "cagr_recent_pct": round(float(m["cagr_recent"]), 6),
            "mdd_recent_pct": round(float(m["mdd_recent"]), 6),
            "days_holdout": int(m["days_holdout"]),
            "days_recent": int(m["days_recent"]),
        }
        if arm == "ARM_V0":
            row["counters"] = stats_v0
            row.update(_delta_metrics(m, metrics_by_arm["ARM_0"], "vs_arm0"))
        elif arm == "ARM_V1":
            row["counters"] = stats_v1
            row.update(_delta_metrics(m, metrics_by_arm["ARM_0"], "vs_arm0"))
        else:
            row["counters"] = _empty_stats()
        arms.append(row)

    report = {
        "task_id": "T-DPLUS1-EXTENDED-US-V1",
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "holdout_period": {"start": str(HOLDOUT_START.date()), "end": str(HOLDOUT_END.date())},
        "sw_recent_period": {"start": str(SW_RECENT_START.date()), "end": str(HOLDOUT_END.date())},
        "params": {
            "variant_baseline": "C4",
            "top_n": int(cfg.top_n),
            "buffer_k": int(cfg.buffer_k),
            "rebalance_cadence": int(cfg.rebalance_cadence),
            "friction_one_way_bps": float(cfg.friction_one_way_bps),
            "settlement_days": int(cfg.settlement_days),
            "base_capital": float(cfg.base_capital),
            "k_damp": float(cfg.k_damp),
            "max_weight_cap": float(cfg.max_weight_cap),
            "min_market_cap": float(min_market_cap),
            "median_scored_tickers_pre_filter": float(median_pre_filter),
            "median_scored_tickers_post_filter": float(median_post_filter),
        },
        "modeling_notes": [
            "Veto duro modelado: R-001 via _spc_instavel.",
            "Nelson/WE e R-037 permanecem fora do motor do experimento porque sao consultivos/Plano B na skill vigente.",
            "ARM_V0 aplica A1D no D+1 e ranking vivo em D+2..D+9.",
            "ARM_V1 aplica apenas locked_list do rebalance em D+1..D+9.",
        ],
        "baseline_gap_diagnostic": baseline_gap_diagnostic,
        "arms": arms,
        "primary_delta": delta_v1_minus_v0,
        "global_verdict": global_verdict,
        "verdict_criteria_snapshot": VERDICT_CRITERIA_V1,
    }

    verdict_path = OUT_DIR / "verdict_v1.json"
    verdict_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("=== ENCERRAMENTO T-DPLUS1-EXTENDED-US-V1 ===")
    print(f"global_verdict: {global_verdict}")
    print(f"delta_v1_minus_v0: {json.dumps(delta_v1_minus_v0, sort_keys=True)}")
    print(f"verdict_path: {verdict_path}")
    print("INSTRUCAO: registrar o veredito final em SALA D-057 e USA D-118 antes de encerrar a task")


if __name__ == "__main__":
    main()
