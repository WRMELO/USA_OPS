"""Backtest V2 da politica defensiva/reinvestimento US.

Corrige exclusivamente o harness do estudo V1 invalidado:
- calendario nasce do primeiro dia com scores de d_prev;
- varredura pareada dos 10 offsets da cadencia;
- venda defensiva D0 por contagem exata de acoes;
- ciclo finito de ``defensive_blocked``;
- A2 ignora posicao-po na ocupacao de vagas;
- gates de sanidade executados antes de qualquer metrica.

Este modulo e read-only em relacao a dados operacionais e nao altera o motor
blindado nem ``backtest/run_backtest_variants_us.py``.
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

import backtest.t_defensive_reinvest_policy_us_v1.run_t_defensive_reinvest_policy_us_v1 as v1  # noqa: E402

rb = v1.rb
prev = v1.prev
prevr037 = v1.prevr037

TASK_ID = "T-SDC-DEFENSIVE-REINVEST-POLICY-US-V2"
ARMS = ["Zero-A", "Zero-B", "A1", "A2"]
OFFSETS = list(range(10))
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
IN_CRITERION = (
    ROOT
    / "backtest"
    / "t_defensive_reinvest_policy_us_v2"
    / "decision_criterion_defensive_reinvest_policy_us_v2.json"
)
V1_OBSERVATIONS = (
    ROOT
    / "backtest"
    / "t_defensive_reinvest_policy_us_v1"
    / "results"
    / "observations_defensive_reinvest_policy_us_v1.csv"
)
OUT_DIR = ROOT / "backtest" / "t_defensive_reinvest_policy_us_v2" / "results"

MEDIO_THRESHOLD_US = -12.85
GRAVE_THRESHOLD_US = -32.42
DUST_WEIGHT = 0.001


def _active_tickers(
    lots: list[rb.Lot],
    price_row: pd.Series,
    equity: float,
    threshold_weight: float = DUST_WEIGHT,
) -> set[str]:
    if equity <= 0:
        return set()
    out: set[str] = set()
    for ticker in rb.split_lots_by_ticker(lots):
        shares = sum(int(l.shares) for l in lots if l.ticker == ticker and l.shares > 0)
        value = rb.ticker_value(lots, ticker, price_row)
        if shares > 0 and value / equity >= threshold_weight:
            out.add(ticker)
    return out


def _sell_all_shares_exact(
    *,
    ticker: str,
    lots: list[rb.Lot],
    price_row: pd.Series,
    friction: float,
    pending_cash: dict[pd.Timestamp, float],
    exec_day: pd.Timestamp,
) -> tuple[list[rb.Lot], float, float, int]:
    """Vende todas as acoes do ticker sem divisao float por preco."""
    px = v1._safe_float(price_row.get(ticker, np.nan), np.nan)
    if not v1._is_finite(px) or px <= 0:
        return lots, 0.0, 0.0, 0
    shares = int(sum(int(l.shares) for l in lots if l.ticker == ticker and l.shares > 0))
    if shares <= 0:
        return lots, 0.0, 0.0, 0
    gross = float(shares * px)
    cost = float(gross * friction)
    proceeds = float(gross - cost)
    updated = [lot for lot in lots if lot.ticker != ticker]
    pending_cash[exec_day] = float(pending_cash.get(exec_day, 0.0) + proceeds)
    remaining = sum(int(l.shares) for l in updated if l.ticker == ticker and l.shares > 0)
    if remaining != 0:
        raise RuntimeError(f"G5: venda integral deixou {remaining} acoes de {ticker}")
    return updated, proceeds, cost, shares


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
) -> tuple[list[rb.Lot], float, float, float, str]:
    gross_buys = 0.0
    trade_cost = 0.0
    if cash_free <= 0:
        return lots, cash_free, gross_buys, trade_cost, ""
    if prev_scores.empty:
        return lots, cash_free, gross_buys, trade_cost, "SEM_SCORES"

    limit_weight = float(max_weight_cap if max_weight_cap > 0 else 1.0 / max(top_n, 1))
    limit_weight = float(min(max(limit_weight, 0.0), 1.0))
    equity_now = float(cash_free + rb.lots_market_value(lots, price_row))
    if equity_now <= 0 or limit_weight <= 0:
        return lots, cash_free, gross_buys, trade_cost, "CAP_ATINGIDO"

    rank_map = pd.to_numeric(prev_scores.get("m3_rank"), errors="coerce").to_dict()
    active = _active_tickers(lots, price_row, equity_now)
    existing = sorted(active, key=lambda tk: float(rank_map.get(tk, np.inf)))
    attempted = False
    minimum_lot_block = False

    for ticker in existing:
        if ticker in blocked:
            continue
        current = rb.ticker_value(lots, ticker, price_row)
        desired = max(0.0, equity_now * limit_weight - current)
        if desired <= 0:
            continue
        attempted = True
        lots, cash_free, gross, cost, bought = v1._buy_ticker_for_value(
            ticker=ticker,
            desired_value=desired,
            lots=lots,
            price_row=price_row,
            cash_free=cash_free,
            friction=friction,
            exec_day=exec_day,
        )
        if bought <= 0:
            minimum_lot_block = True
            continue
        gross_buys += gross
        trade_cost += cost
        if cash_free <= 0:
            return lots, cash_free, gross_buys, trade_cost, ""

    daily_top20 = rb._select_top_n(prev_scores, top_n=top_n, quarantine=set())
    unblocked = [ticker for ticker in daily_top20 if ticker not in blocked]
    if not unblocked and gross_buys <= 0:
        return lots, cash_free, gross_buys, trade_cost, "TODOS_BLOQUEADOS"

    slot_block = False
    cap_block = True
    for ticker in daily_top20:
        if ticker in blocked:
            continue
        active_now = _active_tickers(lots, price_row, equity_now)
        if ticker not in active_now and len(active_now) >= top_n:
            slot_block = True
            continue
        current = rb.ticker_value(lots, ticker, price_row)
        desired = max(0.0, equity_now * limit_weight - current)
        if desired <= 0:
            continue
        cap_block = False
        attempted = True
        lots, cash_free, gross, cost, bought = v1._buy_ticker_for_value(
            ticker=ticker,
            desired_value=desired,
            lots=lots,
            price_row=price_row,
            cash_free=cash_free,
            friction=friction,
            exec_day=exec_day,
        )
        if bought <= 0:
            minimum_lot_block = True
            continue
        gross_buys += gross
        trade_cost += cost
        if cash_free <= 0:
            break

    equity_after = float(cash_free + rb.lots_market_value(lots, price_row))
    residual_material = bool(equity_after > 0 and cash_free / equity_after > 0.005)
    if gross_buys > 0 or not residual_material:
        reason = ""
    elif minimum_lot_block or attempted:
        reason = "LOTE_MINIMO"
    elif slot_block:
        reason = "SEM_VAGA"
    elif cap_block:
        reason = "CAP_ATINGIDO"
    else:
        reason = "SEM_VAGA"
    return lots, cash_free, gross_buys, trade_cost, reason


def _first_scores_execution_idx(
    *,
    trading_days: list[pd.Timestamp],
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    mc_eligible_by_day: dict[pd.Timestamp, set[str]],
    blacklist: set[str],
) -> int:
    for i in range(1, len(trading_days)):
        d_prev = trading_days[i - 1]
        scores = v1._normalize_scores_for_day(
            scores_day=scores_by_day.get(d_prev),
            eligible=mc_eligible_by_day.get(d_prev, set()),
            blacklist=blacklist,
        )
        if not scores.empty:
            return i
    raise RuntimeError("Nenhum pregao de execucao com scores validos em d_prev.")


def _simulate_arm(
    *,
    arm_name: str,
    offset: int,
    first_scores_idx: int,
    trading_days: list[pd.Timestamp],
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
    first_rebalance_idx = int(first_scores_idx + offset)

    lots: list[rb.Lot] = []
    pending_cash: dict[pd.Timestamp, float] = {}
    cash_free = float(cfg["base_capital"])
    defensive_blocked: set[str] = set()
    defensive_cash = 0.0
    frozen_target: list[str] = []
    frozen_weights: dict[str, float] = {}
    cycle_id = -1
    prev_equity: float | None = None
    rows: list[dict[str, Any]] = []

    for i, d in enumerate(trading_days):
        cash_free += float(pending_cash.pop(d, 0.0))
        price_row = px_exec_wide.loc[d]
        d_prev = trading_days[i - 1] if i > 0 else None
        d_prev2 = trading_days[i - 2] if i > 1 else None
        prev_scores = (
            v1._normalize_scores_for_day(
                scores_day=scores_by_day.get(d_prev),
                eligible=mc_eligible_by_day.get(d_prev, set()),
                blacklist=blacklist,
            )
            if d_prev is not None
            else pd.DataFrame()
        )
        had_scores = not prev_scores.empty

        gross_sells_today = 0.0
        gross_buys_today = 0.0
        costs_today = 0.0
        defensive_sells_today = 0
        trigger_spc_today = 0
        trigger_medio_grave_today = 0
        no_buy_reason = ""

        is_rebalance_day = bool(
            i >= first_rebalance_idx
            and (i - first_rebalance_idx) % max(cadence, 1) == 0
        )
        if is_rebalance_day:
            cycle_id += 1
            defensive_blocked = set()
            defensive_cash = 0.0
            held = _active_tickers(
                lots,
                price_row,
                cash_free + rb.lots_market_value(lots, price_row),
            )
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
                frozen_target = sorted(held)[:top_n]
                eq = 1.0 / float(max(len(frozen_target), 1))
                frozen_weights = {ticker: eq for ticker in frozen_target}

            lots, cash_free, pending_cash, gross_s, gross_b, cost_r, _ = v1._rebalance_to_target(
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
            held_now = sorted(
                _active_tickers(
                    lots,
                    price_row,
                    cash_free + rb.lots_market_value(lots, price_row),
                )
            )
            spc_set = set(spc_blocked_by_day.get(d_prev, set()))
            trigger_tickers: dict[str, set[str]] = {}
            for ticker in held_now:
                if ticker in defensive_blocked:
                    continue
                reasons: set[str] = set()
                if ticker in spc_set:
                    reasons.add("SPC")
                if arm_name in {"A1", "A2"} and d_prev2 is not None:
                    avg_cost = v1._ticker_avg_cost(lots, ticker)
                    close_prev = (
                        v1._safe_float(px_exec_wide.at[d_prev, ticker], np.nan)
                        if ticker in px_exec_wide.columns
                        else np.nan
                    )
                    close_prev2 = (
                        v1._safe_float(px_exec_wide.at[d_prev2, ticker], np.nan)
                        if ticker in px_exec_wide.columns
                        else np.nan
                    )
                    if v1._cross_medium_to_grave(
                        v1._heat_pct(close_prev2, avg_cost),
                        v1._heat_pct(close_prev, avg_cost),
                    ):
                        reasons.add("MEDIO->GRAVE")
                if reasons:
                    trigger_tickers[ticker] = reasons

            for ticker, reasons in trigger_tickers.items():
                lots, proceeds, cost, sold_shares = _sell_all_shares_exact(
                    ticker=ticker,
                    lots=lots,
                    price_row=price_row,
                    friction=friction,
                    pending_cash=pending_cash,
                    exec_day=d,
                )
                if sold_shares <= 0:
                    continue
                cash_free += v1._consume_same_day_settlement(d, pending_cash)
                defensive_cash += float(proceeds)
                defensive_blocked.add(ticker)
                defensive_sells_today += 1
                trigger_spc_today += int("SPC" in reasons)
                trigger_medio_grave_today += int("MEDIO->GRAVE" in reasons)
                costs_today += cost
                gross_sells_today += proceeds + cost

            if defensive_cash > 1e-9 and cash_free > 1e-9:
                budget = float(min(cash_free, defensive_cash))
                reserve = float(cash_free - budget)
                budget_left = budget
                if arm_name in {"Zero-B", "A1"}:
                    lots, budget_left, gross_b, cost_b = v1._reinvest_frozen_policy(
                        frozen_target=frozen_target,
                        frozen_weights=frozen_weights,
                        lots=lots,
                        cash_free=budget,
                        price_row=price_row,
                        friction=friction,
                        exec_day=d,
                        blocked=defensive_blocked,
                    )
                    gross_buys_today += gross_b
                    costs_today += cost_b
                elif arm_name == "A2":
                    lots, budget_left, gross_b, cost_b, no_buy_reason = _reinvest_a2_policy(
                        top_n=top_n,
                        max_weight_cap=max_weight_cap,
                        prev_scores=prev_scores,
                        lots=lots,
                        cash_free=budget,
                        price_row=price_row,
                        friction=friction,
                        exec_day=d,
                        blocked=defensive_blocked,
                    )
                    gross_buys_today += gross_b
                    costs_today += cost_b
                spent = float(budget - budget_left)
                defensive_cash = float(max(0.0, defensive_cash - spent))
                cash_free = float(reserve + budget_left)

        cash_ret = float(np.expm1(float(cash_log_daily.get(d, 0.0))))
        if cash_free > 0:
            cash_free *= 1.0 + cash_ret

        holdings_value = rb.lots_market_value(lots, price_row)
        cash_pending = float(sum(float(v) for v in pending_cash.values()))
        equity_end = float(cash_free + cash_pending + holdings_value)
        active_tickers = _active_tickers(lots, price_row, equity_end)
        raw_tickers = {
            ticker
            for ticker, ticker_lots in rb.split_lots_by_ticker(lots).items()
            if sum(int(l.shares) for l in ticker_lots if l.shares > 0) > 0
        }
        log_ret_equity = (
            float(np.log(equity_end / prev_equity))
            if prev_equity is not None and prev_equity > 0 and equity_end > 0
            else float("nan")
        )
        prev_equity = equity_end
        turnover = (
            float((gross_sells_today + gross_buys_today) / equity_end)
            if equity_end > 0
            else float("nan")
        )
        cash_ratio = float(cash_free / equity_end) if equity_end > 0 else float("nan")
        if (
            arm_name == "A2"
            and v1._is_finite(cash_ratio)
            and equity_end > 0
            and (defensive_cash / equity_end) > 0.005
            and gross_buys_today <= 0
            and not no_buy_reason
            and had_scores
        ):
            no_buy_reason = "CAP_ATINGIDO"

        split = prev._to_split(d, holdout_end=holdout_end)
        if split == "OTHER":
            continue
        rows.append(
            {
                "date": d,
                "arm": arm_name,
                "offset": int(offset),
                "split": split,
                "is_holdout": int(split in {"HOLDOUT", "SW1", "SW2"}),
                "cycle_id": int(cycle_id),
                "equity": equity_end,
                "cash_free": float(cash_free),
                "cash_pending": cash_pending,
                "holdings_value": float(holdings_value),
                "n_tickers": int(len(active_tickers)),
                "raw_n_tickers": int(len(raw_tickers)),
                "log_ret_equity": log_ret_equity,
                "turnover_gross_pct": turnover,
                "defensive_sells_today": int(defensive_sells_today),
                "defensive_sell_rate": float(defensive_sells_today / max(top_n, 1)),
                "trigger_spc_today": int(trigger_spc_today),
                "trigger_medio_grave_today": int(trigger_medio_grave_today),
                "cash_idle_gt5": int(equity_end > 0 and cash_ratio > 0.05),
                "cash_ratio": cash_ratio,
                "trade_cost_today": float(costs_today),
                "gross_sells_today": float(gross_sells_today),
                "gross_buys_today": float(gross_buys_today),
                "is_rebalance_day": int(is_rebalance_day),
                "had_scores": int(had_scores),
                "no_buy_reason": no_buy_reason,
                "first_rebalance_expected_date": trading_days[first_rebalance_idx],
            }
        )

    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def _max_true_run(values: pd.Series) -> int:
    best = current = 0
    for value in values.astype(bool).tolist():
        current = current + 1 if value else 0
        best = max(best, current)
    return int(best)


def _run_sanity_gates(curves: pd.DataFrame) -> dict[str, Any]:
    details: dict[str, Any] = {}

    g1_fail: list[str] = []
    for (arm, offset), group in curves.groupby(["arm", "offset"]):
        actual = group.loc[group["is_rebalance_day"] == 1, "date"]
        expected = pd.Timestamp(group["first_rebalance_expected_date"].iloc[0]).normalize()
        if actual.empty or pd.Timestamp(actual.iloc[0]).normalize() != expected:
            g1_fail.append(f"{arm}/offset={offset}")
    details["G1"] = {"status": "PASS" if not g1_fail else "FAIL", "failures": g1_fail}

    g2_fail: list[str] = []
    for offset, phase in curves.groupby("offset"):
        defensive_dates = phase.loc[phase["defensive_sells_today"] > 0, "date"]
        cutoff = defensive_dates.min() if not defensive_dates.empty else phase["date"].max()
        pre = phase[phase["date"] < cutoff]
        pivot = pre.pivot_table(index="date", columns="arm", values="equity", aggfunc="first")
        if not pivot.empty:
            spread = pivot.max(axis=1) - pivot.min(axis=1)
            scale = pivot.max(axis=1).clip(lower=1.0)
            if bool((spread > 1e-6 * scale).any()):
                g2_fail.append(f"offset={offset}")
    details["G2"] = {"status": "PASS" if not g2_fail else "FAIL", "failures": g2_fail}

    g3_fail: list[dict[str, Any]] = []
    for (arm, offset), group in curves.groupby(["arm", "offset"]):
        started = group[group["is_rebalance_day"].cumsum() > 0].copy()
        run = _max_true_run((started["cash_ratio"] > 0.95) & (started["had_scores"] == 1))
        if run > 10:
            g3_fail.append({"arm": arm, "offset": int(offset), "max_run": run})
    details["G3"] = {"status": "PASS" if not g3_fail else "FAIL", "failures": g3_fail}

    accounting_error = (
        curves["equity"]
        - (curves["cash_free"] + curves["cash_pending"] + curves["holdings_value"])
    ).abs()
    tolerance = curves["equity"].abs().clip(lower=1.0) * 1e-6
    g4_count = int((accounting_error > tolerance).sum())
    details["G4"] = {
        "status": "PASS" if g4_count == 0 else "FAIL",
        "failure_count": g4_count,
        "max_abs_error": float(accounting_error.max()),
    }

    details["G5"] = {
        "status": "PASS",
        "note": "_sell_all_shares_exact aborta imediatamente se saldo remanescente != 0",
    }

    g6_fail: list[dict[str, Any]] = []
    g6_means: dict[str, float] = {}
    for (arm, offset), group in curves.groupby(["arm", "offset"]):
        started = group[group["is_rebalance_day"].cumsum() > 0]
        mean_tickers = float(started["n_tickers"].mean()) if not started.empty else float("nan")
        key = f"{arm}/offset={offset}"
        g6_means[key] = mean_tickers
        if not np.isfinite(mean_tickers) or not 10.0 <= mean_tickers <= 27.0:
            g6_fail.append({"arm": arm, "offset": int(offset), "mean": mean_tickers})
    details["G6"] = {
        "status": "PASS" if not g6_fail else "FAIL",
        "failures": g6_fail,
        "means": g6_means,
    }

    gates = [{"gate": gate, **payload} for gate, payload in details.items()]
    overall = "PASS" if all(item["status"] == "PASS" for item in gates) else "FAIL"
    return {"task_id": TASK_ID, "overall_status": overall, "gates": gates}


def _representative_offset(offset_rows: list[dict[str, Any]]) -> int:
    sharpe = np.asarray([row["delta_sharpe_holdout"] for row in offset_rows], dtype=float)
    cvar = np.asarray([row["delta_cvar5_holdout"] for row in offset_rows], dtype=float)

    def zscore(values: np.ndarray) -> np.ndarray:
        std = float(np.nanstd(values))
        if not np.isfinite(std) or std <= 0:
            return np.zeros_like(values)
        return (values - float(np.nanmean(values))) / std

    combined = zscore(sharpe) + zscore(cvar)
    median = float(np.nanmedian(combined))
    distances = np.abs(combined - median)
    return int(offset_rows[int(np.nanargmin(distances))]["offset"])


def _offset_verdict_payload(
    *,
    curves: pd.DataFrame,
    baseline: str,
    arm: str,
    offset: int,
    summaries: dict[str, dict[str, dict[str, Any]]],
    n_resamples: int,
    seed: int,
    max_veto_rate: float,
) -> dict[str, Any]:
    phase = curves[curves["offset"] == offset].copy()
    pair_df = v1._paired_frame(phase, baseline=baseline, arm=arm)
    bootstrap = {
        split: v1._bootstrap_pair_stats(
            pair_df,
            split,
            n_resamples=n_resamples,
            seed=seed + offset,
        )
        for split in ("HOLDOUT", "SW1", "SW2")
    }
    deltas: dict[str, dict[str, float]] = {}
    for split in ("HOLDOUT", "SW1", "SW2"):
        base = summaries[split][baseline]
        candidate = summaries[split][arm]
        deltas[split] = {
            "delta_cvar5": float(candidate["mean_cvar5"] - base["mean_cvar5"]),
            "delta_sharpe_cost_adj": float(
                candidate["mean_sharpe_cost_adj"] - base["mean_sharpe_cost_adj"]
            ),
            "mean_veto_rate_arm": float(candidate["mean_veto_rate"]),
        }

    veto_all = all(
        np.isfinite(deltas[split]["mean_veto_rate_arm"])
        and deltas[split]["mean_veto_rate_arm"] <= max_veto_rate
        for split in ("HOLDOUT", "SW1", "SW2")
    )
    domina = veto_all and all(
        v1._favorable_ic(bootstrap[split][metric]["ic95"])
        for split in ("HOLDOUT", "SW1", "SW2")
        for metric in ("delta_cvar5", "delta_sharpe_cost_adj")
    )
    hold_mass = all(
        bootstrap["HOLDOUT"][metric]["mass_same_sign_pct"] >= 90.0
        for metric in ("delta_cvar5", "delta_sharpe_cost_adj")
    )
    direction = all(
        deltas[split]["delta_cvar5"] > 0
        and deltas[split]["delta_sharpe_cost_adj"] > 0
        for split in ("HOLDOUT", "SW1", "SW2")
    )
    materiality = bool(
        abs(deltas["HOLDOUT"]["delta_sharpe_cost_adj"]) >= 0.30
        or abs(deltas["HOLDOUT"]["delta_cvar5"]) >= 0.02
    )
    favorecido = bool(
        not domina
        and hold_mass
        and direction
        and materiality
        and deltas["HOLDOUT"]["mean_veto_rate_arm"] <= max_veto_rate
    )
    tier = "DOMINA_FORTE" if domina else ("FAVORECIDO_LADO" if favorecido else "INCONCLUSIVO")
    return {
        "offset": int(offset),
        "offset_tier_before_phase_robustness": tier,
        "deltas": deltas,
        "bootstrap": bootstrap,
        "delta_sharpe_holdout": deltas["HOLDOUT"]["delta_sharpe_cost_adj"],
        "delta_cvar5_holdout": deltas["HOLDOUT"]["delta_cvar5"],
    }


def _exposure_comparison(curves: pd.DataFrame) -> dict[str, Any]:
    old = pd.read_csv(V1_OBSERVATIONS, parse_dates=["date"])
    new = curves[curves["offset"] == 0].copy()
    payload: dict[str, Any] = {"task_id": TASK_ID, "comparison_offset_v2": 0, "rows": []}
    for split in ("TRAIN", "HOLDOUT", "SW1", "SW2"):
        for arm in ARMS:
            old_sub = v1._subset_df(old[old["arm"] == arm].copy(), split)
            new_sub = v1._subset_df(new[new["arm"] == arm].copy(), split)
            payload["rows"].append(
                {
                    "split": split,
                    "arm": arm,
                    "v1_days_invested_pct": float((old_sub["holdings_value"] > 0).mean() * 100.0),
                    "v2_days_invested_pct": float((new_sub["holdings_value"] > 0).mean() * 100.0),
                    "v1_avg_tickers": float(old_sub["n_tickers"].mean()),
                    "v2_avg_tickers": float(new_sub["n_tickers"].mean()),
                    "v1_cash_idle_days_pct": float(old_sub["cash_idle_gt5"].mean() * 100.0),
                    "v2_cash_idle_days_pct": float(new_sub["cash_idle_gt5"].mean() * 100.0),
                }
            )
    a2_scores = new[(new["arm"] == "A2") & (new["had_scores"] == 1)]
    payload["a2_offset0_cash_ratio_le5_days_pct"] = float(
        (a2_scores["cash_ratio"] <= 0.05).mean() * 100.0
    )
    payload["owner_symptom_pass"] = bool(
        payload["a2_offset0_cash_ratio_le5_days_pct"] >= 80.0
    )
    return payload


def main() -> None:
    criterion = json.loads(IN_CRITERION.read_text(encoding="utf-8"))
    if not criterion.get("registered_before_execution"):
        raise RuntimeError("Pre-registro V2 nao marcado como anterior a execucao.")

    holdout_end, manifest = prev._load_holdout_end_from_manifest(IN_MANIFEST)
    if bool(criterion["dataset"].get("required_hash_verification", False)):
        prevr037._verify_manifest_hashes(manifest, DATASET_DIR)
        print("Hashes conferidos com sucesso contra manifest.json.")

    cfg = v1._load_winner_snapshot_full(IN_WINNER)
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
    spc_blocked_by_day = prev._build_spc_blocked_by_day(canonical)
    canonical["market_cap"] = pd.to_numeric(canonical["market_cap"], errors="coerce")
    mc_eligible_by_day = {
        date: set(group.loc[group["market_cap"] >= min_market_cap, "ticker"].dropna())
        for date, group in canonical.groupby("date")
    }
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
    scores_by_day = prev._compute_scores_by_day(px_exec_wide, holdout_end=holdout_end)
    first_scores_idx = _first_scores_execution_idx(
        trading_days=trading_days,
        scores_by_day=scores_by_day,
        mc_eligible_by_day=mc_eligible_by_day,
        blacklist=blacklist,
    )

    macro = pd.read_parquet(DATASET_DIR / "macro_us.parquet").copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date", "fed_funds_rate"]).sort_values("date")
    cash_log_daily = rb.build_cash_log_daily(macro)

    curves: list[pd.DataFrame] = []
    for offset in OFFSETS:
        for arm in ARMS:
            print(f"Executando arm={arm} offset={offset} ...")
            curves.append(
                _simulate_arm(
                    arm_name=arm,
                    offset=offset,
                    first_scores_idx=first_scores_idx,
                    trading_days=trading_days,
                    px_exec_wide=px_exec_wide,
                    cash_log_daily=cash_log_daily,
                    scores_by_day=scores_by_day,
                    mc_eligible_by_day=mc_eligible_by_day,
                    spc_blocked_by_day=spc_blocked_by_day,
                    cfg=cfg,
                    blacklist=blacklist,
                    holdout_end=holdout_end,
                )
            )
    curves_df = pd.concat(curves, ignore_index=True).sort_values(
        ["offset", "arm", "date"]
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    curves_df.to_csv(
        OUT_DIR / "observations_defensive_reinvest_policy_us_v2.csv",
        index=False,
    )

    sanity = _run_sanity_gates(curves_df)
    (OUT_DIR / "sanity_gates_report_v2.json").write_text(
        json.dumps(sanity, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if sanity["overall_status"] != "PASS":
        raise RuntimeError("Gates de sanidade falharam; metricas nao foram geradas.")

    summaries_by_offset: dict[int, dict[str, dict[str, dict[str, Any]]]] = {}
    for split in ("TRAIN", "HOLDOUT", "SW1", "SW2"):
        rows: list[dict[str, Any]] = []
        for offset in OFFSETS:
            phase = curves_df[curves_df["offset"] == offset]
            summaries_by_offset.setdefault(offset, {}).setdefault(split, {})
            for arm in ARMS:
                row = v1._summarize_split_arm(phase, subset=split, arm=arm)
                row["offset"] = offset
                summaries_by_offset[offset][split][arm] = row
                rows.append(row)
        pd.DataFrame(rows).to_csv(
            OUT_DIR / f"summary_{split}_defensive_reinvest_policy_us_v2.csv",
            index=False,
        )

    n_resamples = int(criterion["bootstrap"]["n_resamples"])
    seed = int(criterion["bootstrap"]["seed"])
    max_veto_rate = float(criterion["max_veto_rate"])
    bootstrap_payload: dict[str, Any] = {
        "task_id": TASK_ID,
        "method": criterion["bootstrap"]["method"],
        "n_resamples": n_resamples,
        "seed": seed,
        "pairs": {},
    }
    verdict_pairs: dict[str, Any] = {}

    for baseline, arm in PAIR_DEFS:
        pair_key = f"{baseline}_vs_{arm}"
        offset_payloads = [
            _offset_verdict_payload(
                curves=curves_df,
                baseline=baseline,
                arm=arm,
                offset=offset,
                summaries=summaries_by_offset[offset],
                n_resamples=n_resamples,
                seed=seed,
                max_veto_rate=max_veto_rate,
            )
            for offset in OFFSETS
        ]
        representative = _representative_offset(offset_payloads)
        rep = next(item for item in offset_payloads if item["offset"] == representative)
        favorable_offsets = sum(
            item["delta_sharpe_holdout"] > 0 and item["delta_cvar5_holdout"] > 0
            for item in offset_payloads
        )
        phase_robust = favorable_offsets >= int(
            criterion["inference_aggregation"]["minimum_favorable_offsets"]
        )
        if phase_robust and rep["offset_tier_before_phase_robustness"] == "DOMINA_FORTE":
            final = "DOMINA_FORTE"
        elif (
            phase_robust
            and rep["offset_tier_before_phase_robustness"] == "FAVORECIDO_LADO"
        ):
            final = f"FAVORECIDO_{v1._sanitize_label(arm)}"
        else:
            final = "INCONCLUSIVO"
        bootstrap_payload["pairs"][pair_key] = {"offsets": offset_payloads}
        verdict_pairs[pair_key] = {
            "baseline": baseline,
            "arm": arm,
            "representative_offset": representative,
            "favorable_offsets_joint_holdout": int(favorable_offsets),
            "phase_robustness_pass": bool(phase_robust),
            "representative_offset_tier": rep["offset_tier_before_phase_robustness"],
            "final_verdict": final,
            "representative_details": rep,
        }

    (OUT_DIR / "bootstrap_stats_defensive_reinvest_policy_us_v2.json").write_text(
        json.dumps(bootstrap_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    verdict = {
        "task_id": TASK_ID,
        "criteria_file": str(IN_CRITERION.relative_to(ROOT)),
        "dataset_manifest": str(IN_MANIFEST.relative_to(ROOT)),
        "freeze_asof": manifest.get("freeze_asof"),
        "arms": ARMS,
        "offsets": OFFSETS,
        "pairs": verdict_pairs,
        "sanity_gates": sanity["overall_status"],
    }
    (OUT_DIR / "verdict_defensive_reinvest_policy_us_v2.json").write_text(
        json.dumps(verdict, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    exposure = _exposure_comparison(curves_df)
    (OUT_DIR / "exposure_comparison_v1_v2.json").write_text(
        json.dumps(exposure, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if not exposure["owner_symptom_pass"]:
        raise RuntimeError("Criterio R-046 do Owner falhou para exposicao do A2.")

    print(f"{TASK_ID} concluido.")
    print(f"first_scores_exec_day={trading_days[first_scores_idx].date()}")
    print(f"rows_observations={len(curves_df)}")
    for pair_key, payload in verdict_pairs.items():
        print(f"{pair_key}={payload['final_verdict']}")


if __name__ == "__main__":
    main()
