"""T-EXEC-COMPLETION-US-V1.

Experimento offline para medir completude de compras em D+1 apos rebalance,
com liquidação T+1 e impacto de caixa de vendas defensivas.
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
    BacktestConfig,
    Lot,
    _apply_split_adjustment,
    _band_from_z,
    _build_z_table,
    _curve_metrics,
    _persist_points,
    _select_c2_target,
    _settlement_date,
    _to_bool,
    apply_min_market_cap_filter,
    build_cash_log_daily,
    build_market_cap_wide,
    build_scores_by_day,
    compute_target_weights,
    load_blacklist,
    load_inputs,
    lots_market_value,
    split_lots_by_ticker,
    ticker_value,
)

IN_WINNER = ROOT / "config" / "winner_us.json"
IN_BLACKLIST = ROOT / "config" / "blacklist_us.json"
OUT_DIR = ROOT / "backtest" / "t_exec_completion_us" / "results"

HOLDOUT_START = pd.Timestamp("2023-01-02")
HOLDOUT_END = pd.Timestamp("2026-03-16")

# Pre-registro: criterios fixos antes da execucao (nao alterar pos-run).
VERDICT_CRITERIA: dict[str, Any] = {
    "holdout_start": "2023-01-02",
    "holdout_end": "2026-03-16",
    "delta_cagr_threshold_pct": 1.0,
    "delta_mdd_threshold_pct": -5.0,
    "verdict_MELHORA": "delta_cagr > +1.0 AND delta_mdd > -5.0",
    "verdict_NEUTRO": "|delta_cagr| <= 1.0",
    "verdict_PIORA": "delta_cagr < -1.0 OR delta_mdd < -5.0",
    "verdict_INCONCLUSIVO": "demais casos",
}

SPC_COLS = [
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


def _pending_total(pending_cash: dict[pd.Timestamp, dict[str, float]]) -> float:
    return float(sum(sum(src.values()) for src in pending_cash.values()))


def _schedule_pending_cash(
    pending_cash: dict[pd.Timestamp, dict[str, float]],
    settle_dt: pd.Timestamp,
    source: str,
    amount: float,
) -> None:
    if amount <= 0:
        return
    bucket = pending_cash.setdefault(settle_dt, {"rebalance": 0.0, "defensive": 0.0})
    bucket[source] = float(bucket.get(source, 0.0) + amount)


def _sell_ticker_fifo_source(
    ticker: str,
    target_value_to_sell: float,
    lots: list[Lot],
    price_row: pd.Series,
    friction: float,
    trading_dates: list[pd.Timestamp],
    i: int,
    settlement_days: int,
    pending_cash: dict[pd.Timestamp, dict[str, float]],
    source: str,
) -> tuple[list[Lot], float, float, int]:
    px = float(price_row.get(ticker, np.nan))
    if not np.isfinite(px) or px <= 0 or target_value_to_sell <= 0:
        return lots, 0.0, 0.0, 0

    remaining_value = target_value_to_sell
    proceeds_liq = 0.0
    total_cost = 0.0
    sold_shares = 0
    updated_lots: list[Lot] = []

    for lot in lots:
        if lot.ticker != ticker or remaining_value <= 0:
            updated_lots.append(lot)
            continue
        lot_value = lot.shares * px
        if lot_value <= 0:
            continue
        value_to_sell = min(lot_value, remaining_value)
        shares_to_sell = int(value_to_sell // px)
        if shares_to_sell <= 0:
            updated_lots.append(lot)
            continue

        gross = shares_to_sell * px
        cost = gross * friction
        net = gross - cost
        total_cost += cost
        proceeds_liq += net
        sold_shares += shares_to_sell
        remaining_value -= gross
        new_shares = lot.shares - shares_to_sell
        if new_shares > 0:
            updated_lots.append(Lot(ticker=lot.ticker, buy_date=lot.buy_date, shares=new_shares, buy_price=lot.buy_price))

    if proceeds_liq > 0:
        settle_dt = _settlement_date(trading_dates, i, settlement_days)
        _schedule_pending_cash(pending_cash, settle_dt, source, proceeds_liq)
    return updated_lots, proceeds_liq, total_cost, sold_shares


def _sell_all_ticker_source(
    ticker: str,
    lots: list[Lot],
    price_row: pd.Series,
    friction: float,
    trading_dates: list[pd.Timestamp],
    i: int,
    settlement_days: int,
    pending_cash: dict[pd.Timestamp, dict[str, float]],
    source: str,
) -> tuple[list[Lot], float, float, int]:
    value = ticker_value(lots, ticker, price_row)
    return _sell_ticker_fifo_source(
        ticker=ticker,
        target_value_to_sell=value,
        lots=lots,
        price_row=price_row,
        friction=friction,
        trading_dates=trading_dates,
        i=i,
        settlement_days=settlement_days,
        pending_cash=pending_cash,
        source=source,
    )


def _spc_instavel(ticker: str, d_prev: pd.Timestamp, spc_wide: dict[str, pd.DataFrame]) -> bool:
    def _get(col: str) -> float:
        wide = spc_wide[col]
        if d_prev not in wide.index or ticker not in wide.columns:
            return float("nan")
        return float(wide.at[d_prev, ticker])

    iv = _get("i_value")
    iu = _get("i_ucl")
    il = _get("i_lcl")
    mrv = _get("mr_value")
    mru = _get("mr_ucl")
    xv = _get("xbar_value")
    xu = _get("xbar_ucl")
    xl = _get("xbar_lcl")
    rv = _get("r_value")
    ru = _get("r_ucl")

    checks = []
    if np.isfinite(iv) and np.isfinite(iu):
        checks.append(iv > iu)
    if np.isfinite(iv) and np.isfinite(il):
        checks.append(iv < il)
    if np.isfinite(mrv) and np.isfinite(mru):
        checks.append(mrv > mru)
    if np.isfinite(rv) and np.isfinite(ru):
        checks.append(rv > ru)
    if np.isfinite(xv) and np.isfinite(xu):
        checks.append(xv > xu)
    if np.isfinite(xv) and np.isfinite(xl):
        checks.append(xv < xl)
    return any(checks)


def _classify_verdict(delta_cagr: float, delta_mdd: float) -> str:
    if delta_cagr > float(VERDICT_CRITERIA["delta_cagr_threshold_pct"]) and delta_mdd > float(
        VERDICT_CRITERIA["delta_mdd_threshold_pct"]
    ):
        return "MELHORA"
    if abs(delta_cagr) <= float(VERDICT_CRITERIA["delta_cagr_threshold_pct"]):
        return "NEUTRO"
    if delta_cagr < -float(VERDICT_CRITERIA["delta_cagr_threshold_pct"]) or delta_mdd < float(
        VERDICT_CRITERIA["delta_mdd_threshold_pct"]
    ):
        return "PIORA"
    return "INCONCLUSIVO"


def _prepare_wides(
    canonical: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    px_exec_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first").sort_index().ffill()
    )
    split_wide = canonical.pivot_table(index="date", columns="ticker", values="split_factor", aggfunc="first").sort_index()
    split_changed = (split_wide / split_wide.shift(1)).replace([np.inf, -np.inf], np.nan)
    has_split = (split_changed - 1.0).abs() > 1e-12
    px_raw_wide = canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first").sort_index()
    split_event_wide = (px_raw_wide.shift(1) / px_raw_wide).where(has_split)

    for col in SPC_COLS:
        canonical[col] = pd.to_numeric(canonical[col], errors="coerce")
    i_wide = canonical.pivot_table(index="date", columns="ticker", values="i_value", aggfunc="first").sort_index()
    z_wide = _build_z_table(i_wide)

    any_rule = (
        (canonical["i_value"] > canonical["i_ucl"])
        | (canonical["i_value"] < canonical["i_lcl"])
        | (canonical["mr_value"] > canonical["mr_ucl"])
        | (canonical["r_value"] > canonical["r_ucl"])
        | (canonical["xbar_value"] > canonical["xbar_ucl"])
        | (canonical["xbar_value"] < canonical["xbar_lcl"])
    ).astype(float)
    strong_rule = (
        (canonical["i_value"] > canonical["i_ucl"])
        | (canonical["i_value"] < canonical["i_lcl"])
        | (canonical["mr_value"] > canonical["mr_ucl"])
    ).astype(float)

    canonical["_any_rule"] = any_rule
    canonical["_strong_rule"] = strong_rule
    any_rule_wide = canonical.pivot_table(index="date", columns="ticker", values="_any_rule", aggfunc="first").sort_index()
    strong_rule_wide = canonical.pivot_table(
        index="date", columns="ticker", values="_strong_rule", aggfunc="first"
    ).sort_index()

    spc_wide = {
        col: canonical.pivot_table(index="date", columns="ticker", values=col, aggfunc="first").sort_index()
        for col in SPC_COLS
    }
    return px_exec_wide, split_event_wide, i_wide, z_wide, any_rule_wide, strong_rule_wide, spc_wide


def run_arm_with_completion(
    arm_name: str,
    completion_mode: str,
    px_exec_wide: pd.DataFrame,
    split_event_wide: pd.DataFrame,
    i_wide: pd.DataFrame,
    z_wide: pd.DataFrame,
    any_rule_wide: pd.DataFrame,
    strong_rule_wide: pd.DataFrame,
    spc_wide: dict[str, pd.DataFrame],
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    cash_log_daily: pd.Series,
    cfg: BacktestConfig,
) -> tuple[pd.DataFrame, dict[str, int]]:
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
    quarantine_entries = 0

    def25 = 0
    def50 = 0
    def100 = 0
    regime_hist: list[float] = []
    defensive_state = False
    in_streak = 0
    out_streak = 0

    stats = {
        "n_deferred_buy_days": 0,
        "n_skipped_r001": 0,
        "n_substitute_a2": 0,
    }
    locked_completion: dict[str, Any] | None = None

    for i, d in enumerate(trading_dates):
        matured_sources = pending_cash.pop(d, {})
        matured_total = float(sum(matured_sources.values()))
        if matured_total > 0:
            cash_free += matured_total
        matured_defensive_today = float(matured_sources.get("defensive", 0.0))

        split_row = split_event_wide.loc[d] if d in split_event_wide.index else pd.Series(dtype=float)
        lots = _apply_split_adjustment(lots, split_row, d, arm_name, [])

        price_row = px_exec_wide.loc[d]
        prev_d = trading_dates[i - 1] if i > 0 else d
        prev2_d = trading_dates[i - 2] if i > 1 else prev_d
        prev3_d = trading_dates[i - 3] if i > 2 else prev2_d
        prev_scores = scores_by_day.get(prev_d)

        # Camada 1: defensiva permanente
        held = set(split_lots_by_ticker(lots).keys())
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
                    def100 += 1
                elif score == 5:
                    pct = 0.50
                    def50 += 1
                else:
                    pct = 0.25
                    def25 += 1
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
                    quarantine_entries += 1

        # Camada 2: rebalance C4
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
                    if target_sell <= 0:
                        continue
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

        # Compras no proprio D0 de rebalance.
        if is_rebalance_day and target:
            equity_now = cash_free + _pending_total(pending_cash) + lots_market_value(lots, price_row)
            c4_weights = compute_target_weights(prev_scores, target, cfg.k_damp, cfg.max_weight_cap)
            for tk in target:
                if tk in quarantine:
                    continue
                current_val = ticker_value(lots, tk, price_row)
                wt = float(c4_weights.get(tk, 0.0))
                desired_val = max(0.0, (equity_now * wt) - current_val)
                if desired_val <= 0:
                    continue
                px = float(price_row.get(tk, np.nan))
                if (not np.isfinite(px)) or px <= 0:
                    continue
                max_afford = cash_free / (1.0 + friction)
                buy_val = min(desired_val, max_afford)
                if buy_val <= 0:
                    continue
                shares_to_buy = int(buy_val // px)
                if shares_to_buy <= 0:
                    continue
                gross = shares_to_buy * px
                cost = gross * friction
                outflow = gross + cost
                if outflow > cash_free + 1e-12:
                    continue
                cash_free -= outflow
                total_cost += cost
                lots.append(Lot(ticker=tk, buy_date=d, shares=shares_to_buy, buy_price=px))

            pending_tickers: list[str] = []
            weights_full: dict[str, float] = {}
            for tk in target:
                if tk in quarantine:
                    continue
                wt = float(c4_weights.get(tk, 0.0))
                weights_full[tk] = wt
                target_val = max(0.0, equity_now * wt)
                current_post = ticker_value(lots, tk, price_row)
                if target_val - current_post > 1e-9:
                    pending_tickers.append(tk)

            if pending_tickers:
                locked_completion = {
                    "origin_i": i,
                    "locked_list": list(target),
                    "pending_list": pending_tickers,
                    "weights": weights_full,
                    "equity_ref": float(equity_now),
                }
            else:
                locked_completion = None

        # Completude D+1 da carteira travada do rebalance de D0.
        if locked_completion and i == int(locked_completion["origin_i"]) + 1:
            if completion_mode != "none":
                stats["n_deferred_buy_days"] += 1
                pending_set = set(locked_completion["pending_list"])
                if completion_mode == "A2":
                    queue = list(locked_completion["locked_list"])
                else:
                    queue = list(locked_completion["pending_list"])

                completion_budget = float(cash_free)
                if completion_mode == "A1":
                    completion_budget = max(0.0, completion_budget - matured_defensive_today)

                for tk in queue:
                    if completion_budget <= 0:
                        break
                    if tk in quarantine:
                        continue
                    if _spc_instavel(tk, prev_d, spc_wide):
                        if tk in pending_set:
                            stats["n_skipped_r001"] += 1
                        continue

                    wt = float(locked_completion["weights"].get(tk, 0.0))
                    if wt <= 0:
                        continue
                    desired_total = max(0.0, float(locked_completion["equity_ref"]) * wt)
                    current_now = ticker_value(lots, tk, price_row)
                    desired_val = max(0.0, desired_total - current_now)
                    if desired_val <= 0:
                        continue
                    px = float(price_row.get(tk, np.nan))
                    if (not np.isfinite(px)) or px <= 0:
                        continue
                    max_afford = completion_budget / (1.0 + friction)
                    buy_val = min(desired_val, max_afford)
                    if buy_val <= 0:
                        continue
                    shares_to_buy = int(buy_val // px)
                    if shares_to_buy <= 0:
                        continue
                    gross = shares_to_buy * px
                    cost = gross * friction
                    outflow = gross + cost
                    if outflow > cash_free + 1e-12 or outflow > completion_budget + 1e-12:
                        continue
                    cash_free -= outflow
                    completion_budget -= outflow
                    total_cost += cost
                    lots.append(Lot(ticker=tk, buy_date=d, shares=shares_to_buy, buy_price=px))
                    if completion_mode == "A2" and tk not in pending_set:
                        stats["n_substitute_a2"] += 1

            # Janela estrita: apenas D+1.
            locked_completion = None

        cash_log = float(cash_log_daily.get(d, 0.0))
        cash_ret = float(np.expm1(cash_log))
        if cash_free > 0:
            cash_free *= (1.0 + cash_ret)

        # Atualiza regime defensivo para D+1.
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
        conc_vals = []
        if holdings_value > 0:
            for tk in by_ticker:
                conc_vals.append(ticker_value(lots, tk, price_row))
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
                "def_sell_25_cum": int(def25),
                "def_sell_50_cum": int(def50),
                "def_sell_100_cum": int(def100),
                "quarantine_size": int(len(quarantine)),
                "quarantine_entries_cum": int(quarantine_entries),
                "rebalance_cadence": int(rebalance_cadence),
                "is_rebalance_day": int(is_rebalance_day),
            }
        )

    curve = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    if not curve.empty:
        base = float(curve["equity"].iloc[0]) if float(curve["equity"].iloc[0]) > 0 else 1.0
        curve["equity_base100"] = (curve["equity"].astype(float) / base) * 100.0
    else:
        curve["equity_base100"] = pd.Series(dtype="float64")
    return curve, stats


def _load_winner_cfg() -> tuple[BacktestConfig, float]:
    payload = json.loads(IN_WINNER.read_text(encoding="utf-8"))
    snap = payload.get("winner_config_snapshot", {})
    cfg = BacktestConfig(
        top_n=int(snap.get("top_n", 20)),
        buffer_k=int(snap.get("buffer_k", 10)),
        rebalance_cadence=int(snap.get("rebalance_cadence", 10)),
        friction_one_way_bps=float(snap.get("friction_one_way_bps", 2.5)),
        settlement_days=int(snap.get("settlement_days", 1)),
        base_capital=float(snap.get("base_capital", 100000.0)),
        k_damp=float(snap.get("k_damp", 0.0)),
        max_weight_cap=float(snap.get("max_weight_cap", 0.06)),
    )
    min_market_cap = float(snap.get("min_market_cap", 300000000.0))
    return cfg, min_market_cap


def _compute_holdout_metrics(curve: pd.DataFrame) -> tuple[float, float, int]:
    sub = curve[(curve["date"] >= HOLDOUT_START) & (curve["date"] <= HOLDOUT_END)].copy()
    if len(sub) < 2:
        raise RuntimeError("Holdout com menos de 2 linhas no experimento.")
    cagr, mdd = _curve_metrics(sub)
    return float(cagr * 100.0), float(mdd * 100.0), int(len(sub))


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
    px_exec_wide, split_event_wide, i_wide, z_wide, any_rule_wide, strong_rule_wide, spc_wide = _prepare_wides(canonical)

    arm_plan = [
        ("ARM_0", "none"),
        ("ARM_A1", "A1"),
        ("ARM_A2", "A2"),
        ("ARM_A1D", "A1D"),
    ]

    arm_rows: list[dict[str, Any]] = []
    curves: dict[str, pd.DataFrame] = {}
    stats_by_arm: dict[str, dict[str, int]] = {}
    for arm_name, mode in arm_plan:
        curve, stats = run_arm_with_completion(
            arm_name=arm_name,
            completion_mode=mode,
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
        curve.to_csv(OUT_DIR / f"curve_{arm_name}.csv", index=False)
        curves[arm_name] = curve
        stats_by_arm[arm_name] = stats

    base_cagr, base_mdd, holdout_days = _compute_holdout_metrics(curves["ARM_0"])
    for arm_name, mode in arm_plan:
        cagr_pct, mdd_pct, _ = _compute_holdout_metrics(curves[arm_name])
        if arm_name == "ARM_0":
            delta_cagr = 0.0
            delta_mdd = 0.0
            verdict = "BASELINE"
        else:
            delta_cagr = float(cagr_pct - base_cagr)
            delta_mdd = float(mdd_pct - base_mdd)
            verdict = _classify_verdict(delta_cagr, delta_mdd)
        arm_rows.append(
            {
                "arm": arm_name,
                "completion_mode": mode,
                "cagr_pct": round(float(cagr_pct), 6),
                "mdd_pct": round(float(mdd_pct), 6),
                "delta_cagr_vs_ARM0": round(float(delta_cagr), 6),
                "delta_mdd_vs_ARM0": round(float(delta_mdd), 6),
                "n_deferred_buy_days": int(stats_by_arm[arm_name]["n_deferred_buy_days"]),
                "n_skipped_r001": int(stats_by_arm[arm_name]["n_skipped_r001"]),
                "n_substitute_a2": int(stats_by_arm[arm_name]["n_substitute_a2"]),
                "verdict": verdict,
            }
        )

    verdicts = [row["verdict"] for row in arm_rows if row["arm"] != "ARM_0"]
    if any(v == "MELHORA" for v in verdicts):
        global_verdict = "MELHORA_EXECUCAO"
    elif all(v == "NEUTRO" for v in verdicts):
        global_verdict = "NEUTRO"
    elif any(v == "PIORA" for v in verdicts):
        global_verdict = "PIORA_EXECUCAO"
    else:
        global_verdict = "INCONCLUSIVO"

    report = {
        "task_id": "T-EXEC-COMPLETION-US-V1",
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "holdout_period": {
            "start": str(HOLDOUT_START.date()),
            "end": str(HOLDOUT_END.date()),
            "days": holdout_days,
        },
        "params": {
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
        "arms": arm_rows,
        "global_verdict": global_verdict,
        "verdict_criteria_snapshot": VERDICT_CRITERIA,
        "notes": [
            "ARM_0 reproduz comportamento atual: sem tentativa de completude D+1.",
            "A1 nao usa o caixa liquidado no dia vindo de vendas defensivas para completar pendencias do rebalance.",
            "A1D permite uso completo do caixa liquidado no dia, incluindo origem defensiva.",
            "A2 tenta substituicao pela ordem da lista travada do proprio rebalance.",
        ],
    }

    verdict_path = OUT_DIR / "verdict_v1.json"
    verdict_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("T-EXEC-COMPLETION-US-V1 READY")
    print(json.dumps({"global_verdict": global_verdict, "arms": arm_rows}, indent=2))


if __name__ == "__main__":
    main()
