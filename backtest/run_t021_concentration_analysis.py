"""T-021: Analise de concentracao + drawdown por ticker (C4 fixo)."""
from __future__ import annotations

from pathlib import Path
import json

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from run_backtest_variants_us import (
    BacktestConfig,
    IN_BLACKLIST,
    IN_CANONICAL,
    IN_MACRO,
    IN_SCORES,
    OUT_DIR,
    TRAIN_END,
    _sha256,
    _build_z_table,
    _to_bool,
    _band_from_z,
    _persist_points,
    _select_top_n,
    _select_c2_target,
    _settlement_date,
    _apply_split_adjustment,
    apply_min_market_cap_filter,
    build_cash_log_daily,
    build_market_cap_wide,
    build_scores_by_day,
    compute_target_weights,
    load_blacklist,
    load_inputs,
    lots_market_value,
    run_variant,
    sell_all_ticker,
    sell_ticker_fifo,
    split_lots_by_ticker,
    ticker_value,
)

ROOT = Path(__file__).resolve().parents[1]
MIN_MARKET_CAP = 300_000_000.0

# Config fixa (melhor C4 da T-018)
CFG = BacktestConfig(
    top_n=20,
    buffer_k=10,
    rebalance_cadence=10,
    friction_one_way_bps=2.5,
    settlement_days=1,
    base_capital=100_000.0,
    k_damp=0.0,
    max_weight_cap=0.06,
)


def _build_inputs():
    canonical, macro, scores = load_inputs()
    blacklist = load_blacklist(IN_BLACKLIST)
    cash_log_daily = build_cash_log_daily(macro)
    scores_by_day = build_scores_by_day(scores=scores, blacklist=blacklist)
    market_cap_wide = build_market_cap_wide(canonical)
    scores_by_day, median_pre_filter, median_post_filter = apply_min_market_cap_filter(
        scores_by_day=scores_by_day,
        market_cap_wide=market_cap_wide,
        min_market_cap=float(MIN_MARKET_CAP),
    )

    px_exec_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first")
        .sort_index()
        .ffill()
    )
    split_wide = canonical.pivot_table(index="date", columns="ticker", values="split_factor", aggfunc="first").sort_index()
    split_changed = (split_wide / split_wide.shift(1)).replace([np.inf, -np.inf], np.nan)
    has_split = (split_changed - 1.0).abs() > 1e-12
    px_raw_wide = canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first").sort_index()
    split_event_wide = (px_raw_wide.shift(1) / px_raw_wide).where(has_split)

    for col in ["i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl", "xbar_value", "xbar_ucl", "xbar_lcl", "r_value", "r_ucl"]:
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
    strong_rule_wide = canonical.pivot_table(index="date", columns="ticker", values="_strong_rule", aggfunc="first").sort_index()

    return (
        px_exec_wide,
        split_event_wide,
        i_wide,
        z_wide,
        any_rule_wide,
        strong_rule_wide,
        scores_by_day,
        cash_log_daily,
        median_pre_filter,
        median_post_filter,
    )


def run_variant_with_positions(
    px_exec_wide: pd.DataFrame,
    split_event_wide: pd.DataFrame,
    i_wide: pd.DataFrame,
    z_wide: pd.DataFrame,
    any_rule_wide: pd.DataFrame,
    strong_rule_wide: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    cash_log_daily: pd.Series,
    cfg: BacktestConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[pd.Timestamp, dict[str, float]], dict[pd.Timestamp, dict[str, float]]]:
    friction = cfg.friction_one_way_bps / 10_000.0
    rebalance_cadence = max(int(cfg.rebalance_cadence), 1)
    trading_dates = list(px_exec_wide.index.intersection(cash_log_daily.index).sort_values())
    if len(trading_dates) < 30:
        raise RuntimeError("Poucas datas de interseção para simular variante.")

    cash_free = float(cfg.base_capital)
    pending_cash: dict[pd.Timestamp, float] = {}
    lots = []
    rows: list[dict[str, float | int | str]] = []
    total_cost = 0.0
    quarantine: set[str] = set()
    quarantine_entries = 0
    initialized_c3 = False

    def25 = 0
    def50 = 0
    def100 = 0
    regime_hist: list[float] = []
    defensive_state = False
    in_streak = 0
    out_streak = 0

    events_def: list[dict[str, object]] = []
    events_split: list[dict[str, object]] = []
    events_trim: list[dict[str, object]] = []

    values_by_day: dict[pd.Timestamp, dict[str, float]] = {}
    weights_by_day: dict[pd.Timestamp, dict[str, float]] = {}

    for i, d in enumerate(trading_dates):
        matured = float(pending_cash.pop(d, 0.0))
        if matured > 0:
            cash_free += matured

        split_row = split_event_wide.loc[d] if d in split_event_wide.index else pd.Series(dtype=float)
        lots = _apply_split_adjustment(lots, split_row, d, "C4", events_split)

        price_row = px_exec_wide.loc[d]
        prev_d = trading_dates[i - 1] if i > 0 else d
        prev2_d = trading_dates[i - 2] if i > 1 else prev_d
        prev3_d = trading_dates[i - 3] if i > 2 else prev2_d
        prev_scores = scores_by_day.get(prev_d)
        held = set(split_lots_by_ticker(lots).keys())

        # Camada 1: defensiva permanente
        candidates: list[tuple[str, int, float]] = []
        if defensive_state and held:
            for tk in held:
                z_prev = float(z_wide.at[prev_d, tk]) if (prev_d in z_wide.index and tk in z_wide.columns) else np.nan
                z_prev2 = float(z_wide.at[prev2_d, tk]) if (prev2_d in z_wide.index and tk in z_wide.columns) else np.nan
                z_prev3 = float(z_wide.at[prev3_d, tk]) if (prev3_d in z_wide.index and tk in z_wide.columns) else np.nan
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

            for tk, score, z_prev in candidates:
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
                lots, proceeds, cost, sold_shares = sell_ticker_fifo(
                    ticker=tk,
                    target_value_to_sell=target_sell,
                    lots=lots,
                    price_row=price_row,
                    friction=friction,
                    trading_dates=trading_dates,
                    i=i,
                    settlement_days=cfg.settlement_days,
                    pending_cash=pending_cash,
                )
                if sold_shares > 0:
                    total_cost += cost
                    quarantine.add(tk)
                    quarantine_entries += 1
                    events_def.append(
                        {
                            "date": d,
                            "variant": "C4",
                            "ticker": tk,
                            "event": "defensive_sell",
                            "score": int(score),
                            "z_prev": float(z_prev),
                            "sell_pct": float(pct),
                            "sold_shares": int(sold_shares),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "settle_dt": _settlement_date(trading_dates, i, cfg.settlement_days),
                        }
                    )

        # Camada 2: rebalance estilo C2
        held = set(split_lots_by_ticker(lots).keys())
        is_rebalance_day = (i % rebalance_cadence) == 0
        if is_rebalance_day:
            target = _select_c2_target(prev_scores, held, cfg.top_n, cfg.buffer_k, quarantine=quarantine)
            target_set = set(target)
            to_sell = sorted([t for t in held if t not in target_set])
            for tk in to_sell:
                lots, proceeds, cost, sold_shares = sell_all_ticker(
                    ticker=tk,
                    lots=lots,
                    price_row=price_row,
                    friction=friction,
                    trading_dates=trading_dates,
                    i=i,
                    settlement_days=cfg.settlement_days,
                    pending_cash=pending_cash,
                )
                if sold_shares > 0:
                    total_cost += cost
                    events_def.append(
                        {
                            "date": d,
                            "variant": "C4",
                            "ticker": tk,
                            "event": "rebalance_sell",
                            "score": np.nan,
                            "z_prev": np.nan,
                            "sell_pct": 1.0,
                            "sold_shares": int(sold_shares),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "settle_dt": _settlement_date(trading_dates, i, cfg.settlement_days),
                        }
                    )
        else:
            target = sorted(list(held))

        # Camada 2.5: trim de concentracao (C4)
        if is_rebalance_day and target:
            equity_now_trim = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
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
                    lots, proceeds, cost, sold_shares = sell_ticker_fifo(
                        ticker=tk,
                        target_value_to_sell=target_sell,
                        lots=lots,
                        price_row=price_row,
                        friction=friction,
                        trading_dates=trading_dates,
                        i=i,
                        settlement_days=cfg.settlement_days,
                        pending_cash=pending_cash,
                    )
                    if sold_shares <= 0:
                        continue
                    total_cost += cost
                    weight_before = (current_val / equity_now_trim) if equity_now_trim > 0 else 0.0
                    events_trim.append(
                        {
                            "date": d,
                            "variant": "C4",
                            "ticker": tk,
                            "event": "concentration_trim",
                            "weight_before": float(weight_before),
                            "weight_cap": float(cfg.max_weight_cap),
                            "value_sold_gross": float(target_sell),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "sold_shares": int(sold_shares),
                            "settle_dt": _settlement_date(trading_dates, i, cfg.settlement_days),
                        }
                    )

        # Compras (C4)
        held = set(split_lots_by_ticker(lots).keys())
        if is_rebalance_day and target:
            equity_now = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
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
                from run_backtest_variants_us import Lot
                lots.append(Lot(ticker=tk, buy_date=d, shares=shares_to_buy, buy_price=px))

        cash_log = float(cash_log_daily.get(d, 0.0))
        cash_ret = float(np.expm1(cash_log))
        if cash_free > 0:
            cash_free *= (1.0 + cash_ret)

        # Atualiza regime defensivo para D+1
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

        # Snapshot por ticker (valor e peso)
        holdings_value = lots_market_value(lots, price_row)
        by_ticker = split_lots_by_ticker(lots)
        conc_vals = []
        value_map: dict[str, float] = {}
        if holdings_value > 0:
            for tk in by_ticker:
                tv = ticker_value(lots, tk, price_row)
                conc_vals.append(tv)
                value_map[tk] = float(tv)
        equity_end = cash_free + sum(pending_cash.values()) + holdings_value
        max_conc = (max(conc_vals) / equity_end) if conc_vals and equity_end > 0 else 0.0
        weight_map = {tk: (val / equity_end if equity_end > 0 else 0.0) for tk, val in value_map.items()}
        values_by_day[d] = value_map
        weights_by_day[d] = weight_map

        rows.append(
            {
                "date": d,
                "variant": "C4",
                "equity": float(equity_end),
                "cash_free": float(cash_free),
                "cash_pending": float(sum(pending_cash.values())),
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

    return (
        curve,
        pd.DataFrame(events_def),
        pd.DataFrame(events_split),
        pd.DataFrame(events_trim),
        values_by_day,
        weights_by_day,
    )


def _drawdown_windows(curve: pd.DataFrame, threshold: float = -0.10) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    eq = curve[["date", "equity"]].copy()
    eq["running_max"] = eq["equity"].cummax()
    eq["dd"] = (eq["equity"] / eq["running_max"]) - 1.0
    windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    start = None
    for _, r in eq.iterrows():
        if r["dd"] <= threshold and start is None:
            start = r["date"]
        elif r["dd"] > threshold and start is not None:
            windows.append((start, r["date"]))
            start = None
    if start is not None:
        windows.append((start, eq["date"].iloc[-1]))
    return windows


def _mdd_window(curve: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    eq = curve[["date", "equity"]].copy()
    eq["running_max"] = eq["equity"].cummax()
    eq["dd"] = (eq["equity"] / eq["running_max"]) - 1.0
    trough_idx = int(eq["dd"].idxmin())
    trough_date = pd.Timestamp(eq.loc[trough_idx, "date"])
    peak_eq = float(eq.loc[:trough_idx, "equity"].max())
    peak_idx = int(eq.loc[:trough_idx][eq.loc[:trough_idx, "equity"] == peak_eq].index[0])
    peak_date = pd.Timestamp(eq.loc[peak_idx, "date"])
    return peak_date, trough_date


def _contrib_between(
    values_by_day: dict[pd.Timestamp, dict[str, float]],
    d0: pd.Timestamp,
    d1: pd.Timestamp,
) -> pd.DataFrame:
    v0 = values_by_day.get(d0, {})
    v1 = values_by_day.get(d1, {})
    tickers = sorted(list(set(v0.keys()).union(v1.keys())))
    rows = []
    for tk in tickers:
        a = float(v0.get(tk, 0.0))
        b = float(v1.get(tk, 0.0))
        rows.append({"ticker": tk, "value_start": a, "value_end": b, "delta_value": b - a})
    out = pd.DataFrame(rows).sort_values("delta_value")
    total = float(out["delta_value"].sum()) if not out.empty else 0.0
    if abs(total) > 1e-12:
        out["delta_pct_of_total"] = out["delta_value"] / total * 100.0
    else:
        out["delta_pct_of_total"] = 0.0
    out["from_date"] = d0
    out["to_date"] = d1
    return out


def _streaks_top1(daily_conc: pd.DataFrame) -> pd.DataFrame:
    df = daily_conc[["date", "top1_ticker"]].copy()
    df["top1_ticker"] = df["top1_ticker"].fillna("")
    df["chg"] = (df["top1_ticker"] != df["top1_ticker"].shift(1)).astype(int)
    df["gid"] = df["chg"].cumsum()
    st = df.groupby(["gid", "top1_ticker"], as_index=False).size().rename(columns={"size": "streak_days"})
    st = st[st["top1_ticker"] != ""].copy()
    return st.sort_values("streak_days", ascending=False).reset_index(drop=True)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (
        px_exec_wide,
        split_event_wide,
        i_wide,
        z_wide,
        any_rule_wide,
        strong_rule_wide,
        scores_by_day,
        cash_log_daily,
        median_pre_filter,
        median_post_filter,
    ) = _build_inputs()

    # Curva oficial (gate de equivalência)
    curve_ref, _, _, _ = run_variant(
        variant="C4",
        px_exec_wide=px_exec_wide,
        split_event_wide=split_event_wide,
        i_wide=i_wide,
        z_wide=z_wide,
        any_rule_wide=any_rule_wide,
        strong_rule_wide=strong_rule_wide,
        scores_by_day=scores_by_day,
        cash_log_daily=cash_log_daily,
        cfg=CFG,
    )

    # Curva instrumentada
    curve, events_def, events_split, events_trim, values_by_day, weights_by_day = run_variant_with_positions(
        px_exec_wide=px_exec_wide,
        split_event_wide=split_event_wide,
        i_wide=i_wide,
        z_wide=z_wide,
        any_rule_wide=any_rule_wide,
        strong_rule_wide=strong_rule_wide,
        scores_by_day=scores_by_day,
        cash_log_daily=cash_log_daily,
        cfg=CFG,
    )

    # Gate equivalência
    m = curve_ref.merge(curve, on="date", suffixes=("_ref", "_inst"), how="inner")
    eq_cols = ["equity", "cash_free", "cash_pending", "n_tickers", "max_concentration", "cost_total_cum"]
    max_abs_diff = {c: float((m[f"{c}_ref"] - m[f"{c}_inst"]).abs().max()) for c in eq_cols}
    equivalent = all(v == 0.0 for v in max_abs_diff.values())

    # Daily concentration CSV
    trim_counts = (
        events_trim.assign(date=pd.to_datetime(events_trim["date"]).dt.normalize())
        .groupby("date")
        .size()
        .rename("trim_events_today")
        if not events_trim.empty
        else pd.Series(dtype=int)
    )
    rows = []
    for _, r in curve.iterrows():
        d = pd.Timestamp(r["date"])
        weights = weights_by_day.get(d, {})
        if weights:
            top1_ticker = max(weights, key=weights.get)
            top1_weight = float(weights[top1_ticker]) * 100.0
            n_gt10 = int(sum(1 for v in weights.values() if v > 0.10))
        else:
            top1_ticker = ""
            top1_weight = 0.0
            n_gt10 = 0
        rows.append(
            {
                "date": d,
                "equity": float(r["equity"]),
                "top1_ticker": top1_ticker,
                "top1_weight_pct": float(top1_weight),
                "n_tickers": int(r["n_tickers"]),
                "n_positions_gt_10pct": int(n_gt10),
                "max_concentration_pct": float(r["max_concentration"] * 100.0),
                "trim_events_today": int(trim_counts.get(d, 0)),
                "regime_defensive_used": int(r["regime_defensive_used"]),
            }
        )
    daily_conc = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    daily_conc_csv = OUT_DIR / "t021_daily_concentration.csv"
    daily_conc.to_csv(daily_conc_csv, index=False)

    # Decomposição do MDD principal
    mdd_start, mdd_trough = _mdd_window(curve)
    dd_main = _contrib_between(values_by_day, mdd_start, mdd_trough)
    dd_main["window_type"] = "mdd_main"

    # Todos os drawdowns >10%
    dd_windows = _drawdown_windows(curve, threshold=-0.10)
    parts = [dd_main]
    for a, b in dd_windows:
        part = _contrib_between(values_by_day, a, b)
        part["window_type"] = "dd_gt_10pct"
        parts.append(part)
    dd_all = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    dd_csv = OUT_DIR / "t021_drawdown_decomposition.csv"
    dd_all.to_csv(dd_csv, index=False)

    # Métricas concentração crônica / persistência
    st = _streaks_top1(daily_conc)
    top1_distinct = int(daily_conc["top1_ticker"].replace("", np.nan).nunique())
    top1_med = float(daily_conc["top1_weight_pct"].median())
    top1_p90 = float(daily_conc["top1_weight_pct"].quantile(0.90))
    streak_avg = float(st["streak_days"].mean()) if not st.empty else 0.0
    streak_p90 = float(st["streak_days"].quantile(0.90)) if not st.empty else 0.0

    # Efetividade trims
    if events_trim.empty:
        trim_stats = {
            "total_trims": 0,
            "weight_before_mean_pct": 0.0,
            "weight_before_p90_pct": 0.0,
            "trims_per_month": {},
            "avg_days_until_next_exceedance": 0.0,
        }
    else:
        ev = events_trim.copy()
        ev["date"] = pd.to_datetime(ev["date"]).dt.normalize()
        ev["weight_before_pct"] = pd.to_numeric(ev["weight_before"], errors="coerce") * 100.0
        month_counts = ev.groupby(ev["date"].dt.to_period("M")).size()
        # dias até próxima ultrapassagem de 6% para o mesmo ticker
        gaps = []
        for _, r in ev.iterrows():
            d = pd.Timestamp(r["date"])
            tk = str(r["ticker"])
            sub = daily_conc[(daily_conc["date"] > d) & (daily_conc["top1_ticker"] == tk) & (daily_conc["top1_weight_pct"] > 6.0)]
            if not sub.empty:
                gaps.append(int((pd.Timestamp(sub.iloc[0]["date"]) - d).days))
        trim_stats = {
            "total_trims": int(len(ev)),
            "weight_before_mean_pct": float(ev["weight_before_pct"].mean()),
            "weight_before_p90_pct": float(ev["weight_before_pct"].quantile(0.90)),
            "trims_per_month": {str(k): int(v) for k, v in month_counts.items()},
            "avg_days_until_next_exceedance": float(np.mean(gaps)) if gaps else 0.0,
        }

    # Plotly
    p1 = px.line(
        daily_conc,
        x="date",
        y="top1_weight_pct",
        title="T-021 - Top1 weight (%) ao longo do tempo",
        labels={"top1_weight_pct": "Top1 weight (%)", "date": "Data"},
    )
    for y, color in [(6, "green"), (10, "orange"), (20, "red")]:
        p1.add_hline(y=y, line_dash="dash", line_color=color)
    p1.write_html(str(OUT_DIR / "plot_t021_top1_weight_timeseries.html"), include_plotlyjs="cdn")

    # Waterfall/bar contribuintes MDD
    neg5 = dd_main.nsmallest(5, "delta_value")
    pos5 = dd_main.nlargest(5, "delta_value")
    wb = pd.concat([neg5, pos5], ignore_index=True)
    p2 = px.bar(
        wb,
        x="ticker",
        y="delta_value",
        color="delta_value",
        title=f"T-021 - Contribuicao por ticker no MDD principal ({mdd_start.date()} -> {mdd_trough.date()})",
        labels={"delta_value": "Delta valor ($)", "ticker": "Ticker"},
    )
    p2.write_html(str(OUT_DIR / "plot_t021_mdd_top_contributors.html"), include_plotlyjs="cdn")

    # Heatmap top-50 por exposição média
    all_tickers = {}
    for d, wm in weights_by_day.items():
        for tk, w in wm.items():
            all_tickers.setdefault(tk, []).append(w)
    top50 = sorted(all_tickers.keys(), key=lambda t: np.mean(all_tickers[t]), reverse=True)[:50]
    hm_rows = []
    for _, r in daily_conc.iterrows():
        d = pd.Timestamp(r["date"])
        wm = weights_by_day.get(d, {})
        row = {"date": d}
        for tk in top50:
            row[tk] = float(wm.get(tk, 0.0) * 100.0)
        hm_rows.append(row)
    hm = pd.DataFrame(hm_rows).set_index("date")
    p3 = px.imshow(
        hm.T,
        aspect="auto",
        color_continuous_scale="Viridis",
        title="T-021 - Heatmap de concentração (Top-50 tickers por exposição média)",
        labels={"x": "Data", "y": "Ticker", "color": "Weight (%)"},
    )
    p3.write_html(str(OUT_DIR / "plot_t021_heatmap_top50.html"), include_plotlyjs="cdn")

    # Histograma de duração como top-1
    p4 = px.histogram(
        st,
        x="streak_days",
        nbins=30,
        title="T-021 - Distribuição de duração como top-1 (streak_days)",
        labels={"streak_days": "Dias consecutivos como top-1"},
    )
    p4.write_html(str(OUT_DIR / "plot_t021_top1_streak_hist.html"), include_plotlyjs="cdn")

    report = {
        "task_id": "T-021",
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "params": {
            "variant": "C4",
            "top_n": CFG.top_n,
            "rebalance_cadence": CFG.rebalance_cadence,
            "buffer_k": CFG.buffer_k,
            "k_damp": CFG.k_damp,
            "max_weight_cap": CFG.max_weight_cap,
            "friction_one_way_bps": CFG.friction_one_way_bps,
            "settlement_days": CFG.settlement_days,
            "base_capital": CFG.base_capital,
            "min_market_cap": MIN_MARKET_CAP,
        },
        "inputs": {
            "canonical_us": str(IN_CANONICAL),
            "macro_us": str(IN_MACRO),
            "scores_m3_us": str(IN_SCORES),
            "blacklist_us": str(IN_BLACKLIST),
            "sha256_inputs": {
                "canonical_us": _sha256(IN_CANONICAL),
                "macro_us": _sha256(IN_MACRO),
                "scores_m3_us": _sha256(IN_SCORES),
                "blacklist_us": _sha256(IN_BLACKLIST),
            },
        },
        "counts": {
            "daily_rows": int(len(daily_conc)),
            "drawdown_decomposition_rows": int(len(dd_all)),
            "events_split_rows": int(len(events_split)),
            "events_trim_rows": int(len(events_trim)),
            "median_scored_tickers_pre_filter": float(median_pre_filter),
            "median_scored_tickers_post_filter": float(median_post_filter),
        },
        "mdd": {
            "start_date": str(mdd_start.date()),
            "trough_date": str(mdd_trough.date()),
            "mdd_pct": float(((curve["equity"] / curve["equity"].cummax()) - 1.0).min() * 100.0),
            "top5_negative_contributors": neg5[["ticker", "delta_value", "delta_pct_of_total"]].to_dict(orient="records"),
            "top5_positive_contributors": pos5[["ticker", "delta_value", "delta_pct_of_total"]].to_dict(orient="records"),
        },
        "concentration": {
            "top1_weight_median_pct": top1_med,
            "top1_weight_p90_pct": top1_p90,
            "top1_ticker_distinct_count": top1_distinct,
            "top1_streak_avg_days": streak_avg,
            "top1_streak_p90_days": streak_p90,
        },
        "trim_effectiveness": trim_stats,
        "equivalence_gate": {
            "max_abs_diff": max_abs_diff,
            "is_exact_match": bool(equivalent),
        },
        "outputs": {
            "daily_concentration_csv": "backtest/results/t021_daily_concentration.csv",
            "drawdown_decomposition_csv": "backtest/results/t021_drawdown_decomposition.csv",
            "plot_top1_weight_html": "backtest/results/plot_t021_top1_weight_timeseries.html",
            "plot_mdd_contributors_html": "backtest/results/plot_t021_mdd_top_contributors.html",
            "plot_heatmap_html": "backtest/results/plot_t021_heatmap_top50.html",
            "plot_streak_hist_html": "backtest/results/plot_t021_top1_streak_hist.html",
            "report_json": "backtest/results/t021_concentration_report.json",
        },
        "gates": {
            "required_inputs_exist": all(p.exists() for p in [IN_CANONICAL, IN_MACRO, IN_SCORES, IN_BLACKLIST]),
            "instrumented_curve_equivalent_official": bool(equivalent),
            "daily_concentration_non_empty": not daily_conc.empty,
            "drawdown_decomposition_non_empty": not dd_all.empty,
            "outputs_written": all(
                p.exists()
                for p in [
                    OUT_DIR / "t021_daily_concentration.csv",
                    OUT_DIR / "t021_drawdown_decomposition.csv",
                    OUT_DIR / "plot_t021_top1_weight_timeseries.html",
                    OUT_DIR / "plot_t021_mdd_top_contributors.html",
                    OUT_DIR / "plot_t021_heatmap_top50.html",
                    OUT_DIR / "plot_t021_top1_streak_hist.html",
                    OUT_DIR / "t021_concentration_report.json",
                ]
            ),
        },
    }
    report_path = OUT_DIR / "t021_concentration_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report["gates"]["outputs_written"] = all(
        p.exists()
        for p in [
            OUT_DIR / "t021_daily_concentration.csv",
            OUT_DIR / "t021_drawdown_decomposition.csv",
            OUT_DIR / "plot_t021_top1_weight_timeseries.html",
            OUT_DIR / "plot_t021_mdd_top_contributors.html",
            OUT_DIR / "plot_t021_heatmap_top50.html",
            OUT_DIR / "plot_t021_top1_streak_hist.html",
            OUT_DIR / "t021_concentration_report.json",
        ]
    )
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if not all(report["gates"].values()):
        failed = [k for k, v in report["gates"].items() if not v]
        raise RuntimeError(f"T-021 FAIL gates: {failed}")

    print("T-021 PASS")
    print(json.dumps({"gates": report["gates"], "daily_rows": len(daily_conc), "dd_rows": len(dd_all)}, indent=2))


if __name__ == "__main__":
    main()
