#!/usr/bin/env python3
"""T-027: Compare C4 pure vs C4 + ML trigger (hysteresis on y_proba_cash)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import sys

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.engine import apply_hysteresis
from backtest.run_backtest_variants_us import (
    TRAIN_END,
    BacktestConfig,
    Lot,
    _apply_split_adjustment,
    _build_z_table,
    _band_from_z,
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
    run_variant,
    sell_all_ticker,
    sell_ticker_fifo,
    split_lots_by_ticker,
    ticker_value,
)

PRED_PATH = ROOT / "data" / "features" / "predictions_us.parquet"
TRIGGER_CFG_PATH = ROOT / "config" / "ml_trigger_us.json"
WINNER_PATH = ROOT / "config" / "winner_us.json"
OUT_CURVE_PURE = ROOT / "backtest" / "results" / "curve_T027_C4_pure.csv"
OUT_CURVE_TRIGGER = ROOT / "backtest" / "results" / "curve_T027_C4_trigger.csv"
OUT_PLOT = ROOT / "backtest" / "results" / "plot_t027_c4_pure_vs_trigger.html"
OUT_REPORT = ROOT / "data" / "features" / "t027_trigger_comparison_report.json"


@dataclass
class TriggerConfig:
    thr: float
    h_in: int
    h_out: int


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _annualized_sharpe(curve: pd.DataFrame) -> float:
    if curve.empty or len(curve) < 3:
        return 0.0
    eq = pd.to_numeric(curve["equity"], errors="coerce")
    rets = eq.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    if rets.empty:
        return 0.0
    mu = float(rets.mean())
    sigma = float(rets.std(ddof=0))
    if sigma <= 0:
        return 0.0
    return float((mu / sigma) * np.sqrt(252.0))


def _drawdown_series(curve: pd.DataFrame) -> pd.Series:
    if curve.empty:
        return pd.Series(dtype=float)
    eq = pd.to_numeric(curve["equity"], errors="coerce")
    running_max = eq.cummax().replace(0.0, np.nan)
    dd = (eq / running_max) - 1.0
    return dd.fillna(0.0)


def _metrics_for_curve(curve: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for split in ("TRAIN", "HOLDOUT", "GLOBAL"):
        if split == "TRAIN":
            sub = curve[curve["date"] <= TRAIN_END].copy()
        elif split == "HOLDOUT":
            sub = curve[curve["date"] > TRAIN_END].copy()
        else:
            sub = curve.copy()
        if len(sub) < 2:
            out[split] = {}
            continue
        cagr, mdd = _curve_metrics(sub)
        out[split] = {
            "equity_final": float(sub["equity"].iloc[-1]),
            "cagr_pct": float(cagr * 100.0),
            "mdd_pct": float(mdd * 100.0),
            "sharpe": _annualized_sharpe(sub),
            "cost_total": float(sub["cost_total_cum"].iloc[-1]),
            "defensive_days_pct": float(sub["regime_defensive_used"].mean() * 100.0),
            "days": int(len(sub)),
        }
    return out


def _write_plotly_comparison(curve_pure: pd.DataFrame, curve_trigger: pd.DataFrame, out_path: Path) -> None:
    p = curve_pure.copy()
    t = curve_trigger.copy()
    p["date"] = pd.to_datetime(p["date"], errors="coerce")
    t["date"] = pd.to_datetime(t["date"], errors="coerce")

    p_dd = _drawdown_series(p) * 100.0
    t_dd = _drawdown_series(t) * 100.0

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=("Equity Base 100", "Drawdown (%)"),
    )
    fig.add_trace(
        go.Scatter(x=p["date"], y=p["equity_base100"], mode="lines", name="C4 puro"),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=t["date"], y=t["equity_base100"], mode="lines", name="C4 + trigger"),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=p["date"], y=p_dd, mode="lines", name="DD C4 puro"),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=t["date"], y=t_dd, mode="lines", name="DD C4 + trigger"),
        row=2,
        col=1,
    )
    fig.update_layout(
        title="T-027v2: Comparação C4 puro vs C4 + ML Trigger",
        height=900,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="Base 100", row=1, col=1)
    fig.update_yaxes(title_text="Drawdown (%)", row=2, col=1)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(out_path), include_plotlyjs="cdn")


def _load_winner_cfg() -> BacktestConfig:
    winner = json.loads(WINNER_PATH.read_text(encoding="utf-8"))
    snap = winner["winner_config_snapshot"]
    return BacktestConfig(
        top_n=int(snap["top_n"]),
        buffer_k=int(snap["buffer_k"]),
        rebalance_cadence=int(snap["rebalance_cadence"]),
        friction_one_way_bps=float(snap["friction_one_way_bps"]),
        settlement_days=int(snap["settlement_days"]),
        base_capital=float(snap["base_capital"]),
        k_damp=float(snap["k_damp"]),
        max_weight_cap=float(snap["max_weight_cap"]),
    )


def _load_trigger_signal(trading_dates: pd.DatetimeIndex) -> tuple[pd.Series, TriggerConfig]:
    pred = pd.read_parquet(PRED_PATH).copy()
    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.normalize()
    pred["y_proba_cash"] = pd.to_numeric(pred["y_proba_cash"], errors="coerce")
    pred = pred.dropna(subset=["date", "y_proba_cash"]).sort_values("date")

    cfg_json = json.loads(TRIGGER_CFG_PATH.read_text(encoding="utf-8"))
    params = cfg_json["selected_params"]
    cfg = TriggerConfig(thr=float(params["thr"]), h_in=int(params["h_in"]), h_out=int(params["h_out"]))

    raw = apply_hysteresis(pred["y_proba_cash"], thr=cfg.thr, h_in=cfg.h_in, h_out=cfg.h_out)
    signal = pd.Series(raw.values, index=pred["date"], dtype="int64")
    signal = signal[~signal.index.duplicated(keep="last")].sort_index()
    # Execution uses yesterday signal on today's trades.
    signal_exec = signal.shift(1).fillna(0).astype("int64")
    signal_exec = signal_exec.reindex(trading_dates).ffill().fillna(0).astype("int64")
    signal_exec.index = pd.to_datetime(signal_exec.index).normalize()
    return signal_exec, cfg


def _run_variant_with_trigger(
    px_exec_wide: pd.DataFrame,
    split_event_wide: pd.DataFrame,
    i_wide: pd.DataFrame,
    z_wide: pd.DataFrame,
    any_rule_wide: pd.DataFrame,
    strong_rule_wide: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    cash_log_daily: pd.Series,
    cfg: BacktestConfig,
    cash_signal_exec: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    friction = cfg.friction_one_way_bps / 10_000.0
    rebalance_cadence = max(int(cfg.rebalance_cadence), 1)
    trading_dates = list(px_exec_wide.index.intersection(cash_log_daily.index).sort_values())
    if len(trading_dates) < 30:
        raise RuntimeError("Poucas datas de interseção para simular variante.")

    cash_free = float(cfg.base_capital)
    pending_cash: dict[pd.Timestamp, float] = {}
    lots: list[Lot] = []
    rows: list[dict[str, Any]] = []
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

    events_def: list[dict[str, Any]] = []
    events_split: list[dict[str, Any]] = []
    events_trim: list[dict[str, Any]] = []

    for i, d in enumerate(trading_dates):
        matured = float(pending_cash.pop(d, 0.0))
        if matured > 0:
            cash_free += matured

        split_row = split_event_wide.loc[d] if d in split_event_wide.index else pd.Series(dtype=float)
        lots = _apply_split_adjustment(lots, split_row, d, "C4_TRIGGER", events_split)

        price_row = px_exec_wide.loc[d]
        prev_d = trading_dates[i - 1] if i > 0 else d
        prev2_d = trading_dates[i - 2] if i > 1 else prev_d
        prev3_d = trading_dates[i - 3] if i > 2 else prev2_d
        prev_scores = scores_by_day.get(prev_d)
        held = set(split_lots_by_ticker(lots).keys())
        ml_cash_mode = bool(int(cash_signal_exec.get(d, 0)) == 1)

        # If ML cash mode is active, liquidate everything and skip rebalance/buys.
        if ml_cash_mode and held:
            for tk in sorted(list(held)):
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
                            "variant": "C4_TRIGGER",
                            "ticker": tk,
                            "event": "ml_cash_sell",
                            "score": np.nan,
                            "z_prev": np.nan,
                            "sell_pct": 1.0,
                            "sold_shares": int(sold_shares),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "settle_dt": _settlement_date(trading_dates, i, cfg.settlement_days),
                        }
                    )

        # Camada 1: defensiva permanente (só quando trigger não força caixa).
        candidates: list[tuple[str, int, float]] = []
        held = set(split_lots_by_ticker(lots).keys())
        if (not ml_cash_mode) and defensive_state and held:
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
                            "variant": "C4_TRIGGER",
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

        held = set(split_lots_by_ticker(lots).keys())
        is_rebalance_day = (i % rebalance_cadence) == 0
        target: list[str] = []
        if (not ml_cash_mode) and is_rebalance_day:
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
                            "variant": "C4_TRIGGER",
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
        elif not is_rebalance_day:
            target = sorted(list(held))

        if (not ml_cash_mode) and is_rebalance_day and target:
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
                            "variant": "C4_TRIGGER",
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

        held = set(split_lots_by_ticker(lots).keys())
        if (
            (not ml_cash_mode)
            and is_rebalance_day
            and target
            and (not held or True)
        ):
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
                lots.append(Lot(ticker=tk, buy_date=d, shares=shares_to_buy, buy_price=px))

        cash_log = float(cash_log_daily.get(d, 0.0))
        cash_ret = float(np.expm1(cash_log))
        if cash_free > 0:
            cash_free *= (1.0 + cash_ret)

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
                tv = ticker_value(lots, tk, price_row)
                conc_vals.append(tv)
        equity_end = cash_free + sum(pending_cash.values()) + holdings_value
        max_conc = (max(conc_vals) / equity_end) if conc_vals and equity_end > 0 else 0.0
        rows.append(
            {
                "date": d,
                "variant": "C4_TRIGGER",
                "equity": float(equity_end),
                "cash_free": float(cash_free),
                "cash_pending": float(sum(pending_cash.values())),
                "n_tickers": int(len(by_ticker)),
                "max_concentration": float(max_conc),
                "cost_total_cum": float(total_cost),
                "ret_cash": float(cash_ret),
                "regime_defensive_used": int(defensive_state),
                "ml_cash_signal_used": int(ml_cash_mode),
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
    return curve, pd.DataFrame(events_def), pd.DataFrame(events_split), pd.DataFrame(events_trim)


def main() -> None:
    cfg = _load_winner_cfg()
    winner = json.loads(WINNER_PATH.read_text(encoding="utf-8"))
    winner_snap = winner["winner_config_snapshot"]
    min_market_cap = float(winner_snap["min_market_cap"])
    canonical, macro, scores = load_inputs()
    blacklist = load_blacklist(ROOT / "config" / "blacklist_us.json")
    cash_log_daily = build_cash_log_daily(macro)
    scores_by_day = build_scores_by_day(scores=scores, blacklist=blacklist)
    market_cap_wide = build_market_cap_wide(canonical)
    scores_by_day, median_pre_filter, median_post_filter = apply_min_market_cap_filter(
        scores_by_day=scores_by_day,
        market_cap_wide=market_cap_wide,
        min_market_cap=min_market_cap,
    )

    px_exec_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first").sort_index().ffill()
    )
    split_wide = canonical.pivot_table(index="date", columns="ticker", values="split_factor", aggfunc="first").sort_index()
    split_changed = (split_wide / split_wide.shift(1)).replace([np.inf, -np.inf], np.nan)
    has_split = (split_changed - 1.0).abs() > 1e-12
    px_raw_wide = canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first").sort_index()
    split_event_wide = (px_raw_wide.shift(1) / px_raw_wide).where(has_split)

    for col in [
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
    ]:
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

    curve_pure, _, _, _ = run_variant(
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
    curve_pure.to_csv(OUT_CURVE_PURE, index=False)

    signal_exec, trig_cfg = _load_trigger_signal(px_exec_wide.index)
    curve_trigger, _, _, _ = _run_variant_with_trigger(
        px_exec_wide=px_exec_wide,
        split_event_wide=split_event_wide,
        i_wide=i_wide,
        z_wide=z_wide,
        any_rule_wide=any_rule_wide,
        strong_rule_wide=strong_rule_wide,
        scores_by_day=scores_by_day,
        cash_log_daily=cash_log_daily,
        cfg=cfg,
        cash_signal_exec=signal_exec,
    )
    curve_trigger.to_csv(OUT_CURVE_TRIGGER, index=False)
    _write_plotly_comparison(curve_pure, curve_trigger, OUT_PLOT)

    pure_metrics = _metrics_for_curve(curve_pure)
    trigger_metrics = _metrics_for_curve(curve_trigger)
    delta_holdout_cagr = float(trigger_metrics["HOLDOUT"]["cagr_pct"] - pure_metrics["HOLDOUT"]["cagr_pct"])
    delta_holdout_mdd = float(trigger_metrics["HOLDOUT"]["mdd_pct"] - pure_metrics["HOLDOUT"]["mdd_pct"])

    holdout_ok = (
        trigger_metrics["HOLDOUT"]["cagr_pct"] > pure_metrics["HOLDOUT"]["cagr_pct"]
        and trigger_metrics["HOLDOUT"]["mdd_pct"] >= pure_metrics["HOLDOUT"]["mdd_pct"]
    )

    winner_holdout = winner["holdout_metrics"]
    pure_holdout = pure_metrics["HOLDOUT"]
    gate_reconcile = (
        abs(float(pure_holdout["cagr_pct"]) - float(winner_holdout["cagr_pct"])) <= 0.01
        and abs(float(pure_holdout["mdd_pct"]) - float(winner_holdout["mdd_pct"])) <= 0.01
    )

    trigger_stats = {
        "transition_rate_global": float((signal_exec.astype(float).diff().abs() > 0).mean()),
        "pct_days_cash_global": float(signal_exec.mean() * 100.0),
        "counts": {
            "n_days": int(len(signal_exec)),
            "n_cash_days": int(signal_exec.sum()),
            "n_transitions": int((signal_exec.astype(float).diff().abs() > 0).sum()),
        },
    }

    report = {
        "task_id": "T-027",
        "decision_ref": "D-023",
        "winner_reference": "T-024 / D-021",
        "inputs": {
            "paths": {
                "winner_us": str(WINNER_PATH),
                "predictions_us": str(PRED_PATH),
                "ml_trigger_us": str(TRIGGER_CFG_PATH),
                "curve_c4_k10": str(ROOT / "backtest" / "results" / "curve_C4_K10.csv"),
            },
            "sha256_inputs": {
                "winner_us": _sha256(WINNER_PATH),
                "predictions_us": _sha256(PRED_PATH),
                "ml_trigger_us": _sha256(TRIGGER_CFG_PATH),
                "curve_c4_k10": _sha256(ROOT / "backtest" / "results" / "curve_C4_K10.csv"),
            },
        },
        "outputs": {
            "paths": {
                "curve_c4_pure": str(OUT_CURVE_PURE),
                "curve_c4_trigger": str(OUT_CURVE_TRIGGER),
                "plotly_comparison": str(OUT_PLOT),
                "report": str(OUT_REPORT),
            },
            "sha256_outputs": {},
        },
        "config": {
            "winner_snapshot": winner_snap,
            "trigger_selected_params": {"thr": trig_cfg.thr, "h_in": trig_cfg.h_in, "h_out": trig_cfg.h_out},
            "execution_rule": "cash_signal_{D-1} governa trades no dia D",
            "median_tickers_pre_market_cap_filter": median_pre_filter,
            "median_tickers_post_market_cap_filter": median_post_filter,
        },
        "metrics": {
            "pure": pure_metrics,
            "trigger": trigger_metrics,
            "delta_trigger_minus_pure_holdout": {
                "cagr_pct": delta_holdout_cagr,
                "mdd_pct": delta_holdout_mdd,
            },
        },
        "trigger_stats": trigger_stats,
        "decision_gate": {
            "rule": "AGREGA somente se CAGR(trigger) > CAGR(puro) E MDD(trigger) >= MDD(puro)",
            "result": "AGREGA" if holdout_ok else "NAO_AGREGA",
        },
        "gates": {
            "required_inputs_exist": all(
                p.exists() for p in [WINNER_PATH, PRED_PATH, TRIGGER_CFG_PATH, ROOT / "backtest" / "results" / "curve_C4_K10.csv"]
            ),
            "outputs_written": all(p.exists() for p in [OUT_CURVE_PURE, OUT_CURVE_TRIGGER, OUT_PLOT]),
            "baseline_reconcile_with_winner_t024": gate_reconcile,
            "report_has_split_metrics": bool(
                pure_metrics.get("TRAIN")
                and pure_metrics.get("HOLDOUT")
                and trigger_metrics.get("TRAIN")
                and trigger_metrics.get("HOLDOUT")
            ),
            "trigger_stats_present": True,
        },
    }

    report["outputs"]["sha256_outputs"] = {
        "curve_c4_pure": _sha256(OUT_CURVE_PURE),
        "curve_c4_trigger": _sha256(OUT_CURVE_TRIGGER),
        "plotly_comparison": _sha256(OUT_PLOT),
    }
    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report["outputs"]["sha256_outputs"]["report"] = _sha256(OUT_REPORT)
    OUT_REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps({"task_id": "T-027", "decision_gate": report["decision_gate"], "gates": report["gates"]}, indent=2))


if __name__ == "__main__":
    main()
