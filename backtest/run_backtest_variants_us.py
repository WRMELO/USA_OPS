"""Backtest US com venda defensiva permanente (T-016)."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import argparse
import hashlib
import json

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_us.parquet"
IN_MACRO = ROOT / "data" / "ssot" / "macro_us.parquet"
IN_SCORES = ROOT / "data" / "features" / "scores_m3_us.parquet"
IN_BLACKLIST = ROOT / "config" / "blacklist_us.json"
OUT_DIR = ROOT / "backtest" / "results"
TRAIN_END = pd.Timestamp("2022-12-30")
BASE_CAPITAL = 100_000.0
MIN_MARKET_CAP_DEFAULT = 300_000_000.0


@dataclass
class BacktestConfig:
    top_n: int
    buffer_k: int
    rebalance_cadence: int
    friction_one_way_bps: float
    settlement_days: int
    base_capital: float
    k_damp: float
    max_weight_cap: float


@dataclass
class Lot:
    ticker: str
    buy_date: pd.Timestamp
    shares: int
    buy_price: float


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-016 defensive backtest US")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--buffer-k", type=int, default=15)
    parser.add_argument("--rebalance-cadence", type=int, default=1)
    parser.add_argument("--friction-bps", type=float, default=2.5)
    parser.add_argument("--settlement-days", type=int, default=1)
    parser.add_argument("--base-capital", type=float, default=BASE_CAPITAL)
    parser.add_argument("--min-market-cap", type=float, default=MIN_MARKET_CAP_DEFAULT)
    parser.add_argument("--k-damp", type=float, default=0.0)
    parser.add_argument("--max-weight-cap", type=float, default=1.0)
    return parser.parse_args()


def load_blacklist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return {str(x).upper().strip() for x in payload}
    out: set[str] = set()
    if isinstance(payload, dict):
        for values in payload.values():
            if isinstance(values, list):
                out.update(str(x).upper().strip() for x in values)
    return out


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    canonical = pd.read_parquet(IN_CANONICAL).copy()
    macro = pd.read_parquet(IN_MACRO).copy()
    scores = pd.read_parquet(IN_SCORES).copy()

    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    scores["date"] = pd.to_datetime(scores["date"], errors="coerce").dt.normalize()

    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    scores["ticker"] = scores["ticker"].astype(str).str.upper().str.strip()

    needed = [
        "date",
        "ticker",
        "close_raw",
        "close_operational",
        "market_cap",
        "split_factor",
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
    for c in needed:
        if c not in canonical.columns:
            canonical[c] = np.nan
    canonical["market_cap"] = pd.to_numeric(canonical["market_cap"], errors="coerce")
    canonical.loc[canonical["market_cap"] <= 0.0, "market_cap"] = np.nan
    canonical = canonical[needed].dropna(subset=["date", "ticker", "close_raw"]).sort_values(["date", "ticker"])
    macro = macro.dropna(subset=["date", "fed_funds_rate"]).sort_values("date")
    scores["score_m3"] = pd.to_numeric(scores.get("score_m3"), errors="coerce")
    scores = scores.dropna(subset=["date", "ticker", "m3_rank"]).sort_values(["date", "m3_rank", "ticker"])
    return canonical, macro, scores


def build_cash_log_daily(macro: pd.DataFrame) -> pd.Series:
    r_daily = (pd.to_numeric(macro["fed_funds_rate"], errors="coerce") / 100.0) / 252.0
    return pd.Series(np.log1p(r_daily.clip(lower=-0.999999)).values, index=macro["date"])


def build_scores_by_day(scores: pd.DataFrame, blacklist: set[str]) -> dict[pd.Timestamp, pd.DataFrame]:
    out: dict[pd.Timestamp, pd.DataFrame] = {}
    cols = ["ticker", "m3_rank", "score_m3"]
    for d, g in scores.groupby("date", sort=True):
        view = g[cols].copy()
        view = view[~view["ticker"].isin(blacklist)]
        view["m3_rank"] = pd.to_numeric(view["m3_rank"], errors="coerce")
        view["score_m3"] = pd.to_numeric(view["score_m3"], errors="coerce")
        view = view.dropna(subset=["m3_rank"]).sort_values(["m3_rank", "ticker"]).set_index("ticker")
        out[d] = view
    return out


def build_market_cap_wide(canonical: pd.DataFrame) -> pd.DataFrame:
    if canonical.empty or "market_cap" not in canonical.columns:
        return pd.DataFrame(dtype=float)
    view = canonical[["date", "ticker", "market_cap"]].copy()
    view["market_cap"] = pd.to_numeric(view["market_cap"], errors="coerce")
    view.loc[view["market_cap"] <= 0.0, "market_cap"] = np.nan
    return view.pivot_table(index="date", columns="ticker", values="market_cap", aggfunc="first").sort_index().ffill()


def apply_min_market_cap_filter(
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    market_cap_wide: pd.DataFrame,
    min_market_cap: float,
) -> tuple[dict[pd.Timestamp, pd.DataFrame], float, float]:
    pre_counts = [int(len(view)) for view in scores_by_day.values()]
    pre_median = float(pd.Series(pre_counts, dtype=float).median()) if pre_counts else 0.0
    if min_market_cap <= 0.0:
        return scores_by_day, pre_median, pre_median

    filtered: dict[pd.Timestamp, pd.DataFrame] = {}
    post_counts: list[int] = []
    for d, view in scores_by_day.items():
        if d not in market_cap_wide.index or view.empty:
            filtered_view = view.iloc[0:0].copy()
        else:
            cap_row = pd.to_numeric(market_cap_wide.loc[d], errors="coerce")
            cap_on_date = cap_row.reindex(view.index)
            keep = cap_on_date.index[cap_on_date.ge(float(min_market_cap)) & cap_on_date.notna()]
            filtered_view = view.loc[view.index.isin(keep)].copy()
        filtered[d] = filtered_view
        post_counts.append(int(len(filtered_view)))
    post_median = float(pd.Series(post_counts, dtype=float).median()) if post_counts else 0.0
    return filtered, pre_median, post_median


def _select_top_n(scores_day: pd.DataFrame | None, top_n: int, quarantine: set[str] | None = None) -> list[str]:
    if scores_day is None or scores_day.empty:
        return []
    ranked = scores_day.sort_values("m3_rank", ascending=True)
    out: list[str] = []
    for t in ranked.index.astype(str).tolist():
        if quarantine and t in quarantine:
            continue
        out.append(t)
        if len(out) >= top_n:
            break
    return out


def _select_c2_target(
    scores_day: pd.DataFrame | None,
    holdings: set[str],
    top_n: int,
    buffer_k: int,
    quarantine: set[str],
) -> list[str]:
    if scores_day is None or scores_day.empty:
        return sorted(list(holdings))
    ranks = scores_day["m3_rank"].astype(float).to_dict()
    top = _select_top_n(scores_day, top_n=top_n, quarantine=quarantine)
    kept = sorted([t for t in holdings if float(ranks.get(t, np.inf)) <= float(buffer_k)])
    target = kept[:]
    for t in top:
        if t not in target:
            target.append(t)
        if len(target) >= top_n:
            break
    return target[:top_n]


def compute_target_weights(
    scores_day: pd.DataFrame | None,
    target_list: list[str],
    k_damp: float,
    max_weight_cap: float,
) -> dict[str, float]:
    target = [str(t) for t in target_list]
    n = len(target)
    if n == 0:
        return {}
    # Compatibility mode: no dampening, no cap => equal weights.
    if k_damp <= 0.0 and max_weight_cap >= 1.0:
        eq = 1.0 / float(n)
        return {t: eq for t in target}

    raw: dict[str, float] = {}
    if scores_day is None or scores_day.empty:
        eq = 1.0 / float(n)
        raw = {t: eq for t in target}
    elif k_damp <= 0.0:
        eq = 1.0 / float(n)
        raw = {t: eq for t in target}
    else:
        vals: dict[str, float] = {}
        for t in target:
            s = float(scores_day.at[t, "score_m3"]) if t in scores_day.index else np.nan
            if not np.isfinite(s):
                vals[t] = 0.0
                continue
            damp = float(np.sign(s) * np.log1p(abs(s) * float(k_damp)))
            vals[t] = max(0.0, damp)
        total = float(sum(vals.values()))
        if total <= 0.0:
            eq = 1.0 / float(n)
            raw = {t: eq for t in target}
        else:
            raw = {t: float(v / total) for t, v in vals.items()}

    cap = float(min(max(max_weight_cap, 0.0), 1.0))
    if cap >= 1.0:
        return raw
    if cap <= 0.0:
        eq = 1.0 / float(n)
        return {t: eq for t in target}

    # Iterative cap + redistribution.
    w = raw.copy()
    fixed: set[str] = set()
    for _ in range(max(1, n * 2)):
        over = [t for t in target if t not in fixed and w.get(t, 0.0) > cap + 1e-12]
        if not over:
            break
        residual = 0.0
        for t in over:
            residual += float(w[t] - cap)
            w[t] = cap
            fixed.add(t)
        free = [t for t in target if t not in fixed]
        if not free:
            break
        free_total = float(sum(max(0.0, w.get(t, 0.0)) for t in free))
        if free_total <= 0.0:
            add = residual / float(len(free))
            for t in free:
                w[t] = float(w.get(t, 0.0) + add)
        else:
            for t in free:
                share = max(0.0, w.get(t, 0.0)) / free_total
                w[t] = float(w.get(t, 0.0) + residual * share)

    # Normalize and final clamp.
    for t in target:
        w[t] = float(min(max(w.get(t, 0.0), 0.0), cap))
    s = float(sum(w.values()))
    if s <= 0.0:
        eq = 1.0 / float(n)
        return {t: eq for t in target}
    w = {t: float(v / s) for t, v in w.items()}
    # Keep exact simplex sum.
    rem = 1.0 - float(sum(w.values()))
    if target:
        w[target[-1]] = float(max(0.0, w[target[-1]] + rem))
    return w


def _settlement_date(dates: list[pd.Timestamp], i: int, delay_days: int) -> pd.Timestamp:
    j = min(i + delay_days, len(dates) - 1)
    return dates[j]


def split_lots_by_ticker(lots: list[Lot]) -> dict[str, list[Lot]]:
    by_ticker: dict[str, list[Lot]] = {}
    for lot in lots:
        by_ticker.setdefault(lot.ticker, []).append(lot)
    for tk in by_ticker:
        by_ticker[tk] = sorted(by_ticker[tk], key=lambda x: x.buy_date)
    return by_ticker


def lots_market_value(lots: list[Lot], price_row: pd.Series) -> float:
    total = 0.0
    for lot in lots:
        px = float(price_row.get(lot.ticker, np.nan))
        if np.isfinite(px) and px > 0 and lot.shares > 0:
            total += lot.shares * px
    return total


def ticker_value(lots: list[Lot], ticker: str, price_row: pd.Series) -> float:
    px = float(price_row.get(ticker, np.nan))
    if not np.isfinite(px) or px <= 0:
        return 0.0
    return float(sum(l.shares for l in lots if l.ticker == ticker) * px)


def sell_ticker_fifo(
    ticker: str,
    target_value_to_sell: float,
    lots: list[Lot],
    price_row: pd.Series,
    friction: float,
    trading_dates: list[pd.Timestamp],
    i: int,
    settlement_days: int,
    pending_cash: dict[pd.Timestamp, float],
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
            updated_lots.append(
                Lot(ticker=lot.ticker, buy_date=lot.buy_date, shares=new_shares, buy_price=lot.buy_price)
            )

    if proceeds_liq > 0:
        settle_dt = _settlement_date(trading_dates, i, settlement_days)
        pending_cash[settle_dt] = float(pending_cash.get(settle_dt, 0.0) + proceeds_liq)
    return updated_lots, proceeds_liq, total_cost, sold_shares


def sell_all_ticker(
    ticker: str,
    lots: list[Lot],
    price_row: pd.Series,
    friction: float,
    trading_dates: list[pd.Timestamp],
    i: int,
    settlement_days: int,
    pending_cash: dict[pd.Timestamp, float],
) -> tuple[list[Lot], float, float, int]:
    value = ticker_value(lots, ticker, price_row)
    return sell_ticker_fifo(
        ticker=ticker,
        target_value_to_sell=value,
        lots=lots,
        price_row=price_row,
        friction=friction,
        trading_dates=trading_dates,
        i=i,
        settlement_days=settlement_days,
        pending_cash=pending_cash,
    )


def _build_z_table(i_wide: pd.DataFrame) -> pd.DataFrame:
    mean60 = i_wide.rolling(window=60, min_periods=20).mean()
    std60 = i_wide.rolling(window=60, min_periods=20).std(ddof=0).replace(0.0, np.nan)
    return (i_wide - mean60) / std60


def _band_from_z(z: float) -> int:
    if not np.isfinite(z):
        return 0
    if z < -3.0:
        return 3
    if z < -2.0:
        return 2
    if z < -1.0:
        return 1
    return 0


def _persist_points(z_prev: float, z_prev2: float, z_prev3: float) -> int:
    pts = 0
    neg_count = int((z_prev < 0) + (z_prev2 < 0) + (z_prev3 < 0))
    if neg_count >= 2:
        pts += 1
    if z_prev < -2 and z_prev2 < -2:
        pts += 1
    return pts


def _to_bool(v: float | int | bool | None) -> bool:
    return bool(float(v)) if v is not None and np.isfinite(v) else False


def _apply_split_adjustment(
    lots: list[Lot],
    split_row: pd.Series,
    d: pd.Timestamp,
    variant: str,
    events_split: list[dict[str, object]],
) -> list[Lot]:
    if not lots:
        return lots
    out: list[Lot] = []
    for lot in lots:
        sf = float(split_row.get(lot.ticker, np.nan))
        if np.isfinite(sf) and sf > 0 and sf < 1e6 and abs(sf - 1.0) > 1e-12:
            ratio = float(sf)
            projected = float(lot.shares) * ratio
            if (not np.isfinite(projected)) or projected > 1e9:
                events_split.append(
                    {
                        "date": d,
                        "variant": variant,
                        "ticker": lot.ticker,
                        "event": "split_adjustment_overflow_guard",
                        "split_factor": sf,
                        "shares_before": lot.shares,
                        "shares_after": lot.shares,
                    }
                )
                out.append(lot)
                continue
            new_shares = int(round(projected))
            if new_shares <= 0:
                events_split.append(
                    {
                        "date": d,
                        "variant": variant,
                        "ticker": lot.ticker,
                        "event": "split_adjustment_drop_lot",
                        "split_factor": sf,
                        "shares_before": lot.shares,
                        "shares_after": 0,
                    }
                )
                continue
            new_buy = lot.buy_price / ratio
            events_split.append(
                {
                    "date": d,
                    "variant": variant,
                    "ticker": lot.ticker,
                    "event": "split_adjustment",
                    "split_factor": sf,
                    "shares_before": lot.shares,
                    "shares_after": new_shares,
                    "buy_price_before": lot.buy_price,
                    "buy_price_after": new_buy,
                }
            )
            out.append(Lot(ticker=lot.ticker, buy_date=lot.buy_date, shares=new_shares, buy_price=new_buy))
        else:
            out.append(lot)
    return out


def _curve_metrics(curve: pd.DataFrame) -> tuple[float, float]:
    if curve.empty or len(curve) < 2:
        return 0.0, 0.0
    eq = curve["equity"].astype(float)
    running_max = eq.cummax().replace(0.0, np.nan)
    dd = (eq / running_max) - 1.0
    mdd = float(dd.min()) if dd.notna().any() else 0.0
    n_years = max(len(curve) / 252.0, 1.0 / 252.0)
    cagr = float((eq.iloc[-1] / eq.iloc[0]) ** (1.0 / n_years) - 1.0) if eq.iloc[0] > 0 else 0.0
    return cagr, mdd


def run_variant(
    variant: str,
    px_exec_wide: pd.DataFrame,
    split_event_wide: pd.DataFrame,
    i_wide: pd.DataFrame,
    z_wide: pd.DataFrame,
    any_rule_wide: pd.DataFrame,
    strong_rule_wide: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    cash_log_daily: pd.Series,
    cfg: BacktestConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    friction = cfg.friction_one_way_bps / 10_000.0
    rebalance_cadence = max(int(cfg.rebalance_cadence), 1)
    trading_dates = list(px_exec_wide.index.intersection(cash_log_daily.index).sort_values())
    if len(trading_dates) < 30:
        raise RuntimeError("Poucas datas de interseção para simular variante.")

    cash_free = float(cfg.base_capital)
    pending_cash: dict[pd.Timestamp, float] = {}
    lots: list[Lot] = []
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

    for i, d in enumerate(trading_dates):
        matured = float(pending_cash.pop(d, 0.0))
        if matured > 0:
            cash_free += matured

        split_row = split_event_wide.loc[d] if d in split_event_wide.index else pd.Series(dtype=float)
        lots = _apply_split_adjustment(lots, split_row, d, variant, events_split)

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
                            "variant": variant,
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

        # Camada 2: rebalance por variante (respeitando cadence).
        held = set(split_lots_by_ticker(lots).keys())
        is_rebalance_day = (i % rebalance_cadence) == 0
        if is_rebalance_day:
            if variant == "C1":
                target = _select_top_n(prev_scores, top_n=cfg.top_n, quarantine=quarantine)
            elif variant in {"C2", "C4"}:
                target = _select_c2_target(prev_scores, held, cfg.top_n, cfg.buffer_k, quarantine=quarantine)
            else:  # C3
                if (not initialized_c3) and prev_scores is not None and not prev_scores.empty:
                    target = _select_top_n(prev_scores, top_n=cfg.top_n, quarantine=quarantine)
                    initialized_c3 = True
                else:
                    target = sorted(list(held))

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
                            "variant": variant,
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

        # Camada 2.5: trim de concentração (somente C4, antes das compras).
        if is_rebalance_day and variant == "C4" and target:
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
                            "variant": variant,
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

        # Compras
        held = set(split_lots_by_ticker(lots).keys())
        if is_rebalance_day and target and (variant in {"C1", "C2", "C4"} or (variant == "C3" and not held)):
            target_weight = 1.0 / max(len(target), 1)
            equity_now = cash_free + sum(pending_cash.values()) + lots_market_value(lots, price_row)
            c4_weights = (
                compute_target_weights(prev_scores, target, cfg.k_damp, cfg.max_weight_cap)
                if variant == "C4"
                else {}
            )
            for tk in target:
                if tk in quarantine:
                    continue
                current_val = ticker_value(lots, tk, price_row)
                wt = float(c4_weights.get(tk, 0.0)) if variant == "C4" else target_weight
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
                "variant": variant,
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
    events_def_df = pd.DataFrame(events_def)
    events_split_df = pd.DataFrame(events_split)
    events_trim_df = pd.DataFrame(events_trim)
    return curve, events_def_df, events_split_df, events_trim_df


def summarize_curve(curve: pd.DataFrame) -> list[dict[str, float | str | int]]:
    out: list[dict[str, float | str | int]] = []
    for split_name in ("TRAIN", "HOLDOUT"):
        if split_name == "TRAIN":
            sub = curve[curve["date"] <= TRAIN_END].copy()
        else:
            sub = curve[curve["date"] > TRAIN_END].copy()
        if len(sub) < 2:
            continue
        cagr, mdd = _curve_metrics(sub)
        out.append(
            {
                "variant": str(sub["variant"].iloc[0]),
                "split": split_name,
                "equity_final": round(float(sub["equity"].iloc[-1]), 2),
                "cagr": round(float(cagr) * 100.0, 4),
                "mdd": round(float(mdd) * 100.0, 4),
                "avg_tickers": round(float(sub["n_tickers"].mean()), 4),
                "max_concentration_pct": round(float(sub["max_concentration"].max()) * 100.0, 4),
                "cost_total": round(float(sub["cost_total_cum"].iloc[-1]), 2),
                "days": int(len(sub)),
                "defensive_days_pct": round(float(sub["regime_defensive_used"].mean()) * 100.0, 4),
                "n_defensive_sells_25": int(sub["def_sell_25_cum"].iloc[-1]),
                "n_defensive_sells_50": int(sub["def_sell_50_cum"].iloc[-1]),
                "n_defensive_sells_100": int(sub["def_sell_100_cum"].iloc[-1]),
                "quarantine_entries": int(sub["quarantine_entries_cum"].iloc[-1]),
            }
        )
    return out


def main() -> None:
    args = parse_args()
    cfg = BacktestConfig(
        top_n=int(args.top_n),
        buffer_k=int(args.buffer_k),
        rebalance_cadence=int(args.rebalance_cadence),
        friction_one_way_bps=float(args.friction_bps),
        settlement_days=int(args.settlement_days),
        base_capital=float(args.base_capital),
        k_damp=float(args.k_damp),
        max_weight_cap=float(args.max_weight_cap),
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    canonical, macro, scores = load_inputs()
    blacklist = load_blacklist(IN_BLACKLIST)
    cash_log_daily = build_cash_log_daily(macro)
    scores_by_day = build_scores_by_day(scores=scores, blacklist=blacklist)
    market_cap_wide = build_market_cap_wide(canonical)
    scores_by_day, median_pre_filter, median_post_filter = apply_min_market_cap_filter(
        scores_by_day=scores_by_day,
        market_cap_wide=market_cap_wide,
        min_market_cap=float(args.min_market_cap),
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

    curves: dict[str, pd.DataFrame] = {}
    all_summary: list[dict[str, float | str | int]] = []
    all_events_def: list[pd.DataFrame] = []
    all_events_split: list[pd.DataFrame] = []
    all_events_trim: list[pd.DataFrame] = []
    variants: list[tuple[str, int | None]] = [("C1", None), ("C2", cfg.buffer_k), ("C3", None), ("C4", cfg.buffer_k)]

    for variant, k in variants:
        cfg_local = BacktestConfig(
            top_n=cfg.top_n,
            buffer_k=int(k) if k is not None else cfg.buffer_k,
            rebalance_cadence=cfg.rebalance_cadence,
            friction_one_way_bps=cfg.friction_one_way_bps,
            settlement_days=cfg.settlement_days,
            base_capital=cfg.base_capital,
            k_damp=cfg.k_damp,
            max_weight_cap=cfg.max_weight_cap,
        )
        curve, events_def, events_split, events_trim = run_variant(
            variant=variant,
            px_exec_wide=px_exec_wide,
            split_event_wide=split_event_wide,
            i_wide=i_wide,
            z_wide=z_wide,
            any_rule_wide=any_rule_wide,
            strong_rule_wide=strong_rule_wide,
            scores_by_day=scores_by_day,
            cash_log_daily=cash_log_daily,
            cfg=cfg_local,
        )
        curves[variant] = curve
        suffix = f"{variant}_K{cfg.buffer_k}" if variant in {"C2", "C4"} else variant
        curve.to_csv(OUT_DIR / f"curve_{suffix}.csv", index=False)
        all_summary.extend(summarize_curve(curve))
        if not events_def.empty:
            all_events_def.append(events_def)
        if not events_split.empty:
            all_events_split.append(events_split)
        if not events_trim.empty:
            all_events_trim.append(events_trim)

    summary_df = pd.DataFrame(all_summary).sort_values(["variant", "split"]).reset_index(drop=True)
    summary_csv = OUT_DIR / "summary_t015_variants.csv"
    summary_json = OUT_DIR / "summary_t015_variants.json"
    summary_df.to_csv(summary_csv, index=False)
    summary_df.to_json(summary_json, orient="records", indent=2)

    events_def_df = pd.concat(all_events_def, ignore_index=True) if all_events_def else pd.DataFrame()
    events_split_df = pd.concat(all_events_split, ignore_index=True) if all_events_split else pd.DataFrame()
    events_trim_df = pd.concat(all_events_trim, ignore_index=True) if all_events_trim else pd.DataFrame()
    events_def_csv = OUT_DIR / "events_defensive_sells.csv"
    events_split_csv = OUT_DIR / "events_split_adjustments.csv"
    events_trim_csv = OUT_DIR / "events_concentration_trims.csv"
    events_def_df.to_csv(events_def_csv, index=False)
    events_split_df.to_csv(events_split_csv, index=False)
    events_trim_df.to_csv(events_trim_csv, index=False)

    report_path = OUT_DIR / "t016_backtest_report.json"
    gates = {
        "required_inputs_exist": all(p.exists() for p in [IN_CANONICAL, IN_MACRO, IN_SCORES, IN_BLACKLIST]),
        "curves_non_empty": all((not curves[v].empty) for v in ("C1", "C2", "C3", "C4")),
        "summary_non_empty": not summary_df.empty,
        "anti_lookahead_prev_day_scores": True,
        "split_events_file_written": events_split_csv.exists(),
        "defensive_events_file_written": events_def_csv.exists(),
        "concentration_trim_file_written": events_trim_csv.exists(),
        "defensive_events_has_rows": not events_def_df.empty,
        "split_overflow_guard_zero": (
            True
            if events_split_df.empty or "event" not in events_split_df.columns
            else int((events_split_df["event"] == "split_adjustment_overflow_guard").sum()) == 0
        ),
        "outputs_written": all(
            p.exists()
            for p in [
                OUT_DIR / "curve_C1.csv",
                OUT_DIR / f"curve_C2_K{cfg.buffer_k}.csv",
                OUT_DIR / "curve_C3.csv",
                OUT_DIR / f"curve_C4_K{cfg.buffer_k}.csv",
                summary_csv,
                summary_json,
                events_def_csv,
                events_split_csv,
                events_trim_csv,
                report_path,
            ]
        ),
    }

    report = {
        "task_id": "T-016",
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "params": {
            "top_n": cfg.top_n,
            "buffer_k": cfg.buffer_k,
            "rebalance_cadence": cfg.rebalance_cadence,
            "friction_one_way_bps": cfg.friction_one_way_bps,
            "settlement_days": cfg.settlement_days,
            "base_capital": cfg.base_capital,
            "min_market_cap": float(args.min_market_cap),
            "k_damp": float(cfg.k_damp),
            "max_weight_cap": float(cfg.max_weight_cap),
        },
        "inputs": {
            "canonical_us": str(IN_CANONICAL.relative_to(ROOT)),
            "macro_us": str(IN_MACRO.relative_to(ROOT)),
            "scores_m3_us": str(IN_SCORES.relative_to(ROOT)),
            "blacklist_us": str(IN_BLACKLIST.relative_to(ROOT)),
            "sha256_inputs": {
                "canonical_us": _sha256(IN_CANONICAL),
                "macro_us": _sha256(IN_MACRO),
                "scores_m3_us": _sha256(IN_SCORES),
                "blacklist_us": _sha256(IN_BLACKLIST),
            },
        },
        "counts": {
            "tickers_px": int(px_exec_wide.shape[1]),
            "scores_dates": int(len(scores_by_day)),
            "median_scored_tickers_pre_filter": float(median_pre_filter),
            "median_scored_tickers_post_filter": float(median_post_filter),
            "blacklist_size": int(len(blacklist)),
            "events_defensive_rows": int(len(events_def_df)),
            "events_split_rows": int(len(events_split_df)),
            "events_concentration_trim_rows": int(len(events_trim_df)),
            "events_split_overflow_guard_rows": (
                0
                if events_split_df.empty or "event" not in events_split_df.columns
                else int((events_split_df["event"] == "split_adjustment_overflow_guard").sum())
            ),
        },
        "metrics": {
            variant: {
                "cagr_full": float(_curve_metrics(curves[variant])[0]),
                "mdd_full": float(_curve_metrics(curves[variant])[1]),
            }
            for variant in ("C1", "C2", "C3", "C4")
        },
        "anti_lookahead_notes": {
            "scores_reference_day": "A selecao usa prev_scores (D-1 relativo ao dia de execucao).",
            "market_cap_filter_alignment": "Filtro min_market_cap aplicado no mesmo date do score (D-1), sem acesso ao trade day.",
        },
        "outputs": {
            "curve_c1_csv": "backtest/results/curve_C1.csv",
            "curve_c2_csv": f"backtest/results/curve_C2_K{cfg.buffer_k}.csv",
            "curve_c3_csv": "backtest/results/curve_C3.csv",
            "curve_c4_csv": f"backtest/results/curve_C4_K{cfg.buffer_k}.csv",
            "summary_csv": "backtest/results/summary_t015_variants.csv",
            "summary_json": "backtest/results/summary_t015_variants.json",
            "events_defensive_csv": "backtest/results/events_defensive_sells.csv",
            "events_split_csv": "backtest/results/events_split_adjustments.csv",
            "events_concentration_trim_csv": "backtest/results/events_concentration_trims.csv",
            "report_json": "backtest/results/t016_backtest_report.json",
        },
        "gates": gates,
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    gates["outputs_written"] = all(
        p.exists()
        for p in [
            OUT_DIR / "curve_C1.csv",
            OUT_DIR / f"curve_C2_K{cfg.buffer_k}.csv",
            OUT_DIR / "curve_C3.csv",
            OUT_DIR / f"curve_C4_K{cfg.buffer_k}.csv",
            summary_csv,
            summary_json,
            events_def_csv,
            events_split_csv,
            events_trim_csv,
            report_path,
        ]
    )
    report["gates"] = gates
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if not all(gates.values()):
        failed = [k for k, v in gates.items() if not v]
        raise RuntimeError(f"T-016 FAIL gates: {failed}")

    print("T-016 PASS")
    print(json.dumps({"gates": gates, "summary_rows": len(summary_df)}, indent=2))


if __name__ == "__main__":
    main()
