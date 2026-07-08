from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


TASK_ID = "T-SDC-EXIT-CONE-REBALANCE-US-V4"
DECISION_REF = "PENDING-DECISION-LOG"

ROOT = Path("/home/wilson/USA_OPS")
SDC_ROOT = Path("/home/wilson/SALA_DE_CONTROLE")
TASK_DIR = ROOT / "analise_interfabricas" / TASK_ID
DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2_full_history"
DATASET_MANIFEST = DATASET_DIR / "manifest.json"
CANONICAL_HISTORY_CURVE = ROOT / "backtest" / "canonical_daily_history_us" / "curve.parquet"
BACKTEST_MODULE_PATH = ROOT / "backtest" / "run_backtest_variants_us.py"

OUT_JSON = TASK_DIR / "resultados_raw.json"
OUT_MD = TASK_DIR / "resultados.md"
OUT_LOG = TASK_DIR / "output_bruto.txt"

TRAIN_END = pd.Timestamp("2022-12-30")
VALIDATION_START = pd.Timestamp("2023-01-02")
TRADING_DAYS_PER_YEAR = 252.0
BLOCK_SIZE = 21
N_BOOTSTRAP = 5000
BOOTSTRAP_SEED = 42
MATERIALITY_DELTA_SHARPE = 0.30
QUARANTINE_DAYS = 10
BASELINE_TOL_REL = 1e-6

CONE_K_VALUES = [1.5, 2.0, 2.5]

WINNER_CONFIG = {
    "top_n": 20,
    "buffer_k": 10,
    "rebalance_cadence": 10,
    "friction_one_way_bps": 2.5,
    "settlement_days": 1,
    "base_capital": 100_000.0,
    "k_damp": 0.0,
    "max_weight_cap": 0.06,
    "min_market_cap": 300_000_000.0,
}

LOG_LINES: list[str] = []


def log(msg: str) -> None:
    print(msg)
    LOG_LINES.append(msg)


def write_log() -> None:
    OUT_LOG.write_text("\n".join(LOG_LINES) + "\n", encoding="utf-8")


def load_ref_module() -> Any:
    spec = importlib.util.spec_from_file_location("run_backtest_variants_us_ref", BACKTEST_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {BACKTEST_MODULE_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


REF = load_ref_module()
BacktestConfig = REF.BacktestConfig
Lot = REF.Lot


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sanitize(v: Any) -> Any:
    if isinstance(v, dict):
        return {str(k): sanitize(val) for k, val in v.items()}
    if isinstance(v, list):
        return [sanitize(x) for x in v]
    if isinstance(v, tuple):
        return [sanitize(x) for x in v]
    if isinstance(v, (pd.Timestamp, datetime)):
        if pd.isna(v):
            return None
        return pd.Timestamp(v).strftime("%Y-%m-%d")
    if v is pd.NaT:
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        if not np.isfinite(float(v)):
            return None
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(sanitize(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def fail_payload(stage: str, reason: str, partial: dict[str, Any] | None = None) -> dict[str, Any]:
    partial = partial or {}
    payload: dict[str, Any] = {
        "task_id": TASK_ID,
        "decision_ref": DECISION_REF,
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "FAIL_METODOLOGICO",
        "failed_stage": stage,
        "reason": reason,
        "partial": partial,
    }
    payload.update(partial)
    write_json(OUT_JSON, payload)
    write_report_md(payload)
    write_log()
    return payload


def gate1_hashes() -> dict[str, Any]:
    manifest = json.loads(DATASET_MANIFEST.read_text(encoding="utf-8"))
    required = ["canonical_us.parquet", "macro_us.parquet", "blacklist_us.json", "scores_m3_us.parquet"]
    rows: dict[str, dict[str, Any]] = {}
    ok = True
    for name in required:
        path = DATASET_DIR / name
        expected = manifest["files"][name]["sha256"]
        actual = sha256(path)
        match = actual == expected
        ok = ok and match
        rows[name] = {
            "path": str(path),
            "expected_sha256": expected,
            "actual_sha256": actual,
            "match": match,
            "size_bytes": int(path.stat().st_size),
        }
    return {
        "status": "PASS" if ok else "FAIL",
        "dataset_manifest": str(DATASET_MANIFEST),
        "files": rows,
    }


def prepare_inputs() -> dict[str, Any]:
    canonical, macro, scores = REF.load_inputs(
        canonical_path=DATASET_DIR / "canonical_us.parquet",
        macro_path=DATASET_DIR / "macro_us.parquet",
        scores_path=DATASET_DIR / "scores_m3_us.parquet",
    )
    blacklist = REF.load_blacklist(DATASET_DIR / "blacklist_us.json")
    cash_log_daily = REF.build_cash_log_daily(macro)
    scores_by_day = REF.build_scores_by_day(scores=scores, blacklist=blacklist)
    market_cap_wide = REF.build_market_cap_wide(canonical)
    scores_by_day, median_pre_filter, median_post_filter = REF.apply_min_market_cap_filter(
        scores_by_day=scores_by_day,
        market_cap_wide=market_cap_wide,
        min_market_cap=WINNER_CONFIG["min_market_cap"],
    )

    px_exec_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first")
        .sort_index()
        .ffill()
    )
    # Match canonical_daily_history_us: pre-macro cash earns zero return.
    cash_log_daily = cash_log_daily.reindex(px_exec_wide.index).fillna(0.0)
    close_operational_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first")
        .sort_index()
        .ffill()
    )
    split_wide = canonical.pivot_table(index="date", columns="ticker", values="split_factor", aggfunc="first").sort_index()
    split_changed = (split_wide / split_wide.shift(1)).replace([np.inf, -np.inf], np.nan)
    has_split = (split_changed - 1.0).abs() > 1e-12
    px_raw_wide = canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first").sort_index()
    split_event_wide = (px_raw_wide.shift(1) / px_raw_wide).where(has_split)

    spc_cols = ["i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl", "xbar_value", "xbar_ucl", "xbar_lcl", "r_value", "r_ucl"]
    for col in spc_cols:
        canonical[col] = pd.to_numeric(canonical[col], errors="coerce")

    i_wide = canonical.pivot_table(index="date", columns="ticker", values="i_value", aggfunc="first").sort_index()
    i_ucl_wide = canonical.pivot_table(index="date", columns="ticker", values="i_ucl", aggfunc="first").sort_index()
    i_lcl_wide = canonical.pivot_table(index="date", columns="ticker", values="i_lcl", aggfunc="first").sort_index()
    z_wide = REF._build_z_table(i_wide)
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
    mu62_wide = scores.pivot_table(index="date", columns="ticker", values="score_m0", aggfunc="first").sort_index()
    sigma62_wide = scores.pivot_table(index="date", columns="ticker", values="vol_62", aggfunc="first").sort_index()
    logret_wide = np.log(close_operational_wide / close_operational_wide.shift(1)).replace([np.inf, -np.inf], np.nan)

    return {
        "canonical": canonical,
        "macro": macro,
        "scores": scores,
        "blacklist": blacklist,
        "cash_log_daily": cash_log_daily,
        "scores_by_day": scores_by_day,
        "median_pre_filter": median_pre_filter,
        "median_post_filter": median_post_filter,
        "px_exec_wide": px_exec_wide,
        "close_operational_wide": close_operational_wide,
        "split_event_wide": split_event_wide,
        "i_wide": i_wide,
        "i_ucl_wide": i_ucl_wide,
        "i_lcl_wide": i_lcl_wide,
        "z_wide": z_wide,
        "any_rule_wide": any_rule_wide,
        "strong_rule_wide": strong_rule_wide,
        "mu62_wide": mu62_wide,
        "sigma62_wide": sigma62_wide,
        "logret_wide": logret_wide,
    }


def winner_cfg() -> Any:
    return BacktestConfig(
        top_n=int(WINNER_CONFIG["top_n"]),
        buffer_k=int(WINNER_CONFIG["buffer_k"]),
        rebalance_cadence=int(WINNER_CONFIG["rebalance_cadence"]),
        friction_one_way_bps=float(WINNER_CONFIG["friction_one_way_bps"]),
        settlement_days=int(WINNER_CONFIG["settlement_days"]),
        base_capital=float(WINNER_CONFIG["base_capital"]),
        k_damp=float(WINNER_CONFIG["k_damp"]),
        max_weight_cap=float(WINNER_CONFIG["max_weight_cap"]),
    )


def run_baseline(inputs: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return REF.run_variant(
        variant="C4",
        px_exec_wide=inputs["px_exec_wide"],
        split_event_wide=inputs["split_event_wide"],
        i_wide=inputs["i_wide"],
        z_wide=inputs["z_wide"],
        any_rule_wide=inputs["any_rule_wide"],
        strong_rule_wide=inputs["strong_rule_wide"],
        scores_by_day=inputs["scores_by_day"],
        cash_log_daily=inputs["cash_log_daily"],
        cfg=winner_cfg(),
    )


def gate2_reconcile_baseline(baseline_curve: pd.DataFrame) -> dict[str, Any]:
    ref_curve = pd.read_parquet(CANONICAL_HISTORY_CURVE).copy()
    baseline = baseline_curve[["date", "equity"]].copy()
    baseline["date"] = pd.to_datetime(baseline["date"], errors="coerce").dt.normalize()
    ref_curve["date"] = pd.to_datetime(ref_curve["date"], errors="coerce").dt.normalize()
    merged = ref_curve[["date", "equity"]].merge(baseline, on="date", suffixes=("_canonical", "_rerun"))
    if merged.empty:
        return {
            "status": "FAIL",
            "reason": "No overlapping dates between canonical history and rerun baseline.",
            "n_overlap": 0,
        }
    diff = merged["equity_rerun"].astype(float) - merged["equity_canonical"].astype(float)
    denom = merged["equity_canonical"].abs().replace(0.0, np.nan)
    rel = (diff.abs() / denom).replace([np.inf, -np.inf], np.nan)
    max_abs_diff = float(diff.abs().max())
    max_rel_abs_diff = float(rel.max()) if rel.notna().any() else 0.0
    ok = bool(max_rel_abs_diff <= BASELINE_TOL_REL)
    return {
        "status": "PASS" if ok else "FAIL",
        "tolerance_relative": BASELINE_TOL_REL,
        "n_overlap": int(len(merged)),
        "canonical_date_min": merged["date"].min(),
        "canonical_date_max": merged["date"].max(),
        "max_abs_diff": max_abs_diff,
        "max_rel_abs_diff": max_rel_abs_diff,
    }


def value_at(wide: pd.DataFrame, d: pd.Timestamp, tk: str) -> float:
    if d in wide.index and tk in wide.columns:
        return float(wide.at[d, tk])
    return float("nan")


def _cone_state_init(d: pd.Timestamp, prev_d: pd.Timestamp, tk: str, inputs: dict[str, Any]) -> dict[str, Any]:
    mu_reb = value_at(inputs["mu62_wide"], prev_d, tk)
    sigma_reb = value_at(inputs["sigma62_wide"], prev_d, tk)
    cone_active = bool(np.isfinite(mu_reb) and np.isfinite(sigma_reb) and sigma_reb > 0)
    return {
        "reb_anchor_date": d,
        "reb_reference_date": prev_d,
        "mu_reb": float(mu_reb) if np.isfinite(mu_reb) else float("nan"),
        "sigma_reb": float(sigma_reb) if np.isfinite(sigma_reb) else float("nan"),
        "cum_logret": 0.0,
        "n_since_reb": 0,
        "cone_active": cone_active,
    }


def update_holding_states(
    states: dict[str, dict[str, Any]],
    held: set[str],
    d: pd.Timestamp,
    prev_d: pd.Timestamp,
    is_rebalance_day: bool,
    inputs: dict[str, Any],
) -> None:
    for tk in list(states):
        if tk not in held:
            del states[tk]

    for tk in sorted(held):
        if is_rebalance_day or tk not in states:
            states[tk] = _cone_state_init(d=d, prev_d=prev_d, tk=tk, inputs=inputs)
            continue

        st = states[tk]
        lr = value_at(inputs["logret_wide"], d, tk)
        if np.isfinite(lr):
            st["cum_logret"] = float(st.get("cum_logret", 0.0)) + float(lr)
            st["n_since_reb"] = int(st.get("n_since_reb", 0)) + 1


def c3_signal(mechanism: str, threshold: float | None, st: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    if mechanism not in {"cone_mu", "cone_zero"}:
        raise ValueError(f"Unknown mechanism: {mechanism}")
    k = float(threshold) if threshold is not None else float("nan")
    n = int(st.get("n_since_reb", 0))
    mu_reb = float(st.get("mu_reb", np.nan))
    sigma_reb = float(st.get("sigma_reb", np.nan))
    cum_logret = float(st.get("cum_logret", np.nan))
    active = bool(st.get("cone_active", False))
    anchor_value = mu_reb if mechanism == "cone_mu" else 0.0
    lower_bound = float("nan")
    hit = False
    if active and n >= 1 and np.isfinite(k) and np.isfinite(anchor_value) and np.isfinite(sigma_reb) and np.isfinite(cum_logret):
        lower_bound = float(anchor_value * n - k * sigma_reb * math.sqrt(n))
        hit = bool(cum_logret < lower_bound)
    return hit, {
        "anchor_type": mechanism,
        "k": k,
        "anchor_value": anchor_value,
        "mu_reb": mu_reb,
        "sigma_reb": sigma_reb,
        "cum_logret": cum_logret,
        "n_since_reb": n,
        "lower_bound": lower_bound,
        "cone_active": active,
        "reb_anchor_date": st.get("reb_anchor_date"),
        "reb_reference_date": st.get("reb_reference_date"),
    }


def run_variant_with_camada3(
    variant_label: str,
    mechanism: str,
    threshold: float | None,
    inputs: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cfg = winner_cfg()
    friction = cfg.friction_one_way_bps / 10_000.0
    rebalance_cadence = max(int(cfg.rebalance_cadence), 1)
    trading_dates = list(inputs["px_exec_wide"].index.intersection(inputs["cash_log_daily"].index).sort_values())
    if len(trading_dates) < 30:
        raise RuntimeError("Poucas datas de intersecao para simular variante.")

    cash_free = float(cfg.base_capital)
    pending_cash: dict[pd.Timestamp, float] = {}
    lots: list[Lot] = []
    rows: list[dict[str, float | int | str]] = []
    total_cost = 0.0
    quarantine: set[str] = set()
    quarantine_until: dict[str, pd.Timestamp] = {}
    quarantine_entries = 0
    initialized_c3 = False

    def25 = 0
    def50 = 0
    def100 = 0
    c3_sells = 0
    regime_hist: list[float] = []
    defensive_state = False
    in_streak = 0
    out_streak = 0

    states: dict[str, dict[str, Any]] = {}
    events_def: list[dict[str, object]] = []
    events_split: list[dict[str, object]] = []
    events_trim: list[dict[str, object]] = []
    events_c3: list[dict[str, object]] = []

    for i, d in enumerate(trading_dates):
        matured = float(pending_cash.pop(d, 0.0))
        if matured > 0:
            cash_free += matured

        for tk, until in list(quarantine_until.items()):
            if d >= until:
                quarantine_until.pop(tk, None)
                quarantine.discard(tk)
            else:
                quarantine.add(tk)

        split_row = inputs["split_event_wide"].loc[d] if d in inputs["split_event_wide"].index else pd.Series(dtype=float)
        lots = REF._apply_split_adjustment(lots, split_row, d, variant_label, events_split)

        price_row = inputs["px_exec_wide"].loc[d]
        prev_d = trading_dates[i - 1] if i > 0 else d
        prev2_d = trading_dates[i - 2] if i > 1 else prev_d
        prev3_d = trading_dates[i - 3] if i > 2 else prev2_d
        prev_scores = inputs["scores_by_day"].get(prev_d)
        held = set(REF.split_lots_by_ticker(lots).keys())

        # Camada 1: defensiva permanente original.
        candidates: list[tuple[str, int, float]] = []
        if defensive_state and held:
            for tk in held:
                z_prev = value_at(inputs["z_wide"], prev_d, tk)
                z_prev2 = value_at(inputs["z_wide"], prev2_d, tk)
                z_prev3 = value_at(inputs["z_wide"], prev3_d, tk)
                if not np.isfinite(z_prev):
                    continue
                band = REF._band_from_z(z_prev)
                persist = REF._persist_points(z_prev, z_prev2, z_prev3)
                any_rule = REF._to_bool(value_at(inputs["any_rule_wide"], prev_d, tk))
                strong_rule = REF._to_bool(value_at(inputs["strong_rule_wide"], prev_d, tk))
                evidence = (1 if any_rule else 0) + (2 if strong_rule else 0)
                score = int(min(6, band + persist + evidence))
                if z_prev < 0 and score >= 4:
                    candidates.append((tk, score, z_prev))

            candidates = sorted(candidates, key=lambda x: (-x[1], x[2]))[:5]
            cand_set = {t for t, _, _ in candidates}
            for tk in list(quarantine):
                any_rule = REF._to_bool(value_at(inputs["any_rule_wide"], prev_d, tk))
                strong_rule = REF._to_bool(value_at(inputs["strong_rule_wide"], prev_d, tk))
                in_control = not (any_rule or strong_rule)
                if in_control and tk not in cand_set and tk not in quarantine_until:
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

                current_val = REF.ticker_value(lots, tk, price_row)
                target_sell = current_val * pct
                lots, proceeds, cost, sold_shares = REF.sell_ticker_fifo(
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
                            "variant": variant_label,
                            "ticker": tk,
                            "event": "defensive_sell",
                            "score": int(score),
                            "z_prev": float(z_prev),
                            "sell_pct": float(pct),
                            "sold_shares": int(sold_shares),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "settle_dt": REF._settlement_date(trading_dates, i, cfg.settlement_days),
                        }
                    )

        for tk, until in quarantine_until.items():
            if d < until:
                quarantine.add(tk)

        # Camada 3: saida antecipada por mecanismo pre-registrado.
        held = set(REF.split_lots_by_ticker(lots).keys())
        for tk in sorted(held):
            st = states.get(tk)
            if st is None:
                continue
            hit, evidence = c3_signal(mechanism, threshold, st)
            if not hit:
                continue
            lots, proceeds, cost, sold_shares = REF.sell_all_ticker(
                ticker=tk,
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
            c3_sells += 1
            quarantine.add(tk)
            quarantine_until[tk] = trading_dates[min(i + QUARANTINE_DAYS, len(trading_dates) - 1)]
            quarantine_entries += 1
            events_c3.append(
                {
                    "date": d,
                    "signal_date": prev_d,
                    "variant": variant_label,
                    "ticker": tk,
                    "event": f"camada3_{mechanism}",
                    "threshold": threshold,
                    "sold_shares": int(sold_shares),
                    "proceeds_net": float(proceeds),
                    "trade_cost": float(cost),
                    "settle_dt": REF._settlement_date(trading_dates, i, cfg.settlement_days),
                    "quarantine_until": quarantine_until[tk],
                    **evidence,
                }
            )

        # Camada 2: rebalance por variante original.
        held = set(REF.split_lots_by_ticker(lots).keys())
        is_rebalance_day = (i % rebalance_cadence) == 0
        if is_rebalance_day:
            target = REF._select_c2_target(prev_scores, held, cfg.top_n, cfg.buffer_k, quarantine=quarantine)
            target_set = set(target)
            to_sell = sorted([t for t in held if t not in target_set])
            for tk in to_sell:
                lots, proceeds, cost, sold_shares = REF.sell_all_ticker(
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
                            "variant": variant_label,
                            "ticker": tk,
                            "event": "rebalance_sell",
                            "score": np.nan,
                            "z_prev": np.nan,
                            "sell_pct": 1.0,
                            "sold_shares": int(sold_shares),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "settle_dt": REF._settlement_date(trading_dates, i, cfg.settlement_days),
                        }
                    )
        else:
            target = sorted(list(held))

        # Camada 2.5: trim de concentracao original C4.
        if is_rebalance_day and target:
            equity_now_trim = cash_free + sum(pending_cash.values()) + REF.lots_market_value(lots, price_row)
            if equity_now_trim > 0 and cfg.max_weight_cap < 1.0:
                cap_val = float(equity_now_trim * cfg.max_weight_cap)
                shared = sorted(list(set(held).intersection(set(target))))
                for tk in shared:
                    current_val = REF.ticker_value(lots, tk, price_row)
                    if current_val <= cap_val + 1e-12:
                        continue
                    target_sell = max(0.0, current_val - cap_val)
                    if target_sell <= 0:
                        continue
                    lots, proceeds, cost, sold_shares = REF.sell_ticker_fifo(
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
                            "variant": variant_label,
                            "ticker": tk,
                            "event": "concentration_trim",
                            "weight_before": float(weight_before),
                            "weight_cap": float(cfg.max_weight_cap),
                            "value_sold_gross": float(target_sell),
                            "proceeds_net": float(proceeds),
                            "trade_cost": float(cost),
                            "sold_shares": int(sold_shares),
                            "settle_dt": REF._settlement_date(trading_dates, i, cfg.settlement_days),
                        }
                    )

        # Compras originais C4.
        held = set(REF.split_lots_by_ticker(lots).keys())
        if is_rebalance_day and target:
            equity_now = cash_free + sum(pending_cash.values()) + REF.lots_market_value(lots, price_row)
            c4_weights = REF.compute_target_weights(prev_scores, target, cfg.k_damp, cfg.max_weight_cap)
            for tk in target:
                if tk in quarantine:
                    continue
                current_val = REF.ticker_value(lots, tk, price_row)
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
                initialized_c3 = True

        cash_log = float(inputs["cash_log_daily"].get(d, 0.0))
        cash_ret = float(np.expm1(cash_log))
        if cash_free > 0:
            cash_free *= (1.0 + cash_ret)

        # Atualiza regime defensivo original para D+1.
        held = set(REF.split_lots_by_ticker(lots).keys())
        proxy_ret = np.nan
        if held and d in inputs["i_wide"].index:
            vals = inputs["i_wide"].loc[d, list(held)] if len(held) > 0 else pd.Series(dtype=float)
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

        update_holding_states(states=states, held=held, d=d, prev_d=prev_d, is_rebalance_day=is_rebalance_day, inputs=inputs)

        holdings_value = REF.lots_market_value(lots, price_row)
        by_ticker = REF.split_lots_by_ticker(lots)
        conc_vals = []
        if holdings_value > 0:
            for tk in by_ticker:
                conc_vals.append(REF.ticker_value(lots, tk, price_row))
        equity_end = cash_free + sum(pending_cash.values()) + holdings_value
        max_conc = (max(conc_vals) / equity_end) if conc_vals and equity_end > 0 else 0.0
        rows.append(
            {
                "date": d,
                "variant": variant_label,
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
                "c3_sell_cum": int(c3_sells),
                "quarantine_size": int(len(quarantine)),
                "quarantine_entries_cum": int(quarantine_entries),
                "rebalance_cadence": int(rebalance_cadence),
                "is_rebalance_day": int(is_rebalance_day),
            }
        )

    _ = initialized_c3
    curve = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    if not curve.empty:
        base = float(curve["equity"].iloc[0]) if float(curve["equity"].iloc[0]) > 0 else 1.0
        curve["equity_base100"] = (curve["equity"].astype(float) / base) * 100.0
    else:
        curve["equity_base100"] = pd.Series(dtype="float64")
    return curve, pd.DataFrame(events_def), pd.DataFrame(events_split), pd.DataFrame(events_trim), pd.DataFrame(events_c3)


def daily_log_returns(curve: pd.DataFrame, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None) -> pd.Series:
    sub = curve.copy()
    sub["date"] = pd.to_datetime(sub["date"], errors="coerce").dt.normalize()
    if start is not None:
        sub = sub[sub["date"] >= start]
    if end is not None:
        sub = sub[sub["date"] <= end]
    sub = sub.sort_values("date")
    eq = pd.to_numeric(sub["equity"], errors="coerce")
    rets = np.log(eq / eq.shift(1)).replace([np.inf, -np.inf], np.nan)
    return pd.Series(rets.values, index=sub["date"]).dropna()


def cvar(arr: np.ndarray, level: float) -> float:
    vals = np.asarray(arr, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return float("nan")
    threshold = float(np.nanquantile(vals, level))
    tail = vals[vals <= threshold]
    if tail.size == 0:
        return float("nan")
    return float(np.nanmean(tail))


def split_bounds(split: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if split == "calibracao":
        return None, TRAIN_END
    if split == "validacao":
        return VALIDATION_START, None
    if split == "completo":
        return None, None
    raise ValueError(split)


def metric_block(curve: pd.DataFrame, events_sell: pd.DataFrame, events_c3: pd.DataFrame, split: str) -> dict[str, Any]:
    start, end = split_bounds(split)
    sub = curve.copy()
    sub["date"] = pd.to_datetime(sub["date"], errors="coerce").dt.normalize()
    if start is not None:
        sub = sub[sub["date"] >= start]
    if end is not None:
        sub = sub[sub["date"] <= end]
    sub = sub.sort_values("date")
    rets = daily_log_returns(curve, start=start, end=end)
    if sub.empty or len(rets) == 0:
        return {
            "split": split,
            "n_days": int(len(sub)),
            "cagr": float("nan"),
            "sharpe": float("nan"),
            "mdd": float("nan"),
            "cvar5": float("nan"),
            "rotation": float("nan"),
            "sell_events": 0,
            "c3_triggers": 0,
            "friction_cost": float("nan"),
        }
    mu = float(np.nanmean(rets.to_numpy(dtype=float)))
    sd = float(np.nanstd(rets.to_numpy(dtype=float), ddof=0))
    sharpe = float((mu / sd) * math.sqrt(TRADING_DAYS_PER_YEAR)) if sd > 0 else float("nan")
    cagr = float(np.expm1(mu * TRADING_DAYS_PER_YEAR))
    eq = pd.to_numeric(sub["equity"], errors="coerce")
    dd = (eq / eq.cummax()) - 1.0
    mdd = float(dd.min()) if dd.notna().any() else float("nan")

    ev = events_sell.copy() if not events_sell.empty else pd.DataFrame(columns=["date"])
    c3 = events_c3.copy() if not events_c3.empty else pd.DataFrame(columns=["date"])
    for frame in (ev, c3):
        if not frame.empty and "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    if start is not None and not ev.empty:
        ev = ev[ev["date"] >= start]
    if end is not None and not ev.empty:
        ev = ev[ev["date"] <= end]
    if start is not None and not c3.empty:
        c3 = c3[c3["date"] >= start]
    if end is not None and not c3.empty:
        c3 = c3[c3["date"] <= end]

    positions_days = float(pd.to_numeric(sub["n_tickers"], errors="coerce").fillna(0).sum())
    sell_events = int(len(ev) + len(c3))
    cost_start = float(sub["cost_total_cum"].iloc[0])
    cost_end = float(sub["cost_total_cum"].iloc[-1])
    return {
        "split": split,
        "n_days": int(len(sub)),
        "date_min": sub["date"].min(),
        "date_max": sub["date"].max(),
        "equity_start": float(sub["equity"].iloc[0]),
        "equity_final": float(sub["equity"].iloc[-1]),
        "cagr": cagr,
        "sharpe": sharpe,
        "mdd": mdd,
        "cvar5": cvar(rets.to_numpy(dtype=float), 0.05),
        "rotation": float(sell_events / positions_days) if positions_days > 0 else float("nan"),
        "sell_events": sell_events,
        "c3_triggers": int(len(c3)),
        "friction_cost": float(cost_end - cost_start),
        "positions_days": int(positions_days),
    }


def all_metrics(curve: pd.DataFrame, events_sell: pd.DataFrame, events_c3: pd.DataFrame) -> dict[str, Any]:
    return {split: metric_block(curve, events_sell, events_c3, split) for split in ("completo", "calibracao", "validacao")}


def calc_sharpe(a: np.ndarray) -> float:
    vals = np.asarray(a, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size < 2:
        return float("nan")
    sd = float(np.nanstd(vals, ddof=0))
    if sd <= 0:
        return float("nan")
    return float((np.nanmean(vals) / sd) * math.sqrt(TRADING_DAYS_PER_YEAR))


def calc_cagr(a: np.ndarray) -> float:
    vals = np.asarray(a, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return float("nan")
    return float(np.expm1(float(np.nanmean(vals)) * TRADING_DAYS_PER_YEAR))


def block_bootstrap_deltas(baseline: pd.Series, candidate: pd.Series) -> dict[str, Any]:
    joined = pd.concat([baseline.rename("baseline"), candidate.rename("candidate")], axis=1, join="inner").dropna()
    base_arr = joined["baseline"].to_numpy(dtype=float)
    cand_arr = joined["candidate"].to_numpy(dtype=float)
    n = len(base_arr)
    if n < max(10, BLOCK_SIZE):
        return {
            "n_paths_valid": 0,
            "block_size": BLOCK_SIZE,
            "delta_sharpe_ic95": {"lower": float("nan"), "upper": float("nan"), "median": float("nan")},
            "delta_cagr_ic95": {"lower": float("nan"), "upper": float("nan"), "median": float("nan")},
            "p_value_delta_sharpe_le_zero": float("nan"),
        }
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    max_start = max(0, n - BLOCK_SIZE)
    deltas_sh: list[float] = []
    deltas_cg: list[float] = []
    for _ in range(N_BOOTSTRAP):
        idx: list[int] = []
        while len(idx) < n:
            start = int(rng.integers(0, max_start + 1)) if max_start > 0 else 0
            end = min(start + BLOCK_SIZE, n)
            idx.extend(range(start, end))
        pick = np.array(idx[:n], dtype=int)
        b = base_arr[pick]
        c = cand_arr[pick]
        d_sh = calc_sharpe(c) - calc_sharpe(b)
        d_cg = calc_cagr(c) - calc_cagr(b)
        if np.isfinite(d_sh):
            deltas_sh.append(float(d_sh))
        if np.isfinite(d_cg):
            deltas_cg.append(float(d_cg))
    sh = np.array(deltas_sh, dtype=float)
    cg = np.array(deltas_cg, dtype=float)

    def q(arr: np.ndarray, pct: float) -> float:
        return float(np.nanpercentile(arr, pct)) if len(arr) else float("nan")

    return {
        "n_paths_valid": int(len(sh)),
        "block_size": BLOCK_SIZE,
        "n_paths_requested": N_BOOTSTRAP,
        "seed": BOOTSTRAP_SEED,
        "delta_sharpe_ic95": {"lower": q(sh, 2.5), "upper": q(sh, 97.5), "median": q(sh, 50.0)},
        "delta_cagr_ic95": {"lower": q(cg, 2.5), "upper": q(cg, 97.5), "median": q(cg, 50.0)},
        "p_value_delta_sharpe_le_zero": float(np.mean(sh <= 0.0)) if len(sh) else float("nan"),
    }


def verdict(base_metrics: dict[str, Any], cand_metrics: dict[str, Any], boot: dict[str, Any]) -> dict[str, Any]:
    b_cal = base_metrics["calibracao"]
    b_val = base_metrics["validacao"]
    c_cal = cand_metrics["calibracao"]
    c_val = cand_metrics["validacao"]
    d_sh_cal = float(c_cal["sharpe"] - b_cal["sharpe"])
    d_sh_val = float(c_val["sharpe"] - b_val["sharpe"])
    d_cg_val = float(c_val["cagr"] - b_val["cagr"])
    ci = boot["delta_sharpe_ic95"]
    lower = float(ci.get("lower", np.nan))
    upper = float(ci.get("upper", np.nan))
    p_le_zero = float(boot.get("p_value_delta_sharpe_le_zero", np.nan))
    p_better = 1.0 - p_le_zero if np.isfinite(p_le_zero) else float("nan")
    p_worse = p_le_zero

    if np.isfinite(lower) and np.isfinite(upper) and lower > 0 and upper > 0:
        label = "DOMINA_FORTE_MELHOR"
    elif np.isfinite(lower) and np.isfinite(upper) and lower < 0 and upper < 0:
        label = "DOMINA_FORTE_PIOR"
    elif (
        np.isfinite(p_better)
        and p_better >= 0.90
        and d_sh_cal > 0
        and d_sh_val > 0
        and d_cg_val > 0
        and abs(d_sh_val) >= MATERIALITY_DELTA_SHARPE
    ):
        label = "FAVORECIDO_MELHOR"
    elif (
        np.isfinite(p_worse)
        and p_worse >= 0.90
        and d_sh_cal < 0
        and d_sh_val < 0
        and d_cg_val < 0
        and abs(d_sh_val) >= MATERIALITY_DELTA_SHARPE
    ):
        label = "FAVORECIDO_PIOR"
    else:
        label = "INCONCLUSIVO"

    return {
        "veredito": label,
        "delta_sharpe_calibracao": d_sh_cal,
        "delta_sharpe_validacao": d_sh_val,
        "delta_cagr_validacao": d_cg_val,
        "p_bootstrap_melhor": p_better,
        "p_bootstrap_pior": p_worse,
        "materiality_delta_sharpe": MATERIALITY_DELTA_SHARPE,
    }


def candidate_specs() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for k in CONE_K_VALUES:
        suffix = int(round(k * 100))
        out.append({"variant": f"C4_CONE_MU_{suffix:03d}", "family": "cone_mu", "mechanism": "cone_mu", "threshold": k})
    for k in CONE_K_VALUES:
        suffix = int(round(k * 100))
        out.append({"variant": f"C4_CONE_ZERO_{suffix:03d}", "family": "cone_zero", "mechanism": "cone_zero", "threshold": k})
    return out


def write_report_md(payload: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append(f"# Relatorio - {TASK_ID}")
    lines.append("")
    lines.append(f"- Decision Ref: `{payload.get('decision_ref', DECISION_REF)}`")
    lines.append(f"- Status: `{payload.get('status', 'N/A')}`")
    lines.append(f"- Executado em UTC: `{payload.get('run_timestamp_utc', '')}`")
    lines.append("")
    lines.append("## Declaracoes de Escopo")
    lines.append("")
    lines.append("- Nenhuma venda de SNDK/VSH foi decidida por esta task.")
    lines.append("- Nenhuma regra operacional, skill ou motor blindado foi promovido.")
    lines.append("- Espelhamento BR permanece fora do escopo ate haver veredito US e validacao semantica R-026.")
    lines.append("- Nao ha selecao post-hoc de melhor configuracao; se os gates pre-analiticos falham, os bracos candidatos nao sao executados.")
    lines.append("")

    if payload.get("status") == "FAIL_METODOLOGICO":
        lines.append("## Falha Metodologica")
        lines.append("")
        lines.append(f"- Stage: `{payload.get('failed_stage')}`")
        lines.append(f"- Motivo: `{payload.get('reason')}`")
        gate1 = payload.get("gate1_hashes", {})
        gate2 = payload.get("gate2_reconciliation", {})
        if gate1 or gate2:
            lines.append(f"- Gate 1 hashes: `{gate1.get('status')}`")
            lines.append(f"- Gate 2 baseline: `{gate2.get('status')}`, max_rel_abs_diff=`{gate2.get('max_rel_abs_diff')}`")
            lines.append("- Bracos candidatos: `NAO_EXECUTADOS` por bloqueio pre-analitico.")
        OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    lines.append("## Gates")
    lines.append("")
    gate1 = payload.get("gate1_hashes", {})
    gate2 = payload.get("gate2_reconciliation", {})
    lines.append(f"- Gate 1 hashes: `{gate1.get('status')}`")
    lines.append(f"- Gate 2 baseline: `{gate2.get('status')}`, max_rel_abs_diff=`{gate2.get('max_rel_abs_diff')}`")
    lines.append("")
    lines.append("## Comparacoes")
    lines.append("")
    lines.append("| Braco | Familia | k | Veredito | Delta Sharpe val | Delta CAGR val | Triggers val |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in payload.get("comparisons", []):
        m_val = row["metrics"]["validacao"]
        lines.append(
            "| {variant} | {family} | {threshold} | {verdict} | {dsh:.6f} | {dcg:.6f} | {triggers} |".format(
                variant=row["variant"],
                family=row["family"],
                threshold=row.get("threshold"),
                verdict=row["verdict"]["veredito"],
                dsh=float(row["verdict"]["delta_sharpe_validacao"]),
                dcg=float(row["verdict"]["delta_cagr_validacao"]),
                triggers=int(m_val.get("c3_triggers", 0)),
            )
        )
    lines.append("")
    lines.append("## Nota Interpretativa")
    lines.append("")
    lines.append("Este relatorio e consultivo/read-only. Qualquer promocao futura exige nova decisao do Owner e nova task formal.")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    TASK_DIR.mkdir(parents=True, exist_ok=True)
    log(f"=== {TASK_ID} ===")
    log("Gate 1: checking frozen dataset hashes")
    gate1 = gate1_hashes()
    log(f"Gate 1 status: {gate1['status']}")
    if gate1["status"] != "PASS":
        fail_payload("gate1_hashes", "Frozen dataset hash mismatch.", {"gate1_hashes": gate1})
        return

    log("Preparing inputs from frozen dataset")
    inputs = prepare_inputs()
    log("Running unmodified C4 baseline")
    baseline_curve, baseline_events_def, baseline_events_split, baseline_events_trim = run_baseline(inputs)
    gate2 = gate2_reconcile_baseline(baseline_curve)
    log(f"Gate 2 status: {gate2['status']} max_rel_abs_diff={gate2.get('max_rel_abs_diff')}")
    if gate2["status"] != "PASS":
        fail_payload(
            "gate2_reconciliation",
            "Rerun baseline C4 did not reconcile with canonical_daily_history_us/curve.parquet.",
            {"gate1_hashes": gate1, "gate2_reconciliation": gate2},
        )
        return

    baseline_metrics = all_metrics(baseline_curve, baseline_events_def, pd.DataFrame())
    baseline_valid_returns = daily_log_returns(baseline_curve, start=VALIDATION_START, end=None)

    comparisons: list[dict[str, Any]] = []
    curves_summary: dict[str, Any] = {
        "baseline_C4": {
            "metrics": baseline_metrics,
            "events_defensive_rows": int(len(baseline_events_def)),
            "events_split_rows": int(len(baseline_events_split)),
            "events_trim_rows": int(len(baseline_events_trim)),
        }
    }

    for spec in candidate_specs():
        log(f"Running candidate {spec['variant']}")
        curve, ev_def, ev_split, ev_trim, ev_c3 = run_variant_with_camada3(
            variant_label=spec["variant"],
            mechanism=spec["mechanism"],
            threshold=spec["threshold"],
            inputs=inputs,
        )
        metrics = all_metrics(curve, ev_def, ev_c3)
        cand_valid_returns = daily_log_returns(curve, start=VALIDATION_START, end=None)
        boot = block_bootstrap_deltas(baseline_valid_returns, cand_valid_returns)
        ver = verdict(baseline_metrics, metrics, boot)
        comparisons.append(
            {
                **spec,
                "metrics": metrics,
                "bootstrap_validation": boot,
                "verdict": ver,
                "events": {
                    "defensive_rows": int(len(ev_def)),
                    "split_rows": int(len(ev_split)),
                    "trim_rows": int(len(ev_trim)),
                    "camada3_rows": int(len(ev_c3)),
                },
                "camada3_event_samples": ev_c3.head(10).to_dict("records") if not ev_c3.empty else [],
            }
        )
        curves_summary[spec["variant"]] = {
            "metrics": metrics,
            "events": {
                "defensive_rows": int(len(ev_def)),
                "split_rows": int(len(ev_split)),
                "trim_rows": int(len(ev_trim)),
                "camada3_rows": int(len(ev_c3)),
            },
        }

    payload = {
        "task_id": TASK_ID,
        "decision_ref": DECISION_REF,
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "COMPLETED",
        "gate1_hashes": gate1,
        "gate2_reconciliation": gate2,
        "config": {
            "winner_config": WINNER_CONFIG,
            "cone_k_values": CONE_K_VALUES,
            "cone_anchor_families": ["cone_mu", "cone_zero"],
            "quarantine_days": QUARANTINE_DAYS,
            "train_end": TRAIN_END,
            "validation_start": VALIDATION_START,
            "bootstrap": {"block_size": BLOCK_SIZE, "n_paths": N_BOOTSTRAP, "seed": BOOTSTRAP_SEED},
        },
        "inputs_coverage": {
            "canonical_rows": int(len(inputs["canonical"])),
            "canonical_date_min": inputs["canonical"]["date"].min(),
            "canonical_date_max": inputs["canonical"]["date"].max(),
            "scores_rows": int(len(inputs["scores"])),
            "scores_date_min": inputs["scores"]["date"].min(),
            "scores_date_max": inputs["scores"]["date"].max(),
            "blacklist_size": int(len(inputs["blacklist"])),
            "median_scored_tickers_pre_filter": float(inputs["median_pre_filter"]),
            "median_scored_tickers_post_filter": float(inputs["median_post_filter"]),
        },
        "baseline": curves_summary["baseline_C4"],
        "comparisons": comparisons,
    }
    write_json(OUT_JSON, payload)
    write_report_md(payload)
    write_log()
    log("Completed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log("UNHANDLED_EXCEPTION")
        log(str(exc))
        log(traceback.format_exc())
        payload = fail_payload("unhandled_exception", str(exc), {"traceback": traceback.format_exc()})
        write_json(OUT_JSON, payload)
        write_report_md(payload)
        write_log()
        raise

