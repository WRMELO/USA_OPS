"""Backtest da politica defensiva com caixa parado ate o proximo rebalance.

Task: T-SDC-DEFENSIVE-HOLD-CASH-DRIFT-LADDER-US-V1

Estudo read-only pre-registrado (R-041/R-048/R-061), com liquidacao D0:
- Zero-A: C4 puro sem venda defensiva adicional (controle);
- A0_R1: venda defensiva so por violacao SPC (R1), sem reinvestimento intra-ciclo;
- A1_LEVE: R1 OU cruzamento para LEVE, sem reinvestimento intra-ciclo;
- A2_MEDIO: R1 OU cruzamento para MEDIO, sem reinvestimento intra-ciclo;
- A3_GRAVE: R1 OU cruzamento para GRAVE, sem reinvestimento intra-ciclo.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import backtest.t_defensive_reinvest_policy_us_v2.run_t_defensive_reinvest_policy_us_v2 as v2  # noqa: E402

TASK_ID = "T-SDC-DEFENSIVE-HOLD-CASH-DRIFT-LADDER-US-V1"
ARMS = ["Zero-A", "A0_R1", "A1_LEVE", "A2_MEDIO", "A3_GRAVE"]
OFFSETS = list(range(10))
PAIR_DEFS = [
    ("Zero-A", "A0_R1"),
    ("Zero-A", "A1_LEVE"),
    ("Zero-A", "A2_MEDIO"),
    ("Zero-A", "A3_GRAVE"),
    ("A0_R1", "A1_LEVE"),
    ("A1_LEVE", "A2_MEDIO"),
    ("A2_MEDIO", "A3_GRAVE"),
]

DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2"
IN_CANONICAL = DATASET_DIR / "canonical_us.parquet"
IN_MANIFEST = DATASET_DIR / "manifest.json"
IN_WINNER = ROOT / "config" / "winner_us.json"
IN_BLACKLIST = ROOT / "data" / "ssot" / "blacklist_us.json"
IN_CRITERION = (
    ROOT
    / "backtest"
    / "t_defensive_hold_cash_drift_ladder_us_v1"
    / "decision_criterion_defensive_hold_cash_drift_ladder_us_v1.json"
)
OUT_DIR = ROOT / "backtest" / "t_defensive_hold_cash_drift_ladder_us_v1" / "results"

MEDIO_THRESHOLD_US = -12.85
GRAVE_THRESHOLD_US = -32.42


def _cross_below(heat_prev: float, heat_curr: float, threshold: float) -> bool:
    return bool(
        v2.v1._is_finite(heat_prev)
        and v2.v1._is_finite(heat_curr)
        and heat_prev > threshold
        and heat_curr <= threshold
    )


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

    lots: list[v2.rb.Lot] = []
    pending_cash: dict[pd.Timestamp, float] = {}
    cash_free = float(cfg["base_capital"])
    defensive_blocked: set[str] = set()
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
            v2.v1._normalize_scores_for_day(
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
        trigger_r1_today = 0
        trigger_leve_today = 0
        trigger_medio_today = 0
        trigger_grave_today = 0

        is_rebalance_day = bool(
            i >= first_rebalance_idx
            and (i - first_rebalance_idx) % max(cadence, 1) == 0
        )
        if is_rebalance_day:
            cycle_id += 1
            defensive_blocked = set()
            held = v2._active_tickers(
                lots,
                price_row,
                cash_free + v2.rb.lots_market_value(lots, price_row),
            )
            if not prev_scores.empty:
                frozen_target = v2.rb._select_c2_target(
                    prev_scores,
                    holdings=held,
                    top_n=top_n,
                    buffer_k=buffer_k,
                    quarantine=set(),
                )
                frozen_weights = v2.rb.compute_target_weights(
                    prev_scores,
                    frozen_target,
                    k_damp=k_damp,
                    max_weight_cap=max_weight_cap,
                )
            elif held:
                frozen_target = sorted(held)[:top_n]
                eq = 1.0 / float(max(len(frozen_target), 1))
                frozen_weights = {ticker: eq for ticker in frozen_target}

            lots, cash_free, pending_cash, gross_s, gross_b, cost_r, _ = v2.v1._rebalance_to_target(
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
                v2._active_tickers(
                    lots,
                    price_row,
                    cash_free + v2.rb.lots_market_value(lots, price_row),
                )
            )
            spc_set = set(spc_blocked_by_day.get(d_prev, set()))
            trigger_tickers: dict[str, set[str]] = {}

            for ticker in held_now:
                if ticker in defensive_blocked:
                    continue
                reasons: set[str] = set()
                if ticker in spc_set:
                    reasons.add("R1")

                if arm_name != "A0_R1" and d_prev2 is not None:
                    avg_cost = v2.v1._ticker_avg_cost(lots, ticker)
                    close_prev = (
                        v2.v1._safe_float(px_exec_wide.at[d_prev, ticker], np.nan)
                        if ticker in px_exec_wide.columns
                        else np.nan
                    )
                    close_prev2 = (
                        v2.v1._safe_float(px_exec_wide.at[d_prev2, ticker], np.nan)
                        if ticker in px_exec_wide.columns
                        else np.nan
                    )
                    heat_older = v2.v1._heat_pct(close_prev2, avg_cost)
                    heat_newer = v2.v1._heat_pct(close_prev, avg_cost)
                    if arm_name == "A1_LEVE" and _cross_below(heat_older, heat_newer, 0.0):
                        reasons.add("LEVE")
                    elif arm_name == "A2_MEDIO" and _cross_below(
                        heat_older, heat_newer, MEDIO_THRESHOLD_US
                    ):
                        reasons.add("MEDIO")
                    elif arm_name == "A3_GRAVE" and _cross_below(
                        heat_older, heat_newer, GRAVE_THRESHOLD_US
                    ):
                        reasons.add("GRAVE")

                if reasons:
                    trigger_tickers[ticker] = reasons

            for ticker, reasons in trigger_tickers.items():
                lots, proceeds, cost, sold_shares = v2._sell_all_shares_exact(
                    ticker=ticker,
                    lots=lots,
                    price_row=price_row,
                    friction=friction,
                    pending_cash=pending_cash,
                    exec_day=d,
                )
                if sold_shares <= 0:
                    continue
                cash_free += v2.v1._consume_same_day_settlement(d, pending_cash)
                defensive_blocked.add(ticker)
                defensive_sells_today += 1
                trigger_r1_today += int("R1" in reasons)
                trigger_leve_today += int("LEVE" in reasons)
                trigger_medio_today += int("MEDIO" in reasons)
                trigger_grave_today += int("GRAVE" in reasons)
                costs_today += cost
                gross_sells_today += proceeds + cost

        cash_ret = float(np.expm1(float(cash_log_daily.get(d, 0.0))))
        if cash_free > 0:
            cash_free *= 1.0 + cash_ret

        holdings_value = v2.rb.lots_market_value(lots, price_row)
        cash_pending = float(sum(float(v) for v in pending_cash.values()))
        equity_end = float(cash_free + cash_pending + holdings_value)
        active_tickers = v2._active_tickers(lots, price_row, equity_end)
        raw_tickers = {
            ticker
            for ticker, ticker_lots in v2.rb.split_lots_by_ticker(lots).items()
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
        split = v2.prev._to_split(d, holdout_end=holdout_end)
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
                "cash_idle_gt5": int(equity_end > 0 and cash_ratio > 0.05),
                "cash_ratio": cash_ratio,
                "trade_cost_today": float(costs_today),
                "gross_sells_today": float(gross_sells_today),
                "gross_buys_today": float(gross_buys_today),
                "is_rebalance_day": int(is_rebalance_day),
                "had_scores": int(had_scores),
                "first_rebalance_expected_date": trading_days[first_rebalance_idx],
                "trigger_r1_today": int(trigger_r1_today),
                "trigger_leve_today": int(trigger_leve_today),
                "trigger_medio_today": int(trigger_medio_today),
                "trigger_grave_today": int(trigger_grave_today),
            }
        )

    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


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
        run = v2._max_true_run((started["cash_ratio"] > 0.95) & (started["had_scores"] == 1))
        if run > 10:
            g3_fail.append({"arm": arm, "offset": int(offset), "max_run": run})
    details["G3"] = {"status": "PASS" if not g3_fail else "FAIL", "failures": g3_fail}

    accounting_error = (
        curves["equity"] - (curves["cash_free"] + curves["cash_pending"] + curves["holdings_value"])
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

    g7_fail_rows = curves[
        (curves["arm"] != "Zero-A")
        & (pd.to_numeric(curves["gross_buys_today"], errors="coerce") > 0)
        & (pd.to_numeric(curves["is_rebalance_day"], errors="coerce") == 0)
    ]
    g7_fail: list[dict[str, Any]] = []
    for row in g7_fail_rows.itertuples(index=False):
        g7_fail.append(
            {
                "arm": str(row.arm),
                "offset": int(row.offset),
                "date": str(pd.Timestamp(row.date).date()),
                "gross_buys_today": float(row.gross_buys_today),
            }
        )
    details["G7"] = {"status": "PASS" if not g7_fail else "FAIL", "failures": g7_fail}

    gates = [{"gate": gate, **payload} for gate, payload in details.items()]
    overall = "PASS" if all(item["status"] == "PASS" for item in gates) else "FAIL"
    return {"task_id": TASK_ID, "overall_status": overall, "gates": gates}


def main() -> None:
    criterion = json.loads(IN_CRITERION.read_text(encoding="utf-8"))
    if not criterion.get("registered_before_execution"):
        raise RuntimeError("Pre-registro nao marcado como anterior a execucao.")

    holdout_end, manifest = v2.prev._load_holdout_end_from_manifest(IN_MANIFEST)
    if bool(criterion["dataset"].get("required_hash_verification", False)):
        v2.prevr037._verify_manifest_hashes(manifest, DATASET_DIR)
        print("Hashes conferidos com sucesso contra manifest.json.")

    cfg = v2.v1._load_winner_snapshot_full(IN_WINNER)
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

    blacklist = v2.prev._load_blacklist(IN_BLACKLIST)
    if blacklist:
        canonical = canonical[~canonical["ticker"].isin(blacklist)].copy()
    spc_blocked_by_day = v2.prev._build_spc_blocked_by_day(canonical)
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
    scores_by_day = v2.prev._compute_scores_by_day(px_exec_wide, holdout_end=holdout_end)
    first_scores_idx = v2._first_scores_execution_idx(
        trading_days=trading_days,
        scores_by_day=scores_by_day,
        mc_eligible_by_day=mc_eligible_by_day,
        blacklist=blacklist,
    )

    macro = pd.read_parquet(DATASET_DIR / "macro_us.parquet").copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date", "fed_funds_rate"]).sort_values("date")
    cash_log_daily = v2.rb.build_cash_log_daily(macro)

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
    curves_df = pd.concat(curves, ignore_index=True).sort_values(["offset", "arm", "date"])
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    curves_df.to_csv(
        OUT_DIR / "observations_defensive_hold_cash_drift_ladder_us_v1.csv",
        index=False,
    )

    sanity = _run_sanity_gates(curves_df)
    (OUT_DIR / "sanity_gates_report_v1.json").write_text(
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
                row = v2.v1._summarize_split_arm(phase, subset=split, arm=arm)
                row["offset"] = offset
                summaries_by_offset[offset][split][arm] = row
                rows.append(row)
        pd.DataFrame(rows).to_csv(
            OUT_DIR / f"summary_{split}_defensive_hold_cash_drift_ladder_us_v1.csv",
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
            v2._offset_verdict_payload(
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
        representative = v2._representative_offset(offset_payloads)
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
            final = f"FAVORECIDO_{v2.v1._sanitize_label(arm)}"
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

    (OUT_DIR / "bootstrap_stats_defensive_hold_cash_drift_ladder_us_v1.json").write_text(
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
    (OUT_DIR / "verdict_defensive_hold_cash_drift_ladder_us_v1.json").write_text(
        json.dumps(verdict, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"{TASK_ID} concluido.")
    print(f"first_scores_exec_day={trading_days[first_scores_idx].date()}")
    print(f"rows_observations={len(curves_df)}")
    for pair_key, payload in verdict_pairs.items():
        print(f"{pair_key}={payload['final_verdict']}")


if __name__ == "__main__":
    main()
