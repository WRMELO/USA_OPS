"""Backtest V2 da politica defensiva com caixa parado ate rebalance.

Task: T-SDC-DEFENSIVE-HOLD-CASH-DRIFT-LADDER-US-V2

Correcao especifica de harness sobre a V1:
- preserva _simulate_arm e todas as regras de negocio da V1;
- preserva G1/G2/G3/G4/G5/G7 exatamente;
- altera apenas a janela de medicao do G6 para dias de rebalanceamento.
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

import backtest.t_defensive_hold_cash_drift_ladder_us_v1.run_t_defensive_hold_cash_drift_ladder_us_v1 as l1  # noqa: E402

TASK_ID = "T-SDC-DEFENSIVE-HOLD-CASH-DRIFT-LADDER-US-V2"
ARMS = l1.ARMS
OFFSETS = l1.OFFSETS
PAIR_DEFS = l1.PAIR_DEFS

DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2"
IN_CANONICAL = DATASET_DIR / "canonical_us.parquet"
IN_MANIFEST = DATASET_DIR / "manifest.json"
IN_WINNER = ROOT / "config" / "winner_us.json"
IN_BLACKLIST = ROOT / "data" / "ssot" / "blacklist_us.json"
IN_CRITERION = (
    ROOT
    / "backtest"
    / "t_defensive_hold_cash_drift_ladder_us_v2"
    / "decision_criterion_defensive_hold_cash_drift_ladder_us_v2.json"
)
OUT_DIR = ROOT / "backtest" / "t_defensive_hold_cash_drift_ladder_us_v2" / "results"


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
        run = l1.v2._max_true_run((started["cash_ratio"] > 0.95) & (started["had_scores"] == 1))
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
        started = group[group["is_rebalance_day"] == 1]
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

    holdout_end, manifest = l1.v2.prev._load_holdout_end_from_manifest(IN_MANIFEST)
    if bool(criterion["dataset"].get("required_hash_verification", False)):
        l1.v2.prevr037._verify_manifest_hashes(manifest, DATASET_DIR)
        print("Hashes conferidos com sucesso contra manifest.json.")

    cfg = l1.v2.v1._load_winner_snapshot_full(IN_WINNER)
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

    blacklist = l1.v2.prev._load_blacklist(IN_BLACKLIST)
    if blacklist:
        canonical = canonical[~canonical["ticker"].isin(blacklist)].copy()
    spc_blocked_by_day = l1.v2.prev._build_spc_blocked_by_day(canonical)
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
    scores_by_day = l1.v2.prev._compute_scores_by_day(px_exec_wide, holdout_end=holdout_end)
    first_scores_idx = l1.v2._first_scores_execution_idx(
        trading_days=trading_days,
        scores_by_day=scores_by_day,
        mc_eligible_by_day=mc_eligible_by_day,
        blacklist=blacklist,
    )

    macro = pd.read_parquet(DATASET_DIR / "macro_us.parquet").copy()
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce").dt.normalize()
    macro = macro.dropna(subset=["date", "fed_funds_rate"]).sort_values("date")
    cash_log_daily = l1.v2.rb.build_cash_log_daily(macro)

    curves: list[pd.DataFrame] = []
    for offset in OFFSETS:
        for arm in ARMS:
            print(f"Executando arm={arm} offset={offset} ...")
            curves.append(
                l1._simulate_arm(
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
        OUT_DIR / "observations_defensive_hold_cash_drift_ladder_us_v2.csv",
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
                row = l1.v2.v1._summarize_split_arm(phase, subset=split, arm=arm)
                row["offset"] = offset
                summaries_by_offset[offset][split][arm] = row
                rows.append(row)
        pd.DataFrame(rows).to_csv(
            OUT_DIR / f"summary_{split}_defensive_hold_cash_drift_ladder_us_v2.csv",
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
            l1.v2._offset_verdict_payload(
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
        representative = l1.v2._representative_offset(offset_payloads)
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
            final = f"FAVORECIDO_{l1.v2.v1._sanitize_label(arm)}"
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

    (OUT_DIR / "bootstrap_stats_defensive_hold_cash_drift_ladder_us_v2.json").write_text(
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
    (OUT_DIR / "verdict_defensive_hold_cash_drift_ladder_us_v2.json").write_text(
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
