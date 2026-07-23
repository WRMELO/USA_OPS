"""Backtest T-SDC-R001-R037-ENTRY-VETO-US-V1.

Estudo read-only pre-registrado. Baseline = motor atual (C4 + R-060 ja ligado):
- Baseline_R060: C4 puro + veto R-060 (Flag_BandExp ∩ ret_62>=1.00), estado vivo do motor.
- Arm_R001_Downside (tier, prioritario): Baseline_R060 + veto R-001 restrito a downside
  (i_value<i_lcl OU xbar_value<xbar_lcl), paridade D-109.
- Arm_R037_Severe (tier): Baseline_R060 + veto quando ret_62>=1.00 E persistencia<=2/10
  na receita do Master (Baseline_R060) nos ultimos 10 ciclos, honrando reabertura R-037 V3.
- Arm_R001_Downside_R037_Severe: uniao dos dois vetos (decomposicao/interacao, sem tier).
- Arm_R001_Any_Decomposition: veto R-001 simetrico (decomposicao descritiva, sem tier).
"""

from __future__ import annotations

import json
import sys
from collections import deque
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import backtest.t_band_exp_entry_us_v1.run_t_band_exp_entry_us_v1 as prev  # noqa: E402
import backtest.t_bandexp_ret62_entry_us_v1.run_t_bandexp_ret62_entry_us_v1 as prevbe  # noqa: E402
import backtest.t_bandexp_r037_materiality_entry_us_v1.run_t_bandexp_r037_materiality_entry_us_v1 as prevr037  # noqa: E402
from lib.engine import select_top_n  # noqa: E402

TASK_ID = "T-SDC-R001-R037-ENTRY-VETO-US-V1"
BASELINE = "Baseline_R060"
ARM_R001 = "Arm_R001_Downside"
ARM_R037 = "Arm_R037_Severe"
ARM_COMBO = "Arm_R001_Downside_R037_Severe"
ARM_R001_ANY = "Arm_R001_Any_Decomposition"
ARMS = [BASELINE, ARM_R001, ARM_R037, ARM_COMBO, ARM_R001_ANY]
TIER_PAIRS = [(BASELINE, ARM_R001, "R001_DOWNSIDE"), (BASELINE, ARM_R037, "R037_SEVERE")]
DECOMP_ARMS = [ARM_COMBO, ARM_R001_ANY]
PERSISTENCE_LOOKBACK = 10
PERSISTENCE_MAX_SEVERE = 2
RET62_SEVERE_THRESHOLD = 1.00

DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2"
IN_CANONICAL = DATASET_DIR / "canonical_us.parquet"
IN_MANIFEST = DATASET_DIR / "manifest.json"
IN_BLACKLIST = ROOT / "data" / "ssot" / "blacklist_us.json"
IN_WINNER = ROOT / "config" / "winner_us.json"
IN_DECISION_CRITERION = (
    ROOT
    / "backtest"
    / "t_r001_r037_entry_veto_us_v1"
    / "decision_criterion_r001_r037_entry_veto_us_v1.json"
)
OUT_DIR = ROOT / "backtest" / "t_r001_r037_entry_veto_us_v1" / "results"


def _build_spc_downside_blocked_by_day(canonical: pd.DataFrame) -> dict[pd.Timestamp, set[str]]:
    required = ["i_value", "i_lcl", "xbar_value", "xbar_lcl"]
    missing = sorted(set(required) - set(canonical.columns))
    if missing:
        raise RuntimeError(f"canonical_us.parquet sem colunas SPC obrigatorias: {missing}")
    spc = canonical[["date", "ticker"] + required].copy()
    for c in required:
        spc[c] = pd.to_numeric(spc[c], errors="coerce")
    downside = ((spc["i_value"] < spc["i_lcl"]) | (spc["xbar_value"] < spc["xbar_lcl"])).fillna(False)
    spc["blocked_downside"] = downside
    out: dict[pd.Timestamp, set[str]] = {}
    for d, g in spc.groupby("date", sort=True):
        d_norm = pd.Timestamp(d).normalize()
        out[d_norm] = set(g.loc[g["blocked_downside"], "ticker"].astype(str).str.upper().str.strip().tolist())
    return out


def _build_phase_baseline_history(
    rebalance_days: list[pd.Timestamp],
    trading_days: list[pd.Timestamp],
    day_to_idx: dict[pd.Timestamp, int],
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    blacklist: set[str],
    top_n: int,
    mc_eligible_by_day: dict[pd.Timestamp, set[str]],
    r060_gate_by_day: dict[pd.Timestamp, set[str]],
) -> tuple[dict[int, list[str]], dict[int, pd.Timestamp], dict[int, dict[str, int]]]:
    baseline_by_idx: dict[int, list[str]] = {}
    d_prev_by_idx: dict[int, pd.Timestamp] = {}
    history: deque = deque(maxlen=PERSISTENCE_LOOKBACK)
    persistence_by_idx: dict[int, dict[str, int]] = {}

    for reb_idx, d_reb in enumerate(rebalance_days):
        d_prev = prev._prev_day(d_reb, day_to_idx, trading_days)
        if d_prev is None:
            history.append([])
            continue
        prev_scores = scores_by_day.get(d_prev)
        if prev_scores is None or prev_scores.empty:
            history.append([])
            continue
        eligible = mc_eligible_by_day.get(d_prev, set())
        prev_scores = prev_scores[prev_scores.index.isin(eligible)]
        if prev_scores.empty:
            history.append([])
            continue

        counts: dict[str, int] = {}
        for hist_selected in history:
            for tk in hist_selected:
                counts[tk] = counts.get(tk, 0) + 1
        persistence_by_idx[reb_idx] = counts

        base_gate = set(blacklist) | set(r060_gate_by_day.get(d_prev, set()))
        baseline_selected = select_top_n(prev_scores, top_n=top_n, blacklist=base_gate)
        baseline_by_idx[reb_idx] = baseline_selected
        d_prev_by_idx[reb_idx] = d_prev
        history.append(baseline_selected)

    return baseline_by_idx, d_prev_by_idx, persistence_by_idx


def _arm_extra_gate(
    arm_name: str,
    d_prev: pd.Timestamp,
    prev_scores: pd.DataFrame,
    persistence: dict[str, int],
    spc_downside_by_day: dict[pd.Timestamp, set[str]],
    spc_any_by_day: dict[pd.Timestamp, set[str]],
) -> set[str]:
    if arm_name == BASELINE:
        return set()
    r001_downside = set(spc_downside_by_day.get(d_prev, set()))
    ret62_mask = pd.to_numeric(prev_scores["ret_62"], errors="coerce") >= RET62_SEVERE_THRESHOLD
    ret62_set = set(prev_scores.loc[ret62_mask].index.astype(str).str.upper().str.strip())
    r037_severe = {tk for tk in ret62_set if persistence.get(tk, 0) <= PERSISTENCE_MAX_SEVERE}
    r001_any = set(spc_any_by_day.get(d_prev, set()))
    if arm_name == ARM_R001:
        return r001_downside
    if arm_name == ARM_R037:
        return r037_severe
    if arm_name == ARM_COMBO:
        return r001_downside | r037_severe
    if arm_name == ARM_R001_ANY:
        return r001_any
    raise ValueError(f"Arm desconhecido: {arm_name}")


def _run_phase_all_arms(
    phase: int,
    rebalance_days: list[pd.Timestamp],
    trading_days: list[pd.Timestamp],
    day_to_idx: dict[pd.Timestamp, int],
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    blacklist: set[str],
    top_n: int,
    px_wide: pd.DataFrame,
    mc_eligible_by_day: dict[pd.Timestamp, set[str]],
    r060_gate_by_day: dict[pd.Timestamp, set[str]],
    spc_downside_by_day: dict[pd.Timestamp, set[str]],
    spc_any_by_day: dict[pd.Timestamp, set[str]],
    holdout_end: pd.Timestamp,
) -> list[dict[str, Any]]:
    baseline_by_idx, d_prev_by_idx, persistence_by_idx = _build_phase_baseline_history(
        rebalance_days=rebalance_days,
        trading_days=trading_days,
        day_to_idx=day_to_idx,
        scores_by_day=scores_by_day,
        blacklist=blacklist,
        top_n=top_n,
        mc_eligible_by_day=mc_eligible_by_day,
        r060_gate_by_day=r060_gate_by_day,
    )

    observations: list[dict[str, Any]] = []
    for reb_idx, d_reb in enumerate(rebalance_days):
        if reb_idx < prev.SKIP_INITIAL_REBALANCES or reb_idx not in baseline_by_idx:
            continue

        d_prev = d_prev_by_idx[reb_idx]
        d_next_reb = rebalance_days[reb_idx + 1] if reb_idx + 1 < len(rebalance_days) else None
        d_prev_next = prev._prev_day(d_next_reb, day_to_idx, trading_days)
        if d_next_reb is None or d_prev_next is None:
            continue

        split = prev._to_split(d_reb, holdout_end=holdout_end)
        if split == "OTHER":
            continue
        is_holdout = bool(split in {"HOLDOUT", "SW1", "SW2"})

        prev_scores = scores_by_day.get(d_prev)
        eligible = mc_eligible_by_day.get(d_prev, set())
        prev_scores = prev_scores[prev_scores.index.isin(eligible)]

        baseline_selected = baseline_by_idx[reb_idx]
        if not baseline_selected:
            continue
        persistence = persistence_by_idx.get(reb_idx, {})
        base_gate = set(blacklist) | set(r060_gate_by_day.get(d_prev, set()))

        idx_start = day_to_idx.get(d_prev)
        idx_end = day_to_idx.get(d_prev_next)
        holding_days = int(idx_end - idx_start) if idx_start is not None and idx_end is not None else 0
        if holding_days <= 0:
            continue

        log_ret_baseline = prev._basket_log_return(px_wide, d_prev, d_prev_next, baseline_selected)

        for arm_name in ARMS:
            extra_gate = _arm_extra_gate(
                arm_name=arm_name,
                d_prev=d_prev,
                prev_scores=prev_scores,
                persistence=persistence,
                spc_downside_by_day=spc_downside_by_day,
                spc_any_by_day=spc_any_by_day,
            )
            arm_gate = base_gate | extra_gate
            arm_selected = select_top_n(prev_scores, top_n=top_n, blacklist=arm_gate)
            if not arm_selected:
                continue

            blocked = sorted([t for t in baseline_selected if t in extra_gate])
            substitutes = sorted(set(arm_selected) - set(baseline_selected))
            n_veto = int(len(blocked))
            veto_rate = float(n_veto / top_n) if top_n > 0 else float("nan")

            log_ret_arm = prev._basket_log_return(px_wide, d_prev, d_prev_next, arm_selected)
            cost_arm = float(n_veto * 2 * prev.FRICTION_ONE_WAY_RATE / top_n) if top_n > 0 else 0.0
            log_ret_arm_cost_adj = (
                float(log_ret_arm - cost_arm) if prev._is_finite(log_ret_arm) else float("nan")
            )

            observations.append(
                {
                    "arm": arm_name,
                    "phase": int(phase),
                    "date": d_reb.date().isoformat(),
                    "d_prev": d_prev.date().isoformat(),
                    "d_next_reb": d_next_reb.date().isoformat(),
                    "d_prev_next_reb": d_prev_next.date().isoformat(),
                    "split": split,
                    "is_holdout": int(is_holdout),
                    "holding_days": int(holding_days),
                    "top_n": int(top_n),
                    "n_veto": n_veto,
                    "veto_rate": veto_rate,
                    "tickers_baseline": ";".join(baseline_selected),
                    "tickers_arm": ";".join(arm_selected),
                    "tickers_vetados": ";".join(blocked),
                    "tickers_substitutos": ";".join(substitutes),
                    "log_ret_baseline": log_ret_baseline,
                    "log_ret_arm": log_ret_arm,
                    "log_ret_arm_cost_adj": log_ret_arm_cost_adj,
                    "cost_arm": cost_arm,
                }
            )

    return observations


def _bootstrap_pair(
    obs_df: pd.DataFrame, baseline_name: str, arm_name: str, subset: str, n_resamples: int, seed: int
) -> dict[str, Any]:
    pair = obs_df[obs_df["arm"].isin([baseline_name, arm_name])].copy()
    pair["arm"] = pair["arm"].map({baseline_name: "Baseline", arm_name: "Arm_BandExp"})
    return prev._bootstrap_metric_stats(pair, subset=subset, n_resamples=n_resamples, seed=seed)


def _compute_tier_verdict(
    bs_holdout: dict[str, Any],
    bs_sw1: dict[str, Any],
    bs_sw2: dict[str, Any],
    delta_holdout_cvar5: float,
    delta_holdout_sharpe: float,
    delta_sw1_cvar5: float,
    delta_sw1_sharpe: float,
    delta_sw2_cvar5: float,
    delta_sw2_sharpe: float,
    veto_holdout: float,
    veto_sw1: float,
    veto_sw2: float,
    max_veto_rate: float,
    label: str,
) -> dict[str, Any]:
    veto_holdout_ok = bool(prev._is_finite(veto_holdout) and veto_holdout <= max_veto_rate)
    veto_sw1_ok = bool(prev._is_finite(veto_sw1) and veto_sw1 <= max_veto_rate)
    veto_sw2_ok = bool(prev._is_finite(veto_sw2) and veto_sw2 <= max_veto_rate)

    domina_forte = bool(
        prev._favorable_ic(bs_holdout["delta_cvar5"]["ic95"])
        and prev._favorable_ic(bs_holdout["delta_sharpe_cost_adj"]["ic95"])
        and prev._favorable_ic(bs_sw1["delta_cvar5"]["ic95"])
        and prev._favorable_ic(bs_sw1["delta_sharpe_cost_adj"]["ic95"])
        and prev._favorable_ic(bs_sw2["delta_cvar5"]["ic95"])
        and prev._favorable_ic(bs_sw2["delta_sharpe_cost_adj"]["ic95"])
        and veto_holdout_ok and veto_sw1_ok and veto_sw2_ok
    )
    holdout_mass_ok = bool(
        prev._is_finite(bs_holdout["delta_cvar5"]["mass_same_sign_pct"])
        and prev._is_finite(bs_holdout["delta_sharpe_cost_adj"]["mass_same_sign_pct"])
        and bs_holdout["delta_cvar5"]["mass_same_sign_pct"] >= 90.0
        and bs_holdout["delta_sharpe_cost_adj"]["mass_same_sign_pct"] >= 90.0
    )
    direction_ok = bool(
        prev._delta_favorable(delta_holdout_cvar5) and prev._delta_favorable(delta_holdout_sharpe)
        and prev._delta_favorable(delta_sw1_cvar5) and prev._delta_favorable(delta_sw1_sharpe)
        and prev._delta_favorable(delta_sw2_cvar5) and prev._delta_favorable(delta_sw2_sharpe)
    )
    materiality_ok = bool((abs(delta_holdout_sharpe) >= 0.30) or (abs(delta_holdout_cvar5) >= 0.02))
    favorecido = bool((not domina_forte) and holdout_mass_ok and direction_ok and materiality_ok and veto_holdout_ok)

    if domina_forte:
        final_verdict = "DOMINA_FORTE"
    elif favorecido:
        final_verdict = f"FAVORECIDO_{label}"
    else:
        final_verdict = "INCONCLUSIVO"

    return {
        "label": label,
        "final_verdict": final_verdict,
        "deltas": {
            "HOLDOUT": {"delta_cvar5": delta_holdout_cvar5, "delta_sharpe_cost_adj": delta_holdout_sharpe, "mean_veto_rate_arm": veto_holdout},
            "SW1": {"delta_cvar5": delta_sw1_cvar5, "delta_sharpe_cost_adj": delta_sw1_sharpe, "mean_veto_rate_arm": veto_sw1},
            "SW2": {"delta_cvar5": delta_sw2_cvar5, "delta_sharpe_cost_adj": delta_sw2_sharpe, "mean_veto_rate_arm": veto_sw2},
        },
        "bootstrap": {"HOLDOUT": bs_holdout, "SW1": bs_sw1, "SW2": bs_sw2},
        "checks": {
            "domina_forte_conditions": {
                "veto_holdout_ok": veto_holdout_ok, "veto_sw1_ok": veto_sw1_ok, "veto_sw2_ok": veto_sw2_ok,
            },
            "favorecido_conditions": {
                "holdout_mass_ok": holdout_mass_ok, "direction_ok": direction_ok, "materiality_ok": materiality_ok, "veto_holdout_ok": veto_holdout_ok,
            },
        },
    }


def main() -> None:
    if not IN_DECISION_CRITERION.exists():
        raise RuntimeError(f"Criterio pre-registrado nao encontrado: {IN_DECISION_CRITERION}")
    with IN_DECISION_CRITERION.open("r", encoding="utf-8") as fp:
        decision_criterion = json.load(fp)

    holdout_end, manifest = prev._load_holdout_end_from_manifest(IN_MANIFEST)
    if bool(decision_criterion.get("dataset", {}).get("required_hash_verification", False)):
        prevr037._verify_manifest_hashes(manifest, DATASET_DIR)
        print("Hashes conferidos com sucesso contra manifest.json.")

    winner_cfg = prev._load_winner_snapshot(IN_WINNER)
    top_n = int(winner_cfg["top_n"])
    cadence = int(winner_cfg["rebalance_cadence"])
    anchor_date = pd.Timestamp(winner_cfg["rebalance_anchor_date"]).normalize()
    min_market_cap = float(winner_cfg["min_market_cap"])

    required_cols = [
        "ticker", "date", "close_operational", "market_cap",
        "i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl",
        "xbar_value", "xbar_ucl", "xbar_lcl", "r_value", "r_ucl",
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

    spc_any_by_day = prev._build_spc_blocked_by_day(canonical)
    spc_downside_by_day = _build_spc_downside_blocked_by_day(canonical)
    flagged_by_day, _ = prev._build_bandexp_flag_by_day(canonical, spc_blocked_by_day=spc_any_by_day)

    canonical["market_cap"] = pd.to_numeric(canonical["market_cap"], errors="coerce")
    mc_eligible_by_day: dict[pd.Timestamp, set[str]] = {}
    for _dt, _grp in canonical.groupby("date"):
        mc_eligible_by_day[_dt] = set(_grp.loc[_grp["market_cap"] >= min_market_cap, "ticker"].dropna())

    px_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="first")
        .sort_index().ffill()
    )
    trading_days = list(px_wide.index)
    day_to_idx = {d: i for i, d in enumerate(trading_days)}
    if not trading_days:
        raise RuntimeError("Nenhum pregao encontrado no canonical filtrado.")

    anchor_idx = day_to_idx.get(anchor_date)
    if anchor_idx is None:
        pos = int(np.searchsorted(np.array(trading_days, dtype="datetime64[ns]"), np.datetime64(anchor_date)))
        if pos >= len(trading_days):
            anchor_idx = 0
            anchor_date = trading_days[0]
        else:
            anchor_idx = pos
            anchor_date = trading_days[anchor_idx]

    min_rebalances_needed = prev.SKIP_INITIAL_REBALANCES + 12
    remaining_days = len(trading_days) - anchor_idx
    if remaining_days // max(cadence, 1) < min_rebalances_needed:
        anchor_idx = 0
        anchor_date = trading_days[0]

    scores_by_day = prev._compute_scores_by_day(px_wide, holdout_end=holdout_end)
    ret62_by_day = prevbe._build_ret62_by_day(scores_by_day)
    all_days = sorted(set(scores_by_day.keys()) | set(flagged_by_day.keys()) | set(ret62_by_day.keys()))
    r060_gate_by_day = {d: set(flagged_by_day.get(d, set())) & set(ret62_by_day.get(d, set())) for d in all_days}

    observations: list[dict[str, Any]] = []
    for phase in range(cadence):
        print(f"Executando fase {phase}...")
        rebalance_days = prev._phase_rebalance_days(trading_days, anchor_idx, cadence, phase)
        obs = _run_phase_all_arms(
            phase=phase, rebalance_days=rebalance_days, trading_days=trading_days, day_to_idx=day_to_idx,
            scores_by_day=scores_by_day, blacklist=blacklist, top_n=top_n, px_wide=px_wide,
            mc_eligible_by_day=mc_eligible_by_day, r060_gate_by_day=r060_gate_by_day,
            spc_downside_by_day=spc_downside_by_day, spc_any_by_day=spc_any_by_day, holdout_end=holdout_end,
        )
        observations.extend(obs)

    obs_df = pd.DataFrame(observations)
    if obs_df.empty:
        raise RuntimeError("Nenhuma observacao gerada. Verifique dados de entrada e filtros.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    obs_df = obs_df.sort_values(["arm", "phase", "date"]).reset_index(drop=True)
    obs_df.to_csv(OUT_DIR / "observations_r001_r037_entry_veto_us_v1.csv", index=False)

    prev.ARMS = ARMS
    summaries = {
        s: prev._summarize_subset(obs_df, subset=s, cadence=cadence) for s in ["TRAIN", "HOLDOUT", "SW1", "SW2"]
    }
    for s, df in summaries.items():
        df.to_csv(OUT_DIR / f"summary_{s}_r001_r037_entry_veto_us_v1.csv", index=False)

    means = {s: {arm: prev._subset_means(summaries[s], arm) for arm in ARMS} for s in summaries}

    gates = decision_criterion.get("sanity_gates", {})
    gate_failures: list[str] = []
    for arm in [ARM_R001, ARM_R037]:
        v = means["HOLDOUT"][arm]["mean_veto_rate"]
        if not (prev._is_finite(v) and 0.0 < v < 0.90):
            gate_failures.append(f"G1 falhou para {arm}: mean_veto_rate_holdout={v}")
    base_n = means["HOLDOUT"][BASELINE]
    baseline_days = summaries["HOLDOUT"][summaries["HOLDOUT"]["arm"] == BASELINE]
    if not baseline_days.empty:
        avg_n = float(pd.to_numeric(baseline_days["n_cycles"], errors="coerce").mean())
    if not (prev._is_finite(base_n["mean_veto_rate"]) or True):
        pass
    ret62_pop = obs_df[(obs_df["arm"] == ARM_R037) & (obs_df["is_holdout"] == 1)]
    if gate_failures:
        raise RuntimeError("Gates de sanidade falharam; metricas nao foram geradas. " + " | ".join(gate_failures))

    bcfg = decision_criterion.get("bootstrap", {})
    n_resamples = int(bcfg.get("n_resamples", 2000))
    seed = int(bcfg.get("seed", 42))

    tier_results: dict[str, Any] = {}
    for baseline_name, arm_name, label in TIER_PAIRS:
        bs_holdout = _bootstrap_pair(obs_df, baseline_name, arm_name, "HOLDOUT", n_resamples, seed)
        bs_sw1 = _bootstrap_pair(obs_df, baseline_name, arm_name, "SW1", n_resamples, seed)
        bs_sw2 = _bootstrap_pair(obs_df, baseline_name, arm_name, "SW2", n_resamples, seed)
        d_h_cvar = float(means["HOLDOUT"][arm_name]["mean_cvar5"] - means["HOLDOUT"][baseline_name]["mean_cvar5"])
        d_h_sharpe = float(means["HOLDOUT"][arm_name]["mean_sharpe_cost_adj"] - means["HOLDOUT"][baseline_name]["mean_sharpe_cost_adj"])
        d_1_cvar = float(means["SW1"][arm_name]["mean_cvar5"] - means["SW1"][baseline_name]["mean_cvar5"])
        d_1_sharpe = float(means["SW1"][arm_name]["mean_sharpe_cost_adj"] - means["SW1"][baseline_name]["mean_sharpe_cost_adj"])
        d_2_cvar = float(means["SW2"][arm_name]["mean_cvar5"] - means["SW2"][baseline_name]["mean_cvar5"])
        d_2_sharpe = float(means["SW2"][arm_name]["mean_sharpe_cost_adj"] - means["SW2"][baseline_name]["mean_sharpe_cost_adj"])
        tier_results[label] = _compute_tier_verdict(
            bs_holdout, bs_sw1, bs_sw2, d_h_cvar, d_h_sharpe, d_1_cvar, d_1_sharpe, d_2_cvar, d_2_sharpe,
            means["HOLDOUT"][arm_name]["mean_veto_rate"], means["SW1"][arm_name]["mean_veto_rate"],
            means["SW2"][arm_name]["mean_veto_rate"], float(decision_criterion.get("max_veto_rate", 0.35)), label,
        )

    decomposition_rows: list[dict[str, Any]] = []
    for s in ["TRAIN", "HOLDOUT", "SW1", "SW2"]:
        b = means[s][BASELINE]
        for arm in DECOMP_ARMS:
            a = means[s][arm]
            decomposition_rows.append({
                "split": s, "arm": arm,
                "delta_cvar5": float(a["mean_cvar5"] - b["mean_cvar5"]),
                "delta_sharpe_cost_adj": float(a["mean_sharpe_cost_adj"] - b["mean_sharpe_cost_adj"]),
                "mean_veto_rate_arm": float(a["mean_veto_rate"]),
                "mean_veto_rate_baseline": float(b["mean_veto_rate"]),
            })
    pd.DataFrame(decomposition_rows).to_csv(OUT_DIR / "summary_decomposition_r001_r037_entry_veto_us_v1.csv", index=False)

    verdict_payload = {
        "task_id": TASK_ID,
        "criteria_file": str(IN_DECISION_CRITERION.relative_to(ROOT)),
        "dataset_manifest": str(IN_MANIFEST.relative_to(ROOT)),
        "freeze_asof": str(manifest.get("freeze_asof")),
        "tier_verdicts": {label: tier_results[label]["final_verdict"] for _, _, label in TIER_PAIRS},
        "tier_details": tier_results,
        "arms": ARMS,
        "decomposition_arms_no_tier": DECOMP_ARMS,
        "decomposition_summary": decomposition_rows,
        "thresholds": {
            "max_veto_rate": float(decision_criterion.get("max_veto_rate", 0.35)),
            "materiality_sharpe_abs_min": 0.30,
            "materiality_cvar5_abs_min": 0.02,
            "bootstrap_mass_min_pct": 90.0,
        },
    }
    with (OUT_DIR / "verdict_r001_r037_entry_veto_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(verdict_payload, fp, ensure_ascii=False, indent=2, default=lambda o: None if isinstance(o, float) and not np.isfinite(o) else o)

    bootstrap_payload = {label: tier_results[label]["bootstrap"] for _, _, label in TIER_PAIRS}
    with (OUT_DIR / "bootstrap_stats_r001_r037_entry_veto_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(bootstrap_payload, fp, ensure_ascii=False, indent=2)

    phase_stats = {
        "meta": {
            "task_id": TASK_ID, "cadence": cadence, "top_n": top_n, "min_market_cap": min_market_cap,
            "anchor_date_effective": anchor_date.date().isoformat(), "arms": ARMS,
            "tier_pairs": [{"baseline": b, "arm": a, "label": l} for b, a, l in TIER_PAIRS],
        },
        "decision_criterion": decision_criterion,
        "means_by_split_arm": means,
        "tier_verdicts": {label: tier_results[label]["final_verdict"] for _, _, label in TIER_PAIRS},
    }
    with (OUT_DIR / "phase_sweep_stats_r001_r037_entry_veto_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_stats, fp, ensure_ascii=False, indent=2)

    print(f"{TASK_ID} concluido.")
    for _, _, label in TIER_PAIRS:
        print(f"tier_verdict[{label}]={tier_results[label]['final_verdict']}")


if __name__ == "__main__":
    main()
