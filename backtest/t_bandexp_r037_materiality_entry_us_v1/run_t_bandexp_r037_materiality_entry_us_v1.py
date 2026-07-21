"""Backtest T-SDC-BANDEXP-R037-MATERIALITY-ENTRY-US-V1.

Estudo read-only pre-registrado:
- Baseline_C4puro: C4 puro sem veto adicional SPC/R-037.
- Arm_Dupla: veto de entrada apenas na intersecao Flag_BandExp e R037_recon.
- Arm_Dupla_Ret62Puro: sensibilidade descritiva (sem tier R-048).
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import backtest.t_band_exp_entry_us_v1.run_t_band_exp_entry_us_v1 as prev  # noqa: E402
from lib.engine import select_top_n  # noqa: E402

TASK_ID = "T-SDC-BANDEXP-R037-MATERIALITY-ENTRY-US-V1"
ARMS = ["Baseline_C4puro", "Arm_Dupla", "Arm_Dupla_Ret62Puro"]
PRIMARY_BASELINE = "Baseline_C4puro"
PRIMARY_ARM = "Arm_Dupla"
SENS_ARM = "Arm_Dupla_Ret62Puro"

DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2"
IN_CANONICAL = DATASET_DIR / "canonical_us.parquet"
IN_SCORES = DATASET_DIR / "scores_m3_us.parquet"
IN_MACRO = DATASET_DIR / "macro_us.parquet"
IN_MANIFEST = DATASET_DIR / "manifest.json"
IN_BLACKLIST = ROOT / "data" / "ssot" / "blacklist_us.json"
IN_WINNER = ROOT / "config" / "winner_us.json"
IN_DECISION_CRITERION = (
    ROOT
    / "backtest"
    / "t_bandexp_r037_materiality_entry_us_v1"
    / "decision_criterion_bandexp_r037_materiality_entry_us_v1.json"
)
OUT_DIR = ROOT / "backtest" / "t_bandexp_r037_materiality_entry_us_v1" / "results"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _verify_manifest_hashes(manifest: dict[str, Any], dataset_dir: Path) -> None:
    file_specs = manifest.get("files", {})
    required = {
        "canonical_us.parquet": dataset_dir / "canonical_us.parquet",
        "scores_m3_us.parquet": dataset_dir / "scores_m3_us.parquet",
        "macro_us.parquet": dataset_dir / "macro_us.parquet",
    }
    for fname, fpath in required.items():
        if not fpath.exists():
            raise RuntimeError(f"Arquivo esperado ausente no freeze: {fpath}")
        expected = str(file_specs.get(fname, {}).get("sha256", "")).strip().lower()
        if not expected:
            raise RuntimeError(f"manifest sem sha256 para {fname}")
        got = _sha256_file(fpath).lower()
        if got != expected:
            raise RuntimeError(
                f"SHA256 mismatch em {fname}: esperado={expected} obtido={got}"
            )


def _build_persist_top20_by_day(
    canonical: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    mc_eligible_by_day: dict[pd.Timestamp, set[str]],
    top_n: int = 20,
) -> tuple[pd.DataFrame, dict[tuple[pd.Timestamp, str], float]]:
    base = canonical[["date", "ticker"]].dropna().drop_duplicates().copy()
    base["date"] = pd.to_datetime(base["date"], errors="coerce").dt.normalize()
    base["ticker"] = base["ticker"].astype(str).str.upper().str.strip()
    base = base.dropna(subset=["date", "ticker"]).copy()
    base["in_top20"] = 0

    top_hits: list[dict[str, Any]] = []
    for d_prev, scores_day in scores_by_day.items():
        d_norm = pd.Timestamp(d_prev).normalize()
        if scores_day is None or scores_day.empty:
            continue
        eligible = mc_eligible_by_day.get(d_norm, set())
        if not eligible:
            continue
        scored = scores_day[scores_day.index.isin(eligible)]
        if scored.empty:
            continue
        selected = select_top_n(scored, top_n=top_n, blacklist=None)
        if not selected:
            continue
        for tk in selected:
            top_hits.append({"date": d_norm, "ticker": str(tk).upper().strip(), "in_top20_mark": 1})

    if top_hits:
        marks = pd.DataFrame(top_hits).drop_duplicates(subset=["date", "ticker"])
        base = base.merge(marks, on=["date", "ticker"], how="left")
        base["in_top20"] = pd.to_numeric(base["in_top20_mark"], errors="coerce").fillna(0).astype(int)
        base = base.drop(columns=["in_top20_mark"])

    base = base.sort_values(["ticker", "date"]).reset_index(drop=True)
    base["persist_top20"] = (
        base.groupby("ticker", sort=False)["in_top20"]
        .transform(lambda s: s.rolling(10, min_periods=10).sum())
    )

    persist_lookup: dict[tuple[pd.Timestamp, str], float] = {}
    for r in base.itertuples():
        if prev._is_finite(getattr(r, "persist_top20")):
            persist_lookup[(pd.Timestamp(r.date).normalize(), str(r.ticker).upper().strip())] = float(r.persist_top20)
    return base, persist_lookup


def _build_r037_flags_by_day(
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    flags_df: pd.DataFrame,
    persist_lookup: dict[tuple[pd.Timestamp, str], float],
) -> tuple[dict[pd.Timestamp, set[str]], dict[pd.Timestamp, set[str]]]:
    work = flags_df[["date", "ticker", "w_ter"]].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["ticker"] = work["ticker"].astype(str).str.upper().str.strip()
    wter_map = {
        (pd.Timestamp(r.date).normalize(), str(r.ticker).upper().strip()): str(r.w_ter)
        for r in work.itertuples()
    }

    r037_recon_by_day: dict[pd.Timestamp, set[str]] = {}
    r037_live_pure_by_day: dict[pd.Timestamp, set[str]] = {}

    for d_prev, scores_day in scores_by_day.items():
        d_norm = pd.Timestamp(d_prev).normalize()
        if scores_day is None or scores_day.empty:
            r037_recon_by_day[d_norm] = set()
            r037_live_pure_by_day[d_norm] = set()
            continue

        day = scores_day.copy()
        day["ticker"] = day.index.astype(str).str.upper().str.strip()
        day["ret_62"] = pd.to_numeric(day["ret_62"], errors="coerce")
        day["w_ter"] = day["ticker"].map(lambda tk: wter_map.get((d_norm, tk), ""))
        day["persist_top20"] = day["ticker"].map(lambda tk: persist_lookup.get((d_norm, tk), float("nan")))

        live = set(day.loc[day["ret_62"] >= 1.00, "ticker"].astype(str).tolist())
        recon_mask = (
            (day["ret_62"] >= 1.00)
            & (day["w_ter"] == "LARGA")
            & (pd.to_numeric(day["persist_top20"], errors="coerce") <= 2.0)
        )
        recon = set(day.loc[recon_mask, "ticker"].astype(str).tolist())

        r037_live_pure_by_day[d_norm] = live
        r037_recon_by_day[d_norm] = recon

    return r037_recon_by_day, r037_live_pure_by_day


def _arm_gate_set(
    arm_name: str,
    d_prev: pd.Timestamp,
    gate_by_day_by_arm: dict[str, dict[pd.Timestamp, set[str]]],
) -> set[str]:
    if arm_name == PRIMARY_BASELINE:
        return set()
    return set(gate_by_day_by_arm.get(arm_name, {}).get(d_prev, set()))


def _run_phase_arm(
    arm_name: str,
    phase: int,
    rebalance_days: list[pd.Timestamp],
    trading_days: list[pd.Timestamp],
    day_to_idx: dict[pd.Timestamp, int],
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    blacklist: set[str],
    top_n: int,
    px_wide: pd.DataFrame,
    mc_eligible_by_day: dict[pd.Timestamp, set[str]],
    flagged_by_day: dict[pd.Timestamp, set[str]],
    gate_by_day_by_arm: dict[str, dict[pd.Timestamp, set[str]]],
    spc_blocked_by_day: dict[pd.Timestamp, set[str]],
    r037_live_pure_by_day: dict[pd.Timestamp, set[str]],
    holdout_end: pd.Timestamp,
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []

    for reb_idx, d_reb in enumerate(rebalance_days):
        if reb_idx < prev.SKIP_INITIAL_REBALANCES:
            continue

        d_prev = prev._prev_day(d_reb, day_to_idx, trading_days)
        if d_prev is None:
            continue

        d_next_reb = rebalance_days[reb_idx + 1] if reb_idx + 1 < len(rebalance_days) else None
        d_prev_next = prev._prev_day(d_next_reb, day_to_idx, trading_days)
        if d_next_reb is None or d_prev_next is None:
            continue

        split = prev._to_split(d_reb, holdout_end=holdout_end)
        if split == "OTHER":
            continue
        is_holdout = bool(split in {"HOLDOUT", "SW1", "SW2"})

        prev_scores = scores_by_day.get(d_prev)
        if prev_scores is None or prev_scores.empty:
            continue

        eligible = mc_eligible_by_day.get(d_prev, set())
        prev_scores = prev_scores[prev_scores.index.isin(eligible)]
        if prev_scores.empty:
            continue

        baseline_selected = select_top_n(prev_scores, top_n=top_n, blacklist=blacklist)
        if not baseline_selected:
            continue

        blocked_total = _arm_gate_set(
            arm_name=arm_name,
            d_prev=d_prev,
            gate_by_day_by_arm=gate_by_day_by_arm,
        )
        arm_gate_blacklist = set(blacklist) | blocked_total
        arm_selected = select_top_n(prev_scores, top_n=top_n, blacklist=arm_gate_blacklist)
        if not arm_selected:
            continue

        blocked = sorted([t for t in baseline_selected if t in blocked_total])
        substitutes = sorted(set(arm_selected) - set(baseline_selected))
        n_veto = int(len(blocked))
        veto_rate = float(n_veto / top_n) if top_n > 0 else float("nan")

        substitute_ranks: list[int] = []
        for tk in substitutes:
            if tk not in prev_scores.index:
                continue
            rk = pd.to_numeric(prev_scores.at[tk, "m3_rank"], errors="coerce")
            if prev._is_finite(rk):
                substitute_ranks.append(int(rk))
        m3_rank_substitutos = ";".join(str(x) for x in substitute_ranks)
        m3_rank_max_substituto = int(max(substitute_ranks)) if substitute_ranks else float("nan")

        spc_set = set(spc_blocked_by_day.get(d_prev, set()))
        ret62_set = set(r037_live_pure_by_day.get(d_prev, set()))
        vetados_r001 = sorted([t for t in blocked if t in spc_set])
        vetados_ret62 = sorted([t for t in blocked if t in ret62_set])

        idx_start = day_to_idx.get(d_prev)
        idx_end = day_to_idx.get(d_prev_next)
        holding_days = int(idx_end - idx_start) if idx_start is not None and idx_end is not None else 0
        if holding_days <= 0:
            continue

        log_ret_baseline = prev._basket_log_return(px_wide, d_prev, d_prev_next, baseline_selected)
        log_ret_arm = prev._basket_log_return(px_wide, d_prev, d_prev_next, arm_selected)
        cost_arm = (
            float(n_veto * 2 * prev.FRICTION_ONE_WAY_RATE / top_n) if top_n > 0 else 0.0
        )
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
                "n_bandexp_blocked_pool": int(len(flagged_by_day.get(d_prev, set()))),
                "tickers_baseline": ";".join(baseline_selected),
                "tickers_arm": ";".join(arm_selected),
                "tickers_vetados": ";".join(blocked),
                "tickers_substitutos": ";".join(substitutes),
                "m3_rank_substitutos": m3_rank_substitutos,
                "m3_rank_max_substituto": m3_rank_max_substituto,
                "vetados_com_r001_ativo": ";".join(vetados_r001),
                "vetados_com_ret62_puro": ";".join(vetados_ret62),
                "log_ret_baseline": log_ret_baseline,
                "log_ret_arm": log_ret_arm,
                "log_ret_arm_cost_adj": log_ret_arm_cost_adj,
                "cost_arm": cost_arm,
            }
        )

    return observations


def _bootstrap_primary_pair(
    obs_df: pd.DataFrame,
    subset: str,
    n_resamples: int,
    seed: int,
) -> dict[str, Any]:
    pair = obs_df[obs_df["arm"].isin([PRIMARY_BASELINE, PRIMARY_ARM])].copy()
    pair["arm"] = pair["arm"].map(
        {
            PRIMARY_BASELINE: "Baseline",
            PRIMARY_ARM: "Arm_BandExp",
        }
    )
    return prev._bootstrap_metric_stats(
        pair,
        subset=subset,
        n_resamples=n_resamples,
        seed=seed,
    )


def _sensitivity_summary_rows(
    train_summary: pd.DataFrame,
    holdout_summary: pd.DataFrame,
    sw1_summary: pd.DataFrame,
    sw2_summary: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_name = {
        "TRAIN": train_summary,
        "HOLDOUT": holdout_summary,
        "SW1": sw1_summary,
        "SW2": sw2_summary,
    }
    for split, summary_df in by_name.items():
        b = prev._subset_means(summary_df, PRIMARY_BASELINE)
        s = prev._subset_means(summary_df, SENS_ARM)
        rows.append(
            {
                "split": split,
                "delta_cvar5": float(s["mean_cvar5"] - b["mean_cvar5"]),
                "delta_sharpe_cost_adj": float(
                    s["mean_sharpe_cost_adj"] - b["mean_sharpe_cost_adj"]
                ),
                "mean_veto_rate_sens": float(s["mean_veto_rate"]),
                "mean_veto_rate_baseline": float(b["mean_veto_rate"]),
            }
        )
    return rows


def _split_to_dict(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        out[str(row["split"])] = {
            "delta_cvar5": row["delta_cvar5"],
            "delta_sharpe_cost_adj": row["delta_sharpe_cost_adj"],
            "mean_veto_rate_sens": row["mean_veto_rate_sens"],
            "mean_veto_rate_baseline": row["mean_veto_rate_baseline"],
        }
    return out


def main() -> None:
    if not IN_DECISION_CRITERION.exists():
        raise RuntimeError(f"Criterio pre-registrado nao encontrado: {IN_DECISION_CRITERION}")
    with IN_DECISION_CRITERION.open("r", encoding="utf-8") as fp:
        decision_criterion = json.load(fp)

    holdout_end, manifest = prev._load_holdout_end_from_manifest(IN_MANIFEST)
    if bool(decision_criterion.get("dataset", {}).get("required_hash_verification", False)):
        _verify_manifest_hashes(manifest, DATASET_DIR)
        print("Hashes conferidos com sucesso contra manifest.json.")

    winner_cfg = prev._load_winner_snapshot(IN_WINNER)
    top_n = int(winner_cfg["top_n"])
    cadence = int(winner_cfg["rebalance_cadence"])
    anchor_date = pd.Timestamp(winner_cfg["rebalance_anchor_date"]).normalize()
    min_market_cap = float(winner_cfg["min_market_cap"])

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
    print(f"Tickers excluidos por blacklist: {len(blacklist)}")

    spc_blocked_by_day = prev._build_spc_blocked_by_day(canonical)
    print(f"SPC blocked map carregado para {len(spc_blocked_by_day)} pregoes.")

    flagged_by_day, flags_df = prev._build_bandexp_flag_by_day(
        canonical,
        spc_blocked_by_day=spc_blocked_by_day,
    )
    print(f"BandExp flag map carregado para {len(flagged_by_day)} pregoes.")
    print(
        "BandExp stats: "
        f"rows={len(flags_df)} "
        f"flags_total={int(flags_df['flag_bandexp'].sum())} "
        f"flags_ratio={float(flags_df['flag_bandexp'].mean()):.4f}"
    )

    canonical["market_cap"] = pd.to_numeric(canonical["market_cap"], errors="coerce")
    mc_eligible_by_day: dict[pd.Timestamp, set[str]] = {}
    for _dt, _grp in canonical.groupby("date"):
        _eligible = set(_grp.loc[_grp["market_cap"] >= min_market_cap, "ticker"].dropna())
        mc_eligible_by_day[_dt] = _eligible
    print(
        "Market_cap filter: "
        f"{len(mc_eligible_by_day)} dias mapeados "
        f"(min_market_cap={min_market_cap:,.0f})."
    )

    px_wide = (
        canonical.pivot_table(
            index="date",
            columns="ticker",
            values="close_operational",
            aggfunc="first",
        )
        .sort_index()
        .ffill()
    )
    trading_days = list(px_wide.index)
    day_to_idx = {d: i for i, d in enumerate(trading_days)}
    if not trading_days:
        raise RuntimeError("Nenhum pregao encontrado no canonical filtrado.")

    anchor_idx = day_to_idx.get(anchor_date)
    if anchor_idx is None:
        pos = int(
            np.searchsorted(
                np.array(trading_days, dtype="datetime64[ns]"),
                np.datetime64(anchor_date),
            )
        )
        if pos >= len(trading_days):
            anchor_idx = 0
            anchor_date = trading_days[0]
            print(
                "Aviso: rebalance_anchor_date do winner esta apos o periodo analisado; "
                f"usando primeira data do SSOT ({anchor_date.date().isoformat()})."
            )
        else:
            anchor_idx = pos
            anchor_date = trading_days[anchor_idx]

    min_rebalances_needed = prev.SKIP_INITIAL_REBALANCES + 12
    remaining_days = len(trading_days) - anchor_idx
    if remaining_days // max(cadence, 1) < min_rebalances_needed:
        anchor_idx = 0
        anchor_date = trading_days[0]
        print(
            "Aviso: ancora do winner era curta para estudo historico; "
            f"usando primeira data do SSOT ({anchor_date.date().isoformat()})."
        )

    scores_by_day = prev._compute_scores_by_day(px_wide, holdout_end=holdout_end)
    print(f"Scores computados para {len(scores_by_day)} pregoes.")

    persist_df, persist_lookup = _build_persist_top20_by_day(
        canonical=canonical,
        scores_by_day=scores_by_day,
        mc_eligible_by_day=mc_eligible_by_day,
        top_n=top_n,
    )
    print(f"Persist_top20 calculada para {len(persist_df)} linhas ticker-dia.")

    r037_recon_by_day, r037_live_pure_by_day = _build_r037_flags_by_day(
        scores_by_day=scores_by_day,
        flags_df=flags_df,
        persist_lookup=persist_lookup,
    )

    all_days = sorted(set(scores_by_day.keys()) | set(flagged_by_day.keys()))
    gate_dupla_by_day = {
        d: set(flagged_by_day.get(d, set())) & set(r037_recon_by_day.get(d, set()))
        for d in all_days
    }
    gate_ret62_by_day = {
        d: set(flagged_by_day.get(d, set())) & set(r037_live_pure_by_day.get(d, set()))
        for d in all_days
    }
    gate_by_day_by_arm: dict[str, dict[pd.Timestamp, set[str]]] = {
        PRIMARY_BASELINE: {},
        PRIMARY_ARM: gate_dupla_by_day,
        SENS_ARM: gate_ret62_by_day,
    }
    mean_gate_dupla = float(np.mean([len(v) for v in gate_dupla_by_day.values()])) if gate_dupla_by_day else 0.0
    mean_gate_ret62 = float(np.mean([len(v) for v in gate_ret62_by_day.values()])) if gate_ret62_by_day else 0.0
    print(
        "Gate stats por dia: "
        f"dupla_mean={mean_gate_dupla:.2f} "
        f"ret62_mean={mean_gate_ret62:.2f}"
    )

    observations: list[dict[str, Any]] = []
    for arm_name in ARMS:
        print(f"Executando arm {arm_name}...")
        for phase in range(cadence):
            rebalance_days = prev._phase_rebalance_days(
                trading_days=trading_days,
                anchor_idx=anchor_idx,
                cadence=cadence,
                phase=phase,
            )
            phase_obs = _run_phase_arm(
                arm_name=arm_name,
                phase=phase,
                rebalance_days=rebalance_days,
                trading_days=trading_days,
                day_to_idx=day_to_idx,
                scores_by_day=scores_by_day,
                blacklist=blacklist,
                top_n=top_n,
                px_wide=px_wide,
                mc_eligible_by_day=mc_eligible_by_day,
                flagged_by_day=flagged_by_day,
                gate_by_day_by_arm=gate_by_day_by_arm,
                spc_blocked_by_day=spc_blocked_by_day,
                r037_live_pure_by_day=r037_live_pure_by_day,
                holdout_end=holdout_end,
            )
            observations.extend(phase_obs)

    obs_df = pd.DataFrame(observations)
    if obs_df.empty:
        raise RuntimeError("Nenhuma observacao gerada. Verifique dados de entrada e filtros.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    obs_df = obs_df.sort_values(["arm", "phase", "date"]).reset_index(drop=True)
    obs_df.to_csv(OUT_DIR / "observations_bandexp_r037_materiality_entry_us_v1.csv", index=False)

    prev.ARMS = ARMS
    train_summary = prev._summarize_subset(obs_df, subset="TRAIN", cadence=cadence)
    holdout_summary = prev._summarize_subset(obs_df, subset="HOLDOUT", cadence=cadence)
    sw1_summary = prev._summarize_subset(obs_df, subset="SW1", cadence=cadence)
    sw2_summary = prev._summarize_subset(obs_df, subset="SW2", cadence=cadence)

    train_summary.to_csv(OUT_DIR / "summary_TRAIN_bandexp_r037_materiality_entry_us_v1.csv", index=False)
    holdout_summary.to_csv(OUT_DIR / "summary_HOLDOUT_bandexp_r037_materiality_entry_us_v1.csv", index=False)
    sw1_summary.to_csv(OUT_DIR / "summary_SW1_bandexp_r037_materiality_entry_us_v1.csv", index=False)
    sw2_summary.to_csv(OUT_DIR / "summary_SW2_bandexp_r037_materiality_entry_us_v1.csv", index=False)

    holdout_means_by_arm = {arm: prev._subset_means(holdout_summary, arm) for arm in ARMS}
    sw1_means_by_arm = {arm: prev._subset_means(sw1_summary, arm) for arm in ARMS}
    sw2_means_by_arm = {arm: prev._subset_means(sw2_summary, arm) for arm in ARMS}

    baseline_holdout = holdout_means_by_arm[PRIMARY_BASELINE]
    arm_holdout = holdout_means_by_arm[PRIMARY_ARM]
    baseline_sw1 = sw1_means_by_arm[PRIMARY_BASELINE]
    arm_sw1 = sw1_means_by_arm[PRIMARY_ARM]
    baseline_sw2 = sw2_means_by_arm[PRIMARY_BASELINE]
    arm_sw2 = sw2_means_by_arm[PRIMARY_ARM]

    delta_holdout_cvar5 = float(arm_holdout["mean_cvar5"] - baseline_holdout["mean_cvar5"])
    delta_holdout_sharpe = float(arm_holdout["mean_sharpe_cost_adj"] - baseline_holdout["mean_sharpe_cost_adj"])
    delta_sw1_cvar5 = float(arm_sw1["mean_cvar5"] - baseline_sw1["mean_cvar5"])
    delta_sw1_sharpe = float(arm_sw1["mean_sharpe_cost_adj"] - baseline_sw1["mean_sharpe_cost_adj"])
    delta_sw2_cvar5 = float(arm_sw2["mean_cvar5"] - baseline_sw2["mean_cvar5"])
    delta_sw2_sharpe = float(arm_sw2["mean_sharpe_cost_adj"] - baseline_sw2["mean_sharpe_cost_adj"])

    bcfg = decision_criterion.get("bootstrap", {})
    n_resamples = int(bcfg.get("n_resamples", 2000))
    seed = int(bcfg.get("seed", 42))
    bs_holdout = _bootstrap_primary_pair(obs_df, subset="HOLDOUT", n_resamples=n_resamples, seed=seed)
    bs_sw1 = _bootstrap_primary_pair(obs_df, subset="SW1", n_resamples=n_resamples, seed=seed)
    bs_sw2 = _bootstrap_primary_pair(obs_df, subset="SW2", n_resamples=n_resamples, seed=seed)

    bootstrap_payload = {
        "task_id": TASK_ID,
        "pair_for_tier": {
            "baseline": PRIMARY_BASELINE,
            "arm": PRIMARY_ARM,
        },
        "method": "cluster por dia de rebalance",
        "n_resamples": n_resamples,
        "seed": seed,
        "splits": {
            "HOLDOUT": bs_holdout,
            "SW1": bs_sw1,
            "SW2": bs_sw2,
        },
    }
    with (OUT_DIR / "bootstrap_stats_bandexp_r037_materiality_entry_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(bootstrap_payload, fp, ensure_ascii=False, indent=2)

    max_veto_rate = float(decision_criterion.get("max_veto_rate", 0.35))
    veto_holdout_ok = bool(prev._is_finite(arm_holdout["mean_veto_rate"]) and arm_holdout["mean_veto_rate"] <= max_veto_rate)
    veto_sw1_ok = bool(prev._is_finite(arm_sw1["mean_veto_rate"]) and arm_sw1["mean_veto_rate"] <= max_veto_rate)
    veto_sw2_ok = bool(prev._is_finite(arm_sw2["mean_veto_rate"]) and arm_sw2["mean_veto_rate"] <= max_veto_rate)

    domina_forte = bool(
        prev._favorable_ic(bs_holdout["delta_cvar5"]["ic95"])
        and prev._favorable_ic(bs_holdout["delta_sharpe_cost_adj"]["ic95"])
        and prev._favorable_ic(bs_sw1["delta_cvar5"]["ic95"])
        and prev._favorable_ic(bs_sw1["delta_sharpe_cost_adj"]["ic95"])
        and prev._favorable_ic(bs_sw2["delta_cvar5"]["ic95"])
        and prev._favorable_ic(bs_sw2["delta_sharpe_cost_adj"]["ic95"])
        and veto_holdout_ok
        and veto_sw1_ok
        and veto_sw2_ok
    )
    holdout_mass_ok = bool(
        prev._is_finite(bs_holdout["delta_cvar5"]["mass_same_sign_pct"])
        and prev._is_finite(bs_holdout["delta_sharpe_cost_adj"]["mass_same_sign_pct"])
        and bs_holdout["delta_cvar5"]["mass_same_sign_pct"] >= 90.0
        and bs_holdout["delta_sharpe_cost_adj"]["mass_same_sign_pct"] >= 90.0
    )
    direction_ok = bool(
        prev._delta_favorable(delta_holdout_cvar5)
        and prev._delta_favorable(delta_holdout_sharpe)
        and prev._delta_favorable(delta_sw1_cvar5)
        and prev._delta_favorable(delta_sw1_sharpe)
        and prev._delta_favorable(delta_sw2_cvar5)
        and prev._delta_favorable(delta_sw2_sharpe)
    )
    materiality_ok = bool((abs(delta_holdout_sharpe) >= 0.30) or (abs(delta_holdout_cvar5) >= 0.02))
    favorecido_dupla = bool(
        (not domina_forte)
        and holdout_mass_ok
        and direction_ok
        and materiality_ok
        and veto_holdout_ok
    )

    if domina_forte:
        final_verdict = "DOMINA_FORTE"
    elif favorecido_dupla:
        final_verdict = "FAVORECIDO_DUPLA"
    else:
        final_verdict = "INCONCLUSIVO"

    sens_rows = _sensitivity_summary_rows(
        train_summary=train_summary,
        holdout_summary=holdout_summary,
        sw1_summary=sw1_summary,
        sw2_summary=sw2_summary,
    )
    pd.DataFrame(sens_rows).to_csv(
        OUT_DIR / "summary_sensitivity_ret62puro_bandexp_r037_materiality_entry_us_v1.csv",
        index=False,
    )

    checks = {
        "domina_forte_conditions": {
            "holdout_ic_cvar5_favoravel": prev._favorable_ic(bs_holdout["delta_cvar5"]["ic95"]),
            "holdout_ic_sharpe_favoravel": prev._favorable_ic(bs_holdout["delta_sharpe_cost_adj"]["ic95"]),
            "sw1_ic_cvar5_favoravel": prev._favorable_ic(bs_sw1["delta_cvar5"]["ic95"]),
            "sw1_ic_sharpe_favoravel": prev._favorable_ic(bs_sw1["delta_sharpe_cost_adj"]["ic95"]),
            "sw2_ic_cvar5_favoravel": prev._favorable_ic(bs_sw2["delta_cvar5"]["ic95"]),
            "sw2_ic_sharpe_favoravel": prev._favorable_ic(bs_sw2["delta_sharpe_cost_adj"]["ic95"]),
            "veto_holdout_ok": veto_holdout_ok,
            "veto_sw1_ok": veto_sw1_ok,
            "veto_sw2_ok": veto_sw2_ok,
        },
        "favorecido_conditions": {
            "holdout_mass_ok": holdout_mass_ok,
            "direction_ok_holdout_sw1_sw2": direction_ok,
            "materiality_ok": materiality_ok,
            "veto_holdout_ok": veto_holdout_ok,
        },
    }

    verdict_payload = {
        "task_id": TASK_ID,
        "criteria_file": str(IN_DECISION_CRITERION.relative_to(ROOT)),
        "dataset_manifest": str(IN_MANIFEST.relative_to(ROOT)),
        "freeze_asof": str(manifest.get("freeze_asof")),
        "final_verdict": final_verdict,
        "tier_pair": {
            "baseline": PRIMARY_BASELINE,
            "arm": PRIMARY_ARM,
        },
        "arms": ARMS,
        "deltas_primary_pair": {
            "HOLDOUT": {
                "delta_cvar5": delta_holdout_cvar5,
                "delta_sharpe_cost_adj": delta_holdout_sharpe,
                "mean_veto_rate_arm": arm_holdout["mean_veto_rate"],
            },
            "SW1": {
                "delta_cvar5": delta_sw1_cvar5,
                "delta_sharpe_cost_adj": delta_sw1_sharpe,
                "mean_veto_rate_arm": arm_sw1["mean_veto_rate"],
            },
            "SW2": {
                "delta_cvar5": delta_sw2_cvar5,
                "delta_sharpe_cost_adj": delta_sw2_sharpe,
                "mean_veto_rate_arm": arm_sw2["mean_veto_rate"],
            },
        },
        "bootstrap_primary_pair": {
            "HOLDOUT": bs_holdout,
            "SW1": bs_sw1,
            "SW2": bs_sw2,
        },
        "sensitivity_ret62_puro": _split_to_dict(sens_rows),
        "checks": checks,
        "thresholds": {
            "max_veto_rate": max_veto_rate,
            "materiality_sharpe_abs_min": 0.30,
            "materiality_cvar5_abs_min": 0.02,
            "bootstrap_mass_min_pct": 90.0,
        },
    }
    with (OUT_DIR / "verdict_bandexp_r037_materiality_entry_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(verdict_payload, fp, ensure_ascii=False, indent=2)

    phase_stats = {
        "meta": {
            "task_id": TASK_ID,
            "cadence": cadence,
            "top_n": top_n,
            "min_market_cap": min_market_cap,
            "friction_one_way_rate": prev.FRICTION_ONE_WAY_RATE,
            "anchor_date_effective": anchor_date.date().isoformat(),
            "holdout_start": prev.HOLDOUT_START.date().isoformat(),
            "holdout_end": holdout_end.date().isoformat(),
            "subwindows": {
                "SW1": {"start": prev.SW1_START.date().isoformat(), "end": prev.SW1_END.date().isoformat()},
                "SW2": {"start": prev.SW2_START.date().isoformat(), "end": holdout_end.date().isoformat()},
            },
            "arms": ARMS,
            "tier_pair": [PRIMARY_BASELINE, PRIMARY_ARM],
            "notes": "Materialidade da dupla BandExp+R-037_recon no Top-20 de entrada.",
        },
        "decision_criterion": decision_criterion,
        "holdout_means_by_arm": holdout_means_by_arm,
        "subwindow_means_by_arm": {
            "SW1": sw1_means_by_arm,
            "SW2": sw2_means_by_arm,
        },
        "sensitivity_summary": sens_rows,
        "final_verdict": final_verdict,
        "by_phase_train": train_summary.to_dict(orient="records"),
        "by_phase_holdout": holdout_summary.to_dict(orient="records"),
        "by_phase_sw1": sw1_summary.to_dict(orient="records"),
        "by_phase_sw2": sw2_summary.to_dict(orient="records"),
    }
    with (OUT_DIR / "phase_sweep_stats_bandexp_r037_materiality_entry_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_stats, fp, ensure_ascii=False, indent=2)

    print(f"{TASK_ID} concluido.")
    print(f"freeze_asof={manifest.get('freeze_asof')}")
    print(f"observations_total={len(obs_df)}")
    print(f"observations_train={int((obs_df['split'] == 'TRAIN').sum())}")
    print(f"observations_holdout={int((obs_df['is_holdout'] == 1).sum())}")
    print(f"rows_train_summary={len(train_summary)}")
    print(f"rows_holdout_summary={len(holdout_summary)}")
    print(f"rows_sw1_summary={len(sw1_summary)}")
    print(f"rows_sw2_summary={len(sw2_summary)}")
    print(f"delta_holdout_cvar5_primary={delta_holdout_cvar5:+.6f}")
    print(f"delta_holdout_sharpe_primary={delta_holdout_sharpe:+.6f}")
    print(f"final_verdict={final_verdict}")


if __name__ == "__main__":
    main()
