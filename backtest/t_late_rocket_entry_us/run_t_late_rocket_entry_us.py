"""Backtest T-LATE-ROCKET-ENTRY-US-V1: ablation de gate de entrada por ret_62.

Estudo isolado, sem alterar motor produtivo.
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

from lib.engine import compute_m3_scores, select_top_n  # noqa: E402

IN_CANONICAL = ROOT / "data" / "ssot" / "canonical_us.parquet"
IN_BLACKLIST = ROOT / "data" / "ssot" / "blacklist_us.json"
IN_WINNER = ROOT / "config" / "winner_us.json"
IN_DECISION_CRITERION = ROOT / "backtest" / "t_late_rocket_entry_us" / "decision_criterion_late_rocket_us.json"

OUT_DIR = ROOT / "backtest" / "t_late_rocket_entry_us" / "results"

FRICTION_ONE_WAY_RATE = 0.000250  # 2.5 bps one-way
RET62_THRESHOLDS = [0.80, 0.90, 1.00, 1.10, 1.20, 1.30]
ARMS = ["Baseline"] + [f"Arm_A_{thr:.2f}" for thr in RET62_THRESHOLDS]

TRAIN_END = pd.Timestamp("2022-12-30")
HOLDOUT_START = pd.Timestamp("2023-01-02")
HOLDOUT_END = pd.Timestamp("2026-03-16")
SKIP_INITIAL_REBALANCES = 20
MAX_VETO_RATE = 0.35


def _is_finite(v: Any) -> bool:
    return bool(np.isfinite(v))


def _safe_float(v: Any, default: float = float("nan")) -> float:
    try:
        out = float(v)
    except Exception:
        return default
    if not np.isfinite(out):
        return default
    return out


def _load_blacklist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if isinstance(data, list):
        return {str(x).upper().strip() for x in data}
    result: set[str] = set()
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, list):
                result.update(str(x).upper().strip() for x in value)
    return result


def _load_winner_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"winner_us.json nao encontrado: {path}")
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    snap = data.get("winner_config_snapshot", data)
    return {
        "top_n": int(snap.get("top_n", 20)),
        "rebalance_cadence": int(snap.get("rebalance_cadence", 10)),
        "rebalance_anchor_date": str(snap.get("rebalance_anchor_date", "2026-04-16")),
        "min_market_cap": float(snap.get("min_market_cap", 300_000_000.0)),
    }


def _to_split(day: pd.Timestamp) -> str:
    if day <= TRAIN_END:
        return "TRAIN"
    if HOLDOUT_START <= day <= HOLDOUT_END:
        return "HOLDOUT"
    return "OTHER"


def _prev_day(
    day: pd.Timestamp | None,
    day_to_idx: dict[pd.Timestamp, int],
    trading_days: list[pd.Timestamp],
) -> pd.Timestamp | None:
    if day is None:
        return None
    idx = day_to_idx.get(day)
    if idx is None or idx <= 0:
        return None
    return trading_days[idx - 1]


def _phase_rebalance_days(
    trading_days: list[pd.Timestamp],
    anchor_idx: int,
    cadence: int,
    phase: int,
) -> list[pd.Timestamp]:
    out: list[pd.Timestamp] = []
    for idx, day in enumerate(trading_days):
        if idx < anchor_idx:
            continue
        idx_from_anchor = idx - anchor_idx
        if (idx_from_anchor % cadence) == (phase % cadence):
            out.append(day)
    return out


def _basket_log_return(
    px_wide: pd.DataFrame,
    start_day: pd.Timestamp,
    end_day: pd.Timestamp,
    tickers: list[str],
) -> float:
    if not tickers:
        return float("nan")
    if start_day not in px_wide.index or end_day not in px_wide.index:
        return float("nan")

    use_cols = [
        str(t).upper().strip()
        for t in tickers
        if str(t).upper().strip() in px_wide.columns
    ]
    if not use_cols:
        return float("nan")

    start_prices = pd.to_numeric(px_wide.loc[start_day, use_cols], errors="coerce")
    end_prices = pd.to_numeric(px_wide.loc[end_day, use_cols], errors="coerce")
    valid = (
        start_prices.notna()
        & end_prices.notna()
        & (start_prices > 0)
        & (end_prices > 0)
    )
    if not valid.any():
        return float("nan")

    rets = np.log(end_prices[valid].values / start_prices[valid].values)
    if len(rets) == 0:
        return float("nan")
    return float(np.mean(rets))


def _portfolio_sharpe(log_returns: pd.Series, holding_days: pd.Series) -> float:
    r = pd.to_numeric(log_returns, errors="coerce").astype(float).to_numpy()
    d = pd.to_numeric(holding_days, errors="coerce").astype(float).to_numpy()
    valid = np.isfinite(r) & np.isfinite(d) & (d > 0)
    if not valid.any():
        return float("nan")
    daily = r[valid] / d[valid]
    if len(daily) < 2:
        return float("nan")
    sd = float(np.std(daily, ddof=0))
    if sd <= 0 or not np.isfinite(sd):
        return float("nan")
    return float((np.mean(daily) / sd) * np.sqrt(252.0))


def _cvar(arr: np.ndarray, level: float) -> float:
    vals = np.asarray(arr, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return float("nan")
    threshold = float(np.nanquantile(vals, level))
    tail = vals[vals <= threshold]
    if tail.size == 0:
        return float("nan")
    return float(np.nanmean(tail))


def _nanmean(values: pd.Series | np.ndarray) -> float:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if arr.size == 0 or np.isnan(arr).all():
        return float("nan")
    return float(np.nanmean(arr))


def _arm_threshold(arm_name: str) -> float | None:
    if arm_name == "Baseline":
        return None
    try:
        return float(arm_name.split("_")[-1])
    except Exception:
        return None


def _run_phase_arm(
    arm_name: str,
    threshold: float | None,
    phase: int,
    rebalance_days: list[pd.Timestamp],
    trading_days: list[pd.Timestamp],
    day_to_idx: dict[pd.Timestamp, int],
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
    blacklist: set[str],
    top_n: int,
    px_wide: pd.DataFrame,
    mc_eligible_by_day: dict[pd.Timestamp, set[str]],
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []

    for reb_idx, d_reb in enumerate(rebalance_days):
        if reb_idx < SKIP_INITIAL_REBALANCES:
            continue

        d_prev = _prev_day(d_reb, day_to_idx, trading_days)
        if d_prev is None:
            continue

        d_next_reb = rebalance_days[reb_idx + 1] if reb_idx + 1 < len(rebalance_days) else None
        d_prev_next = _prev_day(d_next_reb, day_to_idx, trading_days)
        if d_next_reb is None or d_prev_next is None:
            continue

        split = _to_split(d_reb)
        if split == "OTHER":
            continue

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

        if threshold is None:
            blocked_by_gate: set[str] = set()
        else:
            blocked_mask = pd.to_numeric(prev_scores["ret_62"], errors="coerce") >= float(threshold)
            blocked_by_gate = set(
                prev_scores.loc[blocked_mask].index.astype(str).str.upper().str.strip().tolist()
            )

        arm_gate_blacklist = set(blacklist)
        arm_gate_blacklist.update(blocked_by_gate)
        arm_selected = select_top_n(prev_scores, top_n=top_n, blacklist=arm_gate_blacklist)
        if not arm_selected:
            continue

        blocked = sorted([t for t in baseline_selected if t in blocked_by_gate])
        substitutes = sorted(set(arm_selected) - set(baseline_selected))
        n_veto = int(len(blocked))
        veto_rate = float(n_veto / top_n) if top_n > 0 else float("nan")

        idx_start = day_to_idx.get(d_prev)
        idx_end = day_to_idx.get(d_prev_next)
        holding_days = int(idx_end - idx_start) if idx_start is not None and idx_end is not None else 0
        if holding_days <= 0:
            continue

        log_ret_baseline = _basket_log_return(px_wide, d_prev, d_prev_next, baseline_selected)
        log_ret_arm = _basket_log_return(px_wide, d_prev, d_prev_next, arm_selected)
        cost_arm = float(n_veto * 2 * FRICTION_ONE_WAY_RATE / top_n) if top_n > 0 else 0.0
        log_ret_arm_cost_adj = float(log_ret_arm - cost_arm) if _is_finite(log_ret_arm) else float("nan")

        observations.append(
            {
                "arm": arm_name,
                "threshold": threshold,
                "phase": int(phase),
                "date": d_reb.date().isoformat(),
                "d_prev": d_prev.date().isoformat(),
                "d_next_reb": d_next_reb.date().isoformat(),
                "d_prev_next_reb": d_prev_next.date().isoformat(),
                "split": split,
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


def _summarize_by_split(
    obs_df: pd.DataFrame,
    split: str,
    cadence: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    split_df = obs_df[obs_df["split"] == split].copy()

    for arm in ARMS:
        arm_df = split_df[split_df["arm"] == arm].copy()
        threshold = _arm_threshold(arm)
        for phase in range(cadence):
            g = arm_df[arm_df["phase"] == phase].copy()

            lr_baseline = pd.to_numeric(g["log_ret_baseline"], errors="coerce").to_numpy(dtype=float)
            lr_arm_cost_adj = pd.to_numeric(g["log_ret_arm_cost_adj"], errors="coerce").to_numpy(dtype=float)

            rows.append(
                {
                    "arm": arm,
                    "threshold": threshold,
                    "phase": int(phase),
                    "split": split,
                    "n_cycles": int(len(g)),
                    "mean_cvar5": _cvar(lr_arm_cost_adj, 0.05),
                    "mean_sharpe_cost_adj": _portfolio_sharpe(g["log_ret_arm_cost_adj"], g["holding_days"]),
                    "mean_veto_rate": _nanmean(g["veto_rate"]) if len(g) > 0 else float("nan"),
                    "baseline_cvar5": _cvar(lr_baseline, 0.05),
                    "baseline_sharpe": _portfolio_sharpe(g["log_ret_baseline"], g["holding_days"]),
                }
            )

    return pd.DataFrame(rows).sort_values(["arm", "phase"]).reset_index(drop=True)


def _holdout_means(summary_holdout: pd.DataFrame, arm: str) -> dict[str, float]:
    s = summary_holdout[summary_holdout["arm"] == arm].copy()
    return {
        "mean_cvar5_holdout": _nanmean(s["mean_cvar5"]),
        "mean_sharpe_cost_adj_holdout": _nanmean(s["mean_sharpe_cost_adj"]),
        "mean_veto_rate_holdout": _nanmean(s["mean_veto_rate"]),
        "baseline_cvar5_holdout": _nanmean(s["baseline_cvar5"]),
        "baseline_sharpe_holdout": _nanmean(s["baseline_sharpe"]),
    }


def main() -> None:
    if not IN_DECISION_CRITERION.exists():
        raise RuntimeError(f"Criterio pre-registrado nao encontrado: {IN_DECISION_CRITERION}")
    with IN_DECISION_CRITERION.open("r", encoding="utf-8") as fp:
        decision_criterion = json.load(fp)

    winner_cfg = _load_winner_snapshot(IN_WINNER)
    top_n = int(winner_cfg["top_n"])
    cadence = int(winner_cfg["rebalance_cadence"])
    anchor_date = pd.Timestamp(winner_cfg["rebalance_anchor_date"]).normalize()
    min_market_cap = float(winner_cfg["min_market_cap"])

    required_cols = [
        "ticker",
        "date",
        "close_operational",
        "market_cap",
    ]
    canonical = pd.read_parquet(IN_CANONICAL, columns=required_cols)
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical = canonical.dropna(subset=["ticker", "date", "close_operational"]).copy()
    canonical = canonical[canonical["date"] <= HOLDOUT_END].copy()

    blacklist = _load_blacklist(IN_BLACKLIST)
    if blacklist:
        canonical = canonical[~canonical["ticker"].isin(blacklist)].copy()
    print(f"Tickers excluidos por blacklist: {len(blacklist)}")

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

    min_rebalances_needed = SKIP_INITIAL_REBALANCES + 12
    remaining_days = len(trading_days) - anchor_idx
    if remaining_days // max(cadence, 1) < min_rebalances_needed:
        anchor_idx = 0
        anchor_date = trading_days[0]
        print(
            "Aviso: ancora do winner era curta para estudo historico; "
            f"usando primeira data do SSOT ({anchor_date.date().isoformat()})."
        )

    print("Computando scores M3...")
    scores_by_day = compute_m3_scores(px_wide)
    print(f"Scores computados para {len(scores_by_day)} pregoes.")

    observations: list[dict[str, Any]] = []
    for arm_name in ARMS:
        threshold = _arm_threshold(arm_name)
        print(f"Executando arm {arm_name} (threshold={threshold})...")
        for phase in range(cadence):
            rebalance_days = _phase_rebalance_days(
                trading_days=trading_days,
                anchor_idx=anchor_idx,
                cadence=cadence,
                phase=phase,
            )
            phase_obs = _run_phase_arm(
                arm_name=arm_name,
                threshold=threshold,
                phase=phase,
                rebalance_days=rebalance_days,
                trading_days=trading_days,
                day_to_idx=day_to_idx,
                scores_by_day=scores_by_day,
                blacklist=blacklist,
                top_n=top_n,
                px_wide=px_wide,
                mc_eligible_by_day=mc_eligible_by_day,
            )
            observations.extend(phase_obs)

    obs_df = pd.DataFrame(observations)
    if obs_df.empty:
        raise RuntimeError("Nenhuma observacao gerada. Verifique dados de entrada e filtros.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    obs_df = obs_df.sort_values(["arm", "phase", "date"]).reset_index(drop=True)
    obs_df.to_csv(OUT_DIR / "observations_late_rocket_us.csv", index=False)

    train_summary = _summarize_by_split(obs_df, split="TRAIN", cadence=cadence)
    holdout_summary = _summarize_by_split(obs_df, split="HOLDOUT", cadence=cadence)
    train_summary.to_csv(OUT_DIR / "summary_TRAIN_late_rocket_us.csv", index=False)
    holdout_summary.to_csv(OUT_DIR / "summary_HOLDOUT_late_rocket_us.csv", index=False)

    holdout_means_by_arm = {arm: _holdout_means(holdout_summary, arm) for arm in ARMS}
    baseline_means = holdout_means_by_arm["Baseline"]

    candidate_arms: list[tuple[str, float, dict[str, float]]] = []
    for arm in ARMS:
        if arm == "Baseline":
            continue
        means = holdout_means_by_arm[arm]
        veto = means["mean_veto_rate_holdout"]
        if not np.isfinite(veto) or veto > MAX_VETO_RATE:
            continue
        cvar = means["mean_cvar5_holdout"]
        sharpe = means["mean_sharpe_cost_adj_holdout"]
        if not np.isfinite(cvar) or not np.isfinite(sharpe):
            continue
        candidate_arms.append((arm, cvar + sharpe, means))

    if not candidate_arms:
        best_arm = "nenhum_viavel"
        best_threshold = None
        best_means = None
        checks = {
            "arm_better_tail": False,
            "arm_better_sharpe": False,
            "arm_acceptable_veto": False,
        }
        final_verdict = "INCONCLUSIVO"
    else:
        best_arm, _, best_means = sorted(candidate_arms, key=lambda x: x[1], reverse=True)[0]
        best_threshold = _arm_threshold(best_arm)
        arm_better_tail = bool(best_means["mean_cvar5_holdout"] > baseline_means["mean_cvar5_holdout"])
        arm_better_sharpe = bool(
            best_means["mean_sharpe_cost_adj_holdout"] > baseline_means["mean_sharpe_cost_adj_holdout"]
        )
        arm_acceptable_veto = bool(best_means["mean_veto_rate_holdout"] <= MAX_VETO_RATE)
        checks = {
            "arm_better_tail": arm_better_tail,
            "arm_better_sharpe": arm_better_sharpe,
            "arm_acceptable_veto": arm_acceptable_veto,
        }
        if arm_better_tail and arm_better_sharpe and arm_acceptable_veto:
            final_verdict = "CONFIRMA_SINAL_US"
        elif (not arm_better_tail) and (not arm_better_sharpe):
            final_verdict = "NAO_CONFIRMA_SINAL_US"
        else:
            final_verdict = "INCONCLUSIVO"

    phase_stats = {
        "meta": {
            "task_id": "T-LATE-ROCKET-ENTRY-US-V1",
            "cadence": cadence,
            "top_n": top_n,
            "min_market_cap": min_market_cap,
            "friction_one_way_rate": FRICTION_ONE_WAY_RATE,
            "max_veto_rate": MAX_VETO_RATE,
            "anchor_date_effective": anchor_date.date().isoformat(),
            "holdout_start": HOLDOUT_START.date().isoformat(),
            "holdout_end": HOLDOUT_END.date().isoformat(),
            "arms": ARMS,
            "notes": "Ablation read-only de gate de entrada por ret_62 no winner C4. Sem alteracao do motor produtivo.",
        },
        "decision_criterion": decision_criterion,
        "holdout_means_by_arm": holdout_means_by_arm,
        "best_arm": best_arm,
        "best_arm_threshold": best_threshold,
        "checks": checks,
        "final_verdict": final_verdict,
        "by_phase_train": train_summary.to_dict(orient="records"),
        "by_phase_holdout": holdout_summary.to_dict(orient="records"),
    }
    with (OUT_DIR / "phase_sweep_stats_late_rocket_us.json").open("w", encoding="utf-8") as fp:
        json.dump(phase_stats, fp, ensure_ascii=False, indent=2)

    verdict_payload = {
        "task_id": "T-LATE-ROCKET-ENTRY-US-V1",
        "criteria_file": str(IN_DECISION_CRITERION.relative_to(ROOT)),
        "final_verdict": final_verdict,
        "best_arm": best_arm,
        "best_arm_threshold": best_threshold,
        "checks": checks,
        "baseline_metrics_holdout": baseline_means,
        "best_arm_metrics_holdout": best_means,
        "max_veto_rate_constraint": MAX_VETO_RATE,
    }
    with (OUT_DIR / "verdict_late_rocket_us.json").open("w", encoding="utf-8") as fp:
        json.dump(verdict_payload, fp, ensure_ascii=False, indent=2)

    print("T-LATE-ROCKET-ENTRY-US-V1 concluido.")
    print(f"observations_total={len(obs_df)}")
    print(f"observations_train={int((obs_df['split'] == 'TRAIN').sum())}")
    print(f"observations_holdout={int((obs_df['split'] == 'HOLDOUT').sum())}")
    print(f"rows_train_summary={len(train_summary)}")
    print(f"rows_holdout_summary={len(holdout_summary)}")
    print(f"best_arm={best_arm}")
    print(f"best_arm_threshold={best_threshold}")
    print(f"final_verdict={final_verdict}")


if __name__ == "__main__":
    main()
