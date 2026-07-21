"""Backtest T-SDC-BAND-EXPANSION-ENTRY-VETO-US-V1.

Estudo read-only pre-registrado:
- Baseline: C4 puro.
- Arm_BandExp: veto de entrada por expansao/monotonicidade de banda SPC.
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

TASK_ID = "T-SDC-BAND-EXPANSION-ENTRY-VETO-US-V1"
ARMS = ["Baseline", "Arm_BandExp"]
FRICTION_ONE_WAY_RATE = 0.000250  # 2.5 bps one-way
SKIP_INITIAL_REBALANCES = 20
TRAIN_END = pd.Timestamp("2022-12-30")
HOLDOUT_START = pd.Timestamp("2023-01-02")
SW1_START = pd.Timestamp("2023-01-02")
SW1_END = pd.Timestamp("2024-06-30")
SW2_START = pd.Timestamp("2024-07-01")

DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2"
IN_CANONICAL = DATASET_DIR / "canonical_us.parquet"
IN_SCORES = DATASET_DIR / "scores_m3_us.parquet"
IN_MANIFEST = DATASET_DIR / "manifest.json"
IN_BLACKLIST = ROOT / "data" / "ssot" / "blacklist_us.json"
IN_WINNER = ROOT / "config" / "winner_us.json"
IN_DECISION_CRITERION = (
    ROOT
    / "backtest"
    / "t_band_exp_entry_us_v1"
    / "decision_criterion_band_exp_entry_us_v1.json"
)
OUT_DIR = ROOT / "backtest" / "t_band_exp_entry_us_v1" / "results"


def _is_finite(v: Any) -> bool:
    return bool(np.isfinite(v))


def _load_blacklist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if isinstance(data, list):
        return {str(x).upper().strip() for x in data}
    out: set[str] = set()
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, list):
                out.update(str(x).upper().strip() for x in value)
    return out


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


def _load_holdout_end_from_manifest(path: Path) -> tuple[pd.Timestamp, dict[str, Any]]:
    if not path.exists():
        raise RuntimeError(f"manifest nao encontrado: {path}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    freeze_asof = str(manifest.get("freeze_asof", "")).strip()
    if not freeze_asof:
        raise RuntimeError("manifest sem freeze_asof")
    holdout_end = pd.Timestamp(freeze_asof).normalize()
    return holdout_end, manifest


def _to_split(day: pd.Timestamp, holdout_end: pd.Timestamp) -> str:
    if day <= TRAIN_END:
        return "TRAIN"
    if HOLDOUT_START <= day <= holdout_end:
        if SW1_START <= day <= SW1_END:
            return "SW1"
        if SW2_START <= day <= holdout_end:
            return "SW2"
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


def _build_spc_blocked_by_day(canonical: pd.DataFrame) -> dict[pd.Timestamp, set[str]]:
    required_spc_cols = [
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
    missing = sorted(set(required_spc_cols) - set(canonical.columns))
    if missing:
        raise RuntimeError(f"canonical_us.parquet sem colunas SPC obrigatorias: {missing}")

    spc = canonical[["date", "ticker"] + required_spc_cols].copy()
    for c in required_spc_cols:
        spc[c] = pd.to_numeric(spc[c], errors="coerce")

    any_rule = (
        (spc["i_value"] > spc["i_ucl"])
        | (spc["i_value"] < spc["i_lcl"])
        | (spc["mr_value"] > spc["mr_ucl"])
        | (spc["r_value"] > spc["r_ucl"])
        | (spc["xbar_value"] > spc["xbar_ucl"])
        | (spc["xbar_value"] < spc["xbar_lcl"])
    )

    spc["blocked_spc"] = any_rule.fillna(False)
    out: dict[pd.Timestamp, set[str]] = {}
    for d, g in spc.groupby("date", sort=True):
        d_norm = pd.Timestamp(d).normalize()
        out[d_norm] = set(
            g.loc[g["blocked_spc"], "ticker"].astype(str).str.upper().str.strip().tolist()
        )
    return out


def _build_bandexp_flag_by_day(
    canonical: pd.DataFrame,
    spc_blocked_by_day: dict[pd.Timestamp, set[str]],
) -> tuple[dict[pd.Timestamp, set[str]], pd.DataFrame]:
    work = canonical[
        ["date", "ticker", "i_ucl", "i_lcl", "i_value", "xbar_value", "xbar_lcl", "xbar_ucl", "mr_value", "mr_ucl", "r_value", "r_ucl"]
    ].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["ticker"] = work["ticker"].astype(str).str.upper().str.strip()
    for col in ["i_ucl", "i_lcl"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["date", "ticker"]).sort_values(["ticker", "date"]).reset_index(drop=True)

    g = work.groupby("ticker", sort=False)
    work["width"] = work["i_ucl"] - work["i_lcl"]
    work["width_20ago"] = g["width"].shift(20)
    work["band_exp20"] = work["width"] / work["width_20ago"] - 1.0
    work["w_diff_pos"] = (g["width"].diff() > 0).astype(float)
    work["mono20"] = g["w_diff_pos"].transform(lambda s: s.rolling(20, min_periods=20).mean())

    work["w_pct_dia"] = work.groupby("date", sort=False)["width"].rank(pct=True)
    work["w_ter"] = pd.cut(
        work["w_pct_dia"],
        [0.0, 1.0 / 3.0, 2.0 / 3.0, 1.0],
        labels=["ESTREITA", "MEDIA", "LARGA"],
        include_lowest=True,
    )
    work["exp_pct_dia"] = work.groupby("date", sort=False)["band_exp20"].rank(pct=True)

    def _is_blocked(row: pd.Series) -> bool:
        day = pd.Timestamp(row["date"]).normalize()
        ticker = str(row["ticker"]).upper().strip()
        return ticker in spc_blocked_by_day.get(day, set())

    work["viol_any"] = work.apply(_is_blocked, axis=1)
    work["flag_bandexp"] = (
        (work["w_ter"] == "LARGA")
        & (pd.to_numeric(work["exp_pct_dia"], errors="coerce") >= 0.80)
        & (pd.to_numeric(work["mono20"], errors="coerce") >= 0.65)
        & (~work["viol_any"])
    )
    work["flag_bandexp"] = work["flag_bandexp"].fillna(False)

    flagged_by_day: dict[pd.Timestamp, set[str]] = {}
    for d, grp in work.groupby("date", sort=True):
        d_norm = pd.Timestamp(d).normalize()
        flagged_by_day[d_norm] = set(
            grp.loc[grp["flag_bandexp"], "ticker"].astype(str).str.upper().str.strip().tolist()
        )
    return flagged_by_day, work


def _compute_scores_by_day(px_wide: pd.DataFrame, holdout_end: pd.Timestamp) -> dict[pd.Timestamp, pd.DataFrame]:
    scores_by_day = compute_m3_scores(px_wide)
    out: dict[pd.Timestamp, pd.DataFrame] = {}
    for d, g in scores_by_day.items():
        d_norm = pd.Timestamp(d).normalize()
        if d_norm <= holdout_end:
            out[d_norm] = g
    return out


def _arm_gate_set(
    arm_name: str,
    d_prev: pd.Timestamp,
    flagged_by_day: dict[pd.Timestamp, set[str]],
) -> set[str]:
    if arm_name == "Arm_BandExp":
        return set(flagged_by_day.get(d_prev, set()))
    return set()


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
    holdout_end: pd.Timestamp,
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

        split = _to_split(d_reb, holdout_end=holdout_end)
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
            flagged_by_day=flagged_by_day,
        )
        arm_gate_blacklist = set(blacklist) | blocked_total
        arm_selected = select_top_n(prev_scores, top_n=top_n, blacklist=arm_gate_blacklist)
        if not arm_selected:
            continue

        blocked = sorted([t for t in baseline_selected if t in blocked_total])
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
                "log_ret_baseline": log_ret_baseline,
                "log_ret_arm": log_ret_arm,
                "log_ret_arm_cost_adj": log_ret_arm_cost_adj,
                "cost_arm": cost_arm,
            }
        )

    return observations


def _subset_df(obs_df: pd.DataFrame, subset: str) -> pd.DataFrame:
    if subset == "TRAIN":
        return obs_df[obs_df["split"] == "TRAIN"].copy()
    if subset == "HOLDOUT":
        return obs_df[obs_df["is_holdout"] == 1].copy()
    if subset in {"SW1", "SW2"}:
        return obs_df[obs_df["split"] == subset].copy()
    raise ValueError(f"Subset desconhecido: {subset}")


def _summarize_subset(
    obs_df: pd.DataFrame,
    subset: str,
    cadence: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    subset_df = _subset_df(obs_df, subset=subset)

    for arm in ARMS:
        arm_df = subset_df[subset_df["arm"] == arm].copy()
        for phase in range(cadence):
            g = arm_df[arm_df["phase"] == phase].copy()
            lr_baseline = pd.to_numeric(g["log_ret_baseline"], errors="coerce").to_numpy(dtype=float)
            lr_arm_cost_adj = pd.to_numeric(g["log_ret_arm_cost_adj"], errors="coerce").to_numpy(dtype=float)

            rows.append(
                {
                    "arm": arm,
                    "phase": int(phase),
                    "split": subset,
                    "n_cycles": int(len(g)),
                    "mean_cvar5": _cvar(lr_arm_cost_adj, 0.05),
                    "mean_sharpe_cost_adj": _portfolio_sharpe(g["log_ret_arm_cost_adj"], g["holding_days"]),
                    "mean_veto_rate": _nanmean(g["veto_rate"]) if len(g) > 0 else float("nan"),
                    "baseline_cvar5": _cvar(lr_baseline, 0.05),
                    "baseline_sharpe": _portfolio_sharpe(g["log_ret_baseline"], g["holding_days"]),
                }
            )
    return pd.DataFrame(rows).sort_values(["arm", "phase"]).reset_index(drop=True)


def _subset_means(summary_df: pd.DataFrame, arm: str) -> dict[str, float]:
    s = summary_df[summary_df["arm"] == arm].copy()
    return {
        "mean_cvar5": _nanmean(s["mean_cvar5"]),
        "mean_sharpe_cost_adj": _nanmean(s["mean_sharpe_cost_adj"]),
        "mean_veto_rate": _nanmean(s["mean_veto_rate"]),
        "baseline_cvar5": _nanmean(s["baseline_cvar5"]),
        "baseline_sharpe": _nanmean(s["baseline_sharpe"]),
    }


def _paired_observations(obs_df: pd.DataFrame, subset: str) -> pd.DataFrame:
    sub = _subset_df(obs_df, subset=subset).copy()
    if sub.empty:
        return sub
    sub["date"] = pd.to_datetime(sub["date"], errors="coerce").dt.normalize()
    keep_cols = ["date", "phase", "holding_days", "arm", "log_ret_arm_cost_adj", "veto_rate"]
    sub = sub[keep_cols].copy()
    pivot = sub.pivot_table(
        index=["date", "phase", "holding_days"],
        columns="arm",
        values=["log_ret_arm_cost_adj", "veto_rate"],
        aggfunc="first",
    )
    pivot.columns = [f"{a}_{b}" for a, b in pivot.columns]
    pivot = pivot.reset_index()
    needed = [
        "log_ret_arm_cost_adj_Baseline",
        "log_ret_arm_cost_adj_Arm_BandExp",
    ]
    for col in needed:
        if col not in pivot.columns:
            pivot[col] = np.nan
    pivot = pivot.dropna(subset=needed).copy()
    return pivot


def _bootstrap_metric_stats(
    obs_df: pd.DataFrame,
    subset: str,
    n_resamples: int = 2000,
    seed: int = 42,
) -> dict[str, Any]:
    paired = _paired_observations(obs_df, subset=subset)
    if paired.empty:
        return {
            "subset": subset,
            "n_dates": 0,
            "n_pairs": 0,
            "delta_cvar5": {"delta_mean": float("nan"), "ic95": [float("nan"), float("nan")], "mass_same_sign_pct": float("nan")},
            "delta_sharpe_cost_adj": {"delta_mean": float("nan"), "ic95": [float("nan"), float("nan")], "mass_same_sign_pct": float("nan")},
        }

    rng = np.random.default_rng(seed)
    dates = np.array(sorted(paired["date"].dropna().unique()))
    by_day = {d: paired[paired["date"] == d].copy() for d in dates}

    deltas_cvar: list[float] = []
    deltas_sharpe: list[float] = []

    for _ in range(n_resamples):
        sampled_days = rng.choice(dates, size=len(dates), replace=True)
        boot = pd.concat([by_day[d] for d in sampled_days], ignore_index=True)

        base_ret = pd.to_numeric(boot["log_ret_arm_cost_adj_Baseline"], errors="coerce").to_numpy(dtype=float)
        arm_ret = pd.to_numeric(boot["log_ret_arm_cost_adj_Arm_BandExp"], errors="coerce").to_numpy(dtype=float)
        hold = pd.to_numeric(boot["holding_days"], errors="coerce").to_numpy(dtype=float)

        cvar_base = _cvar(base_ret, 0.05)
        cvar_arm = _cvar(arm_ret, 0.05)
        if _is_finite(cvar_base) and _is_finite(cvar_arm):
            deltas_cvar.append(float(cvar_arm - cvar_base))

        s_base = _portfolio_sharpe(pd.Series(base_ret), pd.Series(hold))
        s_arm = _portfolio_sharpe(pd.Series(arm_ret), pd.Series(hold))
        if _is_finite(s_base) and _is_finite(s_arm):
            deltas_sharpe.append(float(s_arm - s_base))

    dc = np.asarray(deltas_cvar, dtype=float)
    ds = np.asarray(deltas_sharpe, dtype=float)

    def _pack(arr: np.ndarray) -> dict[str, Any]:
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            return {
                "delta_mean": float("nan"),
                "ic95": [float("nan"), float("nan")],
                "mass_same_sign_pct": float("nan"),
            }
        mean = float(np.nanmean(arr))
        lo = float(np.nanpercentile(arr, 2.5))
        hi = float(np.nanpercentile(arr, 97.5))
        if mean > 0:
            mass = float(np.mean(arr > 0) * 100.0)
        elif mean < 0:
            mass = float(np.mean(arr < 0) * 100.0)
        else:
            mass = 50.0
        return {"delta_mean": mean, "ic95": [lo, hi], "mass_same_sign_pct": mass}

    return {
        "subset": subset,
        "n_dates": int(len(dates)),
        "n_pairs": int(len(paired)),
        "delta_cvar5": _pack(dc),
        "delta_sharpe_cost_adj": _pack(ds),
    }


def _favorable_ic(ic95: list[float]) -> bool:
    if len(ic95) != 2:
        return False
    lo, hi = ic95
    if not (_is_finite(lo) and _is_finite(hi)):
        return False
    return bool(lo > 0.0 and hi > 0.0)


def _delta_favorable(v: float) -> bool:
    return bool(_is_finite(v) and v > 0.0)


def main() -> None:
    if not IN_DECISION_CRITERION.exists():
        raise RuntimeError(f"Criterio pre-registrado nao encontrado: {IN_DECISION_CRITERION}")
    with IN_DECISION_CRITERION.open("r", encoding="utf-8") as fp:
        decision_criterion = json.load(fp)

    holdout_end, manifest = _load_holdout_end_from_manifest(IN_MANIFEST)
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

    blacklist = _load_blacklist(IN_BLACKLIST)
    if blacklist:
        canonical = canonical[~canonical["ticker"].isin(blacklist)].copy()
    print(f"Tickers excluidos por blacklist: {len(blacklist)}")

    spc_blocked_by_day = _build_spc_blocked_by_day(canonical)
    print(f"SPC blocked map carregado para {len(spc_blocked_by_day)} pregoes.")
    flagged_by_day, flags_df = _build_bandexp_flag_by_day(canonical, spc_blocked_by_day=spc_blocked_by_day)
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

    min_rebalances_needed = SKIP_INITIAL_REBALANCES + 12
    remaining_days = len(trading_days) - anchor_idx
    if remaining_days // max(cadence, 1) < min_rebalances_needed:
        anchor_idx = 0
        anchor_date = trading_days[0]
        print(
            "Aviso: ancora do winner era curta para estudo historico; "
            f"usando primeira data do SSOT ({anchor_date.date().isoformat()})."
        )

    scores_by_day = _compute_scores_by_day(px_wide, holdout_end=holdout_end)
    print(f"Scores computados para {len(scores_by_day)} pregoes.")

    observations: list[dict[str, Any]] = []
    for arm_name in ARMS:
        print(f"Executando arm {arm_name}...")
        for phase in range(cadence):
            rebalance_days = _phase_rebalance_days(
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
                holdout_end=holdout_end,
            )
            observations.extend(phase_obs)

    obs_df = pd.DataFrame(observations)
    if obs_df.empty:
        raise RuntimeError("Nenhuma observacao gerada. Verifique dados de entrada e filtros.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    obs_df = obs_df.sort_values(["arm", "phase", "date"]).reset_index(drop=True)
    obs_df.to_csv(OUT_DIR / "observations_band_exp_entry_us_v1.csv", index=False)

    train_summary = _summarize_subset(obs_df, subset="TRAIN", cadence=cadence)
    holdout_summary = _summarize_subset(obs_df, subset="HOLDOUT", cadence=cadence)
    sw1_summary = _summarize_subset(obs_df, subset="SW1", cadence=cadence)
    sw2_summary = _summarize_subset(obs_df, subset="SW2", cadence=cadence)

    train_summary.to_csv(OUT_DIR / "summary_TRAIN_band_exp_entry_us_v1.csv", index=False)
    holdout_summary.to_csv(OUT_DIR / "summary_HOLDOUT_band_exp_entry_us_v1.csv", index=False)
    sw1_summary.to_csv(OUT_DIR / "summary_SW1_band_exp_entry_us_v1.csv", index=False)
    sw2_summary.to_csv(OUT_DIR / "summary_SW2_band_exp_entry_us_v1.csv", index=False)

    holdout_means_by_arm = {arm: _subset_means(holdout_summary, arm) for arm in ARMS}
    sw1_means_by_arm = {arm: _subset_means(sw1_summary, arm) for arm in ARMS}
    sw2_means_by_arm = {arm: _subset_means(sw2_summary, arm) for arm in ARMS}
    baseline_holdout = holdout_means_by_arm["Baseline"]
    arm_holdout = holdout_means_by_arm["Arm_BandExp"]

    delta_holdout_cvar5 = float(arm_holdout["mean_cvar5"] - baseline_holdout["mean_cvar5"])
    delta_holdout_sharpe = float(
        arm_holdout["mean_sharpe_cost_adj"] - baseline_holdout["mean_sharpe_cost_adj"]
    )
    delta_sw1_cvar5 = float(sw1_means_by_arm["Arm_BandExp"]["mean_cvar5"] - sw1_means_by_arm["Baseline"]["mean_cvar5"])
    delta_sw1_sharpe = float(
        sw1_means_by_arm["Arm_BandExp"]["mean_sharpe_cost_adj"] - sw1_means_by_arm["Baseline"]["mean_sharpe_cost_adj"]
    )
    delta_sw2_cvar5 = float(sw2_means_by_arm["Arm_BandExp"]["mean_cvar5"] - sw2_means_by_arm["Baseline"]["mean_cvar5"])
    delta_sw2_sharpe = float(
        sw2_means_by_arm["Arm_BandExp"]["mean_sharpe_cost_adj"] - sw2_means_by_arm["Baseline"]["mean_sharpe_cost_adj"]
    )

    bcfg = decision_criterion.get("bootstrap", {})
    n_resamples = int(bcfg.get("n_resamples", 2000))
    seed = int(bcfg.get("seed", 42))
    bs_holdout = _bootstrap_metric_stats(obs_df, subset="HOLDOUT", n_resamples=n_resamples, seed=seed)
    bs_sw1 = _bootstrap_metric_stats(obs_df, subset="SW1", n_resamples=n_resamples, seed=seed)
    bs_sw2 = _bootstrap_metric_stats(obs_df, subset="SW2", n_resamples=n_resamples, seed=seed)

    bootstrap_payload = {
        "task_id": TASK_ID,
        "method": "cluster por dia de rebalance",
        "n_resamples": n_resamples,
        "seed": seed,
        "splits": {
            "HOLDOUT": bs_holdout,
            "SW1": bs_sw1,
            "SW2": bs_sw2,
        },
    }
    with (OUT_DIR / "bootstrap_stats_band_exp_entry_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(bootstrap_payload, fp, ensure_ascii=False, indent=2)

    max_veto_rate = float(decision_criterion.get("max_veto_rate", 0.35))
    veto_holdout_ok = bool(_is_finite(arm_holdout["mean_veto_rate"]) and arm_holdout["mean_veto_rate"] <= max_veto_rate)
    veto_sw1_ok = bool(
        _is_finite(sw1_means_by_arm["Arm_BandExp"]["mean_veto_rate"])
        and sw1_means_by_arm["Arm_BandExp"]["mean_veto_rate"] <= max_veto_rate
    )
    veto_sw2_ok = bool(
        _is_finite(sw2_means_by_arm["Arm_BandExp"]["mean_veto_rate"])
        and sw2_means_by_arm["Arm_BandExp"]["mean_veto_rate"] <= max_veto_rate
    )

    domina_forte = bool(
        _favorable_ic(bs_holdout["delta_cvar5"]["ic95"])
        and _favorable_ic(bs_holdout["delta_sharpe_cost_adj"]["ic95"])
        and _favorable_ic(bs_sw1["delta_cvar5"]["ic95"])
        and _favorable_ic(bs_sw1["delta_sharpe_cost_adj"]["ic95"])
        and _favorable_ic(bs_sw2["delta_cvar5"]["ic95"])
        and _favorable_ic(bs_sw2["delta_sharpe_cost_adj"]["ic95"])
        and veto_holdout_ok
        and veto_sw1_ok
        and veto_sw2_ok
    )

    holdout_mass_ok = bool(
        _is_finite(bs_holdout["delta_cvar5"]["mass_same_sign_pct"])
        and _is_finite(bs_holdout["delta_sharpe_cost_adj"]["mass_same_sign_pct"])
        and bs_holdout["delta_cvar5"]["mass_same_sign_pct"] >= 90.0
        and bs_holdout["delta_sharpe_cost_adj"]["mass_same_sign_pct"] >= 90.0
    )
    direction_ok = bool(
        _delta_favorable(delta_holdout_cvar5)
        and _delta_favorable(delta_holdout_sharpe)
        and _delta_favorable(delta_sw1_cvar5)
        and _delta_favorable(delta_sw1_sharpe)
        and _delta_favorable(delta_sw2_cvar5)
        and _delta_favorable(delta_sw2_sharpe)
    )
    materiality_ok = bool(
        (abs(delta_holdout_sharpe) >= 0.30) or (abs(delta_holdout_cvar5) >= 0.02)
    )
    favorecido_bandexp = bool(
        (not domina_forte)
        and holdout_mass_ok
        and direction_ok
        and materiality_ok
        and veto_holdout_ok
    )

    if domina_forte:
        final_verdict = "DOMINA_FORTE"
    elif favorecido_bandexp:
        final_verdict = "FAVORECIDO_BANDEXP"
    else:
        final_verdict = "INCONCLUSIVO"

    checks = {
        "domina_forte_conditions": {
            "holdout_ic_cvar5_favoravel": _favorable_ic(bs_holdout["delta_cvar5"]["ic95"]),
            "holdout_ic_sharpe_favoravel": _favorable_ic(bs_holdout["delta_sharpe_cost_adj"]["ic95"]),
            "sw1_ic_cvar5_favoravel": _favorable_ic(bs_sw1["delta_cvar5"]["ic95"]),
            "sw1_ic_sharpe_favoravel": _favorable_ic(bs_sw1["delta_sharpe_cost_adj"]["ic95"]),
            "sw2_ic_cvar5_favoravel": _favorable_ic(bs_sw2["delta_cvar5"]["ic95"]),
            "sw2_ic_sharpe_favoravel": _favorable_ic(bs_sw2["delta_sharpe_cost_adj"]["ic95"]),
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
        "arms": ARMS,
        "deltas": {
            "HOLDOUT": {
                "delta_cvar5": delta_holdout_cvar5,
                "delta_sharpe_cost_adj": delta_holdout_sharpe,
                "mean_veto_rate_arm": arm_holdout["mean_veto_rate"],
            },
            "SW1": {
                "delta_cvar5": delta_sw1_cvar5,
                "delta_sharpe_cost_adj": delta_sw1_sharpe,
                "mean_veto_rate_arm": sw1_means_by_arm["Arm_BandExp"]["mean_veto_rate"],
            },
            "SW2": {
                "delta_cvar5": delta_sw2_cvar5,
                "delta_sharpe_cost_adj": delta_sw2_sharpe,
                "mean_veto_rate_arm": sw2_means_by_arm["Arm_BandExp"]["mean_veto_rate"],
            },
        },
        "bootstrap": {
            "HOLDOUT": bs_holdout,
            "SW1": bs_sw1,
            "SW2": bs_sw2,
        },
        "checks": checks,
        "thresholds": {
            "max_veto_rate": max_veto_rate,
            "materiality_sharpe_abs_min": 0.30,
            "materiality_cvar5_abs_min": 0.02,
            "bootstrap_mass_min_pct": 90.0,
        },
    }
    with (OUT_DIR / "verdict_band_exp_entry_us_v1.json").open("w", encoding="utf-8") as fp:
        json.dump(verdict_payload, fp, ensure_ascii=False, indent=2)

    phase_stats = {
        "meta": {
            "task_id": TASK_ID,
            "cadence": cadence,
            "top_n": top_n,
            "min_market_cap": min_market_cap,
            "friction_one_way_rate": FRICTION_ONE_WAY_RATE,
            "anchor_date_effective": anchor_date.date().isoformat(),
            "holdout_start": HOLDOUT_START.date().isoformat(),
            "holdout_end": holdout_end.date().isoformat(),
            "subwindows": {
                "SW1": {"start": SW1_START.date().isoformat(), "end": SW1_END.date().isoformat()},
                "SW2": {"start": SW2_START.date().isoformat(), "end": holdout_end.date().isoformat()},
            },
            "arms": ARMS,
            "notes": "Ablation read-only para gate de expansao de banda em entrada.",
        },
        "decision_criterion": decision_criterion,
        "holdout_means_by_arm": holdout_means_by_arm,
        "subwindow_means_by_arm": {
            "SW1": sw1_means_by_arm,
            "SW2": sw2_means_by_arm,
        },
        "final_verdict": final_verdict,
        "by_phase_train": train_summary.to_dict(orient="records"),
        "by_phase_holdout": holdout_summary.to_dict(orient="records"),
        "by_phase_sw1": sw1_summary.to_dict(orient="records"),
        "by_phase_sw2": sw2_summary.to_dict(orient="records"),
    }
    with (OUT_DIR / "phase_sweep_stats_band_exp_entry_us_v1.json").open("w", encoding="utf-8") as fp:
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
    print(f"delta_holdout_cvar5={delta_holdout_cvar5:+.6f}")
    print(f"delta_holdout_sharpe={delta_holdout_sharpe:+.6f}")
    print(f"final_verdict={final_verdict}")


if __name__ == "__main__":
    main()
