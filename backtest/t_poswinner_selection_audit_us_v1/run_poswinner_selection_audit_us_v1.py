"""Backtest read-only: pos-winner selection audit US V1.

Task: T-SDC-POSWINNER-SELECTION-AUDIT-US-V1
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

from backtest.run_backtest_variants_us import load_inputs  # noqa: E402
from lib.engine import compute_m3_scores, select_top_n, zscore_cross_section  # noqa: E402

TASK_ID = "T-SDC-POSWINNER-SELECTION-AUDIT-US-V1"
IN_BLACKLIST = ROOT / "data" / "ssot" / "blacklist_us.json"
IN_WINNER = ROOT / "config" / "winner_us.json"
IN_CRITERION = (
    ROOT
    / "backtest"
    / "t_poswinner_selection_audit_us_v1"
    / "decision_criterion_poswinner_selection_audit_us_v1.json"
)
OUT_DIR = ROOT / "backtest" / "t_poswinner_selection_audit_us_v1" / "results"

OUT_OBS = OUT_DIR / "observations_poswinner_us_v1.csv"
OUT_OBS_TICKER = OUT_DIR / "observations_ticker_late_rocket_us_v1.csv"
OUT_BOOTSTRAP = OUT_DIR / "bootstrap_diagnostics_us_v1.json"
OUT_VERDICT = OUT_DIR / "verdict_poswinner_us_v1.json"

BASELINE_ARM = "Baseline_300M"
ARMS_FRENTE1 = ["Arm_RecentSlopeVeto", "Arm_ShortTermScore"]
ARMS_FRENTE2 = ["Arm_150M", "Arm_75M"]


def _is_finite(v: Any) -> bool:
    return bool(np.isfinite(v))


def _safe_float(v: Any) -> float:
    x = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    return float(x) if np.isfinite(x) else float("nan")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"arquivo nao encontrado: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_blacklist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    data = _load_json(path)
    if isinstance(data, list):
        return {str(x).upper().strip() for x in data}
    out: set[str] = set()
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                out.update(str(x).upper().strip() for x in v)
    return out


def _load_winner_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"winner_us.json nao encontrado: {path}")
    data = _load_json(path)
    snap = data.get("winner_config_snapshot", data)
    return {
        "top_n": int(snap.get("top_n", 20)),
        "rebalance_cadence": int(snap.get("rebalance_cadence", 10)),
        "rebalance_anchor_date": str(snap.get("rebalance_anchor_date", "2026-04-16")),
        "min_market_cap": float(snap.get("min_market_cap", 300_000_000.0)),
    }


def _to_split(
    day: pd.Timestamp,
    train_end: pd.Timestamp,
    holdout_start: pd.Timestamp,
    holdout_end: pd.Timestamp,
    sw1_start: pd.Timestamp,
    sw1_end: pd.Timestamp,
    sw2_start: pd.Timestamp,
    sw2_end: pd.Timestamp,
) -> str:
    if day <= train_end:
        return "TRAIN"
    if holdout_start <= day <= holdout_end:
        if sw1_start <= day <= sw1_end:
            return "SW1"
        if sw2_start <= day <= sw2_end:
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


def _ticker_log_return(
    px_wide: pd.DataFrame,
    start_day: pd.Timestamp,
    end_day: pd.Timestamp,
    ticker: str,
) -> float:
    t = str(ticker).upper().strip()
    if start_day not in px_wide.index or end_day not in px_wide.index or t not in px_wide.columns:
        return float("nan")
    p0 = _safe_float(px_wide.at[start_day, t])
    p1 = _safe_float(px_wide.at[end_day, t])
    if not _is_finite(p0) or not _is_finite(p1) or p0 <= 0 or p1 <= 0:
        return float("nan")
    return float(np.log(p1 / p0))


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


def _subset_df(obs_df: pd.DataFrame, subset: str) -> pd.DataFrame:
    if subset == "TRAIN":
        return obs_df[obs_df["split"] == "TRAIN"].copy()
    if subset == "HOLDOUT":
        return obs_df[obs_df["is_holdout"] == 1].copy()
    if subset in {"SW1", "SW2"}:
        return obs_df[obs_df["split"] == subset].copy()
    raise ValueError(f"subset desconhecido: {subset}")


def _compute_metrics(df: pd.DataFrame) -> dict[str, float]:
    lr = pd.to_numeric(df["log_ret_arm_cost_adj"], errors="coerce").to_numpy(dtype=float)
    hd = pd.to_numeric(df["holding_days"], errors="coerce").to_numpy(dtype=float)
    valid = np.isfinite(lr) & np.isfinite(hd) & (hd > 0)
    if not valid.any():
        return {
            "mean_cvar5": float("nan"),
            "mean_sharpe_cost_adj": float("nan"),
            "mean_cagr_proxy": float("nan"),
            "n_cycles": 0.0,
        }
    lr_v = lr[valid]
    hd_v = hd[valid]
    return {
        "mean_cvar5": _cvar(lr_v, 0.05),
        "mean_sharpe_cost_adj": _portfolio_sharpe(pd.Series(lr_v), pd.Series(hd_v)),
        "mean_cagr_proxy": float(np.mean(lr_v / hd_v) * 252.0),
        "n_cycles": float(len(lr_v)),
    }


def _pair_by_cycle(arm_df: pd.DataFrame, base_df: pd.DataFrame) -> pd.DataFrame:
    arm = arm_df.copy()
    base = base_df.copy()
    arm["cycle_id"] = arm["phase"].astype(str) + "|" + arm["date"].astype(str)
    base["cycle_id"] = base["phase"].astype(str) + "|" + base["date"].astype(str)

    arm = arm.set_index("cycle_id")
    base = base.set_index("cycle_id")
    common = sorted(set(arm.index).intersection(set(base.index)))
    if not common:
        return pd.DataFrame()

    paired = pd.DataFrame({"cycle_id": common})
    paired["split"] = [str(arm.at[c, "split"]) for c in common]
    paired["arm_ret"] = [arm.at[c, "log_ret_arm_cost_adj"] for c in common]
    paired["arm_days"] = [arm.at[c, "holding_days"] for c in common]
    paired["base_ret"] = [base.at[c, "log_ret_arm_cost_adj"] for c in common]
    paired["base_days"] = [base.at[c, "holding_days"] for c in common]
    return paired


def _metrics_from_arrays(ret: np.ndarray, days: np.ndarray) -> dict[str, float]:
    valid = np.isfinite(ret) & np.isfinite(days) & (days > 0)
    if not valid.any():
        return {
            "cvar5": float("nan"),
            "sharpe": float("nan"),
            "cagr_proxy": float("nan"),
        }
    rv = ret[valid]
    dv = days[valid]
    return {
        "cvar5": _cvar(rv, 0.05),
        "sharpe": _portfolio_sharpe(pd.Series(rv), pd.Series(dv)),
        "cagr_proxy": float(np.mean(rv / dv) * 252.0),
    }


def _bootstrap_pair_deltas(
    paired: pd.DataFrame,
    n_bootstrap: int,
    seed: int = 42,
) -> dict[str, Any]:
    if paired.empty:
        return {
            "n_cycles": 0,
            "delta_sharpe_cost_adj": {"point": float("nan"), "ci95": [float("nan"), float("nan")], "mass_pos": float("nan"), "mass_neg": float("nan")},
            "delta_cvar5": {"point": float("nan"), "ci95": [float("nan"), float("nan")], "mass_pos": float("nan"), "mass_neg": float("nan")},
            "delta_cagr_proxy": {"point": float("nan"), "ci95": [float("nan"), float("nan")], "mass_pos": float("nan"), "mass_neg": float("nan")},
        }

    arm_ret = pd.to_numeric(paired["arm_ret"], errors="coerce").to_numpy(dtype=float)
    arm_days = pd.to_numeric(paired["arm_days"], errors="coerce").to_numpy(dtype=float)
    base_ret = pd.to_numeric(paired["base_ret"], errors="coerce").to_numpy(dtype=float)
    base_days = pd.to_numeric(paired["base_days"], errors="coerce").to_numpy(dtype=float)

    valid = (
        np.isfinite(arm_ret)
        & np.isfinite(arm_days)
        & np.isfinite(base_ret)
        & np.isfinite(base_days)
        & (arm_days > 0)
        & (base_days > 0)
    )
    arm_ret = arm_ret[valid]
    arm_days = arm_days[valid]
    base_ret = base_ret[valid]
    base_days = base_days[valid]
    n = len(arm_ret)
    if n < 2:
        return {
            "n_cycles": int(n),
            "delta_sharpe_cost_adj": {"point": float("nan"), "ci95": [float("nan"), float("nan")], "mass_pos": float("nan"), "mass_neg": float("nan")},
            "delta_cvar5": {"point": float("nan"), "ci95": [float("nan"), float("nan")], "mass_pos": float("nan"), "mass_neg": float("nan")},
            "delta_cagr_proxy": {"point": float("nan"), "ci95": [float("nan"), float("nan")], "mass_pos": float("nan"), "mass_neg": float("nan")},
        }

    arm_m = _metrics_from_arrays(arm_ret, arm_days)
    base_m = _metrics_from_arrays(base_ret, base_days)
    point_sharpe = arm_m["sharpe"] - base_m["sharpe"]
    point_cvar = arm_m["cvar5"] - base_m["cvar5"]
    point_cagr = arm_m["cagr_proxy"] - base_m["cagr_proxy"]

    rng = np.random.default_rng(seed)
    d_sh: list[float] = []
    d_cv: list[float] = []
    d_cg: list[float] = []
    for _ in range(int(n_bootstrap)):
        idx = rng.integers(0, n, size=n)
        arm_b = _metrics_from_arrays(arm_ret[idx], arm_days[idx])
        base_b = _metrics_from_arrays(base_ret[idx], base_days[idx])
        d_sh.append(float(arm_b["sharpe"] - base_b["sharpe"]))
        d_cv.append(float(arm_b["cvar5"] - base_b["cvar5"]))
        d_cg.append(float(arm_b["cagr_proxy"] - base_b["cagr_proxy"]))

    def _stats(arr: list[float], point: float) -> dict[str, Any]:
        a = np.asarray(arr, dtype=float)
        a = a[np.isfinite(a)]
        if a.size == 0:
            return {
                "point": float(point),
                "ci95": [float("nan"), float("nan")],
                "mass_pos": float("nan"),
                "mass_neg": float("nan"),
            }
        return {
            "point": float(point),
            "ci95": [float(np.quantile(a, 0.025)), float(np.quantile(a, 0.975))],
            "mass_pos": float(np.mean(a > 0.0)),
            "mass_neg": float(np.mean(a < 0.0)),
        }

    return {
        "n_cycles": int(n),
        "delta_sharpe_cost_adj": _stats(d_sh, point_sharpe),
        "delta_cvar5": _stats(d_cv, point_cvar),
        "delta_cagr_proxy": _stats(d_cg, point_cagr),
    }


def _point_deltas(paired: pd.DataFrame) -> dict[str, float]:
    if paired.empty:
        return {
            "delta_sharpe_cost_adj": float("nan"),
            "delta_cvar5": float("nan"),
            "delta_cagr_proxy": float("nan"),
        }
    arm_ret = pd.to_numeric(paired["arm_ret"], errors="coerce").to_numpy(dtype=float)
    arm_days = pd.to_numeric(paired["arm_days"], errors="coerce").to_numpy(dtype=float)
    base_ret = pd.to_numeric(paired["base_ret"], errors="coerce").to_numpy(dtype=float)
    base_days = pd.to_numeric(paired["base_days"], errors="coerce").to_numpy(dtype=float)
    valid = (
        np.isfinite(arm_ret)
        & np.isfinite(arm_days)
        & np.isfinite(base_ret)
        & np.isfinite(base_days)
        & (arm_days > 0)
        & (base_days > 0)
    )
    arm_m = _metrics_from_arrays(arm_ret[valid], arm_days[valid])
    base_m = _metrics_from_arrays(base_ret[valid], base_days[valid])
    return {
        "delta_sharpe_cost_adj": float(arm_m["sharpe"] - base_m["sharpe"]),
        "delta_cvar5": float(arm_m["cvar5"] - base_m["cvar5"]),
        "delta_cagr_proxy": float(arm_m["cagr_proxy"] - base_m["cagr_proxy"]),
    }


def _evaluate_arm_verdict(
    arm_name: str,
    holdout_boot: dict[str, Any],
    sw1_point: dict[str, float],
    sw2_point: dict[str, float],
    gate_ok: bool,
    sharpe_abs_min: float,
    cagr_abs_min: float,
) -> dict[str, Any]:
    m_sh = holdout_boot["delta_sharpe_cost_adj"]
    m_cv = holdout_boot["delta_cvar5"]
    m_cg = holdout_boot["delta_cagr_proxy"]

    ci_sh = m_sh["ci95"]
    ci_cv = m_cv["ci95"]
    ci_cg = m_cg["ci95"]

    domina = (
        gate_ok
        and _is_finite(ci_sh[0])
        and _is_finite(ci_cv[0])
        and _is_finite(ci_cg[0])
        and (ci_sh[0] > 0.0)
        and (ci_cv[0] > 0.0)
        and (ci_cg[0] > 0.0)
        and _is_finite(sw1_point["delta_sharpe_cost_adj"])
        and _is_finite(sw1_point["delta_cvar5"])
        and _is_finite(sw1_point["delta_cagr_proxy"])
        and _is_finite(sw2_point["delta_sharpe_cost_adj"])
        and _is_finite(sw2_point["delta_cvar5"])
        and _is_finite(sw2_point["delta_cagr_proxy"])
        and (sw1_point["delta_sharpe_cost_adj"] > 0.0)
        and (sw1_point["delta_cvar5"] > 0.0)
        and (sw1_point["delta_cagr_proxy"] > 0.0)
        and (sw2_point["delta_sharpe_cost_adj"] > 0.0)
        and (sw2_point["delta_cvar5"] > 0.0)
        and (sw2_point["delta_cagr_proxy"] > 0.0)
    )
    if domina:
        return {"arm": arm_name, "verdict": "DOMINA_FORTE", "reason": "IC95 favoravel em 3 metricas + estabilidade SW1/SW2 + gate operacional"}

    count_pos90 = int(m_sh["mass_pos"] >= 0.90) + int(m_cv["mass_pos"] >= 0.90) + int(m_cg["mass_pos"] >= 0.90)
    count_neg90 = int(m_sh["mass_neg"] >= 0.90) + int(m_cv["mass_neg"] >= 0.90) + int(m_cg["mass_neg"] >= 0.90)
    no_heavy_contra = bool(
        (m_sh["mass_neg"] <= 0.60)
        and (m_cv["mass_neg"] <= 0.60)
        and (m_cg["mass_neg"] <= 0.60)
    )
    material = bool(
        (abs(float(m_sh["point"])) >= sharpe_abs_min)
        or (abs(float(m_cg["point"])) >= cagr_abs_min)
    )

    if gate_ok and count_pos90 >= 2 and no_heavy_contra and material:
        return {"arm": arm_name, "verdict": "FAVORECIDO_ARM", "reason": "massa bootstrap >=90% a favor em >=2 metricas com materialidade e gate operacional"}
    if count_neg90 >= 2 and material:
        return {"arm": arm_name, "verdict": "FAVORECIDO_BASELINE", "reason": "massa bootstrap >=90% contra o arm em >=2 metricas com materialidade"}
    return {"arm": arm_name, "verdict": "INCONCLUSIVO", "reason": "criterios de dominancia/favorecimento nao satisfeitos"}


def _evaluate_late_rocket_verdict(
    mean_diff: float,
    ci95: list[float],
    mass_neg: float,
    mass_pos: float,
    sw1_mean: float,
    sw2_mean: float,
    abs_min: float,
) -> dict[str, Any]:
    domina = (
        _is_finite(ci95[1])
        and (ci95[1] < 0.0)
        and _is_finite(mean_diff)
        and (mean_diff <= -abs_min)
        and _is_finite(sw1_mean)
        and _is_finite(sw2_mean)
        and (sw1_mean < 0.0)
        and (sw2_mean < 0.0)
    )
    if domina:
        return {"verdict": "DOMINA_FORTE", "reason": "IC95 inteiramente negativo + materialidade + sinal negativo em SW1/SW2"}
    if _is_finite(mass_neg) and (mass_neg >= 0.90) and _is_finite(mean_diff) and (abs(mean_diff) >= abs_min):
        return {"verdict": "FAVORECIDO_ARM", "reason": "massa bootstrap >=90% para diff<0 com materialidade"}
    if _is_finite(mass_pos) and (mass_pos >= 0.90) and _is_finite(mean_diff) and (abs(mean_diff) >= abs_min):
        return {"verdict": "FAVORECIDO_BASELINE", "reason": "massa bootstrap >=90% para diff>0 com materialidade"}
    return {"verdict": "INCONCLUSIVO", "reason": "sem evidencias suficientes de dominancia/favorecimento"}


def main() -> None:
    criterion = _load_json(IN_CRITERION)
    if not bool(criterion.get("registered_before_execution", False)):
        raise RuntimeError("pre-registro invalido: registered_before_execution precisa ser true")

    winner_cfg = _load_winner_snapshot(IN_WINNER)
    top_n = int(criterion["runtime_params"]["top_n"])
    cadence = int(criterion["runtime_params"]["rebalance_cadence"])
    anchor_date = pd.Timestamp(criterion["runtime_params"]["rebalance_anchor_date"]).normalize()
    skip_initial_rebalances = int(criterion["runtime_params"]["skip_initial_rebalances"])
    friction_one_way_rate = float(criterion["runtime_params"]["friction_one_way_rate"])
    max_veto_rate = float(criterion["runtime_params"]["max_veto_rate"])
    max_turnover_rate = float(criterion["runtime_params"]["max_turnover_rate"])
    n_bootstrap = int(criterion["runtime_params"]["n_bootstrap"])
    late_rocket_ret62_threshold = float(criterion["runtime_params"]["late_rocket_ret62_threshold"])
    sharpe_abs_min = float(criterion["r048_single_factory_adaptation"]["materialidade"]["sharpe_abs_min"])
    cagr_abs_min = float(criterion["r048_single_factory_adaptation"]["materialidade"]["cagr_abs_min"])
    late_abs_min = float(
        criterion["r048_single_factory_adaptation"]["materialidade"]["late_rocket_mean_diff_abs_min"]
    )

    train_end = pd.Timestamp(criterion["windows"]["train_end"])
    holdout_start = pd.Timestamp(criterion["windows"]["holdout_start"])
    holdout_end = pd.Timestamp(criterion["windows"]["holdout_end"])
    sw1_start = pd.Timestamp(criterion["windows"]["subwindows"]["SW1"]["start"])
    sw1_end = pd.Timestamp(criterion["windows"]["subwindows"]["SW1"]["end"])
    sw2_start = pd.Timestamp(criterion["windows"]["subwindows"]["SW2"]["start"])
    sw2_end = pd.Timestamp(criterion["windows"]["subwindows"]["SW2"]["end"])

    if winner_cfg["top_n"] != top_n:
        print(f"Aviso: top_n do winner ({winner_cfg['top_n']}) difere do criterio ({top_n}). Usando criterio.")
    if winner_cfg["rebalance_cadence"] != cadence:
        print(
            "Aviso: rebalance_cadence do winner "
            f"({winner_cfg['rebalance_cadence']}) difere do criterio ({cadence}). Usando criterio."
        )

    canonical, _, _ = load_inputs()
    canonical = canonical.copy()
    canonical["ticker"] = canonical["ticker"].astype(str).str.upper().str.strip()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical["close_operational"] = pd.to_numeric(canonical["close_operational"], errors="coerce")
    canonical["market_cap"] = pd.to_numeric(canonical["market_cap"], errors="coerce")
    canonical = canonical.dropna(subset=["ticker", "date", "close_operational"]).copy()
    canonical = canonical[canonical["date"] <= holdout_end].copy()

    blacklist = _load_blacklist(IN_BLACKLIST)
    if blacklist:
        canonical = canonical[~canonical["ticker"].isin(blacklist)].copy()

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
        raise RuntimeError("nenhum pregao encontrado")

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
                "Aviso: anchor_date fora do periodo; usando primeira data do dataset "
                f"({anchor_date.date().isoformat()})."
            )
        else:
            anchor_idx = pos
            anchor_date = trading_days[anchor_idx]

    min_rebalances_needed = skip_initial_rebalances + 12
    remaining_days = len(trading_days) - anchor_idx
    if remaining_days // max(cadence, 1) < min_rebalances_needed:
        anchor_idx = 0
        anchor_date = trading_days[0]
        print(
            "Aviso: ancora curta para estudo historico; usando primeira data do dataset "
            f"({anchor_date.date().isoformat()})."
        )

    print("Computando score_m3 por pregao...")
    scores_by_day = compute_m3_scores(px_wide)
    print(f"scores_by_day: {len(scores_by_day)}")

    logret = np.log(px_wide / px_wide.shift(1))
    ret10_wide = logret.rolling(window=10, min_periods=10).sum()
    z_ret10_by_day: dict[pd.Timestamp, pd.Series] = {}
    for day in ret10_wide.index:
        row = ret10_wide.loc[day].dropna()
        if len(row) < 3:
            continue
        z_ret10_by_day[pd.Timestamp(day)] = zscore_cross_section(row)

    mc_eligible_by_day: dict[float, dict[pd.Timestamp, set[str]]] = {
        300_000_000.0: {},
        150_000_000.0: {},
        75_000_000.0: {},
    }
    for d, g in canonical.groupby("date", sort=True):
        tickers = g[["ticker", "market_cap"]].dropna(subset=["ticker", "market_cap"])
        for thr in mc_eligible_by_day:
            mc_eligible_by_day[thr][d] = set(
                tickers.loc[tickers["market_cap"] >= float(thr), "ticker"].astype(str).str.upper().str.strip().tolist()
            )

    observations: list[dict[str, Any]] = []
    ticker_observations: list[dict[str, Any]] = []

    for phase in range(cadence):
        rebalance_days = _phase_rebalance_days(
            trading_days=trading_days,
            anchor_idx=anchor_idx,
            cadence=cadence,
            phase=phase,
        )
        for reb_idx, d_reb in enumerate(rebalance_days):
            if reb_idx < skip_initial_rebalances:
                continue

            d_prev = _prev_day(d_reb, day_to_idx, trading_days)
            if d_prev is None:
                continue
            d_next_reb = rebalance_days[reb_idx + 1] if reb_idx + 1 < len(rebalance_days) else None
            d_prev_next = _prev_day(d_next_reb, day_to_idx, trading_days)
            if d_next_reb is None or d_prev_next is None:
                continue

            split = _to_split(
                day=d_reb,
                train_end=train_end,
                holdout_start=holdout_start,
                holdout_end=holdout_end,
                sw1_start=sw1_start,
                sw1_end=sw1_end,
                sw2_start=sw2_start,
                sw2_end=sw2_end,
            )
            if split == "OTHER":
                continue
            is_holdout = int(split in {"HOLDOUT", "SW1", "SW2"})

            prev_scores = scores_by_day.get(d_prev)
            if prev_scores is None or prev_scores.empty:
                continue

            eligible_300 = mc_eligible_by_day[300_000_000.0].get(d_prev, set())
            base_pool = prev_scores[prev_scores.index.isin(eligible_300)].copy()
            if base_pool.empty:
                continue

            baseline_selected = select_top_n(base_pool, top_n=top_n, blacklist=blacklist)
            if not baseline_selected:
                continue

            idx_start = day_to_idx.get(d_prev)
            idx_end = day_to_idx.get(d_prev_next)
            holding_days = int(idx_end - idx_start) if idx_start is not None and idx_end is not None else 0
            if holding_days <= 0:
                continue

            log_ret_baseline = _basket_log_return(px_wide, d_prev, d_prev_next, baseline_selected)

            baseline_row = {
                "arm": BASELINE_ARM,
                "frente": "baseline",
                "phase": int(phase),
                "date": d_reb.date().isoformat(),
                "d_prev": d_prev.date().isoformat(),
                "d_next_reb": d_next_reb.date().isoformat(),
                "d_prev_next_reb": d_prev_next.date().isoformat(),
                "split": split,
                "is_holdout": is_holdout,
                "holding_days": int(holding_days),
                "top_n": int(top_n),
                "n_veto": 0,
                "n_turnover": 0,
                "n_veto_or_turnover": 0,
                "veto_rate": 0.0,
                "turnover_rate": 0.0,
                "tickers_baseline": ";".join(baseline_selected),
                "tickers_arm": ";".join(baseline_selected),
                "tickers_vetados": "",
                "tickers_substitutos": "",
                "log_ret_baseline": log_ret_baseline,
                "log_ret_arm": log_ret_baseline,
                "log_ret_arm_cost_adj": log_ret_baseline,
                "cost_arm": 0.0,
            }
            observations.append(baseline_row)

            for t in baseline_selected:
                ret_62_t = _safe_float(base_pool.at[t, "ret_62"]) if t in base_pool.index else float("nan")
                late_flag = int(_is_finite(ret_62_t) and (ret_62_t >= late_rocket_ret62_threshold))
                fwd_ret = _ticker_log_return(px_wide, d_prev, d_prev_next, t)
                ticker_observations.append(
                    {
                        "phase": int(phase),
                        "date": d_reb.date().isoformat(),
                        "ticker": t,
                        "ret_62": ret_62_t,
                        "late_rocket": late_flag,
                        "forward_logret": fwd_ret,
                        "split": split,
                    }
                )

            ret10_row = ret10_wide.loc[d_prev] if d_prev in ret10_wide.index else pd.Series(dtype=float)
            ret10_row = pd.to_numeric(ret10_row, errors="coerce")
            blocked = set(
                base_pool.loc[
                    (pd.to_numeric(base_pool["ret_62"], errors="coerce") >= late_rocket_ret62_threshold)
                    & (ret10_row.reindex(base_pool.index).fillna(np.nan) < 0.0)
                ].index.astype(str).str.upper().str.strip().tolist()
            )
            selected_veto = select_top_n(
                base_pool,
                top_n=top_n,
                blacklist=set(blacklist) | blocked,
            )
            if selected_veto:
                blocked_in_base = sorted([t for t in baseline_selected if t in blocked])
                substitutes = sorted(set(selected_veto) - set(baseline_selected))
                n_veto = int(len(blocked_in_base))
                n_turnover = int(len(substitutes))
                n_cost = n_veto
                veto_rate = float(n_veto / top_n) if top_n > 0 else float("nan")
                turnover_rate = float(n_turnover / top_n) if top_n > 0 else float("nan")
                log_ret_arm = _basket_log_return(px_wide, d_prev, d_prev_next, selected_veto)
                cost_arm = float(n_cost * 2.0 * friction_one_way_rate / top_n) if top_n > 0 else 0.0
                log_ret_arm_cost_adj = (
                    float(log_ret_arm - cost_arm) if _is_finite(log_ret_arm) else float("nan")
                )
                observations.append(
                    {
                        "arm": "Arm_RecentSlopeVeto",
                        "frente": "frente1_horizonte_score",
                        "phase": int(phase),
                        "date": d_reb.date().isoformat(),
                        "d_prev": d_prev.date().isoformat(),
                        "d_next_reb": d_next_reb.date().isoformat(),
                        "d_prev_next_reb": d_prev_next.date().isoformat(),
                        "split": split,
                        "is_holdout": is_holdout,
                        "holding_days": int(holding_days),
                        "top_n": int(top_n),
                        "n_veto": n_veto,
                        "n_turnover": n_turnover,
                        "n_veto_or_turnover": n_cost,
                        "veto_rate": veto_rate,
                        "turnover_rate": turnover_rate,
                        "tickers_baseline": ";".join(baseline_selected),
                        "tickers_arm": ";".join(selected_veto),
                        "tickers_vetados": ";".join(blocked_in_base),
                        "tickers_substitutos": ";".join(substitutes),
                        "log_ret_baseline": log_ret_baseline,
                        "log_ret_arm": log_ret_arm,
                        "log_ret_arm_cost_adj": log_ret_arm_cost_adj,
                        "cost_arm": cost_arm,
                    }
                )

            z_ret10 = z_ret10_by_day.get(d_prev)
            if z_ret10 is not None:
                alt_pool = base_pool.copy()
                common = alt_pool.index.intersection(z_ret10.index)
                alt_pool = alt_pool.loc[common].copy()
                if not alt_pool.empty:
                    alt_pool["score_m3"] = (
                        pd.to_numeric(z_ret10.reindex(common), errors="coerce").to_numpy(dtype=float)
                        + pd.to_numeric(alt_pool["z_ret"], errors="coerce").to_numpy(dtype=float)
                        - pd.to_numeric(alt_pool["z_vol"], errors="coerce").to_numpy(dtype=float)
                    )
                    alt_pool = alt_pool.dropna(subset=["score_m3"])
                    selected_alt = select_top_n(alt_pool, top_n=top_n, blacklist=blacklist)
                    if selected_alt:
                        substitutes = sorted(set(selected_alt) - set(baseline_selected))
                        n_turnover = int(len(substitutes))
                        n_cost = n_turnover
                        turnover_rate = float(n_turnover / top_n) if top_n > 0 else float("nan")
                        log_ret_arm = _basket_log_return(px_wide, d_prev, d_prev_next, selected_alt)
                        cost_arm = float(n_cost * 2.0 * friction_one_way_rate / top_n) if top_n > 0 else 0.0
                        log_ret_arm_cost_adj = (
                            float(log_ret_arm - cost_arm) if _is_finite(log_ret_arm) else float("nan")
                        )
                        observations.append(
                            {
                                "arm": "Arm_ShortTermScore",
                                "frente": "frente1_horizonte_score",
                                "phase": int(phase),
                                "date": d_reb.date().isoformat(),
                                "d_prev": d_prev.date().isoformat(),
                                "d_next_reb": d_next_reb.date().isoformat(),
                                "d_prev_next_reb": d_prev_next.date().isoformat(),
                                "split": split,
                                "is_holdout": is_holdout,
                                "holding_days": int(holding_days),
                                "top_n": int(top_n),
                                "n_veto": 0,
                                "n_turnover": n_turnover,
                                "n_veto_or_turnover": n_cost,
                                "veto_rate": 0.0,
                                "turnover_rate": turnover_rate,
                                "tickers_baseline": ";".join(baseline_selected),
                                "tickers_arm": ";".join(selected_alt),
                                "tickers_vetados": "",
                                "tickers_substitutos": ";".join(substitutes),
                                "log_ret_baseline": log_ret_baseline,
                                "log_ret_arm": log_ret_arm,
                                "log_ret_arm_cost_adj": log_ret_arm_cost_adj,
                                "cost_arm": cost_arm,
                            }
                        )

            for arm_name, cap_thr in [("Arm_150M", 150_000_000.0), ("Arm_75M", 75_000_000.0)]:
                eligible_cap = mc_eligible_by_day[cap_thr].get(d_prev, set())
                pool_cap = prev_scores[prev_scores.index.isin(eligible_cap)].copy()
                if pool_cap.empty:
                    continue
                selected_cap = select_top_n(pool_cap, top_n=top_n, blacklist=blacklist)
                if not selected_cap:
                    continue
                substitutes = sorted(set(selected_cap) - set(baseline_selected))
                n_turnover = int(len(substitutes))
                n_cost = n_turnover
                turnover_rate = float(n_turnover / top_n) if top_n > 0 else float("nan")
                log_ret_arm = _basket_log_return(px_wide, d_prev, d_prev_next, selected_cap)
                cost_arm = float(n_cost * 2.0 * friction_one_way_rate / top_n) if top_n > 0 else 0.0
                log_ret_arm_cost_adj = float(log_ret_arm - cost_arm) if _is_finite(log_ret_arm) else float("nan")
                observations.append(
                    {
                        "arm": arm_name,
                        "frente": "frente2_piso_tamanho",
                        "phase": int(phase),
                        "date": d_reb.date().isoformat(),
                        "d_prev": d_prev.date().isoformat(),
                        "d_next_reb": d_next_reb.date().isoformat(),
                        "d_prev_next_reb": d_prev_next.date().isoformat(),
                        "split": split,
                        "is_holdout": is_holdout,
                        "holding_days": int(holding_days),
                        "top_n": int(top_n),
                        "n_veto": 0,
                        "n_turnover": n_turnover,
                        "n_veto_or_turnover": n_cost,
                        "veto_rate": 0.0,
                        "turnover_rate": turnover_rate,
                        "tickers_baseline": ";".join(baseline_selected),
                        "tickers_arm": ";".join(selected_cap),
                        "tickers_vetados": "",
                        "tickers_substitutos": ";".join(substitutes),
                        "log_ret_baseline": log_ret_baseline,
                        "log_ret_arm": log_ret_arm,
                        "log_ret_arm_cost_adj": log_ret_arm_cost_adj,
                        "cost_arm": cost_arm,
                    }
                )

    obs_df = pd.DataFrame(observations)
    if obs_df.empty:
        raise RuntimeError("nenhuma observacao gerada")
    obs_df = obs_df.sort_values(["arm", "phase", "date"]).reset_index(drop=True)

    ticker_obs_df = pd.DataFrame(ticker_observations)
    if ticker_obs_df.empty:
        raise RuntimeError("nenhuma observacao de ticker late_rocket gerada")
    ticker_obs_df = ticker_obs_df.sort_values(["phase", "date", "ticker"]).reset_index(drop=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    obs_df.to_csv(OUT_OBS, index=False)
    ticker_obs_df.to_csv(OUT_OBS_TICKER, index=False)

    subsets = ["HOLDOUT", "SW1", "SW2"]
    summary_rows: list[dict[str, Any]] = []
    for subset in subsets:
        sub = _subset_df(obs_df, subset=subset)
        for arm in [BASELINE_ARM] + ARMS_FRENTE1 + ARMS_FRENTE2:
            g = sub[sub["arm"] == arm].copy()
            m = _compute_metrics(g)
            gate_value = _nanmean(g["veto_rate"] if arm == "Arm_RecentSlopeVeto" else g["turnover_rate"]) if len(g) else float("nan")
            summary_rows.append(
                {
                    "subset": subset,
                    "arm": arm,
                    "n_cycles": int(m["n_cycles"]),
                    "mean_cvar5": m["mean_cvar5"],
                    "mean_sharpe_cost_adj": m["mean_sharpe_cost_adj"],
                    "mean_cagr_proxy": m["mean_cagr_proxy"],
                    "gate_metric_value": gate_value,
                    "gate_metric_name": "veto_rate" if arm == "Arm_RecentSlopeVeto" else "turnover_rate",
                }
            )
    summary_df = pd.DataFrame(summary_rows)

    holdout_base = _subset_df(obs_df, "HOLDOUT")
    holdout_base = holdout_base[holdout_base["arm"] == BASELINE_ARM].copy()
    sw1_base = _subset_df(obs_df, "SW1")
    sw1_base = sw1_base[sw1_base["arm"] == BASELINE_ARM].copy()
    sw2_base = _subset_df(obs_df, "SW2")
    sw2_base = sw2_base[sw2_base["arm"] == BASELINE_ARM].copy()

    arm_bootstrap_payload: dict[str, Any] = {}
    arm_verdict_payload: dict[str, Any] = {}
    for arm in ARMS_FRENTE1 + ARMS_FRENTE2:
        holdout_arm = _subset_df(obs_df, "HOLDOUT")
        holdout_arm = holdout_arm[holdout_arm["arm"] == arm].copy()
        sw1_arm = _subset_df(obs_df, "SW1")
        sw1_arm = sw1_arm[sw1_arm["arm"] == arm].copy()
        sw2_arm = _subset_df(obs_df, "SW2")
        sw2_arm = sw2_arm[sw2_arm["arm"] == arm].copy()

        paired_holdout = _pair_by_cycle(holdout_arm, holdout_base)
        paired_sw1 = _pair_by_cycle(sw1_arm, sw1_base)
        paired_sw2 = _pair_by_cycle(sw2_arm, sw2_base)

        holdout_boot = _bootstrap_pair_deltas(paired_holdout, n_bootstrap=n_bootstrap, seed=42 + len(arm))
        sw1_point = _point_deltas(paired_sw1)
        sw2_point = _point_deltas(paired_sw2)

        holdout_gate_series = holdout_arm["veto_rate"] if arm == "Arm_RecentSlopeVeto" else holdout_arm["turnover_rate"]
        holdout_gate_value = _nanmean(holdout_gate_series)
        holdout_gate_limit = max_veto_rate if arm == "Arm_RecentSlopeVeto" else max_turnover_rate
        gate_ok = bool(_is_finite(holdout_gate_value) and (holdout_gate_value <= holdout_gate_limit))

        verdict = _evaluate_arm_verdict(
            arm_name=arm,
            holdout_boot=holdout_boot,
            sw1_point=sw1_point,
            sw2_point=sw2_point,
            gate_ok=gate_ok,
            sharpe_abs_min=sharpe_abs_min,
            cagr_abs_min=cagr_abs_min,
        )

        arm_bootstrap_payload[arm] = {
            "holdout_bootstrap": holdout_boot,
            "sw1_point_deltas": sw1_point,
            "sw2_point_deltas": sw2_point,
            "holdout_gate_metric_name": "veto_rate" if arm == "Arm_RecentSlopeVeto" else "turnover_rate",
            "holdout_gate_metric_value": holdout_gate_value,
            "holdout_gate_limit": holdout_gate_limit,
            "holdout_gate_ok": gate_ok,
        }
        arm_verdict_payload[arm] = verdict

    tick_holdout = ticker_obs_df[ticker_obs_df["split"].isin(["HOLDOUT", "SW1", "SW2"])].copy()
    tick_holdout["cycle_id"] = tick_holdout["phase"].astype(str) + "|" + tick_holdout["date"].astype(str)
    cycle_rows: list[dict[str, Any]] = []
    for cycle_id, g in tick_holdout.groupby("cycle_id", sort=True):
        late = pd.to_numeric(g.loc[g["late_rocket"] == 1, "forward_logret"], errors="coerce").dropna()
        non = pd.to_numeric(g.loc[g["late_rocket"] == 0, "forward_logret"], errors="coerce").dropna()
        if len(late) == 0 or len(non) == 0:
            continue
        cycle_rows.append(
            {
                "cycle_id": cycle_id,
                "split": str(g["split"].iloc[0]),
                "mean_late": float(late.mean()),
                "mean_non_late": float(non.mean()),
                "diff_late_minus_non": float(late.mean() - non.mean()),
            }
        )
    cycle_df = pd.DataFrame(cycle_rows)
    if cycle_df.empty:
        late_bootstrap = {
            "n_cycles": 0,
            "mean_diff": float("nan"),
            "ci95": [float("nan"), float("nan")],
            "mass_neg": float("nan"),
            "mass_pos": float("nan"),
            "sw1_mean_diff": float("nan"),
            "sw2_mean_diff": float("nan"),
        }
        late_verdict = {"verdict": "INCONCLUSIVO", "reason": "sem ciclos com late_rocket e non_late simultaneos"}
    else:
        diffs = pd.to_numeric(cycle_df["diff_late_minus_non"], errors="coerce").to_numpy(dtype=float)
        diffs = diffs[np.isfinite(diffs)]
        rng = np.random.default_rng(20260716)
        boot_vals: list[float] = []
        n = len(diffs)
        for _ in range(n_bootstrap):
            idx = rng.integers(0, n, size=n)
            boot_vals.append(float(np.mean(diffs[idx])))
        b = np.asarray(boot_vals, dtype=float)
        b = b[np.isfinite(b)]
        mean_diff = float(np.mean(diffs))
        ci95 = [float(np.quantile(b, 0.025)), float(np.quantile(b, 0.975))] if b.size else [float("nan"), float("nan")]
        mass_neg = float(np.mean(b < 0.0)) if b.size else float("nan")
        mass_pos = float(np.mean(b > 0.0)) if b.size else float("nan")
        sw1_mean = _nanmean(cycle_df.loc[cycle_df["split"] == "SW1", "diff_late_minus_non"])
        sw2_mean = _nanmean(cycle_df.loc[cycle_df["split"] == "SW2", "diff_late_minus_non"])
        late_bootstrap = {
            "n_cycles": int(n),
            "mean_diff": mean_diff,
            "ci95": ci95,
            "mass_neg": mass_neg,
            "mass_pos": mass_pos,
            "sw1_mean_diff": sw1_mean,
            "sw2_mean_diff": sw2_mean,
        }
        late_verdict = _evaluate_late_rocket_verdict(
            mean_diff=mean_diff,
            ci95=ci95,
            mass_neg=mass_neg,
            mass_pos=mass_pos,
            sw1_mean=sw1_mean,
            sw2_mean=sw2_mean,
            abs_min=late_abs_min,
        )

    bootstrap_payload = {
        "task_id": TASK_ID,
        "criterion_file": str(IN_CRITERION.relative_to(ROOT)),
        "runtime": {
            "top_n": top_n,
            "rebalance_cadence": cadence,
            "anchor_date_effective": anchor_date.date().isoformat(),
            "skip_initial_rebalances": skip_initial_rebalances,
            "friction_one_way_rate": friction_one_way_rate,
            "n_bootstrap": n_bootstrap,
            "late_rocket_ret62_threshold": late_rocket_ret62_threshold,
            "max_veto_rate": max_veto_rate,
            "max_turnover_rate": max_turnover_rate,
        },
        "arms_bootstrap_vs_baseline_300m": arm_bootstrap_payload,
        "late_rocket_diagnostic": {
            "cycle_diffs_holdout": cycle_df.to_dict(orient="records"),
            "bootstrap_summary": late_bootstrap,
        },
        "summary_by_subset": summary_df.to_dict(orient="records"),
    }
    OUT_BOOTSTRAP.write_text(json.dumps(bootstrap_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    verdict_payload = {
        "task_id": TASK_ID,
        "criterion_file": str(IN_CRITERION.relative_to(ROOT)),
        "fronts": {
            "frente1_horizonte_score": {arm: arm_verdict_payload[arm] for arm in ARMS_FRENTE1},
            "frente2_piso_tamanho": {arm: arm_verdict_payload[arm] for arm in ARMS_FRENTE2},
        },
        "late_rocket_diagnostic": late_verdict,
        "notes": [
            "Estudo read-only. Nenhuma mudanca de motor aplicada.",
            "REPL/SMWB julho/2026 estao fora da amostra quantitativa por limite do dataset congelado.",
            "A comparacao entre fases compartilha historico de precos e nao e totalmente independente.",
            "Dataset congelado nao possui coluna de volume/ADV para teste de liquidez na Frente 2.",
        ],
    }
    OUT_VERDICT.write_text(json.dumps(verdict_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"wrote {OUT_OBS.relative_to(ROOT)}")
    print(f"wrote {OUT_OBS_TICKER.relative_to(ROOT)}")
    print(f"wrote {OUT_BOOTSTRAP.relative_to(ROOT)}")
    print(f"wrote {OUT_VERDICT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
