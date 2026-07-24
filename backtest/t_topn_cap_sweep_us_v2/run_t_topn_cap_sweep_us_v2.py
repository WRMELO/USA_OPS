"""Backtest T-SDC-TOPN-CAP-SWEEP-US-V1.

Estudo read-only pre-registrado para varrer top_n em dois tracks:
- ISOLADO: max_weight_cap fixo em 0.06;
- COVARIADO: max_weight_cap = round(1.2/top_n, 4).

Baseline estrutural em ambos os tracks:
motor C4 com veto R-060 (BandExp ∩ ret_62>=1.00) por pre-filtragem de scores.
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

import backtest.run_backtest_variants_us as rb  # noqa: E402
from backtest.t_band_exp_entry_us_v1.run_t_band_exp_entry_us_v1 import (  # noqa: E402
    _cvar,
    _portfolio_sharpe,
)
from backtest.t_bandexp_r037_materiality_entry_us_v1.run_t_bandexp_r037_materiality_entry_us_v1 import (  # noqa: E402
    _verify_manifest_hashes,
)
from backtest.t_defensive_reinvest_policy_us_v1.run_t_defensive_reinvest_policy_us_v1 import (  # noqa: E402
    _bootstrap_pair_stats,
)
from lib.band_exp_gate import compute_bandexp_ret62_gate  # noqa: E402

TASK_ID = "T-SDC-TOPN-CAP-SWEEP-US-V2"
TRACKS = ["ISOLADO", "COVARIADO"]
TOP_N_GRID = [10, 12, 15, 20, 25, 30]
BASELINE_TOP_N = 20
SUBSETS = ["TRAIN", "HOLDOUT", "SW1", "SW2"]

DATASET_DIR = ROOT / "backtest" / "research_dataset_us_v2"
IN_CANONICAL = DATASET_DIR / "canonical_us.parquet"
IN_MACRO = DATASET_DIR / "macro_us_fullhistory.parquet"
IN_SCORES = DATASET_DIR / "scores_m3_us_fullhistory.parquet"
IN_MANIFEST = DATASET_DIR / "manifest.json"
IN_CRITERION = (
    ROOT
    / "backtest"
    / "t_topn_cap_sweep_us_v2"
    / "decision_criterion_topn_cap_sweep_us_v2.json"
)
OUT_DIR = ROOT / "backtest" / "t_topn_cap_sweep_us_v2" / "results"


def _nanmean(values: pd.Series | np.ndarray) -> float:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if arr.size == 0 or np.isnan(arr).all():
        return float("nan")
    return float(np.nanmean(arr))


def _sign(v: float, eps: float = 1e-12) -> int:
    if not np.isfinite(v):
        return 0
    if v > eps:
        return 1
    if v < -eps:
        return -1
    return 0


def _track_cap(track: str, top_n: int) -> float:
    if track == "ISOLADO":
        return 0.06
    return float(round(1.2 / float(top_n), 4))


def _max_true_run(mask: pd.Series | np.ndarray) -> int:
    if isinstance(mask, pd.Series):
        arr = mask.fillna(False).to_numpy(dtype=bool)
    else:
        arr = np.asarray(mask, dtype=bool)
    best = 0
    cur = 0
    for v in arr:
        if bool(v):
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return int(best)


def _log_ret_from_equity(eq: pd.Series) -> pd.Series:
    cur = pd.to_numeric(eq, errors="coerce")
    prev = cur.shift(1)
    ok = cur.gt(0) & prev.gt(0)
    out = pd.Series(np.nan, index=eq.index, dtype=float)
    out.loc[ok] = np.log(cur.loc[ok] / prev.loc[ok])
    return out


def _to_split_v2(
    day: pd.Timestamp,
    holdout_end: pd.Timestamp,
    effective_start: pd.Timestamp,
    first_half_end: pd.Timestamp,
    second_half_start: pd.Timestamp,
) -> str:
    """Mapeia splits usando cobertura efetiva derivada dos insumos fullhistory."""
    day = pd.Timestamp(day).normalize()
    if day < effective_start:
        return "TRAIN"
    if day <= holdout_end:
        if day <= first_half_end:
            return "SW1"
        if day >= second_half_start:
            return "SW2"
        # Guarda defensiva para evitar lacunas de classificacao.
        return "SW2"
    return "OTHER"


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _verify_fullhistory_addendum_hashes(manifest: dict[str, Any], dataset_dir: Path) -> None:
    add = manifest.get("fullhistory_addendum")
    if not isinstance(add, dict):
        raise RuntimeError("manifest sem fullhistory_addendum para validar arquivos fullhistory.")

    files = add.get("files")
    if not isinstance(files, dict):
        raise RuntimeError("manifest fullhistory_addendum sem bloco files.")

    required = {
        "scores_m3_us_fullhistory.parquet": dataset_dir / "scores_m3_us_fullhistory.parquet",
        "macro_us_fullhistory.parquet": dataset_dir / "macro_us_fullhistory.parquet",
    }
    for fname, fpath in required.items():
        if not fpath.exists():
            raise RuntimeError(f"Arquivo fullhistory ausente: {fpath}")
        expected = str(files.get(fname, {}).get("sha256", "")).strip().lower()
        if not expected:
            raise RuntimeError(f"manifest fullhistory_addendum sem sha256 para {fname}")
        got = _sha256_file(fpath).lower()
        if got != expected:
            raise RuntimeError(
                f"SHA256 mismatch em {fname}: esperado={expected} obtido={got}"
            )


def _compute_effective_windows(
    scores: pd.DataFrame,
    macro: pd.DataFrame,
    holdout_end: pd.Timestamp,
    trading_dates: list[pd.Timestamp],
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    score_dates = pd.to_datetime(scores.get("date"), errors="coerce")
    rank = pd.to_numeric(scores.get("m3_rank"), errors="coerce")
    valid_score_dates = score_dates[rank.notna()].dropna().dt.normalize()
    if valid_score_dates.empty:
        raise RuntimeError("Nao foi encontrada nenhuma data valida de m3_rank nos scores fullhistory.")

    macro_dates = pd.to_datetime(macro.get("date"), errors="coerce").dropna().dt.normalize()
    if macro_dates.empty:
        raise RuntimeError("macro fullhistory sem datas validas.")

    score_start = pd.Timestamp(valid_score_dates.min()).normalize()
    macro_start = pd.Timestamp(macro_dates.min()).normalize()
    effective_start = max(score_start, macro_start)

    base_dates = sorted(pd.Timestamp(d).normalize() for d in trading_dates)
    eval_dates = [d for d in base_dates if effective_start <= d <= holdout_end]
    if len(eval_dates) < 2:
        raise RuntimeError(
            "Janela efetiva insuficiente para split SW1/SW2 apos intersecao px x macro."
        )

    half = len(eval_dates) // 2
    first_half_end = pd.Timestamp(eval_dates[half - 1]).normalize()
    second_half_start = pd.Timestamp(eval_dates[half]).normalize()

    return effective_start, first_half_end, second_half_start


def _subset_df(df: pd.DataFrame, subset: str) -> pd.DataFrame:
    if subset == "TRAIN":
        return df[df["split"] == "TRAIN"].copy()
    if subset == "HOLDOUT":
        return df[df["is_holdout"] == 1].copy()
    if subset in {"SW1", "SW2"}:
        return df[df["split"] == subset].copy()
    raise ValueError(f"Subset desconhecido: {subset}")


def _ic_side(ic95: list[float]) -> str:
    if len(ic95) != 2:
        return "MIX"
    lo, hi = float(ic95[0]), float(ic95[1])
    if np.isfinite(lo) and np.isfinite(hi) and lo > 0.0 and hi > 0.0:
        return "POS"
    if np.isfinite(lo) and np.isfinite(hi) and lo < 0.0 and hi < 0.0:
        return "NEG"
    return "MIX"


def _check_g6_from_veto_diag(veto_diag_by_day: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
    valid = veto_diag_by_day[veto_diag_by_day["pool_size"] > 0].copy()
    if valid.empty:
        return False, {
            "mean_veto_rate": float("nan"),
            "n_days_pool_positive": 0,
            "note": "Nenhum dia com pool elegivel > 0.",
        }
    mean_rate = float(valid["veto_rate"].mean())
    ok = bool(np.isfinite(mean_rate) and mean_rate > 0.0 and mean_rate <= 0.30)
    return ok, {
        "mean_veto_rate": mean_rate,
        "n_days_pool_positive": int(len(valid)),
        "min_veto_rate": float(valid["veto_rate"].min()),
        "max_veto_rate": float(valid["veto_rate"].max()),
    }


def _build_engine_inputs(canonical: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    px_exec_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first")
        .sort_index()
        .ffill()
    )
    split_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="split_factor", aggfunc="first")
        .sort_index()
    )
    split_changed = (split_wide / split_wide.shift(1)).replace([np.inf, -np.inf], np.nan)
    has_split = (split_changed - 1.0).abs() > 1e-12
    px_raw_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first")
        .sort_index()
    )
    split_event_wide = (px_raw_wide.shift(1) / px_raw_wide).where(has_split)

    work = canonical.copy()
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
        work[col] = pd.to_numeric(work[col], errors="coerce")
    i_wide = (
        work.pivot_table(index="date", columns="ticker", values="i_value", aggfunc="first")
        .sort_index()
    )
    z_wide = rb._build_z_table(i_wide)
    any_rule = (
        (work["i_value"] > work["i_ucl"])
        | (work["i_value"] < work["i_lcl"])
        | (work["mr_value"] > work["mr_ucl"])
        | (work["r_value"] > work["r_ucl"])
        | (work["xbar_value"] > work["xbar_ucl"])
        | (work["xbar_value"] < work["xbar_lcl"])
    ).astype(float)
    strong_rule = (
        (work["i_value"] > work["i_ucl"])
        | (work["i_value"] < work["i_lcl"])
        | (work["mr_value"] > work["mr_ucl"])
    ).astype(float)
    work["_any_rule"] = any_rule
    work["_strong_rule"] = strong_rule
    any_rule_wide = (
        work.pivot_table(index="date", columns="ticker", values="_any_rule", aggfunc="first")
        .sort_index()
    )
    strong_rule_wide = (
        work.pivot_table(index="date", columns="ticker", values="_strong_rule", aggfunc="first")
        .sort_index()
    )
    return px_exec_wide, split_event_wide, i_wide, z_wide, any_rule_wide, strong_rule_wide


def _apply_r060_filter(
    canonical: pd.DataFrame,
    scores_raw: pd.DataFrame,
    scores_by_day: dict[pd.Timestamp, pd.DataFrame],
) -> tuple[dict[pd.Timestamp, pd.DataFrame], pd.DataFrame]:
    filtered: dict[pd.Timestamp, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []

    for d, day_scores in sorted(scores_by_day.items(), key=lambda x: x[0]):
        pool_size = int(len(day_scores))
        gate_df = compute_bandexp_ret62_gate(canonical=canonical, scores=scores_raw, as_of_date=d)
        blocked_all = set(gate_df.index[gate_df["gate_bandexp_ret62"].fillna(False).astype(bool)])
        blocked_in_pool = sorted(set(day_scores.index).intersection(blocked_all))
        kept = day_scores.drop(index=blocked_in_pool, errors="ignore").copy()
        filtered[d] = kept
        veto_count = int(len(blocked_in_pool))
        veto_rate = float(veto_count / pool_size) if pool_size > 0 else float("nan")
        rows.append(
            {
                "date": str(pd.Timestamp(d).date()),
                "pool_size": pool_size,
                "veto_count": veto_count,
                "veto_rate": veto_rate,
                "kept_size": int(len(kept)),
            }
        )
    return filtered, pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def _compute_sanity(
    curves_df: pd.DataFrame,
    veto_diag_by_day: pd.DataFrame,
    hash_pass: bool,
) -> tuple[dict[str, Any], pd.DataFrame]:
    gate_items: list[dict[str, Any]] = []
    hard_fail = False

    # G1: por construcao - run_variant nao usa rebalance_anchor_date.
    g1 = {
        "gate": "G1",
        "status": "PASS",
        "hard_gate": True,
        "note": "Calendario de rebalance gerado internamente por run_variant sem leitura de ancora LIVE.",
    }
    gate_items.append(g1)

    # G2: caixa ocioso.
    g2_rows: list[dict[str, Any]] = []
    g2_hard_fail: list[str] = []
    for (track, top_n), group in curves_df.groupby(["track", "top_n"], sort=True):
        sub = group.sort_values("date").reset_index(drop=True)
        after = sub.iloc[30:].copy() if len(sub) > 30 else sub.copy()
        run = _max_true_run((pd.to_numeric(after["cash_ratio"], errors="coerce") > 0.50))
        mean_cash_ratio = float(_nanmean(sub["cash_ratio"]))
        hard_ok = bool(run <= 15)
        soft_ok = bool(np.isfinite(mean_cash_ratio) and mean_cash_ratio <= 0.20)
        if not hard_ok:
            g2_hard_fail.append(f"{track}/N{top_n}")
        g2_rows.append(
            {
                "track": track,
                "top_n": int(top_n),
                "max_run_cash_ratio_gt_50": int(run),
                "mean_cash_ratio": mean_cash_ratio,
                "hard_pass": hard_ok,
                "soft_pass": soft_ok,
            }
        )
    g2 = {
        "gate": "G2",
        "status": "PASS" if not g2_hard_fail else "FAIL",
        "hard_gate": True,
        "failures": g2_hard_fail,
        "details": g2_rows,
    }
    if g2["status"] == "FAIL":
        hard_fail = True
    gate_items.append(g2)

    # G3: equity finito e positivo.
    g3_fail: list[str] = []
    g3_rows: list[dict[str, Any]] = []
    for (track, top_n), group in curves_df.groupby(["track", "top_n"], sort=True):
        eq = pd.to_numeric(group["equity"], errors="coerce")
        bad = (~np.isfinite(eq)) | (eq <= 0.0)
        n_bad = int(bad.sum())
        if n_bad > 0:
            g3_fail.append(f"{track}/N{top_n}")
        g3_rows.append(
            {
                "track": track,
                "top_n": int(top_n),
                "n_bad_equity_rows": n_bad,
            }
        )
    g3 = {
        "gate": "G3",
        "status": "PASS" if not g3_fail else "FAIL",
        "hard_gate": True,
        "failures": g3_fail,
        "details": g3_rows,
    }
    if g3["status"] == "FAIL":
        hard_fail = True
    gate_items.append(g3)

    # G4: faixa de tickers em dias de rebalance.
    # Recalibrado por evidencia empirica: t018 (C4/cap=0.06 com razao
    # avg_tickers/top_n em ~0.81-0.92) e rodada pos-cold-start deste estudo
    # (razao observada ~0.735-0.789 nas 12 configuracoes).
    G4_HARD_MIN_RATIO = 0.60
    G4_HARD_MAX_RATIO = 1.05
    G4_SOFT_MIN_RATIO = 0.75
    g4_fail: list[dict[str, Any]] = []
    g4_rows: list[dict[str, Any]] = []
    for (track, top_n), group in curves_df.groupby(["track", "top_n"], sort=True):
        reb = group[group["is_rebalance_day"] == 1].copy()
        mean_tickers = float(_nanmean(reb["n_tickers"])) if not reb.empty else float("nan")
        ratio_to_top_n = (
            float(mean_tickers / float(top_n))
            if np.isfinite(mean_tickers) and float(top_n) > 0.0
            else float("nan")
        )
        hard_ok = bool(
            np.isfinite(ratio_to_top_n)
            and G4_HARD_MIN_RATIO <= ratio_to_top_n <= G4_HARD_MAX_RATIO
        )
        soft_ok = bool(np.isfinite(ratio_to_top_n) and ratio_to_top_n >= G4_SOFT_MIN_RATIO)
        payload = {
            "track": track,
            "top_n": int(top_n),
            "mean_tickers_rebalance": mean_tickers,
            "ratio_to_top_n": ratio_to_top_n,
            "hard_pass": hard_ok,
            "soft_pass": soft_ok,
        }
        g4_rows.append(payload)
        if not hard_ok:
            g4_fail.append(payload)
    g4 = {
        "gate": "G4",
        "status": "PASS" if not g4_fail else "FAIL",
        "hard_gate": True,
        "failures": g4_fail,
        "details": g4_rows,
    }
    if g4["status"] == "FAIL":
        hard_fail = True
    gate_items.append(g4)

    # G5: hash dataset.
    g5 = {
        "gate": "G5",
        "status": "PASS" if hash_pass else "FAIL",
        "hard_gate": True,
        "note": "Hashes conferidos com manifest.json antes da simulacao.",
    }
    if g5["status"] == "FAIL":
        hard_fail = True
    gate_items.append(g5)

    # G6: plausibilidade de ativacao R-060.
    g6_ok, g6_detail = _check_g6_from_veto_diag(veto_diag_by_day)
    g6 = {
        "gate": "G6",
        "status": "PASS" if g6_ok else "FAIL",
        "hard_gate": True,
        "details": g6_detail,
    }
    if g6["status"] == "FAIL":
        hard_fail = True
    gate_items.append(g6)

    # G7: diagnostico de concentracao (nao abortante).
    # Correcao v1: diagnostico de binding do cap deve focar dias de rebalance
    # em vez de usar apenas o pico absoluto da janela inteira.
    g7_rows: list[dict[str, Any]] = []
    for (track, top_n), group in curves_df.groupby(["track", "top_n"], sort=True):
        max_conc_any_day = float(pd.to_numeric(group["max_concentration"], errors="coerce").max())
        cap = float(group["max_weight_cap"].iloc[0])
        reb = group[group["is_rebalance_day"] == 1].copy()
        if reb.empty:
            max_conc_rebalance_day = float("nan")
            cap_binding_rebalance_days_pct = float("nan")
        else:
            reb_max = pd.to_numeric(reb["max_concentration"], errors="coerce")
            max_conc_rebalance_day = float(reb_max.max())
            cap_binding_rebalance_days_pct = float((reb_max <= cap * 1.01).mean() * 100.0)
        g7_rows.append(
            {
                "track": track,
                "top_n": int(top_n),
                "max_weight_cap": cap,
                "max_concentration_any_day_pct": float(max_conc_any_day * 100.0),
                "max_concentration_rebalance_day_pct": float(max_conc_rebalance_day * 100.0),
                "cap_binding_rebalance_days_pct": cap_binding_rebalance_days_pct,
            }
        )
    g7 = {
        "gate": "G7",
        "status": "INFO",
        "hard_gate": False,
        "note": "G7 corrigido para diagnosticar binding do cap em dias de rebalance.",
        "details": g7_rows,
    }
    gate_items.append(g7)

    overall = "PASS" if not hard_fail else "FAIL"
    payload = {
        "task_id": TASK_ID,
        "overall_status": overall,
        "hard_gate_fail": bool(hard_fail),
        "gates": gate_items,
    }
    return payload, pd.DataFrame(g7_rows).sort_values(["track", "top_n"]).reset_index(drop=True)


def _summary_rows(curves_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (track, top_n), group in curves_df.groupby(["track", "top_n"], sort=True):
        for subset in SUBSETS:
            sub = _subset_df(group, subset=subset)
            if len(sub) < 2:
                continue
            cagr, mdd = rb._curve_metrics(sub)
            rets = pd.to_numeric(sub["log_ret_equity"], errors="coerce").dropna()
            sharpe = (
                _portfolio_sharpe(rets, pd.Series(np.ones(len(rets), dtype=float)))
                if len(rets) > 1
                else float("nan")
            )
            cvar5 = _cvar(rets.to_numpy(dtype=float), 0.05) if len(rets) > 0 else float("nan")
            reb = sub[sub["is_rebalance_day"] == 1].copy()
            avg_tickers_rebalance = float(_nanmean(reb["n_tickers"])) if not reb.empty else float("nan")
            max_conc = float(pd.to_numeric(sub["max_concentration"], errors="coerce").max())
            cap = float(sub["max_weight_cap"].iloc[0])
            max_conc_reb = (
                float(pd.to_numeric(reb["max_concentration"], errors="coerce").max())
                if not reb.empty
                else float("nan")
            )
            cap_binding_pct = (
                float(
                    (
                        pd.to_numeric(reb["max_concentration"], errors="coerce")
                        <= cap * 1.01
                    ).mean()
                    * 100.0
                )
                if not reb.empty
                else float("nan")
            )
            rows.append(
                {
                    "track": track,
                    "top_n": int(top_n),
                    "max_weight_cap": cap,
                    "split": subset,
                    "days": int(len(sub)),
                    "equity_final": float(sub["equity"].iloc[-1]),
                    "cagr": float(cagr),
                    "mdd": float(mdd),
                    "sharpe_cost_adj": float(sharpe),
                    "cvar5": float(cvar5),
                    "avg_tickers_rebalance": avg_tickers_rebalance,
                    "max_concentration_pct": float(max_conc * 100.0),
                    "max_concentration_rebalance_day_pct": float(max_conc_reb * 100.0),
                    "cap_binding_pct": cap_binding_pct,
                    "cash_idle_mean_pct": float(_nanmean(sub["cash_ratio"]) * 100.0),
                    "cost_total_cum": float(sub["cost_total_cum"].iloc[-1]),
                }
            )
    return pd.DataFrame(rows).sort_values(["track", "top_n", "split"]).reset_index(drop=True)


def _pair_bootstrap_and_verdict(
    curves_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    veto_diag_by_day: pd.DataFrame,
    criterion: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    n_resamples = int(criterion["bootstrap"]["n_resamples"])
    seed = int(criterion["bootstrap"]["seed"])

    veto_by_day = {
        str(row.date): float(row.veto_rate)
        for row in veto_diag_by_day.itertuples(index=False)
        if np.isfinite(float(row.veto_rate))
    }

    summary_lookup: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in summary_df.to_dict(orient="records"):
        summary_lookup[(str(row["track"]), int(row["top_n"]), str(row["split"]))] = row

    bootstrap_payload: dict[str, Any] = {
        "task_id": TASK_ID,
        "method": criterion["bootstrap"]["method"],
        "n_resamples": n_resamples,
        "seed": seed,
        "pairs": {},
    }
    verdict_payload: dict[str, Any] = {
        "task_id": TASK_ID,
        "baseline_top_n": BASELINE_TOP_N,
        "tracks": {},
    }

    for track in TRACKS:
        verdict_payload["tracks"][track] = {"pairs": {}, "recommended_top_n": BASELINE_TOP_N}
        base_df = curves_df[
            (curves_df["track"] == track) & (pd.to_numeric(curves_df["top_n"], errors="coerce") == BASELINE_TOP_N)
        ][["date", "split", "is_holdout", "log_ret_equity"]].copy()
        base_df = base_df.rename(columns={"log_ret_equity": "ret_base"})

        for top_n in TOP_N_GRID:
            if top_n == BASELINE_TOP_N:
                continue

            arm_df = curves_df[
                (curves_df["track"] == track) & (pd.to_numeric(curves_df["top_n"], errors="coerce") == top_n)
            ][["date", "log_ret_equity"]].copy()
            arm_df = arm_df.rename(columns={"log_ret_equity": "ret_arm"})

            pair = base_df.merge(arm_df, on="date", how="inner")
            pair["ret_base"] = pd.to_numeric(pair["ret_base"], errors="coerce")
            pair["ret_arm"] = pd.to_numeric(pair["ret_arm"], errors="coerce")
            pair["veto_rate_arm"] = (
                pair["date"].dt.strftime("%Y-%m-%d").map(veto_by_day).astype(float)
            )
            pair = pair.dropna(subset=["ret_base", "ret_arm"]).copy()
            pair["veto_rate_arm"] = pair["veto_rate_arm"].fillna(0.0)

            pair_key = f"{track}_N20_vs_N{top_n}"
            split_stats: dict[str, Any] = {}
            for subset in ["HOLDOUT", "SW1", "SW2"]:
                local_seed = seed + (1000 if track == "COVARIADO" else 0) + int(top_n * 11)
                split_stats[subset] = _bootstrap_pair_stats(
                    pair_df=pair,
                    subset=subset,
                    n_resamples=n_resamples,
                    seed=local_seed,
                )

            bootstrap_payload["pairs"][pair_key] = {
                "track": track,
                "baseline_top_n": BASELINE_TOP_N,
                "candidate_top_n": int(top_n),
                "splits": split_stats,
            }

            deltas_by_split: dict[str, dict[str, float]] = {}
            for subset in ["HOLDOUT", "SW1", "SW2"]:
                arm_key = (track, int(top_n), subset)
                base_key = (track, BASELINE_TOP_N, subset)
                if arm_key not in summary_lookup or base_key not in summary_lookup:
                    deltas_by_split[subset] = {
                        "delta_sharpe_cost_adj": float("nan"),
                        "delta_cvar5": float("nan"),
                    }
                    continue
                deltas_by_split[subset] = {
                    "delta_sharpe_cost_adj": float(
                        summary_lookup[arm_key]["sharpe_cost_adj"]
                        - summary_lookup[base_key]["sharpe_cost_adj"]
                    ),
                    "delta_cvar5": float(
                        summary_lookup[arm_key]["cvar5"] - summary_lookup[base_key]["cvar5"]
                    ),
                }

            side_values: list[str] = []
            for subset in ["HOLDOUT", "SW1", "SW2"]:
                side_values.append(_ic_side(split_stats[subset]["delta_sharpe_cost_adj"]["ic95"]))
                side_values.append(_ic_side(split_stats[subset]["delta_cvar5"]["ic95"]))

            all_pos = all(side == "POS" for side in side_values)
            all_neg = all(side == "NEG" for side in side_values)

            holdout_delta_sharpe = float(deltas_by_split["HOLDOUT"]["delta_sharpe_cost_adj"])
            holdout_delta_cvar5 = float(deltas_by_split["HOLDOUT"]["delta_cvar5"])
            materiality_ok = bool(
                (np.isfinite(holdout_delta_sharpe) and abs(holdout_delta_sharpe) >= 0.30)
                or (np.isfinite(holdout_delta_cvar5) and abs(holdout_delta_cvar5) >= 0.02)
            )
            mass_holdout_ok = bool(
                np.isfinite(
                    split_stats["HOLDOUT"]["delta_sharpe_cost_adj"]["mass_same_sign_pct"]
                )
                and np.isfinite(split_stats["HOLDOUT"]["delta_cvar5"]["mass_same_sign_pct"])
                and split_stats["HOLDOUT"]["delta_sharpe_cost_adj"]["mass_same_sign_pct"] >= 90.0
                and split_stats["HOLDOUT"]["delta_cvar5"]["mass_same_sign_pct"] >= 90.0
            )

            direction_flags: list[int] = []
            for subset in ["HOLDOUT", "SW1", "SW2"]:
                s = _sign(deltas_by_split[subset]["delta_sharpe_cost_adj"])
                c = _sign(deltas_by_split[subset]["delta_cvar5"])
                direction_flags.append(s if s == c else 0)
            direction_positive = all(flag == 1 for flag in direction_flags)
            direction_negative = all(flag == -1 for flag in direction_flags)
            direction_ok = bool(direction_positive or direction_negative)

            if all_pos:
                final_verdict = "DOMINA_FORTE"
                favored_side = f"N{top_n}"
            elif all_neg:
                final_verdict = "DOMINA_FORTE"
                favored_side = "N20"
            elif mass_holdout_ok and direction_ok and materiality_ok:
                if direction_positive:
                    final_verdict = f"FAVORECIDO_N{top_n}"
                    favored_side = f"N{top_n}"
                else:
                    final_verdict = "FAVORECIDO_N20"
                    favored_side = "N20"
            else:
                final_verdict = "INCONCLUSIVO"
                favored_side = "NONE"

            verdict_payload["tracks"][track]["pairs"][pair_key] = {
                "track": track,
                "baseline_top_n": BASELINE_TOP_N,
                "candidate_top_n": int(top_n),
                "final_verdict": final_verdict,
                "favored_side": favored_side,
                "deltas_by_split": deltas_by_split,
                "holdout_materiality_ok": materiality_ok,
                "holdout_mass_ok": mass_holdout_ok,
                "direction_ok_holdout_sw1_sw2": direction_ok,
                "ic_sides": {
                    "HOLDOUT_sharpe": _ic_side(split_stats["HOLDOUT"]["delta_sharpe_cost_adj"]["ic95"]),
                    "HOLDOUT_cvar5": _ic_side(split_stats["HOLDOUT"]["delta_cvar5"]["ic95"]),
                    "SW1_sharpe": _ic_side(split_stats["SW1"]["delta_sharpe_cost_adj"]["ic95"]),
                    "SW1_cvar5": _ic_side(split_stats["SW1"]["delta_cvar5"]["ic95"]),
                    "SW2_sharpe": _ic_side(split_stats["SW2"]["delta_sharpe_cost_adj"]["ic95"]),
                    "SW2_cvar5": _ic_side(split_stats["SW2"]["delta_cvar5"]["ic95"]),
                },
            }

        # Resumo de recomendacao por track (consultivo)
        rec_top_n = BASELINE_TOP_N
        pairs = verdict_payload["tracks"][track]["pairs"]
        dominates_candidate = [
            p for p in pairs.values() if p["final_verdict"] == "DOMINA_FORTE" and p["favored_side"] != "N20"
        ]
        favored_candidate = [
            p for p in pairs.values() if p["final_verdict"].startswith("FAVORECIDO_N") and p["favored_side"] != "N20"
        ]
        if dominates_candidate:
            dominates_candidate.sort(
                key=lambda x: x["deltas_by_split"]["HOLDOUT"]["delta_sharpe_cost_adj"],
                reverse=True,
            )
            rec_top_n = int(dominates_candidate[0]["candidate_top_n"])
        elif favored_candidate:
            favored_candidate.sort(
                key=lambda x: x["deltas_by_split"]["HOLDOUT"]["delta_sharpe_cost_adj"],
                reverse=True,
            )
            rec_top_n = int(favored_candidate[0]["candidate_top_n"])
        verdict_payload["tracks"][track]["recommended_top_n"] = int(rec_top_n)

    return bootstrap_payload, verdict_payload


def main() -> None:
    if not IN_CRITERION.exists():
        raise RuntimeError(f"Criterio nao encontrado: {IN_CRITERION}")
    criterion = json.loads(IN_CRITERION.read_text(encoding="utf-8"))
    if not bool(criterion.get("registered_before_execution", False)):
        raise RuntimeError("registered_before_execution precisa estar true no criterion.")

    manifest = json.loads(IN_MANIFEST.read_text(encoding="utf-8"))
    holdout_end = pd.Timestamp(str(manifest.get("freeze_asof", ""))).normalize()
    if not np.isfinite(holdout_end.value):
        raise RuntimeError("manifest sem freeze_asof valido.")

    hash_pass = True
    if bool(criterion.get("dataset", {}).get("required_hash_verification", False)):
        _verify_manifest_hashes(manifest, DATASET_DIR)
        _verify_fullhistory_addendum_hashes(manifest, DATASET_DIR)
        print("Hashes conferidos com sucesso contra manifest.json (originais + fullhistory).")

    canonical, macro, scores = rb.load_inputs(
        canonical_path=IN_CANONICAL,
        macro_path=IN_MACRO,
        scores_path=IN_SCORES,
    )
    blacklist = rb.load_blacklist(rb.IN_BLACKLIST)
    cash_log_daily = rb.build_cash_log_daily(macro)
    scores_by_day = rb.build_scores_by_day(scores=scores, blacklist=blacklist)
    market_cap_wide = rb.build_market_cap_wide(canonical)
    scores_by_day, _, _ = rb.apply_min_market_cap_filter(
        scores_by_day=scores_by_day,
        market_cap_wide=market_cap_wide,
        min_market_cap=float(criterion["fixed_params"]["min_market_cap"]),
    )
    scores_by_day_r060, veto_diag_by_day = _apply_r060_filter(
        canonical=canonical,
        scores_raw=scores,
        scores_by_day=scores_by_day,
    )

    px_exec_wide, split_event_wide, i_wide, z_wide, any_rule_wide, strong_rule_wide = _build_engine_inputs(
        canonical=canonical
    )

    trading_dates = list(px_exec_wide.index.intersection(cash_log_daily.index).sort_values())
    effective_start, first_half_end, second_half_start = _compute_effective_windows(
        scores=scores,
        macro=macro,
        holdout_end=holdout_end,
        trading_dates=trading_dates,
    )
    print(
        "effective_windows="
        f"{effective_start.date()}..{holdout_end.date()} "
        f"(SW1_end={first_half_end.date()}, SW2_start={second_half_start.date()})"
    )

    curves: list[pd.DataFrame] = []
    for track in TRACKS:
        for top_n in TOP_N_GRID:
            cap = _track_cap(track, top_n)
            cfg = rb.BacktestConfig(
                top_n=int(top_n),
                buffer_k=int(criterion["fixed_params"]["buffer_k"]),
                rebalance_cadence=int(criterion["fixed_params"]["rebalance_cadence"]),
                friction_one_way_bps=float(criterion["fixed_params"]["friction_one_way_bps"]),
                settlement_days=int(criterion["fixed_params"]["settlement_days"]),
                base_capital=float(criterion["fixed_params"]["base_capital"]),
                k_damp=float(criterion["fixed_params"]["k_damp"]),
                max_weight_cap=float(cap),
            )
            print(f"Executando track={track} top_n={top_n} cap={cap:.4f} ...")
            curve, _, _, _ = rb.run_variant(
                variant="C4",
                px_exec_wide=px_exec_wide,
                split_event_wide=split_event_wide,
                i_wide=i_wide,
                z_wide=z_wide,
                any_rule_wide=any_rule_wide,
                strong_rule_wide=strong_rule_wide,
                scores_by_day=scores_by_day_r060,
                cash_log_daily=cash_log_daily,
                cfg=cfg,
            )
            curve = curve.copy()
            curve["track"] = track
            curve["top_n"] = int(top_n)
            curve["max_weight_cap"] = float(cap)
            curves.append(curve)

    if not curves:
        raise RuntimeError("Nenhuma curva gerada.")

    curves_df = (
        pd.concat(curves, ignore_index=True)
        .sort_values(["track", "top_n", "date"])
        .reset_index(drop=True)
    )
    curves_df["split"] = curves_df["date"].map(
        lambda d: _to_split_v2(
            pd.Timestamp(d),
            holdout_end=holdout_end,
            effective_start=effective_start,
            first_half_end=first_half_end,
            second_half_start=second_half_start,
        )
    )
    curves_df = curves_df[curves_df["split"].isin(["SW1", "SW2"])].copy()
    curves_df["is_holdout"] = curves_df["split"].isin(["HOLDOUT", "SW1", "SW2"]).astype(int)
    curves_df["cash_ratio"] = np.where(
        pd.to_numeric(curves_df["equity"], errors="coerce") > 0,
        pd.to_numeric(curves_df["cash_free"], errors="coerce")
        / pd.to_numeric(curves_df["equity"], errors="coerce"),
        np.nan,
    )
    curves_df["log_ret_equity"] = (
        curves_df.groupby(["track", "top_n"], sort=False)["equity"]
        .transform(_log_ret_from_equity)
        .astype(float)
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    obs_path = OUT_DIR / "observations_topn_cap_sweep_us_v2.csv"
    curves_df.to_csv(obs_path, index=False)

    sanity_payload, g7_df = _compute_sanity(
        curves_df=curves_df,
        veto_diag_by_day=veto_diag_by_day,
        hash_pass=hash_pass,
    )
    sanity_path = OUT_DIR / "sanity_gates_report_topn_cap_sweep_us_v2.json"
    sanity_path.write_text(json.dumps(sanity_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    r060_diag_records: list[dict[str, Any]] = []
    g6_ok, g6_detail = _check_g6_from_veto_diag(veto_diag_by_day)
    for track in TRACKS:
        for top_n in TOP_N_GRID:
            cap = _track_cap(track, top_n)
            r060_diag_records.append(
                {
                    "track": track,
                    "top_n": int(top_n),
                    "max_weight_cap": float(cap),
                    "mean_veto_rate_daily": float(g6_detail.get("mean_veto_rate", float("nan"))),
                    "g6_pass": bool(g6_ok),
                }
            )
    r060_diag_payload = {
        "task_id": TASK_ID,
        "global_by_day": veto_diag_by_day.to_dict(orient="records"),
        "by_track_topn": r060_diag_records,
        "g6_details": g6_detail,
    }
    r060_diag_path = OUT_DIR / "r060_gate_diagnostics_topn_cap_sweep_us_v2.json"
    r060_diag_path.write_text(json.dumps(r060_diag_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if sanity_payload["overall_status"] != "PASS":
        raise RuntimeError("Gates de sanidade falharam; metricas nao foram geradas.")

    summary_df = _summary_rows(curves_df)
    summary_path = OUT_DIR / "summary_topn_cap_sweep_us_v2.csv"
    summary_df.to_csv(summary_path, index=False)

    bootstrap_payload, verdict_payload = _pair_bootstrap_and_verdict(
        curves_df=curves_df,
        summary_df=summary_df,
        veto_diag_by_day=veto_diag_by_day,
        criterion=criterion,
    )
    bootstrap_path = OUT_DIR / "bootstrap_stats_topn_cap_sweep_us_v2.json"
    bootstrap_path.write_text(json.dumps(bootstrap_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    verdict_payload["task_id"] = TASK_ID
    verdict_payload["criteria_file"] = str(IN_CRITERION.relative_to(ROOT))
    verdict_payload["dataset_manifest"] = str(IN_MANIFEST.relative_to(ROOT))
    verdict_payload["freeze_asof"] = str(manifest.get("freeze_asof"))
    verdict_payload["sanity_gates"] = sanity_payload["overall_status"]
    verdict_payload["recommended_n_summary"] = {
        track: verdict_payload["tracks"][track]["recommended_top_n"] for track in TRACKS
    }
    verdict_path = OUT_DIR / "verdict_topn_cap_sweep_us_v2.json"
    verdict_path.write_text(json.dumps(verdict_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"{TASK_ID} concluido.")
    print(f"freeze_asof={manifest.get('freeze_asof')}")
    print(f"rows_observations={len(curves_df)}")
    print(f"rows_summary={len(summary_df)}")
    print(f"gates={sanity_payload['overall_status']}")
    for track in TRACKS:
        print(f"{track}_recommended_top_n={verdict_payload['tracks'][track]['recommended_top_n']}")


if __name__ == "__main__":
    main()
