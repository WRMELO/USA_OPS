from __future__ import annotations

import json
import math
import zlib
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd


# Pre-registered constants (do not change post-hoc)
H1_RET62 = 0.80
H2_RUN = 6
H3_DRAWDOWN = -0.10
H4_STRICT = 0.0
HORIZON = 5
BOOTSTRAP_N = 2000
SEED = 42
CAL_END = pd.Timestamp("2025-07-31")
VAL_START = pd.Timestamp("2025-08-01")


@dataclass
class SegmentStats:
    n_signal: int
    n_control: int
    mean_signal: float
    mean_control: float
    ci95_signal: tuple[float, float]
    ci95_control: tuple[float, float]
    p_value_one_sided: float


def make_rng(tag: str) -> np.random.Generator:
    seed = (SEED + zlib.adler32(tag.encode("utf-8"))) % (2**32)
    return np.random.default_rng(seed)


def consecutive_run(mask: pd.Series) -> pd.Series:
    values = mask.fillna(False).to_numpy(dtype=bool)
    out = np.zeros(len(values), dtype=np.int32)
    run = 0
    for i, val in enumerate(values):
        if val:
            run += 1
        else:
            run = 0
        out[i] = run
    return pd.Series(out, index=mask.index)


def bootstrap_ci_mean(values: np.ndarray, n_iter: int, rng: np.random.Generator) -> tuple[float, float]:
    if values.size == 0:
        return (float("nan"), float("nan"))
    samples = np.empty(n_iter, dtype=np.float64)
    n = values.size
    for i in range(n_iter):
        idx = rng.integers(0, n, size=n)
        samples[i] = float(values[idx].mean())
    low, high = np.percentile(samples, [2.5, 97.5])
    return (float(low), float(high))


def permutation_p_one_sided_less(
    signal_values: np.ndarray, control_values: np.ndarray, n_iter: int, rng: np.random.Generator
) -> float:
    if signal_values.size == 0 or control_values.size == 0:
        return float("nan")
    observed = float(signal_values.mean() - control_values.mean())
    combined = np.concatenate([signal_values, control_values])
    n_sig = signal_values.size
    count = 0
    for _ in range(n_iter):
        perm = combined[rng.permutation(combined.size)]
        diff = float(perm[:n_sig].mean() - perm[n_sig:].mean())
        if diff <= observed:
            count += 1
    return float((count + 1) / (n_iter + 1))


def segment_stats(df: pd.DataFrame, signal_col: str, tag: str) -> SegmentStats:
    work = df[[signal_col, "forward_5d_ret"]].dropna()
    if work.empty:
        return SegmentStats(0, 0, float("nan"), float("nan"), (float("nan"), float("nan")), (float("nan"), float("nan")), float("nan"))
    signal_values = work.loc[work[signal_col], "forward_5d_ret"].to_numpy(dtype=np.float64)
    control_values = work.loc[~work[signal_col], "forward_5d_ret"].to_numpy(dtype=np.float64)
    if signal_values.size == 0 or control_values.size == 0:
        return SegmentStats(
            int(signal_values.size),
            int(control_values.size),
            float(signal_values.mean()) if signal_values.size else float("nan"),
            float(control_values.mean()) if control_values.size else float("nan"),
            (float("nan"), float("nan")),
            (float("nan"), float("nan")),
            float("nan"),
        )
    rng_sig = make_rng(f"{tag}:sig")
    rng_ctrl = make_rng(f"{tag}:ctrl")
    rng_perm = make_rng(f"{tag}:perm")
    ci_sig = bootstrap_ci_mean(signal_values, BOOTSTRAP_N, rng_sig)
    ci_ctrl = bootstrap_ci_mean(control_values, BOOTSTRAP_N, rng_ctrl)
    p_val = permutation_p_one_sided_less(signal_values, control_values, BOOTSTRAP_N, rng_perm)
    return SegmentStats(
        n_signal=int(signal_values.size),
        n_control=int(control_values.size),
        mean_signal=float(signal_values.mean()),
        mean_control=float(control_values.mean()),
        ci95_signal=ci_sig,
        ci95_control=ci_ctrl,
        p_value_one_sided=p_val,
    )


def verdict(cal: SegmentStats, val: SegmentStats) -> str:
    if (
        cal.n_signal == 0
        or cal.n_control == 0
        or val.n_signal == 0
        or val.n_control == 0
        or not np.isfinite(cal.mean_signal)
        or not np.isfinite(cal.mean_control)
        or not np.isfinite(val.mean_signal)
        or not np.isfinite(val.mean_control)
    ):
        return "INCONCLUSIVO"

    cal_pass = cal.mean_signal < cal.mean_control and np.isfinite(cal.p_value_one_sided) and cal.p_value_one_sided < 0.10
    val_pass = (
        val.mean_signal < val.mean_control
        and np.isfinite(val.p_value_one_sided)
        and val.p_value_one_sided < 0.10
        and val.mean_signal < 0.0
    )

    if val_pass:
        return "PASS"
    if cal_pass and not val_pass:
        return "INCONCLUSIVO"
    if val.mean_signal < val.mean_control:
        return "INCONCLUSIVO"
    return "FAIL"


def case_checks(opw: pd.DataFrame, universe: pd.DataFrame) -> dict[str, object]:
    out: dict[str, object] = {}

    # SATL: would H2 trigger before 2026-06-03?
    satl = opw[opw["ticker"] == "SATL"].sort_values("date").copy()
    satl["center_line_calc"] = (satl["i_ucl"] + satl["i_lcl"]) / 2.0
    satl["below_center"] = satl["i_value"] < satl["center_line_calc"]
    satl["run_below_center"] = consecutive_run(satl["below_center"])
    satl_trigger = satl[satl["run_below_center"] >= H2_RUN]
    satl_first_trigger = satl_trigger["date"].min() if not satl_trigger.empty else pd.NaT
    satl_run_on_0603 = satl.loc[satl["date"] == pd.Timestamp("2026-06-03"), "run_below_center"]
    out["SATL_H2"] = {
        "first_trigger_date_run_ge_6": satl_first_trigger.strftime("%Y-%m-%d") if pd.notna(satl_first_trigger) else None,
        "run_length_on_2026_06_03": int(satl_run_on_0603.iloc[0]) if not satl_run_on_0603.empty else None,
        "trigger_before_2026_06_03": bool(pd.notna(satl_first_trigger) and satl_first_trigger < pd.Timestamp("2026-06-03")),
    }

    # FCEL: H1 on 2026-06-02
    fcel_row = universe[(universe["ticker"] == "FCEL") & (universe["date"] == pd.Timestamp("2026-06-02"))]
    if not fcel_row.empty:
        r = fcel_row.iloc[0]
        out["FCEL_H1"] = {
            "date": "2026-06-02",
            "ret_62_log": float(r["ret_62"]),
            "amplitude": float(r["amplitude"]),
            "amplitude_larga": bool(r["amplitude_larga"]),
            "signal_h1": bool(r["signal_h1"]),
        }
    else:
        out["FCEL_H1"] = {"date": "2026-06-02", "found_in_universe": False}

    return out


def main() -> None:
    base_dir = Path(__file__).resolve().parents[2]
    task_dir = Path(__file__).resolve().parent
    results_json = task_dir / "resultados_raw.json"

    opw = pd.read_parquet(
        base_dir / "data/ssot/operational_window.parquet",
        columns=["date", "ticker", "close_operational", "i_value", "i_ucl", "i_lcl"],
    )
    scores = pd.read_parquet(
        base_dir / "data/features/scores_m3_us.parquet",
        columns=["date", "ticker", "m3_rank", "score_m3"],
    )

    opw["date"] = pd.to_datetime(opw["date"]).dt.normalize()
    scores["date"] = pd.to_datetime(scores["date"]).dt.normalize()
    opw["ticker"] = opw["ticker"].astype(str).str.upper()
    scores["ticker"] = scores["ticker"].astype(str).str.upper()

    opw = opw.sort_values(["ticker", "date"]).reset_index(drop=True)
    opw["center_line_calc"] = (opw["i_ucl"] + opw["i_lcl"]) / 2.0
    opw["amplitude"] = opw["i_ucl"] - opw["i_lcl"]
    opw["prev_close_62"] = opw.groupby("ticker")["close_operational"].shift(62)
    opw["ret_62"] = np.log(opw["close_operational"] / opw["prev_close_62"])
    opw["next_close_h"] = opw.groupby("ticker")["close_operational"].shift(-HORIZON)
    opw["forward_5d_ret"] = np.log(opw["next_close_h"] / opw["close_operational"])
    opw["below_center"] = opw["i_value"] < opw["center_line_calc"]
    opw["run_below_center"] = opw.groupby("ticker", group_keys=False)["below_center"].apply(consecutive_run)
    opw["rolling_max_10"] = (
        opw.groupby("ticker")["close_operational"].rolling(10, min_periods=1).max().reset_index(level=0, drop=True)
    )
    opw["drawdown_10"] = opw["close_operational"] / opw["rolling_max_10"] - 1.0

    top20 = scores.loc[scores["m3_rank"] <= 20, ["date", "ticker", "m3_rank", "score_m3"]].drop_duplicates()
    universe = top20.merge(
        opw[
            [
                "date",
                "ticker",
                "close_operational",
                "i_value",
                "i_ucl",
                "i_lcl",
                "center_line_calc",
                "amplitude",
                "ret_62",
                "run_below_center",
                "drawdown_10",
                "forward_5d_ret",
            ]
        ],
        on=["date", "ticker"],
        how="left",
    )

    q667 = universe.groupby("date")["amplitude"].quantile(2.0 / 3.0).rename("amp_q667").reset_index()
    universe = universe.merge(q667, on="date", how="left")
    universe["amplitude_larga"] = universe["amplitude"] >= universe["amp_q667"]

    first_dates = top20.groupby("ticker")["date"].min().rename("entry_date").reset_index()
    entry_close = first_dates.merge(
        opw[["date", "ticker", "close_operational"]],
        left_on=["entry_date", "ticker"],
        right_on=["date", "ticker"],
        how="left",
    )[["ticker", "entry_date", "close_operational"]].rename(columns={"close_operational": "entry_close"})
    universe = universe.merge(first_dates, on="ticker", how="left")
    universe = universe.merge(entry_close, on=["ticker", "entry_date"], how="left")
    universe["ret_since_entry"] = universe["close_operational"] / universe["entry_close"] - 1.0

    # Pre-registered signals
    universe["signal_h1"] = (universe["ret_62"] >= H1_RET62) & (universe["amplitude_larga"])
    universe["signal_h2"] = universe["run_below_center"] >= H2_RUN
    universe["signal_h3"] = universe["drawdown_10"] < H3_DRAWDOWN
    universe["signal_h4"] = universe["amplitude_larga"] & (universe["ret_since_entry"] < H4_STRICT)

    cal = universe[universe["date"] <= CAL_END].copy()
    val = universe[universe["date"] >= VAL_START].copy()

    hypotheses = {
        "H1_exaustao_parabolica_sem_persistencia": "signal_h1",
        "H2_run_downside_carta_i": "signal_h2",
        "H3_drawdown_desde_pico_local": "signal_h3",
        "H4_amplitude_larga_mais_negativo_desde_ignicao": "signal_h4",
    }

    results: dict[str, dict[str, object]] = {}
    print("=== CALIBRACAO PREVENTIVE ALERT US V1 ===")
    for name, signal_col in hypotheses.items():
        cal_stats = segment_stats(cal, signal_col, f"{name}:cal")
        val_stats = segment_stats(val, signal_col, f"{name}:val")
        status = verdict(cal_stats, val_stats)
        results[name] = {
            "signal_column": signal_col,
            "calibration": asdict(cal_stats),
            "validation": asdict(val_stats),
            "veredito": status,
        }
        print(f"\n{name}")
        print(f"  CAL: n_sig={cal_stats.n_signal} n_ctrl={cal_stats.n_control} mean_sig={cal_stats.mean_signal:.6f} mean_ctrl={cal_stats.mean_control:.6f} p={cal_stats.p_value_one_sided:.4f}")
        print(f"  VAL: n_sig={val_stats.n_signal} n_ctrl={val_stats.n_control} mean_sig={val_stats.mean_signal:.6f} mean_ctrl={val_stats.mean_control:.6f} p={val_stats.p_value_one_sided:.4f}")
        print(f"  VEREDITO: {status}")

    checks = case_checks(opw=opw, universe=universe)

    payload = {
        "meta": {
            "task_id": "T-SDC-SATL-FCEL-PREVENTIVE-ALERT-CALIBRATION-US-V1",
            "constants": {
                "H1_RET62": H1_RET62,
                "H2_RUN": H2_RUN,
                "H3_DRAWDOWN": H3_DRAWDOWN,
                "H4_STRICT": H4_STRICT,
                "HORIZON": HORIZON,
                "BOOTSTRAP_N": BOOTSTRAP_N,
                "SEED": SEED,
                "CAL_END": CAL_END.strftime("%Y-%m-%d"),
                "VAL_START": VAL_START.strftime("%Y-%m-%d"),
            },
        },
        "coverage": {
            "opw_date_min": opw["date"].min().strftime("%Y-%m-%d"),
            "opw_date_max": opw["date"].max().strftime("%Y-%m-%d"),
            "scores_date_min": scores["date"].min().strftime("%Y-%m-%d"),
            "scores_date_max": scores["date"].max().strftime("%Y-%m-%d"),
            "universe_rows": int(len(universe)),
            "calibration_rows": int(len(cal)),
            "validation_rows": int(len(val)),
        },
        "hypotheses": results,
        "case_checks": checks,
    }

    results_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nResultados salvos em: {results_json}")


if __name__ == "__main__":
    main()
