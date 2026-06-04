from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


# Constantes pré-registradas (não alterar pós-hoc)
DRAWDOWN_THRESHOLDS = [-0.10, -0.125, -0.15, -0.175, -0.20]
SLOPE_THRESHOLDS = [-0.08, -0.10, -0.12]
R035_THRESHOLD = -0.1285
MIN_HOLDING_DAYS = 5
CAL_END = pd.Timestamp("2025-07-31")
VAL_START = pd.Timestamp("2025-08-01")


def _segment_top20_runs(top20: pd.DataFrame, all_days: list[pd.Timestamp]) -> pd.DataFrame:
    day_to_idx = {d: i for i, d in enumerate(all_days)}
    rows: list[dict[str, object]] = []
    for ticker, g in top20.groupby("ticker", sort=False):
        dates = sorted(g["date"].drop_duplicates().tolist())
        if not dates:
            continue
        start = dates[0]
        prev = dates[0]
        run_len = 1
        for d in dates[1:]:
            if day_to_idx[d] == day_to_idx[prev] + 1:
                run_len += 1
                prev = d
            else:
                if run_len >= MIN_HOLDING_DAYS:
                    rows.append(
                        {
                            "ticker": ticker,
                            "entry_date": start,
                            "last_date": prev,
                            "run_len": run_len,
                        }
                    )
                start = d
                prev = d
                run_len = 1
        if run_len >= MIN_HOLDING_DAYS:
            rows.append(
                {
                    "ticker": ticker,
                    "entry_date": start,
                    "last_date": prev,
                    "run_len": run_len,
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["holding_id", "ticker", "entry_date", "last_date", "run_len"])
    out = out.sort_values(["entry_date", "ticker"]).reset_index(drop=True)
    out["holding_id"] = np.arange(1, len(out) + 1)
    return out[["holding_id", "ticker", "entry_date", "last_date", "run_len"]]


def _prepare_holding_timeseries(holdings: pd.DataFrame, opw: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for r in holdings.itertuples(index=False):
        h = opw[(opw["ticker"] == r.ticker) & (opw["date"] >= r.entry_date) & (opw["date"] <= r.last_date)].copy()
        if h.empty:
            continue
        h = h.sort_values("date").reset_index(drop=True)
        h["holding_id"] = r.holding_id
        h["entry_date"] = r.entry_date
        h["last_date"] = r.last_date
        h["run_len"] = r.run_len
        rows.append(h)
    if not rows:
        return pd.DataFrame(
            columns=["holding_id", "ticker", "date", "close_operational", "entry_date", "last_date", "run_len"]
        )
    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["holding_id", "date"]).reset_index(drop=True)
    out["entry_price"] = out.groupby("holding_id")["close_operational"].transform("first")
    out["ret_from_entry"] = out["close_operational"] / out["entry_price"] - 1.0
    out["rolling_max_10"] = (
        out.groupby("holding_id")["close_operational"].rolling(10, min_periods=1).max().reset_index(level=0, drop=True)
    )
    out["drawdown"] = out["close_operational"] / out["rolling_max_10"] - 1.0
    out["slope_5d"] = (
        out.groupby("holding_id")["close_operational"].transform(lambda s: (s - s.shift(5)) / s.shift(5))
    )
    return out


def _baseline_by_holding(ts: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for hid, g in ts.groupby("holding_id", sort=False):
        g = g.sort_values("date").reset_index(drop=True)
        breach = g[g["ret_from_entry"] <= R035_THRESHOLD]
        if breach.empty:
            exit_row = g.iloc[-1]
            baseline_hit = False
        else:
            exit_row = breach.iloc[0]
            baseline_hit = True
        records.append(
            {
                "holding_id": int(hid),
                "ticker": str(g.iloc[0]["ticker"]),
                "entry_date": pd.Timestamp(g.iloc[0]["entry_date"]),
                "entry_price": float(g.iloc[0]["entry_price"]),
                "baseline_hit": bool(baseline_hit),
                "baseline_exit_date": pd.Timestamp(exit_row["date"]),
                "baseline_exit_price": float(exit_row["close_operational"]),
            }
        )
    return pd.DataFrame(records)


def _metrics_for_config(ts: pd.DataFrame, base: pd.DataFrame, dd_thr: float, sl_thr: float) -> pd.DataFrame:
    events: list[dict[str, object]] = []
    for hid, g in ts.groupby("holding_id", sort=False):
        g = g.sort_values("date").reset_index(drop=True)
        signal = g[(g["drawdown"] <= dd_thr) & (g["slope_5d"] <= sl_thr)]
        if signal.empty:
            cand_hit = False
            cand_date = pd.NaT
            cand_price = np.nan
        else:
            s = signal.iloc[0]
            cand_hit = True
            cand_date = pd.Timestamp(s["date"])
            cand_price = float(s["close_operational"])
        events.append(
            {
                "holding_id": int(hid),
                "cand_hit": bool(cand_hit),
                "cand_date": cand_date,
                "cand_price": cand_price,
            }
        )
    ev = pd.DataFrame(events)
    merged = base.merge(ev, on="holding_id", how="left")
    merged["cand_before"] = merged["cand_hit"] & (merged["cand_date"] < merged["baseline_exit_date"])
    merged["cand_before_or_equal"] = merged["cand_hit"] & (merged["cand_date"] <= merged["baseline_exit_date"])
    merged["price_ratio"] = merged["cand_price"] / merged["baseline_exit_price"]
    merged["false_positive"] = merged["cand_hit"] & (~merged["baseline_hit"])
    merged["config_id"] = f"DD{abs(dd_thr):.3f}_SL{abs(sl_thr):.3f}"
    return merged


def _split_stats(df: pd.DataFrame) -> dict[str, float]:
    n_total = int(len(df))
    n_cand = int(df["cand_hit"].sum())
    n_before = int(df["cand_before"].sum())
    n_before_or_equal = int(df["cand_before_or_equal"].sum())
    n_false = int(df["false_positive"].sum())
    improvement = float(df.loc[df["cand_before"], "price_ratio"].mean()) if n_before > 0 else np.nan
    false_pct = float(n_false / n_cand) if n_cand > 0 else np.nan
    coverage = float(n_before_or_equal / n_total) if n_total > 0 else np.nan
    return {
        "n_total": n_total,
        "n_cand": n_cand,
        "n_before": n_before,
        "n_before_or_equal": n_before_or_equal,
        "n_false_positive": n_false,
        "melhoria_preco": improvement,
        "falso_positivo_pct": false_pct,
        "cobertura": coverage,
    }


def _verdict(cal: dict[str, float], val: dict[str, float]) -> str:
    cal_impr = cal["melhoria_preco"]
    val_impr = val["melhoria_preco"]
    cal_fp = cal["falso_positivo_pct"]
    val_fp = val["falso_positivo_pct"]

    cal_pass = np.isfinite(cal_impr) and np.isfinite(cal_fp) and cal_impr > 1.03 and cal_fp < 0.30
    val_pass = np.isfinite(val_impr) and np.isfinite(val_fp) and val_impr > 1.03 and val_fp < 0.30
    if cal_pass and val_pass:
        return "PASS"

    fail_hard = (
        (np.isfinite(cal_impr) and cal_impr <= 1.00)
        or (np.isfinite(val_impr) and val_impr <= 1.00)
        or (np.isfinite(cal_fp) and cal_fp >= 0.50)
        or (np.isfinite(val_fp) and val_fp >= 0.50)
    )
    if fail_hard:
        return "FAIL"

    return "INCONCLUSIVO"


def _sanitize(v: object) -> object:
    if v is pd.NaT:
        return None
    if v is None:
        return None
    if isinstance(v, str):
        return v
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (np.floating, float)):
        return None if not np.isfinite(v) else float(v)
    if isinstance(v, (np.integer, int)):
        return int(v)
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d")
    return v


def _satl_sanity(opw: pd.DataFrame) -> dict[str, object]:
    entry_date = pd.Timestamp("2026-05-18")
    entry_price = 9.7982
    target_candidate_date = pd.Timestamp("2026-06-01")
    target_candidate_price = 8.67

    satl = opw[(opw["ticker"] == "SATL") & (opw["date"] >= entry_date)].copy()
    satl = satl.sort_values("date").reset_index(drop=True)
    if satl.empty:
        return {
            "entry_date": _sanitize(entry_date),
            "entry_price": entry_price,
            "target_candidate_date": _sanitize(target_candidate_date),
            "target_candidate_price": target_candidate_price,
            "baseline_r035_date": None,
            "baseline_r035_price": None,
            "per_config": {},
            "note": "SATL sem dados no recorte.",
        }

    satl["ret_from_entry_const"] = satl["close_operational"] / entry_price - 1.0
    satl["rolling_max_10"] = satl["close_operational"].rolling(10, min_periods=1).max()
    satl["drawdown"] = satl["close_operational"] / satl["rolling_max_10"] - 1.0
    satl["slope_5d"] = (satl["close_operational"] - satl["close_operational"].shift(5)) / satl["close_operational"].shift(5)

    baseline_df = satl[satl["ret_from_entry_const"] <= R035_THRESHOLD]
    if baseline_df.empty:
        baseline_date = pd.NaT
        baseline_price = np.nan
    else:
        b = baseline_df.iloc[0]
        baseline_date = pd.Timestamp(b["date"])
        baseline_price = float(b["close_operational"])

    per_config: dict[str, object] = {}
    for dd in DRAWDOWN_THRESHOLDS:
        for sl in SLOPE_THRESHOLDS:
            cfg_id = f"DD{abs(dd):.3f}_SL{abs(sl):.3f}"
            sig = satl[(satl["drawdown"] <= dd) & (satl["slope_5d"] <= sl)]
            if sig.empty:
                per_config[cfg_id] = {
                    "cand_hit": False,
                    "cand_date": None,
                    "cand_price": None,
                    "cand_before_baseline": False,
                    "cand_on_2026_06_01": False,
                    "baseline_date": _sanitize(baseline_date),
                    "baseline_price": _sanitize(baseline_price),
                }
            else:
                s = sig.iloc[0]
                cand_date = pd.Timestamp(s["date"])
                cand_price = float(s["close_operational"])
                before = bool(pd.notna(baseline_date) and cand_date < baseline_date)
                on_target = bool(cand_date == target_candidate_date)
                per_config[cfg_id] = {
                    "cand_hit": True,
                    "cand_date": _sanitize(cand_date),
                    "cand_price": _sanitize(cand_price),
                    "cand_before_baseline": before,
                    "cand_on_2026_06_01": on_target,
                    "baseline_date": _sanitize(baseline_date),
                    "baseline_price": _sanitize(baseline_price),
                }

    return {
        "entry_date": _sanitize(entry_date),
        "entry_price": entry_price,
        "target_candidate_date": _sanitize(target_candidate_date),
        "target_candidate_price": target_candidate_price,
        "baseline_r035_date": _sanitize(baseline_date),
        "baseline_r035_price": _sanitize(baseline_price),
        "per_config": per_config,
    }


def main() -> None:
    base_dir = Path(__file__).resolve().parents[2]
    out_dir = Path(__file__).resolve().parent
    out_json = out_dir / "resultados_raw.json"

    opw = pd.read_parquet(
        base_dir / "data/ssot/operational_window.parquet",
        columns=["date", "ticker", "close_operational"],
    )
    scores = pd.read_parquet(
        base_dir / "data/features/scores_m3_us.parquet",
        columns=["date", "ticker", "m3_rank"],
    )
    opw["date"] = pd.to_datetime(opw["date"]).dt.normalize()
    scores["date"] = pd.to_datetime(scores["date"]).dt.normalize()
    opw["ticker"] = opw["ticker"].astype(str).str.upper()
    scores["ticker"] = scores["ticker"].astype(str).str.upper()

    top20 = scores[scores["m3_rank"] <= 20][["date", "ticker"]].drop_duplicates()
    all_days = sorted(top20["date"].drop_duplicates().tolist())
    holdings = _segment_top20_runs(top20=top20, all_days=all_days)
    ts = _prepare_holding_timeseries(holdings=holdings, opw=opw)
    base = _baseline_by_holding(ts=ts)

    results: list[dict[str, object]] = []

    print("=== CALIBRACAO EXIT SIGNAL US V1 ===")
    for dd in DRAWDOWN_THRESHOLDS:
        for sl in SLOPE_THRESHOLDS:
            cfg = _metrics_for_config(ts=ts, base=base, dd_thr=dd, sl_thr=sl)
            cal_df = cfg[cfg["entry_date"] <= CAL_END]
            val_df = cfg[cfg["entry_date"] >= VAL_START]
            cal_stats = _split_stats(cal_df)
            val_stats = _split_stats(val_df)
            verdict = _verdict(cal_stats, val_stats)
            cfg_id = cfg["config_id"].iloc[0]
            print(
                f"{cfg_id} | cal_melh={cal_stats['melhoria_preco']:.4f} cal_fp={cal_stats['falso_positivo_pct']:.4f} "
                f"| val_melh={val_stats['melhoria_preco']:.4f} val_fp={val_stats['falso_positivo_pct']:.4f} | {verdict}"
            )
            results.append(
                {
                    "config_id": cfg_id,
                    "drawdown_threshold": dd,
                    "slope_threshold": sl,
                    "calibration": cal_stats,
                    "validation": val_stats,
                    "veredito": verdict,
                }
            )

    best_pass = [r for r in results if r["veredito"] == "PASS"]
    best_config = None
    if best_pass:
        best_config = max(best_pass, key=lambda x: x["validation"]["melhoria_preco"])

    satl_sanity = _satl_sanity(opw=opw)

    payload = {
        "meta": {
            "task_id": "T-SDC-EXIT-SIGNAL-CALIBRATION-US-V1",
            "constants": {
                "DRAWDOWN_THRESHOLDS": DRAWDOWN_THRESHOLDS,
                "SLOPE_THRESHOLDS": SLOPE_THRESHOLDS,
                "R035_THRESHOLD": R035_THRESHOLD,
                "MIN_HOLDING_DAYS": MIN_HOLDING_DAYS,
                "CAL_END": CAL_END.strftime("%Y-%m-%d"),
                "VAL_START": VAL_START.strftime("%Y-%m-%d"),
            },
            "coverage": {
                "opw_date_min": opw["date"].min().strftime("%Y-%m-%d"),
                "opw_date_max": opw["date"].max().strftime("%Y-%m-%d"),
                "scores_date_min": scores["date"].min().strftime("%Y-%m-%d"),
                "scores_date_max": scores["date"].max().strftime("%Y-%m-%d"),
                "holding_segments": int(len(base)),
                "holding_days": int(len(ts)),
            },
        },
        "results": results,
        "best_pass_config": best_config,
        "sanity_check_satl": satl_sanity,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Resultados salvos em {out_json}")


if __name__ == "__main__":
    main()
