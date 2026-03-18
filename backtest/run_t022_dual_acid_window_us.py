#!/usr/bin/env python3
"""T-022: Dual acid window US (SP500 + proxy broad index via FRED)."""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from lib.adapters import FredAdapter
from run_backtest_variants_us import (
    BacktestConfig,
    IN_BLACKLIST,
    IN_CANONICAL,
    IN_MACRO,
    IN_SCORES,
    OUT_DIR,
    _build_z_table,
    _curve_metrics,
    _sha256,
    apply_min_market_cap_filter,
    build_cash_log_daily,
    build_market_cap_wide,
    build_scores_by_day,
    load_blacklist,
    load_inputs,
    run_variant,
)

ROOT = Path(__file__).resolve().parents[1]
LABELS_PATH = ROOT / "data" / "features" / "labels_us.parquet"
HOLDOUT_START = pd.Timestamp("2023-01-02")
HOLDOUT_END = pd.Timestamp("2026-03-16")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-022 dual acid window analysis")
    parser.add_argument("--r1000-series-id", default="RU1000PR")
    parser.add_argument("--min-window-days", type=int, default=126)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--buffer-k", type=int, default=10)
    parser.add_argument("--rebalance-cadence", type=int, default=10)
    parser.add_argument("--friction-bps", type=float, default=2.5)
    parser.add_argument("--settlement-days", type=int, default=1)
    parser.add_argument("--base-capital", type=float, default=100_000.0)
    parser.add_argument("--k-damp", type=float, default=0.0)
    parser.add_argument("--max-weight-cap", type=float, default=0.06)
    parser.add_argument("--min-market-cap", type=float, default=300_000_000.0)
    parser.add_argument("--out-json", default=str(OUT_DIR / "acid_analysis_us.json"))
    return parser.parse_args()


def _window_metrics(series: pd.Series) -> dict[str, float]:
    ser = pd.to_numeric(series, errors="coerce").dropna()
    if len(ser) < 2:
        return {"return_total_pct": 0.0, "cagr_pct": 0.0, "mdd_pct": 0.0, "days": int(len(ser))}
    ret_total = (float(ser.iloc[-1]) / float(ser.iloc[0]) - 1.0) * 100.0
    years = max(float(len(ser)) / 252.0, 1.0 / 252.0)
    cagr = ((float(ser.iloc[-1]) / float(ser.iloc[0])) ** (1.0 / years) - 1.0) * 100.0
    running_max = ser.cummax().replace(0.0, np.nan)
    dd = (ser / running_max) - 1.0
    mdd = float(dd.min()) * 100.0 if dd.notna().any() else 0.0
    return {"return_total_pct": ret_total, "cagr_pct": cagr, "mdd_pct": mdd, "days": int(len(ser))}


def _pick_acid_window(price: pd.Series, min_days: int) -> dict[str, object] | None:
    p = pd.to_numeric(price, errors="coerce").dropna().astype(float)
    if len(p) < 2:
        return None

    running_max = p.cummax()
    dd = p / running_max - 1.0
    candidates: list[dict[str, object]] = []
    for trough_date, dd_val in dd.sort_values().items():
        if not np.isfinite(dd_val):
            continue
        hist = p.loc[:trough_date]
        if hist.empty:
            continue
        peak_date = hist.idxmax()
        peak_px = float(p.loc[peak_date])
        fwd = p.loc[trough_date:]
        rec = fwd[fwd >= peak_px]
        natural_end = rec.index[0] if len(rec) > 0 else p.index[-1]
        start_pos = int(p.index.get_loc(peak_date))
        min_end_pos = start_pos + int(min_days) - 1
        if min_end_pos >= len(p.index):
            continue
        min_end_date = p.index[min_end_pos]
        end_date = max(natural_end, min_end_date)
        span = p.loc[peak_date:end_date]
        days = int(len(span))
        candidates.append(
            {
                "peak_date": str(pd.Timestamp(peak_date).date()),
                "trough_date": str(pd.Timestamp(trough_date).date()),
                "recovery_or_end_date": str(pd.Timestamp(end_date).date()),
                "dd_pct": float(dd_val) * 100.0,
                "window_days": days,
            }
        )
    if not candidates:
        return None
    candidates = sorted(candidates, key=lambda x: x["dd_pct"])
    return candidates[0]


def _load_sp500_from_labels() -> pd.Series:
    if not LABELS_PATH.exists():
        raise FileNotFoundError(f"Arquivo ausente: {LABELS_PATH}")
    labels = pd.read_parquet(LABELS_PATH)[["date", "sp500_close"]].copy()
    labels["date"] = pd.to_datetime(labels["date"], errors="coerce").dt.normalize()
    labels["sp500_close"] = pd.to_numeric(labels["sp500_close"], errors="coerce")
    labels = labels.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last").sort_values("date")
    return pd.Series(labels["sp500_close"].values, index=labels["date"], name="sp500_close")


def _load_proxy_from_fred(series_id: str) -> pd.Series:
    fred = FredAdapter(timeout_seconds=30.0, max_retries=5)
    df = fred.fetch_series(series_id, "r1000_proxy_close")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["r1000_proxy_close"] = pd.to_numeric(df["r1000_proxy_close"], errors="coerce")
    df = df.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last").sort_values("date")
    return pd.Series(df["r1000_proxy_close"].values, index=df["date"], name="r1000_proxy_close")


def _build_backtest_inputs(min_market_cap: float):
    canonical, macro, scores = load_inputs()
    blacklist = load_blacklist(IN_BLACKLIST)
    cash_log_daily = build_cash_log_daily(macro)
    scores_by_day = build_scores_by_day(scores=scores, blacklist=blacklist)
    market_cap_wide = build_market_cap_wide(canonical)
    scores_by_day, median_pre_filter, median_post_filter = apply_min_market_cap_filter(
        scores_by_day=scores_by_day,
        market_cap_wide=market_cap_wide,
        min_market_cap=float(min_market_cap),
    )

    px_exec_wide = (
        canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first")
        .sort_index()
        .ffill()
    )
    split_wide = canonical.pivot_table(index="date", columns="ticker", values="split_factor", aggfunc="first").sort_index()
    split_changed = (split_wide / split_wide.shift(1)).replace([np.inf, -np.inf], np.nan)
    has_split = (split_changed - 1.0).abs() > 1e-12
    px_raw_wide = canonical.pivot_table(index="date", columns="ticker", values="close_raw", aggfunc="first").sort_index()
    split_event_wide = (px_raw_wide.shift(1) / px_raw_wide).where(has_split)

    for col in ["i_value", "i_ucl", "i_lcl", "mr_value", "mr_ucl", "xbar_value", "xbar_ucl", "xbar_lcl", "r_value", "r_ucl"]:
        canonical[col] = pd.to_numeric(canonical[col], errors="coerce")
    i_wide = canonical.pivot_table(index="date", columns="ticker", values="i_value", aggfunc="first").sort_index()
    z_wide = _build_z_table(i_wide)
    any_rule = (
        (canonical["i_value"] > canonical["i_ucl"])
        | (canonical["i_value"] < canonical["i_lcl"])
        | (canonical["mr_value"] > canonical["mr_ucl"])
        | (canonical["r_value"] > canonical["r_ucl"])
        | (canonical["xbar_value"] > canonical["xbar_ucl"])
        | (canonical["xbar_value"] < canonical["xbar_lcl"])
    ).astype(float)
    strong_rule = (
        (canonical["i_value"] > canonical["i_ucl"])
        | (canonical["i_value"] < canonical["i_lcl"])
        | (canonical["mr_value"] > canonical["mr_ucl"])
    ).astype(float)
    canonical["_any_rule"] = any_rule
    canonical["_strong_rule"] = strong_rule
    any_rule_wide = canonical.pivot_table(index="date", columns="ticker", values="_any_rule", aggfunc="first").sort_index()
    strong_rule_wide = canonical.pivot_table(index="date", columns="ticker", values="_strong_rule", aggfunc="first").sort_index()
    return (
        px_exec_wide,
        split_event_wide,
        i_wide,
        z_wide,
        any_rule_wide,
        strong_rule_wide,
        scores_by_day,
        cash_log_daily,
        float(median_pre_filter),
        float(median_post_filter),
    )


def _plot_window(df: pd.DataFrame, w: dict[str, object], title: str, out_path: Path) -> None:
    w_start = pd.Timestamp(str(w["peak_date"]))
    w_end = pd.Timestamp(str(w["recovery_or_end_date"]))
    base = df.loc[df["date"] >= w_start].head(1)
    if base.empty:
        return
    b_sp = float(base["sp500_close"].iloc[0])
    b_r1 = float(base["r1000_proxy_close"].iloc[0])
    b_c4 = float(base["equity_c4"].iloc[0])
    b_c2 = float(base["equity_c2"].iloc[0])

    plot = df.copy()
    plot["sp500_base100"] = (plot["sp500_close"] / b_sp) * 100.0
    plot["r1000_base100"] = (plot["r1000_proxy_close"] / b_r1) * 100.0
    plot["c4_base100"] = (plot["equity_c4"] / b_c4) * 100.0
    plot["c2_base100"] = (plot["equity_c2"] / b_c2) * 100.0

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=plot["date"], y=plot["sp500_base100"], name="SP500 (Base100)"))
    fig.add_trace(go.Scatter(x=plot["date"], y=plot["r1000_base100"], name="R1000 Proxy FRED (Base100)"))
    fig.add_trace(go.Scatter(x=plot["date"], y=plot["c4_base100"], name="Motor C4 (Base100)"))
    fig.add_trace(go.Scatter(x=plot["date"], y=plot["c2_base100"], name="Motor C2 (Base100)"))
    fig.add_vrect(
        x0=w_start,
        x1=w_end,
        fillcolor="LightSalmon",
        opacity=0.25,
        layer="below",
        line_width=0,
        annotation_text="Acid window",
        annotation_position="top left",
    )
    fig.update_layout(title=title, xaxis_title="Date", yaxis_title="Base 100")
    fig.write_html(str(out_path), include_plotlyjs="cdn")


def main() -> int:
    args = parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    required_inputs_exist = all(p.exists() for p in [IN_CANONICAL, IN_MACRO, IN_SCORES, IN_BLACKLIST, LABELS_PATH])

    sp500_all = _load_sp500_from_labels()
    r1000_all = _load_proxy_from_fred(series_id=args.r1000_series_id)

    holdout_dates = pd.date_range(HOLDOUT_START, HOLDOUT_END, freq="B")
    bench = pd.DataFrame({"date": holdout_dates})
    bench["sp500_close"] = bench["date"].map(sp500_all)
    bench["r1000_proxy_close"] = bench["date"].map(r1000_all)
    bench["sp500_close"] = pd.to_numeric(bench["sp500_close"], errors="coerce").ffill()
    bench["r1000_proxy_close"] = pd.to_numeric(bench["r1000_proxy_close"], errors="coerce").ffill()
    bench = bench.dropna(subset=["sp500_close", "r1000_proxy_close"]).reset_index(drop=True)

    series_sp500_non_empty = not bench["sp500_close"].dropna().empty
    series_r1000_non_empty = not bench["r1000_proxy_close"].dropna().empty

    if not series_sp500_non_empty:
        raise RuntimeError("T-022 FAIL: série SP500 vazia no HOLDOUT")
    if not series_r1000_non_empty:
        raise RuntimeError(
            f"T-022 FAIL: série proxy R1000 vazia no HOLDOUT para id={args.r1000_series_id}. "
            "Use --r1000-series-id com série FRED válida."
        )

    w_sp500 = _pick_acid_window(bench.set_index("date")["sp500_close"], min_days=int(args.min_window_days))
    w_r1000 = _pick_acid_window(bench.set_index("date")["r1000_proxy_close"], min_days=int(args.min_window_days))
    acid_windows_found = bool(w_sp500 is not None and w_r1000 is not None)
    if not acid_windows_found:
        raise RuntimeError(
            f"T-022 FAIL: não encontrou acid window com duração>={args.min_window_days} dias para as duas séries."
        )

    (
        px_exec_wide,
        split_event_wide,
        i_wide,
        z_wide,
        any_rule_wide,
        strong_rule_wide,
        scores_by_day,
        cash_log_daily,
        median_pre_filter,
        median_post_filter,
    ) = _build_backtest_inputs(min_market_cap=float(args.min_market_cap))

    cfg_c4 = BacktestConfig(
        top_n=int(args.top_n),
        buffer_k=int(args.buffer_k),
        rebalance_cadence=int(args.rebalance_cadence),
        friction_one_way_bps=float(args.friction_bps),
        settlement_days=int(args.settlement_days),
        base_capital=float(args.base_capital),
        k_damp=float(args.k_damp),
        max_weight_cap=float(args.max_weight_cap),
    )
    cfg_c2 = BacktestConfig(
        top_n=int(args.top_n),
        buffer_k=int(args.buffer_k),
        rebalance_cadence=int(args.rebalance_cadence),
        friction_one_way_bps=float(args.friction_bps),
        settlement_days=int(args.settlement_days),
        base_capital=float(args.base_capital),
        k_damp=0.0,
        max_weight_cap=1.0,
    )

    curve_c4, _, _, _ = run_variant(
        variant="C4",
        px_exec_wide=px_exec_wide,
        split_event_wide=split_event_wide,
        i_wide=i_wide,
        z_wide=z_wide,
        any_rule_wide=any_rule_wide,
        strong_rule_wide=strong_rule_wide,
        scores_by_day=scores_by_day,
        cash_log_daily=cash_log_daily,
        cfg=cfg_c4,
    )
    curve_c2, _, _, _ = run_variant(
        variant="C2",
        px_exec_wide=px_exec_wide,
        split_event_wide=split_event_wide,
        i_wide=i_wide,
        z_wide=z_wide,
        any_rule_wide=any_rule_wide,
        strong_rule_wide=strong_rule_wide,
        scores_by_day=scores_by_day,
        cash_log_daily=cash_log_daily,
        cfg=cfg_c2,
    )
    curve_c4 = curve_c4[(curve_c4["date"] >= HOLDOUT_START) & (curve_c4["date"] <= HOLDOUT_END)][["date", "equity"]].copy()
    curve_c2 = curve_c2[(curve_c2["date"] >= HOLDOUT_START) & (curve_c2["date"] <= HOLDOUT_END)][["date", "equity"]].copy()
    curve_c4 = curve_c4.rename(columns={"equity": "equity_c4"})
    curve_c2 = curve_c2.rename(columns={"equity": "equity_c2"})

    analysis = bench.merge(curve_c4, on="date", how="inner").merge(curve_c2, on="date", how="inner")
    if analysis.empty:
        raise RuntimeError("T-022 FAIL: merge de benchmarks com curvas do motor resultou vazio.")

    def build_window_block(name: str, w: dict[str, object]) -> dict[str, object]:
        start = pd.Timestamp(str(w["peak_date"]))
        end = pd.Timestamp(str(w["recovery_or_end_date"]))
        sub = analysis[(analysis["date"] >= start) & (analysis["date"] <= end)].copy()
        return {
            "name": name,
            "window": w,
            "metrics_sp500": _window_metrics(sub["sp500_close"]),
            "metrics_r1000_proxy": _window_metrics(sub["r1000_proxy_close"]),
            "metrics_motor_c4": _window_metrics(sub["equity_c4"]),
            "metrics_motor_c2": _window_metrics(sub["equity_c2"]),
        }

    block_sp500 = build_window_block("SP500", w_sp500)
    block_r1000 = build_window_block(f"R1000_PROXY_{args.r1000_series_id}", w_r1000)

    plot_sp500 = OUT_DIR / "plot_t022_sp500_window.html"
    plot_r1000 = OUT_DIR / "plot_t022_r1000_proxy_window.html"
    _plot_window(analysis, w_sp500, "T-022 Acid Window (SP500)", plot_sp500)
    _plot_window(analysis, w_r1000, f"T-022 Acid Window (R1000 Proxy: {args.r1000_series_id})", plot_r1000)

    gates = {
        "required_inputs_exist": bool(required_inputs_exist),
        "series_sp500_non_empty": bool(series_sp500_non_empty),
        "series_r1000_non_empty": bool(series_r1000_non_empty),
        "acid_windows_found": bool(acid_windows_found),
        "outputs_written": bool(out_json.exists() and plot_sp500.exists() and plot_r1000.exists()),
    }

    report = {
        "task_id": "T-022",
        "decision_ref": "D-002",
        "params": {
            "r1000_series_id": args.r1000_series_id,
            "min_window_days": int(args.min_window_days),
            "cfg_c4": asdict(cfg_c4),
            "cfg_c2": asdict(cfg_c2),
            "min_market_cap": float(args.min_market_cap),
        },
        "inputs": {
            "paths": {
                "canonical": str(IN_CANONICAL),
                "macro": str(IN_MACRO),
                "scores": str(IN_SCORES),
                "blacklist": str(IN_BLACKLIST),
                "labels_us": str(LABELS_PATH),
            },
            "bench_rows_holdout": int(len(bench)),
            "analysis_rows_holdout": int(len(analysis)),
            "sha256_inputs": {
                "script": _sha256(Path(__file__).resolve()),
                "labels_us": _sha256(LABELS_PATH),
            },
        },
        "windows": {
            "sp500": block_sp500,
            "r1000_proxy": block_r1000,
        },
        "global_holdout_metrics": {
            "c4": {
                "cagr_pct": float(_curve_metrics(analysis.rename(columns={"equity_c4": "equity"}))[0] * 100.0),
                "mdd_pct": float(_curve_metrics(analysis.rename(columns={"equity_c4": "equity"}))[1] * 100.0),
            },
            "c2": {
                "cagr_pct": float(_curve_metrics(analysis.rename(columns={"equity_c2": "equity"}))[0] * 100.0),
                "mdd_pct": float(_curve_metrics(analysis.rename(columns={"equity_c2": "equity"}))[1] * 100.0),
            },
            "scores_median_pre_filter": float(median_pre_filter),
            "scores_median_post_filter": float(median_post_filter),
        },
        "outputs": {
            "acid_analysis_json": str(out_json),
            "plot_sp500_html": str(plot_sp500),
            "plot_r1000_html": str(plot_r1000),
        },
        "gates": gates,
    }

    out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    gates["outputs_written"] = bool(out_json.exists() and plot_sp500.exists() and plot_r1000.exists())
    report["gates"] = gates
    out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    failed = [k for k, v in gates.items() if not v]
    if failed:
        raise RuntimeError(f"T-022 FAIL gates: {failed}")

    print("T-022 PASS")
    print(
        json.dumps(
            {
                "r1000_series_id": args.r1000_series_id,
                "window_sp500": block_sp500["window"],
                "window_r1000_proxy": block_r1000["window"],
                "gates": gates,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
