"""T-018: Ablacao C4 (dampening + cap de concentracao) para backtest US."""
from __future__ import annotations

from pathlib import Path
import argparse
import json

import numpy as np
import pandas as pd

from run_backtest_variants_us import (
    BacktestConfig,
    IN_BLACKLIST,
    IN_CANONICAL,
    IN_MACRO,
    IN_SCORES,
    MIN_MARKET_CAP_DEFAULT,
    OUT_DIR,
    _curve_metrics,
    _sha256,
    _build_z_table,
    apply_min_market_cap_filter,
    build_cash_log_daily,
    build_market_cap_wide,
    build_scores_by_day,
    load_blacklist,
    load_inputs,
    run_variant,
    summarize_curve,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-018 ablation C4 (dampening + cap)")
    parser.add_argument("--topn-grid", type=str, default="20,25")
    parser.add_argument("--cadence-grid", type=str, default="10")
    parser.add_argument("--k-grid", type=str, default="10,30")
    parser.add_argument("--k-damp-grid", type=str, default="0,10,100")
    parser.add_argument("--max-weight-cap-grid", type=str, default="0.06,0.08,0.10")
    parser.add_argument("--friction-bps", type=float, default=2.5)
    parser.add_argument("--settlement-days", type=int, default=1)
    parser.add_argument("--base-capital", type=float, default=100000.0)
    parser.add_argument("--min-market-cap", type=float, default=MIN_MARKET_CAP_DEFAULT)
    return parser.parse_args()


def _parse_int_grid(raw: str) -> list[int]:
    vals = [int(x.strip()) for x in raw.split(",") if x.strip()]
    vals = sorted(list(set(v for v in vals if v > 0)))
    if not vals:
        raise ValueError("Grid inteiro vazio/invalido.")
    return vals


def _parse_float_grid(raw: str, positive_only: bool = False) -> list[float]:
    vals = [float(x.strip()) for x in raw.split(",") if x.strip()]
    if positive_only:
        vals = [v for v in vals if v > 0.0]
    vals = sorted(list(set(vals)))
    if not vals:
        raise ValueError("Grid float vazio/invalido.")
    return vals


def main() -> None:
    args = parse_args()
    topn_grid = _parse_int_grid(args.topn_grid)
    cadence_grid = _parse_int_grid(args.cadence_grid)
    k_grid = _parse_int_grid(args.k_grid)
    k_damp_grid = _parse_float_grid(args.k_damp_grid)
    max_weight_cap_grid = _parse_float_grid(args.max_weight_cap_grid, positive_only=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    canonical, macro, scores = load_inputs()
    blacklist = load_blacklist(IN_BLACKLIST)
    cash_log_daily = build_cash_log_daily(macro)
    scores_by_day = build_scores_by_day(scores=scores, blacklist=blacklist)
    market_cap_wide = build_market_cap_wide(canonical)
    scores_by_day, median_pre_filter, median_post_filter = apply_min_market_cap_filter(
        scores_by_day=scores_by_day,
        market_cap_wide=market_cap_wide,
        min_market_cap=float(args.min_market_cap),
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
    strong_rule_wide = canonical.pivot_table(
        index="date", columns="ticker", values="_strong_rule", aggfunc="first"
    ).sort_index()

    summary_rows: list[dict[str, object]] = []
    combo_rows: list[dict[str, object]] = []
    total_expected = 0
    total_executed = 0
    c2_baseline_combos = 0
    c4_combos = 0

    for top_n in topn_grid:
        for cadence in cadence_grid:
            for k in k_grid:
                # Baseline C2 (um por combinacao top_n x cadence x k)
                total_expected += 1
                c2_baseline_combos += 1
                cfg_c2 = BacktestConfig(
                    top_n=top_n,
                    buffer_k=k,
                    rebalance_cadence=cadence,
                    friction_one_way_bps=float(args.friction_bps),
                    settlement_days=int(args.settlement_days),
                    base_capital=float(args.base_capital),
                    k_damp=0.0,
                    max_weight_cap=1.0,
                )
                curve, _, events_split, events_trim = run_variant(
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
                total_executed += 1
                cagr_full, mdd_full = _curve_metrics(curve)
                combo_rows.append(
                    {
                        "variant": "C2",
                        "top_n": int(top_n),
                        "rebalance_cadence": int(cadence),
                        "buffer_k": int(k),
                        "k_damp": float(cfg_c2.k_damp),
                        "max_weight_cap": float(cfg_c2.max_weight_cap),
                        "equity_final": float(curve["equity"].iloc[-1]) if not curve.empty else np.nan,
                        "cagr_full": float(cagr_full),
                        "mdd_full": float(mdd_full),
                        "split_events": int(len(events_split)),
                        "trim_events": int(len(events_trim)),
                    }
                )
                for row in summarize_curve(curve):
                    row["top_n"] = int(top_n)
                    row["rebalance_cadence"] = int(cadence)
                    row["buffer_k"] = int(k)
                    row["k_damp"] = float(cfg_c2.k_damp)
                    row["max_weight_cap"] = float(cfg_c2.max_weight_cap)
                    summary_rows.append(row)

                # Grid da C4 (dampening + cap)
                for k_damp in k_damp_grid:
                    for max_cap in max_weight_cap_grid:
                        total_expected += 1
                        c4_combos += 1
                        cfg_c4 = BacktestConfig(
                            top_n=top_n,
                            buffer_k=k,
                            rebalance_cadence=cadence,
                            friction_one_way_bps=float(args.friction_bps),
                            settlement_days=int(args.settlement_days),
                            base_capital=float(args.base_capital),
                            k_damp=float(k_damp),
                            max_weight_cap=float(max_cap),
                        )
                        curve, _, events_split, events_trim = run_variant(
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
                        total_executed += 1
                        cagr_full, mdd_full = _curve_metrics(curve)
                        combo_rows.append(
                            {
                                "variant": "C4",
                                "top_n": int(top_n),
                                "rebalance_cadence": int(cadence),
                                "buffer_k": int(k),
                                "k_damp": float(k_damp),
                                "max_weight_cap": float(max_cap),
                                "equity_final": float(curve["equity"].iloc[-1]) if not curve.empty else np.nan,
                                "cagr_full": float(cagr_full),
                                "mdd_full": float(mdd_full),
                                "split_events": int(len(events_split)),
                                "trim_events": int(len(events_trim)),
                            }
                        )
                        for row in summarize_curve(curve):
                            row["top_n"] = int(top_n)
                            row["rebalance_cadence"] = int(cadence)
                            row["buffer_k"] = int(k)
                            row["k_damp"] = float(k_damp)
                            row["max_weight_cap"] = float(max_cap)
                            summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows).sort_values(
        ["variant", "top_n", "rebalance_cadence", "buffer_k", "k_damp", "max_weight_cap", "split"]
    ).reset_index(drop=True)
    combos_df = pd.DataFrame(combo_rows).sort_values(
        ["variant", "top_n", "rebalance_cadence", "buffer_k", "k_damp", "max_weight_cap"]
    ).reset_index(drop=True)

    summary_csv = OUT_DIR / "t018_ablation_summary.csv"
    summary_json = OUT_DIR / "t018_ablation_summary.json"
    report_json = OUT_DIR / "t018_ablation_report.json"
    summary_df.to_csv(summary_csv, index=False)
    summary_df.to_json(summary_json, orient="records", indent=2)

    gates = {
        "required_inputs_exist": all(p.exists() for p in [IN_CANONICAL, IN_MACRO, IN_SCORES, IN_BLACKLIST]),
        "summary_non_empty": not summary_df.empty,
        "combos_non_empty": not combos_df.empty,
        "combos_executed_expected": int(total_executed) == int(total_expected),
        "outputs_written": all(p.exists() for p in [summary_csv, summary_json, report_json]),
    }

    report = {
        "task_id": "T-018",
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "params": {
            "topn_grid": topn_grid,
            "cadence_grid": cadence_grid,
            "k_grid": k_grid,
            "k_damp_grid": k_damp_grid,
            "max_weight_cap_grid": max_weight_cap_grid,
            "friction_one_way_bps": float(args.friction_bps),
            "settlement_days": int(args.settlement_days),
            "base_capital": float(args.base_capital),
            "min_market_cap": float(args.min_market_cap),
        },
        "parity_notes": {
            "dampening": "Nao existe no RENDA_OPS; divergencia deliberada para comprimir caudas extremas no universo US (D-019).",
            "concentration_cap": "Nao existe no RENDA_OPS; divergencia deliberada para limitar concentracao mecanica no rebalanceamento US (D-019).",
            "split_factor_semantics": "US canonical usa split_factor cumulativo; no backtest o ratio do split e derivado do preco raw (px_{D-1}/px_D).",
        },
        "inputs": {
            "canonical_us": str(IN_CANONICAL),
            "macro_us": str(IN_MACRO),
            "scores_m3_us": str(IN_SCORES),
            "blacklist_us": str(IN_BLACKLIST),
            "sha256_inputs": {
                "canonical_us": _sha256(IN_CANONICAL),
                "macro_us": _sha256(IN_MACRO),
                "scores_m3_us": _sha256(IN_SCORES),
                "blacklist_us": _sha256(IN_BLACKLIST),
            },
        },
        "counts": {
            "combos_expected": int(total_expected),
            "combos_executed": int(total_executed),
            "c2_baseline_combos": int(c2_baseline_combos),
            "c4_combos": int(c4_combos),
            "summary_rows": int(len(summary_df)),
            "median_scored_tickers_pre_filter": float(median_pre_filter),
            "median_scored_tickers_post_filter": float(median_post_filter),
        },
        "outputs": {
            "summary_csv": "backtest/results/t018_ablation_summary.csv",
            "summary_json": "backtest/results/t018_ablation_summary.json",
            "report_json": "backtest/results/t018_ablation_report.json",
        },
        "best_holdout_by_cagr": (
            combos_df.sort_values("cagr_full", ascending=False).head(10).to_dict(orient="records")
            if not combos_df.empty
            else []
        ),
        "gates": gates,
    }
    report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    gates["outputs_written"] = all(p.exists() for p in [summary_csv, summary_json, report_json])
    report["gates"] = gates
    report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if not all(gates.values()):
        failed = [k for k, v in gates.items() if not v]
        raise RuntimeError(f"T-018 FAIL gates: {failed}")

    print("T-018 PASS")
    print(json.dumps({"gates": gates, "combos": total_executed, "summary_rows": len(summary_df)}, indent=2))


if __name__ == "__main__":
    main()
