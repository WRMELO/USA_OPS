"""Replay C4 on the frozen dataset and compare it with the sealed winner."""
from __future__ import annotations

from pathlib import Path
import hashlib
import json
import sys
from datetime import datetime, timezone

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.run_backtest_variants_us import (
    BacktestConfig,
    _curve_metrics,
    apply_min_market_cap_filter,
    build_cash_log_daily,
    build_market_cap_wide,
    build_scores_by_day,
    load_blacklist,
    load_inputs,
    run_variant,
)
from backtest.t_exec_completion_us.run_t_exec_completion_us_v2 import _prepare_wides


DATASET_DIR = ROOT / "backtest" / "research_dataset_us"
RESULTS_DIR = DATASET_DIR / "results"
MANIFEST = DATASET_DIR / "manifest.json"
WINNER = ROOT / "config" / "winner_us.json"
WINNER_CURVE = ROOT / "backtest" / "results" / "curve_C4_K10.csv"
BLACKLIST = ROOT / "config" / "blacklist_us.json"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _round_pct(value: float) -> float:
    return round(float(value) * 100.0, 6)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    winner = json.loads(WINNER.read_text(encoding="utf-8"))
    cfg_snapshot = winner["winner_config_snapshot"]
    holdout = winner["holdout_period"]
    holdout_start = pd.Timestamp(holdout["start"])
    holdout_end = pd.Timestamp(holdout["end"])

    cfg = BacktestConfig(
        top_n=int(cfg_snapshot["top_n"]),
        buffer_k=int(cfg_snapshot["buffer_k"]),
        rebalance_cadence=int(cfg_snapshot["rebalance_cadence"]),
        friction_one_way_bps=float(cfg_snapshot["friction_one_way_bps"]),
        settlement_days=int(cfg_snapshot["settlement_days"]),
        base_capital=float(cfg_snapshot["base_capital"]),
        k_damp=float(cfg_snapshot["k_damp"]),
        max_weight_cap=float(cfg_snapshot["max_weight_cap"]),
    )

    canonical, macro, scores = load_inputs(
        canonical_path=DATASET_DIR / "canonical_us.parquet",
        macro_path=DATASET_DIR / "macro_us.parquet",
        scores_path=DATASET_DIR / "scores_m3_us.parquet",
    )
    blacklist = load_blacklist(BLACKLIST)
    cash_log_daily = build_cash_log_daily(macro)
    scores_by_day = build_scores_by_day(scores=scores, blacklist=blacklist)
    market_cap_wide = build_market_cap_wide(canonical)
    scores_by_day, median_pre_filter, median_post_filter = apply_min_market_cap_filter(
        scores_by_day=scores_by_day,
        market_cap_wide=market_cap_wide,
        min_market_cap=float(cfg_snapshot["min_market_cap"]),
    )
    px_exec_wide, split_event_wide, i_wide, z_wide, any_rule_wide, strong_rule_wide, _ = _prepare_wides(canonical)
    curve, _, _, _ = run_variant(
        variant="C4",
        px_exec_wide=px_exec_wide,
        split_event_wide=split_event_wide,
        i_wide=i_wide,
        z_wide=z_wide,
        any_rule_wide=any_rule_wide,
        strong_rule_wide=strong_rule_wide,
        scores_by_day=scores_by_day,
        cash_log_daily=cash_log_daily,
        cfg=cfg,
    )

    holdout_curve = curve[(curve["date"] >= holdout_start) & (curve["date"] <= holdout_end)].copy()
    if len(holdout_curve) < 2:
        raise RuntimeError("Curva congelada insuficiente para calcular HOLDOUT.")
    frozen_cagr, frozen_mdd = _curve_metrics(holdout_curve)

    sealed_metrics = winner["holdout_metrics"]
    sealed_curve_rows = int(len(pd.read_csv(WINNER_CURVE))) if WINNER_CURVE.exists() else 0
    result = {
        "task_id": "T-RESEARCH-DATASET-FREEZE-US-V1",
        "status": "OK",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_manifest": {
            "path": str(MANIFEST.relative_to(ROOT)),
            "freeze_asof": manifest["freeze_asof"],
            "sha256": _sha256(MANIFEST),
        },
        "holdout_period": {"start": str(holdout_start.date()), "end": str(holdout_end.date())},
        "frozen_replay": {
            "cagr_pct": _round_pct(frozen_cagr),
            "mdd_pct": _round_pct(frozen_mdd),
            "equity_final": round(float(holdout_curve["equity"].iloc[-1]), 2),
            "days": int(len(holdout_curve)),
            "scores_median_pre_filter": float(median_pre_filter),
            "scores_median_post_filter": float(median_post_filter),
        },
        "sealed_winner": {
            "source": str(WINNER.relative_to(ROOT)),
            "curve_path": str(WINNER_CURVE.relative_to(ROOT)),
            "curve_rows": sealed_curve_rows,
            "cagr_pct": float(sealed_metrics["cagr_pct"]),
            "mdd_pct": float(sealed_metrics["mdd_pct"]),
            "equity_final": float(sealed_metrics["equity_final"]),
        },
        "deltas_frozen_minus_sealed": {
            "cagr_pct_points": round(_round_pct(frozen_cagr) - float(sealed_metrics["cagr_pct"]), 6),
            "mdd_pct_points": round(_round_pct(frozen_mdd) - float(sealed_metrics["mdd_pct"]), 6),
            "equity_final": round(float(holdout_curve["equity"].iloc[-1]) - float(sealed_metrics["equity_final"]), 2),
        },
        "root_cause": (
            "A igualdade numerica com o winner selado de marco nao e criterio desta reconciliacao: "
            "os insumos originais de canonical/scores/macro eram gitignored e foram mutados pelos ingests "
            "posteriores. Este replay congela o estado atual como novo ancora versionado para "
            "reprodutibilidade prospectiva."
        ),
        "governance_decision": (
            "Experimentos offline de motor US devem usar backtest/research_dataset_us com manifesto SHA256; "
            "o SSOT vivo permanece operacional e nao deve servir como baseline de pesquisa."
        ),
    }
    out = RESULTS_DIR / "reconciliation_v1.json"
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(result["deltas_frozen_minus_sealed"], indent=2))


if __name__ == "__main__":
    main()
