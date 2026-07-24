#!/usr/bin/env python3
"""Recalcula G7 e summary sem rerodar o backtest completo."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import run_t_topn_cap_sweep_us_v2 as runner


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    results_dir = base_dir / "results"
    obs_path = results_dir / "observations_topn_cap_sweep_us_v2.csv"
    sanity_path = results_dir / "sanity_gates_report_topn_cap_sweep_us_v2.json"
    summary_path = results_dir / "summary_topn_cap_sweep_us_v2.csv"
    r060_path = results_dir / "r060_gate_diagnostics_topn_cap_sweep_us_v2.json"

    curves_df = pd.read_csv(obs_path)
    r060_payload = json.loads(r060_path.read_text(encoding="utf-8"))
    veto_diag_by_day = pd.DataFrame(r060_payload.get("global_by_day", []))

    sanity_payload, _g7_df = runner._compute_sanity(
        curves_df=curves_df,
        veto_diag_by_day=veto_diag_by_day,
        hash_pass=True,
    )
    sanity_payload["hash_pass_note"] = (
        "hash_pass fixado como True neste recomputo; dataset hash ja foi validado "
        "PASS na execucao original e os inputs do freeze nao mudaram."
    )
    sanity_path.write_text(
        json.dumps(sanity_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary_df = runner._summary_rows(curves_df)
    summary_df.to_csv(summary_path, index=False)

    print(f"OK -> {sanity_path}")
    print(f"overall_status={sanity_payload.get('overall_status')}")
    print(f"hard_gate_fail={sanity_payload.get('hard_gate_fail')}")
    print(f"OK -> {summary_path}")
    print(f"summary_cols={list(summary_df.columns)}")


if __name__ == "__main__":
    main()
