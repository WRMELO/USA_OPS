#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _norm_ticker(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="T-009v2: Excluir tickers US que possuem BDR na B3."
    )
    parser.add_argument("--workspace", required=True)
    parser.add_argument(
        "--in-us-universe",
        default="data/ssot/us_universe_operational.parquet",
    )
    parser.add_argument(
        "--in-bdr-universe",
        default="/home/wilson/RENDA_OPS/data/ssot/bdr_universe.parquet",
    )
    parser.add_argument(
        "--out-exclusion-json",
        default="data/ssot/bdr_exclusion_list.json",
    )
    parser.add_argument(
        "--out-report-json",
        default="data/ssot/t009v2_bdr_exclusion_report.json",
    )
    return parser.parse_args()


def _sample(values: list[str], size: int = 20) -> list[str]:
    return sorted(values)[:size]


def main() -> None:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    in_us = workspace / args.in_us_universe
    out_exclusion = workspace / args.out_exclusion_json
    out_report = workspace / args.out_report_json
    in_bdr = Path(args.in_bdr_universe).resolve()

    if not in_us.exists():
        raise FileNotFoundError(f"Input ausente: {in_us}")
    if not in_bdr.exists():
        raise FileNotFoundError(f"Input ausente: {in_bdr}")

    us = pd.read_parquet(in_us, columns=["ticker", "is_operational"])
    us["ticker_norm"] = us["ticker"].map(_norm_ticker)
    us = us[us["ticker_norm"] != ""].copy()

    per_ticker_nunique = us.groupby("ticker_norm")["is_operational"].nunique(dropna=False)
    varying = per_ticker_nunique[per_ticker_nunique > 1]
    if not varying.empty:
        raise ValueError(
            "Gate FAIL: is_operational varia por ticker. "
            f"Tickers afetados: {sorted(varying.index.tolist())[:10]}"
        )

    us_by_ticker = us.groupby("ticker_norm", as_index=False)["is_operational"].first()
    operational = us_by_ticker[us_by_ticker["is_operational"] == True].copy()  # noqa: E712
    operational_tickers = sorted(operational["ticker_norm"].tolist())
    operational_set = set(operational_tickers)

    bdr = pd.read_parquet(in_bdr)
    if "ticker" not in bdr.columns:
        raise ValueError("Schema inválido: bdr_universe.parquet sem coluna 'ticker'")
    bdr["ticker_norm"] = bdr["ticker"].map(_norm_ticker)
    bdr_tickers = sorted(set([t for t in bdr["ticker_norm"].tolist() if t]))
    bdr_set = set(bdr_tickers)

    excluded = sorted(operational_set.intersection(bdr_set))
    remaining = sorted(operational_set.difference(bdr_set))

    operational_total = len(operational_set)
    bdr_underlyings_total = len(bdr_set)
    excluded_count = len(excluded)
    remaining_count = len(remaining)
    coherence_gate = remaining_count == (operational_total - excluded_count)

    inputs_sha = {
        "us_universe_operational": _sha256(in_us),
        "bdr_universe_renda_ops": _sha256(in_bdr),
    }

    now_iso = datetime.now(timezone.utc).isoformat()
    exclusion_payload = {
        "task_id": "T-009v2",
        "decision_ref": "D-007",
        "generated_at": now_iso,
        "inputs": {
            "us_universe_operational_path": str(in_us),
            "bdr_universe_path": str(in_bdr),
            "sha256_inputs": inputs_sha,
        },
        "counts": {
            "operational_total": operational_total,
            "bdr_underlyings_total": bdr_underlyings_total,
            "excluded_count": excluded_count,
            "remaining_count": remaining_count,
        },
        "gates": {
            "remaining_equals_operational_minus_excluded": coherence_gate,
        },
        "excluded_tickers": excluded,
        "sample": {
            "match": _sample(excluded, 20),
            "operational_not_in_bdr": _sample(remaining, 20),
            "bdr_not_operational": _sample(sorted(bdr_set.difference(operational_set)), 20),
        },
    }

    report_payload = {
        "task_id": "T-009v2",
        "generated_at": now_iso,
        "decision_ref": "D-007",
        "counts": exclusion_payload["counts"],
        "gates": exclusion_payload["gates"],
        "samples": exclusion_payload["sample"],
        "top20_excluded": _sample(excluded, 20),
        "top20_remaining": _sample(remaining, 20),
        "schema": {
            "us_universe_columns_used": ["ticker", "is_operational"],
            "bdr_universe_columns": list(bdr.columns),
        },
        "inputs": exclusion_payload["inputs"],
    }

    out_exclusion.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out_exclusion.write_text(json.dumps(exclusion_payload, indent=2, ensure_ascii=False))
    out_report.write_text(json.dumps(report_payload, indent=2, ensure_ascii=False))

    if not coherence_gate:
        raise ValueError(
            "Gate FAIL: remaining_count != operational_total - excluded_count "
            f"({remaining_count} != {operational_total} - {excluded_count})"
        )

    print("T-009v2 PASS")
    print(
        json.dumps(
            {
                "operational_total": operational_total,
                "bdr_underlyings_total": bdr_underlyings_total,
                "excluded_count": excluded_count,
                "remaining_count": remaining_count,
                "gate_remaining_equals_operational_minus_excluded": coherence_gate,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
