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
        description="T-010v2: construir canonical_us.parquet consolidando SPC+reference+BDR exclusion."
    )
    parser.add_argument("--workspace", required=True)
    parser.add_argument(
        "--in-operational",
        default="data/ssot/us_universe_operational.parquet",
    )
    parser.add_argument(
        "--in-reference",
        default="data/ssot/ticker_reference_us.parquet",
    )
    parser.add_argument(
        "--in-bdr-exclusion",
        default="data/ssot/bdr_exclusion_list.json",
    )
    parser.add_argument(
        "--out-canonical",
        default="data/ssot/canonical_us.parquet",
    )
    parser.add_argument(
        "--out-report",
        default="data/ssot/t010v2_canonical_report.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    in_operational = workspace / args.in_operational
    in_reference = workspace / args.in_reference
    in_bdr_exclusion = workspace / args.in_bdr_exclusion
    out_canonical = workspace / args.out_canonical
    out_report = workspace / args.out_report

    for p in [in_operational, in_reference, in_bdr_exclusion]:
        if not p.exists():
            raise FileNotFoundError(f"Input ausente: {p}")

    with in_bdr_exclusion.open("r", encoding="utf-8") as f:
        bdr_payload = json.load(f)
    excluded_tickers = set(_norm_ticker(t) for t in bdr_payload.get("excluded_tickers", []))
    remaining_count_expected = int(bdr_payload["counts"]["remaining_count"])

    required_operational = [
        "date",
        "ticker",
        "is_operational",
        "close_raw",
        "close_operational",
        "split_factor",
        "dividend_rate",
        "log_ret_nominal",
        "X_real",
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
        "center_line",
        "mr_bar",
        "r_bar",
        "quality_flag",
        "blacklist_level",
        "blacklist_reason",
    ]
    op = pd.read_parquet(in_operational)
    missing = sorted(set(required_operational) - set(op.columns))
    if missing:
        raise ValueError(f"Schema inválido em us_universe_operational: faltando {missing}")

    op["ticker"] = op["ticker"].map(_norm_ticker)
    op = op[op["ticker"] != ""].copy()
    op = op[op["is_operational"] == True].copy()  # noqa: E712
    op = op[~op["ticker"].isin(excluded_tickers)].copy()

    reference_cols = [
        "ticker",
        "active",
        "list_date",
        "delisted_utc",
        "primary_exchange",
        "type",
        "market_cap",
        "fetch_status",
    ]
    ref = pd.read_parquet(in_reference, columns=reference_cols).copy()
    ref["ticker"] = ref["ticker"].map(_norm_ticker)
    ref = ref[ref["ticker"] != ""].drop_duplicates(subset=["ticker"], keep="first")

    canonical = op.merge(ref, on="ticker", how="left")
    canonical["universe_tag"] = "US_V2_OPER_BDR_EXCL"

    canonical = canonical[
        [
            "date",
            "ticker",
            "close_raw",
            "close_operational",
            "split_factor",
            "dividend_rate",
            "log_ret_nominal",
            "X_real",
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
            "center_line",
            "mr_bar",
            "r_bar",
            "universe_tag",
            "quality_flag",
            "blacklist_level",
            "blacklist_reason",
            "active",
            "list_date",
            "delisted_utc",
            "primary_exchange",
            "type",
            "market_cap",
            "fetch_status",
        ]
    ].copy()

    canonical = canonical.sort_values(["ticker", "date"]).reset_index(drop=True)
    dup_count = int(canonical.duplicated(subset=["date", "ticker"]).sum())
    if dup_count > 0:
        canonical = canonical.drop_duplicates(subset=["date", "ticker"], keep="last").copy()

    output_tickers = int(canonical["ticker"].nunique())
    output_rows = int(len(canonical))
    output_dates = int(pd.to_datetime(canonical["date"], errors="coerce").nunique())
    gate_ticker_count = output_tickers == remaining_count_expected
    gate_zero_dups = int(canonical.duplicated(subset=["date", "ticker"]).sum()) == 0

    out_canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.to_parquet(out_canonical, index=False)

    report = {
        "task_id": "T-010v2",
        "decision_ref": "D-007",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "operational_path": str(in_operational),
            "reference_path": str(in_reference),
            "bdr_exclusion_path": str(in_bdr_exclusion),
            "sha256_inputs": {
                "us_universe_operational": _sha256(in_operational),
                "ticker_reference_us": _sha256(in_reference),
                "bdr_exclusion_list": _sha256(in_bdr_exclusion),
            },
        },
        "counts": {
            "remaining_count_expected": remaining_count_expected,
            "output_tickers": output_tickers,
            "output_rows": output_rows,
            "output_dates": output_dates,
            "excluded_tickers_from_t009": len(excluded_tickers),
        },
        "gates": {
            "output_tickers_equals_remaining_count": gate_ticker_count,
            "zero_duplicates_date_ticker": gate_zero_dups,
        },
        "sample": {
            "tickers_head": sorted(canonical["ticker"].dropna().unique().tolist())[:20],
        },
        "output": {
            "canonical_path": str(out_canonical),
            "canonical_sha256": _sha256(out_canonical),
        },
    }
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out_report.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    if not gate_ticker_count:
        raise ValueError(
            "Gate FAIL: output_tickers != remaining_count_expected "
            f"({output_tickers} != {remaining_count_expected})"
        )
    if not gate_zero_dups:
        raise ValueError("Gate FAIL: duplicatas (date,ticker) no canônico")

    print("T-010v2 PASS")
    print(
        json.dumps(
            {
                "remaining_count_expected": remaining_count_expected,
                "output_tickers": output_tickers,
                "output_rows": output_rows,
                "output_dates": output_dates,
                "gate_output_tickers_equals_remaining_count": gate_ticker_count,
                "gate_zero_duplicates_date_ticker": gate_zero_dups,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
