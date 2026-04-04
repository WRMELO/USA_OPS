"""Step 02 — ingest prices US (wrapper de T-007v2)."""
from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def run(end_date: date | None = None) -> dict:
    raw_path = ROOT / "data" / "ssot" / "us_market_data_raw.parquet"
    delta_path = ROOT / "data" / "ssot" / "us_market_data_raw_delta.parquet"
    report_path = ROOT / "logs" / "t007_ingestion_report_full_delta.json"
    failures_path = ROOT / "logs" / "t007_failures_full_delta.json"

    target_end = end_date or date.today()

    start_dt = None
    if raw_path.exists():
        df = pd.read_parquet(raw_path, columns=["date"]).copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        if not df["date"].dropna().empty:
            last_dt = pd.Timestamp(df["date"].max()).normalize().date()
            start_dt = (pd.Timestamp(last_dt) + pd.Timedelta(days=1)).date()

    cmd = [
        str(sys.executable),
        str(ROOT / "scripts" / "t007_ingest_us_market_data_raw.py"),
        "--workspace",
        str(ROOT),
        "--chunk-size",
        "200",
        "--max-workers",
        "12",
    ]

    # Se já temos raw_full, ingere somente o delta [last_date+1, target_end] e faz merge.
    if start_dt is not None and start_dt <= target_end:
        cmd.extend(
            [
                "--start-date",
                str(start_dt),
                "--end-date",
                str(target_end),
                "--out-path",
                str(delta_path.relative_to(ROOT)),
                "--report-path",
                str(report_path.relative_to(ROOT)),
                "--failures-path",
                str(failures_path.relative_to(ROOT)),
            ]
        )
        subprocess.run(cmd, check=True, cwd=str(ROOT))

        if delta_path.exists():
            base = pd.read_parquet(raw_path).copy() if raw_path.exists() else pd.DataFrame()
            delta = pd.read_parquet(delta_path).copy()
            merged = pd.concat([base, delta], ignore_index=True) if not base.empty else delta
            merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.normalize()
            merged["ticker"] = merged["ticker"].astype(str).str.upper().str.strip()
            merged = merged.dropna(subset=["date", "ticker"])
            if "ingested_at" in merged.columns:
                merged = merged.sort_values(["ticker", "date", "ingested_at"])
            else:
                merged = merged.sort_values(["ticker", "date"])
            merged = merged.drop_duplicates(subset=["date", "ticker"], keep="last").reset_index(drop=True)
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            merged.to_parquet(raw_path, index=False)
    else:
        # Bootstrap (ou nada a fazer): mantém comportamento antigo (full range até target_end).
        cmd.extend(["--end-date", str(target_end)])
        subprocess.run(cmd, check=True, cwd=str(ROOT))
    return {
        "status": "ok",
        "end_date": str(target_end),
        "raw_path": "data/ssot/us_market_data_raw.parquet",
    }
