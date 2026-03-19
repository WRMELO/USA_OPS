"""Step 02 — ingest prices US (wrapper de T-007v2)."""
from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(end_date: date | None = None) -> dict:
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
    if end_date:
        cmd.extend(["--end-date", str(end_date)])
    subprocess.run(cmd, check=True, cwd=str(ROOT))
    return {
        "status": "ok",
        "end_date": str(end_date) if end_date else None,
        "raw_path": "data/ssot/us_market_data_raw.parquet",
    }
