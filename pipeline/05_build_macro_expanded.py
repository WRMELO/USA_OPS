"""Step 05 — build macro expanded features."""
from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(end_date: date | None = None) -> dict:
    # Reaproveita a lógica consolidada de T-011v2.
    cmd = [
        str(sys.executable),
        str(ROOT / "scripts" / "t011_ingest_macro_us_v2.py"),
        "--workspace",
        str(ROOT),
    ]
    subprocess.run(cmd, check=True, cwd=str(ROOT))
    return {
        "status": "ok",
        "end_date": str(end_date) if end_date else None,
        "macro_features_path": "data/features/macro_features_us.parquet",
    }
