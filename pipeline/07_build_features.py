"""Step 07 — build feature dataset US."""
from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(end_date: date | None = None) -> dict:
    cmd = [
        str(sys.executable),
        str(ROOT / "scripts" / "t013_build_features_us.py"),
        "--workspace",
        str(ROOT),
    ]
    subprocess.run(cmd, check=True, cwd=str(ROOT))
    return {
        "status": "ok",
        "end_date": str(end_date) if end_date else None,
        "dataset_path": "data/features/dataset_us.parquet",
    }
