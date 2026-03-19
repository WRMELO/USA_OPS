"""Step 06 — compute M3-US scores."""
from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(end_date: date | None = None) -> dict:
    cmd = [
        str(sys.executable),
        str(ROOT / "scripts" / "t012_compute_scores_m3_us.py"),
        "--workspace",
        str(ROOT),
    ]
    subprocess.run(cmd, check=True, cwd=str(ROOT))
    return {
        "status": "ok",
        "end_date": str(end_date) if end_date else None,
        "scores_path": "data/features/scores_m3_us.parquet",
    }
