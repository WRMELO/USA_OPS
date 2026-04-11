"""Step 04 — build canonical US (T-008v2 -> T-009v2 -> T-010v2)."""
from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run_logged_subprocess(command: list[str], log_filename: str) -> None:
    logs_dir = ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / log_filename
    with log_path.open("w", encoding="utf-8") as log_fh:
        subprocess.run(
            command,
            check=True,
            cwd=str(ROOT),
            stdout=log_fh,
            stderr=log_fh,
        )


def run(end_date: date | None = None) -> dict:
    # 1) SPC + universo operacional
    cmd_008 = [
        str(sys.executable),
        str(ROOT / "scripts" / "t008_quality_spc_and_blacklist_v2.py"),
        "--workspace",
        str(ROOT),
        "--out-blacklist",
        "data/ssot/blacklist_us.json",
        "--out-report",
        "data/ssot/t008v2_quality_report.json",
        "--chunk-size",
        "250",
        "--max-workers",
        "10",
    ]
    _run_logged_subprocess(cmd_008, "t008_canonical_subprocess.log")

    # 2) exclusão de BDR
    cmd_009 = [
        str(sys.executable),
        str(ROOT / "scripts" / "t009_exclude_bdrs_v2.py"),
        "--workspace",
        str(ROOT),
    ]
    _run_logged_subprocess(cmd_009, "t009_canonical_subprocess.log")

    # 3) canônico final
    cmd_010 = [
        str(sys.executable),
        str(ROOT / "scripts" / "t010_build_canonical_us_v2.py"),
        "--workspace",
        str(ROOT),
    ]
    _run_logged_subprocess(cmd_010, "t010_canonical_subprocess.log")

    return {
        "status": "ok",
        "end_date": str(end_date) if end_date else None,
        "canonical_path": "data/ssot/canonical_us.parquet",
    }
