"""Verify integrity and loader routing for the frozen US research dataset."""
from __future__ import annotations

from pathlib import Path
import hashlib
import json
import os
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.run_backtest_variants_us import load_inputs  # noqa: E402

DATASET_DIR = ROOT / "backtest" / "research_dataset_us"
MANIFEST = DATASET_DIR / "manifest.json"
REQUIRED_FILES = ["canonical_us.parquet", "macro_us.parquet", "scores_m3_us.parquet"]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    if not MANIFEST.exists():
        _fail(f"manifest ausente: {MANIFEST}")
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for field in ["freeze_asof", "window", "files", "tables"]:
        if field not in manifest:
            _fail(f"manifest sem campo obrigatorio: {field}")

    for name in REQUIRED_FILES:
        path = DATASET_DIR / name
        if not path.exists():
            _fail(f"arquivo ausente: {path}")
        expected = manifest["files"].get(name, {}).get("sha256")
        if not expected:
            _fail(f"manifest sem sha256 para {name}")
        actual = _sha256(path)
        if actual != expected:
            _fail(f"sha256 divergente para {name}: expected={expected} actual={actual}")

    old_env = os.environ.get("US_RESEARCH_DATASET_DIR")
    os.environ["US_RESEARCH_DATASET_DIR"] = str(DATASET_DIR)
    try:
        canonical, _, _ = load_inputs()
    finally:
        if old_env is None:
            os.environ.pop("US_RESEARCH_DATASET_DIR", None)
        else:
            os.environ["US_RESEARCH_DATASET_DIR"] = old_env

    max_date = str(canonical["date"].max().date())
    if max_date != manifest["freeze_asof"]:
        _fail(f"load_inputs nao leu dataset congelado: max_date={max_date} freeze_asof={manifest['freeze_asof']}")

    print(json.dumps({"status": "OK", "freeze_asof": manifest["freeze_asof"], "max_date": max_date}, indent=2))


if __name__ == "__main__":
    main()
