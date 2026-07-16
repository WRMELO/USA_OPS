"""Freeze a versioned research dataset for US motor backtests."""
from __future__ import annotations

from pathlib import Path
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.run_backtest_variants_us import load_inputs  # noqa: E402

OUT_DIR = ROOT / "backtest" / "research_dataset_us_v2"
WINDOW_START = pd.Timestamp("2021-01-01")
FILES = {
    "canonical": OUT_DIR / "canonical_us.parquet",
    "macro": OUT_DIR / "macro_us.parquet",
    "scores": OUT_DIR / "scores_m3_us.parquet",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_output(*args: str) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except subprocess.CalledProcessError:
        return "N/A"


def _table_meta(df: pd.DataFrame) -> dict[str, object]:
    dates = pd.to_datetime(df["date"], errors="coerce")
    meta: dict[str, object] = {
        "n_rows": int(len(df)),
        "date_min": str(dates.min().date()) if len(df) else None,
        "date_max": str(dates.max().date()) if len(df) else None,
        "columns": list(df.columns),
    }
    if "ticker" in df.columns:
        meta["n_tickers"] = int(df["ticker"].nunique())
    return meta


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    canonical, macro, scores = load_inputs()
    freeze_asof = pd.to_datetime(canonical["date"], errors="coerce").max().normalize()
    if pd.isna(freeze_asof):
        raise RuntimeError("canonical_us sem data valida para freeze_asof.")

    start = WINDOW_START
    end = freeze_asof
    scores_frozen = scores[(scores["date"] >= start) & (scores["date"] <= end)].copy()
    ticker_universe = set(scores_frozen["ticker"].astype(str).str.upper().str.strip().unique())
    canonical_frozen = canonical[
        (canonical["date"] >= start)
        & (canonical["date"] <= end)
        & (canonical["ticker"].isin(ticker_universe))
    ].copy()
    macro_frozen = macro[(macro["date"] >= start) & (macro["date"] <= end)].copy()

    frames = {
        "canonical": canonical_frozen,
        "macro": macro_frozen,
        "scores": scores_frozen,
    }
    for key, path in FILES.items():
        frames[key].to_parquet(path, index=False, compression="zstd")

    manifest = {
        "task_id": "T-RESEARCH-DATASET-FREEZE-US-V2",
        "dataset_version": "v2",
        "freeze_asof": str(end.date()),
        "window": {"start": str(start.date()), "end": str(end.date())},
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_output("rev-parse", "HEAD"),
        "git_describe": _git_output("describe", "--always", "--dirty"),
        "files": {},
        "tables": {key: _table_meta(df) for key, df in frames.items()},
        "notes": [
            "Dataset congelado para reprodutibilidade de backtests US.",
            "Fonte original: SSOT vivo no momento do freeze; insumos historicos do selamento de marco nao sao recuperaveis por git.",
        ],
    }
    for key, path in FILES.items():
        manifest["files"][path.name] = {
            "sha256": _sha256(path),
            "size_bytes": int(path.stat().st_size),
        }

    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "freeze_asof": manifest["freeze_asof"], "files": manifest["files"]}, indent=2))


if __name__ == "__main__":
    main()
