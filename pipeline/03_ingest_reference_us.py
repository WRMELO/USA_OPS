"""Step 03 — ingest reference/index US."""
from __future__ import annotations

import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _ensure_index_compositions_from_existing_data() -> None:
    out_path = ROOT / "data" / "ssot" / "index_compositions.parquet"
    if out_path.exists():
        return

    # Fallback técnico: se o artefato de composição não existir,
    # criamos snapshot mínimo a partir dos tickers já presentes no raw.
    raw_path = ROOT / "data" / "ssot" / "us_market_data_raw.parquet"
    if not raw_path.exists():
        raise FileNotFoundError(f"Input ausente para fallback de composição: {raw_path}")
    raw = pd.read_parquet(raw_path, columns=["ticker"]).copy()
    tickers = sorted({str(t).strip().upper() for t in raw["ticker"].dropna().tolist() if str(t).strip()})
    if not tickers:
        raise RuntimeError("Não foi possível derivar index_compositions: universo vazio.")
    today = pd.Timestamp(datetime.now(tz=UTC).date())
    comp = pd.DataFrame(
        {
            "date": today,
            "ticker": tickers,
            "is_member": True,
            "effective_from": today,
            "effective_to": pd.NaT,
            "primary_exchange": "UNK",
            "source": "fallback_from_us_market_data_raw",
        }
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    comp.to_parquet(out_path, index=False)


def run(end_date: date | None = None) -> dict:
    _ensure_index_compositions_from_existing_data()

    cmd = [
        str(sys.executable),
        str(ROOT / "scripts" / "t008a_ingest_ticker_reference_us.py"),
        "--workspace",
        str(ROOT),
        "--chunk-size",
        "200",
        "--max-workers",
        "12",
    ]
    subprocess.run(cmd, check=True, cwd=str(ROOT))

    return {
        "status": "ok",
        "end_date": str(end_date) if end_date else None,
        "index_compositions_path": "data/ssot/index_compositions.parquet",
        "ticker_reference_path": "data/ssot/ticker_reference_us.parquet",
    }
