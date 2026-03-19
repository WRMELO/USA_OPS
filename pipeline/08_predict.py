"""Step 08 — predict stub (motor C4 puro sem ML trigger)."""
from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def run(end_date: date | None = None) -> pd.DataFrame:
    ds_path = ROOT / "data" / "features" / "dataset_us.parquet"
    out_path = ROOT / "data" / "features" / "predictions_us.parquet"
    if not ds_path.exists():
        raise FileNotFoundError(f"Input ausente: {ds_path}")

    ds = pd.read_parquet(ds_path, columns=["date"]).copy()
    ds["date"] = pd.to_datetime(ds["date"], errors="coerce").dt.normalize()
    ds = ds.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last").sort_values("date")

    if end_date:
        ds = ds[ds["date"] <= pd.Timestamp(end_date)].copy()
    if ds.empty:
        raise RuntimeError("dataset_us vazio para gerar predição stub.")

    pred = pd.DataFrame(
        {
            "date": ds["date"],
            "y_proba_cash": 0.0,
            "model_name": "C4_PURE_STUB_NO_ML",
            "generated_at": datetime.now(tz=UTC).isoformat(),
        }
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pred.to_parquet(out_path, index=False)
    return pred
