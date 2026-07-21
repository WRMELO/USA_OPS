"""Diagnostico de redundancia para T-SDC-BANDEXP-R037-MATERIALITY-ENTRY-US-V1."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OBS_PATH = (
    ROOT
    / "backtest"
    / "t_bandexp_r037_materiality_entry_us_v1"
    / "results"
    / "observations_bandexp_r037_materiality_entry_us_v1.csv"
)
OUT_JSON = (
    ROOT
    / "backtest"
    / "t_bandexp_r037_materiality_entry_us_v1"
    / "results"
    / "redundancy_diagnostics_bandexp_r037_materiality_entry_us_v1.json"
)
OUT_CSV = (
    ROOT
    / "backtest"
    / "t_bandexp_r037_materiality_entry_us_v1"
    / "results"
    / "substitute_rank_stats_bandexp_r037_materiality_entry_us_v1.csv"
)


def _tokens(raw: Any) -> list[str]:
    if not isinstance(raw, str) or not raw.strip():
        return []
    return [x.strip().upper() for x in raw.split(";") if x.strip()]


def _pct(num: float, den: float) -> float:
    if den <= 0:
        return float("nan")
    return float((num / den) * 100.0)


def _num_or_none(v: float) -> float | None:
    if np.isfinite(v):
        return float(v)
    return None


def main() -> None:
    if not OBS_PATH.exists():
        raise RuntimeError(f"Observations nao encontrado: {OBS_PATH}")

    obs = pd.read_csv(OBS_PATH)
    holdout = obs[(obs["arm"] == "Arm_Dupla") & (obs["is_holdout"] == 1)].copy()
    veto_events = holdout[pd.to_numeric(holdout["n_veto"], errors="coerce").fillna(0).astype(int) > 0].copy()
    if veto_events.empty:
        raise RuntimeError("Sem eventos de veto no HOLDOUT para Arm_Dupla.")

    n_events = float(len(veto_events))
    with_r001 = 0.0
    full_ret62_cover = 0.0
    total_vetados = 0.0
    total_vetados_ret62 = 0.0

    for r in veto_events.itertuples():
        vetados = _tokens(getattr(r, "tickers_vetados", ""))
        vetados_r001 = _tokens(getattr(r, "vetados_com_r001_ativo", ""))
        vetados_ret62 = _tokens(getattr(r, "vetados_com_ret62_puro", ""))

        if vetados_r001:
            with_r001 += 1.0
        if set(vetados) == set(vetados_ret62):
            full_ret62_cover += 1.0

        total_vetados += float(len(vetados))
        total_vetados_ret62 += float(len(vetados_ret62))

    event_r001_pct = _pct(with_r001, n_events)
    event_ret62_full_cover_pct = _pct(full_ret62_cover, n_events)
    ticker_ret62_coverage_pct = _pct(total_vetados_ret62, total_vetados)

    rank_rows: list[dict[str, Any]] = []
    for split in ["HOLDOUT", "SW1", "SW2"]:
        if split == "HOLDOUT":
            sub = veto_events.copy()
        else:
            sub = veto_events[veto_events["split"] == split].copy()
        x = pd.to_numeric(sub["m3_rank_max_substituto"], errors="coerce")
        x = x[np.isfinite(x)]
        rank_rows.append(
            {
                "split": split,
                "n_events": int(len(sub)),
                "n_events_with_rank": int(len(x)),
                "mean_m3_rank_max_substituto": _num_or_none(float(np.nanmean(x)) if len(x) else float("nan")),
                "median_m3_rank_max_substituto": _num_or_none(float(np.nanmedian(x)) if len(x) else float("nan")),
                "p90_m3_rank_max_substituto": _num_or_none(float(np.nanpercentile(x, 90)) if len(x) else float("nan")),
                "max_m3_rank_max_substituto": _num_or_none(float(np.nanmax(x)) if len(x) else float("nan")),
            }
        )
    rank_df = pd.DataFrame(rank_rows)
    rank_df.to_csv(OUT_CSV, index=False)

    payload = {
        "task_id": "T-SDC-BANDEXP-R037-MATERIALITY-ENTRY-US-V1",
        "subset": "Arm_Dupla HOLDOUT veto_events",
        "n_events": int(n_events),
        "event_overlap_r001_nonempty_pct": event_r001_pct,
        "event_full_cover_ret62_puro_pct": event_ret62_full_cover_pct,
        "ticker_level_ret62_puro_coverage_pct": ticker_ret62_coverage_pct,
        "substitute_rank_stats": rank_rows,
        "artifacts": {
            "observations": str(OBS_PATH.relative_to(ROOT)),
            "rank_stats_csv": str(OUT_CSV.relative_to(ROOT)),
        },
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)

    print("redundancy diagnostics concluido.")
    print(f"n_events={int(n_events)}")
    print(f"event_overlap_r001_nonempty_pct={event_r001_pct:.4f}")
    print(f"event_full_cover_ret62_puro_pct={event_ret62_full_cover_pct:.4f}")
    print(f"ticker_level_ret62_puro_coverage_pct={ticker_ret62_coverage_pct:.4f}")
    print(rank_df.to_string(index=False, float_format=lambda z: f"{z:.4f}"))


if __name__ == "__main__":
    main()
