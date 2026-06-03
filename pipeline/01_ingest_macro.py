"""Step 01 — ingest macro US (wrapper de T-011v2)."""
from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from lib.trading_calendar import prev_session

ROOT = Path(__file__).resolve().parents[1]
BASE_SERIES = [
    "vix_close",
    "usd_index_broad",
    "ust_10y_yield",
    "ust_2y_yield",
    "fed_funds_rate",
    "hy_oas",
    "ig_oas",
]


def _normalize_target_date(end_date: date | None) -> pd.Timestamp:
    run_day = end_date or date.today()
    return pd.Timestamp(prev_session(run_day, exchange="XNYS")).normalize()


def _extend_macro_with_ffill(macro_path: Path, target_day: pd.Timestamp) -> tuple[pd.DataFrame, bool]:
    macro_df = pd.read_parquet(macro_path).copy()
    if macro_df.empty:
        raise RuntimeError("Fallback indisponivel: macro_us.parquet vazio.")

    macro_df["date"] = pd.to_datetime(macro_df["date"], errors="coerce").dt.normalize()
    macro_df = macro_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if macro_df.empty:
        raise RuntimeError("Fallback indisponivel: macro_us.parquet sem datas validas.")

    last_day = pd.Timestamp(macro_df["date"].iloc[-1]).normalize()
    extended = False
    if target_day > last_day:
        new_row = macro_df.iloc[[-1]].copy()
        new_row.loc[:, "date"] = target_day
        macro_df = pd.concat([macro_df, new_row], ignore_index=True)
        macro_df = macro_df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
        extended = True

    macro_df.to_parquet(macro_path, index=False)
    return macro_df, extended


def _rebuild_features_from_macro(macro_df: pd.DataFrame, features_path: Path) -> None:
    missing = [col for col in BASE_SERIES if col not in macro_df.columns]
    if missing:
        raise RuntimeError(f"Fallback indisponivel: colunas ausentes em macro_us.parquet: {missing}")

    out = pd.DataFrame({"date": pd.to_datetime(macro_df["date"], errors="coerce").dt.normalize()})
    feature_cols: list[str] = []
    for alias in BASE_SERIES:
        level_col = f"feature_{alias}_level"
        diff_col = f"feature_{alias}_diff_1d"
        pct_col = f"feature_{alias}_pct_1d"
        series = pd.to_numeric(macro_df[alias], errors="coerce")
        out[level_col] = series
        out[diff_col] = series.diff(1)
        out[pct_col] = series.pct_change(1)
        feature_cols.extend([level_col, diff_col, pct_col])

    out[feature_cols] = out[feature_cols].shift(1)
    cutoff = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize("UTC")
    out["feature_timestamp_cutoff"] = cutoff - pd.Timedelta(days=1) + pd.Timedelta(hours=23, minutes=59, seconds=59)
    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    features_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(features_path, index=False)


def run(end_date: date | None = None) -> dict:
    # T-011v2 já materializa macro SSOT + macro_features com regras de lookahead.
    cmd = [
        str(sys.executable),
        str(ROOT / "scripts" / "t011_ingest_macro_us_v2.py"),
        "--workspace",
        str(ROOT),
    ]
    try:
        subprocess.run(cmd, check=True, cwd=str(ROOT))
        return {
            "status": "ok",
            "end_date": str(end_date) if end_date else None,
            "macro_path": "data/ssot/macro_us.parquet",
            "macro_features_path": "data/features/macro_features_us.parquet",
        }
    except Exception as exc:  # noqa: BLE001
        try:
            macro_path = ROOT / "data" / "ssot" / "macro_us.parquet"
            features_path = ROOT / "data" / "features" / "macro_features_us.parquet"
            if not macro_path.exists():
                raise RuntimeError(f"Fallback indisponivel: arquivo ausente {macro_path}") from exc

            target_day = _normalize_target_date(end_date)
            macro_df, extended = _extend_macro_with_ffill(macro_path, target_day)
            _rebuild_features_from_macro(macro_df, features_path)
            macro_date_max = pd.to_datetime(macro_df["date"], errors="coerce").max()
            msg = (
                f"FRED fallback ativado em {target_day.date()}: "
                f"{'macro_us extendido com ffill' if extended else 'macro_us reutilizado sem extensao'}."
            )
            print(f"[WARN] Step 01 macro US falhou: {exc!r}")
            print(f"[WARN] {msg}")
            subprocess.run(["notify-send", "USA OPS", msg], check=False)
            return {
                "status": "macro_fallback",
                "reason": str(exc),
                "end_date": str(end_date) if end_date else None,
                "macro_path": "data/ssot/macro_us.parquet",
                "macro_features_path": "data/features/macro_features_us.parquet",
                "fallback_target_date": str(target_day.date()),
                "fallback_extended": extended,
                "macro_date_max": str(macro_date_max.date()) if pd.notna(macro_date_max) else None,
            }
        except Exception as fallback_exc:  # noqa: BLE001
            raise RuntimeError(f"Step 01 macro US falhou e fallback tambem falhou: {fallback_exc}") from exc
