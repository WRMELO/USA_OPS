"""Rebuild `operational_window.parquet` from SSOT full artifacts (D-026).

Uso:
- Rotina semanal (após `--full`): sincroniza o modo diário com o SSOT completo.
- Bootstrap: cria `operational_market_data_raw.parquet` e `operational_window.parquet` se ausentes.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
WINDOW_TRADING_DAYS = 504


def _tail_dates(path: Path, n: int) -> list[pd.Timestamp]:
    df = pd.read_parquet(path, columns=["date"]).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    dates = sorted(df["date"].dropna().drop_duplicates().tolist())
    return [pd.Timestamp(d).normalize() for d in dates[-n:]]


def _tickers_from_canonical(path: Path) -> list[str]:
    df = pd.read_parquet(path, columns=["ticker"]).copy()
    tickers = (
        df["ticker"]
        .astype(str)
        .str.upper()
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    return sorted(tickers)


def run(end_date: date | None = None) -> dict:
    canonical_full = ROOT / "data" / "ssot" / "canonical_us.parquet"
    raw_full = ROOT / "data" / "ssot" / "us_market_data_raw.parquet"

    op_raw = ROOT / "data" / "ssot" / "operational_market_data_raw.parquet"
    op_universe = ROOT / "data" / "ssot" / "us_universe_operational_window.parquet"
    op_blacklist = ROOT / "data" / "ssot" / "blacklist_window_us.json"
    op_bdr_excl = ROOT / "data" / "ssot" / "bdr_exclusion_list_window.json"
    op_canonical = ROOT / "data" / "ssot" / "operational_window.parquet"
    report_path = ROOT / "data" / "ssot" / "operational_window_report.json"

    if not canonical_full.exists():
        raise FileNotFoundError(f"Input ausente: {canonical_full}")
    if not raw_full.exists():
        raise FileNotFoundError(f"Input ausente: {raw_full}")

    dates_keep = _tail_dates(canonical_full, WINDOW_TRADING_DAYS)
    tickers_keep = _tickers_from_canonical(canonical_full)
    if not dates_keep or not tickers_keep:
        raise RuntimeError("Não foi possível derivar janela (dates/tickers vazios).")

    raw = pd.read_parquet(raw_full).copy()
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.normalize()
    raw["ticker"] = raw["ticker"].astype(str).str.upper().str.strip()
    raw = raw.dropna(subset=["date", "ticker"])
    raw = raw[raw["date"].isin(dates_keep)].copy()
    raw = raw[raw["ticker"].isin(set(tickers_keep))].copy()
    raw = raw.sort_values(["ticker", "date", "ingested_at"]).drop_duplicates(subset=["date", "ticker"], keep="last")
    op_raw.parent.mkdir(parents=True, exist_ok=True)
    raw.to_parquet(op_raw, index=False)

    tmp_dir = ROOT / "data" / "ssot" / "tmp_t008_window_chunks"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            str(sys.executable),
            str(ROOT / "scripts" / "t008_quality_spc_and_blacklist_v2.py"),
            "--workspace",
            str(ROOT),
            "--raw-path",
            str(op_raw.relative_to(ROOT)),
            "--ref-path",
            "data/ssot/ticker_reference_us.parquet",
            "--out-parquet",
            str(op_universe.relative_to(ROOT)),
            "--out-blacklist",
            str(op_blacklist.relative_to(ROOT)),
            "--out-report",
            "data/ssot/t008v2_quality_report_window.json",
            "--tmp-dir",
            str(tmp_dir.relative_to(ROOT)),
            "--chunk-size",
            "250",
            "--max-workers",
            "10",
        ],
        check=True,
        cwd=str(ROOT),
    )

    subprocess.run(
        [
            str(sys.executable),
            str(ROOT / "scripts" / "t009_exclude_bdrs_v2.py"),
            "--workspace",
            str(ROOT),
            "--in-us-universe",
            str(op_universe.relative_to(ROOT)),
            "--out-exclusion-json",
            str(op_bdr_excl.relative_to(ROOT)),
            "--out-report-json",
            "data/ssot/t009v2_bdr_exclusion_report_window.json",
        ],
        check=True,
        cwd=str(ROOT),
    )

    subprocess.run(
        [
            str(sys.executable),
            str(ROOT / "scripts" / "t010_build_canonical_us_v2.py"),
            "--workspace",
            str(ROOT),
            "--in-operational",
            str(op_universe.relative_to(ROOT)),
            "--in-bdr-exclusion",
            str(op_bdr_excl.relative_to(ROOT)),
            "--out-canonical",
            str(op_canonical.relative_to(ROOT)),
            "--out-report",
            "data/ssot/t010v2_operational_window_report.json",
        ],
        check=True,
        cwd=str(ROOT),
    )

    # Report mínimo
    out = pd.read_parquet(op_canonical, columns=["date", "ticker"]).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    payload = {
        "task_id": "T-034",
        "step": "rebuild_operational_window",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "window_trading_days": WINDOW_TRADING_DAYS,
        "inputs": {
            "canonical_full": str(canonical_full),
            "raw_full": str(raw_full),
        },
        "outputs": {
            "operational_raw": str(op_raw),
            "operational_universe": str(op_universe),
            "operational_blacklist": str(op_blacklist),
            "operational_canonical": str(op_canonical),
        },
        "counts": {
            "dates": int(out["date"].nunique()),
            "tickers": int(out["ticker"].nunique()),
            "rows": int(len(out)),
            "date_max": str(out["date"].max().date()) if not out.empty else None,
        },
    }
    report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload

