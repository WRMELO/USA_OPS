"""Step 00 — ingestão incremental + rebuild da janela operacional (D-026).

Objetivo: garantir que `operational_window.parquet` esteja atualizado até D-1 do `target_date`,
com mecanismo de recuperação de gaps.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
WINDOW_TRADING_DAYS = 504


def _date_max(path: Path) -> pd.Timestamp | None:
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["date"]).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    if df["date"].dropna().empty:
        return None
    return pd.Timestamp(df["date"].max()).normalize()


def _tail_dates(path: Path, n: int) -> list[pd.Timestamp]:
    df = pd.read_parquet(path, columns=["date"]).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    dates = sorted(df["date"].dropna().drop_duplicates().tolist())
    return [pd.Timestamp(d).normalize() for d in dates[-n:]]


def _tickers_from_parquet(path: Path, col: str = "ticker") -> list[str]:
    df = pd.read_parquet(path, columns=[col]).copy()
    tickers = (
        df[col]
        .astype(str)
        .str.upper()
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    return sorted(tickers)


def _trim_to_last_n_dates(df: pd.DataFrame, n: int) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    keep = sorted(df["date"].dropna().drop_duplicates().tolist())[-n:]
    return df[df["date"].isin(set(keep))].copy()


def run(target_date: date) -> dict:
    canonical_full = ROOT / "data" / "ssot" / "canonical_us.parquet"
    raw_full = ROOT / "data" / "ssot" / "us_market_data_raw.parquet"

    op_raw = ROOT / "data" / "ssot" / "operational_market_data_raw.parquet"
    op_raw_delta = ROOT / "data" / "ssot" / "operational_market_data_raw_delta.parquet"

    op_universe = ROOT / "data" / "ssot" / "us_universe_operational_window.parquet"
    op_blacklist = ROOT / "data" / "ssot" / "blacklist_window_us.json"
    op_bdr_excl = ROOT / "data" / "ssot" / "bdr_exclusion_list_window.json"
    op_canonical = ROOT / "data" / "ssot" / "operational_window.parquet"

    report_path = ROOT / "logs" / "operational_incremental_ingest.json"
    delta_report = ROOT / "logs" / "t007_ingestion_report_delta.json"
    delta_failures = ROOT / "logs" / "t007_failures_delta.json"
    deactivated_artifacts: list[str] = []

    if not canonical_full.exists():
        raise FileNotFoundError(f"Input ausente: {canonical_full}")
    if not raw_full.exists():
        raise FileNotFoundError(f"Input ausente: {raw_full}")

    # Bootstrap (se ainda não existe parquet operacional raw)
    if not op_raw.exists():
        dates_keep = _tail_dates(canonical_full, WINDOW_TRADING_DAYS)
        tickers_keep = _tickers_from_parquet(canonical_full, col="ticker")
        raw = pd.read_parquet(raw_full).copy()
        raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.normalize()
        raw["ticker"] = raw["ticker"].astype(str).str.upper().str.strip()
        raw = raw.dropna(subset=["date", "ticker"])
        raw = raw[raw["date"].isin(set(dates_keep))].copy()
        raw = raw[raw["ticker"].isin(set(tickers_keep))].copy()
        raw = raw.sort_values(["ticker", "date", "ingested_at"]).drop_duplicates(subset=["date", "ticker"], keep="last")
        op_raw.parent.mkdir(parents=True, exist_ok=True)
        raw.to_parquet(op_raw, index=False)

    # Ingestão incremental até D-1 (calendário).
    target_end = (pd.Timestamp(target_date) - pd.Timedelta(days=1)).normalize().date()
    last_dt = _date_max(op_raw)
    start_dt = (last_dt + pd.Timedelta(days=1)).date() if last_dt is not None else target_end

    delta_rows = 0
    if start_dt <= target_end:
        # Chama T-007 em modo delta (range curto) e restringe tickers ao universo do canonical.
        subprocess.run(
            [
                str(sys.executable),
                str(ROOT / "scripts" / "t007_ingest_us_market_data_raw.py"),
                "--workspace",
                str(ROOT),
                "--start-date",
                str(start_dt),
                "--end-date",
                str(target_end),
                "--tickers-parquet",
                "data/ssot/canonical_us.parquet",
                "--tickers-column",
                "ticker",
                "--chunk-size",
                "200",
                "--max-workers",
                "12",
                "--out-path",
                str(op_raw_delta.relative_to(ROOT)),
                "--report-path",
                "logs/t007_ingestion_report_delta.json",
                "--failures-path",
                "logs/t007_failures_delta.json",
            ],
            check=True,
            cwd=str(ROOT),
        )

        if op_raw_delta.exists():
            delta = pd.read_parquet(op_raw_delta).copy()
            delta_rows = int(len(delta))
            if not delta.empty:
                base = pd.read_parquet(op_raw).copy()
                base = pd.concat([base, delta], ignore_index=True)
                base["date"] = pd.to_datetime(base["date"], errors="coerce").dt.normalize()
                base["ticker"] = base["ticker"].astype(str).str.upper().str.strip()
                base = base.dropna(subset=["date", "ticker"])
                if "ingested_at" in base.columns:
                    base = base.sort_values(["ticker", "date", "ingested_at"])
                else:
                    base = base.sort_values(["ticker", "date"])
                base = base.drop_duplicates(subset=["date", "ticker"], keep="last")
                base = _trim_to_last_n_dates(base, WINDOW_TRADING_DAYS)
                base.to_parquet(op_raw, index=False)
        # Delta é transitório: após merge, desativa artefato para evitar confusão.
        if op_raw_delta.exists():
            op_raw_delta.unlink()
            deactivated_artifacts.append(str(op_raw_delta))
        if delta_report.exists():
            delta_report.unlink()
            deactivated_artifacts.append(str(delta_report))
        if delta_failures.exists():
            delta_failures.unlink()
            deactivated_artifacts.append(str(delta_failures))

    # Higiene defensiva: remove resíduos delta de execuções anteriores,
    # mesmo quando não há novo range incremental no dia.
    if op_raw_delta.exists():
        op_raw_delta.unlink()
        deactivated_artifacts.append(str(op_raw_delta))
    if delta_report.exists():
        delta_report.unlink()
        deactivated_artifacts.append(str(delta_report))
    if delta_failures.exists():
        delta_failures.unlink()
        deactivated_artifacts.append(str(delta_failures))

    tmp_dir = ROOT / "data" / "ssot" / "tmp_t008_window_chunks"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
        deactivated_artifacts.append(str(tmp_dir))
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Rebuild da janela operacional (SPC + BDR excl + canonical window)
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

    out = pd.read_parquet(op_canonical, columns=["date", "ticker"]).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    payload = {
        "task_id": "T-034",
        "step": "00_incremental_ingest",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "target_date": str(target_date),
        "target_end_date_d_minus_1": str(target_end),
        "window_trading_days": WINDOW_TRADING_DAYS,
        "delta": {
            "start_date": str(start_dt),
            "end_date": str(target_end),
            "delta_rows": int(delta_rows),
            "delta_path": str(op_raw_delta),
        },
        "deactivated_artifacts": deactivated_artifacts,
        "outputs": {
            "operational_raw": str(op_raw),
            "operational_universe": str(op_universe),
            "operational_blacklist": str(op_blacklist),
            "operational_canonical": str(op_canonical),
        },
        "counts": {
            "date_max": str(out["date"].max().date()) if not out.empty else None,
            "dates": int(out["date"].nunique()),
            "tickers": int(out["ticker"].nunique()),
            "rows": int(len(out)),
        },
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload

