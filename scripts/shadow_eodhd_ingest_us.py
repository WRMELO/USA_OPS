#!/usr/bin/env python3
"""Builds a sandbox shadow ingest US run using local EODHD base."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Shadow ingest US via local EODHD base.")
    parser.add_argument("--workspace", default=".", help="USA_OPS workspace root.")
    parser.add_argument("--sandbox-rel", default="data/shadow_eodhd_ws", help="Sandbox path relative to workspace.")
    parser.add_argument("--base-cutoff", default="2026-07-24", help="Last official date kept before EODHD tail.")
    parser.add_argument("--tail-start", default="2026-07-25", help="First date from EODHD tail.")
    parser.add_argument("--tail-end", default="2026-07-29", help="Last date from EODHD tail.")
    parser.add_argument(
        "--snapshot-pre",
        default="/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/relatorios/sombra_snapshot_pre.json",
        help="Snapshot JSON with pre-run official hashes.",
    )
    parser.add_argument(
        "--snapshot-pos",
        default="/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/relatorios/sombra_snapshot_pos.json",
        help="Path to write post-run snapshot hashes.",
    )
    parser.add_argument(
        "--report-path",
        default="/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/relatorios/sombra_shadow_exec_us.json",
        help="Execution report output.",
    )
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_payload(files: list[Path]) -> dict:
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "files": [],
        "sha256": {},
        "mtime_epoch": {},
    }
    for path in files:
        stat = path.stat()
        item = {
            "path": str(path),
            "size_bytes": stat.st_size,
            "mtime_epoch": stat.st_mtime,
            "mtime_iso_utc": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
            "sha256": _sha256(path),
        }
        payload["files"].append(item)
        payload["sha256"][str(path)] = item["sha256"]
        payload["mtime_epoch"][str(path)] = item["mtime_epoch"]
    return payload


def _run_logged(cmd: list[str], cwd: Path, log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        subprocess.run(cmd, cwd=str(cwd), check=True, stdout=log, stderr=log)


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    sandbox = workspace / args.sandbox_rel
    script_dir = workspace / "scripts"

    official_files = [
        workspace / "data/ssot/us_market_data_raw.parquet",
        workspace / "data/ssot/operational_market_data_raw.parquet",
        workspace / "data/ssot/canonical_us.parquet",
        workspace / "data/ssot/operational_window.parquet",
    ]
    for path in official_files:
        if not path.exists():
            raise FileNotFoundError(f"Artefato oficial ausente: {path}")

    pre_snapshot_path = Path(args.snapshot_pre)
    if not pre_snapshot_path.exists():
        raise FileNotFoundError(f"Snapshot pre ausente: {pre_snapshot_path}")
    pre_snapshot = json.loads(pre_snapshot_path.read_text(encoding="utf-8"))

    # Build/rebuild sandbox workspace.
    if sandbox.exists():
        shutil.rmtree(sandbox)
    (sandbox / "data/ssot").mkdir(parents=True, exist_ok=True)
    (sandbox / "logs").mkdir(parents=True, exist_ok=True)
    (sandbox / "config").mkdir(parents=True, exist_ok=True)

    # Copy lib and optional env to satisfy scripts that import from workspace/lib.
    shutil.copytree(workspace / "lib", sandbox / "lib")
    env_src = workspace / ".env"
    if env_src.exists():
        shutil.copy2(env_src, sandbox / ".env")

    # Copy direct dependencies read via --workspace defaults.
    for rel in [
        "data/ssot/ticker_reference_us.parquet",
        "data/ssot/macro_us.parquet",
    ]:
        src = workspace / rel
        if src.exists():
            dst = sandbox / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)

    # Build shadow raw: official <= cutoff + EODHD tail.
    if str(workspace) not in sys.path:
        sys.path.insert(0, str(workspace))
    from lib.eodhd_source_us import load_incremental_rows_from_eodhd

    cutoff = date.fromisoformat(args.base_cutoff)
    tail_start = date.fromisoformat(args.tail_start)
    tail_end = date.fromisoformat(args.tail_end)
    if tail_start > tail_end:
        raise ValueError("tail_start nao pode ser maior que tail_end")

    raw_official = pd.read_parquet(workspace / "data/ssot/us_market_data_raw.parquet").copy()
    raw_official["date"] = pd.to_datetime(raw_official["date"], errors="coerce")
    raw_official["ticker"] = raw_official["ticker"].astype(str).str.upper().str.strip()
    raw_official = raw_official.dropna(subset=["date", "ticker"])
    official_cols = list(raw_official.columns)

    base_shadow = raw_official[raw_official["date"] <= pd.Timestamp(cutoff)].copy()
    tickers = sorted(raw_official["ticker"].dropna().unique().tolist())
    last_by_ticker = (
        base_shadow.groupby("ticker", as_index=True)["date"].max().to_dict()
        if not base_shadow.empty
        else {}
    )
    ticker_last_dates = {k: v.date() for k, v in last_by_ticker.items()}

    eodhd_rows = load_incremental_rows_from_eodhd(
        tickers=tickers,
        ticker_last_dates=ticker_last_dates,
        end_date=tail_end,
    )
    eodhd_rows = eodhd_rows.copy()
    eodhd_rows["date"] = pd.to_datetime(eodhd_rows["date"], errors="coerce")
    eodhd_rows = eodhd_rows[
        (eodhd_rows["date"] >= pd.Timestamp(tail_start))
        & (eodhd_rows["date"] <= pd.Timestamp(tail_end))
    ].copy()

    for col in official_cols:
        if col not in eodhd_rows.columns:
            eodhd_rows[col] = pd.NA
    eodhd_rows = eodhd_rows[official_cols]

    shadow_raw = pd.concat([base_shadow[official_cols], eodhd_rows], ignore_index=True)
    shadow_raw["ingested_at"] = pd.to_datetime(shadow_raw["ingested_at"], errors="coerce", utc=True)
    shadow_raw = shadow_raw.sort_values(["ticker", "date", "ingested_at"], na_position="last")
    shadow_raw = shadow_raw.drop_duplicates(subset=["date", "ticker"], keep="last").reset_index(drop=True)

    shadow_raw_path = sandbox / "data/ssot/us_market_data_raw.parquet"
    shadow_raw.to_parquet(shadow_raw_path, index=False)

    # Run canonical build chain on sandbox.
    python_bin = workspace / ".venv/bin/python"
    steps = [
        (
            "t008",
            [
                str(python_bin),
                str(script_dir / "t008_quality_spc_and_blacklist_v2.py"),
                "--workspace",
                str(sandbox),
                "--out-blacklist",
                "data/ssot/blacklist_us.json",
                "--out-report",
                "data/ssot/t008v2_quality_report.json",
                "--tmp-dir",
                "data/ssot/tmp_t008_chunks",
            ],
            sandbox / "logs/t008_shadow.log",
        ),
        (
            "t009",
            [
                str(python_bin),
                str(script_dir / "t009_exclude_bdrs_v2.py"),
                "--workspace",
                str(sandbox),
            ],
            sandbox / "logs/t009_shadow.log",
        ),
        (
            "t010",
            [
                str(python_bin),
                str(script_dir / "t010_build_canonical_us_v2.py"),
                "--workspace",
                str(sandbox),
            ],
            sandbox / "logs/t010_shadow.log",
        ),
    ]
    for _, cmd, log_path in steps:
        _run_logged(cmd, cwd=workspace, log_path=log_path)

    # Post snapshot and contamination check.
    post_snapshot = _snapshot_payload(official_files)
    post_snapshot_path = Path(args.snapshot_pos)
    post_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    post_snapshot_path.write_text(json.dumps(post_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

    pre_sha = pre_snapshot.get("sha256", {})
    post_sha = post_snapshot.get("sha256", {})
    changed = []
    for key, pre_hash in pre_sha.items():
        cur_hash = post_sha.get(key)
        if cur_hash != pre_hash:
            changed.append({"path": key, "pre": pre_hash, "post": cur_hash})
    if changed:
        raise RuntimeError(f"Contaminacao detectada nos artefatos oficiais: {changed}")

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "workspace": str(workspace),
        "sandbox": str(sandbox),
        "base_cutoff": args.base_cutoff,
        "tail_start": args.tail_start,
        "tail_end": args.tail_end,
        "inputs": {
            "official_raw_rows": int(len(raw_official)),
            "base_shadow_rows": int(len(base_shadow)),
            "eodhd_tail_rows": int(len(eodhd_rows)),
            "shadow_raw_rows": int(len(shadow_raw)),
            "tickers_total": int(len(tickers)),
        },
        "outputs": {
            "shadow_raw_path": str(shadow_raw_path),
            "shadow_canonical_path": str(sandbox / "data/ssot/canonical_us.parquet"),
            "shadow_blacklist_path": str(sandbox / "data/ssot/blacklist_us.json"),
            "shadow_bdr_exclusion_path": str(sandbox / "data/ssot/bdr_exclusion_list.json"),
        },
        "logs": {
            "t008": str(sandbox / "logs/t008_shadow.log"),
            "t009": str(sandbox / "logs/t009_shadow.log"),
            "t010": str(sandbox / "logs/t010_shadow.log"),
        },
        "official_integrity": {
            "pre_snapshot": str(pre_snapshot_path),
            "post_snapshot": str(post_snapshot_path),
            "changed": changed,
        },
    }
    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "ok", "report_path": str(report_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
