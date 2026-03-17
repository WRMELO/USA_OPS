#!/usr/bin/env python3
"""T-008av2: ingest ticker reference data (details + events) with resumable chunks."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from polygon import RESTClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-008av2 reference data ingestion")
    parser.add_argument("--workspace", default=".", help="Workspace root")
    parser.add_argument("--chunk-size", type=int, default=200, help="Tickers per chunk")
    parser.add_argument("--max-workers", type=int, default=12, help="Parallel workers per chunk")
    parser.add_argument("--timeout-seconds", type=float, default=20.0, help="Polygon timeout")
    parser.add_argument("--max-retries", type=int, default=5, help="Retry attempts per endpoint")
    return parser.parse_args()


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


class RefClient:
    def __init__(self, api_key: str, timeout_seconds: float, max_retries: int) -> None:
        self.client = RESTClient(
            api_key=api_key,
            trace=False,
            connect_timeout=timeout_seconds,
            read_timeout=timeout_seconds,
        )
        self.max_retries = max_retries
        self.retries_total = 0

    @staticmethod
    def _is_non_retryable(exc: Exception) -> bool:
        msg = str(exc).lower()
        markers = [
            "status 400",
            "status 404",
            "bad request",
            "not found",
            "does not exist",
            "unknown ticker",
            "invalid ticker",
        ]
        return any(m in msg for m in markers)

    def _retry(self, fn, label: str):
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if self._is_non_retryable(exc):
                    raise RuntimeError(f"Falha não-recuperável em {label}: {exc}") from exc
                if attempt == self.max_retries:
                    break
                self.retries_total += 1
                wait_s = min(2**attempt, 60)
                print(f"[T-008av2] retry {attempt}/{self.max_retries} for {label} in {wait_s}s")
                time.sleep(float(wait_s))
        raise RuntimeError(f"Falha Polygon em {label}") from last_exc

    def get_details(self, ticker: str):
        return self._retry(lambda: self.client.get_ticker_details(ticker), f"details:{ticker}")

    def get_events(self, ticker: str):
        return self._retry(lambda: self.client.get_ticker_events(ticker=ticker, types="ticker_change"), f"events:{ticker}")


def to_json_safe(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, default=str)


def worker_fetch(
    ticker: str,
    api_key: str,
    timeout_seconds: float,
    max_retries: int,
) -> tuple[dict[str, Any], dict[str, Any] | None, int]:
    client = RefClient(api_key=api_key, timeout_seconds=timeout_seconds, max_retries=max_retries)
    ingested_at = datetime.now(tz=UTC).isoformat()
    asof_date = datetime.now(tz=UTC).date().isoformat()

    try:
        details = client.get_details(ticker)
        events = client.get_events(ticker)

        ticker_changes = []
        if hasattr(events, "events") and events.events:
            ticker_changes = events.events

        row = {
            "ticker": ticker,
            "asof_date": asof_date,
            "active": getattr(details, "active", pd.NA),
            "list_date": str(getattr(details, "list_date", pd.NA)) if getattr(details, "list_date", None) else pd.NA,
            "delisted_utc": str(getattr(details, "delisted_utc", pd.NA))
            if getattr(details, "delisted_utc", None)
            else pd.NA,
            "primary_exchange": getattr(details, "primary_exchange", pd.NA),
            "type": getattr(details, "type", pd.NA),
            "market_cap": getattr(details, "market_cap", pd.NA),
            "ticker_changes_json": to_json_safe(ticker_changes),
            "source": "polygon_ticker_details_v3+events",
            "ingested_at": ingested_at,
            "fetch_status": "OK",
            "error": pd.NA,
        }
        return row, None, client.retries_total
    except Exception as exc:  # noqa: BLE001
        failure = {
            "ticker": ticker,
            "endpoint": "details/events",
            "final_attempt": max_retries,
            "error": str(exc)[:600],
            "stack_summary": repr(exc)[:600],
        }
        row = {
            "ticker": ticker,
            "asof_date": asof_date,
            "active": pd.NA,
            "list_date": pd.NA,
            "delisted_utc": pd.NA,
            "primary_exchange": pd.NA,
            "type": pd.NA,
            "market_cap": pd.NA,
            "ticker_changes_json": "[]",
            "source": "polygon_ticker_details_v3+events",
            "ingested_at": ingested_at,
            "fetch_status": "FAIL",
            "error": str(exc)[:600],
        }
        return row, failure, client.retries_total


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    if str(workspace) not in sys.path:
        sys.path.insert(0, str(workspace))

    from lib.io import read_parquet, write_json, write_parquet

    load_dotenv(workspace / ".env")
    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY ausente no ambiente/.env.")

    index_path = workspace / "data/ssot/index_compositions.parquet"
    if not index_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {index_path}")

    df_universe = read_parquet(index_path)
    tickers = sorted({str(t).strip().upper() for t in df_universe["ticker"].dropna().tolist() if str(t).strip()})
    total_tickers = len(tickers)
    if total_tickers == 0:
        raise RuntimeError("Universo vazio em index_compositions.parquet.")

    chunk_dir = workspace / "data/ssot/tmp_t008a_chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    out_parquet = workspace / "data/ssot/ticker_reference_us.parquet"
    out_report = workspace / "data/ssot/t008a_reference_report.json"
    out_failures = workspace / "data/ssot/t008a_reference_failures.json"

    chunk_size = max(1, int(args.chunk_size))
    max_workers = max(1, int(args.max_workers))
    total_chunks = (total_tickers + chunk_size - 1) // chunk_size

    started = time.time()
    attempted = 0
    succeeded = 0
    failed = 0
    skipped_by_resume = 0
    retries_total = 0
    failures: list[dict[str, Any]] = []

    for chunk_idx in range(total_chunks):
        left = chunk_idx * chunk_size
        right = min(left + chunk_size, total_tickers)
        batch = tickers[left:right]
        chunk_file = chunk_dir / f"chunk_{chunk_idx:05d}.parquet"

        if chunk_file.exists():
            prev = pd.read_parquet(chunk_file)
            prev_rows = len(prev)
            attempted += prev_rows
            succeeded += int((prev["fetch_status"] == "OK").sum()) if "fetch_status" in prev.columns else prev_rows
            failed += int((prev["fetch_status"] == "FAIL").sum()) if "fetch_status" in prev.columns else 0
            skipped_by_resume += prev_rows
            continue

        rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    worker_fetch,
                    ticker=tkr,
                    api_key=api_key,
                    timeout_seconds=float(args.timeout_seconds),
                    max_retries=int(args.max_retries),
                ): tkr
                for tkr in batch
            }
            for future in as_completed(futures):
                row, failure, retries = future.result()
                rows.append(row)
                retries_total += int(retries)
                attempted += 1
                if row["fetch_status"] == "OK":
                    succeeded += 1
                else:
                    failed += 1
                if failure is not None:
                    failures.append(failure)

                elapsed = max(time.time() - started, 1e-9)
                rate = attempted / elapsed
                remaining = max(total_tickers - attempted, 0)
                eta_s = int(remaining / rate) if rate > 0 else -1
                print(
                    f"[T-008av2] {attempted}/{total_tickers} "
                    f"({(attempted / total_tickers) * 100:.2f}%) "
                    f"ok={succeeded} fail={failed} eta_s={eta_s}"
                )

        chunk_df = pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)
        write_parquet(chunk_df, chunk_file)

    # Consolidation
    chunk_files = sorted(chunk_dir.glob("chunk_*.parquet"))
    all_df = pd.concat([pd.read_parquet(p) for p in chunk_files], ignore_index=True) if chunk_files else pd.DataFrame()
    all_df["ticker"] = all_df["ticker"].astype(str).str.strip().str.upper()
    all_df["ingested_at"] = pd.to_datetime(all_df["ingested_at"], errors="coerce", utc=True)
    all_df = all_df.sort_values(["ticker", "ingested_at"]).drop_duplicates(subset=["ticker"], keep="last")
    all_df = all_df.sort_values("ticker").reset_index(drop=True)

    write_parquet(all_df, out_parquet)
    write_json(failures, out_failures, indent=2)

    unique_tickers_parquet = int(all_df["ticker"].nunique()) if not all_df.empty else 0
    logical_gate_pass = unique_tickers_parquet == total_tickers and len(all_df) == unique_tickers_parquet

    report = {
        "task_id": "T-008av2",
        "decision_ref": "D-007",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "source": "polygon_ticker_details_v3+events",
        "total_tickers_universe": total_tickers,
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
        "success_rate": (succeeded / attempted) if attempted else 0.0,
        "skipped_by_resume": skipped_by_resume,
        "chunks": {"chunk_size": chunk_size, "total_chunks": total_chunks, "materialized_chunks": len(chunk_files)},
        "rows_total": int(len(all_df)),
        "unique_tickers_parquet": unique_tickers_parquet,
        "retries_total": retries_total,
        "runtime_seconds": round(time.time() - started, 3),
        "logical_gate_unique_ticker_row": {
            "passed": logical_gate_pass,
            "universe_tickers": total_tickers,
            "parquet_rows": int(len(all_df)),
            "parquet_unique_tickers": unique_tickers_parquet,
        },
        "failures_sample": failures[:20],
    }
    write_json(report, out_report, indent=2)

    if not logical_gate_pass:
        print("[T-008av2] FAIL logical gate: parquet != universo em cardinalidade de ticker.")
        return 3

    print(f"[T-008av2] wrote {out_parquet}")
    print(f"[T-008av2] wrote {out_report}")
    print(f"[T-008av2] wrote {out_failures}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
