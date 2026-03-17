#!/usr/bin/env python3
"""T-007v2: ingest OHLCV/dividends/splits with adjusted=False and resumable chunks."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-007v2 ingest US market data raw with chunk resume.")
    parser.add_argument("--workspace", default=".", help="Workspace root")
    parser.add_argument("--start-date", default="2018-01-01", help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="Inclusive end date (YYYY-MM-DD), default=today UTC")
    parser.add_argument("--chunk-size", type=int, default=100, help="Tickers per chunk")
    parser.add_argument("--timeout-seconds", type=float, default=20.0, help="Polygon request timeout")
    parser.add_argument("--max-retries", type=int, default=5, help="Retries per endpoint")
    parser.add_argument("--max-workers", type=int, default=10, help="Parallel workers per chunk")
    return parser.parse_args()


def load_dotenv(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def merge_corporate_events(ohlcv: pd.DataFrame, dividends: pd.DataFrame, splits: pd.DataFrame) -> pd.DataFrame:
    df = ohlcv.copy()
    if dividends.empty:
        df["dividend_rate"] = pd.NA
    else:
        div = dividends.rename(columns={"amount": "dividend_rate"})[["date", "dividend_rate"]]
        df = df.merge(div, on="date", how="left")
    if splits.empty:
        df["split_from"] = pd.NA
        df["split_to"] = pd.NA
    else:
        df = df.merge(splits[["date", "split_from", "split_to"]], on="date", how="left")
    return df


def build_ticker_frame(
    ticker: str,
    start_dt: date,
    end_dt: date,
    api_key: str,
    timeout_seconds: float,
    max_retries: int,
) -> tuple[pd.DataFrame, dict[str, Any] | None, int]:
    from lib.adapters import PolygonAdapter

    adapter = PolygonAdapter(api_key=api_key, timeout_seconds=timeout_seconds, max_retries=max_retries)
    try:
        ohlcv = adapter.get_ohlcv(ticker=ticker, start=start_dt, end=end_dt, adjusted=False)
        dividends = adapter.get_dividends(ticker=ticker, start=start_dt, end=end_dt)
        splits = adapter.get_splits(ticker=ticker, start=start_dt, end=end_dt)
    except Exception as exc:  # noqa: BLE001
        err = str(exc)
        endpoint = "unknown"
        if "aggs:" in err:
            endpoint = "aggs"
        elif "dividends:" in err:
            endpoint = "dividends"
        elif "splits:" in err:
            endpoint = "splits"
        return (
            pd.DataFrame(),
            {
                "ticker": ticker,
                "endpoint": endpoint,
                "final_attempt": max_retries,
                "error": err[:600],
                "stack_summary": repr(exc)[:600],
            },
            int(adapter.retry_events),
        )

    if ohlcv.empty:
        return pd.DataFrame(), None, int(adapter.retry_events)

    merged = merge_corporate_events(ohlcv=ohlcv, dividends=dividends, splits=splits)
    merged["ticker"] = ticker
    merged["source"] = "polygon_aggs_adjusted_false_v2"
    merged["ingested_at"] = pd.Timestamp.now(tz="UTC")
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.normalize()
    return (
        merged[
            [
                "date",
                "ticker",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "dividend_rate",
                "split_from",
                "split_to",
                "source",
                "ingested_at",
            ]
        ],
        None,
        int(adapter.retry_events),
    )


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    if str(workspace) not in sys.path:
        sys.path.insert(0, str(workspace))

    load_dotenv(workspace / ".env")

    from lib.io import read_parquet, write_json, write_parquet

    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY ausente no ambiente/.env.")

    index_path = workspace / "data/ssot/index_compositions.parquet"
    if not index_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {index_path}")

    start_dt = pd.Timestamp(args.start_date).date()
    end_dt = pd.Timestamp(args.end_date).date() if args.end_date else datetime.now(tz=UTC).date()
    if end_dt < start_dt:
        raise ValueError("end-date não pode ser menor que start-date.")

    universe_df = read_parquet(index_path)
    tickers = sorted({str(t).strip().upper() for t in universe_df["ticker"].dropna().tolist() if str(t).strip()})
    total_tickers = len(tickers)
    if total_tickers == 0:
        raise RuntimeError("Universo vazio em index_compositions.parquet.")

    chunk_dir = workspace / "data/ssot/tmp_t007_chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    failures_path = workspace / "data/ssot/t007_failures.json"
    report_path = workspace / "data/ssot/t007_ingestion_report.json"
    out_path = workspace / "data/ssot/us_market_data_raw.parquet"

    started = time.time()
    failures: list[dict[str, Any]] = []
    attempted = 0
    succeeded = 0
    skipped_by_resume = 0
    retries_total = 0

    chunk_size = max(1, int(args.chunk_size))
    total_chunks = (total_tickers + chunk_size - 1) // chunk_size

    for chunk_idx in range(total_chunks):
        left = chunk_idx * chunk_size
        right = min(left + chunk_size, total_tickers)
        batch = tickers[left:right]
        chunk_file = chunk_dir / f"chunk_{chunk_idx:05d}.parquet"

        if chunk_file.exists():
            skipped_by_resume += len(batch)
            attempted += len(batch)
            succeeded += len(batch)
            continue

        frames: list[pd.DataFrame] = []
        max_workers = max(1, int(args.max_workers))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    build_ticker_frame,
                    ticker=tkr,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    api_key=api_key,
                    timeout_seconds=float(args.timeout_seconds),
                    max_retries=int(args.max_retries),
                ): tkr
                for tkr in batch
            }
            for future in as_completed(futures):
                frame, err, retry_count = future.result()
                retries_total += int(retry_count)
                attempted += 1
                if err is not None:
                    failures.append(err)
                else:
                    succeeded += 1
                    if not frame.empty:
                        frames.append(frame)

                elapsed = max(time.time() - started, 1e-9)
                rate = attempted / elapsed
                remaining = max(total_tickers - attempted, 0)
                eta_s = int(remaining / rate) if rate > 0 else -1
                print(
                    f"[T-007v2] {attempted}/{total_tickers} "
                    f"({(attempted / total_tickers) * 100:.2f}%) "
                    f"ok={succeeded} fail={len(failures)} eta_s={eta_s}"
                )

        chunk_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
            columns=[
                "date",
                "ticker",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "dividend_rate",
                "split_from",
                "split_to",
                "source",
                "ingested_at",
            ]
        )
        write_parquet(chunk_df, chunk_file)

    chunk_files = sorted(chunk_dir.glob("chunk_*.parquet"))
    all_frames = [pd.read_parquet(p) for p in chunk_files if p.exists()]
    final_df = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()

    if not final_df.empty:
        final_df["date"] = pd.to_datetime(final_df["date"], errors="coerce").dt.normalize()
        final_df["ticker"] = final_df["ticker"].astype(str).str.strip().str.upper()
        final_df = final_df.dropna(subset=["date", "ticker"])
        duplicates_before = int(final_df.duplicated(subset=["date", "ticker"]).sum())
        final_df = final_df.sort_values(["ticker", "date", "ingested_at"]).drop_duplicates(
            subset=["date", "ticker"], keep="last"
        )
        final_df = final_df.reset_index(drop=True)
    else:
        duplicates_before = 0

    write_parquet(final_df, out_path)

    duplicates_after = int(final_df.duplicated(subset=["date", "ticker"]).sum()) if not final_df.empty else 0
    report = {
        "task_id": "T-007v2",
        "decision_ref": "D-007",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "adjusted": False,
        "source": "polygon_aggs_adjusted_false_v2",
        "range": {"start_date": str(start_dt), "end_date": str(end_dt)},
        "total_tickers_universe": total_tickers,
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": len(failures),
        "success_rate": (succeeded / attempted) if attempted else 0.0,
        "skipped_by_resume": skipped_by_resume,
        "chunks": {"chunk_size": chunk_size, "total_chunks": total_chunks, "materialized_chunks": len(chunk_files)},
        "rows_total": int(len(final_df)),
        "date_min": str(final_df["date"].min().date()) if not final_df.empty else None,
        "date_max": str(final_df["date"].max().date()) if not final_df.empty else None,
        "duplicates_before_dedup": duplicates_before,
        "duplicates_count": duplicates_after,
        "retries_total": int(retries_total),
        "runtime_seconds": round(time.time() - started, 3),
    }
    write_json(report, report_path, indent=2)
    write_json(failures, failures_path, indent=2)

    if report["duplicates_count"] != 0:
        print("[T-007v2] FAIL logical gate: duplicates_count != 0")
        return 3

    print(f"[T-007v2] wrote {out_path}")
    print(f"[T-007v2] wrote {report_path}")
    print(f"[T-007v2] wrote {failures_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
