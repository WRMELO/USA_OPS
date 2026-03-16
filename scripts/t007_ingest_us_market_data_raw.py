"""T-007: mass ingest OHLCV/dividends/splits for US universe."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-007 ingest US market data raw parquet.")
    parser.add_argument("--workspace", type=str, default=".", help="Workspace root path.")
    parser.add_argument("--start-date", type=str, default="2018-01-01", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", type=str, default=date.today().isoformat(), help="End date (YYYY-MM-DD).")
    parser.add_argument("--max-tickers", type=int, default=0, help="Optional limit for number of tickers (0 = all).")
    parser.add_argument("--chunk-size", type=int, default=50, help="Tickers per chunk write.")
    parser.add_argument("--max-retries", type=int, default=5, help="Adapter retries per request.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Adapter timeout in seconds.")
    return parser.parse_args()


def format_seconds(value: float) -> str:
    total = max(0, int(value))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def load_universe(index_path: Path) -> list[str]:
    idx = pd.read_parquet(index_path)
    if idx.empty:
        return []
    if "is_member" in idx.columns:
        idx = idx[idx["is_member"] == True]  # noqa: E712
    tickers = (
        idx["ticker"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    return tickers


def build_ticker_frame(
    adapter: Any,
    ticker: str,
    start_dt: date,
    end_dt: date,
    ingested_at: pd.Timestamp,
) -> pd.DataFrame:
    ohlcv = adapter.get_ohlcv(ticker, start_dt, end_dt)
    if ohlcv.empty:
        return pd.DataFrame()

    div = adapter.get_dividends(ticker, start_dt, end_dt).rename(columns={"amount": "dividend_rate"})
    spl = adapter.get_splits(ticker, start_dt, end_dt)

    out = ohlcv.copy()
    out = out.merge(div, on="date", how="left")
    out = out.merge(spl, on="date", how="left")
    out["ticker"] = ticker
    out["source"] = "polygon"
    out["ingested_at"] = ingested_at

    required = [
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
    for col in required:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[required].drop_duplicates(subset=["date", "ticker"]).sort_values(["ticker", "date"]).reset_index(drop=True)
    return out


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    if str(workspace) not in sys.path:
        sys.path.insert(0, str(workspace))

    from lib.adapters import PolygonAdapter
    from lib.io import write_json, write_parquet

    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        print("ERROR: POLYGON_API_KEY not found in environment.")
        return 2

    start_dt = pd.Timestamp(args.start_date).date()
    end_dt = pd.Timestamp(args.end_date).date()
    if start_dt > end_dt:
        print("ERROR: --start-date must be <= --end-date.")
        return 2

    index_path = workspace / "data" / "ssot" / "index_compositions.parquet"
    tickers = load_universe(index_path)
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]
    if not tickers:
        print("ERROR: empty ticker universe from index_compositions.parquet.")
        return 3

    adapter = PolygonAdapter(api_key=api_key, timeout_seconds=args.timeout, max_retries=args.max_retries)

    tmp_dir = workspace / "data" / "ssot" / "tmp_t007_chunks"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    report_path = workspace / "data" / "ssot" / "t007_ingestion_report.json"
    failures_path = workspace / "data" / "ssot" / "t007_failures.json"
    output_path = workspace / "data" / "ssot" / "us_market_data_raw.parquet"

    total = len(tickers)
    start_ts = time.time()
    failures: list[dict[str, Any]] = []
    chunk_paths: list[Path] = []
    batch_frames: list[pd.DataFrame] = []
    ok_count = 0
    fail_count = 0

    print(f"[T-007] Start ingest: {total} tickers, range {start_dt}..{end_dt}")
    for idx, ticker in enumerate(tickers, start=1):
        try:
            frame = build_ticker_frame(
                adapter=adapter,
                ticker=ticker,
                start_dt=start_dt,
                end_dt=end_dt,
                ingested_at=pd.Timestamp.now(tz="UTC"),
            )
            if not frame.empty:
                batch_frames.append(frame)
                ok_count += 1
            else:
                fail_count += 1
                failures.append({"ticker": ticker, "error": "empty_ohlcv"})
        except Exception as exc:  # noqa: BLE001
            fail_count += 1
            failures.append({"ticker": ticker, "error": str(exc)})

        if len(batch_frames) >= args.chunk_size:
            chunk_df = pd.concat(batch_frames, ignore_index=True)
            chunk_df = chunk_df.drop_duplicates(subset=["date", "ticker"], keep="last")
            chunk_path = tmp_dir / f"chunk_{idx:06d}.parquet"
            write_parquet(chunk_df, chunk_path)
            chunk_paths.append(chunk_path)
            batch_frames = []

        elapsed = time.time() - start_ts
        rate = idx / elapsed if elapsed > 0 else 0.0
        remaining = (total - idx) / rate if rate > 0 else 0.0
        pct = (idx / total) * 100.0
        print(
            f"[T-007] {idx}/{total} ({pct:6.2f}%) "
            f"ok={ok_count} fail={fail_count} "
            f"elapsed={format_seconds(elapsed)} eta={format_seconds(remaining)}"
        )

        if idx % args.chunk_size == 0 or idx == total:
            progress_payload = {
                "task_id": "T-007",
                "status": "IN_PROGRESS" if idx < total else "FINALIZING",
                "range": {"start_date": str(start_dt), "end_date": str(end_dt)},
                "progress": {
                    "tickers_total": total,
                    "tickers_done": idx,
                    "tickers_ok": ok_count,
                    "tickers_failed": fail_count,
                    "percent": round(pct, 4),
                    "elapsed_seconds": round(elapsed, 3),
                    "eta_seconds": round(remaining, 3),
                },
            }
            write_json(progress_payload, report_path, indent=2)
            write_json({"task_id": "T-007", "failures": failures}, failures_path, indent=2)

    if batch_frames:
        chunk_df = pd.concat(batch_frames, ignore_index=True)
        chunk_df = chunk_df.drop_duplicates(subset=["date", "ticker"], keep="last")
        chunk_path = tmp_dir / f"chunk_{total:06d}.parquet"
        write_parquet(chunk_df, chunk_path)
        chunk_paths.append(chunk_path)

    combined_frames: list[pd.DataFrame] = []
    if output_path.exists():
        combined_frames.append(pd.read_parquet(output_path))
    for path in chunk_paths:
        combined_frames.append(pd.read_parquet(path))

    if combined_frames:
        final_df = pd.concat(combined_frames, ignore_index=True)
    else:
        final_df = pd.DataFrame(
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
    final_df["date"] = pd.to_datetime(final_df["date"], errors="coerce").dt.normalize()
    final_df = final_df.dropna(subset=["date", "ticker"])
    final_df = final_df.drop_duplicates(subset=["date", "ticker"], keep="last").sort_values(["ticker", "date"]).reset_index(drop=True)
    write_parquet(final_df, output_path)

    min_date = final_df["date"].min().date().isoformat() if not final_df.empty else None
    max_date = final_df["date"].max().date().isoformat() if not final_df.empty else None
    final_report = {
        "task_id": "T-007",
        "status": "PASS" if not final_df.empty else "FAIL",
        "range": {"start_date": str(start_dt), "end_date": str(end_dt)},
        "progress": {
            "tickers_total": total,
            "tickers_done": total,
            "tickers_ok": ok_count,
            "tickers_failed": fail_count,
            "percent": 100.0,
            "elapsed_seconds": round(time.time() - start_ts, 3),
            "eta_seconds": 0.0,
        },
        "output": {
            "rows_total": int(len(final_df)),
            "unique_tickers_in_output": int(final_df["ticker"].nunique()) if not final_df.empty else 0,
            "min_date": min_date,
            "max_date": max_date,
            "duplicates_date_ticker": int(final_df.duplicated(subset=["date", "ticker"]).sum()) if not final_df.empty else 0,
        },
        "failures_summary": {
            "count": len(failures),
            "sample": failures[:20],
        },
    }
    write_json(final_report, report_path, indent=2)
    write_json({"task_id": "T-007", "count": len(failures), "failures": failures}, failures_path, indent=2)

    if final_df.empty:
        print("[T-007] FAIL logical gate: output parquet is empty.")
        return 3

    print(f"[T-007] PASS. rows={len(final_df)} unique_tickers={final_df['ticker'].nunique()} failures={len(failures)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

