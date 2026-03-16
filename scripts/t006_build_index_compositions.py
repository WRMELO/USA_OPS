"""Build snapshot index compositions for T-006 using iShares public CSVs."""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import pandas as pd


@dataclass
class ProxySpec:
    index_id: str
    composite_ticker: str
    holdings_csv_url: str


def with_retry_bytes(url: str, timeout: float, max_retries: int) -> bytes:
    last_exc: Exception | None = None
    req = Request(url, headers={"User-Agent": "USA_OPS-T006/1.0"})
    for attempt in range(1, max_retries + 1):
        try:
            with urlopen(req, timeout=timeout) as response:
                return response.read()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == max_retries:
                break
            wait_s = min(2**attempt, 60)
            print(f"[T-006] Download failed on attempt {attempt}/{max_retries}; retry in {wait_s}s")
            time.sleep(float(wait_s))
    raise RuntimeError(f"Failed request after retries: {url}") from last_exc


def parse_snapshot_date(lines: list[str]) -> pd.Timestamp:
    for line in lines:
        if line.startswith("Fund Holdings as of,"):
            right = line.split(",", maxsplit=1)[1].strip().strip('"')
            ts = pd.to_datetime(right, errors="coerce")
            if pd.isna(ts):
                raise ValueError(f"Invalid snapshot date in line: {line}")
            return ts.normalize()
    raise ValueError("CSV header missing 'Fund Holdings as of' row.")


def parse_ishares_csv(raw: bytes, proxy: ProxySpec) -> tuple[pd.DataFrame, dict[str, Any]]:
    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    snapshot_date = parse_snapshot_date(lines)

    header_idx = -1
    for idx, line in enumerate(lines):
        if line.startswith("Ticker,"):
            header_idx = idx
            break
    if header_idx < 0:
        raise ValueError("CSV table header not found (expected line starting with 'Ticker,').")

    # pandas expects a buffer; use StringIO from stdlib for deterministic parsing.
    from io import StringIO

    table = pd.read_csv(StringIO("\n".join(lines[header_idx:])))
    total_rows_csv = int(len(table))

    if "Asset Class" not in table.columns:
        raise ValueError("CSV missing 'Asset Class' column.")
    eq = table[table["Asset Class"].astype(str).str.strip().eq("Equity")].copy()

    if "Ticker" not in eq.columns:
        raise ValueError("CSV missing 'Ticker' column.")
    eq["Ticker"] = eq["Ticker"].astype(str).str.strip().str.upper()
    eq = eq[eq["Ticker"] != ""]

    # Keep operational US tickers only (allowing common suffixes like BRK.B / BF-B).
    valid_pattern = re.compile(r"^[A-Z0-9]+([.-][A-Z0-9]+)?$")
    has_space = eq["Ticker"].str.contains(r"\s", na=False)
    is_invalid = ~eq["Ticker"].str.match(valid_pattern, na=False)
    dropped = eq[has_space | is_invalid].copy()
    eq = eq[~(has_space | is_invalid)].copy()

    if "Weight (%)" in eq.columns:
        weight_col = "Weight (%)"
    elif "Market Weight" in eq.columns:
        weight_col = "Market Weight"
    else:
        weight_col = None

    out = pd.DataFrame(
        {
            "date": snapshot_date,
            "index_id": proxy.index_id,
            "ticker": eq["Ticker"],
            "is_member": True,
            "effective_from": snapshot_date,
            "effective_to": pd.NaT,
            "name": eq["Name"] if "Name" in eq.columns else None,
            "sector": eq["Sector"] if "Sector" in eq.columns else None,
            "weight": pd.to_numeric(eq[weight_col], errors="coerce") if weight_col else pd.NA,
            "composite_ticker": proxy.composite_ticker,
            "source": "ishares_csv_public",
            "coverage_mode": "snapshot",
        }
    )
    out = out.drop_duplicates(subset=["index_id", "ticker"]).sort_values(["index_id", "ticker"]).reset_index(drop=True)

    null_weight_tickers = out[out["weight"].isna()]["ticker"].dropna().astype(str).tolist()
    dropped_tickers = dropped["Ticker"].dropna().astype(str).tolist()

    coverage = {
        "index_id": proxy.index_id,
        "composite_ticker": proxy.composite_ticker,
        "source_url": proxy.holdings_csv_url,
        "snapshot_date": snapshot_date.date().isoformat(),
        "coverage_mode": "snapshot",
        "total_rows_csv": total_rows_csv,
        "equity_rows": int(len(eq)),
        "unique_tickers": int(out["ticker"].nunique()),
        "dropped_invalid_tickers_count": len(dropped_tickers),
        "dropped_invalid_tickers_sample": dropped_tickers[:20],
        "null_weight_count": len(null_weight_tickers),
        "null_weight_tickers_sample": null_weight_tickers[:20],
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
    }
    return out, coverage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build T-006 snapshot index composition parquet and coverage evidence.")
    parser.add_argument("--workspace", type=str, default=".", help="Workspace root path")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds")
    parser.add_argument("--max-retries", type=int, default=5, help="Max retries per request")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    if str(workspace) not in sys.path:
        sys.path.insert(0, str(workspace))
    from lib.io import read_json, write_json, write_parquet

    config_path = workspace / "config" / "index_proxies_us.json"
    config = read_json(config_path)
    proxies = [
        ProxySpec(
            index_id=p["index_id"],
            composite_ticker=p["composite_ticker"],
            holdings_csv_url=p["holdings_csv_url"],
        )
        for p in config["proxies"]
    ]

    all_frames: list[pd.DataFrame] = []
    coverage_rows: list[dict[str, Any]] = []
    for proxy in proxies:
        raw = with_retry_bytes(proxy.holdings_csv_url, timeout=args.timeout, max_retries=args.max_retries)
        frame, coverage = parse_ishares_csv(raw, proxy)
        all_frames.append(frame)
        coverage_rows.append(coverage)

    final_df = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    if not final_df.empty:
        final_df = final_df.sort_values(["index_id", "ticker"]).reset_index(drop=True)

    iwb_tickers = set(final_df.loc[final_df["index_id"] == "R1000", "ticker"].dropna().unique())
    ijr_tickers = set(final_df.loc[final_df["index_id"] == "SP600", "ticker"].dropna().unique())
    overlap_count = len(iwb_tickers.intersection(ijr_tickers))

    coverage_payload = {
        "task_id": "T-006",
        "decision_ref": "D-005",
        "source": "ishares_csv_public",
        "coverage_mode": "snapshot",
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "limitations": [
            "Composicao em modo snapshot atual de holdings iShares.",
            "Nao representa composicao historica por effective_date; limitacao aceita em D-005."
        ],
        "coverage_by_index": coverage_rows,
        "totals": {
            "rows": int(len(final_df)),
            "unique_tickers": int(final_df["ticker"].nunique()) if not final_df.empty else 0,
            "overlap_iwb_ijr": overlap_count,
        },
    }

    coverage_path = workspace / "data" / "ssot" / "index_compositions_coverage.json"
    write_json(coverage_payload, coverage_path, indent=2)

    parquet_path = workspace / "data" / "ssot" / "index_compositions.parquet"
    write_parquet(final_df, parquet_path)

    print(f"[T-006] wrote parquet: {parquet_path}")
    print(f"[T-006] wrote coverage: {coverage_path}")

    iwb_equities = next((int(r["equity_rows"]) for r in coverage_rows if r["index_id"] == "R1000"), 0)
    ijr_equities = next((int(r["equity_rows"]) for r in coverage_rows if r["index_id"] == "SP600"), 0)
    if iwb_equities < 500 or ijr_equities < 400:
        print("[T-006] FAIL logical gate: sanity threshold not met (IWB>=500, IJR>=400).")
        return 3

    print("[T-006] PASS snapshot coverage gate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
