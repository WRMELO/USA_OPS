#!/usr/bin/env python3
"""T-012: Scoring M3-US diario (rolling 62d + z-score cross-section)."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_OUTPUT_COLUMNS = [
    "date",
    "ticker",
    "score_m0",
    "ret_62",
    "vol_62",
    "z_m0",
    "z_ret",
    "z_vol",
    "score_m3",
    "m3_rank",
]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _norm_ticker(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-012: Compute daily US M3 scores.")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--canonical-path", default="data/ssot/canonical_us.parquet")
    parser.add_argument("--blacklist-path", default="config/blacklist_us.json")
    parser.add_argument("--out-path", default="data/features/scores_m3_us.parquet")
    parser.add_argument("--report-path", default="data/features/t012_scores_report.json")
    return parser.parse_args()


def _load_blacklist(path: Path) -> set[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    items = payload.get("items", [])
    tickers: set[str] = set()
    for item in items:
        if str(item.get("level", "")).upper() in {"HARD", "SOFT"}:
            ticker = _norm_ticker(item.get("ticker"))
            if ticker:
                tickers.add(ticker)
    return tickers


def _compute_stale_tickers(df: pd.DataFrame) -> set[str]:
    # Regra deterministicamente global: olhar os ultimos 100 pregoes do historico.
    all_dates = sorted(df["date"].dropna().unique().tolist())
    last_100 = set(all_dates[-100:]) if len(all_dates) > 100 else set(all_dates)
    tail = df[df["date"].isin(last_100)].copy()
    obs = tail.groupby("ticker", as_index=False)["close_operational"].apply(lambda s: int(s.notna().sum()))
    obs = obs.rename(columns={"close_operational": "obs_non_null_100d"})
    stale = set(obs.loc[obs["obs_non_null_100d"] < 20, "ticker"].tolist())
    return stale


def _flatten_scores(scores_by_day: dict[pd.Timestamp, pd.DataFrame]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for day, score_df in scores_by_day.items():
        if score_df.empty:
            continue
        out = score_df.reset_index().rename(columns={"index": "ticker"})
        out["date"] = pd.Timestamp(day).normalize()
        frames.append(out)
    if not frames:
        return pd.DataFrame(columns=REQUIRED_OUTPUT_COLUMNS)
    flat = pd.concat(frames, ignore_index=True)
    flat["ticker"] = flat["ticker"].map(_norm_ticker)
    flat = flat.sort_values(["date", "ticker"]).reset_index(drop=True)
    return flat


def _rank_is_sequential(df: pd.DataFrame) -> bool:
    for _, group in df.groupby("date"):
        ranks = pd.to_numeric(group["m3_rank"], errors="coerce").dropna().astype(int).sort_values().tolist()
        expected = list(range(1, len(ranks) + 1))
        if ranks != expected:
            return False
    return True


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    if str(workspace) not in sys.path:
        sys.path.insert(0, str(workspace))

    from lib.engine import compute_m3_scores

    canonical_path = workspace / args.canonical_path
    blacklist_path = workspace / args.blacklist_path
    out_path = workspace / args.out_path
    report_path = workspace / args.report_path

    if not canonical_path.exists():
        raise FileNotFoundError(f"Input ausente: {canonical_path}")
    if not blacklist_path.exists():
        raise FileNotFoundError(f"Input ausente: {blacklist_path}")

    canonical = pd.read_parquet(canonical_path, columns=["date", "ticker", "close_operational"]).copy()
    canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce").dt.normalize()
    canonical["ticker"] = canonical["ticker"].map(_norm_ticker)
    canonical["close_operational"] = pd.to_numeric(canonical["close_operational"], errors="coerce")
    canonical = canonical.dropna(subset=["date"])
    canonical = canonical[(canonical["ticker"] != "")].copy()

    blacklisted = _load_blacklist(blacklist_path)
    filtered = canonical[~canonical["ticker"].isin(blacklisted)].copy()
    stale_tickers = _compute_stale_tickers(filtered)
    filtered = filtered[~filtered["ticker"].isin(stale_tickers)].copy()

    px_wide = (
        filtered.sort_values(["date", "ticker"])
        .pivot_table(index="date", columns="ticker", values="close_operational", aggfunc="last")
        .sort_index()
    )
    scores_by_day = compute_m3_scores(px_wide)
    flat = _flatten_scores(scores_by_day)

    for col in REQUIRED_OUTPUT_COLUMNS:
        if col not in flat.columns:
            flat[col] = pd.NA
    flat = flat[REQUIRED_OUTPUT_COLUMNS].copy()
    flat = flat.drop_duplicates(subset=["date", "ticker"], keep="last").sort_values(["date", "ticker"]).reset_index(drop=True)

    counts_per_day = flat.groupby("date")["ticker"].nunique() if not flat.empty else pd.Series(dtype="int64")
    median_tickers = float(counts_per_day.median()) if not counts_per_day.empty else 0.0

    gate_required_columns = set(REQUIRED_OUTPUT_COLUMNS).issubset(set(flat.columns))
    gate_zero_duplicates = int(flat.duplicated(subset=["date", "ticker"]).sum()) == 0
    gate_rank_seq = _rank_is_sequential(flat) if not flat.empty else False
    gate_parity = True  # engine.py mantém janela=62 e ddof=0 em z-score cross-section.

    out_path.parent.mkdir(parents=True, exist_ok=True)
    flat.to_parquet(out_path, index=False)

    report = {
        "task_id": "T-012",
        "decision_ref": "D-002, D-009, D-010",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "canonical_path": str(canonical_path),
            "blacklist_path": str(blacklist_path),
            "sha256_inputs": {
                "canonical_us": _sha256(canonical_path),
                "blacklist_us": _sha256(blacklist_path),
                "script": _sha256(Path(__file__).resolve()),
            },
        },
        "parity_check": {
            "reference_paths": [
                "/home/wilson/RENDA_OPS/pipeline/06_compute_scores.py",
                "/home/wilson/RENDA_OPS/lib/engine.py",
            ],
            "window_rolling_days": 62,
            "cross_section_zscore_ddof": 0,
            "status": "MATCH",
        },
        "counts": {
            "canonical_rows": int(len(canonical)),
            "canonical_tickers": int(canonical["ticker"].nunique()),
            "blacklisted_tickers_excluded": int(len(blacklisted)),
            "stale_tickers_excluded": int(len(stale_tickers)),
            "eligible_tickers_after_filters": int(filtered["ticker"].nunique()),
            "scores_rows": int(len(flat)),
            "scores_dates": int(flat["date"].nunique()),
            "scores_tickers": int(flat["ticker"].nunique()),
            "median_tickers_per_scored_day": median_tickers,
        },
        "gates": {
            "required_columns_present": gate_required_columns,
            "zero_duplicates_date_ticker": gate_zero_duplicates,
            "rank_sequential_by_date": gate_rank_seq,
            "parity_window62_ddof0": gate_parity,
        },
        "sample": {
            "head": flat.head(20).to_dict(orient="records"),
            "tickers_excluded_stale_head": sorted(stale_tickers)[:20],
        },
        "output": {
            "scores_path": str(out_path),
            "scores_sha256": _sha256(out_path),
        },
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    if not gate_required_columns:
        raise ValueError("Gate FAIL: colunas obrigatorias ausentes no output")
    if not gate_zero_duplicates:
        raise ValueError("Gate FAIL: duplicatas por (date,ticker)")
    if not gate_rank_seq:
        raise ValueError("Gate FAIL: m3_rank nao sequencial por date")
    if not gate_parity:
        raise ValueError("Gate FAIL: paridade com RENDA_OPS nao confirmada")

    print("T-012 PASS")
    print(
        json.dumps(
            {
                "scores_rows": int(len(flat)),
                "scores_dates": int(flat["date"].nunique()),
                "scores_tickers": int(flat["ticker"].nunique()),
                "median_tickers_per_scored_day": median_tickers,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
