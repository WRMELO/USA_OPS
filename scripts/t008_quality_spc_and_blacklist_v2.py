#!/usr/bin/env python3
"""T-008v2: SPC Shewhart completo (I-MR + Xbar-R) e blacklist HARD/SOFT."""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


SUBGROUP_N = 4
REF_WINDOW_K = 60
A2_N4 = 0.729
D4_N4 = 2.282
E2_IMR_N2 = 2.66
D4_IMR_N2 = 3.267


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="T-008v2 SPC + blacklist")
    parser.add_argument("--workspace", default=".", help="Workspace root")
    parser.add_argument("--chunk-size", type=int, default=200, help="Tickers por chunk")
    parser.add_argument("--max-workers", type=int, default=8, help="Workers por chunk")
    parser.add_argument("--resume", action="store_true", help="Pular chunks existentes")
    parser.add_argument("--raw-path", default="data/ssot/us_market_data_raw.parquet")
    parser.add_argument("--ref-path", default="data/ssot/ticker_reference_us.parquet")
    parser.add_argument("--out-parquet", default="data/ssot/us_universe_operational.parquet")
    parser.add_argument("--out-blacklist", default="config/blacklist_us.json")
    parser.add_argument("--out-report", default="data/ssot/t008v2_quality_report.json")
    parser.add_argument("--tmp-dir", default="data/ssot/tmp_t008_chunks")
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


def normalize_ticker(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper()


def compute_split_factor(df_ticker: pd.DataFrame) -> pd.Series:
    ratio = pd.Series(1.0, index=df_ticker.index, dtype="float64")
    valid = (
        df_ticker["split_from"].notna()
        & df_ticker["split_to"].notna()
        & (pd.to_numeric(df_ticker["split_from"], errors="coerce") > 0)
        & (pd.to_numeric(df_ticker["split_to"], errors="coerce") > 0)
    )
    split_from = pd.to_numeric(df_ticker.loc[valid, "split_from"], errors="coerce")
    split_to = pd.to_numeric(df_ticker.loc[valid, "split_to"], errors="coerce")
    ratio.loc[valid] = split_from / split_to

    # Ajusta apenas datas anteriores ao evento (não aplica o split no próprio dia do evento).
    shifted = ratio.shift(-1, fill_value=1.0)
    return shifted.iloc[::-1].cumprod().iloc[::-1]


def build_rf_series(raw_dates: pd.Series, workspace: Path) -> pd.DataFrame:
    macro_path = workspace / "data/ssot/macro_us.parquet"
    if macro_path.exists():
        macro = pd.read_parquet(macro_path)
        if "date" not in macro.columns or "fed_funds_rate" not in macro.columns:
            raise RuntimeError("macro_us.parquet existe, mas sem colunas date/fed_funds_rate.")
        rf = macro[["date", "fed_funds_rate"]].copy()
    else:
        if str(workspace) not in sys.path:
            sys.path.insert(0, str(workspace))
        from lib.adapters import FredAdapter

        fred = FredAdapter(timeout_seconds=30.0, max_retries=5)
        rf = fred.fetch_series("DFF", "fed_funds_rate")

    rf["date"] = pd.to_datetime(rf["date"], errors="coerce").dt.normalize()
    rf["fed_funds_rate"] = pd.to_numeric(rf["fed_funds_rate"], errors="coerce")
    rf = rf.dropna(subset=["date"]).sort_values("date")

    market_dates = pd.DataFrame({"date": pd.to_datetime(raw_dates, errors="coerce").dropna().drop_duplicates().sort_values()})
    merged = market_dates.merge(rf, on="date", how="left").sort_values("date")
    merged["fed_funds_rate"] = merged["fed_funds_rate"].ffill()

    rf_daily = (1.0 + (merged["fed_funds_rate"] / 100.0)).pow(1.0 / 252.0) - 1.0
    gross = (1.0 + rf_daily).where((1.0 + rf_daily) > 0)
    merged["fed_funds_log_daily"] = np.log(gross)
    merged["fed_funds_log_daily"] = pd.to_numeric(merged["fed_funds_log_daily"], errors="coerce")
    return merged[["date", "fed_funds_rate", "fed_funds_log_daily"]]


def compute_spc_for_ticker(
    ticker: str,
    df_ticker: pd.DataFrame,
    rf_map: pd.DataFrame,
    ref_row: dict[str, Any] | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    df = df_ticker.sort_values("date").copy()
    df["close_raw"] = pd.to_numeric(df["close"], errors="coerce")
    df["split_factor"] = compute_split_factor(df)
    df["close_operational"] = df["close_raw"] * df["split_factor"]
    df = df.merge(rf_map, on="date", how="left")

    ratio = df["close_operational"] / df["close_operational"].shift(1)
    df["log_ret_nominal"] = pd.to_numeric(np.log(ratio.where(ratio > 0)), errors="coerce")
    df["X_real"] = df["log_ret_nominal"] - pd.to_numeric(df["fed_funds_log_daily"], errors="coerce")
    df["i_value"] = df["X_real"]
    df["mr_value"] = (df["i_value"] - df["i_value"].shift(1)).abs()

    df["center_line"] = df["i_value"].rolling(REF_WINDOW_K, min_periods=REF_WINDOW_K).mean().shift(1)
    df["mr_bar"] = df["mr_value"].rolling(REF_WINDOW_K, min_periods=REF_WINDOW_K).mean().shift(1)
    df["i_ucl"] = df["center_line"] + (E2_IMR_N2 * df["mr_bar"])
    df["i_lcl"] = df["center_line"] - (E2_IMR_N2 * df["mr_bar"])
    df["mr_ucl"] = D4_IMR_N2 * df["mr_bar"]

    df["xbar_value"] = df["i_value"].rolling(SUBGROUP_N, min_periods=SUBGROUP_N).mean()
    df["r_value"] = df["i_value"].rolling(SUBGROUP_N, min_periods=SUBGROUP_N).max() - df["i_value"].rolling(
        SUBGROUP_N, min_periods=SUBGROUP_N
    ).min()
    df["r_bar"] = df["r_value"].rolling(REF_WINDOW_K, min_periods=REF_WINDOW_K).mean().shift(1)
    df["xbar_ucl"] = df["center_line"] + (A2_N4 * df["r_bar"])
    df["xbar_lcl"] = df["center_line"] - (A2_N4 * df["r_bar"])
    df["r_ucl"] = D4_N4 * df["r_bar"]

    out_i = (df["i_value"] > df["i_ucl"]) | (df["i_value"] < df["i_lcl"])
    out_mr = df["mr_value"] > df["mr_ucl"]
    out_xbar = (df["xbar_value"] > df["xbar_ucl"]) | (df["xbar_value"] < df["xbar_lcl"])
    out_r = df["r_value"] > df["r_ucl"]
    has_limits = df[["i_ucl", "i_lcl", "mr_ucl", "xbar_ucl", "xbar_lcl", "r_ucl"]].notna().any(axis=1)
    out_any = (out_i | out_mr | out_xbar | out_r) & has_limits

    history_days = int(df["close_operational"].notna().sum())
    if has_limits.any():
        outlier_rate = float(out_any[has_limits].mean())
    else:
        outlier_rate = 0.0

    hard_reasons: list[str] = []
    soft_reasons: list[str] = []

    if df["close_raw"].isna().any() or (pd.to_numeric(df["close_raw"], errors="coerce") <= 0).any():
        hard_reasons.append("ohlcv_invalid_structure")

    if ref_row is None:
        hard_reasons.append("missing_reference_row")
    else:
        if str(ref_row.get("fetch_status", "")) == "FAIL":
            hard_reasons.append("reference_fetch_fail")
        if str(ref_row.get("active", "")).lower() in {"false", "0"}:
            hard_reasons.append("reference_active_false")
        if pd.notna(ref_row.get("delisted_utc")) and str(ref_row.get("delisted_utc")).strip() != "":
            hard_reasons.append("reference_delisted")

    if history_days < 252:
        soft_reasons.append("history_days_lt_252")

    if hard_reasons:
        blacklist_level = "HARD"
        blacklist_reason = ";".join(sorted(set(hard_reasons)))
        is_operational = False
        quality_flag = "HARD_BLACKLIST"
    elif soft_reasons:
        blacklist_level = "SOFT"
        blacklist_reason = ";".join(sorted(set(soft_reasons)))
        is_operational = False
        quality_flag = "SOFT_BLACKLIST"
    else:
        blacklist_level = pd.NA
        blacklist_reason = pd.NA
        is_operational = True
        quality_flag = "OK"

    df["ticker"] = ticker
    df["is_operational"] = is_operational
    df["quality_flag"] = quality_flag
    df["blacklist_level"] = blacklist_level
    df["blacklist_reason"] = blacklist_reason
    df["outlier_rate"] = outlier_rate
    df["history_days"] = history_days

    metrics = {
        "ticker": ticker,
        "history_days": history_days,
        "outlier_rate": outlier_rate,
        "rows": int(len(df)),
        "is_operational": bool(is_operational),
        "blacklist_level": None if pd.isna(blacklist_level) else str(blacklist_level),
        "blacklist_reason": None if pd.isna(blacklist_reason) else str(blacklist_reason),
    }
    return df, metrics


def upsert_changelog_line(changelog_path: Path, line: str, today: str) -> None:
    content = changelog_path.read_text(encoding="utf-8")
    if line in content:
        return
    section = f"## {today}"
    if section in content:
        content = content.rstrip() + "\n" + line + "\n"
    else:
        content = content.rstrip() + f"\n\n{section}\n\n{line}\n"
    changelog_path.write_text(content, encoding="utf-8")


def main() -> int:
    args = parse_args()
    started = time.time()
    workspace = Path(args.workspace).resolve()
    if str(workspace) not in sys.path:
        sys.path.insert(0, str(workspace))

    from lib.io import read_json, read_parquet, write_json, write_parquet

    load_dotenv(workspace / ".env")

    raw_path = workspace / str(args.raw_path)
    ref_path = workspace / str(args.ref_path)
    out_parquet = workspace / str(args.out_parquet)
    out_blacklist = workspace / str(args.out_blacklist)
    out_report = workspace / str(args.out_report)
    tmp_dir = workspace / str(args.tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    if not raw_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {raw_path}")
    if not ref_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {ref_path}")

    raw = read_parquet(raw_path)
    ref = read_parquet(ref_path)
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.normalize()
    raw["ticker"] = normalize_ticker(raw["ticker"])
    ref["ticker"] = normalize_ticker(ref["ticker"])

    required_raw = {"date", "ticker", "open", "high", "low", "close", "volume", "split_from", "split_to"}
    required_ref = {"ticker", "fetch_status", "active", "delisted_utc"}
    missing_raw = sorted(required_raw - set(raw.columns))
    missing_ref = sorted(required_ref - set(ref.columns))
    if missing_raw:
        raise RuntimeError(f"Colunas ausentes no raw: {missing_raw}")
    if missing_ref:
        raise RuntimeError(f"Colunas ausentes no reference: {missing_ref}")

    duplicates_input = int(raw.duplicated(subset=["date", "ticker"]).sum())
    if duplicates_input > 0:
        raise RuntimeError(f"FAIL lógico: raw possui duplicatas (date,ticker): {duplicates_input}")

    rf_map = build_rf_series(raw["date"], workspace)
    ref_map: dict[str, dict[str, Any]] = {
        row["ticker"]: row for row in ref[["ticker", "fetch_status", "active", "delisted_utc"]].to_dict(orient="records")
    }

    raw_tickers = sorted(raw["ticker"].dropna().unique().tolist())
    ref_tickers = sorted(ref["ticker"].dropna().unique().tolist())
    all_tickers = sorted(set(raw_tickers) | set(ref_tickers))

    chunk_size = max(1, int(args.chunk_size))
    total_chunks = (len(raw_tickers) + chunk_size - 1) // chunk_size
    max_workers = max(1, int(args.max_workers))

    attempted = 0
    skipped_by_resume = 0
    all_metrics: list[dict[str, Any]] = []

    for chunk_idx in range(total_chunks):
        left = chunk_idx * chunk_size
        right = min(left + chunk_size, len(raw_tickers))
        batch = raw_tickers[left:right]
        part_path = tmp_dir / f"part_{chunk_idx:05d}.parquet"
        metrics_path = tmp_dir / f"part_{chunk_idx:05d}.metrics.json"

        if args.resume and part_path.exists() and metrics_path.exists():
            skipped_by_resume += len(batch)
            attempted += len(batch)
            all_metrics.extend(read_json(metrics_path))
            continue

        raw_chunk = raw[raw["ticker"].isin(batch)].copy()
        grouped = {t: g.copy() for t, g in raw_chunk.groupby("ticker", sort=True)}

        futures = []
        outputs: list[pd.DataFrame] = []
        metrics_list: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for ticker in batch:
                ticker_df = grouped.get(ticker)
                if ticker_df is None or ticker_df.empty:
                    continue
                futures.append(
                    executor.submit(
                        compute_spc_for_ticker,
                        ticker,
                        ticker_df,
                        rf_map,
                        ref_map.get(ticker),
                    )
                )
            for f in as_completed(futures):
                out_df, metrics = f.result()
                outputs.append(out_df)
                metrics_list.append(metrics)
                attempted += 1
                elapsed = max(time.time() - started, 1e-9)
                rate = attempted / elapsed
                remaining = max(len(raw_tickers) - attempted, 0)
                eta_s = int(remaining / rate) if rate > 0 else -1
                print(f"[T-008v2] {attempted}/{len(raw_tickers)} ({(attempted/len(raw_tickers))*100:.2f}%) eta_s={eta_s}")

        chunk_df = pd.concat(outputs, ignore_index=True) if outputs else pd.DataFrame()
        if not chunk_df.empty:
            chunk_df = chunk_df.sort_values(["date", "ticker"]).reset_index(drop=True)
        write_parquet(chunk_df, part_path)
        write_json(metrics_list, metrics_path, indent=2)
        all_metrics.extend(metrics_list)

    part_files = sorted(tmp_dir.glob("part_*.parquet"))
    final_df = pd.concat([pd.read_parquet(p) for p in part_files], ignore_index=True) if part_files else pd.DataFrame()
    if not final_df.empty:
        final_df["date"] = pd.to_datetime(final_df["date"], errors="coerce").dt.normalize()
        final_df["ticker"] = normalize_ticker(final_df["ticker"])
        final_df = final_df.dropna(subset=["date", "ticker"])
        final_df = final_df.sort_values(["date", "ticker"]).reset_index(drop=True)

    duplicates_output = int(final_df.duplicated(subset=["date", "ticker"]).sum()) if not final_df.empty else 0
    anti_lookahead_nulls = int(final_df["center_line"].isna().sum() + final_df["mr_bar"].isna().sum() + final_df["r_bar"].isna().sum())

    # Adiciona tickers do reference ausentes no raw diretamente na blacklist (HARD).
    ticker_to_metrics = {m["ticker"]: m for m in all_metrics}
    missing_in_raw = sorted(set(ref_tickers) - set(raw_tickers))
    for ticker in missing_in_raw:
        ticker_to_metrics[ticker] = {
            "ticker": ticker,
            "history_days": 0,
            "outlier_rate": 0.0,
            "rows": 0,
            "is_operational": False,
            "blacklist_level": "HARD",
            "blacklist_reason": "missing_in_raw",
        }

    blacklist_entries = []
    for ticker in sorted(ticker_to_metrics):
        item = ticker_to_metrics[ticker]
        lvl = item.get("blacklist_level")
        if lvl in {"HARD", "SOFT"}:
            blacklist_entries.append(
                {
                    "ticker": ticker,
                    "level": lvl,
                    "reason": item.get("blacklist_reason"),
                }
            )

    blacklist_payload = {
        "task_id": "T-008v2",
        "decision_ref": "D-009",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "counts": {
            "hard": int(sum(1 for x in blacklist_entries if x["level"] == "HARD")),
            "soft": int(sum(1 for x in blacklist_entries if x["level"] == "SOFT")),
            "total": int(len(blacklist_entries)),
        },
        "items": blacklist_entries,
    }

    write_parquet(final_df, out_parquet)
    write_json(blacklist_payload, out_blacklist, indent=2)

    report = {
        "task_id": "T-008v2",
        "decision_ref": "D-009",
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "inputs": {
            "raw_path": str(raw_path.relative_to(workspace)),
            "reference_path": str(ref_path.relative_to(workspace)),
            "rf_source": "macro_us.parquet.fed_funds_rate" if (workspace / "data/ssot/macro_us.parquet").exists() else "fred:DFF",
        },
        "params": {
            "subgroup_n": SUBGROUP_N,
            "ref_window_k": REF_WINDOW_K,
            "constants": {"A2": A2_N4, "D4": D4_N4, "E2": E2_IMR_N2, "D4_IMR": D4_IMR_N2},
            "soft_rule": "history_days_lt_252_only",
            "chunk_size": chunk_size,
            "max_workers": max_workers,
            "resume": bool(args.resume),
        },
        "counts": {
            "raw_rows": int(len(raw)),
            "raw_tickers": int(len(raw_tickers)),
            "reference_tickers": int(len(ref_tickers)),
            "all_tickers_union": int(len(all_tickers)),
            "missing_in_raw": int(len(missing_in_raw)),
            "output_rows": int(len(final_df)),
            "output_tickers": int(final_df["ticker"].nunique()) if not final_df.empty else 0,
            "blacklist_hard": blacklist_payload["counts"]["hard"],
            "blacklist_soft": blacklist_payload["counts"]["soft"],
            "operational_tickers": int(sum(1 for m in ticker_to_metrics.values() if m.get("is_operational") is True)),
        },
        "gates": {
            "input_duplicates_date_ticker": duplicates_input,
            "output_duplicates_date_ticker": duplicates_output,
            "anti_lookahead_shift1_warmup_nulls": anti_lookahead_nulls,
            "logical_pass_zero_output_duplicates": duplicates_output == 0,
            "logical_pass_operational_min_4000": int(sum(1 for m in ticker_to_metrics.values() if m.get("is_operational") is True))
            >= 4000,
        },
        "progress": {
            "attempted_raw_tickers": attempted,
            "skipped_by_resume": skipped_by_resume,
            "total_chunks": total_chunks,
            "materialized_chunks": len(part_files),
            "runtime_seconds": round(time.time() - started, 3),
        },
        "blacklist_top_reasons": pd.Series([x["reason"] for x in blacklist_entries]).value_counts().head(20).to_dict(),
    }
    write_json(report, out_report, indent=2)

    if duplicates_output != 0:
        print("[T-008v2] FAIL lógico: duplicatas no output final.")
        return 3

    print(f"[T-008v2] wrote {out_parquet}")
    print(f"[T-008v2] wrote {out_blacklist}")
    print(f"[T-008v2] wrote {out_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
