#!/usr/bin/env python3
"""Compare EODHD shadow ingest outputs against official US operational data."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Comparar sombra EODHD US vs oficial.")
    parser.add_argument("--workspace", default=".", help="USA_OPS workspace root.")
    parser.add_argument("--sala-root", default="/home/wilson/SALA_DE_CONTROLE", help="SALA root path.")
    parser.add_argument("--sandbox-rel", default="data/shadow_eodhd_ws", help="Sandbox path relative to workspace.")
    parser.add_argument("--sessions", default="2026-07-27,2026-07-28,2026-07-29", help="Sessoes alvo CSV.")
    parser.add_argument("--history-sessions", type=int, default=20, help="Numero de sessoes historicas comuns.")
    parser.add_argument(
        "--diagnostico-path",
        default="/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/relatorios/diagnostico_dividendos_us.json",
        help="Diagnostico nominal de dividendos (classificacao de casos de conversao).",
    )
    parser.add_argument(
        "--out-json",
        default="/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/relatorios/sombra_ingest_us.json",
        help="Relatorio JSON.",
    )
    parser.add_argument(
        "--out-md",
        default="/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/relatorios/sombra_ingest_us.md",
        help="Relatorio Markdown.",
    )
    parser.add_argument(
        "--out-draft-exclusions",
        default="/home/wilson/SALA_DE_CONTROLE/eodhd_base_unica/relatorios/universe_exclusions_us_draft.json",
        help="Draft de exclusoes de universo.",
    )
    return parser.parse_args()


def _norm_ticker(series: pd.Series) -> pd.Series:
    return series.astype(str).str.upper().str.strip()


def _norm_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _price_metrics(merged: pd.DataFrame, col_off: str, col_shadow: str) -> dict[str, Any]:
    if merged.empty:
        return {
            "rows": 0,
            "p50_abs_pct_diff": None,
            "p95_abs_pct_diff": None,
            "max_abs_pct_diff": None,
            "within_0_5pct_rate": None,
        }
    off = pd.to_numeric(merged[col_off], errors="coerce")
    sh = pd.to_numeric(merged[col_shadow], errors="coerce")
    valid = off.notna() & sh.notna()
    if not valid.any():
        return {
            "rows": 0,
            "p50_abs_pct_diff": None,
            "p95_abs_pct_diff": None,
            "max_abs_pct_diff": None,
            "within_0_5pct_rate": None,
        }
    off_v = off[valid]
    sh_v = sh[valid]
    denom = np.where(np.abs(off_v.to_numpy()) > 1e-12, np.abs(off_v.to_numpy()), 1.0)
    abs_pct = np.abs(sh_v.to_numpy() - off_v.to_numpy()) / denom
    return {
        "rows": int(len(abs_pct)),
        "p50_abs_pct_diff": float(np.quantile(abs_pct, 0.50)),
        "p95_abs_pct_diff": float(np.quantile(abs_pct, 0.95)),
        "max_abs_pct_diff": float(np.max(abs_pct)),
        "within_0_5pct_rate": float(np.mean(abs_pct <= 0.005)),
    }


def _to_dict_of_sets(frame: pd.DataFrame) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {}
    for d, part in frame.groupby("date", sort=False):
        grouped[str(d)] = set(part["ticker"].tolist())
    return grouped


def _load_open_positions(ledger_path: Path) -> set[str]:
    if not ledger_path.exists():
        return set()
    qty: dict[str, float] = {}
    for line in ledger_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        event_type = str(event.get("event_type") or event.get("type") or "").upper().strip()
        ticker = str(event.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        q = float(event.get("qtd", event.get("qty", 0)) or 0)
        if event_type == "BUY":
            qty[ticker] = qty.get(ticker, 0.0) + q
        elif event_type == "SELL":
            qty[ticker] = qty.get(ticker, 0.0) - q
    return {t for t, q in qty.items() if abs(q) > 1e-9}


def _collect_tickers_recursive(obj: Any) -> set[str]:
    out: set[str] = set()
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key.lower() == "ticker" and value is not None:
                out.add(str(value).upper().strip())
            else:
                out.update(_collect_tickers_recursive(value))
    elif isinstance(obj, list):
        for item in obj:
            out.update(_collect_tickers_recursive(item))
    return out


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    sala_root = Path(args.sala_root).resolve()
    sessions = [item.strip() for item in args.sessions.split(",") if item.strip()]
    if not sessions:
        raise RuntimeError("Nenhuma sessao foi informada em --sessions")

    sandbox = workspace / args.sandbox_rel
    shadow_canonical_path = sandbox / "data/ssot/canonical_us.parquet"
    if not shadow_canonical_path.exists():
        raise FileNotFoundError(f"Canonical sombra ausente: {shadow_canonical_path}")

    official_opw_path = workspace / "data/ssot/operational_window.parquet"
    official_canonical_path = workspace / "data/ssot/canonical_us.parquet"
    eodhd_raw_path = sala_root / "eodhd_base_unica/data/eodhd_raw_us.parquet"
    universe_path = sala_root / "eodhd_base_unica/data/universo_us.json"

    opw = pd.read_parquet(official_opw_path).copy()
    opw["date"] = _norm_date(opw["date"])
    opw["ticker"] = _norm_ticker(opw["ticker"])
    opw = opw.dropna(subset=["date", "ticker"])

    shadow = pd.read_parquet(shadow_canonical_path).copy()
    shadow["date"] = _norm_date(shadow["date"])
    shadow["ticker"] = _norm_ticker(shadow["ticker"])
    shadow = shadow.dropna(subset=["date", "ticker"])

    official_canonical = pd.read_parquet(official_canonical_path).copy()
    official_canonical["date"] = _norm_date(official_canonical["date"])
    official_canonical["ticker"] = _norm_ticker(official_canonical["ticker"])
    official_canonical = official_canonical.dropna(subset=["date", "ticker"])

    coverage_by_session: dict[str, Any] = {}
    missing_union: set[str] = set()
    for sess in sessions:
        off_set = set(opw.loc[opw["date"] == sess, "ticker"].tolist())
        sh_set = set(shadow.loc[shadow["date"] == sess, "ticker"].tolist())
        missing = sorted(off_set - sh_set)
        covered = len(off_set & sh_set)
        coverage_rate = (covered / len(off_set)) if off_set else None
        coverage_by_session[sess] = {
            "opw_tickers": len(off_set),
            "shadow_tickers": len(sh_set),
            "covered_tickers": covered,
            "coverage_rate": coverage_rate,
            "missing_in_shadow_count": len(missing),
            "missing_in_shadow": missing,
        }
        missing_union.update(missing)

    merged_sessions = opw[opw["date"].isin(sessions)][["date", "ticker", "close_raw", "close_operational"]].merge(
        shadow[shadow["date"].isin(sessions)][["date", "ticker", "close_raw", "close_operational"]],
        on=["date", "ticker"],
        how="inner",
        suffixes=("_off", "_shadow"),
    )
    price_sessions_raw = _price_metrics(merged_sessions, "close_raw_off", "close_raw_shadow")
    price_sessions_oper = _price_metrics(merged_sessions, "close_operational_off", "close_operational_shadow")

    common_dates = sorted(set(opw["date"].unique()) & set(shadow["date"].unique()))
    hist_dates = [d for d in common_dates if d < sessions[0]][-args.history_sessions :]
    merged_hist = opw[opw["date"].isin(hist_dates)][["date", "ticker", "close_raw", "close_operational"]].merge(
        shadow[shadow["date"].isin(hist_dates)][["date", "ticker", "close_raw", "close_operational"]],
        on=["date", "ticker"],
        how="inner",
        suffixes=("_off", "_shadow"),
    )
    price_hist_raw = _price_metrics(merged_hist, "close_raw_off", "close_raw_shadow")
    price_hist_oper = _price_metrics(merged_hist, "close_operational_off", "close_operational_shadow")

    split_merge = opw[["date", "ticker", "split_factor"]].merge(
        shadow[["date", "ticker", "split_factor"]],
        on=["date", "ticker"],
        how="inner",
        suffixes=("_off", "_shadow"),
    )
    split_off = pd.to_numeric(split_merge["split_factor_off"], errors="coerce")
    split_shadow = pd.to_numeric(split_merge["split_factor_shadow"], errors="coerce")
    event_mask = (
        split_off.fillna(1.0).ne(1.0)
        | split_shadow.fillna(1.0).ne(1.0)
    )
    split_events = split_merge.loc[event_mask].copy()
    split_events["split_factor_off"] = split_off[event_mask]
    split_events["split_factor_shadow"] = split_shadow[event_mask]
    split_match = split_events["split_factor_off"].eq(split_events["split_factor_shadow"])
    split_mismatch_rows = split_events.loc[~split_match.fillna(False), ["date", "ticker", "split_factor_off", "split_factor_shadow"]]

    diag_path = Path(args.diagnostico_path)
    diag_payload = _load_json(diag_path) if diag_path.exists() else {}
    diag_by_class = (
        diag_payload.get("by_classification", {}) if isinstance(diag_payload, dict) else {}
    )
    currency_case_tickers = {
        str(t).upper().strip()
        for t in diag_by_class.get("currency_conversion", [])
        if str(t).strip()
    }

    div_merge = opw[opw["date"].isin(sessions)][["date", "ticker", "dividend_rate"]].merge(
        shadow[shadow["date"].isin(sessions)][["date", "ticker", "dividend_rate"]],
        on=["date", "ticker"],
        how="outer",
        suffixes=("_off", "_shadow"),
    )
    div_merge["dividend_rate_off"] = pd.to_numeric(div_merge["dividend_rate_off"], errors="coerce").fillna(0.0)
    div_merge["dividend_rate_shadow"] = pd.to_numeric(div_merge["dividend_rate_shadow"], errors="coerce").fillna(0.0)
    div_merge["abs_diff"] = (div_merge["dividend_rate_off"] - div_merge["dividend_rate_shadow"]).abs()
    div_threshold = 1e-4
    div_mismatch_all = div_merge[div_merge["abs_diff"] > div_threshold].copy()
    div_currency_cases = div_mismatch_all[div_mismatch_all["ticker"].isin(currency_case_tickers)].copy()
    div_mismatch = div_mismatch_all[~div_mismatch_all["ticker"].isin(currency_case_tickers)].copy()

    # Presence in EODHD base and draft exclusions.
    universe_payload = _load_json(universe_path)
    universe_tickers = universe_payload.get("tickers", [])
    universe = sorted({str(t).upper().strip() for t in universe_tickers if str(t).strip()})

    eodhd_raw = pd.read_parquet(eodhd_raw_path, columns=["date", "ticker"]).copy()
    eodhd_raw["date"] = _norm_date(eodhd_raw["date"])
    eodhd_raw["ticker"] = _norm_ticker(eodhd_raw["ticker"])
    eodhd_raw = eodhd_raw.dropna(subset=["date", "ticker"])
    eodhd_sessions = eodhd_raw[eodhd_raw["date"].isin(sessions)].copy()
    by_date = _to_dict_of_sets(eodhd_sessions)

    presence_count = {ticker: 0 for ticker in universe}
    for sess in sessions:
        sess_set = by_date.get(sess, set())
        for ticker in sess_set:
            if ticker in presence_count:
                presence_count[ticker] += 1

    min_presence = len(sessions)
    market_day = sessions[-1]
    missing_market_day = sorted([t for t in universe if t not in by_date.get(market_day, set())])
    low_presence = sorted([t for t in universe if presence_count.get(t, 0) < min_presence])
    excluded_tickers = sorted(set(missing_market_day) | set(low_presence))
    kept_tickers = len(universe) - len(excluded_tickers)

    draft = {
        "generated_at": datetime.now(UTC).isoformat(),
        "decision_ref": "PENDING-DECISION-LOG",
        "criterion": {
            "lookback_sessions": len(sessions),
            "min_presence_sessions": min_presence,
            "source": str(eodhd_raw_path),
            "note": (
                "DRAFT da sombra EODHD US. Amostra limitada a 3 sessoes de bulk "
                "apos reparo de notacao; usar apenas como evidencia pre-Pacote 2."
            ),
        },
        "counts": {
            "universe_recent": len(universe),
            "excluded": len(excluded_tickers),
            "kept": kept_tickers,
            "excluded_low_presence": len(low_presence),
            "excluded_missing_market_day": len(missing_market_day),
        },
        "excluded_tickers": excluded_tickers,
    }
    draft_path = Path(args.out_draft_exclusions)
    draft_path.parent.mkdir(parents=True, exist_ok=True)
    draft_path.write_text(json.dumps(draft, ensure_ascii=False, indent=2), encoding="utf-8")

    # Intersections with live positions/candidates.
    open_positions = _load_open_positions(workspace / "data/live_real_test/ledger_real.jsonl")
    contexto = _load_json(workspace / "data/ssot/contexto_analista_us.json")
    candidates = _collect_tickers_recursive(contexto.get("candidates", []))
    missing_union_sorted = sorted(missing_union)
    missing_in_open_positions = sorted(set(missing_union_sorted) & open_positions)
    missing_in_candidates = sorted(set(missing_union_sorted) & candidates)

    # Credits estimate.
    obs = _load_json(sala_root / "eodhd_base_unica/relatorios/observacao_diaria_us.json")
    diag = _load_json(sala_root / "eodhd_base_unica/relatorios/diagnostico_bulk_notacao_us.json")
    credits_repair = int(obs.get("ultima_execucao", {}).get("credits_consumed", 0) or 0)
    credits_diag = 100 if diag.get("status") == "OK" else 0
    credits_total = credits_repair + credits_diag

    coverage_values = [v["coverage_rate"] for v in coverage_by_session.values() if v["coverage_rate"] is not None]
    coverage_ok = bool(coverage_values) and min(coverage_values) >= 0.995
    price_sessions_ok = (
        price_sessions_oper["within_0_5pct_rate"] is not None
        and price_sessions_oper["within_0_5pct_rate"] >= 0.99
        and price_sessions_oper["p95_abs_pct_diff"] is not None
        and price_sessions_oper["p95_abs_pct_diff"] <= 0.001
    )
    price_hist_ok = (
        price_hist_oper["within_0_5pct_rate"] is not None
        and price_hist_oper["within_0_5pct_rate"] >= 0.99
        and price_hist_oper["p95_abs_pct_diff"] is not None
        and price_hist_oper["p95_abs_pct_diff"] <= 0.001
    )
    split_ok = int(len(split_mismatch_rows)) == 0
    dividend_ok = int(len(div_mismatch)) == 0
    intersections_ok = len(missing_in_open_positions) == 0 and len(missing_in_candidates) == 0

    reasons: list[str] = []
    if not coverage_ok:
        reasons.append("coverage_rate abaixo de 0.995 em pelo menos uma sessao alvo")
    if not price_sessions_ok:
        reasons.append("metricas de preco (janela sombra) fora dos limiares")
    if not price_hist_ok:
        reasons.append("metricas de preco (20 sessoes historicas comuns) fora dos limiares")
    if not split_ok:
        reasons.append("split_factor divergente em dias de evento")
    if not dividend_ok:
        reasons.append("dividend_rate divergente na janela sombra")
    if not intersections_ok:
        reasons.append("ausencias em sombra intersectam posicoes abertas/candidates")
    if credits_total > 1000:
        reasons.append("consumo de creditos EODHD acima do teto de 1000")

    verdict = "GO" if not reasons else "NO-GO"

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "task_id": "T-SDC-EODHD-US-SOMBRA-INGEST-SEMANTICA-V1",
        "decision_ref": "PENDING-DECISION-LOG",
        "sessions": sessions,
        "history_sessions_common": hist_dates,
        "inputs": {
            "official_operational_window": str(official_opw_path),
            "official_canonical": str(official_canonical_path),
            "shadow_canonical": str(shadow_canonical_path),
            "eodhd_raw_us": str(eodhd_raw_path),
            "universe_us": str(universe_path),
            "ledger_real": str(workspace / "data/live_real_test/ledger_real.jsonl"),
            "contexto_analista_us": str(workspace / "data/ssot/contexto_analista_us.json"),
            "diagnostico_dividendos_us": str(diag_path),
        },
        "coverage_by_session": coverage_by_session,
        "price_metrics": {
            "sessions": {
                "close_raw": price_sessions_raw,
                "close_operational": price_sessions_oper,
            },
            "history_common": {
                "close_raw": price_hist_raw,
                "close_operational": price_hist_oper,
            },
        },
        "split_events": {
            "event_rows": int(len(split_events)),
            "mismatch_count": int(len(split_mismatch_rows)),
            "mismatch_sample": split_mismatch_rows.head(20).to_dict(orient="records"),
        },
        "dividend_window": {
            "rows_compared": int(len(div_merge)),
            "mismatch_count": int(len(div_mismatch)),
            "mismatch_count_all_above_threshold": int(len(div_mismatch_all)),
            "currency_case_count": int(len(div_currency_cases)),
            "mismatch_sample": div_mismatch.head(20).to_dict(orient="records"),
        },
        "dividend_currency_cases": {
            "source_path": str(diag_path),
            "tickers": sorted(currency_case_tickers),
            "count": int(len(div_currency_cases)),
            "sample": div_currency_cases.head(20).to_dict(orient="records"),
        },
        "missing_intersections": {
            "missing_union_count": len(missing_union_sorted),
            "missing_union": missing_union_sorted,
            "open_positions_count": len(open_positions),
            "candidates_count": len(candidates),
            "missing_in_open_positions": missing_in_open_positions,
            "missing_in_candidates": missing_in_candidates,
        },
        "credits": {
            "diagnostic_bulk": credits_diag,
            "repair_sessions": credits_repair,
            "total_estimated": credits_total,
            "ceiling": 1000,
        },
        "universe_exclusions_draft": {
            "path": str(draft_path),
            "excluded_count": len(excluded_tickers),
            "missing_market_day_count": len(missing_market_day),
            "low_presence_count": len(low_presence),
        },
        "thresholds": {
            "coverage_rate_min": 0.995,
            "within_0_5pct_rate_min": 0.99,
            "p95_abs_pct_diff_max": 0.001,
            "split_factor_exact_match_required": True,
            "dividend_rate_abs_diff_max": div_threshold,
            "missing_intersection_allowed": 0,
            "credits_ceiling": 1000,
        },
        "verdict": {
            "status": verdict,
            "checks": {
                "coverage_ok": coverage_ok,
                "price_sessions_ok": price_sessions_ok,
                "price_history_ok": price_hist_ok,
                "split_ok": split_ok,
                "dividend_ok": dividend_ok,
                "intersections_ok": intersections_ok,
                "credits_ok": credits_total <= 1000,
            },
            "reasons": reasons,
        },
    }

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Sombra EODHD US - Relatorio",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- verdict: {verdict}",
        f"- reasons: {', '.join(reasons) if reasons else '[]'}",
        f"- credits_total_estimated: {credits_total}",
        "",
        "## Coverage por sessao",
    ]
    for sess, item in coverage_by_session.items():
        lines.append(
            f"- {sess}: coverage_rate={item['coverage_rate']}, covered={item['covered_tickers']}, "
            f"opw={item['opw_tickers']}, missing={item['missing_in_shadow_count']}"
        )
    lines.extend(
        [
            "",
            "## Preco (close_operational)",
            f"- janela_sombra: {json.dumps(price_sessions_oper, ensure_ascii=False)}",
            f"- historico_comum: {json.dumps(price_hist_oper, ensure_ascii=False)}",
            "",
            "## Eventos",
            f"- split_mismatch_count: {len(split_mismatch_rows)}",
            f"- dividend_mismatch_count: {len(div_mismatch)}",
            f"- dividend_currency_case_count: {len(div_currency_cases)}",
            "",
            "## Intersecoes de risco",
            f"- missing_in_open_positions: {missing_in_open_positions}",
            f"- missing_in_candidates: {missing_in_candidates}",
            "",
            f"## Draft exclusoes: {draft_path}",
        ]
    )
    Path(args.out_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "ok", "verdict": verdict, "out_json": str(out_json)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
