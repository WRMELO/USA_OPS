"""Camada canonica de calculo para o Analista US.

Produz data/ssot/contexto_analista_us.json com todos os valores que a skill
analista-usa reexecutava em linguagem natural, eliminando duplicacao de logica
com o motor. O JSON e a unica fonte da verdade para os passos numericos da skill.

Executar:
    ./.venv/bin/python pipeline/analise_us.py [--date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline import ledger as _ledger_mod  # noqa: E402
from lib.spc import spc_status_and_rules as _spc_status_and_rules  # noqa: E402
from lib.trading_calendar import next_session as _next_session  # noqa: E402
from lib.trading_calendar import sessions_in_range as _sessions_in_range  # noqa: E402


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        out = float(v)
        return out if out == out else default
    except Exception:
        return default


def _load_trading_days_us() -> list[date]:
    p = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not p.exists():
        return []
    cal = pd.read_parquet(p, columns=["date"])
    if cal.empty:
        return []
    cal["date"] = pd.to_datetime(cal["date"], errors="coerce")
    return sorted(set(cal["date"].dt.date.dropna().tolist()))


def _iter_daily_docs() -> list[tuple[date, date | None, dict[str, Any]]]:
    daily_dir = ROOT / "data" / "daily"
    if not daily_dir.exists():
        return []

    docs: list[tuple[date, date | None, dict[str, Any]]] = []
    for p in daily_dir.glob("decision_*.json"):
        try:
            decision_day = date.fromisoformat(p.stem.replace("decision_", ""))
        except Exception:
            continue
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue

        ref_day: date | None = None
        for key in ("scores_reference_date_d_minus_1", "market_day"):
            raw = payload.get(key)
            if raw:
                try:
                    ref_day = date.fromisoformat(str(raw))
                    break
                except Exception:
                    pass
        docs.append((decision_day, ref_day, payload))

    docs.sort(key=lambda x: x[0], reverse=True)
    return docs


def _load_daily_for_market_day(market_day: date) -> dict[str, Any] | None:
    docs = _iter_daily_docs()
    exact = [item for item in docs if item[1] == market_day]
    if exact:
        return exact[0][2]

    eligible = [item for item in docs if item[0] <= market_day]
    if eligible:
        return eligible[0][2]

    return docs[0][2] if docs else None


def _load_latest_daily(as_of_day: date) -> dict[str, Any] | None:
    return _load_daily_for_market_day(as_of_day)


def _load_prev_day_daily(as_of_day: date) -> dict[str, Any] | None:
    trading_days = _load_trading_days_us()
    prev_days = [d for d in trading_days if d < as_of_day]
    if not prev_days:
        return None
    prev_day = max(prev_days)
    return _load_daily_for_market_day(prev_day)


def _load_latest_real(as_of_day: date) -> dict[str, Any] | None:
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return None
    candidates = []
    for p in real_dir.glob("*.json"):
        try:
            d = date.fromisoformat(p.stem)
            if d <= as_of_day:
                candidates.append((d, p))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return json.loads(candidates[0][1].read_text(encoding="utf-8"))


def _default_real_ledger_path() -> Path:
    return ROOT / "data" / "live_real_test" / "ledger_real.jsonl"


def _real_test_active(ledger_path: Path) -> bool:
    if not ledger_path.exists():
        return False
    try:
        with ledger_path.open("r", encoding="utf-8") as fp:
            for raw_line in fp:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if str(payload.get("type", "")).upper() == "APORTE":
                    return True
    except Exception:
        return False
    return False


def _load_real_ledger_doc(ledger_path: Path, exec_day: date) -> dict[str, Any]:
    previous_path = _ledger_mod.LEDGER_PATH
    try:
        _ledger_mod.LEDGER_PATH = ledger_path
        cash = _ledger_mod.compute_cash(exec_day)
        snapshot = _ledger_mod.export_snapshot(exec_day)
    finally:
        _ledger_mod.LEDGER_PATH = previous_path

    return {
        "positions_snapshot": snapshot,
        "cash_free": float(cash.get("cash_free", 0.0)),
        "cash_accounting": float(cash.get("cash_accounting", 0.0)),
    }


def _load_last_rebalance() -> dict[str, Any]:
    p = ROOT / "data" / "daily" / "last_rebalance.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _calc_rebalance_info(last_rebalance_dt_str: str, cadence: int, as_of_day: date) -> dict[str, Any]:
    cadence = max(int(cadence), 1)
    try:
        last_dt = date.fromisoformat(str(last_rebalance_dt_str))
    except Exception:
        return {
            "is_rebalance_day": None,
            "last_rebalance_dt": None,
            "next_rebalance_date": None,
            "cycles_to_next_rebalance": None,
        }

    since = _sessions_in_range(last_dt + timedelta(days=1), as_of_day, exchange="XNYS")
    sessions_since = len(since)
    is_reb = sessions_since == cadence

    if is_reb:
        return {
            "is_rebalance_day": True,
            "last_rebalance_dt": str(last_dt),
            "next_rebalance_date": str(as_of_day),
            "cycles_to_next_rebalance": 0,
        }

    cursor = as_of_day
    next_reb: date | None = None
    for _ in range(500):
        cursor = _next_session(cursor, exchange="XNYS")
        sessions_to_cursor = _sessions_in_range(last_dt + timedelta(days=1), cursor, exchange="XNYS")
        if len(sessions_to_cursor) >= cadence:
            next_reb = cursor
            break

    cycles_remaining: int | None = None
    if next_reb is not None:
        between = _sessions_in_range(as_of_day, next_reb, exchange="XNYS")
        cycles_remaining = max(len(between) - 1, 0)

    return {
        "is_rebalance_day": False,
        "last_rebalance_dt": str(last_dt),
        "next_rebalance_date": str(next_reb) if next_reb else None,
        "cycles_to_next_rebalance": cycles_remaining,
    }


def _load_spc_window(market_day: date, tickers: list[str]) -> pd.DataFrame:
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists() or not tickers:
        return pd.DataFrame()

    cols = [
        "date",
        "ticker",
        "close_operational",
        "i_value",
        "i_ucl",
        "i_lcl",
        "mr_value",
        "mr_ucl",
        "xbar_value",
        "xbar_ucl",
        "xbar_lcl",
        "r_value",
        "r_ucl",
    ]
    df = pd.read_parquet(path, columns=cols).copy()
    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df[(df["date"] <= pd.Timestamp(market_day)) & (df["ticker"].isin(tickers))]
    if df.empty:
        return pd.DataFrame()
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)


def _load_scores_for_day(market_day: date, tickers: list[str]) -> dict[str, dict[str, Any]]:
    path = ROOT / "data" / "features" / "scores_m3_us.parquet"
    if not path.exists() or not tickers:
        return {}

    cols = ["date", "ticker", "m3_rank", "score_m3", "ret_62"]
    try:
        scores = pd.read_parquet(path, columns=cols).copy()
    except Exception:
        return {}

    if scores.empty:
        return {}

    scores["date"] = pd.to_datetime(scores["date"], errors="coerce").dt.normalize()
    scores["ticker"] = scores["ticker"].astype(str).str.upper().str.strip()
    scores = scores[(scores["date"] <= pd.Timestamp(market_day)) & (scores["ticker"].isin(tickers))]
    if scores.empty:
        return {}

    scores = scores.sort_values(["ticker", "date"])
    out: dict[str, dict[str, Any]] = {}
    for tk, g in scores.groupby("ticker", sort=False):
        last = g.iloc[-1]
        rank_raw = pd.to_numeric(last.get("m3_rank"), errors="coerce")
        score_raw = pd.to_numeric(last.get("score_m3"), errors="coerce")
        ret62_raw = pd.to_numeric(last.get("ret_62"), errors="coerce")
        out[str(tk)] = {
            "m3_rank": int(rank_raw) if pd.notna(rank_raw) else None,
            "score_m3": float(score_raw) if pd.notna(score_raw) else None,
            "ret_62": float(ret62_raw) if pd.notna(ret62_raw) else None,
        }
    return out


def _compute_persistencia(ticker: str, as_of_day: date, cadence: int = 10) -> int:
    tk = str(ticker).upper().strip()
    if not tk:
        return 0

    docs = _iter_daily_docs()
    eligible = []
    for decision_day, ref_day, payload in docs:
        if ref_day is not None and ref_day <= as_of_day:
            eligible.append((decision_day, payload))
        elif ref_day is None and decision_day <= as_of_day:
            eligible.append((decision_day, payload))

    window = eligible[: max(int(cadence), 1)]
    count = 0
    for _, payload in window:
        operational = payload.get("operational_ranking", []) or payload.get("portfolio", []) or []
        tickers = {
            str(item.get("ticker", "")).upper().strip()
            for item in operational
            if isinstance(item, dict)
        }
        if tk in tickers:
            count += 1
    return count


def _normalize_positions(positions_snapshot: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker: dict[str, dict[str, Any]] = {}
    for pos in positions_snapshot:
        tk = str(pos.get("ticker", "")).upper().strip()
        if not tk:
            continue
        qty = _safe_float(pos.get("qtd", pos.get("quantity", pos.get("qty", 0))), 0.0)
        if qty <= 0:
            continue
        cost = _safe_float(pos.get("preco_compra", pos.get("avg_cost", pos.get("average_price", 0.0))), 0.0)
        purchase_date = str(pos.get("data_compra", pos.get("purchase_date", pos.get("entry_date", ""))) or "")
        rec = by_ticker.setdefault(
            tk,
            {
                "ticker": tk,
                "qty": 0.0,
                "cost_value": 0.0,
                "purchase_date": purchase_date,
            },
        )
        rec["qty"] += qty
        rec["cost_value"] += qty * cost
        if purchase_date and (not rec["purchase_date"] or purchase_date < rec["purchase_date"]):
            rec["purchase_date"] = purchase_date

    out = []
    for rec in by_ticker.values():
        qty = float(rec["qty"])
        avg_cost = rec["cost_value"] / qty if qty > 0 else 0.0
        out.append(
            {
                "ticker": rec["ticker"],
                "qty": qty,
                "avg_cost": round(avg_cost, 4),
                "purchase_date": rec["purchase_date"],
            }
        )
    return sorted(out, key=lambda x: x["ticker"])


def build_context(
    market_day: date,
    real_test: str = "auto",
    ledger_path: Path | None = None,
    exec_day: date | None = None,
) -> dict[str, Any]:
    cfg_path = ROOT / "config" / "winner_us.json"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8")) if cfg_path.exists() else {}
    wcfg = cfg.get("winner_config_snapshot", {})
    top_n = int(wcfg.get("top_n", 20))
    cadence = int(wcfg.get("rebalance_cadence", 10))
    anchor = str(wcfg.get("rebalance_anchor_date", ""))
    max_weight_cap = float(wcfg.get("max_weight_cap", 0.06))

    last_reb_doc = _load_last_rebalance()
    last_rebalance_dt = str(last_reb_doc.get("last_rebalance_dt", "") or anchor)
    rebalance_info = _calc_rebalance_info(last_rebalance_dt, cadence, market_day)

    daily_doc = _load_latest_daily(market_day) or {}
    prev_daily_doc = _load_prev_day_daily(market_day) or {}
    real_ledger_path = ledger_path or _default_real_ledger_path()
    if real_test == "on":
        real_test_active = True
    elif real_test == "off":
        real_test_active = False
    else:
        real_test_active = _real_test_active(real_ledger_path)

    real_test_exec_day = exec_day or _next_session(market_day, exchange="XNYS")
    if real_test_active:
        real_doc = _load_real_ledger_doc(real_ledger_path, real_test_exec_day)
    else:
        real_doc = _load_latest_real(market_day) or {}

    action = str(daily_doc.get("action", "MERCADO")).upper()
    top20_by_score = daily_doc.get("top20_by_score", []) or []
    selected_tickers = [str(t).upper().strip() for t in (daily_doc.get("selected_tickers", []) or []) if str(t).strip()]
    operational_raw = daily_doc.get("operational_ranking", []) or daily_doc.get("portfolio", []) or []
    target_weights: dict[str, float] = {}
    for tk, w in (daily_doc.get("target_weights", {}) or {}).items():
        key = str(tk).upper().strip()
        if not key:
            continue
        target_weights[key] = _safe_float(w, 0.0)

    master_entries = []
    for pos, row in enumerate(operational_raw, 1):
        if not isinstance(row, dict):
            continue
        tk = str(row.get("ticker", "")).upper().strip()
        if not tk:
            continue
        display_rank = pd.to_numeric(row.get("rank", pos), errors="coerce")
        m3_rank = pd.to_numeric(row.get("m3_rank", row.get("rank", pos)), errors="coerce")
        score = pd.to_numeric(row.get("score_m3"), errors="coerce")
        master_entries.append(
            {
                "ticker": tk,
                "rank": int(display_rank) if pd.notna(display_rank) else -1,
                "m3_rank": int(m3_rank) if pd.notna(m3_rank) else -1,
                "raw_m3_rank": int(m3_rank) if pd.notna(m3_rank) else -1,
                "score_m3": float(score) if pd.notna(score) else None,
            }
        )
    master_entries.sort(key=lambda x: x["m3_rank"])

    master_map = {item["ticker"]: item for item in master_entries}
    positions = _normalize_positions(real_doc.get("positions_snapshot", []) or [])
    held_tickers = [p["ticker"] for p in positions]

    candidate_tickers = [item["ticker"] for item in master_entries if item["ticker"] not in set(held_tickers)]
    score_tickers = sorted(set(held_tickers + candidate_tickers + selected_tickers))
    spc_window = _load_spc_window(market_day, score_tickers)
    scores_map = _load_scores_for_day(market_day, score_tickers)

    cash_free = _safe_float(real_doc.get("cash_free", 0.0))
    cash_acc = _safe_float(real_doc.get("cash_accounting", 0.0))
    trading_days_all = _load_trading_days_us()

    holdings_out: list[dict[str, Any]] = []
    total_mkt = 0.0
    for pos in positions:
        tk = pos["ticker"]
        qty = float(pos["qty"])
        avg_cost = _safe_float(pos["avg_cost"])
        purchase_date = pos.get("purchase_date", "")
        df_tk = spc_window[spc_window["ticker"] == tk].sort_values("date")

        close_d1 = _safe_float(df_tk.iloc[-1].get("close_operational"), 0.0) if not df_tk.empty else 0.0
        value = qty * close_d1
        total_mkt += value

        try:
            ign_date = date.fromisoformat(purchase_date) if purchase_date else None
        except Exception:
            ign_date = None

        heat_pct = ((close_d1 / avg_cost) - 1.0) * 100.0 if avg_cost > 0 else 0.0
        if ign_date is not None and not df_tk.empty:
            df_since = df_tk[df_tk["date"] >= pd.Timestamp(ign_date)]
            peak_close = _safe_float(df_since["close_operational"].max(), close_d1) if not df_since.empty else close_d1
        else:
            peak_close = close_d1

        drawdown_pct = ((close_d1 / peak_close) - 1.0) * 100.0 if peak_close > 0 else 0.0
        spc_status, spc_rules, nelson_flags = _spc_status_and_rules(df_tk)
        score_row = scores_map.get(tk, {})
        m_entry = master_map.get(tk, {})

        holdings_out.append(
            {
                "ticker": tk,
                "qty": qty,
                "avg_cost": round(avg_cost, 4),
                "close_d1": round(close_d1, 4),
                "valor_mercado": round(value, 2),
                "heat_pct": round(heat_pct, 2),
                "peak_close": round(peak_close, 4),
                "drawdown_pct": round(drawdown_pct, 2),
                "in_master": tk in master_map,
                "master_rank": int(m_entry.get("m3_rank", -1)) if tk in master_map else -1,
                "score_m3": m_entry.get("score_m3", score_row.get("score_m3")),
                "spc_status": spc_status,
                "spc_rules_fired": spc_rules,
                "nelson_we_flags": nelson_flags,
                "carga_termica_pct": 0.0,
                "ciclos_aceso": len([d for d in trading_days_all if ign_date is not None and ign_date <= d <= market_day])
                if ign_date is not None
                else 0,
                "purchase_date": purchase_date,
            }
        )

    total_ativo = total_mkt + cash_free + cash_acc
    for h in holdings_out:
        h["carga_termica_pct"] = round((h["valor_mercado"] / total_ativo) * 100.0, 2) if total_ativo > 0 else 0.0

    hhindex = sum((h["carga_termica_pct"] / 100.0) ** 2 for h in holdings_out) if holdings_out else 0.0
    rule1_blocked = sorted([h["ticker"] for h in holdings_out if h["spc_status"] == "INSTAVEL"])

    cycles_to_next = rebalance_info.get("cycles_to_next_rebalance")
    candidates_out: list[dict[str, Any]] = []
    for entry in master_entries:
        tk = entry["ticker"]
        if tk in set(held_tickers):
            continue

        df_tk = spc_window[spc_window["ticker"] == tk].sort_values("date")
        close_d1 = _safe_float(df_tk.iloc[-1].get("close_operational"), 0.0) if not df_tk.empty else 0.0
        spc_status, spc_rules, nelson_flags = _spc_status_and_rules(df_tk)
        persistencia = _compute_persistencia(tk, market_day, cadence=10)
        score_row = scores_map.get(tk, {})
        ret_62 = score_row.get("ret_62")

        veto: str | None = None
        if int(entry.get("m3_rank", -1)) > top_n:
            veto = "VETADO_TOP_N"
        elif spc_status == "INSTAVEL":
            veto = "VETADO_SPC"
        elif ret_62 is not None and float(ret_62) >= 1.0:
            veto = "VETADO_R037"

        alerta_parts: list[str] = []
        if cycles_to_next is not None and int(cycles_to_next) <= 2 and persistencia <= 2:
            alerta_parts.append("GATE_R034")
        if nelson_flags and veto is None:
            alerta_parts.append("ALERTA_WE")
        alerta = "+".join(alerta_parts) if alerta_parts else None

        candidates_out.append(
            {
                "ticker": tk,
                "master_rank": int(entry.get("m3_rank", -1)),
                "score_m3": entry.get("score_m3", score_row.get("score_m3")),
                "close_d1": round(close_d1, 4),
                "spc_status": spc_status,
                "spc_rules_fired": spc_rules,
                "nelson_we_flags": nelson_flags,
                "ret_62": round(float(ret_62), 6) if ret_62 is not None else None,
                "persistencia": persistencia,
                "veto": veto,
                "alerta": alerta,
            }
        )

    veto_events = daily_doc.get("bandexp_ret62_veto_events", []) or []
    candidate_tickers_set = {str(c.get("ticker", "")).upper().strip() for c in candidates_out}
    held_tickers_set = set(held_tickers)
    for event in veto_events:
        if not isinstance(event, dict):
            continue
        tk = str(event.get("ticker", "")).upper().strip()
        if not tk or tk in candidate_tickers_set or tk in held_tickers_set:
            continue
        m3_rank = pd.to_numeric(event.get("m3_rank"), errors="coerce")
        score = pd.to_numeric(event.get("score_m3"), errors="coerce")
        ret_62 = pd.to_numeric(event.get("ret_62"), errors="coerce")
        candidates_out.append(
            {
                "ticker": tk,
                "master_rank": int(m3_rank) if pd.notna(m3_rank) else -1,
                "score_m3": float(score) if pd.notna(score) else None,
                "close_d1": None,
                "spc_status": None,
                "spc_rules_fired": [],
                "nelson_we_flags": [],
                "ret_62": round(float(ret_62), 6) if pd.notna(ret_62) else None,
                "persistencia": None,
                "veto": "VETADO_BANDEXP_RET62",
                "alerta": None,
            }
        )
        candidate_tickers_set.add(tk)
    candidates_out.sort(key=lambda x: x["master_rank"])

    holdings_value_map = {h["ticker"]: float(h["valor_mercado"]) for h in holdings_out}
    is_d1_pos_rebalance = bool(
        prev_daily_doc
        and (
            bool(prev_daily_doc.get("is_rebalance_day"))
            or str(prev_daily_doc.get("action", "")).upper() == "REBALANCE"
        )
    )

    pending_list_d1: list[dict[str, Any]] | None = None
    if is_d1_pos_rebalance and total_ativo > 0:
        prev_targets = prev_daily_doc.get("target_weights", {}) or {}
        prev_selected = [str(t).upper().strip() for t in (prev_daily_doc.get("selected_tickers", []) or []) if str(t).strip()]
        pending_rows: list[dict[str, Any]] = []
        for tk in prev_selected:
            tw = _safe_float(prev_targets.get(tk, 0.0), 0.0)
            if tw <= 0:
                continue
            current_val = _safe_float(holdings_value_map.get(tk, 0.0), 0.0)
            if current_val >= (total_ativo * tw * 0.98):
                continue

            df_tk = spc_window[spc_window["ticker"] == tk].sort_values("date")
            spc_status, _, _ = _spc_status_and_rules(df_tk)
            pending_rows.append(
                {
                    "ticker": tk,
                    "target_weight": round(tw, 6),
                    "spc_status": spc_status,
                    "veto_r001": spc_status == "INSTAVEL",
                }
            )
        pending_rows.sort(key=lambda x: (-x["target_weight"], x["ticker"]))
        pending_list_d1 = pending_rows

    return {
        "market_day": str(market_day),
        "generated_at": str(pd.Timestamp.now(tz="UTC").isoformat()),
        "real_test": {
            "active": real_test_active,
            "exec_day": str(real_test_exec_day) if real_test_active else None,
            "ledger_source": str(real_ledger_path) if real_test_active else str(ROOT / "data" / "real"),
        },
        "forno": {
            "action": action,
            "top_n": top_n,
            "rebalance_cadence": cadence,
            "max_weight_cap": max_weight_cap,
            "rebalance_anchor_date": anchor,
            "last_rebalance_dt": rebalance_info.get("last_rebalance_dt"),
            "is_rebalance_day": rebalance_info.get("is_rebalance_day"),
            "next_rebalance_date": rebalance_info.get("next_rebalance_date"),
            "cycles_to_next_rebalance": rebalance_info.get("cycles_to_next_rebalance"),
            "is_d1_pos_rebalance": is_d1_pos_rebalance,
        },
        "master": {
            "date": str(daily_doc.get("target_date", daily_doc.get("date", ""))),
            "operational_ranking": master_entries,
            "top20_by_score": top20_by_score,
            "bandexp_ret62_veto_events": veto_events,
            "selected_tickers": selected_tickers,
            "target_weights": target_weights,
        },
        "holdings": holdings_out,
        "cash": {
            "cash_free": round(cash_free, 2),
            "cash_accounting": round(cash_acc, 2),
            "total_ativo": round(total_ativo, 2),
            "hhindex": round(hhindex, 4),
        },
        "candidates": candidates_out,
        "rule1_blocked": rule1_blocked,
        "pending_list_d1": pending_list_d1,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera contexto canonico para Analista US.")
    parser.add_argument("--date", help="market_day YYYY-MM-DD (default: ultimo pregao no operational window)")
    parser.add_argument(
        "--real-test",
        choices=["auto", "on", "off"],
        default="auto",
        help="Seleciona a fonte de caixa/posicoes: auto (detecta ledger real), on (forca ledger real), off (forca dry-run).",
    )
    parser.add_argument(
        "--ledger-dir",
        default=None,
        help="Diretorio do ledger real (default: data/live_real_test).",
    )
    args = parser.parse_args()

    if args.date:
        market_day = date.fromisoformat(args.date)
    else:
        trading_days = _load_trading_days_us()
        if not trading_days:
            print("ERRO: operational_window.parquet ausente ou vazio", file=sys.stderr)
            sys.exit(1)
        market_day = max(trading_days)

    ledger_path: Path | None = None
    if args.ledger_dir:
        ledger_dir = Path(args.ledger_dir)
        if not ledger_dir.is_absolute():
            ledger_dir = (ROOT / ledger_dir).resolve()
        ledger_path = ledger_dir / "ledger_real.jsonl"

    print(f"Calculando contexto US para market_day={market_day} ...")
    ctx = build_context(market_day, real_test=args.real_test, ledger_path=ledger_path)

    out_path = ROOT / "data" / "ssot" / "contexto_analista_us.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(ctx, indent=2, default=str, ensure_ascii=False), encoding="utf-8")

    print(f"OK -> {out_path}")
    print(f"  real_test_active={ctx['real_test']['active']}")
    print(f"  is_rebalance_day={ctx['forno']['is_rebalance_day']}")
    print(f"  cycles_to_next_rebalance={ctx['forno']['cycles_to_next_rebalance']}")
    print(f"  next_rebalance_date={ctx['forno']['next_rebalance_date']}")
    print(f"  holdings={len(ctx['holdings'])}  candidates={len(ctx['candidates'])}")


if __name__ == "__main__":
    main()

