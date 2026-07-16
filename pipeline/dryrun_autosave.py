"""Autosave do dry-run US (F-18)."""
from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from pipeline import painel_diario, servidor

ROOT = Path(__file__).resolve().parents[1]


def _load_trading_days(max_day: date) -> list[date]:
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists():
        return [max_day]
    try:
        df = pd.read_parquet(path, columns=["date"])
    except Exception:
        return [max_day]
    if df.empty:
        return [max_day]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    days = sorted({d for d in df["date"].dt.date.dropna().tolist() if d <= max_day})
    return days or [max_day]


def _existing_market_days(max_day: date) -> set[date]:
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return set()
    out: set[date] = set()
    for p in real_dir.glob("*.json"):
        try:
            d = date.fromisoformat(p.stem)
        except ValueError:
            continue
        if d <= max_day:
            out.add(d)
    return out


def _pending_market_days(max_day: date) -> list[date]:
    trading_days = _load_trading_days(max_day)
    existing = _existing_market_days(max_day)
    if not existing:
        return [d for d in trading_days if d <= max_day]
    last_saved = max(existing)
    return [d for d in trading_days if last_saved < d <= max_day and d not in existing]


def _market_day_to_exec_day(market_day: date) -> date:
    return painel_diario._resolve_trade_day(market_day + timedelta(days=1))


def _append_autosave_log(payload: dict[str, Any]) -> None:
    path = ROOT / "data" / "daily" / "autosave_log.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def autosave_pending_days(as_of: date | None = None) -> list[dict[str, Any]]:
    ref_day = as_of or date.today()
    max_market_day = painel_diario.get_d_minus_1(ref_day)
    pending_days = _pending_market_days(max_market_day)
    results: list[dict[str, Any]] = []

    for market_day in pending_days:
        out_path = ROOT / "data" / "real" / f"{market_day.isoformat()}.json"
        if out_path.exists():
            continue

        exec_day = _market_day_to_exec_day(market_day)
        computed = painel_diario.compute_dryrun_autosave_operations(exec_day)
        computed_market_day = str(computed.get("market_day", "")).strip()
        if computed_market_day and computed_market_day != market_day.isoformat():
            raise RuntimeError(
                f"Autosave inconsistente: market_day esperado={market_day.isoformat()} "
                f"calculado={computed_market_day}"
            )

        payload = {
            "exec_day": str(computed.get("exec_day", exec_day.isoformat())),
            "market_day": market_day.isoformat(),
            "trade_day": str(computed.get("trade_day", exec_day.isoformat())),
            "operations": list(computed.get("operations", [])),
            "cash_movements": [],
            "cash_transfers": [],
        }
        result = servidor.apply_boletim_operations(payload)
        _append_autosave_log(
            {
                "timestamp": datetime.now(tz=UTC).isoformat(),
                "source": "autosave",
                "market_day": market_day.isoformat(),
                "exec_day": payload["exec_day"],
                "trade_day": payload["trade_day"],
                "n_operations": len(payload["operations"]),
            }
        )
        results.append({"market_day": market_day.isoformat(), "paths": result.get("paths", [])})
    return results

