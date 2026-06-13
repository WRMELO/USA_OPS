"""Painel diario US no formato BR (T-037 / D-027)."""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.ledger import (
    EventType,
    compute_cash,
    compute_positions,
    pending_settlements,
    sells_in_settlement,
    read_all_events,
)
from lib.trading_calendar import sessions_in_range
PROJECT_START = date(2026, 3, 19)


class Lot:
    def __init__(self, ticker: str, buy_date: str, qtd: int, buy_price: float):
        self.ticker = ticker
        self.buy_date = buy_date
        self.qtd = qtd
        self.buy_price = buy_price

    @property
    def buy_value(self) -> float:
        return self.qtd * self.buy_price


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _detect_and_adjust_splits(
    lots: list["Lot"],
    as_of_day: date,
) -> tuple[list["Lot"], list[dict[str, Any]]]:
    """Detecta splits via split_factor e ajusta qtd/preco dos lotes."""
    if not lots:
        return lots, []
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists():
        return lots, []
    tickers = sorted({lot.ticker for lot in lots})
    try:
        df = pd.read_parquet(path, columns=["date", "ticker", "split_factor"])
    except Exception:
        return lots, []

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["split_factor"] = pd.to_numeric(df["split_factor"], errors="coerce").fillna(1.0)
    # Auditor Gemini H1: nunca usar split_factor futuro para boletim historico.
    df = df[df["date"] <= pd.Timestamp(as_of_day)]
    df = df[df["ticker"].isin(tickers)].sort_values(["ticker", "date"]).dropna(subset=["date"])
    if df.empty:
        return lots, []

    sf_latest: dict[str, float] = {}
    sf_by_ticker: dict[str, pd.DataFrame] = {}
    for tk in tickers:
        sub = df[df["ticker"] == tk]
        if sub.empty:
            continue
        sf_latest[tk] = float(sub.iloc[-1]["split_factor"])
        sf_by_ticker[tk] = sub

    corporate_actions: list[dict[str, Any]] = []
    adjusted: list[Lot] = []
    seen_splits: set[tuple[str, str]] = set()

    for lot in lots:
        tk = lot.ticker
        sub = sf_by_ticker.get(tk)
        if sub is None:
            adjusted.append(lot)
            continue

        buy_ts = pd.Timestamp(lot.buy_date)
        sub_buy = sub[sub["date"] <= buy_ts]
        if sub_buy.empty:
            sub_buy = sub.head(1)
        sf_buy = float(sub_buy.iloc[-1]["split_factor"])
        sf_now = sf_latest.get(tk, sf_buy)

        if abs(sf_buy - sf_now) < 1e-9:
            adjusted.append(lot)
            continue

        ratio = sf_now / sf_buy
        new_qtd = round(lot.qtd * ratio)
        new_price = round(lot.buy_price / ratio, 4)
        int_ratio = int(round(ratio))
        ratio_str = f"{int_ratio}:1" if ratio > 1 else f"1:{int(round(1 / ratio))}"

        key = (tk, ratio_str)
        if key not in seen_splits:
            seen_splits.add(key)
            corporate_actions.append(
                {
                    "type": "SPLIT",
                    "ticker": tk,
                    "ratio": ratio_str,
                    "detection_date": as_of_day.isoformat(),
                    "source": f"operational_window.split_factor {sf_buy:.6f} -> {sf_now:.6f}",
                    "adjustment_applied": {
                        "qtd_before": lot.qtd,
                        "qtd_after": new_qtd,
                        "preco_compra_before": lot.buy_price,
                        "preco_compra_after": new_price,
                    },
                    "note": (
                        f"Forward split {ratio_str} detectado. "
                        f"Posicao ajustada: custo total invariante (${lot.buy_value:,.2f})."
                    ),
                }
            )

        adjusted.append(Lot(ticker=tk, buy_date=lot.buy_date, qtd=new_qtd, buy_price=new_price))

    return adjusted, corporate_actions


def _fmt_date_br(v: str | date) -> str:
    if isinstance(v, date):
        d = v
    else:
        d = date.fromisoformat(str(v))
    return f"{d.day:02d}/{d.month:02d}/{d.year:04d}"


def _fmt_int(v: int | float) -> str:
    return f"{int(v):,}"


def _fmt_money(v: float | int) -> str:
    return f"$ {float(v):,.2f}"


def _fmt_pct(v: float | int) -> str:
    return f"{float(v):.2f}%"


def list_real_files_upto(max_day: date) -> list[Path]:
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return []
    files: list[tuple[date, Path]] = []
    for p in real_dir.glob("*.json"):
        try:
            d = date.fromisoformat(p.stem)
            if d <= max_day:
                files.append((d, p))
        except Exception:
            continue
    files.sort(key=lambda x: x[0])
    return [p for _, p in files]


def load_latest_real_before(ref_day: date) -> tuple[date | None, dict[str, Any] | None]:
    files = list_real_files_upto(ref_day)
    if not files:
        return None, None
    p = files[-1]
    return date.fromisoformat(p.stem), _read_json(p)


def get_d_minus_1(exec_day: date) -> date:
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists():
        return exec_day
    df = pd.read_parquet(path, columns=["date"])
    if df.empty:
        return exec_day
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    days = sorted(set(df["date"].dt.date.dropna().tolist()))
    prev = [d for d in days if d < exec_day]
    return max(prev) if prev else exec_day


def _resolve_to_trading_day(civil_day: date) -> date:
    """Resolve uma data civil para o último pregão real <= civil_day (D-038)."""
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists():
        return civil_day
    df = pd.read_parquet(path, columns=["date"])
    if df.empty:
        return civil_day
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    days = sorted(set(df["date"].dt.date.dropna().tolist()))
    cands = [d for d in days if d <= civil_day]
    return cands[-1] if cands else civil_day


def _resolve_trade_day(civil_day: date) -> date:
    """Resolve uma data civil para o próximo pregão real >= civil_day."""
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists():
        return civil_day
    df = pd.read_parquet(path, columns=["date"])
    if df.empty:
        return civil_day
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    days = sorted(set(df["date"].dt.date.dropna().tolist()))
    if civil_day in days:
        return civil_day
    cands = [d for d in days if d > civil_day]
    return cands[0] if cands else civil_day


def get_latest_prices(tickers: list[str], as_of_day: date) -> dict[str, float]:
    prices: dict[str, float] = {}
    if not tickers:
        return prices
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists():
        return prices
    df = pd.read_parquet(path, columns=["date", "ticker", "close_operational"])
    if df.empty:
        return prices
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = df[df["date"] <= pd.Timestamp(as_of_day)]
    for tk in tickers:
        s = df[df["ticker"] == tk].sort_values("date")
        if not s.empty:
            prices[tk] = _safe_float(s.iloc[-1]["close_operational"], 0.0)
    return prices


def _extract_operations(day_payload: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for op in day_payload.get("operations", []):
        typ = str(op.get("type", "")).upper().strip()
        if typ not in {"COMPRA", "VENDA"}:
            continue
        out.append(
            {
                "type": typ,
                "ticker": str(op.get("ticker", "")).upper().strip(),
                "qtd": _safe_int(op.get("qtd"), 0),
                "preco": _safe_float(op.get("preco"), 0.0),
            }
        )
    return out


def _extract_cash_movements(day_payload: dict[str, Any]) -> tuple[float, float]:
    aportes = 0.0
    retiradas = 0.0
    for mv in day_payload.get("cash_movements", []):
        typ = str(mv.get("type", "")).upper().strip()
        val = _safe_float(mv.get("value", mv.get("valor", 0.0)), 0.0)
        if typ in {"APORTE", "DEPOSITO", "DIVIDENDO", "JCP", "BONIFICACAO", "BONUS", "SUBSCRICAO"}:
            aportes += val
        elif typ in {"RETIRADA", "SAQUE"}:
            retiradas += val
    return aportes, retiradas


def _extract_transfers(day_payload: dict[str, Any]) -> float:
    total = 0.0
    for tr in day_payload.get("cash_transfers", []):
        total += _safe_float(tr.get("value", tr.get("valor", 0.0)), 0.0)
    return total


def _calc_cash_balances(
    prev_free: float,
    prev_acc: float,
    buy: float,
    sell: float,
    aporte: float,
    retirada: float,
    transfer: float,
) -> tuple[float, float]:
    free = prev_free + transfer + aporte - retirada - buy
    acc = prev_acc + sell - transfer
    return free, acc


def _pending_sales_for_transfer(exec_day: date) -> list[dict[str, Any]]:
    return pending_settlements(exec_day)


def _sells_in_settlement_for_display(exec_day: date) -> list[dict[str, Any]]:
    return sells_in_settlement(exec_day)


def build_lot_ledger(until_day: date) -> tuple[list[Lot], list[str]]:
    pos = compute_positions(until_day)
    out: list[Lot] = []
    for tk in sorted(pos.keys()):
        for lot in pos[tk]:
            qtd = _safe_int(lot.get("qtd"), 0)
            px = _safe_float(lot.get("buy_price"), 0.0)
            buy_date = str(lot.get("buy_date", until_day.isoformat()))
            if qtd <= 0 or px <= 0:
                continue
            out.append(Lot(ticker=tk, buy_date=buy_date, qtd=qtd, buy_price=px))
    return out, []


def _load_trading_days_us() -> list[date]:
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if path.exists():
        try:
            df = pd.read_parquet(path, columns=["date"])
        except Exception:
            df = pd.DataFrame(columns=["date"])
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            days = sorted(set(df["date"].dt.date.dropna().tolist()))
            if days:
                return days
    return sessions_in_range(start=PROJECT_START, end=date.today(), exchange="XNYS")


def _load_last_rebalance_dt() -> date | None:
    payload = _read_json(ROOT / "data" / "daily" / "last_rebalance.json")
    raw = str(payload.get("last_rebalance_dt", "")).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except Exception:
        return None


def _calc_next_rebalance_us(last_rebalance_dt: date | None, cadence: int = 10) -> date | None:
    if last_rebalance_dt is None or cadence <= 0:
        return None
    start = last_rebalance_dt + timedelta(days=1)
    end = last_rebalance_dt + timedelta(days=cadence * 3)
    sessions = sessions_in_range(start=start, end=end, exchange="XNYS")
    if len(sessions) < cadence:
        return None
    return sessions[cadence - 1]


def _load_curve_until(as_of_day: date) -> pd.DataFrame:
    curve_path = ROOT / "data" / "daily" / "winner_curve_us.parquet"
    if not curve_path.exists():
        return pd.DataFrame(columns=["date", "equity"])
    curve = pd.read_parquet(curve_path)
    if curve.empty:
        return pd.DataFrame(columns=["date", "equity"])
    curve["date"] = pd.to_datetime(curve["date"], errors="coerce")
    curve["equity"] = pd.to_numeric(curve["equity"], errors="coerce")
    curve = curve.dropna(subset=["date", "equity"]).sort_values("date")
    curve = curve[curve["date"] <= pd.Timestamp(as_of_day)].copy()
    if curve.empty:
        return curve
    curve["running_max"] = curve["equity"].cummax()
    curve["drawdown_pct"] = ((curve["equity"] / curve["running_max"]) - 1.0) * 100.0
    return curve


def _build_chart_252(
    curve: pd.DataFrame,
    as_of_day: date,
    decision: dict[str, Any] | None = None,
) -> tuple[str, str]:
    if curve.empty:
        chart_html = "<div class='chart-empty'>Curva de equity indisponível.</div>"
        decision = decision or {}
        is_rebalance_day = bool(decision.get("is_rebalance_day", False))
        last_rebalance_dt = _load_last_rebalance_dt()
        next_rebalance_dt = _calc_next_rebalance_us(last_rebalance_dt, cadence=10)
        motor_items = [
            ("Motor", "C4 Puro", "ok"),
            ("TopN", "20", ""),
            ("Cadência", "10 pregões", ""),
            ("Cap por ticker", "6%", ""),
            ("Market cap mín", ">= $300M", ""),
            ("Hoje é rebalanceamento", "SIM" if is_rebalance_day else "NÃO", "ok" if is_rebalance_day else "bad"),
            ("Próximo rebalanceamento", _fmt_date_br(next_rebalance_dt) if next_rebalance_dt else "N/D", "warn" if next_rebalance_dt else ""),
            ("Último rebalanceamento", _fmt_date_br(last_rebalance_dt) if last_rebalance_dt else "N/D", ""),
        ]
        motor_cells = "".join(
            f"<div class='motor-item'><div class='motor-label'>{label}</div><div class='motor-value {klass}'>{value}</div></div>"
            for label, value, klass in motor_items
        )
        motor_status_html = (
            "<div class='motor-status-wrap'>"
            "<div class='motor-status-title'>Motor C4 — Status Operacional</div>"
            f"<div class='motor-status-grid'>{motor_cells}</div>"
            "</div>"
        )
        return chart_html, motor_status_html
    last_252 = curve.tail(252).copy()
    if last_252.empty:
        chart_html = "<div class='chart-empty'>Curva de equity indisponível.</div>"
        decision = decision or {}
        is_rebalance_day = bool(decision.get("is_rebalance_day", False))
        last_rebalance_dt = _load_last_rebalance_dt()
        next_rebalance_dt = _calc_next_rebalance_us(last_rebalance_dt, cadence=10)
        motor_items = [
            ("Motor", "C4 Puro", "ok"),
            ("TopN", "20", ""),
            ("Cadência", "10 pregões", ""),
            ("Cap por ticker", "6%", ""),
            ("Market cap mín", ">= $300M", ""),
            ("Hoje é rebalanceamento", "SIM" if is_rebalance_day else "NÃO", "ok" if is_rebalance_day else "bad"),
            ("Próximo rebalanceamento", _fmt_date_br(next_rebalance_dt) if next_rebalance_dt else "N/D", "warn" if next_rebalance_dt else ""),
            ("Último rebalanceamento", _fmt_date_br(last_rebalance_dt) if last_rebalance_dt else "N/D", ""),
        ]
        motor_cells = "".join(
            f"<div class='motor-item'><div class='motor-label'>{label}</div><div class='motor-value {klass}'>{value}</div></div>"
            for label, value, klass in motor_items
        )
        motor_status_html = (
            "<div class='motor-status-wrap'>"
            "<div class='motor-status-title'>Motor C4 — Status Operacional</div>"
            f"<div class='motor-status-grid'>{motor_cells}</div>"
            "</div>"
        )
        return chart_html, motor_status_html

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.76, 0.24],
    )
    fig.add_trace(
        go.Scatter(
            x=last_252["date"],
            y=last_252["equity"],
            mode="lines",
            name="Equity",
            line=dict(color="#1f77b4", width=2),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=last_252["date"],
            y=last_252["drawdown_pct"],
            mode="lines",
            name="Drawdown",
            line=dict(color="#dc2626", width=1.8),
            fill="tozeroy",
        ),
        row=2,
        col=1,
    )
    fig.update_layout(
        height=430,
        template="plotly_white",
        margin=dict(l=30, r=20, t=24, b=30),
        separators=",.",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        font_size=11,
    )
    fig.update_yaxes(row=1, col=1)
    fig.update_yaxes(row=2, col=1)
    fig.update_xaxes(type="date", tickformat="%d/%m", row=1, col=1, showticklabels=False)
    fig.update_xaxes(type="date", tickformat="%d/%m", row=2, col=1)

    decision = decision or {}
    is_rebalance_day = bool(decision.get("is_rebalance_day", False))
    last_rebalance_dt = _load_last_rebalance_dt()
    next_rebalance_dt = _calc_next_rebalance_us(last_rebalance_dt, cadence=10)
    motor_items = [
        ("Motor", "C4 Puro", "ok"),
        ("TopN", "20", ""),
        ("Cadência", "10 pregões", ""),
        ("Cap por ticker", "6%", ""),
        ("Market cap mín", ">= $300M", ""),
        ("Hoje é rebalanceamento", "SIM" if is_rebalance_day else "NÃO", "ok" if is_rebalance_day else "bad"),
        ("Próximo rebalanceamento", _fmt_date_br(next_rebalance_dt) if next_rebalance_dt else "N/D", "warn" if next_rebalance_dt else ""),
        ("Último rebalanceamento", _fmt_date_br(last_rebalance_dt) if last_rebalance_dt else "N/D", ""),
    ]
    motor_cells = "".join(
        f"<div class='motor-item'><div class='motor-label'>{label}</div><div class='motor-value {klass}'>{value}</div></div>"
        for label, value, klass in motor_items
    )
    motor_status_html = (
        "<div class='motor-status-wrap'>"
        "<div class='motor-status-title'>Motor C4 — Status Operacional</div>"
        f"<div class='motor-status-grid'>{motor_cells}</div>"
        "</div>"
    )

    chart_html = fig.to_html(full_html=False, include_plotlyjs=False).replace("%d\\u002f%m", "%d/%m")
    return chart_html, motor_status_html


def _build_real_base1_series(as_of_day: date) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])

    for p in sorted(real_dir.glob("*.json")):
        try:
            file_day = date.fromisoformat(p.stem)
        except Exception:
            continue
        payload = _read_json(p)
        exec_raw = str(payload.get("exec_day", payload.get("date", ""))).strip()
        try:
            exec_day = date.fromisoformat(exec_raw) if exec_raw else file_day
        except Exception:
            exec_day = file_day
        ref_raw = str(payload.get("market_day", payload.get("reference_decision", ""))).strip()
        try:
            ref_day = date.fromisoformat(ref_raw) if ref_raw else exec_day
        except Exception:
            ref_day = exec_day
        ref_day = _resolve_to_trading_day(ref_day)
        if ref_day > as_of_day:
            continue
        snapshot = payload.get("positions_snapshot", [])
        cash_free = _safe_float(payload.get("cash_free", payload.get("cash_balance", 0.0)), 0.0)
        cash_acc = _safe_float(payload.get("cash_accounting", payload.get("caixa_liquidando", 0.0)), 0.0)
        if (not snapshot) and abs(cash_free) < 1e-9 and abs(cash_acc) < 1e-9:
            continue
        records.append(
            {
                "exec_day": exec_day,
                "ref_day": ref_day,
                "payload": payload,
                "snapshot": snapshot,
                "cash_free": cash_free,
                "cash_acc": cash_acc,
            }
        )

    if not records:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])

    by_ref_day: dict[date, dict[str, Any]] = {}
    for rec in records:
        curr = by_ref_day.get(rec["ref_day"])
        if curr is None or rec["exec_day"] > curr["exec_day"]:
            by_ref_day[rec["ref_day"]] = rec
    ordered = [by_ref_day[d] for d in sorted(by_ref_day.keys())]
    if ordered and ordered[-1]["ref_day"] < as_of_day:
        last_rec = ordered[-1]
        ordered.append(
            {
                "exec_day": as_of_day,
                "ref_day": as_of_day,
                "snapshot": last_rec["snapshot"],
                "cash_free": last_rec["cash_free"],
                "cash_acc": last_rec["cash_acc"],
            }
        )

    # Aportes/retiradas via ledger SSOT (paridade D-095 BR); fallback legado para cash_movements de boletins
    try:
        _ledger_events = read_all_events()
        _use_ledger = len(_ledger_events) > 0
    except Exception:
        _ledger_events = []
        _use_ledger = False

    cum_aportes = 0.0
    cum_retiradas = 0.0
    base_patrimonio_by_rec: list[float] = []
    for rec in ordered:
        ref_d = rec.get("ref_day") or rec.get("exec_day")
        # exec_day e a data em que o caixa entrou/saiu; usar como corte para eventos do ledger (D-040/D-069)
        _cutoff = rec.get("exec_day") or ref_d
        if _use_ledger and _cutoff is not None:
            cum_aportes = sum(
                float(ev.amount)
                for ev in _ledger_events
                if ev.type in {EventType.APORTE, EventType.DIVIDENDO}
                and ev.exec_date <= _cutoff
            )
            cum_retiradas = sum(
                float(ev.amount)
                for ev in _ledger_events
                if ev.type == EventType.RETIRADA and ev.exec_date <= _cutoff
            )
        else:
            aporte, retirada = _extract_cash_movements(rec.get("payload", {}))
            cum_aportes += aporte
            cum_retiradas += retirada
        base_patrimonio_by_rec.append(cum_aportes - cum_retiradas)

    if not base_patrimonio_by_rec or base_patrimonio_by_rec[0] <= 0:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])

    tickers: set[str] = set()
    for rec in ordered:
        for pos in rec["snapshot"]:
            tk = str(pos.get("ticker", "")).upper().strip()
            if tk:
                tickers.add(tk)

    prices = pd.DataFrame(columns=["date", "ticker", "close_operational", "split_factor"])
    win_path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if tickers and win_path.exists():
        prices = pd.read_parquet(win_path, columns=["date", "ticker", "close_operational", "split_factor"])
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
        prices["ticker"] = prices["ticker"].astype(str).str.upper().str.strip()
        prices["close_operational"] = pd.to_numeric(prices["close_operational"], errors="coerce")
        prices["split_factor"] = pd.to_numeric(prices["split_factor"], errors="coerce").fillna(1.0)
        prices = prices.dropna(subset=["date", "ticker", "close_operational"])
        prices = prices[(prices["date"] <= pd.Timestamp(as_of_day)) & (prices["ticker"].isin(tickers))]
        prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    by_ticker: dict[str, pd.DataFrame] = {}
    if not prices.empty:
        for tk in prices["ticker"].unique():
            sub = prices[prices["ticker"] == tk][["date", "close_operational", "split_factor"]].copy()
            by_ticker[tk] = sub

    rows: list[dict[str, Any]] = []
    for idx, rec in enumerate(ordered):
        ref_ts = pd.Timestamp(rec["ref_day"])
        total_mkt = 0.0
        for pos in rec["snapshot"]:
            tk = str(pos.get("ticker", "")).upper().strip()
            qtd = _safe_int(pos.get("qtd"), 0)
            if not tk or qtd <= 0:
                continue
            buy_date_str = str(pos.get("buy_date", pos.get("data_compra", "")))
            buy_ts: pd.Timestamp | None = None
            if buy_date_str:
                try:
                    buy_ts = pd.Timestamp(buy_date_str)
                except Exception:
                    buy_ts = None
            sub = by_ticker.get(tk)
            if sub is not None and not sub.empty and buy_ts is not None:
                sf_at_buy = sub[sub["date"] <= buy_ts]
                sf_at_ref = sub[sub["date"] <= ref_ts]
                if not sf_at_buy.empty and not sf_at_ref.empty:
                    sf_buy = float(sf_at_buy.iloc[-1]["split_factor"])
                    sf_ref = float(sf_at_ref.iloc[-1]["split_factor"])
                    if abs(sf_buy - sf_ref) > 1e-9:
                        qtd = round(qtd * (sf_ref / sf_buy))
            px = _safe_float(pos.get("preco_compra", pos.get("buy_price", 0.0)), 0.0)
            if sub is not None and not sub.empty and (buy_ts is None or buy_ts <= ref_ts):
                sub_until = sub[sub["date"] <= ref_ts]
                if not sub_until.empty:
                    px = _safe_float(sub_until.iloc[-1]["close_operational"], px)
            total_mkt += qtd * px
        total_ativo = total_mkt + _safe_float(rec["cash_free"], 0.0) + _safe_float(rec["cash_acc"], 0.0)
        plot_day = rec["ref_day"]
        rows.append(
            {
                "date": pd.Timestamp(plot_day),
                "total_ativo": total_ativo,
                "base_patrimonio": base_patrimonio_by_rec[idx],
            }
        )

    out = pd.DataFrame(rows).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    if out.empty:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])
    out["base1"] = out["total_ativo"] / out["base_patrimonio"]
    out["daily_var_pct"] = out["base1"].pct_change() * 100.0
    out = out.drop(columns=["base_patrimonio"])
    return out.reset_index(drop=True)


def _build_chart_base1(as_of_day: date) -> str:
    proj = _build_real_base1_series(as_of_day=as_of_day)
    if proj.empty:
        return "<div class='chart-empty'>Base 1 indisponível.</div>"
    if len(proj) < 2:
        fig = go.Figure()
        fig.add_annotation(
            text="Apenas 1 dia de operação - gráfico disponível a partir do 2o pregão.",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=13, color="#666"),
        )
        fig.update_layout(
            height=430,
            template="plotly_white",
            margin=dict(l=30, r=20, t=24, b=30),
            separators=",.",
        )
        return fig.to_html(full_html=False, include_plotlyjs=False)

    trading_days = sorted(set(_load_trading_days_us()))
    axis_dates: list[pd.Timestamp] = []
    if trading_days:
        start_day = pd.Timestamp(proj["date"].min()).date()
        axis_dates = [pd.Timestamp(d) for d in trading_days if start_day <= d <= as_of_day]
    if not axis_dates:
        axis_dates = sorted(set(pd.to_datetime(proj["date"]).tolist()))
    axis_df = pd.DataFrame({"date": pd.to_datetime(axis_dates)})
    carteira_line = axis_df.merge(proj[["date", "base1"]], on="date", how="left")
    if "base1" in carteira_line.columns:
        carteira_line["base1"] = pd.to_numeric(carteira_line["base1"], errors="coerce").ffill().bfill()

    bar_df = proj.dropna(subset=["daily_var_pct"]).copy()
    bar_colors = ["#26a69a" if _safe_float(v, 0.0) >= 0 else "#ef5350" for v in bar_df["daily_var_pct"]]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if not bar_df.empty:
        fig.add_trace(
            go.Bar(
                x=bar_df["date"],
                y=bar_df["daily_var_pct"],
                name="Var. Diaria %",
                marker=dict(color=bar_colors),
                opacity=0.45,
            ),
            secondary_y=True,
        )
    fig.add_trace(
        go.Scatter(
            x=carteira_line["date"],
            y=carteira_line["base1"],
            mode="lines+markers",
            name="Carteira Real",
            line=dict(color="#1f77b4", width=2.5),
            marker=dict(size=6),
            connectgaps=True,
        ),
        secondary_y=False,
    )
    fig.update_layout(
        height=430,
        template="plotly_white",
        margin=dict(l=30, r=20, t=24, b=30),
        separators=",.",
        legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="right", x=1),
    )
    fig.update_xaxes(type="date", tickformat="%d/%m")
    return fig.to_html(full_html=False, include_plotlyjs=False).replace("%d\\u002f%m", "%d/%m")


def _load_score_map(as_of_day: date) -> dict[str, float]:
    path = ROOT / "data" / "features" / "scores_m3_us.parquet"
    if not path.exists():
        return {}
    df = pd.read_parquet(path, columns=["date", "ticker", "score_m3"])
    if df.empty:
        return {}
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["score_m3"] = pd.to_numeric(df["score_m3"], errors="coerce")
    df = df[(df["date"] == pd.Timestamp(as_of_day))].dropna(subset=["score_m3"])
    if df.empty:
        return {}
    return {str(r["ticker"]): float(r["score_m3"]) for _, r in df.iterrows()}


def _load_spc_snapshot(as_of_day: date, holdings_qty: dict[str, int]) -> pd.DataFrame:
    held = sorted([t for t, q in holdings_qty.items() if int(q) > 0])
    if not held:
        return pd.DataFrame()
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(
        path,
        columns=[
            "date",
            "ticker",
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
        ],
    ).copy()
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    day_df = df[(df["date"] == pd.Timestamp(as_of_day)) & (df["ticker"].isin(held))].copy()
    if day_df.empty:
        return pd.DataFrame()
    return day_df


def _compute_defensive_actions_from_holdings(spc_day: pd.DataFrame, holdings_qty: dict[str, int]) -> list[dict[str, Any]]:
    if spc_day.empty:
        return []
    actions: list[dict[str, Any]] = []
    for _, row in spc_day.iterrows():
        tk = str(row.get("ticker", "")).upper().strip()
        qtd = int(holdings_qty.get(tk, 0))
        if not tk or qtd <= 0:
            continue
        iv = _safe_float(pd.to_numeric(row.get("i_value"), errors="coerce"), float("nan"))
        iu = _safe_float(pd.to_numeric(row.get("i_ucl"), errors="coerce"), float("nan"))
        il = _safe_float(pd.to_numeric(row.get("i_lcl"), errors="coerce"), float("nan"))
        mv = _safe_float(pd.to_numeric(row.get("mr_value"), errors="coerce"), float("nan"))
        mu = _safe_float(pd.to_numeric(row.get("mr_ucl"), errors="coerce"), float("nan"))
        xv = _safe_float(pd.to_numeric(row.get("xbar_value"), errors="coerce"), float("nan"))
        xu = _safe_float(pd.to_numeric(row.get("xbar_ucl"), errors="coerce"), float("nan"))
        xl = _safe_float(pd.to_numeric(row.get("xbar_lcl"), errors="coerce"), float("nan"))
        rv = _safe_float(pd.to_numeric(row.get("r_value"), errors="coerce"), float("nan"))
        ru = _safe_float(pd.to_numeric(row.get("r_ucl"), errors="coerce"), float("nan"))

        any_rule = bool(
            (math.isfinite(iv) and math.isfinite(iu) and iv > iu)
            or (math.isfinite(iv) and math.isfinite(il) and iv < il)
            or (math.isfinite(mv) and math.isfinite(mu) and mv > mu)
            or (math.isfinite(xv) and math.isfinite(xu) and xv > xu)
            or (math.isfinite(xv) and math.isfinite(xl) and xv < xl)
            or (math.isfinite(rv) and math.isfinite(ru) and rv > ru)
        )
        if not any_rule:
            continue

        # D-USA-DOWNSIDE-GATE: gerar venda defensiva somente para instabilidade negativa
        # (paridade conceitual BR _build_defensive_candidates linha 777: `if z_prev < 0 and score >= 4`)
        downside = (
            (math.isfinite(iv) and math.isfinite(il) and iv < il)
            or (math.isfinite(xv) and math.isfinite(xl) and xv < xl)
        )
        if not downside:
            continue

        score = 4
        sell_pct = 25.0
        if (math.isfinite(iv) and math.isfinite(il) and iv < il) or (math.isfinite(mv) and math.isfinite(mu) and mv > mu):
            score = 5
            sell_pct = 50.0
        if math.isfinite(iv) and math.isfinite(il) and iv < (il - abs(il) * 0.2):
            score = 6
            sell_pct = 100.0
        actions.append(
            {
                "ticker": tk,
                "score": score,
                "sell_pct": sell_pct,
                "qtd": qtd,
            }
        )

    return sorted(actions, key=lambda x: (-int(x["score"]), str(x["ticker"])))[:5]


def _build_sell_suggestions(
    holdings_qty: dict[str, int],
    prices_d1: dict[str, float],
    as_of_day: date,
) -> list[dict[str, Any]]:
    spc_day = _load_spc_snapshot(as_of_day=as_of_day, holdings_qty=holdings_qty)
    actions = _compute_defensive_actions_from_holdings(spc_day=spc_day, holdings_qty=holdings_qty)
    out: list[dict[str, Any]] = []
    for item in actions:
        tk = str(item.get("ticker", "")).upper().strip()
        score = int(item.get("score", 0))
        if not tk:
            continue
        out.append(
            {
                "ticker": tk,
                "sell_pct": _safe_float(item.get("sell_pct", 0.0), 0.0),
                "close_d1": _safe_float(prices_d1.get(tk, 0.0), 0.0),
                "reason": f"DEFESA SPC: score={score} (venda parcial por severidade).",
            }
        )
    return out


def _build_rebalance_sell_suggestions(
    decision: dict[str, Any],
    holdings_qty: dict[str, int],
    prices_d1: dict[str, float],
    total_ativo: float,
) -> list[dict[str, Any]]:
    if not bool(decision.get("is_rebalance_day", False)):
        return []

    selected_raw = decision.get("selected_tickers", [])
    target_weights_raw = decision.get("target_weights", {})
    if not isinstance(selected_raw, list) or not isinstance(target_weights_raw, dict):
        return []

    selected = {str(t).upper().strip() for t in selected_raw if str(t).strip()}
    target_weights = {
        str(k).upper().strip(): _safe_float(v, 0.0)
        for k, v in target_weights_raw.items()
        if str(k).strip()
    }
    if not selected or not target_weights:
        return []

    cfg_path = ROOT / "config" / "winner_us.json"
    cfg = _read_json(cfg_path)
    ws = cfg.get("winner_config_snapshot", {})
    max_cap = _safe_float(ws.get("max_weight_cap", 0.06), 0.06)
    top_n = max(1, _safe_int(ws.get("top_n", 20), 20))
    target_w_default = 1.0 / float(top_n)

    out: list[dict[str, Any]] = []
    for tk_raw, qty_raw in holdings_qty.items():
        tk = str(tk_raw).upper().strip()
        qty = _safe_int(qty_raw, 0)
        if not tk or qty <= 0:
            continue

        px = _safe_float(prices_d1.get(tk, 0.0), 0.0)
        if px <= 0:
            continue

        val = qty * px
        carga = (val / total_ativo) if total_ativo > 0 else 0.0
        if tk not in selected:
            out.append(
                {
                    "ticker": tk,
                    "sell_pct": 100.0,
                    "close_d1": px,
                    "reason": "REBALANCE: fora da lista travada - venda total",
                    "qty_sell": qty,
                }
            )
            continue

        if carga > max_cap:
            target_w = _safe_float(target_weights.get(tk, target_w_default), target_w_default)
            qty_target = math.floor(total_ativo * target_w / px) if px > 0 else 0
            qty_sell = max(0, qty - qty_target)
            if qty_sell > 0:
                out.append(
                    {
                        "ticker": tk,
                        "sell_pct": round((qty_sell / qty) * 100.0, 1),
                        "close_d1": px,
                        "reason": (
                            f"REBALANCE: aparo - carga {carga * 100:.1f}% > {max_cap * 100:.0f}% "
                            f"(aparar ate {target_w * 100:.0f}%)"
                        ),
                        "qty_sell": qty_sell,
                    }
                )

    return sorted(out, key=lambda x: -_safe_float(x.get("sell_pct", 0.0), 0.0))


def _make_positions_snapshot(lots: list[Lot]) -> list[dict[str, Any]]:
    out = []
    for lot in lots:
        if lot.qtd <= 0:
            continue
        out.append(
            {
                "ticker": lot.ticker,
                "data_compra": lot.buy_date,
                "qtd": lot.qtd,
                "preco_compra": lot.buy_price,
            }
        )
    return out


def _build_tables_and_cards(exec_day: date) -> tuple[str, dict[str, Any], list[str]]:
    d1 = get_d_minus_1(exec_day)
    cutoff_day = exec_day - timedelta(days=1)
    d1_real_day, _ = load_latest_real_before(cutoff_day)

    lots, warnings = build_lot_ledger(cutoff_day)
    lots, corporate_actions = _detect_and_adjust_splits(lots, as_of_day=d1)
    tickers = sorted({x.ticker for x in lots})
    prices_d1 = get_latest_prices(tickers, as_of_day=d1)

    total_buy = sum(l.buy_value for l in lots)
    total_current = sum(l.qtd * _safe_float(prices_d1.get(l.ticker, l.buy_price), l.buy_price) for l in lots)

    rows_bought = []
    rows_current = []
    holdings_qty: dict[str, int] = {}
    for lot in lots:
        curr_px = _safe_float(prices_d1.get(lot.ticker, lot.buy_price), lot.buy_price)
        curr_val = lot.qtd * curr_px
        buy_val = lot.buy_value
        w_buy = (buy_val / total_buy * 100.0) if total_buy > 0 else 0.0
        w_cur = (curr_val / total_current * 100.0) if total_current > 0 else 0.0
        ret_log = (math.log(curr_val / buy_val) * 100.0) if buy_val > 0 and curr_val > 0 else 0.0
        holdings_qty[lot.ticker] = holdings_qty.get(lot.ticker, 0) + lot.qtd
        rows_bought.append(
            "<tr>"
            f"<td>{lot.ticker}</td><td>{_fmt_date_br(lot.buy_date)}</td><td style='text-align:right'>{_fmt_int(lot.qtd)}</td>"
            f"<td style='text-align:right'>{_fmt_money(lot.buy_price)}</td>"
            f"<td style='text-align:right'>{_fmt_money(buy_val)}</td>"
            f"<td style='text-align:right'>{_fmt_pct(w_buy)}</td>"
            "</tr>"
        )
        rows_current.append(
            "<tr>"
            f"<td>{lot.ticker}</td><td>{_fmt_date_br(lot.buy_date)}</td><td style='text-align:right'>{_fmt_int(lot.qtd)}</td>"
            f"<td style='text-align:right'>{_fmt_money(curr_px)}</td>"
            f"<td style='text-align:right'>{_fmt_money(curr_val)}</td>"
            f"<td style='text-align:right'>{_fmt_pct(w_cur)}</td>"
            f"<td style='text-align:right'>{_fmt_pct(ret_log)}</td>"
            "</tr>"
        )

    cash_prev = compute_cash(cutoff_day)
    cash_free_calc = _safe_float(cash_prev.get("cash_free", 0.0), 0.0)
    cash_acc_calc = _safe_float(cash_prev.get("cash_accounting", 0.0), 0.0)
    total_ativo_calc = total_current + cash_free_calc + cash_acc_calc

    total_bought_row = (
        "<tr class='total-row'>"
        "<td class='total-title' colspan='4'><strong>Total Geral</strong></td>"
        f"<td style='text-align:right'><strong>{_fmt_money(total_buy)}</strong></td>"
        "<td style='text-align:right'><strong>100.00%</strong></td>"
        "</tr>"
    )
    total_current_row = (
        "<tr class='total-row'>"
        "<td class='total-title' colspan='4'><strong>Total Geral</strong></td>"
        f"<td style='text-align:right'><strong>{_fmt_money(total_current)}</strong></td>"
        "<td style='text-align:right'><strong>100.00%</strong></td>"
        "<td style='text-align:right'>-</td>"
        "</tr>"
    )
    tables_html = f"""
    <div class="twocol">
      <div>
        <h3>Carteira Comprada</h3>
        <table>
          <colgroup><col style="width:14%"><col style="width:16%"><col style="width:12%"><col style="width:18%"><col style="width:22%"><col style="width:12%"></colgroup>
          <tr><th>Ticker</th><th>Data da Compra</th><th>Qtd</th><th>Preco Compra</th><th>Valor Compra</th><th>Peso %</th></tr>
          {''.join(rows_bought) if rows_bought else '<tr><td colspan="6">Sem posicoes</td></tr>'}
          {total_bought_row}
        </table>
      </div>
      <div>
        <h3>Carteira Atual (D-1)</h3>
        <table>
          <colgroup><col style="width:12%"><col style="width:14%"><col style="width:10%"><col style="width:14%"><col style="width:18%"><col style="width:10%"><col style="width:16%"></colgroup>
          <tr><th>Ticker</th><th>Data Compra</th><th>Qtd</th><th>Preco D-1</th><th>Valor Atual</th><th>Peso %</th><th>Retorno Log %</th></tr>
          {''.join(rows_current) if rows_current else '<tr><td colspan="7">Sem posicoes</td></tr>'}
          {total_current_row}
        </table>
      </div>
    </div>
    """

    aporte_acc = 0.0
    retirada_acc = 0.0
    for ev in read_all_events():
        if ev.exec_date > cutoff_day:
            continue
        if ev.type in {EventType.APORTE, EventType.DIVIDENDO}:
            aporte_acc += _safe_float(ev.amount, 0.0)
        elif ev.type == EventType.RETIRADA:
            retirada_acc += _safe_float(ev.amount, 0.0)

    report_ctx = {
        "d1": d1.isoformat(),
        "d1_br": _fmt_date_br(d1),
        "d1_real_day": d1_real_day.isoformat() if d1_real_day else "",
        "cash_free_prev": cash_free_calc,
        "cash_accounting_prev": cash_acc_calc,
        "cash_free": cash_free_calc,
        "cash_acc": cash_acc_calc,
        "holdings_qty": holdings_qty,
        "prices_d1": prices_d1,
        "lots_snapshot": _make_positions_snapshot(lots),
        "carteira_valor_d1": total_current,
        "total_ativo": total_ativo_calc,
        "pending_sales": _pending_sales_for_transfer(exec_day),
        "sells_in_settlement": _sells_in_settlement_for_display(exec_day),
        "aporte_acumulado": aporte_acc,
        "retirada_acumulada": retirada_acc,
        "corporate_actions": corporate_actions,
    }
    return tables_html, report_ctx, warnings


def build_painel(exec_day: date) -> Path:
    decision = _read_json(ROOT / "data" / "daily" / f"decision_{exec_day.isoformat()}.json")
    report_html, ctx, warnings = _build_tables_and_cards(exec_day)
    d1 = get_d_minus_1(exec_day)
    trade_day = _resolve_trade_day(exec_day)
    decision_date = d1.isoformat()

    portfolio_active = decision.get("portfolio", [])
    top20_info = decision.get("top20_by_score", [])
    use_top20_info = bool(top20_info)
    source_rows = top20_info if use_top20_info else portfolio_active
    top_tickers = [str(x.get("ticker", "")).upper().strip() for x in source_rows if str(x.get("ticker", "")).strip()]
    prices_top = get_latest_prices(top_tickers, as_of_day=d1)
    score_map = _load_score_map(d1)

    rows_info_top = []
    for p in source_rows[:20]:
        t = str(p.get("ticker", "")).upper().strip()
        if use_top20_info:
            score = _safe_float(p.get("score_m3"), 0.0)
        else:
            score = _safe_float(score_map.get(t, 0.0), 0.0)
        rows_info_top.append(
            "<tr>"
            f"<td>{t}</td>"
            f"<td style='text-align:right'>{score:.4f}</td>"
            f"<td style='text-align:right'>{_fmt_money(_safe_float(prices_top.get(t, 0.0), 0.0))}</td>"
            "</tr>"
        )
    if not rows_info_top:
        rows_info_top.append("<tr><td colspan='3'>Top-20 indisponivel (sem decisao).</td></tr>")

    prices_all = {**ctx["prices_d1"], **prices_top}
    sell_suggestions = _build_sell_suggestions(
        holdings_qty=ctx["holdings_qty"],
        prices_d1=prices_all,
        as_of_day=d1,
    )
    total_ativo = _safe_float(ctx.get("total_ativo", 0.0), 0.0)
    if total_ativo <= 0:
        total_ativo = (
            sum(
                _safe_int(qty, 0) * _safe_float(prices_all.get(str(tk).upper().strip(), 0.0), 0.0)
                for tk, qty in ctx["holdings_qty"].items()
            )
            + _safe_float(ctx.get("cash_free", ctx.get("cash_free_prev", 0.0)), 0.0)
            + _safe_float(ctx.get("cash_acc", ctx.get("cash_accounting_prev", 0.0)), 0.0)
        )
    rebalance_sell_suggestions = _build_rebalance_sell_suggestions(
        decision=decision,
        holdings_qty=ctx["holdings_qty"],
        prices_d1=prices_all,
        total_ativo=total_ativo,
    )
    rows_sell = []
    for s in sell_suggestions:
        rows_sell.append(
            "<tr>"
            f"<td>{s['ticker']}</td>"
            f"<td style='text-align:right'>{_fmt_pct(_safe_float(s['sell_pct'], 0.0))}</td>"
            f"<td style='text-align:right'>{_fmt_money(_safe_float(s['close_d1'], 0.0))}</td>"
            f"<td>{s['reason']}</td>"
            "</tr>"
        )
    if not rows_sell:
        rows_sell.append("<tr><td colspan='4'>Nenhuma venda sugerida para D-1.</td></tr>")

    rows_sell_rebalance = []
    for s in rebalance_sell_suggestions:
        rows_sell_rebalance.append(
            "<tr>"
            f"<td>{s['ticker']}</td>"
            f"<td style='text-align:right'>{_fmt_pct(_safe_float(s.get('sell_pct', 0.0), 0.0))}</td>"
            f"<td style='text-align:right'>{_fmt_int(_safe_int(s.get('qty_sell', 0), 0))}</td>"
            f"<td style='text-align:right'>{_fmt_money(_safe_float(s.get('close_d1', 0.0), 0.0))}</td>"
            f"<td>{s.get('reason', '')}</td>"
            "</tr>"
        )

    action_rows: list[dict[str, Any]] = []
    action_tickers = [
        str(x.get("ticker", "")).upper().strip() for x in portfolio_active if str(x.get("ticker", "")).strip()
    ]
    action_prices = get_latest_prices(action_tickers, as_of_day=d1)
    for b in action_tickers[:20]:
        action_rows.append({"type": "COMPRA", "ticker": b, "qtd": 0, "preco": _safe_float(action_prices.get(b, 0.0), 0.0)})

    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{w}</li>" for w in warnings)
        warnings_html = f"<div class='warnings'><strong>Avisos de consistencia:</strong><ul>{items}</ul></div>"

    split_alert_html = ""
    corporate_actions = ctx.get("corporate_actions", [])
    if corporate_actions:
        items = []
        for ca in corporate_actions:
            adj = ca.get("adjustment_applied", {})
            items.append(
                f"<li><strong>{ca['ticker']}</strong> — split {ca.get('ratio', '?')} detectado em "
                f"{ca.get('detection_date', '?')}. Posicao ajustada: "
                f"{adj.get('qtd_before', '?')} → {adj.get('qtd_after', '?')} cotas, "
                f"preco ${adj.get('preco_compra_before', 0):.2f} → ${adj.get('preco_compra_after', 0):.4f}. "
                f"Custo total invariante.</li>"
            )
        split_alert_html = (
            "<div class='split-alert'>"
            "<strong>CORPORATE ACTION — Split detectado no SSOT</strong>"
            f"<ul>{''.join(items)}</ul>"
            "<p style='margin:6px 0 0;font-size:12px;'>O snapshot de posicoes foi ajustado automaticamente. "
            "Confira o extrato da corretora e salve o boletim para registrar o ajuste.</p>"
            "</div>"
        )

    curve = _load_curve_until(d1)
    chart_252_html, motor_status_html = _build_chart_252(curve=curve, as_of_day=d1, decision=decision)
    chart_base1_html = _build_chart_base1(as_of_day=d1)

    cycle_dir = ROOT / "data" / "cycles" / d1.isoformat()
    cycle_dir.mkdir(parents=True, exist_ok=True)
    out_cycle = cycle_dir / "painel.html"
    out_daily = ROOT / "data" / "daily" / f"painel_{d1.isoformat()}.html"
    out_daily.parent.mkdir(parents=True, exist_ok=True)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>Painel Diario - Mercado: {_fmt_date_br(d1)} | Execucao: {_fmt_date_br(exec_day)}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
body {{ font-family: Segoe UI, Tahoma, sans-serif; background:#f5f7fb; color:#1f2937; margin:0; }}
.wrap {{ max-width: 1600px; margin: 0 auto; padding: 16px; }}
h1 {{ margin:0; font-size:24px; color:#0f172a; }}
.sub {{ color:#475569; margin-top:4px; margin-bottom:14px; }}
.block {{ background:white; border:1px solid #dbe2ea; border-radius:10px; padding:14px; margin-bottom:14px; }}
.twocol {{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; }}
.chart-grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; margin-top:14px; }}
.chart-wrap {{ border:1px solid #dbe2ea; border-radius:8px; padding:8px; background:#fff; min-height:455px; }}
.motor-status-wrap {{ border:1px solid #0f172a; border-radius:10px; background:#0f172a; padding:12px 14px; margin-top:14px; }}
.motor-status-title {{ color:#f8fafc; font-size:16px; font-weight:700; margin-bottom:10px; }}
.motor-status-grid {{ display:grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap:10px 12px; }}
.motor-item {{ background:#1e293b; border:1px solid #334155; border-radius:8px; padding:8px 10px; }}
.motor-label {{ color:#94a3b8; font-size:12px; margin-bottom:4px; }}
.motor-value {{ color:#e2e8f0; font-size:16px; font-weight:700; line-height:1.2; }}
.motor-value.ok {{ color:#22c55e; }}
.motor-value.bad {{ color:#ef4444; }}
.motor-value.warn {{ color:#f59e0b; }}
.chart-empty {{ color:#64748b; font-size:13px; padding:10px; }}
.info-grid {{ display:grid; grid-template-columns: 0.40fr 0.60fr; gap:14px; }}
table {{ width:100%; border-collapse: collapse; font-size:13px; table-layout:fixed; }}
th {{ background:#0f172a; color:white; padding:7px; text-align:left; }}
td {{ border-bottom:1px solid #e5e7eb; padding:6px 7px; }}
.total-row td {{ background:#f8fafc; border-top:2px solid #cbd5e1; }}
.total-row .total-title {{ white-space:nowrap; font-weight:700; }}
.section-title {{ font-size:18px; margin-bottom:10px; color:#0f172a; }}
.muted {{ color:#64748b; font-size:12px; }}
.btn {{ background:#0f4c81; color:white; border:none; border-radius:8px; padding:10px 14px; cursor:pointer; font-weight:600; }}
.btn-add {{ background:#334155; }}
input, select {{ width:100%; padding:6px; border:1px solid #cbd5e1; border-radius:6px; font-size:13px; }}
.ops-head, .op-grid {{ display:grid; grid-template-columns: 120px 160px 120px 140px 140px 40px; gap:8px; align-items:center; }}
.ops-head {{ font-size:12px; font-weight:700; color:#334155; margin-bottom:6px; }}
.cash-grid {{ display:grid; grid-template-columns: 140px 120px 1fr 40px; gap:8px; margin-bottom:8px; align-items:center; }}
.save-msg {{ margin-left:8px; font-size:13px; }}
.save-msg.error {{ color:#b91c1c; font-weight:600; }}
.save-msg.ok {{ color:#166534; }}
.warnings {{ background:#fff7ed; border:1px solid #fed7aa; color:#7c2d12; border-radius:8px; padding:10px; margin:10px 0; }}
.split-alert {{ background:#fef2f2; border:2px solid #f87171; color:#991b1b; border-radius:10px; padding:14px; margin:10px 0; font-size:14px; }}
.split-alert strong {{ font-size:15px; }}
.split-alert ul {{ margin:6px 0 0 16px; padding:0; }}
.top10-table td, .top10-table th {{ font-size:12px; padding:5px 6px; }}
.cash-layout {{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; margin-top:14px; }}
.cash-panel {{ border:1px solid #dbe2ea; border-radius:8px; padding:10px; background:#fafcff; }}
.cash-panel h4 {{ margin:0 0 10px 0; color:#0f172a; }}
.cash-row {{ display:flex; justify-content:space-between; gap:10px; padding:4px 0; border-bottom:1px dashed #e5e7eb; font-size:13px; }}
.cash-row:last-child {{ border-bottom:none; }}
.cash-row strong {{ color:#0f172a; }}
.cash-real {{ margin-top:10px; }}
@media (max-width: 1200px) {{
  .twocol, .chart-grid, .info-grid, .cash-layout {{ grid-template-columns: 1fr; }}
  .motor-status-grid {{ grid-template-columns: repeat(2, minmax(160px, 1fr)); }}
}}
@media print {{
  @page {{ size: A3 landscape; margin: 8mm; }}
  body {{ background:#fff; }}
  .wrap {{ max-width:none; padding:0; }}
}}
</style>
</head>
<body>
  <div class="wrap">
    <h1>Painel Diario - Mercado: {_fmt_date_br(d1)} | Execucao: {_fmt_date_br(exec_day)}</h1>
    <div class="sub">Documento unico: Relatorio + Boletim | D-1 de mercado: {ctx["d1_br"]}</div>

    {split_alert_html}

    <div class="block">
      <div class="section-title">Sessao Relatorio</div>
      {warnings_html}
      {report_html}
      <div class="chart-grid">
        <div class="chart-wrap">{chart_252_html}</div>
        <div class="chart-wrap">{chart_base1_html}</div>
      </div>
      {motor_status_html}
    </div>

    <div class="block">
      <div class="section-title">Sessao Boletim - Informacao</div>
      <div class="info-grid">
        <div>
          <h3>Top-20 para compra (D-1)</h3>
          <table class="top10-table">
            <tr><th>Ticker</th><th>M3</th><th>Fechamento D-1</th></tr>
            {''.join(rows_info_top)}
          </table>
        </div>
        <div>
          <h3>Card de Venda (sugestao tecnica)</h3>
          <table>
            <tr><th>Ticker</th><th>% Venda</th><th>Fechamento D-1</th><th>Razao tecnica</th></tr>
            {''.join(rows_sell)}
          </table>
          {(
            "<h3>Vendas de Rebalance D0 (ajuste a lista travada)</h3>"
            "<table>"
            "<tr><th>Ticker</th><th>% Venda</th><th>Qtd</th><th>Fechamento D-1</th><th>Razao</th></tr>"
            + "".join(rows_sell_rebalance)
            + "</table>"
          ) if rebalance_sell_suggestions else ""}
        </div>
      </div>
    </div>

    <div class="block">
      <div class="section-title">Sessao Boletim - Acao do Owner</div>
      <p class="muted" style="margin-bottom:10px;">Informe as operacoes do dia, movimentacoes extraordinarias e transferencias Contabil -> Livre.</p>

      <h3>Operacoes do dia</h3>
      <div class="ops-head">
        <div>Tipo</div>
        <div>Ticker</div>
        <div>Quantidade</div>
        <div>Preco</div>
        <div>Valor</div>
        <div></div>
      </div>
      <div id="opsRows"></div>
      <button class="btn btn-add" onclick="addOp()">+ Adicionar operacao</button>

      <h3 style="margin-top:14px;">Movimentacoes extraordinarias de caixa</h3>
      <div id="cashRows"></div>
      <button class="btn btn-add" onclick="addCash()">+ Adicionar movimento</button>

      <h3 style="margin-top:14px;">Transferencias Contabil -> Livre</h3>
      <p class="muted" style="font-size:13px;">Vendas realizadas em dias anteriores cujo valor ainda nao foi transferido para Caixa Livre.</p>
      <div id="pendingSalesTable">
        <table style="font-size:13px;width:100%;">
          <tr style="background:#f1f5f9;"><th style="width:5%;"></th><th>Data Venda</th><th>Ticker</th><th style="text-align:right">Qtd</th><th style="text-align:right">Preco</th><th style="text-align:right">Valor Venda</th><th style="text-align:right">Pendente</th></tr>
          <tbody id="pendingSalesBody"></tbody>
        </table>
      </div>
      <div id="transferRows" style="margin-top:8px;"></div>
      <button class="btn btn-add" onclick="addTransfer()">+ Adicionar transferencia manual</button>

      <h3 style="margin-top:14px;">Vendas em Liquidacao (informativo)</h3>
      <p class="muted" style="font-size:13px;">Vendas executadas aguardando liquidacao T+1. Nao sao transferiveis ainda. Compoem o Caixa Contabil da sessao abaixo.</p>
      <div id="inSettlementTable">
        <table style="font-size:13px;width:100%;">
          <tr style="background:#fef9c3;"><th>Data Venda</th><th>Ticker</th><th style="text-align:right">Qtd</th><th style="text-align:right">Preco</th><th style="text-align:right">Valor Venda</th><th style="text-align:right">Liquida em</th></tr>
          <tbody id="inSettlementBody"></tbody>
        </table>
      </div>
      <div class="cash-row" style="margin-top:6px;font-size:13px;border:1px solid #e2e8f0;border-radius:6px;padding:6px 10px;background:#f8fafc;">
        <span>Pronto p/ transferir + Em liquidacao</span>
        <span><strong id="reconcile_acc">-</strong><span id="reconcile_ok" style="font-size:11px;margin-left:8px;"></span></span>
      </div>

      <div class="section-title" style="margin-top:14px;">Sessao Caixa</div>
      <div class="cash-layout">
        <div class="cash-panel">
          <h4>Balanco Simplificado (D)</h4>
          <div class="cash-row"><span>Carteira de Acoes (valor D-1)</span><strong id="bal_carteira">-</strong></div>
          <div class="cash-row"><span>Caixa Livre</span><strong id="bal_caixa_livre">-</strong></div>
          <div class="cash-row"><span>Caixa Contabil</span><strong id="bal_caixa_contabil">-</strong></div>
          <div class="cash-row"><span><strong>Total do Ativo</strong></span><strong id="bal_total_ativo">-</strong></div>
          <div class="cash-row"><span>Aportes acumulados</span><strong id="bal_aporte_acc">-</strong></div>
          <div class="cash-row"><span>Retiradas acumuladas</span><strong id="bal_retirada_acc">-</strong></div>
          <div class="cash-row"><span><strong>Capital Liquido Aportado</strong></span><strong id="bal_patrimonio_inicial">-</strong></div>
          <div class="cash-row"><span><strong>Resultado acumulado</strong></span><strong id="bal_resultado_acc">-</strong></div>
          <div class="cash-row"><span><strong>Rentabilidade acumulada</strong></span><strong id="bal_rent_acc">-</strong></div>
        </div>
        <div class="cash-panel">
          <h4>DFC Simplificado (D)</h4>
          <div class="cash-row"><span>Caixa Livre anterior (D-1)</span><strong id="dfc_free_open">-</strong></div>
          <div class="cash-row"><span>(+) Transferencias Contabil -> Livre</span><strong id="dfc_transfer">-</strong></div>
          <div class="cash-row"><span>(+) Aportes</span><strong id="dfc_aporte">-</strong></div>
          <div class="cash-row"><span>(-) Retiradas</span><strong id="dfc_retirada">-</strong></div>
          <div class="cash-row"><span>(-) Compras do dia</span><strong id="dfc_buy">-</strong></div>
          <div class="cash-row"><span><strong>Saldo Final Caixa Livre (D)</strong></span><strong id="dfc_free_close">-</strong></div>
          <div class="cash-row"><span>Caixa Contabil anterior (D-1)</span><strong id="dfc_acc_open">-</strong></div>
          <div class="cash-row"><span>(+) Vendas do dia</span><strong id="dfc_sell">-</strong></div>
          <div class="cash-row"><span>(-) Transferencias -> Livre</span><strong id="dfc_acc_transfer">-</strong></div>
          <div class="cash-row"><span><strong>Saldo Final Caixa Contabil (D)</strong></span><strong id="dfc_acc_close">-</strong></div>
          <div class="cash-real">
            <label for="cash_real_input" class="muted">Caixa Liquido Real (informado pelo Owner)</label>
            <input id="cash_real_input" type="number" step="0.01" min="0" placeholder="Ex.: 179099.69" />
          </div>
        </div>
      </div>

      <div style="margin-top:14px;">
        <button id="btnSave" class="btn" onclick="savePanel()">Salvar Boletim (JSON)</button>
        <span id="saveMsg" class="save-msg"></span>
      </div>
    </div>
  </div>

<script>
const EXEC_DATE = "{exec_day.isoformat()}";
const MARKET_DAY = "{d1.isoformat()}";
const TRADE_DAY = "{trade_day.isoformat()}";
const DECISION_DATE = "{decision_date}";
const PREV_FREE = {ctx["cash_free_prev"]};
const PREV_ACC = {ctx["cash_accounting_prev"]};
const CARTEIRA_D1 = {ctx["carteira_valor_d1"]};
const APORTE_ACC = {ctx["aporte_acumulado"]};
const RETIRADA_ACC = {ctx["retirada_acumulada"]};
const ACTION_ROWS = {json.dumps(action_rows, ensure_ascii=False)};
const SNAPSHOT_D1 = {json.dumps(ctx["lots_snapshot"], ensure_ascii=False)};
const PENDING_SALES = {json.dumps(ctx["pending_sales"], ensure_ascii=False)};
const IN_SETTLEMENT = {json.dumps(ctx["sells_in_settlement"], ensure_ascii=False)};
const CORPORATE_ACTIONS = {json.dumps(corporate_actions, ensure_ascii=False)};

let opIdx = 0;
let cashIdx = 0;
let trIdx = 0;

function moneyUSD(v) {{
  return '$ ' + Number(v || 0).toLocaleString('en-US', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
}}
function pctUS(v) {{
  return Number(v || 0).toLocaleString('en-US', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }}) + '%';
}}
function renderPendingSales() {{
  const tbody = document.getElementById('pendingSalesBody');
  if (!tbody) return;
  tbody.innerHTML = '';
  if (PENDING_SALES.length === 0) {{
    tbody.innerHTML = '<tr><td colspan="7" style="color:#64748b;padding:8px;">Nenhuma venda pendente de transferencia.</td></tr>';
    return;
  }}
  PENDING_SALES.forEach((s, i) => {{
    const tr = document.createElement('tr');
    const dateParts = s.sale_date.split('-');
    const dateBR = dateParts[2] + '/' + dateParts[1] + '/' + dateParts[0];
    tr.innerHTML = `
      <td style="text-align:center"><input type="checkbox" id="ps_chk_${{i}}" onchange="recalc()" /></td>
      <td>${{dateBR}}</td>
      <td>${{s.ticker}}</td>
      <td style="text-align:right">${{Number(s.qtd).toLocaleString('en-US')}}</td>
      <td style="text-align:right">${{moneyUSD(s.preco)}}</td>
      <td style="text-align:right">${{moneyUSD(s.valor_venda)}}</td>
      <td style="text-align:right">${{moneyUSD(s.pendente)}}</td>
    `;
    tbody.appendChild(tr);
  }});
}}
function renderInSettlement() {{
  const tbody = document.getElementById('inSettlementBody');
  if (!tbody) return;
  tbody.innerHTML = '';
  if (!IN_SETTLEMENT || IN_SETTLEMENT.length === 0) {{
    tbody.innerHTML = '<tr><td colspan="6" style="color:#64748b;padding:8px;">Nenhuma venda em liquidacao.</td></tr>';
  }} else {{
    IN_SETTLEMENT.forEach(s => {{
      const tr = document.createElement('tr');
      const dp = s.sale_date.split('-');
      const dateBR = dp[2] + '/' + dp[1] + '/' + dp[0];
      const sp = (s.settle_date || '').split('-');
      const sdBR = sp.length === 3 ? sp[2] + '/' + sp[1] + '/' + sp[0] : (s.settle_date || '');
      tr.innerHTML = `
        <td>${{dateBR}}</td>
        <td>${{s.ticker}}</td>
        <td style="text-align:right">${{Number(s.qtd).toLocaleString('en-US')}}</td>
        <td style="text-align:right">${{moneyUSD(s.preco)}}</td>
        <td style="text-align:right">${{moneyUSD(s.valor_venda)}}</td>
        <td style="text-align:right">${{sdBR}}</td>
      `;
      tbody.appendChild(tr);
    }});
  }}
  const readyTotal = (PENDING_SALES || []).reduce((a, b) => a + (b.pendente || 0), 0);
  const inSettTotal = (IN_SETTLEMENT || []).reduce((a, b) => a + (b.pendente || 0), 0);
  const reconTotal = readyTotal + inSettTotal;
  const el = document.getElementById('reconcile_acc');
  if (el) el.textContent = moneyUSD(reconTotal);
  const diff = Math.abs(reconTotal - PREV_ACC);
  const okEl = document.getElementById('reconcile_ok');
  if (okEl) {{
    okEl.textContent = diff < 0.02 ? 'OK (reconcilia)' : 'ATENCAO: diff $' + diff.toFixed(2);
    okEl.style.color = diff < 0.02 ? '#166534' : '#b91c1c';
  }}
}}
function addOp(pref = null) {{
  const box = document.getElementById('opsRows');
  const i = opIdx++;
  const typ = pref?.type || 'COMPRA';
  const tk = pref?.ticker || '';
  const qtd = pref?.qtd || 0;
  const px = pref?.preco || 0;
  const row = document.createElement('div');
  row.className = 'op-grid';
  row.id = `op_row_${{i}}`;
  row.innerHTML = `
    <select id="op_type_${{i}}" onchange="recalc()">
      <option value="COMPRA" ${{typ==='COMPRA'?'selected':''}}>COMPRA</option>
      <option value="VENDA" ${{typ==='VENDA'?'selected':''}}>VENDA</option>
    </select>
    <input id="op_tk_${{i}}" value="${{tk}}" placeholder="Ticker" />
    <input id="op_qtd_${{i}}" type="number" min="0" value="${{qtd}}" onchange="recalc()" />
    <input id="op_px_${{i}}" type="number" min="0" step="0.01" value="${{px}}" onchange="recalc()" />
    <input id="op_val_${{i}}" type="text" value="$ 0.00" readonly />
    <button onclick="removeRow('op_row_${{i}}');recalc()">x</button>
  `;
  box.appendChild(row);
  recalc();
}}
function addCash(pref = null) {{
  const box = document.getElementById('cashRows');
  const i = cashIdx++;
  const typ = pref?.type || 'APORTE';
  const val = pref?.value || 0;
  const desc = pref?.description || '';
  const row = document.createElement('div');
  row.className = 'cash-grid';
  row.id = `cash_row_${{i}}`;
  row.innerHTML = `
    <select id="cash_type_${{i}}" onchange="recalc()">
      <option value="APORTE" ${{typ==='APORTE'?'selected':''}}>APORTE</option>
      <option value="DIVIDENDO" ${{typ==='DIVIDENDO'?'selected':''}}>DIVIDENDO</option>
      <option value="JCP" ${{typ==='JCP'?'selected':''}}>JCP</option>
      <option value="BONIFICACAO" ${{typ==='BONIFICACAO'?'selected':''}}>BONIFICACAO</option>
      <option value="BONUS" ${{typ==='BONUS'?'selected':''}}>BONUS</option>
      <option value="SUBSCRICAO" ${{typ==='SUBSCRICAO'?'selected':''}}>SUBSCRICAO</option>
      <option value="RETIRADA" ${{typ==='RETIRADA'?'selected':''}}>RETIRADA</option>
    </select>
    <input id="cash_val_${{i}}" type="number" min="0" step="0.01" value="${{val}}" onchange="recalc()" />
    <input id="cash_desc_${{i}}" value="${{desc}}" placeholder="Descricao" />
    <button onclick="removeRow('cash_row_${{i}}');recalc()">x</button>
  `;
  box.appendChild(row);
  recalc();
}}
function addTransfer(pref = null) {{
  const box = document.getElementById('transferRows');
  const i = trIdx++;
  const val = pref?.value || 0;
  const note = pref?.note || '';
  const row = document.createElement('div');
  row.className = 'cash-grid';
  row.id = `tr_row_${{i}}`;
  row.innerHTML = `
    <input value="TRANSFERENCIA" disabled />
    <input id="tr_val_${{i}}" type="number" min="0" step="0.01" value="${{val}}" onchange="recalc()" />
    <input id="tr_note_${{i}}" value="${{note}}" placeholder="Referencia da liquidacao" />
    <button onclick="removeRow('tr_row_${{i}}');recalc()">x</button>
  `;
  box.appendChild(row);
  recalc();
}}
function removeRow(id) {{
  const el = document.getElementById(id);
  if (el) el.remove();
}}
function collectOps() {{
  const out = [];
  for (let i = 0; i < opIdx; i++) {{
    if (!document.getElementById(`op_row_${{i}}`)) continue;
    const type = document.getElementById(`op_type_${{i}}`).value;
    const ticker = (document.getElementById(`op_tk_${{i}}`).value || '').trim().toUpperCase();
    const qtd = parseInt(document.getElementById(`op_qtd_${{i}}`).value || '0');
    const preco = parseFloat(document.getElementById(`op_px_${{i}}`).value || '0');
    if (!ticker || qtd <= 0 || preco <= 0) continue;
    out.push({{ type, ticker, qtd, preco }});
  }}
  return out;
}}
function collectCashMovs() {{
  const out = [];
  for (let i = 0; i < cashIdx; i++) {{
    if (!document.getElementById(`cash_row_${{i}}`)) continue;
    const type = document.getElementById(`cash_type_${{i}}`).value;
    const value = parseFloat(document.getElementById(`cash_val_${{i}}`).value || '0');
    const description = (document.getElementById(`cash_desc_${{i}}`).value || '').trim();
    if (value <= 0) continue;
    out.push({{ type, value, description }});
  }}
  return out;
}}
function collectTransfers() {{
  const out = [];
  PENDING_SALES.forEach((s, i) => {{
    const chk = document.getElementById(`ps_chk_${{i}}`);
    if (chk && chk.checked) {{
      out.push({{ value: s.pendente, note: s.ref }});
    }}
  }});
  for (let i = 0; i < trIdx; i++) {{
    if (!document.getElementById(`tr_row_${{i}}`)) continue;
    const value = parseFloat(document.getElementById(`tr_val_${{i}}`).value || '0');
    const note = (document.getElementById(`tr_note_${{i}}`).value || '').trim();
    if (value <= 0) continue;
    out.push({{ value, note }});
  }}
  return out;
}}
function recalc() {{
  const ops = collectOps();
  for (let i = 0; i < opIdx; i++) {{
    if (!document.getElementById(`op_row_${{i}}`)) continue;
    const qtd = parseInt(document.getElementById(`op_qtd_${{i}}`).value || '0');
    const preco = parseFloat(document.getElementById(`op_px_${{i}}`).value || '0');
    const el = document.getElementById(`op_val_${{i}}`);
    if (el) el.value = moneyUSD(qtd * preco);
  }}
  const cashMovs = collectCashMovs();
  const transfers = collectTransfers();
  const buy = ops.filter(x => x.type === 'COMPRA').reduce((a,b) => a + b.qtd*b.preco, 0);
  const sell = ops.filter(x => x.type === 'VENDA').reduce((a,b) => a + b.qtd*b.preco, 0);
  const aporte = cashMovs.filter(x => ['APORTE','DIVIDENDO','JCP','BONIFICACAO','BONUS','SUBSCRICAO'].includes(x.type)).reduce((a,b) => a + b.value, 0);
  const retirada = cashMovs.filter(x => x.type === 'RETIRADA').reduce((a,b) => a + b.value, 0);
  const transfer = transfers.reduce((a,b) => a + b.value, 0);

  const free = PREV_FREE + transfer + aporte - retirada - buy;
  const acc = PREV_ACC + sell - transfer;
  const carteiraD = CARTEIRA_D1 + buy - sell;
  const totalAtivo = carteiraD + free + acc;
  const basePatrimonio = (APORTE_ACC + aporte) - (RETIRADA_ACC + retirada);
  const resultadoAcc = totalAtivo - basePatrimonio;
  const rentAcc = basePatrimonio > 0 ? (resultadoAcc / basePatrimonio) * 100.0 : 0.0;

  document.getElementById('dfc_free_open').textContent = moneyUSD(PREV_FREE);
  document.getElementById('dfc_transfer').textContent = moneyUSD(transfer);
  document.getElementById('dfc_aporte').textContent = moneyUSD(aporte);
  document.getElementById('dfc_retirada').textContent = moneyUSD(retirada);
  document.getElementById('dfc_buy').textContent = moneyUSD(buy);
  document.getElementById('dfc_free_close').textContent = moneyUSD(free);
  document.getElementById('dfc_acc_open').textContent = moneyUSD(PREV_ACC);
  document.getElementById('dfc_sell').textContent = moneyUSD(sell);
  document.getElementById('dfc_acc_transfer').textContent = moneyUSD(transfer);
  document.getElementById('dfc_acc_close').textContent = moneyUSD(acc);

  document.getElementById('bal_carteira').textContent = moneyUSD(carteiraD);
  document.getElementById('bal_caixa_livre').textContent = moneyUSD(free);
  document.getElementById('bal_caixa_contabil').textContent = moneyUSD(acc);
  document.getElementById('bal_total_ativo').textContent = moneyUSD(totalAtivo);
  document.getElementById('bal_aporte_acc').textContent = moneyUSD(APORTE_ACC + aporte);
  document.getElementById('bal_retirada_acc').textContent = moneyUSD(RETIRADA_ACC + retirada);
  document.getElementById('bal_patrimonio_inicial').textContent = moneyUSD(basePatrimonio);
  document.getElementById('bal_resultado_acc').textContent = moneyUSD(resultadoAcc);
  document.getElementById('bal_rent_acc').textContent = pctUS(rentAcc);

  const btn = document.getElementById('btnSave');
  const msg = document.getElementById('saveMsg');
  if (free < -0.00001) {{
    btn.disabled = true;
    btn.style.opacity = '0.6';
    btn.style.cursor = 'not-allowed';
    msg.className = 'save-msg error';
    msg.textContent = 'Compra invalida: Caixa Livre final ficaria negativo.';
  }} else {{
    btn.disabled = false;
    btn.style.opacity = '1';
    btn.style.cursor = 'pointer';
    if (msg.classList.contains('error')) {{
      msg.className = 'save-msg';
      msg.textContent = '';
    }}
  }}
}}
function buildSnapshotAfterOps(ops) {{
  const lots = JSON.parse(JSON.stringify(SNAPSHOT_D1 || []));
  const byTicker = {{}};
  lots.forEach(l => {{
    const t = l.ticker;
    if (!byTicker[t]) byTicker[t] = [];
    byTicker[t].push({{ ...l }});
  }});
  Object.values(byTicker).forEach(arr => arr.sort((a,b) => (a.data_compra || '').localeCompare(b.data_compra || '')));
  for (const op of ops) {{
    const t = op.ticker;
    if (op.type === 'COMPRA') {{
      if (!byTicker[t]) byTicker[t] = [];
      byTicker[t].push({{
        ticker: t,
        data_compra: EXEC_DATE,
        qtd: op.qtd,
        preco_compra: op.preco
      }});
      byTicker[t].sort((a,b) => (a.data_compra || '').localeCompare(b.data_compra || ''));
    }} else if (op.type === 'VENDA') {{
      let remain = op.qtd;
      const arr = byTicker[t] || [];
      for (const lot of arr) {{
        if (remain <= 0) break;
        const c = Math.min(remain, lot.qtd || 0);
        lot.qtd = (lot.qtd || 0) - c;
        remain -= c;
      }}
      byTicker[t] = arr.filter(l => (l.qtd || 0) > 0);
    }}
  }}
  const out = [];
  Object.keys(byTicker).sort().forEach(t => {{
    byTicker[t].forEach(l => {{
      if ((l.qtd || 0) > 0) out.push(l);
    }});
  }});
  return out;
}}
function savePanel() {{
  const ops = collectOps();
  const cashMovements = collectCashMovs();
  const cashTransfers = collectTransfers();
  const buy = ops.filter(x => x.type === 'COMPRA').reduce((a,b) => a + b.qtd*b.preco, 0);
  const sell = ops.filter(x => x.type === 'VENDA').reduce((a,b) => a + b.qtd*b.preco, 0);
  const aporte = cashMovements.filter(x => ['APORTE','DIVIDENDO','JCP','BONIFICACAO','BONUS','SUBSCRICAO'].includes(x.type)).reduce((a,b) => a + b.value, 0);
  const retirada = cashMovements.filter(x => x.type === 'RETIRADA').reduce((a,b) => a + b.value, 0);
  const transfer = cashTransfers.reduce((a,b) => a + b.value, 0);
  const cash_free = PREV_FREE + transfer + aporte - retirada - buy;
  const cash_accounting = PREV_ACC + sell - transfer;
  const caixaLiquidoRealRaw = (document.getElementById('cash_real_input').value || '').trim();
  const caixaLiquidoReal = caixaLiquidoRealRaw === '' ? null : parseFloat(caixaLiquidoRealRaw);
  if (cash_free < -0.00001) {{
    const msg = document.getElementById('saveMsg');
    msg.className = 'save-msg error';
    msg.textContent = 'Compra invalida: Caixa Livre final ficaria negativo.';
    return;
  }}
  const payload = {{
    date: EXEC_DATE,
    reference_decision: DECISION_DATE,
    exec_day: EXEC_DATE,
    market_day: MARKET_DAY,
    trade_day: TRADE_DAY,
    operations: ops,
    cash_movements: cashMovements,
    cash_transfers: cashTransfers,
    corporate_actions: CORPORATE_ACTIONS.length > 0 ? CORPORATE_ACTIONS : undefined,
    cash_free: cash_free,
    cash_accounting: cash_accounting,
    caixa_liquido_real: caixaLiquidoReal,
    positions_snapshot: buildSnapshotAfterOps(ops),
    cash_balance: cash_free,
    caixa_liquidando: cash_accounting
  }};
  fetch('/salvar', {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/json' }},
    body: JSON.stringify(payload, null, 2)
  }}).then(r => r.json()).then(data => {{
    const msg = document.getElementById('saveMsg');
    if (data.ok) {{
      msg.textContent = 'Salvo: ' + (data.paths || [data.path || 'data/real']).join(' | ');
      msg.className = 'save-msg ok';
    }} else {{
      msg.textContent = 'Erro: ' + (data.error || 'falha ao salvar');
      msg.className = 'save-msg error';
    }}
  }}).catch(err => {{
    const msg = document.getElementById('saveMsg');
    msg.textContent = 'Erro de conexao: ' + err;
    msg.className = 'save-msg error';
  }});
}}
renderPendingSales();
renderInSettlement();
for (const a of ACTION_ROWS) {{
  addOp(a);
}}
recalc();
if (window.location.protocol === 'file:') {{
  const msg = document.getElementById('saveMsg');
  msg.className = 'save-msg error';
  msg.textContent = 'Painel aberto via arquivo. Para salvar, use o lancador em http://127.0.0.1:8788';
  document.getElementById('btnSave').disabled = true;
  document.getElementById('btnSave').style.opacity = '0.6';
}}
</script>
</body></html>
"""
    out_cycle.write_text(html, encoding="utf-8")
    out_daily.write_text(html, encoding="utf-8")
    return out_daily


def run(target_date: date | None = None) -> str:
    if target_date is None:
        target_date = datetime.now(tz=UTC).date()
    path = build_painel(target_date)
    return str(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera painel diario USA_OPS no formato BR")
    parser.add_argument("--date", type=str, default=None, help="Data alvo (YYYY-MM-DD)")
    args = parser.parse_args()
    target = date.fromisoformat(args.date) if args.date else None
    out = run(target_date=target)
    print(out)


if __name__ == "__main__":
    main()
