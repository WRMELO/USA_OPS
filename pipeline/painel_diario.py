"""Painel diario US no formato BR (T-037 / D-027)."""
from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[1]
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
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return []

    all_transfers: list[dict[str, Any]] = []
    for p in sorted(real_dir.glob("*.json")):
        try:
            d = date.fromisoformat(p.stem)
        except Exception:
            continue
        if d >= exec_day:
            continue
        payload = _read_json(p)
        for tr in payload.get("cash_transfers", []):
            ref = tr.get("note", tr.get("ref", ""))
            val = _safe_float(tr.get("value", tr.get("valor", 0.0)), 0.0)
            all_transfers.append({"ref": str(ref), "value": val})

    pending: list[dict[str, Any]] = []
    for p in sorted(real_dir.glob("*.json")):
        try:
            d = date.fromisoformat(p.stem)
        except Exception:
            continue
        if d >= exec_day:
            continue
        payload = _read_json(p)
        for op in payload.get("operations", []):
            if str(op.get("type", "")).upper() != "VENDA":
                continue
            ticker = str(op.get("ticker", "")).upper().strip()
            qtd = _safe_int(op.get("qtd"), 0)
            preco = _safe_float(op.get("preco"), 0.0)
            valor = qtd * preco
            sale_ref = f"VENDA {ticker} {d.isoformat()}"
            already = sum(
                t["value"]
                for t in all_transfers
                if sale_ref.lower() in t["ref"].lower()
                or (ticker.lower() in t["ref"].lower() and d.isoformat() in t["ref"])
            )
            remain = valor - already
            if remain > 0.50:
                pending.append(
                    {
                        "sale_date": d.isoformat(),
                        "ticker": ticker,
                        "qtd": qtd,
                        "preco": preco,
                        "valor_venda": valor,
                        "ja_transferido": already,
                        "pendente": remain,
                        "ref": sale_ref,
                    }
                )
    return pending


def build_lot_ledger(until_day: date) -> tuple[list[Lot], list[str]]:
    files = list_real_files_upto(until_day)
    lots_by_ticker: dict[str, list[Lot]] = {}
    warnings: list[str] = []
    for p in files:
        day = date.fromisoformat(p.stem)
        payload = _read_json(p)
        ops = _extract_operations(payload)
        for op in ops:
            typ = op["type"]
            tk = op["ticker"]
            qtd = _safe_int(op["qtd"], 0)
            px = _safe_float(op["preco"], 0.0)
            if not tk or qtd <= 0 or px <= 0:
                continue
            if typ == "COMPRA":
                lots_by_ticker.setdefault(tk, []).append(Lot(ticker=tk, buy_date=day.isoformat(), qtd=qtd, buy_price=px))
            else:
                remain = qtd
                queue = lots_by_ticker.get(tk, [])
                i = 0
                while i < len(queue) and remain > 0:
                    lot = queue[i]
                    consume = min(lot.qtd, remain)
                    lot.qtd -= consume
                    remain -= consume
                    if lot.qtd == 0:
                        i += 1
                queue = [x for x in queue if x.qtd > 0]
                lots_by_ticker[tk] = queue
                if remain > 0:
                    warnings.append(
                        f"Venda excedente em {day.isoformat()} para {tk}: faltaram {remain} acoes para baixar."
                    )

    out: list[Lot] = []
    for t in sorted(lots_by_ticker.keys()):
        out.extend(lots_by_ticker[t])
    return out, warnings


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


def _build_chart_252(curve: pd.DataFrame, as_of_day: date) -> str:
    if curve.empty:
        return "<div class='chart-empty'>Curva de equity indisponível.</div>"
    last_252 = curve.tail(252).copy()
    if last_252.empty:
        return "<div class='chart-empty'>Curva de equity indisponível.</div>"

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.10,
        row_heights=[0.70, 0.30],
        subplot_titles=("Curva de Equity - Ultimos 252 Pregoes", "Drawdown (%)"),
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

    fig.add_vline(
        x=pd.Timestamp(PROJECT_START).timestamp() * 1000,
        line_dash="dash",
        line_color="purple",
        line_width=2,
        annotation_text="INICIO REAL 19/03/2026",
        annotation_position="top left",
        annotation_font_size=10,
        annotation_font_color="purple",
        row=1,
        col=1,
    )
    fig.update_layout(
        height=430,
        template="plotly_white",
        margin=dict(l=50, r=20, t=45, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="right", x=1),
        font_size=11,
    )
    fig.update_yaxes(title_text="Equity (USD)", row=1, col=1)
    fig.update_yaxes(title_text="Drawdown (%)", row=2, col=1)
    _ = as_of_day
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _build_real_base1_series(as_of_day: date) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])

    for p in sorted(real_dir.glob("*.json")):
        try:
            exec_day = date.fromisoformat(p.stem)
        except Exception:
            continue
        payload = _read_json(p)
        ref_raw = str(payload.get("reference_decision", "")).strip()
        try:
            ref_day = date.fromisoformat(ref_raw) if ref_raw else exec_day
        except Exception:
            ref_day = exec_day
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

    tickers: set[str] = set()
    for rec in ordered:
        for pos in rec["snapshot"]:
            tk = str(pos.get("ticker", "")).upper().strip()
            if tk:
                tickers.add(tk)

    prices = pd.DataFrame(columns=["date", "ticker", "close_raw"])
    win_path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if tickers and win_path.exists():
        prices = pd.read_parquet(win_path, columns=["date", "ticker", "close_raw"])
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
        prices["ticker"] = prices["ticker"].astype(str).str.upper().str.strip()
        prices["close_raw"] = pd.to_numeric(prices["close_raw"], errors="coerce")
        prices = prices.dropna(subset=["date", "ticker", "close_raw"])
        prices = prices[(prices["date"] <= pd.Timestamp(as_of_day)) & (prices["ticker"].isin(tickers))]
        prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    by_ticker: dict[str, pd.DataFrame] = {}
    if not prices.empty:
        for tk in prices["ticker"].unique():
            by_ticker[tk] = prices[prices["ticker"] == tk][["date", "close_raw"]].copy()

    rows: list[dict[str, Any]] = []
    for rec in ordered:
        ref_ts = pd.Timestamp(rec["ref_day"])
        total_mkt = 0.0
        for pos in rec["snapshot"]:
            tk = str(pos.get("ticker", "")).upper().strip()
            qtd = _safe_int(pos.get("qtd"), 0)
            if not tk or qtd <= 0:
                continue
            px = _safe_float(pos.get("preco_compra"), 0.0)
            sub = by_ticker.get(tk)
            if sub is not None and not sub.empty:
                sub_until = sub[sub["date"] <= ref_ts]
                if not sub_until.empty:
                    px = _safe_float(sub_until.iloc[-1]["close_raw"], px)
            total_mkt += qtd * px
        total_ativo = total_mkt + _safe_float(rec["cash_free"], 0.0) + _safe_float(rec["cash_acc"], 0.0)
        rows.append({"date": ref_ts, "total_ativo": total_ativo})

    out = pd.DataFrame(rows).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    if out.empty:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])
    base = _safe_float(out["total_ativo"].iloc[0], 0.0)
    if base <= 0:
        return pd.DataFrame(columns=["date", "total_ativo", "base1", "daily_var_pct"])
    out["base1"] = out["total_ativo"] / base
    out["daily_var_pct"] = out["base1"].pct_change() * 100.0
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
            title=dict(text=f"Base 1 - Inicio: {_fmt_date_br(proj['date'].iloc[0].date())}", font_size=13),
            height=430,
            template="plotly_white",
            margin=dict(l=50, r=20, t=50, b=30),
        )
        return fig.to_html(full_html=False, include_plotlyjs=False)

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
            x=proj["date"],
            y=proj["base1"],
            mode="lines+markers",
            name="Carteira Real",
            line=dict(color="#1f77b4", width=2.5),
            marker=dict(size=6),
        ),
        secondary_y=False,
    )
    fig.update_layout(
        title=dict(
            text=f"Base 1 - Inicio: {_fmt_date_br(proj['date'].iloc[0].date())} | Ate: {_fmt_date_br(as_of_day)}",
            font_size=13,
        ),
        height=430,
        template="plotly_white",
        margin=dict(l=50, r=20, t=50, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="Base 1", secondary_y=False)
    fig.update_yaxes(title_text="Var. Diaria (%)", secondary_y=True)
    return fig.to_html(full_html=False, include_plotlyjs=False)


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


def _build_sell_suggestions(decision: dict[str, Any] | None, prices_d1: dict[str, float]) -> list[dict[str, Any]]:
    if not decision:
        return []
    actions = decision.get("defensive_actions", [])
    out: list[dict[str, Any]] = []
    for item in actions:
        tk = str(item.get("ticker", "")).upper().strip()
        if not tk:
            continue
        sell_pct = _safe_float(item.get("sell_pct", 0.0), 0.0)
        if sell_pct <= 1.0:
            sell_pct *= 100.0
        reason = str(item.get("reason", "")).strip() or "Acao defensiva do motor."
        out.append(
            {
                "ticker": tk,
                "sell_pct": sell_pct,
                "close_d1": _safe_float(prices_d1.get(tk, 0.0), 0.0),
                "reason": reason,
            }
        )
    return out


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
    d1_real_day, d1_payload = load_latest_real_before(cutoff_day)
    d2 = None
    if d1_real_day:
        _, d2_payload = load_latest_real_before(d1_real_day - timedelta(days=1))
        d2 = d2_payload

    lots, warnings = build_lot_ledger(cutoff_day)
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

    cash_free_prev = _safe_float((d2 or {}).get("cash_free", (d2 or {}).get("cash_balance", 0.0)), 0.0)
    cash_acc_prev = _safe_float((d2 or {}).get("cash_accounting", (d2 or {}).get("caixa_liquidando", 0.0)), 0.0)
    d1_ops = _extract_operations(d1_payload or {})
    d1_buy = sum(_safe_int(o.get("qtd"), 0) * _safe_float(o.get("preco"), 0.0) for o in d1_ops if o["type"] == "COMPRA")
    d1_sell = sum(_safe_int(o.get("qtd"), 0) * _safe_float(o.get("preco"), 0.0) for o in d1_ops if o["type"] == "VENDA")
    d1_aporte, d1_retirada = _extract_cash_movements(d1_payload or {})
    d1_transfer = _extract_transfers(d1_payload or {})
    cash_free_calc, cash_acc_calc = _calc_cash_balances(
        prev_free=cash_free_prev,
        prev_acc=cash_acc_prev,
        buy=d1_buy,
        sell=d1_sell,
        aporte=d1_aporte,
        retirada=d1_retirada,
        transfer=d1_transfer,
    )

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
    for p in list_real_files_upto(cutoff_day):
        pp = _read_json(p)
        a, r = _extract_cash_movements(pp)
        aporte_acc += a
        retirada_acc += r

    report_ctx = {
        "d1": d1.isoformat(),
        "d1_br": _fmt_date_br(d1),
        "d1_real_day": d1_real_day.isoformat() if d1_real_day else "",
        "cash_free_prev": cash_free_calc,
        "cash_accounting_prev": cash_acc_calc,
        "holdings_qty": holdings_qty,
        "prices_d1": prices_d1,
        "lots_snapshot": _make_positions_snapshot(lots),
        "carteira_valor_d1": total_current,
        "pending_sales": _pending_sales_for_transfer(exec_day),
        "aporte_acumulado": aporte_acc,
        "retirada_acumulada": retirada_acc,
    }
    return tables_html, report_ctx, warnings


def build_painel(exec_day: date) -> Path:
    decision = _read_json(ROOT / "data" / "daily" / f"decision_{exec_day.isoformat()}.json")
    decision_date = str(decision.get("target_date", exec_day.isoformat()))
    report_html, ctx, warnings = _build_tables_and_cards(exec_day)
    d1 = get_d_minus_1(exec_day)

    top20 = decision.get("portfolio", [])
    top_tickers = [str(x.get("ticker", "")).upper().strip() for x in top20 if str(x.get("ticker", "")).strip()]
    prices_top = get_latest_prices(top_tickers, as_of_day=d1)
    score_map = _load_score_map(d1)

    rows_info_top = []
    for p in top20[:20]:
        t = str(p.get("ticker", "")).upper().strip()
        rows_info_top.append(
            "<tr>"
            f"<td>{t}</td>"
            f"<td style='text-align:right'>{_safe_float(score_map.get(t, 0.0), 0.0):.4f}</td>"
            f"<td style='text-align:right'>{_fmt_money(_safe_float(prices_top.get(t, 0.0), 0.0))}</td>"
            "</tr>"
        )
    if not rows_info_top:
        rows_info_top.append("<tr><td colspan='3'>Top-20 indisponivel (sem decisao).</td></tr>")

    sell_suggestions = _build_sell_suggestions(decision=decision, prices_d1={**ctx["prices_d1"], **prices_top})
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

    action_rows: list[dict[str, Any]] = []
    for b in top_tickers[:20]:
        action_rows.append({"type": "COMPRA", "ticker": b, "qtd": 0, "preco": _safe_float(prices_top.get(b, 0.0), 0.0)})

    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{w}</li>" for w in warnings)
        warnings_html = f"<div class='warnings'><strong>Avisos de consistencia:</strong><ul>{items}</ul></div>"

    curve = _load_curve_until(d1)
    chart_252_html = _build_chart_252(curve=curve, as_of_day=d1)
    chart_base1_html = _build_chart_base1(as_of_day=d1)

    cycle_dir = ROOT / "data" / "cycles" / exec_day.isoformat()
    cycle_dir.mkdir(parents=True, exist_ok=True)
    out_cycle = cycle_dir / "painel.html"
    out_daily = ROOT / "data" / "daily" / f"painel_{exec_day.isoformat()}.html"
    out_daily.parent.mkdir(parents=True, exist_ok=True)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>Painel Diario - {_fmt_date_br(exec_day)}</title>
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
    <h1>Painel Diario - {_fmt_date_br(exec_day)}</h1>
    <div class="sub">Documento unico: Relatorio + Boletim | D-1 de mercado: {ctx["d1_br"]}</div>

    <div class="block">
      <div class="section-title">Sessao Relatorio</div>
      {warnings_html}
      {report_html}
      <div class="chart-grid">
        <div class="chart-wrap">{chart_252_html}</div>
        <div class="chart-wrap">{chart_base1_html}</div>
      </div>
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
const DECISION_DATE = "{decision_date}";
const PREV_FREE = {ctx["cash_free_prev"]};
const PREV_ACC = {ctx["cash_accounting_prev"]};
const CARTEIRA_D1 = {ctx["carteira_valor_d1"]};
const APORTE_ACC = {ctx["aporte_acumulado"]};
const RETIRADA_ACC = {ctx["retirada_acumulada"]};
const ACTION_ROWS = {json.dumps(action_rows, ensure_ascii=False)};
const SNAPSHOT_D1 = {json.dumps(ctx["lots_snapshot"], ensure_ascii=False)};
const PENDING_SALES = {json.dumps(ctx["pending_sales"], ensure_ascii=False)};

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
    operations: ops,
    cash_movements: cashMovements,
    cash_transfers: cashTransfers,
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
