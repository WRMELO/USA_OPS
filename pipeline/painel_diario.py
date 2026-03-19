"""Step 12 - painel diario HTML com duplo-caixa T+1 (T-032)."""
from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

ROOT = Path(__file__).resolve().parents[1]
NYSE_SESSION_TEXT = "NYSE 09:30-16:00 ET (rodar pos-fechamento)"
WINDOW_DAYS = 252


def _load_json(path: Path) -> dict:
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


def _fmt_usd(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    return f"USD {float(value):,.2f}"


def _fmt_pct(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.{digits}f}%"


def _fmt_num(value: float | int | None, digits: int = 4) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.{digits}f}"


def _load_curve(curve_path: Path) -> pd.DataFrame:
    if not curve_path.exists():
        return pd.DataFrame()
    curve = pd.read_parquet(curve_path).copy()
    curve["date"] = pd.to_datetime(curve.get("date"), errors="coerce").dt.normalize()
    curve["equity"] = pd.to_numeric(curve.get("equity"), errors="coerce")
    curve = curve.dropna(subset=["date", "equity"]).sort_values("date").drop_duplicates("date", keep="last")
    if curve.empty:
        return curve
    if "ret_1d" not in curve.columns:
        curve["ret_1d"] = curve["equity"].pct_change()
    else:
        curve["ret_1d"] = pd.to_numeric(curve["ret_1d"], errors="coerce")
        curve["ret_1d"] = curve["ret_1d"].fillna(curve["equity"].pct_change())
    if "equity_base100" not in curve.columns:
        base = float(curve["equity"].iloc[0]) if float(curve["equity"].iloc[0]) > 0 else 1.0
        curve["equity_base100"] = (curve["equity"] / base) * 100.0
    else:
        curve["equity_base100"] = pd.to_numeric(curve["equity_base100"], errors="coerce")
    curve["running_max"] = curve["equity"].cummax()
    curve["drawdown_pct"] = ((curve["equity"] / curve["running_max"]) - 1.0) * 100.0
    return curve.reset_index(drop=True)


def _window_curve(curve: pd.DataFrame, target_ts: pd.Timestamp) -> pd.DataFrame:
    if curve.empty:
        return curve
    part = curve[curve["date"] <= target_ts].copy()
    if part.empty:
        return curve.tail(WINDOW_DAYS).copy()
    return part.tail(WINDOW_DAYS).copy()


def _build_plot_blocks(window: pd.DataFrame) -> tuple[str, str, str]:
    if window.empty:
        msg = "<p>Sem dados de curva para exibir graficos.</p>"
        return msg, msg, msg

    x = window["date"]
    fig_base = go.Figure()
    fig_base.add_trace(
        go.Scatter(
            x=x,
            y=window["equity_base100"],
            mode="lines",
            name="Equity Base100",
            line={"width": 2},
        )
    )
    fig_base.update_layout(title=f"Curva Base100 - ultimos {len(window)} pregoes", template="plotly_white", height=320)

    fig_dd = go.Figure()
    fig_dd.add_trace(
        go.Scatter(
            x=x,
            y=window["drawdown_pct"],
            mode="lines",
            name="Drawdown %",
            line={"width": 2, "color": "#ef4444"},
            fill="tozeroy",
        )
    )
    fig_dd.update_layout(title="Drawdown (%)", template="plotly_white", height=320)

    fig_ret = go.Figure()
    fig_ret.add_trace(
        go.Bar(
            x=x,
            y=window["ret_1d"].fillna(0.0) * 100.0,
            name="Retorno diario %",
            marker={"color": "#2563eb"},
        )
    )
    fig_ret.update_layout(title="Retornos diarios (%)", template="plotly_white", height=320)

    return (
        fig_base.to_html(full_html=False, include_plotlyjs="cdn"),
        fig_dd.to_html(full_html=False, include_plotlyjs=False),
        fig_ret.to_html(full_html=False, include_plotlyjs=False),
    )


def _portfolio_table(decision: dict) -> str:
    rows = []
    for item in decision.get("portfolio", []):
        ticker = str(item.get("ticker", "")).upper()
        w = float(item.get("target_weight", 0.0)) * 100.0
        rows.append((ticker, w))
    rows = sorted(rows, key=lambda x: (-x[1], x[0]))
    if not rows:
        return "<p>Sem carteira alvo disponivel.</p>"
    html_rows = "".join([f"<tr><td>{t}</td><td>{w:.2f}%</td></tr>" for t, w in rows])
    return (
        "<table><thead><tr><th>Ticker</th><th>Peso alvo</th></tr></thead>"
        f"<tbody>{html_rows}</tbody></table>"
    )


def _defensive_table(decision: dict) -> str:
    actions = decision.get("defensive_actions", [])
    if not actions:
        return "<p>Nenhuma acao defensiva no dia.</p>"
    rows = []
    for item in actions:
        ticker = str(item.get("ticker", "")).upper()
        score = int(item.get("score", 0))
        sell = float(item.get("sell_pct", 0.0)) * 100.0
        rows.append((ticker, score, sell))
    rows = sorted(rows, key=lambda x: (-x[1], x[0]))
    html_rows = "".join([f"<tr><td>{t}</td><td>{s}</td><td>{p:.1f}%</td></tr>" for t, s, p in rows])
    return (
        "<table><thead><tr><th>Ticker</th><th>Score</th><th>Sell %</th></tr></thead>"
        f"<tbody>{html_rows}</tbody></table>"
    )


def _events_summary(target_day: date, selected_tickers: list[str]) -> dict:
    path = ROOT / "data" / "ssot" / "us_market_data_raw.parquet"
    if not path.exists():
        return {}
    try:
        cols = ["date", "ticker", "dividend_rate", "split_from", "split_to"]
        raw = pd.read_parquet(path, columns=cols).copy()
    except Exception:
        return {}

    raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.normalize()
    raw["ticker"] = raw["ticker"].astype(str).str.upper().str.strip()
    raw["dividend_rate"] = pd.to_numeric(raw.get("dividend_rate"), errors="coerce").fillna(0.0)
    raw["split_from"] = pd.to_numeric(raw.get("split_from"), errors="coerce")
    raw["split_to"] = pd.to_numeric(raw.get("split_to"), errors="coerce")
    raw = raw.dropna(subset=["date", "ticker"])

    begin = pd.Timestamp(target_day - timedelta(days=92)).normalize()
    end = pd.Timestamp(target_day).normalize()
    raw = raw[(raw["date"] >= begin) & (raw["date"] <= end)].copy()
    if selected_tickers:
        st = {str(x).upper().strip() for x in selected_tickers}
        raw = raw[raw["ticker"].isin(st)].copy()

    if raw.empty:
        return {"dividend_events": 0, "split_events": 0}

    div_events = int((raw["dividend_rate"] > 0).sum())
    split_mask = (
        raw["split_from"].notna()
        & raw["split_to"].notna()
        & (raw["split_from"] > 0)
        & (raw["split_to"] > 0)
        & ((raw["split_from"] / raw["split_to"]) != 1.0)
    )
    split_events = int(split_mask.sum())
    return {"dividend_events": div_events, "split_events": split_events}


def _list_real_files_upto(max_day: date) -> list[Path]:
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


def _load_latest_real_before(ref_day: date) -> tuple[date | None, dict[str, Any] | None]:
    files = _list_real_files_upto(ref_day)
    if not files:
        return None, None
    p = files[-1]
    return date.fromisoformat(p.stem), _load_json(p)


def _extract_ops_for_prefill(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for op in payload.get("operations", []):
        typ = str(op.get("type", "")).upper().strip()
        if typ not in {"COMPRA", "VENDA"}:
            continue
        rows.append(
            {
                "type": typ,
                "ticker": str(op.get("ticker", "")).upper().strip(),
                "qtd": _safe_int(op.get("qtd"), 0),
                "preco": _safe_float(op.get("preco"), 0.0),
            }
        )
    return rows


def _extract_cash_movements(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], float, float]:
    rows: list[dict[str, Any]] = []
    aporte = 0.0
    retirada = 0.0
    for mv in payload.get("cash_movements", []):
        typ = str(mv.get("type", "")).upper().strip()
        val = _safe_float(mv.get("value", mv.get("valor", 0.0)), 0.0)
        desc = str(mv.get("desc", mv.get("description", ""))).strip()
        if typ:
            rows.append({"type": typ, "value": val, "desc": desc})
        if typ in {"APORTE", "DEPOSITO", "DIVIDENDO", "JCP", "BONUS"}:
            aporte += val
        elif typ in {"RETIRADA", "SAQUE"}:
            retirada += val
    return rows, aporte, retirada


def _extract_transfer_rows(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], float]:
    rows: list[dict[str, Any]] = []
    total = 0.0
    for tr in payload.get("cash_transfers", []):
        val = _safe_float(tr.get("value", tr.get("valor", 0.0)), 0.0)
        note = str(tr.get("note", tr.get("ref", "Transferencia C->L"))).strip()
        rows.append({"value": val, "note": note})
        total += val
    return rows, total


def _calc_cash_balances(
    prev_free: float,
    prev_acc: float,
    buy: float,
    sell: float,
    aporte: float,
    retirada: float,
    transfer: float,
) -> tuple[float, float]:
    # Regra T+1 US: transferencias C->L no D geralmente liquidam o saldo contabil de D-1.
    free = prev_free + transfer + aporte - retirada - buy
    acc = prev_acc + sell - transfer
    return free, acc


def run(target_date: date | None = None) -> str:
    if target_date is None:
        target_date = datetime.now(tz=UTC).date()
    target_ts = pd.Timestamp(target_date).normalize()

    decision_path = ROOT / "data" / "daily" / f"decision_{target_date}.json"
    curve_path = ROOT / "data" / "daily" / "winner_curve_us.parquet"
    recon_path = ROOT / "logs" / "metrics_reconciliation.json"
    out_path = ROOT / "data" / "daily" / f"painel_{target_date}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    decision = _load_json(decision_path)
    recon = _load_json(recon_path)
    curve = _load_curve(curve_path)
    window = _window_curve(curve, target_ts)
    current_real = _load_json(ROOT / "data" / "real" / f"{target_date.isoformat()}.json")
    d1_real_day, d1_real = _load_latest_real_before(target_date - timedelta(days=1))
    d1_real = d1_real or {}

    base_plot_html, dd_plot_html, ret_plot_html = _build_plot_blocks(window)

    curve_last = {}
    if not window.empty:
        tail = window.iloc[-1]
        prev = window.iloc[-2] if len(window) >= 2 else tail
        equity = float(tail["equity"])
        equity_prev = float(prev["equity"])
        ret_1d = float(tail.get("ret_1d", 0.0))
        pnl_1d = equity - equity_prev
        pnl_window = equity - float(window["equity"].iloc[0])
        curve_last = {
            "date": str(pd.Timestamp(tail["date"]).date()),
            "equity": equity,
            "equity_base100": float(tail.get("equity_base100", 0.0)),
            "ret_1d_pct": ret_1d * 100.0,
            "pnl_1d": pnl_1d,
            "pnl_window": pnl_window,
            "window_points": int(len(window)),
        }

    cfg = decision.get("winner_config_snapshot", {})
    selected_tickers = [str(x).upper().strip() for x in decision.get("selected_tickers", []) if str(x).strip()]
    events = _events_summary(target_day=target_date, selected_tickers=selected_tickers)
    prev_free = _safe_float(d1_real.get("cash_free", d1_real.get("cash_balance", 0.0)), 0.0)
    prev_acc = _safe_float(d1_real.get("cash_accounting", d1_real.get("caixa_liquidando", 0.0)), 0.0)

    ops_prefill = _extract_ops_for_prefill(current_real)
    cash_rows, aporte_prefill, retirada_prefill = _extract_cash_movements(current_real)
    transfer_rows, transfer_prefill = _extract_transfer_rows(current_real)
    if not transfer_rows:
        transfer_prefill = prev_acc
        transfer_rows = [{"value": transfer_prefill, "note": "Transferencia T+1 C->L (padrao)"}]
    buy_prefill = sum(
        _safe_int(op.get("qtd"), 0) * _safe_float(op.get("preco"), 0.0) for op in ops_prefill if op.get("type") == "COMPRA"
    )
    sell_prefill = sum(
        _safe_int(op.get("qtd"), 0) * _safe_float(op.get("preco"), 0.0) for op in ops_prefill if op.get("type") == "VENDA"
    )
    free_calc, acc_calc = _calc_cash_balances(
        prev_free=prev_free,
        prev_acc=prev_acc,
        buy=buy_prefill,
        sell=sell_prefill,
        aporte=aporte_prefill,
        retirada=retirada_prefill,
        transfer=transfer_prefill,
    )
    carteira_d1 = _safe_float(curve_last.get("equity"), 0.0)
    total_ativo = carteira_d1 + free_calc + acc_calc

    ops_prefill_json = json.dumps(ops_prefill, ensure_ascii=False)
    cash_prefill_json = json.dumps(cash_rows, ensure_ascii=False)
    transfer_prefill_json = json.dumps(transfer_rows, ensure_ascii=False)
    snapshot_prefill_json = json.dumps(decision.get("portfolio", []), ensure_ascii=False)

    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>USA_OPS Painel Diario - {target_date}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #111827; background: #f9fafb; }}
    h1, h2, h3 {{ margin: 8px 0; }}
    .muted {{ color: #4b5563; }}
    .grid {{ display: grid; grid-template-columns: repeat(2, minmax(320px, 1fr)); gap: 12px; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 12px; margin: 10px 0; background: #ffffff; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #e5e7eb; padding: 6px 8px; font-size: 14px; }}
    th {{ text-align: left; background: #f3f4f6; }}
    .ok {{ color: #065f46; font-weight: 700; }}
    .fail {{ color: #991b1b; font-weight: 700; }}
    .small {{ font-size: 13px; }}
    .row-grid {{ display: grid; grid-template-columns: 130px 120px 120px 120px 40px; gap: 8px; margin-bottom: 8px; }}
    .row-grid-3 {{ display: grid; grid-template-columns: 160px 140px 1fr 40px; gap: 8px; margin-bottom: 8px; }}
    .row-grid-2 {{ display: grid; grid-template-columns: 160px 140px 1fr 40px; gap: 8px; margin-bottom: 8px; }}
    input, select {{ padding: 6px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 13px; }}
    button {{ padding: 8px 12px; border: 0; border-radius: 6px; cursor: pointer; }}
    .btn-primary {{ background: #1d4ed8; color: #fff; }}
    .btn-secondary {{ background: #e5e7eb; color: #111827; }}
    .cash-grid {{ display: grid; grid-template-columns: repeat(2, minmax(280px, 1fr)); gap: 12px; }}
    .cash-panel {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 10px; background: #fafcff; }}
    .cash-row {{ display: flex; justify-content: space-between; margin-bottom: 6px; font-size: 13px; }}
    .save-msg {{ margin-left: 8px; font-size: 13px; }}
    .save-ok {{ color: #166534; }}
    .save-err {{ color: #991b1b; }}
  </style>
</head>
<body>
  <h1>USA_OPS - Painel Diario</h1>
  <p class="muted">Gerado em UTC: {datetime.now(tz=UTC).isoformat()} | Sessao: {NYSE_SESSION_TEXT}</p>
  <p class="small muted">Plotly carregado via CDN (internet necessaria para assets JS).</p>

  <div class="card">
    <h2>Resumo Operacional (D)</h2>
    <div class="grid">
      <div>
        <p><b>Data alvo (D):</b> {decision.get("target_date", str(target_date))}</p>
        <p><b>Data score (D-1):</b> {decision.get("scores_reference_date_d_minus_1", "N/A")}</p>
        <p><b>Acao:</b> {decision.get("action", "N/A")}</p>
        <p><b>Tickers selecionados:</b> {decision.get("selected_count", 0)}</p>
      </div>
      <div>
        <p><b>Variant:</b> {cfg.get("variant", "N/A")}</p>
        <p><b>TopN/Cad/K:</b> {cfg.get("top_n", "N/A")} / {cfg.get("rebalance_cadence", "N/A")} / {cfg.get("buffer_k", "N/A")}</p>
        <p><b>Cap max:</b> {_fmt_pct(float(cfg["max_weight_cap"]) * 100.0, 2) if "max_weight_cap" in cfg else "N/A"}</p>
        <p><b>Min Market Cap:</b> {_fmt_usd(cfg.get("min_market_cap"))}</p>
        <p><b>Settlement:</b> T+{cfg.get("settlement_days", "N/A")}</p>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>Curva Operacional - Ultimos 252 pregoes</h2>
    <p><b>Ultima data na curva:</b> {curve_last.get("date", "N/A")} | <b>Pontos:</b> {curve_last.get("window_points", "N/A")}</p>
    <p><b>Equity:</b> {_fmt_usd(curve_last.get("equity"))} | <b>Base100:</b> {_fmt_num(curve_last.get("equity_base100"), 2)}</p>
    <p><b>Retorno 1D:</b> {_fmt_pct(curve_last.get("ret_1d_pct"))} | <b>PnL 1D:</b> {_fmt_usd(curve_last.get("pnl_1d"))} | <b>PnL Janela:</b> {_fmt_usd(curve_last.get("pnl_window"))}</p>
    {base_plot_html}
    {dd_plot_html}
    {ret_plot_html}
  </div>

  <div class="card">
    <h2>Carteira Alvo</h2>
    {_portfolio_table(decision)}
  </div>

  <div class="card">
    <h2>Acoes Defensivas</h2>
    {_defensive_table(decision)}
  </div>

  <div class="card">
    <h2>Reconciliacao</h2>
    <p><b>Status:</b> <span class="{'ok' if recon.get('status') == 'PASS' else 'fail'}">{recon.get("status", "N/A")}</span></p>
    <p><b>Tolerancia (p.p.):</b> {_fmt_num(recon.get("tolerance_pct_points"), 2)}</p>
    <p><b>CAGR op.:</b> {_fmt_pct(recon.get("operational_metrics", {}).get("cagr_pct"))} | <b>MDD op.:</b> {_fmt_pct(recon.get("operational_metrics", {}).get("mdd_pct"))} | <b>Sharpe:</b> {_fmt_num(recon.get("operational_metrics", {}).get("sharpe"), 3)}</p>
    <p><b>Diff CAGR (p.p.):</b> {_fmt_num(recon.get("diffs_abs", {}).get("cagr_pct_points"), 4)} | <b>Diff MDD (p.p.):</b> {_fmt_num(recon.get("diffs_abs", {}).get("mdd_pct_points"), 4)}</p>
  </div>

  <div class="card">
    <h2>Proventos/Events (ultimo trimestre)</h2>
    <p><b>Dividend events:</b> {events.get("dividend_events", "N/A")} | <b>Split events:</b> {events.get("split_events", "N/A")}</p>
    <p class="small muted">Secao informativa. Curva operacional permanece conforme Step 10.</p>
  </div>

  <div class="card">
    <h2>Duplo-Caixa US (T+1)</h2>
    <p class="small muted">
      Regra normativa US T+1: por padrao, o Caixa Contabil de D-1 liquida em D via transferencia Contabil -> Livre.
      D-1 real carregado de: {d1_real_day.isoformat() if d1_real_day else "N/A"}.
    </p>

    <h3>Operacoes do dia</h3>
    <div id="opsRows"></div>
    <button class="btn-secondary" onclick="addOp()">+ Operacao</button>

    <h3 style="margin-top:12px;">Movimentos extraordinarios de caixa</h3>
    <div id="cashRows"></div>
    <button class="btn-secondary" onclick="addCash()">+ Movimento</button>

    <h3 style="margin-top:12px;">Transferencias Contabil -> Livre</h3>
    <div id="transferRows"></div>
    <button class="btn-secondary" onclick="addTransfer()">+ Transferencia</button>

    <div class="cash-grid" style="margin-top:12px;">
      <div class="cash-panel">
        <h3>Balanço simplificado (D)</h3>
        <div class="cash-row"><span>Carteira (proxy equity)</span><strong id="bal_carteira">{_fmt_usd(carteira_d1)}</strong></div>
        <div class="cash-row"><span>Caixa Livre (D)</span><strong id="bal_free">{_fmt_usd(free_calc)}</strong></div>
        <div class="cash-row"><span>Caixa Contabil (D)</span><strong id="bal_acc">{_fmt_usd(acc_calc)}</strong></div>
        <div class="cash-row"><span><b>Total do Ativo</b></span><strong id="bal_total">{_fmt_usd(total_ativo)}</strong></div>
      </div>
      <div class="cash-panel">
        <h3>DFC simplificado (D)</h3>
        <div class="cash-row"><span>Caixa Livre anterior</span><strong id="dfc_free_open">{_fmt_usd(prev_free)}</strong></div>
        <div class="cash-row"><span>Caixa Contabil anterior</span><strong id="dfc_acc_open">{_fmt_usd(prev_acc)}</strong></div>
        <div class="cash-row"><span>Compras do dia</span><strong id="dfc_buy">{_fmt_usd(buy_prefill)}</strong></div>
        <div class="cash-row"><span>Vendas do dia</span><strong id="dfc_sell">{_fmt_usd(sell_prefill)}</strong></div>
        <div class="cash-row"><span>Aportes/Retiradas</span><strong id="dfc_mov">{_fmt_usd(aporte_prefill - retirada_prefill)}</strong></div>
        <div class="cash-row"><span>Transferencias C->L</span><strong id="dfc_transfer">{_fmt_usd(transfer_prefill)}</strong></div>
      </div>
    </div>

    <p style="margin-top:10px;">
      <label>Caixa liquido real (informado):</label>
      <input id="cash_real_input" type="number" step="0.01" min="0" value="{_safe_float(current_real.get('caixa_liquido_real'), 0.0)}" />
    </p>
    <p>
      <button class="btn-primary" onclick="saveBoletim()">Salvar boletim</button>
      <span id="save_msg" class="save-msg"></span>
    </p>
  </div>

<script>
const TARGET_DATE = "{target_date.isoformat()}";
const DECISION_DATE = "{decision.get('target_date', target_date.isoformat())}";
const PREV_FREE = {prev_free};
const PREV_ACC = {prev_acc};
const CARTEIRA_D1 = {carteira_d1};
const OPS_PREFILL = {ops_prefill_json};
const CASH_PREFILL = {cash_prefill_json};
const TRANSFER_PREFILL = {transfer_prefill_json};
const SNAPSHOT_PREFILL = {snapshot_prefill_json};

let opIdx = 0;
let cashIdx = 0;
let trIdx = 0;

function usd(v) {{
  const n = Number(v || 0);
  return `USD ${{n.toLocaleString(undefined, {{minimumFractionDigits:2, maximumFractionDigits:2}})}}`;
}}
function num(id) {{
  const el = document.getElementById(id);
  if (!el) return 0;
  const v = Number(el.value);
  return Number.isFinite(v) ? v : 0;
}}
function removeRow(id) {{
  const el = document.getElementById(id);
  if (el) el.remove();
  recalc();
}}
function addOp(pref = null) {{
  const i = opIdx++;
  const box = document.getElementById("opsRows");
  const typ = pref?.type || "COMPRA";
  const tk = pref?.ticker || "";
  const qtd = pref?.qtd || 0;
  const px = pref?.preco || 0;
  const row = document.createElement("div");
  row.className = "row-grid";
  row.id = `op_${{i}}`;
  row.innerHTML = `
    <select id="op_type_${{i}}" onchange="recalc()">
      <option value="COMPRA" ${{typ==='COMPRA'?'selected':''}}>COMPRA</option>
      <option value="VENDA" ${{typ==='VENDA'?'selected':''}}>VENDA</option>
    </select>
    <input id="op_ticker_${{i}}" value="${{tk}}" placeholder="Ticker" />
    <input id="op_qtd_${{i}}" type="number" min="0" step="1" value="${{qtd}}" onchange="recalc()" />
    <input id="op_px_${{i}}" type="number" min="0" step="0.01" value="${{px}}" onchange="recalc()" />
    <button onclick="removeRow('op_${{i}}')">x</button>
  `;
  box.appendChild(row);
}}
function addCash(pref = null) {{
  const i = cashIdx++;
  const box = document.getElementById("cashRows");
  const typ = pref?.type || "APORTE";
  const val = pref?.value || 0;
  const desc = pref?.desc || "";
  const row = document.createElement("div");
  row.className = "row-grid-3";
  row.id = `cash_${{i}}`;
  row.innerHTML = `
    <select id="cash_type_${{i}}" onchange="recalc()">
      <option value="APORTE" ${{typ==='APORTE'?'selected':''}}>APORTE</option>
      <option value="RETIRADA" ${{typ==='RETIRADA'?'selected':''}}>RETIRADA</option>
      <option value="DIVIDENDO" ${{typ==='DIVIDENDO'?'selected':''}}>DIVIDENDO</option>
      <option value="JCP" ${{typ==='JCP'?'selected':''}}>JCP</option>
      <option value="DEPOSITO" ${{typ==='DEPOSITO'?'selected':''}}>DEPOSITO</option>
      <option value="SAQUE" ${{typ==='SAQUE'?'selected':''}}>SAQUE</option>
    </select>
    <input id="cash_val_${{i}}" type="number" min="0" step="0.01" value="${{val}}" onchange="recalc()" />
    <input id="cash_desc_${{i}}" value="${{desc}}" placeholder="Descricao" />
    <button onclick="removeRow('cash_${{i}}')">x</button>
  `;
  box.appendChild(row);
}}
function addTransfer(pref = null) {{
  const i = trIdx++;
  const box = document.getElementById("transferRows");
  const val = pref?.value || 0;
  const note = pref?.note || "Transferencia C->L";
  const row = document.createElement("div");
  row.className = "row-grid-2";
  row.id = `tr_${{i}}`;
  row.innerHTML = `
    <input value="TRANSFERENCIA_C2L" disabled />
    <input id="tr_val_${{i}}" type="number" min="0" step="0.01" value="${{val}}" onchange="recalc()" />
    <input id="tr_note_${{i}}" value="${{note}}" placeholder="Observacao" />
    <button onclick="removeRow('tr_${{i}}')">x</button>
  `;
  box.appendChild(row);
}}
function collectOps() {{
  const rows = [];
  for (let i = 0; i < opIdx; i++) {{
    const row = document.getElementById(`op_${{i}}`);
    if (!row) continue;
    rows.push({{
      type: document.getElementById(`op_type_${{i}}`).value,
      ticker: (document.getElementById(`op_ticker_${{i}}`).value || '').toUpperCase().trim(),
      qtd: Number(document.getElementById(`op_qtd_${{i}}`).value || 0),
      preco: Number(document.getElementById(`op_px_${{i}}`).value || 0),
    }});
  }}
  return rows.filter(r => r.ticker && r.qtd > 0 && r.preco >= 0);
}}
function collectCash() {{
  const rows = [];
  for (let i = 0; i < cashIdx; i++) {{
    const row = document.getElementById(`cash_${{i}}`);
    if (!row) continue;
    rows.push({{
      type: document.getElementById(`cash_type_${{i}}`).value,
      value: Number(document.getElementById(`cash_val_${{i}}`).value || 0),
      desc: document.getElementById(`cash_desc_${{i}}`).value || '',
    }});
  }}
  return rows.filter(r => r.value > 0);
}}
function collectTransfers() {{
  const rows = [];
  for (let i = 0; i < trIdx; i++) {{
    const row = document.getElementById(`tr_${{i}}`);
    if (!row) continue;
    rows.push({{
      type: "TRANSFERENCIA_C2L",
      value: Number(document.getElementById(`tr_val_${{i}}`).value || 0),
      note: document.getElementById(`tr_note_${{i}}`).value || '',
    }});
  }}
  return rows.filter(r => r.value > 0);
}}
function recalc() {{
  const ops = collectOps();
  const cash = collectCash();
  const trs = collectTransfers();
  let buy = 0, sell = 0, aporte = 0, retirada = 0, transfer = 0;
  for (const op of ops) {{
    const val = (Number(op.qtd) || 0) * (Number(op.preco) || 0);
    if (op.type === "COMPRA") buy += val;
    if (op.type === "VENDA") sell += val;
  }}
  for (const mv of cash) {{
    if (["APORTE","DEPOSITO","DIVIDENDO","JCP","BONUS"].includes(mv.type)) aporte += Number(mv.value)||0;
    if (["RETIRADA","SAQUE"].includes(mv.type)) retirada += Number(mv.value)||0;
  }}
  for (const t of trs) transfer += Number(t.value)||0;
  const free = PREV_FREE + transfer + aporte - retirada - buy;
  const acc = PREV_ACC + sell - transfer;
  const total = CARTEIRA_D1 + free + acc;
  document.getElementById("bal_free").innerText = usd(free);
  document.getElementById("bal_acc").innerText = usd(acc);
  document.getElementById("bal_total").innerText = usd(total);
  document.getElementById("dfc_buy").innerText = usd(buy);
  document.getElementById("dfc_sell").innerText = usd(sell);
  document.getElementById("dfc_mov").innerText = usd(aporte - retirada);
  document.getElementById("dfc_transfer").innerText = usd(transfer);
}}
function buildPayload() {{
  const operations = collectOps();
  const cash_movements = collectCash();
  const cash_transfers = collectTransfers();
  let buy = 0, sell = 0, aporte = 0, retirada = 0, transfer = 0;
  for (const op of operations) {{
    const val = (Number(op.qtd) || 0) * (Number(op.preco) || 0);
    if (op.type === "COMPRA") buy += val;
    if (op.type === "VENDA") sell += val;
  }}
  for (const mv of cash_movements) {{
    if (["APORTE","DEPOSITO","DIVIDENDO","JCP","BONUS"].includes(mv.type)) aporte += Number(mv.value)||0;
    if (["RETIRADA","SAQUE"].includes(mv.type)) retirada += Number(mv.value)||0;
  }}
  for (const tr of cash_transfers) transfer += Number(tr.value)||0;
  const cash_free = PREV_FREE + transfer + aporte - retirada - buy;
  const cash_accounting = PREV_ACC + sell - transfer;
  return {{
    date: TARGET_DATE,
    reference_decision: DECISION_DATE,
    operations,
    cash_movements,
    cash_transfers,
    cash_free,
    cash_accounting,
    cash_balance: cash_free,
    caixa_liquidando: cash_accounting,
    caixa_liquido_real: Number(document.getElementById("cash_real_input").value || 0),
    positions_snapshot: SNAPSHOT_PREFILL,
  }};
}}
async function saveBoletim() {{
  const msg = document.getElementById("save_msg");
  msg.className = "save-msg";
  msg.innerText = "Salvando...";
  const payload = buildPayload();
  try {{
    const resp = await fetch("/salvar", {{
      method: "POST",
      headers: {{ "Content-Type": "application/json" }},
      body: JSON.stringify(payload),
    }});
    const data = await resp.json();
    if (!resp.ok || !data.ok) {{
      msg.className = "save-msg save-err";
      msg.innerText = "Erro: " + (data.error || `HTTP ${{resp.status}}`);
      return;
    }}
    msg.className = "save-msg save-ok";
    msg.innerText = "Salvo em " + (data.path || "data/real");
  }} catch (_err) {{
    // Fallback offline: baixar JSON localmente.
    const blob = new Blob([JSON.stringify(payload, null, 2)], {{type: "application/json"}});
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${{TARGET_DATE}}.json`;
    a.click();
    URL.revokeObjectURL(url);
    msg.className = "save-msg save-ok";
    msg.innerText = "Servidor indisponivel; JSON baixado localmente.";
  }}
}}
for (const op of OPS_PREFILL) addOp(op);
for (const mv of CASH_PREFILL) addCash(mv);
for (const tr of TRANSFER_PREFILL) addTransfer(tr);
if (OPS_PREFILL.length === 0) addOp();
recalc();
</script>
</body>
</html>
"""
    out_path.write_text(html, encoding="utf-8")
    return str(out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera painel diario USA_OPS")
    parser.add_argument("--date", type=str, default=None, help="Data alvo (YYYY-MM-DD)")
    args = parser.parse_args()
    target = date.fromisoformat(args.date) if args.date else None
    path = run(target_date=target)
    print(path)


if __name__ == "__main__":
    main()
