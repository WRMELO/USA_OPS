"""Step 12 - painel diario HTML (T-030)."""
from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

ROOT = Path(__file__).resolve().parents[1]
NYSE_SESSION_TEXT = "NYSE 09:30-16:00 ET (rodar pos-fechamento)"
WINDOW_DAYS = 252


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


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
