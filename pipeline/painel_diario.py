"""Step 12 — painel diário mínimo (stub operacional para T-029)."""
from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def run(target_date: date | None = None) -> str:
    if target_date is None:
        target_date = datetime.now(tz=UTC).date()

    decision_path = ROOT / "data" / "daily" / f"decision_{target_date}.json"
    curve_path = ROOT / "data" / "daily" / "winner_curve_us.parquet"
    recon_path = ROOT / "logs" / "metrics_reconciliation.json"
    out_path = ROOT / "data" / "daily" / f"painel_{target_date}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    decision = {}
    if decision_path.exists():
        decision = json.loads(decision_path.read_text(encoding="utf-8"))

    curve_last = {}
    if curve_path.exists():
        curve = pd.read_parquet(curve_path).copy()
        curve["date"] = pd.to_datetime(curve["date"], errors="coerce").dt.normalize()
        curve = curve.dropna(subset=["date"]).sort_values("date")
        if not curve.empty:
            tail = curve.iloc[-1]
            curve_last = {
                "date": str(pd.Timestamp(tail["date"]).date()),
                "equity": float(tail.get("equity", 0.0)),
                "equity_base100": float(tail.get("equity_base100", 0.0)),
            }

    recon = {}
    if recon_path.exists():
        recon = json.loads(recon_path.read_text(encoding="utf-8"))

    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>USA_OPS Painel Diário - {target_date}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
    h1 {{ margin-bottom: 6px; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 12px; margin: 12px 0; }}
    code {{ background: #f3f4f6; padding: 2px 4px; border-radius: 4px; }}
    ul {{ margin: 6px 0; }}
  </style>
</head>
<body>
  <h1>USA_OPS - Painel Diário (Stub T-029)</h1>
  <p>Gerado em: {datetime.now(tz=UTC).isoformat()}</p>
  <div class="card">
    <h2>Decisão do Dia</h2>
    <p><b>Ação:</b> {decision.get("action", "N/A")}</p>
    <p><b>Data alvo:</b> {decision.get("target_date", "N/A")}</p>
    <p><b>Data score D-1:</b> {decision.get("scores_reference_date_d_minus_1", "N/A")}</p>
    <p><b>Tickers selecionados:</b> {decision.get("selected_count", 0)}</p>
    <p><code>{", ".join(decision.get("selected_tickers", [])[:20])}</code></p>
  </div>
  <div class="card">
    <h2>Curva Operacional</h2>
    <p><b>Última data:</b> {curve_last.get("date", "N/A")}</p>
    <p><b>Equity:</b> {curve_last.get("equity", "N/A")}</p>
    <p><b>Base 100:</b> {curve_last.get("equity_base100", "N/A")}</p>
  </div>
  <div class="card">
    <h2>Reconciliação</h2>
    <p><b>Status:</b> {recon.get("status", "N/A")}</p>
    <p><b>Diferença CAGR (p.p.):</b> {recon.get("diffs_abs", {}).get("cagr_pct_points", "N/A")}</p>
    <p><b>Diferença MDD (p.p.):</b> {recon.get("diffs_abs", {}).get("mdd_pct_points", "N/A")}</p>
  </div>
</body>
</html>
"""
    out_path.write_text(html, encoding="utf-8")
    return str(out_path)
