"""Boletim web LIVE-REAL-TEST com rascunho intermediario."""
from __future__ import annotations

import html
import json
import math
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

import pipeline.ledger as ledger_mod
from pipeline.ledger import EventType, append_event, compute_cash, create_event, export_snapshot, is_duplicate
from scripts.lookup_shadow_price import DEFAULT_WINDOW_PATH, lookup_close


DRAFT_DIR = ROOT / "data" / "live_real_test"
REAL_LEDGER_NAME = "ledger_real.jsonl"
SHADOW_LEDGER_NAME = "ledger_shadow.jsonl"
LIQUIDACAO_JA_NO_CAIXA = "JA_NO_CAIXA"
LIQUIDACAO_EM_LIQUIDACAO = "EM_LIQUIDACAO"


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _normalize_liquidacao(value: Any) -> str | None:
    raw = str(value or "").upper().strip()
    if raw in {LIQUIDACAO_JA_NO_CAIXA, LIQUIDACAO_EM_LIQUIDACAO}:
        return raw
    return None


def _fmt_usd(value: Any) -> str:
    return f"$ {_safe_float(value):,.2f}"


def _fmt_pct(value: Any) -> str:
    return f"{_safe_float(value):.2f}%"


def _fmt_qtd(value: Any) -> str:
    v = _safe_float(value)
    s = f"{v:.6f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _sign_class(value: Any) -> str:
    v = _safe_float(value, 0.0)
    if abs(v) <= 1e-9:
        return "flat"
    return "gpos" if v > 0 else "gneg"


def _fmt_signed_usd(value: float) -> tuple[str, str]:
    v = _safe_float(value, 0.0)
    if abs(v) <= 1e-9:
        return "$ 0.00", "flat"
    sign = "+" if v > 0 else "-"
    return f"{sign}$ {abs(v):,.2f}", ("gpos" if v > 0 else "gneg")


def _fmt_signed_pct(value: float) -> tuple[str, str]:
    v = _safe_float(value, 0.0)
    if abs(v) <= 1e-12:
        return "0.00%", "flat"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%", ("gpos" if v > 0 else "gneg")


def _sparkline_svg(values: list[float], *, width: int = 150, height: int = 30) -> str:
    vals: list[float] = []
    for value in values:
        try:
            n = float(value)
        except Exception:
            continue
        if math.isfinite(n):
            vals.append(n)
    if len(vals) < 2:
        return "<span class='flat'>-</span>"
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1.0
    n_vals = len(vals)
    pts: list[str] = []
    for idx, value in enumerate(vals):
        x = 1 + (width - 2) * idx / (n_vals - 1)
        y = (height - 2) - (height - 4) * ((value - lo) / span)
        pts.append(f"{x:.1f},{y:.1f}")
    color = "var(--green)" if vals[-1] >= vals[0] else "var(--red)"
    return (
        f"<svg class='spark' viewBox='0 0 {width} {height}' width='{width}' height='{height}'"
        " aria-hidden='true'>"
        f"<polyline fill='none' stroke='{color}' stroke-width='1.6' points='{' '.join(pts)}' /></svg>"
    )


def _base1_chart_svg(series: list[dict[str, Any]], *, width: int = 880, height: int = 240) -> str:
    clean: list[tuple[str, float, float]] = []
    for row in series:
        if not isinstance(row, dict):
            continue
        base = _safe_float(row.get("base1"), float("nan"))
        expect = _safe_float(row.get("cagr_expect"), float("nan"))
        if not (math.isfinite(base) and math.isfinite(expect)):
            continue
        clean.append((str(row.get("date", "")), base, expect))

    if len(clean) < 2:
        return "<div class='chart-empty'>Base 1 indisponivel.</div>"

    values = [v for _, b, e in clean for v in (b, e)]
    lo = min(values)
    hi = max(values)
    span = (hi - lo) or 1.0

    left = 36.0
    top = 10.0
    right = 14.0
    bottom = 24.0
    plot_w = width - left - right
    plot_h = height - top - bottom

    def _pt(idx: int, value: float) -> str:
        x = left + (plot_w * idx / (len(clean) - 1))
        y = top + (plot_h * (1.0 - ((value - lo) / span)))
        return f"{x:.1f},{y:.1f}"

    base_pts = " ".join(_pt(idx, base) for idx, (_, base, _) in enumerate(clean))
    exp_pts = " ".join(_pt(idx, exp) for idx, (_, _, exp) in enumerate(clean))

    y_guides: list[str] = []
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = top + plot_h * frac
        y_guides.append(
            f"<line x1='{left:.1f}' y1='{y:.1f}' x2='{left + plot_w:.1f}' y2='{y:.1f}' "
            "stroke='rgba(134,143,155,0.22)' stroke-width='1' />"
        )

    first_day = html.escape(clean[0][0])
    last_day = html.escape(clean[-1][0])
    return (
        f"<svg class='base1-chart' viewBox='0 0 {width} {height}' width='100%' height='{height}' "
        "role='img' aria-label='Base 1 real versus CAGR esperado'>"
        + "".join(y_guides)
        + f"<polyline fill='none' stroke='var(--amber)' stroke-width='1.9' stroke-dasharray='5 4' points='{exp_pts}' />"
        + f"<polyline fill='none' stroke='var(--teal)' stroke-width='2.3' points='{base_pts}' />"
        + f"<text x='{left:.1f}' y='{height - 6:.1f}' fill='var(--muted)' font-size='10'>{first_day}</text>"
        + f"<text x='{left + plot_w - 4:.1f}' y='{height - 6:.1f}' text-anchor='end' fill='var(--muted)' font-size='10'>{last_day}</text>"
        + "</svg>"
    )


def _load_sparklines(tickers: list[str], as_of: date, *, lookback: int = 62) -> dict[str, str]:
    if not tickers:
        return {}
    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists():
        return {}
    try:
        import pandas as pd

        opw = pd.read_parquet(path, columns=["date", "ticker", "close_operational"])
    except Exception:
        return {}

    opw["date"] = pd.to_datetime(opw["date"], errors="coerce").dt.normalize()
    opw["ticker"] = opw["ticker"].astype(str).str.upper().str.strip()
    opw["close_operational"] = pd.to_numeric(opw["close_operational"], errors="coerce")
    opw = opw.dropna(subset=["date", "ticker", "close_operational"])
    opw = opw[opw["date"] <= pd.Timestamp(as_of)]

    out: dict[str, str] = {}
    for ticker in tickers:
        sub = opw[opw["ticker"] == ticker].sort_values("date").tail(lookback)
        if sub.empty:
            out[ticker] = "<span class='flat'>-</span>"
            continue
        out[ticker] = _sparkline_svg(sub["close_operational"].tolist())
    return out


def _draft_skeleton(exec_day: date) -> dict[str, Any]:
    return {
        "exec_day": exec_day.isoformat(),
        "updated_at": _now_iso(),
        "operations": [],
    }


def draft_path(exec_day: date) -> Path:
    return DRAFT_DIR / f"draft_{exec_day.isoformat()}.json"


def load_draft(exec_day: date) -> dict[str, Any]:
    path = draft_path(exec_day)
    if not path.exists():
        return _draft_skeleton(exec_day)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _draft_skeleton(exec_day)
    out = _draft_skeleton(exec_day)
    out["exec_day"] = str(payload.get("exec_day", exec_day.isoformat()))
    out["updated_at"] = str(payload.get("updated_at", _now_iso()))
    ops = payload.get("operations", [])
    out["operations"] = ops if isinstance(ops, list) else []
    return out


def _save_draft(exec_day: date, payload: dict[str, Any]) -> dict[str, Any]:
    DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    payload["exec_day"] = exec_day.isoformat()
    payload["updated_at"] = _now_iso()
    path = draft_path(exec_day)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def _lookup_shadow_price(ticker: str, exec_day: date) -> tuple[float | None, bool]:
    try:
        result = lookup_close(ticker, exec_day, DEFAULT_WINDOW_PATH)
    except Exception:
        return None, False
    if not bool(result.get("found")):
        return None, False
    close_value = _safe_float(result.get("close"), 0.0)
    if close_value <= 0:
        return None, False
    return close_value, True


def add_operation(
    exec_day: date,
    tipo: str,
    ticker: str,
    *,
    qtd: float | None = None,
    preco: float,
    corretagem: float,
    preco_sombra: float | None = None,
    valor_investido: float | None = None,
    liquidacao: str | None = None,
) -> dict[str, Any]:
    op_type = str(tipo or "").upper().strip()
    op_ticker = str(ticker or "").upper().strip()
    op_preco = _safe_float(preco)
    op_corretagem = _safe_float(corretagem)
    if op_type not in {"COMPRA", "VENDA"}:
        raise ValueError("Tipo invalido")
    if not op_ticker:
        raise ValueError("Ticker obrigatorio")
    if op_preco <= 0:
        raise ValueError("Preco invalido")
    if op_corretagem < 0:
        raise ValueError("Corretagem invalida")
    op_liquidacao = _normalize_liquidacao(liquidacao)
    if op_type == "VENDA" and op_liquidacao is None:
        raise ValueError("Liquidacao invalida para VENDA (use JA_NO_CAIXA ou EM_LIQUIDACAO)")

    op_valor_investido = _safe_float(valor_investido, 0.0) if valor_investido is not None else 0.0
    if op_valor_investido > 0:
        op_qtd = ledger_mod.qtd_from_invested(op_valor_investido, op_preco)
    else:
        op_qtd = _safe_float(qtd, 0.0)
    if op_qtd <= 0:
        raise ValueError("Quantidade invalida: informe valor_investido ou qtd")

    shadow_auto = False
    shadow_price: float | None = None
    if preco_sombra is None:
        shadow_price, shadow_auto = _lookup_shadow_price(op_ticker, exec_day)
    else:
        provided = _safe_float(preco_sombra)
        shadow_price = provided if provided > 0 else None
        shadow_auto = False

    row = {
        "id": str(uuid.uuid4()),
        "type": op_type,
        "ticker": op_ticker,
        "qtd": round(op_qtd, 8),
        "preco": op_preco,
        "corretagem": op_corretagem,
        "preco_sombra": shadow_price,
        "preco_sombra_auto": shadow_auto,
        "valor_investido_informado": op_valor_investido if op_valor_investido > 0 else None,
        "liquidacao": op_liquidacao if op_type == "VENDA" else None,
        "created_at": _now_iso(),
    }
    payload = load_draft(exec_day)
    payload.setdefault("operations", []).append(row)
    return _save_draft(exec_day, payload)


def remove_operation(exec_day: date, row_id: str) -> dict[str, Any]:
    payload = load_draft(exec_day)
    row_id = str(row_id or "").strip()
    payload["operations"] = [
        op for op in payload.get("operations", []) if str(op.get("id", "")).strip() != row_id
    ]
    return _save_draft(exec_day, payload)


def archive_draft(exec_day: date) -> Path | None:
    source = draft_path(exec_day)
    if not source.exists():
        return None
    stamp = datetime.now(tz=UTC).strftime("%H%M%S")
    target = DRAFT_DIR / f"draft_{exec_day.isoformat()}_encerrado_{stamp}.json"
    seq = 1
    while target.exists():
        target = DRAFT_DIR / f"draft_{exec_day.isoformat()}_encerrado_{stamp}_{seq}.json"
        seq += 1
    source.rename(target)
    return target


def _resolve_ledger_dir(ledger_dir: Path) -> Path:
    if ledger_dir.is_absolute():
        return ledger_dir
    return (ROOT / ledger_dir).resolve()


def apply_draft_to_ledger(exec_day: date, ledger_dir: Path, operations: list[dict[str, Any]]) -> dict[str, Any]:
    resolved_ledger_dir = _resolve_ledger_dir(ledger_dir)
    resolved_ledger_dir.mkdir(parents=True, exist_ok=True)
    real_ledger_path = resolved_ledger_dir / REAL_LEDGER_NAME
    shadow_ledger_path = resolved_ledger_dir / SHADOW_LEDGER_NAME

    events_created: list[dict[str, Any]] = []
    warnings: list[str] = []

    ledger_mod.LEDGER_PATH = real_ledger_path
    try:
        for idx, raw in enumerate(operations, start=1):
            op_type = str(raw.get("type", "")).upper().strip()
            ticker = str(raw.get("ticker", "")).upper().strip()
            qtd = _safe_float(raw.get("qtd"))
            preco = _safe_float(raw.get("preco"))
            corretagem = _safe_float(raw.get("corretagem"))
            preco_sombra = _safe_float(raw.get("preco_sombra"), 0.0)
            liquidacao_raw = raw.get("liquidacao")
            liquidacao = _normalize_liquidacao(liquidacao_raw)
            if op_type == "VENDA" and liquidacao is None:
                # Compatibilidade para rascunhos legados sem o campo.
                liquidacao = LIQUIDACAO_JA_NO_CAIXA

            if op_type not in {"COMPRA", "VENDA"}:
                warnings.append(f"Operacao {idx} ignorada: tipo invalido.")
                continue
            if not ticker or qtd <= 0 or preco <= 0:
                warnings.append(f"Operacao {idx} ignorada: dados incompletos.")
                continue
            if op_type == "VENDA" and liquidacao not in {LIQUIDACAO_JA_NO_CAIXA, LIQUIDACAO_EM_LIQUIDACAO}:
                warnings.append(f"Operacao {idx} ignorada: liquidacao invalida para VENDA.")
                continue

            event_type = EventType.BUY if op_type == "COMPRA" else EventType.SELL
            amount = float(qtd * preco)
            event_reason = "LIVE-REAL-TEST web close"
            settle_date = None
            if op_type == "VENDA":
                event_reason += f" | liquidacao={liquidacao}"
                if liquidacao == LIQUIDACAO_JA_NO_CAIXA:
                    settle_date = exec_day
            event = create_event(
                event_type,
                exec_day,
                amount,
                ticker=ticker,
                qtd=qtd,
                price=preco,
                reason=event_reason,
                settle_date=settle_date,
            )
            if is_duplicate(event):
                warnings.append(f"Operacao {idx} ignorada: duplicada no ledger real.")
                continue
            append_event(event)
            events_created.append(
                {
                    "kind": event.type.value,
                    "id": event.id,
                    "ticker": ticker,
                    "qtd": qtd,
                    "price": preco,
                    "amount": amount,
                }
            )

            if op_type == "VENDA" and liquidacao == LIQUIDACAO_JA_NO_CAIXA:
                settlement = create_event(
                    EventType.SETTLEMENT,
                    exec_day,
                    amount,
                    settle_date=exec_day,
                    ref_id=event.id,
                    reason="Liquidacao same-day via boletim web (liquidacao=JA_NO_CAIXA)",
                )
                if not is_duplicate(settlement):
                    append_event(settlement)
                    events_created.append(
                        {
                            "kind": settlement.type.value,
                            "id": settlement.id,
                            "amount": amount,
                            "ref_id": event.id,
                        }
                    )

            if corretagem > 0:
                fee = create_event(
                    EventType.FEE,
                    exec_day,
                    float(corretagem),
                    ticker=ticker,
                    ref_id=event.id,
                    reason="Corretagem registrada via boletim web",
                )
                if not is_duplicate(fee):
                    append_event(fee)
                    events_created.append(
                        {
                            "kind": fee.type.value,
                            "id": fee.id,
                            "ticker": ticker,
                            "amount": float(corretagem),
                            "ref_id": event.id,
                        }
                    )

            if preco_sombra > 0:
                try:
                    ledger_mod.LEDGER_PATH = shadow_ledger_path
                    shadow_amount = float(qtd * preco_sombra)
                    shadow_reason = "LIVE-REAL-TEST shadow via boletim web"
                    shadow_event = create_event(
                        event_type,
                        exec_day,
                        shadow_amount,
                        ticker=ticker,
                        qtd=qtd,
                        price=preco_sombra,
                        reason=shadow_reason,
                    )
                    if not is_duplicate(shadow_event):
                        append_event(shadow_event)
                        events_created.append(
                            {
                                "kind": f"{shadow_event.type.value}_SHADOW",
                                "id": shadow_event.id,
                                "ticker": ticker,
                                "qtd": qtd,
                                "price": preco_sombra,
                                "amount": shadow_amount,
                            }
                        )
                    else:
                        warnings.append(f"Operacao {idx}: sombra duplicada, ignorada.")
                finally:
                    ledger_mod.LEDGER_PATH = real_ledger_path
    finally:
        ledger_mod.LEDGER_PATH = real_ledger_path

    return {"events_created": events_created, "warnings": warnings}


def close_day(exec_day: date, ledger_dir: Path, caixa_real: float | None = None) -> dict[str, Any]:
    from scripts.friction_ruler import build_friction_report_payload
    from scripts.live_real_cutover import build_boletim_payload

    resolved_ledger_dir = _resolve_ledger_dir(ledger_dir)
    draft = load_draft(exec_day)
    operations = draft.get("operations", [])
    result = apply_draft_to_ledger(exec_day, resolved_ledger_dir, operations)

    caixa_real_value = _safe_float(caixa_real, 0.0) if caixa_real is not None else 0.0
    if caixa_real_value > 0:
        ledger_mod.LEDGER_PATH = resolved_ledger_dir / REAL_LEDGER_NAME
        informed_event = create_event(
            EventType.CAIXA_REAL_INFORMADO,
            exec_day,
            caixa_real_value,
            reason="Caixa Real informado pelo Owner no encerramento do dia via /painel",
        )
        if not is_duplicate(informed_event):
            append_event(informed_event)
            result.setdefault("events_created", []).append(
                {
                    "kind": informed_event.type.value,
                    "id": informed_event.id,
                    "amount": caixa_real_value,
                }
            )

    boletim_payload = build_boletim_payload(exec_day, ledger_dir=resolved_ledger_dir)
    boletim_path = resolved_ledger_dir / f"{exec_day.isoformat()}.json"
    boletim_path.write_text(
        json.dumps(boletim_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    friction_payload = build_friction_report_payload(
        exec_day,
        ledger_dir=resolved_ledger_dir,
        real_dir=ROOT / "data" / "real",
    )
    friction_path = resolved_ledger_dir / f"friction_report_{exec_day.isoformat()}.json"
    friction_path.write_text(
        json.dumps(friction_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    archived = archive_draft(exec_day)
    return {
        "exec_day": exec_day.isoformat(),
        "operations_count": len(operations),
        "events_created": result.get("events_created", []),
        "warnings": result.get("warnings", []),
        "boletim_path": str(boletim_path),
        "friction_report_path": str(friction_path),
        "archived_draft_path": str(archived) if archived else None,
    }


def close_stale_drafts(today: date, ledger_dir: Path) -> list[dict[str, Any]]:
    if not DRAFT_DIR.exists():
        return []

    closed_days: list[dict[str, Any]] = []
    for candidate in sorted(DRAFT_DIR.glob("draft_*.json")):
        if "_encerrado_" in candidate.name:
            continue
        stem = candidate.stem
        if not stem.startswith("draft_"):
            continue
        raw_day = stem.replace("draft_", "", 1)
        try:
            stale_day = date.fromisoformat(raw_day)
        except ValueError:
            continue
        if stale_day >= today:
            continue
        try:
            result = close_day(stale_day, ledger_dir)
            result["auto_closed"] = True
            closed_days.append(result)
        except Exception as exc:  # noqa: BLE001
            closed_days.append(
                {
                    "exec_day": stale_day.isoformat(),
                    "auto_closed": False,
                    "error": str(exc),
                }
            )
    return closed_days


def load_live_view(today: date, ledger_dir: Path) -> dict[str, Any]:
    resolved_ledger_dir = _resolve_ledger_dir(ledger_dir)
    real_ledger_path = resolved_ledger_dir / REAL_LEDGER_NAME

    previous_ledger_path = ledger_mod.LEDGER_PATH
    informed: dict[str, Any] | None = None
    base1_series: list[dict[str, Any]] = []
    corretagem_total = 0.0
    corretagem_dia = 0.0
    capital_uso = 0.0
    try:
        ledger_mod.LEDGER_PATH = real_ledger_path
        cash = compute_cash(today)
        positions = export_snapshot(today)
        operations_book_raw = ledger_mod.build_operations_book(today)
        informed = ledger_mod.latest_informed_cash(today)
        base1_series = ledger_mod.build_real_base1_series(
            today,
            live_snapshot=positions,
            live_cash_free=float(cash.get("cash_free", 0.0)),
            live_cash_accounting=float(cash.get("cash_accounting", 0.0)),
        )
        events_today = [
            ev for ev in ledger_mod.read_all_events() if ev.type == EventType.FEE and ev.exec_date == today
        ]
        corretagem_dia = sum(float(ev.amount) for ev in events_today)
        corretagem_total = ledger_mod.total_fees(today)
        capital_uso = ledger_mod.capital_em_uso(today)
    finally:
        ledger_mod.LEDGER_PATH = previous_ledger_path

    context_path = ROOT / "data" / "ssot" / "contexto_analista_us.json"
    context: dict[str, Any] = {}
    if context_path.exists():
        try:
            context = json.loads(context_path.read_text(encoding="utf-8"))
        except Exception:
            context = {}

    master = context.get("master", {}) if isinstance(context.get("master"), dict) else {}
    forno = context.get("forno", {}) if isinstance(context.get("forno"), dict) else {}
    holdings = context.get("holdings", []) if isinstance(context.get("holdings"), list) else []
    holdings_map = {str(row.get("ticker", "")).upper().strip(): row for row in holdings}
    target_weights = master.get("target_weights", {}) if isinstance(master.get("target_weights"), dict) else {}
    operations_book: dict[str, Any] = {}
    for ticker, row in operations_book_raw.items():
        if not isinstance(row, dict):
            continue
        hold = holdings_map.get(ticker, {})
        close_d1 = _safe_float(hold.get("close_d1"), 0.0)
        qtd_liquida = _safe_float(row.get("qtd_liquida"), 0.0)
        custo_medio = _safe_float(row.get("custo_medio"), 0.0)
        nao_realizado: float | None = None
        if qtd_liquida > 0 and close_d1 > 0:
            nao_realizado = round(qtd_liquida * (close_d1 - custo_medio), 2)
        row_out = dict(row)
        row_out["close_d1"] = close_d1
        row_out["nao_realizado"] = nao_realizado
        operations_book[ticker] = row_out

    positions_enriched: list[dict[str, Any]] = []
    held_set: set[str] = set()
    for row in positions:
        ticker = str(row.get("ticker", "")).upper().strip()
        held_set.add(ticker)
        hold = holdings_map.get(ticker, {})
        positions_enriched.append(
            {
                "ticker": ticker,
                "data_compra": row.get("data_compra", ""),
                "qtd": round(_safe_float(row.get("qtd")), 8),
                "preco_compra": _safe_float(row.get("preco_compra")),
                "close_d1": _safe_float(hold.get("close_d1"), 0.0),
                "heat_pct": _safe_float(hold.get("heat_pct"), 0.0),
            }
        )
    carteira_d1_valor = round(
        sum(_safe_float(row.get("qtd"), 0.0) * _safe_float(row.get("close_d1"), 0.0) for row in positions_enriched),
        4,
    )

    top_operational = master.get("operational_ranking", [])
    if not isinstance(top_operational, list):
        top_operational = []
    top_sorted = sorted(
        top_operational,
        key=lambda row: _safe_int(row.get("m3_rank", row.get("rank", 10**9)), 10**9)
        if isinstance(row, dict)
        else 10**9,
    )[:20]
    sparkline_tickers = sorted(
        {
            str(row.get("ticker", "")).upper().strip()
            for row in top_sorted
            if isinstance(row, dict) and str(row.get("ticker", "")).strip()
        }
        | held_set
    )

    draft = load_draft(today)
    closed_boletim_path = resolved_ledger_dir / f"{today.isoformat()}.json"
    caixa_real_informado = float(informed["amount"]) if informed else None
    caixa_real_informado_date = str(informed["exec_date"]) if informed else None
    friccao_balanco_real = (
        round(float(cash.get("cash_free", 0.0)) - caixa_real_informado, 2)
        if caixa_real_informado is not None
        else None
    )
    return {
        "today": today.isoformat(),
        "market_day": str(context.get("market_day", today.isoformat())),
        "cash_free": float(cash.get("cash_free", 0.0)),
        "cash_accounting": float(cash.get("cash_accounting", 0.0)),
        "caixa_real_informado": caixa_real_informado,
        "caixa_real_informado_date": caixa_real_informado_date,
        "friccao_balanco_real": friccao_balanco_real,
        "positions": positions_enriched,
        "holdings": holdings,
        "held_set": sorted(held_set),
        "sparklines_tickers": sparkline_tickers,
        "top_operational": top_operational,
        "target_weights": target_weights,
        "operations_book": operations_book,
        "base1_series": base1_series,
        "corretagem_dia": round(float(corretagem_dia), 4),
        "corretagem_total": round(float(corretagem_total), 4),
        "capital_em_uso": round(float(capital_uso), 4),
        "carteira_d1_valor": carteira_d1_valor,
        "forno": forno,
        "draft": draft,
        "closed_boletim_exists": closed_boletim_path.exists(),
        "context_available": bool(context),
    }


def _suggested_defensive_sells(view: dict[str, Any]) -> dict[str, Any]:
    forno = view.get("forno", {}) if isinstance(view.get("forno"), dict) else {}
    action = str(forno.get("action") or "HOLD").upper()
    is_rebalance_day = bool(forno.get("is_rebalance_day"))

    holdings = view.get("holdings", []) if isinstance(view.get("holdings"), list) else []
    held_set = {
        str(ticker).upper().strip()
        for ticker in (view.get("held_set", []) if isinstance(view.get("held_set"), list) else [])
        if str(ticker).strip()
    }
    defensive: list[dict[str, Any]] = []
    for row in holdings:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker", "")).upper().strip()
        if not ticker:
            continue
        if held_set and ticker not in held_set:
            continue
        heat = _safe_float(row.get("heat_pct"), 0.0)
        spc = str(row.get("spc_status", "")).upper().strip()
        drawdown = _safe_float(row.get("drawdown_pct"), 0.0)
        reasons: list[str] = []
        if heat <= -8.0:
            reasons.append(f"heat {heat:.1f}%")
        if spc and spc != "ESTAVEL":
            reasons.append(f"SPC {spc}")
        if drawdown <= -15.0:
            reasons.append(f"drawdown {drawdown:.1f}%")
        if reasons:
            defensive.append(
                {
                    "ticker": ticker,
                    "qty": _safe_float(row.get("qty"), 0.0),
                    "close_d1": _safe_float(row.get("close_d1"), 0.0),
                    "heat_pct": heat,
                    "reason": " - ".join(reasons),
                }
            )

    motor_lines: list[str] = []
    if action == "HOLD" and not is_rebalance_day:
        motor_lines.append("HOLD - nenhuma operacao de rebalanceamento exigida hoje.")
    elif is_rebalance_day:
        motor_lines.append("Dia de REBALANCE - executar vendas/compras conforme ranking operacional.")
    else:
        motor_lines.append(f"Acao do forno: {action}")

    return {
        "action": action,
        "is_rebalance_day": is_rebalance_day,
        "next_rebalance": forno.get("next_rebalance_date"),
        "cycles_to_next": forno.get("cycles_to_next_rebalance"),
        "motor_lines": motor_lines,
        "defensive": defensive,
    }


def render_live_html(view: dict[str, Any]) -> str:
    today = str(view.get("today", ""))
    market_day = str(view.get("market_day", today))
    cash_free_raw = _safe_float(view.get("cash_free", 0.0), 0.0)
    cash_accounting_raw = _safe_float(view.get("cash_accounting", 0.0), 0.0)
    cash_free = _fmt_usd(cash_free_raw)
    cash_accounting = _fmt_usd(cash_accounting_raw)

    caixa_real_informado_raw = view.get("caixa_real_informado")
    caixa_real_informado_fmt = (
        _fmt_usd(caixa_real_informado_raw) if caixa_real_informado_raw is not None else "pendente"
    )
    friccao_balanco_real_raw = view.get("friccao_balanco_real")
    friccao_balanco_real_fmt = (
        _fmt_usd(friccao_balanco_real_raw) if friccao_balanco_real_raw is not None else "pendente"
    )

    corretagem_dia_raw = _safe_float(view.get("corretagem_dia", 0.0), 0.0)
    corretagem_total_raw = _safe_float(view.get("corretagem_total", 0.0), 0.0)
    capital_em_uso_raw = _safe_float(view.get("capital_em_uso", 0.0), 0.0)
    carteira_d1_valor_raw = _safe_float(view.get("carteira_d1_valor", 0.0), 0.0)
    total_ativo_raw = carteira_d1_valor_raw + cash_free_raw + cash_accounting_raw
    total_bruto_raw = total_ativo_raw
    has_caixa_real = caixa_real_informado_raw is not None
    caixa_real_atual = _safe_float(caixa_real_informado_raw, 0.0) if has_caixa_real else 0.0
    friccao_operacional_raw = cash_free_raw - caixa_real_atual if has_caixa_real else 0.0
    friccao_total_raw = corretagem_total_raw + friccao_operacional_raw
    nav_raw = total_ativo_raw - friccao_operacional_raw
    resultado_raw = nav_raw - capital_em_uso_raw
    rent_raw = (resultado_raw / capital_em_uso_raw * 100.0) if capital_em_uso_raw > 0 else 0.0
    delta_ajustado_liquidacao_raw = (
        (cash_free_raw + cash_accounting_raw) - caixa_real_atual
        if has_caixa_real and cash_accounting_raw > 0
        else None
    )

    delta_fmt = "pendente"
    delta_cls = "flat"
    fric_op_fmt = "pendente"
    fric_op_cls = "flat"
    if has_caixa_real:
        delta_fmt, delta_cls = _fmt_signed_usd(friccao_operacional_raw)
        fric_op_fmt, fric_op_cls = _fmt_signed_usd(friccao_operacional_raw)
    delta_liq_fmt = "n/a"
    delta_liq_cls = "flat"
    if delta_ajustado_liquidacao_raw is not None:
        delta_liq_fmt, delta_liq_cls = _fmt_signed_usd(delta_ajustado_liquidacao_raw)

    fric_total_fmt, fric_total_cls = _fmt_signed_usd(friccao_total_raw)
    resultado_fmt, resultado_cls = _fmt_signed_usd(resultado_raw)
    rent_fmt, rent_cls = _fmt_signed_pct(rent_raw)

    forno = view.get("forno", {}) if isinstance(view.get("forno"), dict) else {}
    is_rebalance = forno.get("is_rebalance_day")
    cycles = forno.get("cycles_to_next_rebalance")
    next_rebalance = forno.get("next_rebalance_date")
    is_d1_pos_rebalance = bool(forno.get("is_d1_pos_rebalance", False))
    suggestions = _suggested_defensive_sells(view)

    positions = view.get("positions", []) if isinstance(view.get("positions"), list) else []
    operations_book = view.get("operations_book", {}) if isinstance(view.get("operations_book"), dict) else {}
    holdings = view.get("holdings", []) if isinstance(view.get("holdings"), list) else []
    holdings_map: dict[str, dict[str, Any]] = {}
    for row in holdings:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker", "")).upper().strip()
        if ticker:
            holdings_map[ticker] = row

    held_result: dict[str, float] = {}
    pos_rows: list[str] = []
    if positions:
        for row in positions:
            ticker = str(row.get("ticker", "")).upper().strip()
            qty = _safe_float(row.get("qtd"), 0.0)
            close_d1 = _safe_float(row.get("close_d1"), 0.0)
            preco_compra = _safe_float(row.get("preco_compra"), 0.0)
            heat_pct = _safe_float(row.get("heat_pct"), 0.0)
            book_row = operations_book.get(ticker, {})
            if not isinstance(book_row, dict):
                book_row = {}
            nao_realizado_raw = book_row.get("nao_realizado")
            if nao_realizado_raw is None and qty > 0 and close_d1 > 0:
                nao_realizado_raw = round(qty * (close_d1 - preco_compra), 2)

            if nao_realizado_raw is not None:
                held_result[ticker] = _safe_float(nao_realizado_raw, 0.0)
            else:
                held_result[ticker] = heat_pct

            nao_realizado_txt = _fmt_usd(nao_realizado_raw) if nao_realizado_raw is not None else "-"
            nao_realizado_cls = _sign_class(nao_realizado_raw if nao_realizado_raw is not None else 0.0)
            heat_class = _sign_class(heat_pct)
            pos_rows.append(
                "<tr>"
                f"<td>{html.escape(ticker)}</td>"
                f"<td>{html.escape(str(row.get('data_compra', '')))}</td>"
                f"<td style='text-align:right'>{_fmt_qtd(qty)}</td>"
                f"<td style='text-align:right'>{_fmt_usd(preco_compra)}</td>"
                f"<td style='text-align:right'>{_fmt_usd(close_d1) if close_d1 > 0 else '-'}</td>"
                f"<td class='{heat_class}' style='text-align:right'>{_fmt_pct(heat_pct)}</td>"
                f"<td class='{nao_realizado_cls}' style='text-align:right'>{nao_realizado_txt}</td>"
                "</tr>"
            )
    else:
        pos_rows.append("<tr><td colspan='7'>Nenhuma posicao registrada no ledger real.</td></tr>")

    book_rows: list[str] = []

    def _format_ops_column(rows: Any) -> str:
        if not isinstance(rows, list) or not rows:
            return "-"
        parts: list[str] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            op_date = html.escape(str(item.get("date", "")))
            op_qtd = _fmt_qtd(item.get("qtd"))
            op_preco = _fmt_usd(item.get("preco", 0.0))
            parts.append(f"{op_date} {op_qtd}@{op_preco}")
        return "; ".join(parts) if parts else "-"

    if operations_book:
        for ticker in sorted(operations_book.keys()):
            row = operations_book.get(ticker, {})
            if not isinstance(row, dict):
                continue
            nao_realizado_raw = row.get("nao_realizado")
            nao_realizado_fmt = _fmt_usd(nao_realizado_raw) if nao_realizado_raw is not None else "-"
            nao_realizado_cls = _sign_class(nao_realizado_raw if nao_realizado_raw is not None else 0.0)
            book_rows.append(
                "<tr>"
                f"<td>{html.escape(ticker)}</td>"
                f"<td>{_format_ops_column(row.get('compras', []))}</td>"
                f"<td>{_format_ops_column(row.get('vendas', []))}</td>"
                f"<td style='text-align:right'>{_fmt_qtd(row.get('qtd_liquida', 0))}</td>"
                f"<td style='text-align:right'>{_fmt_usd(row.get('custo_medio', 0.0))}</td>"
                f"<td style='text-align:right'>{_fmt_usd(row.get('close_d1', 0.0))}</td>"
                f"<td style='text-align:right'>{_fmt_usd(row.get('realizado', 0.0))}</td>"
                f"<td class='{nao_realizado_cls}' style='text-align:right'>{nao_realizado_fmt}</td>"
                "</tr>"
            )
    if not book_rows:
        book_rows.append("<tr><td colspan='8'>Sem operacoes registradas por ativo.</td></tr>")

    held_set = {
        str(ticker).upper().strip()
        for ticker in (view.get("held_set", []) if isinstance(view.get("held_set"), list) else [])
        if str(ticker).strip()
    }
    ranking = view.get("top_operational", []) if isinstance(view.get("top_operational"), list) else []

    def _rank_key(item: dict[str, Any]) -> int:
        try:
            return int(item.get("m3_rank"))
        except Exception:
            try:
                return int(item.get("rank"))
            except Exception:
                return 10**9

    top_sorted = sorted((row for row in ranking if isinstance(row, dict)), key=_rank_key)[:20]
    sparklines_tickers = view.get("sparklines_tickers")
    if not isinstance(sparklines_tickers, list):
        sparklines_tickers = [str(row.get("ticker", "")).upper().strip() for row in top_sorted]
    today_date = date.today()
    try:
        today_date = date.fromisoformat(today)
    except Exception:
        pass
    spark_map = _load_sparklines(
        sorted({str(tk).upper().strip() for tk in sparklines_tickers if str(tk).strip()}),
        today_date,
    )

    top_rows: list[str] = []
    if top_sorted:
        for row in top_sorted:
            ticker = str(row.get("ticker", "")).upper().strip()
            close_d1 = _safe_float(row.get("close_d1"), 0.0)
            if close_d1 <= 0:
                hold = holdings_map.get(ticker, {})
                close_d1 = _safe_float(hold.get("close_d1"), 0.0) if isinstance(hold, dict) else 0.0
            if ticker in held_set:
                res = held_result.get(ticker, 0.0)
                if res > 1e-9:
                    indicator = "held-pos"
                elif res < -1e-9:
                    indicator = "held-neg"
                else:
                    indicator = "held-flat"
            else:
                indicator = "cand"
            top_rows.append(
                "<tr>"
                f"<td><span class='{indicator}'></span>{html.escape(ticker)}</td>"
                f"<td style='text-align:right'>{_rank_key(row)}</td>"
                f"<td style='text-align:right'>{_safe_float(row.get('score_m3', 0.0)):.4f}</td>"
                f"<td style='text-align:right'>{_fmt_usd(close_d1) if close_d1 > 0 else '-'}</td>"
                f"<td class='spark-cell'>{spark_map.get(ticker, '<span class=\"flat\">-</span>')}</td>"
                "</tr>"
            )
    else:
        top_rows.append("<tr><td colspan='5'>Top-20 nao disponivel - rode pipeline/analise_us.py.</td></tr>")

    draft = view.get("draft", {}) if isinstance(view.get("draft"), dict) else {}
    operations = draft.get("operations", []) if isinstance(draft.get("operations"), list) else []
    draft_rows: list[str] = []
    for op in operations:
        op_id = html.escape(str(op.get("id", "")))
        op_type = str(op.get("type", "")).upper().strip()
        liquidacao_txt = "-"
        if op_type == "VENDA":
            liquidacao_txt = html.escape(str(op.get("liquidacao") or LIQUIDACAO_JA_NO_CAIXA))
        draft_rows.append(
            "<tr>"
            f"<td>{html.escape(op_type)}</td>"
            f"<td>{html.escape(str(op.get('ticker', '')))}</td>"
            f"<td style='text-align:right'>{_fmt_qtd(op.get('qtd'))}</td>"
            f"<td style='text-align:right'>{_fmt_usd(op.get('preco', 0.0))}</td>"
            f"<td style='text-align:right'>{_fmt_usd(op.get('corretagem', 0.0))}</td>"
            f"<td style='text-align:right'>{_fmt_usd(op.get('preco_sombra', 0.0)) if _safe_float(op.get('preco_sombra', 0.0)) > 0 else '-'}</td>"
            f"<td>{liquidacao_txt}</td>"
            "<td>"
            "<form method='POST' action='/painel/rascunho/remover'>"
            f"<input type='hidden' name='exec_day' value='{html.escape(today)}' />"
            f"<input type='hidden' name='row_id' value='{op_id}' />"
            "<button type='submit'>Remover</button>"
            "</form>"
            "</td>"
            "</tr>"
        )
    if not draft_rows:
        draft_rows.append("<tr><td colspan='8'>Nenhuma operacao em rascunho.</td></tr>")

    base1_series = view.get("base1_series", []) if isinstance(view.get("base1_series"), list) else []
    base_chart = _base1_chart_svg(base1_series)
    base_now = _safe_float(base1_series[-1].get("base1"), 0.0) if base1_series else 0.0
    expect_now = _safe_float(base1_series[-1].get("cagr_expect"), 0.0) if base1_series else 0.0
    base_delta_txt, base_delta_cls = _fmt_signed_pct((base_now - 1.0) * 100.0)
    vs_cagr_txt, vs_cagr_cls = _fmt_signed_pct((base_now - expect_now) * 100.0)
    cota_preco_raw = base_now * 100.0 if base_now > 0 else 0.0
    cotas_estimadas = (total_ativo_raw / cota_preco_raw) if cota_preco_raw > 0 else 0.0

    closed_note = (
        "<p class='pill'>Dia encerrado: boletim de fechamento ja existe para hoje.</p>"
        if bool(view.get("closed_boletim_exists"))
        else ""
    )
    caixa_real_value_attr = (
        f"value='{_safe_float(caixa_real_informado_raw):.2f}'" if caixa_real_informado_raw is not None else ""
    )
    caixa_real_bridge = _fmt_usd(caixa_real_informado_raw) if has_caixa_real else "pendente"

    defensive_rows: list[str] = []
    for row in suggestions.get("defensive", []):
        if not isinstance(row, dict):
            continue
        heat = _safe_float(row.get("heat_pct", 0.0), 0.0)
        heat_class = _sign_class(heat)
        defensive_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('ticker', '')))}</td>"
            f"<td style='text-align:right'>{_fmt_qtd(row.get('qty', 0.0))}</td>"
            f"<td style='text-align:right'>{_fmt_usd(row.get('close_d1', 0.0)) if _safe_float(row.get('close_d1', 0.0), 0.0) > 0 else '-'}</td>"
            f"<td class='{heat_class}' style='text-align:right'>{_fmt_pct(heat)}</td>"
            f"<td>{html.escape(str(row.get('reason', '')))}</td>"
            "</tr>"
        )
    if not defensive_rows:
        defensive_rows.append(
            "<tr><td colspan='5'><span class='flat'>Nenhuma venda defensiva sugerida (heat/SPC/drawdown dentro dos limites).</span></td></tr>"
        )
    motor_html = "".join(f"<li>{html.escape(str(line))}</li>" for line in suggestions.get("motor_lines", []))

    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Boletim LIVE-REAL-TEST</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet" />
  <style>
    :root {{ --ink:#0A3161; --surface:#0F3D73; --surface-2:#134A86; --hair:#2A5A96; --hair-soft:#1C4A7A; --bone:#E9E5D8; --muted:#A8B8CC; --muted-2:#7A91AB; --teal:#52B9A3; --teal-dim:#2F5F57; --amber:#E2A959; --amber-dim:#6B552F; --green:#4FBE7B; --red:#E0664F; }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; padding:20px; background:var(--ink); color:var(--bone); font-family:"Space Grotesk",sans-serif; line-height:1.4; }}
    .mono {{ font-family:"IBM Plex Mono",monospace; }}
    .topbar {{ display:flex; justify-content:space-between; gap:16px; flex-wrap:wrap; border-bottom:1px solid var(--hair); padding-bottom:12px; margin-bottom:16px; }}
    .eyebrow {{ font-family:"IBM Plex Mono",monospace; color:var(--muted); font-size:11px; letter-spacing:.12em; text-transform:uppercase; }}
    h1,h2,h3 {{ margin:0 0 8px 0; }}
    .pill {{ color:var(--amber); font-size:12px; }}
    .meta {{ display:flex; gap:14px; flex-wrap:wrap; text-align:right; }}
    .meta .k {{ color:var(--muted); font-size:10px; letter-spacing:.1em; text-transform:uppercase; font-family:"IBM Plex Mono",monospace; }}
    .meta .v {{ font-family:"IBM Plex Mono",monospace; margin-top:2px; }}
    .sec-label {{ display:flex; align-items:center; gap:8px; color:var(--muted); font-size:10px; text-transform:uppercase; letter-spacing:.14em; font-family:"IBM Plex Mono",monospace; margin:14px 0 8px; }}
    .sec-label .idx {{ color:var(--muted-2); }}
    .sec-label .tag {{ margin-left:auto; border:1px solid var(--hair); border-radius:99px; padding:2px 8px; font-size:9px; letter-spacing:.08em; color:var(--muted-2); }}
    .sec-label .tag.governs {{ border-color:var(--teal-dim); color:var(--teal); }}
    .card {{ background:var(--surface); border:1px solid var(--hair); border-radius:10px; padding:14px; margin-bottom:12px; }}
    .muted {{ color:var(--muted); font-size:12px; }}
    table {{ width:100%; border-collapse:collapse; font-family:"IBM Plex Mono",monospace; font-size:12px; }}
    th,td {{ border-bottom:1px solid var(--hair-soft); padding:6px; text-align:left; vertical-align:middle; }}
    tr:last-child td {{ border-bottom:none; }}
    .gpos {{ color:var(--green); font-weight:700; }}
    .gneg {{ color:var(--red); font-weight:700; }}
    .flat {{ color:var(--muted); }}
    .spark {{ display:block; }}
    .spark-cell {{ text-align:center; min-width:160px; }}
    .chart-head {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:10px; margin-bottom:8px; }}
    .chart-head .k {{ font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; font-family:"IBM Plex Mono",monospace; }}
    .chart-head .v {{ font-size:19px; font-family:"IBM Plex Mono",monospace; }}
    .chart-head .sub {{ font-size:12px; color:var(--muted-2); }}
    .chart-wrap {{ width:100%; min-height:220px; }}
    .hero {{ border-left:4px solid var(--teal); padding-left:12px; }}
    .hero-word {{ font-size:30px; font-weight:700; }}
    .hero-word .dot {{ display:inline-block; width:10px; height:10px; border-radius:50%; background:var(--teal); margin-right:8px; }}
    .hero-sub {{ color:var(--muted); font-family:"IBM Plex Mono",monospace; font-size:11px; }}
    .facts {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:10px; margin-top:10px; }}
    .fact {{ background:var(--surface-2); border:1px solid var(--hair); border-radius:8px; padding:10px; }}
    .fact .fk {{ font-family:"IBM Plex Mono",monospace; color:var(--muted); font-size:9px; text-transform:uppercase; letter-spacing:.1em; }}
    .fact .fv {{ font-family:"IBM Plex Mono",monospace; font-size:18px; margin-top:4px; }}
    .fact .ok {{ color:var(--teal); }}
    ul.motor {{ margin:8px 0 0 18px; font-family:"IBM Plex Mono",monospace; font-size:12px; }}
    .held, .held-pos, .held-neg, .held-flat, .cand {{ display:inline-block; width:7px; height:7px; border-radius:50%; margin-right:6px; vertical-align:middle; }}
    .held {{ background:var(--teal); }}
    .held-pos {{ background:var(--green); }}
    .held-neg {{ background:var(--red); }}
    .held-flat {{ background:var(--muted-2); }}
    .cand {{ background:transparent; border:1.5px solid var(--muted-2); }}
    .op-gate {{ background:var(--surface-2); border:1px solid var(--teal-dim); border-radius:8px; padding:12px; margin-bottom:12px; }}
    .op-gate .gate-k {{ color:var(--teal); font-family:"IBM Plex Mono",monospace; font-size:10px; text-transform:uppercase; letter-spacing:.1em; }}
    .op-gate .gate-v {{ font-size:22px; margin:5px 0; }}
    .cash-adj {{ max-width:220px; padding:8px; border-radius:6px; border:1px solid var(--teal-dim); background:#061F3F; color:var(--bone); font-family:"IBM Plex Mono",monospace; }}
    .op-rows {{ display:flex; flex-direction:column; gap:8px; margin-top:10px; }}
    .op-row {{ display:grid; grid-template-columns:110px 1fr 1fr 1fr 1fr 100px; gap:8px; align-items:end; background:var(--surface-2); border:1px solid var(--hair); border-radius:8px; padding:10px; }}
    .op-row.blank {{ opacity:.85; border-style:dashed; }}
    .op-row label {{ display:flex; flex-direction:column; gap:4px; font-family:"IBM Plex Mono",monospace; font-size:9px; letter-spacing:.08em; text-transform:uppercase; color:var(--muted); }}
    .op-field {{ width:100%; padding:8px; border-radius:6px; border:1px solid var(--hair); background:#061F3F; color:var(--bone); font-family:"IBM Plex Mono",monospace; }}
    .wf .row {{ display:flex; justify-content:space-between; gap:10px; border-bottom:1px dashed var(--hair-soft); padding:8px 0; }}
    .wf .row:last-child {{ border-bottom:none; }}
    .wf .l {{ color:var(--muted); }}
    .wf .v {{ font-family:"IBM Plex Mono",monospace; }}
    .wf .row .op {{ display:inline-block; width:16px; color:var(--muted-2); font-family:"IBM Plex Mono",monospace; }}
    .wf .row.sub {{ border-top:1px solid var(--hair); margin-top:4px; padding-top:10px; border-bottom:none; }}
    .wf .row.sub .l, .wf .row.sub .v {{ color:var(--bone); font-weight:700; }}
    .wf .row.fric .l, .wf .row.fric .v {{ color:var(--amber); }}
    .wf .row.nav {{ background:rgba(82,185,163,.08); border:1px solid var(--teal-dim); border-radius:8px; padding:10px; margin-top:6px; }}
    .wf .row.nav .l, .wf .row.nav .v {{ color:var(--teal); font-weight:700; }}
    .wf .row.res .v {{ font-weight:700; }}
    .sources {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
    .src {{ background:var(--surface-2); border:1px solid var(--hair); border-radius:8px; padding:10px; }}
    .src.manual {{ border-color:var(--teal-dim); }}
    .src .sk {{ font-family:"IBM Plex Mono",monospace; font-size:9px; color:var(--muted); letter-spacing:.1em; text-transform:uppercase; }}
    .src .sv {{ margin-top:6px; font-family:"IBM Plex Mono",monospace; font-size:12px; }}
    .recon {{ display:grid; grid-template-columns:1fr auto 1fr; gap:10px; margin-top:10px; align-items:stretch; }}
    .rcol {{ background:var(--surface-2); border:1px solid var(--hair); border-radius:8px; padding:10px; text-align:center; }}
    .rcol.real {{ border-color:var(--teal-dim); }}
    .rcol .rk {{ color:var(--muted); font-family:"IBM Plex Mono",monospace; font-size:9px; text-transform:uppercase; letter-spacing:.1em; }}
    .rcol .rv {{ font-family:"IBM Plex Mono",monospace; font-size:20px; margin-top:6px; }}
    .rgap {{ display:flex; flex-direction:column; align-items:center; justify-content:center; min-width:90px; }}
    .rgap .gk {{ font-family:"IBM Plex Mono",monospace; font-size:9px; text-transform:uppercase; color:var(--muted-2); }}
    .rgap .gv {{ font-family:"IBM Plex Mono",monospace; font-size:22px; color:var(--amber); }}
    form {{ margin:0; }}
    .form-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:8px; }}
    input,select,button {{ width:100%; padding:8px; border-radius:6px; border:1px solid var(--hair); background:var(--surface-2); color:var(--bone); }}
    button {{ cursor:pointer; font-weight:600; }}
    @media (max-width:900px) {{ .op-row {{ grid-template-columns:1fr 1fr; }} .recon {{ grid-template-columns:1fr; }} .sources {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="topbar">
    <div class="brand">
      <div class="eyebrow">Boletim LIVE-REAL-TEST</div>
      <h1>Fabrica US / C4</h1>
      {closed_note}
    </div>
    <div class="meta">
      <div><div class="k">market_day</div><div class="v">{html.escape(market_day)}</div></div>
      <div><div class="k">exec_day</div><div class="v">{html.escape(today)}</div></div>
      <div><div class="k">status d+1</div><div class="v">is_d1_pos_rebalance={html.escape(str(is_d1_pos_rebalance))}</div></div>
    </div>
  </div>

  <div class="sec-label"><span class="idx">01</span> Evolucao - Base 1 / cota vs CAGR motor <span class="tag">real - desde aporte</span></div>
  <div class="card">
    <div class="chart-head">
      <div><div class="k">Base 1 atual</div><div class="v mono">{base_now:.4f}</div><div class="sub {base_delta_cls}">{base_delta_txt} vs ancora</div></div>
      <div><div class="k">CAGR esperado</div><div class="v mono">{expect_now:.4f}</div><div class="sub">composicao diaria em 252 pregoes</div></div>
      <div><div class="k">Delta Base1 - CAGR</div><div class="v mono {vs_cagr_cls}">{vs_cagr_txt}</div><div class="sub">pontos de base</div></div>
    </div>
    <div class="chart-wrap">{base_chart}</div>
  </div>

  <div class="sec-label"><span class="idx">02</span> Acao do dia <span class="tag">motor + defensivas</span></div>
  <div class="card hero">
    <div class="hero-word"><span class="dot"></span>{html.escape(str(suggestions.get("action", "HOLD")))}</div>
    <div class="hero-sub">proximo rebalance: {html.escape(str(suggestions.get("next_rebalance")))} - ciclos: {html.escape(str(suggestions.get("cycles_to_next")))}</div>
    <div class="facts">
      <div class="fact">
        <div class="fk">Rebalanceamento hoje</div>
        <div class="fv {'ok' if not suggestions.get('is_rebalance_day') else ''}">{'SIM' if suggestions.get('is_rebalance_day') else 'NAO'}</div>
      </div>
      <div class="fact">
        <div class="fk">Vendas defensivas</div>
        <div class="fv {'ok' if not suggestions.get('defensive') else ''}">{len(suggestions.get('defensive', [])) if suggestions.get('defensive') else 'Nenhuma'}</div>
      </div>
      <div class="fact">
        <div class="fk">Caixa Livre de Balanco</div>
        <div class="fv mono">{cash_free}</div>
      </div>
    </div>
    <p class="muted" style="margin-top:12px">Operacoes sugeridas pelo motor</p>
    <ul class="motor">{motor_html}</ul>
    <p class="muted" style="margin-top:12px">Vendas defensivas</p>
    <table>
      <tr><th>Ticker</th><th style="text-align:right">Qtd</th><th style="text-align:right">Fech. D-1</th><th style="text-align:right">Heat</th><th>Motivo</th></tr>
      {''.join(defensive_rows)}
    </table>
  </div>

  <div class="sec-label"><span class="idx">03</span> Carteira real <span class="tag">posicoes - heat - nao realizado</span></div>
  <div class="card">
    <table>
      <tr><th>Ticker</th><th>Data Compra</th><th style="text-align:right">Qtd</th><th style="text-align:right">Preco Compra</th><th style="text-align:right">Fech. D-1</th><th style="text-align:right">Heat %</th><th style="text-align:right">Nao Realizado</th></tr>
      {''.join(pos_rows)}
    </table>
  </div>

  <div class="sec-label"><span class="idx">04</span> Livro de operacoes <span class="tag">apenas operacoes acontecidas</span></div>
  <div class="card">
    <table>
      <tr><th>Ticker</th><th>Compras</th><th>Vendas</th><th style="text-align:right">Qtd Liquida</th><th style="text-align:right">Custo Medio</th><th style="text-align:right">Fech. D-1</th><th style="text-align:right">Resultado Realizado</th><th style="text-align:right">Resultado Nao Realizado</th></tr>
      {''.join(book_rows)}
    </table>
  </div>

  <div class="sec-label"><span class="idx">05</span> Top-20 operacional <span class="tag">sparkline 62 pregoes</span></div>
  <div class="card">
    <p class="muted" style="margin-bottom:8px">
      Bolinha:
      <span class="held-pos"></span> em carteira positivo -
      <span class="held-neg"></span> em carteira negativo -
      <span class="held-flat"></span> em carteira neutro -
      <span class="cand"></span> fora da carteira
    </p>
    <table>
      <tr><th>Ticker</th><th style="text-align:right">M3 Rank</th><th style="text-align:right">Score M3</th><th style="text-align:right">Fech. D-1</th><th style="text-align:right">62d</th></tr>
      {''.join(top_rows)}
    </table>
  </div>

  <div class="sec-label"><span class="idx">06</span> Operacoes sugeridas pelo Analista <span class="tag">owner preenche - local</span></div>
  <div class="card">
    <div class="op-gate">
      <div class="gate-k">Caixa Livre de Balanco</div>
      <div class="gate-v mono">{cash_free}</div>
      <p class="muted">Ajuste local para analise: nao grava no ledger.</p>
      <div class="gate-k" style="margin-top:10px">Caixa Livre Real (espelho local)</div>
      <input class="cash-adj" id="analystCaixaReal" type="number" step="0.01" placeholder="saldo no app BTG" />
      <p class="muted" style="margin-top:8px">Friccao: Delta = Balanco - Real.</p>
    </div>
    <p class="muted">Sugestao corrente do motor</p>
    <ul class="motor">{motor_html}</ul>
    <table style="margin-top:8px">
      <tr><th>Ticker</th><th style="text-align:right">Qtd</th><th style="text-align:right">Fech. D-1</th><th style="text-align:right">Heat</th><th>Motivo</th></tr>
      {''.join(defensive_rows)}
    </table>
    <p class="muted" style="margin-top:10px">Linhas locais de sugestao (sem persistencia):</p>
    <div class="op-rows" id="analystOpsRows"></div>
  </div>

  <div class="sec-label"><span class="idx">07</span> Balancete simplificado <span class="tag governs">caixa + ativo + cota</span></div>
  <div class="card">
    <div class="wf">
      <div class="row"><div class="l"><span class="op">&nbsp;</span>Carteira (Fech. D-1)</div><div class="v" id="bridgeCarteira">{_fmt_usd(carteira_d1_valor_raw)}</div></div>
      <div class="row"><div class="l"><span class="op">+</span>Caixa Livre de Balanco</div><div class="v" id="bridgeCaixaBalanco">{_fmt_usd(cash_free_raw)}</div></div>
      <div class="row"><div class="l"><span class="op">+</span>Caixa Contabil (em liquidacao)</div><div class="v" id="bridgeCaixaContabil">{_fmt_usd(cash_accounting_raw)}</div></div>
      <div class="row sub"><div class="l">= Total do Ativo</div><div class="v" id="bridgeTotalBruto">{_fmt_usd(total_ativo_raw)}</div></div>
      <div class="row"><div class="l">Capital em uso</div><div class="v" id="bridgeCapital">{_fmt_usd(capital_em_uso_raw)}</div></div>
      <div class="row"><div class="l">Preco da cota (Base 1 x 100)</div><div class="v mono">{_fmt_usd(cota_preco_raw)}</div></div>
      <div class="row"><div class="l">Cotas estimadas</div><div class="v mono">{cotas_estimadas:,.4f}</div></div>
      <div class="row"><div class="l">Base 1 atual</div><div class="v mono">{base_now:.4f}</div></div>
      <div class="row nav"><div class="l">NAV reconciliado (Total - Delta Balanco-Real)</div><div class="v" id="bridgeNav">{_fmt_usd(nav_raw)}</div></div>
    </div>
    <h3 style="margin-top:12px">Encerramento definitivo do dia</h3>
    <form method="POST" action="/painel/encerrar">
      <input type="hidden" name="exec_day" value="{html.escape(today)}" />
      <label>Caixa Livre Real BTG (saldo do app, opcional)<input id="caixaRealInput" type="number" name="caixa_real" min="0.00" step="0.01" placeholder="ex: 950.00" {caixa_real_value_attr} /></label>
      <label style="display:flex; align-items:center; gap:8px; margin-top:8px"><input type="checkbox" name="confirmar" value="sim" required style="width:auto" /> Confirmo encerramento definitivo.</label>
      <button type="submit" style="margin-top:8px">Encerrar o Dia</button>
    </form>
    <p class="muted">Caixa Real permanece observacional: nao altera compute_cash; serve para reconciliacao do fechamento.</p>
  </div>

  <div class="sec-label"><span class="idx">08</span> DFC simplificado + reconciliacao <span class="tag">balanco vs BTG</span></div>
  <div class="card">
    <div class="sources">
      <div class="src">
        <div class="sk">Caixa Livre de Balanco</div>
        <div class="sv">Derivado do ledger real: APORTE/DIVIDENDO/SETTLEMENT - RETIRADA/BUY/FEE.</div>
      </div>
      <div class="src manual">
        <div class="sk">Caixa Livre Real BTG</div>
        <div class="sv">Observacional, informado pelo Owner no encerramento.</div>
      </div>
    </div>
    <div class="wf" style="margin-top:10px">
      <div class="row"><div class="l">Caixa Livre de Balanco</div><div class="v" id="reconCaixaBalanco">{_fmt_usd(cash_free_raw)}</div></div>
      <div class="row"><div class="l">Caixa Livre Real BTG</div><div class="v" id="bridgeCaixaReal">{caixa_real_bridge}</div></div>
      <div class="row"><div class="l">Delta Livre - Real</div><div class="v {delta_cls}" id="bridgeDelta">{delta_fmt}</div></div>
      <div class="row"><div class="l">Caixa Contabil (em liquidacao)</div><div class="v">{_fmt_usd(cash_accounting_raw)}</div></div>
      <div class="row"><div class="l">Delta ajustado por liquidacao</div><div class="v {delta_liq_cls}" id="bridgeDeltaAjustadoLiquidacao">{delta_liq_fmt}</div></div>
      <div class="row"><div class="l">Corretagem do dia</div><div class="v">{_fmt_usd(corretagem_dia_raw)}</div></div>
      <div class="row"><div class="l">Corretagem acumulada</div><div class="v" id="bridgeCorretagem">{_fmt_usd(corretagem_total_raw)}</div></div>
      <div class="row fric"><div class="l">Friccao operacional (Livre - Real)</div><div class="v {fric_op_cls}" id="bridgeFriccaoOperacional">{fric_op_fmt}</div></div>
      <div class="row fric"><div class="l">Friccao total</div><div class="v {fric_total_cls}" id="bridgeFriccaoTotal">{fric_total_fmt}</div></div>
      <div class="row res"><div class="l">Resultado acumulado</div><div class="v {resultado_cls}" id="bridgeResultado">{resultado_fmt}</div></div>
      <div class="row res"><div class="l">Rentabilidade acumulada</div><div class="v {rent_cls}" id="bridgeRent">{rent_fmt}</div></div>
    </div>
    <div class="recon" style="margin-top:12px">
      <div class="rcol"><div class="rk">Caixa Livre de Balanco</div><div class="rv" id="reconCaixaBalancoMirror">{_fmt_usd(cash_free_raw)}</div></div>
      <div class="rgap"><div class="gk">Delta</div><div class="gv {delta_cls}" id="reconDelta">{delta_fmt}</div></div>
      <div class="rcol real"><div class="rk">Caixa Livre Real BTG</div><div class="rv" id="reconCaixaRealMirror">{caixa_real_bridge}</div></div>
    </div>
  </div>

  <div class="card">
    <h2>Rascunho operacional (persistente)</h2>
    <table>
      <tr><th>Tipo</th><th>Ticker</th><th style="text-align:right">Qtd</th><th style="text-align:right">Preco real</th><th style="text-align:right">Corretagem</th><th style="text-align:right">Preco-sombra</th><th>Liquidacao</th><th>Acoes</th></tr>
      {''.join(draft_rows)}
    </table>
    <p class="muted">Salvar rascunho nao modifica o ledger definitivo.</p>
  </div>

  <div class="card">
    <h2>Adicionar operacao</h2>
    <form method="POST" action="/painel/rascunho">
      <div class="form-grid">
        <input type="hidden" name="exec_day" value="{html.escape(today)}" />
        <label>Tipo
          <select name="tipo">
            <option value="COMPRA">COMPRA</option>
            <option value="VENDA">VENDA</option>
          </select>
        </label>
        <label>Ticker<input type="text" name="ticker" required /></label>
        <label>Valor investido US$ (opcional, prioridade sobre Quantidade)<input type="number" name="valor_investido" min="0.00" step="0.01" placeholder="ex: 1000.00" /></label>
        <label>Quantidade (preencha se NAO usar Valor investido)<input type="number" name="qtd" min="0" step="0.00000001" /></label>
        <label>Preco real<input type="number" name="preco" min="0.01" step="0.01" required /></label>
        <label>Corretagem<input type="number" name="corretagem" min="0.00" step="0.01" value="2.50" /></label>
        <label>Preco-sombra (opcional)<input type="number" name="preco_sombra" min="0.00" step="0.01" placeholder="auto-lookup se vazio" /></label>
        <label>Liquidacao da venda
          <select name="liquidacao">
            <option value="JA_NO_CAIXA">JA_NO_CAIXA</option>
            <option value="EM_LIQUIDACAO">EM_LIQUIDACAO</option>
          </select>
        </label>
      </div>
      <p class="muted">Se preco-sombra ficar vazio, o sistema tenta auto-lookup no SSOT operacional. Para VENDA, o campo Liquidacao e obrigatorio.</p>
      <button type="submit">Salvar rascunho</button>
    </form>
  </div>

  <div class="card">
    <h2>Mapa de custos</h2>
    <table>
      <tr><th>Componente</th><th>Onde vive</th><th>Nivel</th></tr>
      <tr><td>Corretagem (taxa)</td><td>Evento FEE no ledger real (impacta Caixa Livre)</td><td>operacional / caixa</td></tr>
      <tr><td>Friccao acumulada</td><td>friction_report_&lt;dia&gt;.json</td><td>diagnostico</td></tr>
      <tr><td>Slippage</td><td>Comparacao real vs sombra</td><td>desempenho</td></tr>
    </table>
  </div>

  <p class="muted">Fallback disponivel via scripts/atalhos: USA_REGISTRAR_ORDEM e USA_ENCERRAR_DIA. Base1/CAGR, sparklines 62d e ponte de friccao rodam localmente sem dependencias externas.</p>

  <script>
  (function() {{
    const caixaBalanco = {cash_free_raw:.8f};
    const caixaContabil = {cash_accounting_raw:.8f};
    const carteira = {carteira_d1_valor_raw:.8f};
    const corretagem = {corretagem_total_raw:.8f};
    const capitalUso = {capital_em_uso_raw:.8f};
    const totalBruto = carteira + caixaBalanco + caixaContabil;

    const input = document.getElementById("caixaRealInput");
    const analystInput = document.getElementById("analystCaixaReal");
    const opsRows = document.getElementById("analystOpsRows");

    function fmtUsd(value) {{
      const v = Number(value) || 0;
      return "$ " + Math.abs(v).toLocaleString("en-US", {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
    }}

    function fmtSignedUsd(value) {{
      const v = Number(value) || 0;
      if (Math.abs(v) < 1e-9) return {{ txt: "$ 0.00", cls: "flat" }};
      const abs = Math.abs(v).toLocaleString("en-US", {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
      return v > 0 ? {{ txt: "+$ " + abs, cls: "gpos" }} : {{ txt: "-$ " + abs, cls: "gneg" }};
    }}

    function fmtSignedPct(value) {{
      const v = Number(value) || 0;
      if (Math.abs(v) < 1e-12) return {{ txt: "0.00%", cls: "flat" }};
      const txt = (v > 0 ? "+" : "") + v.toFixed(2) + "%";
      return {{ txt, cls: v > 0 ? "gpos" : "gneg" }};
    }}

    function setText(id, txt, cls) {{
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = txt;
      el.classList.remove("gpos", "gneg", "flat");
      if (cls) el.classList.add(cls);
    }}

    function refresh() {{
      let raw = "";
      if (analystInput && analystInput.value !== "") {{
        raw = analystInput.value;
      }} else if (input) {{
        raw = input.value;
      }}

      const has = raw !== "" && !Number.isNaN(Number(raw));
      const caixaReal = has ? Number(raw) : null;
      const delta = has ? (caixaBalanco - caixaReal) : 0.0;
      const friccaoTotal = corretagem + delta;
      const nav = totalBruto - delta;
      const resultado = nav - capitalUso;
      const rent = capitalUso > 0 ? (resultado / capitalUso) * 100.0 : 0.0;

      if (has) {{
        setText("bridgeCaixaReal", fmtUsd(caixaReal), "");
        const d = fmtSignedUsd(delta);
        const dLiq = fmtSignedUsd(delta + caixaContabil);
        setText("bridgeDelta", d.txt, d.cls);
        setText("bridgeFriccaoOperacional", d.txt, d.cls);
        if (Math.abs(caixaContabil) > 1e-9) {{
          setText("bridgeDeltaAjustadoLiquidacao", dLiq.txt, dLiq.cls);
        }} else {{
          setText("bridgeDeltaAjustadoLiquidacao", "n/a", "flat");
        }}
        setText("reconDelta", d.txt, d.cls);
        setText("reconCaixaRealMirror", fmtUsd(caixaReal), "");
      }} else {{
        setText("bridgeCaixaReal", "pendente", "flat");
        setText("bridgeDelta", "pendente", "flat");
        setText("bridgeFriccaoOperacional", "pendente", "flat");
        setText("bridgeDeltaAjustadoLiquidacao", "n/a", "flat");
        setText("reconDelta", "pendente", "flat");
        setText("reconCaixaRealMirror", "pendente", "flat");
      }}

      const ft = fmtSignedUsd(friccaoTotal);
      const rs = fmtSignedUsd(resultado);
      const rp = fmtSignedPct(rent);
      setText("bridgeFriccaoTotal", ft.txt, ft.cls);
      setText("bridgeNav", fmtUsd(nav), "");
      setText("bridgeResultado", rs.txt, rs.cls);
      setText("bridgeRent", rp.txt, rp.cls);
    }}

    function makeAnalystRow(isBlank) {{
      const row = document.createElement("div");
      row.className = "op-row" + (isBlank ? " blank" : "");
      row.innerHTML = `
        <label>Tipo
          <select class="op-field op-tipo">
            <option value="">-</option>
            <option value="COMPRA">COMPRA</option>
            <option value="VENDA">VENDA</option>
          </select>
        </label>
        <label>Ticker<input class="op-field" type="text" placeholder="ex: REPL" /></label>
        <label>Valor US$<input class="op-field" type="number" step="0.01" min="0" placeholder="ex: 1000.00" /></label>
        <label>Preco<input class="op-field" type="number" step="0.01" min="0" placeholder="preco medio" /></label>
        <label>Qtd (opcional)<input class="op-field" type="number" step="0.00000001" min="0" placeholder="ou valor/preco" /></label>
        <label>Corretagem<input class="op-field" type="number" step="0.01" min="0" value="2.50" /></label>
      `;
      const sel = row.querySelector(".op-tipo");
      if (sel) {{
        sel.addEventListener("change", () => {{
          if (!sel.value) return;
          row.classList.remove("blank");
          const lastSel = opsRows ? opsRows.querySelector(".op-row:last-child .op-tipo") : null;
          if (lastSel && lastSel.value && opsRows) {{
            opsRows.appendChild(makeAnalystRow(true));
          }}
        }});
      }}
      return row;
    }}

    if (opsRows) {{
      opsRows.appendChild(makeAnalystRow(true));
    }}

    if (input) {{
      input.addEventListener("input", () => {{
        if (analystInput && document.activeElement === input) {{
          analystInput.value = input.value;
        }}
        refresh();
      }});
    }}
    if (analystInput) {{
      analystInput.addEventListener("input", () => {{
        if (input && document.activeElement === analystInput) {{
          input.value = analystInput.value;
        }}
        refresh();
      }});
    }}

    refresh();
  }})();
  </script>
</body>
</html>"""

