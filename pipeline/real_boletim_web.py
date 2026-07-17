"""Boletim web LIVE-REAL-TEST com rascunho intermediario."""
from __future__ import annotations

import html
import json
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


def _fmt_usd(value: Any) -> str:
    return f"$ {_safe_float(value):,.2f}"


def _fmt_pct(value: Any) -> str:
    return f"{_safe_float(value):.2f}%"


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
    qtd: int,
    preco: float,
    corretagem: float,
    preco_sombra: float | None = None,
) -> dict[str, Any]:
    op_type = str(tipo or "").upper().strip()
    op_ticker = str(ticker or "").upper().strip()
    op_qtd = _safe_int(qtd)
    op_preco = _safe_float(preco)
    op_corretagem = _safe_float(corretagem)
    if op_type not in {"COMPRA", "VENDA"}:
        raise ValueError("Tipo invalido")
    if not op_ticker:
        raise ValueError("Ticker obrigatorio")
    if op_qtd <= 0:
        raise ValueError("Quantidade invalida")
    if op_preco <= 0:
        raise ValueError("Preco invalido")
    if op_corretagem < 0:
        raise ValueError("Corretagem invalida")

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
        "qtd": op_qtd,
        "preco": op_preco,
        "corretagem": op_corretagem,
        "preco_sombra": shadow_price,
        "preco_sombra_auto": shadow_auto,
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
            qtd = _safe_int(raw.get("qtd"))
            preco = _safe_float(raw.get("preco"))
            corretagem = _safe_float(raw.get("corretagem"))
            preco_sombra = _safe_float(raw.get("preco_sombra"), 0.0)

            if op_type not in {"COMPRA", "VENDA"}:
                warnings.append(f"Operacao {idx} ignorada: tipo invalido.")
                continue
            if not ticker or qtd <= 0 or preco <= 0:
                warnings.append(f"Operacao {idx} ignorada: dados incompletos.")
                continue

            event_type = EventType.BUY if op_type == "COMPRA" else EventType.SELL
            amount = float(qtd * preco)
            event_reason = "LIVE-REAL-TEST web close"
            event = create_event(
                event_type,
                exec_day,
                amount,
                ticker=ticker,
                qtd=qtd,
                price=preco,
                reason=event_reason,
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


def close_day(exec_day: date, ledger_dir: Path) -> dict[str, Any]:
    from scripts.friction_ruler import build_friction_report_payload
    from scripts.live_real_cutover import build_boletim_payload

    resolved_ledger_dir = _resolve_ledger_dir(ledger_dir)
    draft = load_draft(exec_day)
    operations = draft.get("operations", [])
    result = apply_draft_to_ledger(exec_day, resolved_ledger_dir, operations)

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
    try:
        ledger_mod.LEDGER_PATH = real_ledger_path
        cash = compute_cash(today)
        positions = export_snapshot(today)
        operations_book_raw = ledger_mod.build_operations_book(today)
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
        qtd_liquida = int(_safe_int(row.get("qtd_liquida"), 0))
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
                "qtd": int(_safe_int(row.get("qtd"))),
                "preco_compra": _safe_float(row.get("preco_compra")),
                "close_d1": _safe_float(hold.get("close_d1"), 0.0),
                "heat_pct": _safe_float(hold.get("heat_pct"), 0.0),
            }
        )

    top_operational = master.get("operational_ranking", [])
    if not isinstance(top_operational, list):
        top_operational = []

    draft = load_draft(today)
    closed_boletim_path = resolved_ledger_dir / f"{today.isoformat()}.json"
    return {
        "today": today.isoformat(),
        "market_day": str(context.get("market_day", today.isoformat())),
        "cash_free": float(cash.get("cash_free", 0.0)),
        "cash_accounting": float(cash.get("cash_accounting", 0.0)),
        "positions": positions_enriched,
        "held_set": sorted(held_set),
        "top_operational": top_operational,
        "target_weights": target_weights,
        "operations_book": operations_book,
        "forno": forno,
        "draft": draft,
        "closed_boletim_exists": closed_boletim_path.exists(),
        "context_available": bool(context),
    }


def render_live_html(view: dict[str, Any]) -> str:
    today = str(view.get("today", ""))
    market_day = str(view.get("market_day", today))
    cash_free = _fmt_usd(view.get("cash_free", 0.0))
    cash_accounting = _fmt_usd(view.get("cash_accounting", 0.0))
    forno = view.get("forno", {}) if isinstance(view.get("forno"), dict) else {}
    is_rebalance = forno.get("is_rebalance_day")
    cycles = forno.get("cycles_to_next_rebalance")
    next_rebalance = forno.get("next_rebalance_date")
    is_d1_pos_rebalance = bool(forno.get("is_d1_pos_rebalance", False))

    positions = view.get("positions", []) if isinstance(view.get("positions"), list) else []
    pos_rows: list[str] = []
    if positions:
        for row in positions:
            pos_rows.append(
                "<tr>"
                f"<td>{html.escape(str(row.get('ticker', '')))}</td>"
                f"<td>{html.escape(str(row.get('data_compra', '')))}</td>"
                f"<td style='text-align:right'>{int(_safe_int(row.get('qtd')))}</td>"
                f"<td style='text-align:right'>{_fmt_usd(row.get('preco_compra', 0.0))}</td>"
                f"<td style='text-align:right'>{_fmt_usd(row.get('close_d1', 0.0))}</td>"
                f"<td style='text-align:right'>{_fmt_pct(row.get('heat_pct', 0.0))}</td>"
                "</tr>"
            )
    else:
        pos_rows.append("<tr><td colspan='6'>Nenhuma posicao registrada no ledger real.</td></tr>")

    operations_book = view.get("operations_book", {}) if isinstance(view.get("operations_book"), dict) else {}
    book_rows: list[str] = []

    def _format_ops_column(rows: Any) -> str:
        if not isinstance(rows, list) or not rows:
            return "-"
        parts: list[str] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            op_date = html.escape(str(item.get("date", "")))
            op_qtd = int(_safe_int(item.get("qtd"), 0))
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
            book_rows.append(
                "<tr>"
                f"<td>{html.escape(ticker)}</td>"
                f"<td>{_format_ops_column(row.get('compras', []))}</td>"
                f"<td>{_format_ops_column(row.get('vendas', []))}</td>"
                f"<td style='text-align:right'>{int(_safe_int(row.get('qtd_liquida', 0)))}</td>"
                f"<td style='text-align:right'>{_fmt_usd(row.get('custo_medio', 0.0))}</td>"
                f"<td style='text-align:right'>{_fmt_usd(row.get('close_d1', 0.0))}</td>"
                f"<td style='text-align:right'>{_fmt_usd(row.get('realizado', 0.0))}</td>"
                f"<td style='text-align:right'>{nao_realizado_fmt}</td>"
                "</tr>"
            )
    if not book_rows:
        book_rows.append("<tr><td colspan='8'>Sem operacoes registradas por ativo.</td></tr>")

    held_set = set(view.get("held_set", []))
    ranking = view.get("top_operational", []) if isinstance(view.get("top_operational"), list) else []
    target_weights = view.get("target_weights", {}) if isinstance(view.get("target_weights"), dict) else {}

    def _rank_key(item: dict[str, Any]) -> int:
        try:
            return int(item.get("m3_rank"))
        except Exception:
            try:
                return int(item.get("rank"))
            except Exception:
                return 10**9

    top_rows: list[str] = []
    if ranking:
        for row in sorted(ranking, key=_rank_key)[:20]:
            ticker = str(row.get("ticker", "")).upper().strip()
            indicator = "held" if ticker in held_set else "cand"
            tw = target_weights.get(ticker, row.get("target_weight", 0.0))
            top_rows.append(
                "<tr>"
                f"<td><span class='{indicator}'></span>{html.escape(ticker)}</td>"
                f"<td style='text-align:right'>{_rank_key(row)}</td>"
                f"<td style='text-align:right'>{_safe_float(row.get('score_m3', 0.0)):.4f}</td>"
                f"<td style='text-align:right'>{_fmt_pct(_safe_float(tw, 0.0) * 100.0)}</td>"
                f"<td style='text-align:right'>{_fmt_usd(row.get('close_d1', 0.0))}</td>"
                "</tr>"
            )
    else:
        top_rows.append("<tr><td colspan='5'>Top-20 nao disponivel — rode pipeline/analise_us.py.</td></tr>")

    draft = view.get("draft", {}) if isinstance(view.get("draft"), dict) else {}
    operations = draft.get("operations", []) if isinstance(draft.get("operations"), list) else []
    draft_rows: list[str] = []
    for op in operations:
        op_id = html.escape(str(op.get("id", "")))
        draft_rows.append(
            "<tr>"
            f"<td>{html.escape(str(op.get('type', '')))}</td>"
            f"<td>{html.escape(str(op.get('ticker', '')))}</td>"
            f"<td style='text-align:right'>{int(_safe_int(op.get('qtd')))}</td>"
            f"<td style='text-align:right'>{_fmt_usd(op.get('preco', 0.0))}</td>"
            f"<td style='text-align:right'>{_fmt_usd(op.get('corretagem', 0.0))}</td>"
            f"<td style='text-align:right'>{_fmt_usd(op.get('preco_sombra', 0.0)) if _safe_float(op.get('preco_sombra', 0.0)) > 0 else '-'}</td>"
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
        draft_rows.append("<tr><td colspan='7'>Nenhuma operacao em rascunho.</td></tr>")

    closed_note = (
        "<p class='pill'>Dia encerrado: boletim de fechamento ja existe para hoje.</p>"
        if bool(view.get("closed_boletim_exists"))
        else ""
    )

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
    :root {{ --ink:#0E1318; --surface:#161D25; --surface2:#1A222B; --hair:#29333D; --bone:#E9E5D8; --muted:#868F9B; --teal:#52B9A3; --amber:#E2A959; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; padding: 20px; background: var(--ink); color: var(--bone); font-family: "Space Grotesk", sans-serif; }}
    .mono {{ font-family: "IBM Plex Mono", monospace; }}
    .top {{ display:flex; justify-content:space-between; gap:12px; flex-wrap:wrap; border-bottom:1px solid var(--hair); margin-bottom:16px; padding-bottom:12px; }}
    .badge {{ border:1px solid var(--amber); color:var(--amber); border-radius:99px; padding:4px 10px; font-size:11px; text-transform:uppercase; letter-spacing:.08em; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px; margin-bottom:12px; }}
    .card {{ background:var(--surface); border:1px solid var(--hair); border-radius:10px; padding:12px; }}
    h1,h2,h3 {{ margin:0 0 8px 0; }}
    table {{ width:100%; border-collapse:collapse; font-family:"IBM Plex Mono", monospace; font-size:12px; }}
    th,td {{ border-bottom:1px solid var(--hair); padding:6px; text-align:left; }}
    tr:last-child td {{ border-bottom:none; }}
    .held,.cand {{ display:inline-block; width:7px; height:7px; border-radius:99px; margin-right:6px; }}
    .held {{ background: var(--teal); }}
    .cand {{ border:1px solid var(--muted); }}
    .pill {{ font-size:12px; color:var(--amber); }}
    form {{ margin:0; }}
    .form-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:8px; }}
    input,select,button {{ width:100%; padding:8px; border-radius:6px; border:1px solid var(--hair); background:var(--surface2); color:var(--bone); }}
    button {{ cursor:pointer; font-weight:600; }}
    .muted {{ color: var(--muted); font-size:12px; }}
  </style>
</head>
<body>
  <div class="top">
    <div>
      <h1>Boletim LIVE-REAL-TEST</h1>
      <div class="badge mono">/painel web operacional</div>
      {closed_note}
    </div>
    <div class="mono">
      <div>market_day: {html.escape(market_day)}</div>
      <div>exec_day: {html.escape(today)}</div>
    </div>
  </div>

  <div class="grid">
    <div class="card"><h3>Caixa Livre</h3><div class="mono">{cash_free}</div></div>
    <div class="card"><h3>Caixa Contabil</h3><div class="mono">{cash_accounting}</div></div>
    <div class="card">
      <h3>Status Rebalance / D+2</h3>
      <div class="mono">is_rebalance_day: {html.escape(str(is_rebalance))}</div>
      <div class="mono">cycles_to_next: {html.escape(str(cycles))}</div>
      <div class="mono">next_rebalance: {html.escape(str(next_rebalance))}</div>
      <div class="mono">is_d1_pos_rebalance: {html.escape(str(is_d1_pos_rebalance))}</div>
    </div>
  </div>

  <div class="card">
    <h2>Carteira real</h2>
    <table>
      <tr><th>Ticker</th><th>Data Compra</th><th>Qtd</th><th>Preco Compra</th><th>Fech. D-1</th><th>Heat %</th></tr>
      {''.join(pos_rows)}
    </table>
  </div>

  <div class="card">
    <h2>Livro de operacoes por ativo</h2>
    <table>
      <tr><th>Ticker</th><th>Compras</th><th>Vendas</th><th>Qtd Liquida</th><th>Custo Medio</th><th>Fech. D-1</th><th>Resultado Realizado</th><th>Resultado Nao Realizado</th></tr>
      {''.join(book_rows)}
    </table>
  </div>

  <div class="card">
    <h2>Top-20 operacional (m3_rank)</h2>
    <table>
      <tr><th>Ticker</th><th>M3 Rank</th><th>Score M3</th><th>Peso-Alvo</th><th>Fech. D-1</th></tr>
      {''.join(top_rows)}
    </table>
  </div>

  <div class="card">
    <h2>Rascunho do dia</h2>
    <table>
      <tr><th>Tipo</th><th>Ticker</th><th>Qtd</th><th>Preco real</th><th>Corretagem</th><th>Preco-sombra</th><th>Acoes</th></tr>
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
        <label>Quantidade<input type="number" name="qtd" min="1" step="1" required /></label>
        <label>Preco real<input type="number" name="preco" min="0.01" step="0.01" required /></label>
        <label>Corretagem<input type="number" name="corretagem" min="0.00" step="0.01" value="2.50" /></label>
        <label>Preco-sombra (opcional)<input type="number" name="preco_sombra" min="0.00" step="0.01" placeholder="auto-lookup se vazio" /></label>
      </div>
      <p class="muted">Se preco-sombra ficar vazio, o sistema tenta auto-lookup no SSOT operacional.</p>
      <button type="submit">Salvar rascunho</button>
    </form>
  </div>

  <div class="card">
    <h2>Encerramento definitivo do dia</h2>
    <form method="POST" action="/painel/encerrar">
      <input type="hidden" name="exec_day" value="{html.escape(today)}" />
      <label><input type="checkbox" name="confirmar" value="sim" required /> Confirmo encerramento definitivo.</label>
      <button type="submit">Encerrar o Dia</button>
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

  <p class="muted">Fallback continua disponivel via scripts/atalhos: USA_REGISTRAR_ORDEM e USA_ENCERRAR_DIA. Grafico Base-1 e reconciliacao de Caixa Ajustado ficam fora desta iteracao.</p>
</body>
</html>"""

