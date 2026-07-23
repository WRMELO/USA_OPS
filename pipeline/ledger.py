"""SSOT financeiro imutavel (T-045 / D-045)."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LEDGER_PATH = ROOT / "data" / "ssot" / "ledger.jsonl"
_QTD_EPS = 1e-6  # tolerancia para comparacoes de quantidade fracionaria (D-139)


class EventType(str, Enum):
    APORTE = "APORTE"
    RETIRADA = "RETIRADA"
    DIVIDENDO = "DIVIDENDO"
    BUY = "BUY"
    SELL = "SELL"
    SETTLEMENT = "SETTLEMENT"
    FEE = "FEE"
    CAIXA_REAL_INFORMADO = "CAIXA_REAL_INFORMADO"
    CORRECTION = "CORRECTION"
    RECON_ADJUST = "RECON_ADJUST"


@dataclass(frozen=True)
class LedgerEvent:
    id: str
    type: EventType
    exec_date: date
    created_at: datetime
    ticker: str | None = None
    qtd: float | None = None
    price: float | None = None
    amount: float = 0.0
    settle_date: date | None = None
    ref_id: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type.value,
            "exec_date": self.exec_date.isoformat(),
            "created_at": self.created_at.astimezone(UTC).isoformat(),
            "ticker": self.ticker,
            "qtd": self.qtd,
            "price": self.price,
            "amount": float(self.amount),
            "settle_date": self.settle_date.isoformat() if self.settle_date else None,
            "ref_id": self.ref_id,
            "reason": self.reason,
        }


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


def _to_date(v: Any) -> date | None:
    if not v:
        return None
    try:
        return date.fromisoformat(str(v))
    except Exception:
        return None


def _to_datetime(v: Any) -> datetime:
    try:
        return datetime.fromisoformat(str(v))
    except Exception:
        return datetime.now(tz=UTC)


def _safe_str_or_none(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in {"none", "null"}:
        return None
    return s


def qtd_from_invested(invested: float, price: float) -> float:
    if price <= 0:
        return 0.0
    return round(float(invested) / float(price), 8)


def _next_trading_day(from_day: date) -> date:
    opw = ROOT / "data" / "ssot" / "operational_window.parquet"
    if opw.exists():
        try:
            df = pd.read_parquet(opw, columns=["date"])
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            days = sorted({d for d in df["date"].dropna().tolist()})
            for d in days:
                if d > from_day:
                    return d
        except Exception:
            pass
    from lib.trading_calendar import is_session
    candidate = from_day + timedelta(days=1)
    while not is_session(candidate, exchange="XNYS"):
        candidate += timedelta(days=1)
    return candidate


def ensure_event_id() -> str:
    return str(uuid.uuid4())


def create_event(
    event_type: EventType,
    exec_date: date,
    amount: float,
    *,
    ticker: str | None = None,
    qtd: float | None = None,
    price: float | None = None,
    settle_date: date | None = None,
    ref_id: str | None = None,
    reason: str | None = None,
    event_id: str | None = None,
) -> LedgerEvent:
    if event_type == EventType.RECON_ADJUST:
        # Ajuste de reconciliacao nunca pode mover caixa/investido.
        amount = 0.0
    if settle_date is None and event_type in {EventType.BUY, EventType.SELL}:
        settle_date = _next_trading_day(exec_date)
    return LedgerEvent(
        id=event_id or ensure_event_id(),
        type=event_type,
        exec_date=exec_date,
        created_at=datetime.now(tz=UTC),
        ticker=(ticker or "").upper().strip() if ticker else None,
        qtd=qtd,
        price=price,
        amount=float(amount),
        settle_date=settle_date,
        ref_id=ref_id,
        reason=reason,
    )


def _from_dict(d: dict[str, Any]) -> LedgerEvent | None:
    try:
        event_type = EventType(str(d.get("type", "")).upper().strip())
    except Exception:
        return None
    exec_date = _to_date(d.get("exec_date"))
    if exec_date is None:
        return None
    return LedgerEvent(
        id=str(d.get("id", "")).strip() or ensure_event_id(),
        type=event_type,
        exec_date=exec_date,
        created_at=_to_datetime(d.get("created_at")),
        ticker=(_safe_str_or_none(d.get("ticker")) or "").upper().strip() or None,
        qtd=_safe_float(d.get("qtd"), 0.0) if d.get("qtd") is not None else None,
        price=_safe_float(d.get("price"), 0.0) if d.get("price") is not None else None,
        amount=_safe_float(d.get("amount"), 0.0),
        settle_date=_to_date(d.get("settle_date")),
        ref_id=_safe_str_or_none(d.get("ref_id")),
        reason=_safe_str_or_none(d.get("reason")),
    )


def append_event(event: LedgerEvent) -> None:
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER_PATH.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
        fp.flush()


def read_all_events() -> list[LedgerEvent]:
    if not LEDGER_PATH.exists():
        return []
    out: list[LedgerEvent] = []
    with LEDGER_PATH.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            ev = _from_dict(payload)
            if ev is not None:
                out.append(ev)
    out.sort(key=lambda e: (e.exec_date, e.created_at, e.id))
    return out


def is_duplicate(event: LedgerEvent) -> bool:
    for ev in read_all_events():
        if ev.type != event.type or ev.exec_date != event.exec_date:
            continue
        # Tipos monetarios sem ticker.
        if event.type in {EventType.APORTE, EventType.RETIRADA, EventType.DIVIDENDO}:
            if abs(ev.amount - event.amount) <= 0.01:
                return True
            continue
        # Caixa real informado: dedupe por (amount, reason, date).
        if event.type == EventType.CAIXA_REAL_INFORMADO:
            same_amount = abs(ev.amount - event.amount) <= 0.01
            same_reason = (ev.reason or "") == (event.reason or "")
            if same_amount and same_reason:
                return True
            continue
        # Liquidação é única por (ref_id, amount, date). Se não houver ref_id, usa reason.
        if event.type == EventType.SETTLEMENT:
            same_amount = abs(ev.amount - event.amount) <= 0.01
            same_ref = (ev.ref_id or "") == (event.ref_id or "")
            same_reason = (ev.reason or "") == (event.reason or "")
            if same_amount and ((event.ref_id and same_ref) or (not event.ref_id and same_reason)):
                return True
            continue
        # Corretagem segue o mesmo contrato de unicidade de SETTLEMENT.
        if event.type == EventType.FEE:
            same_amount = abs(ev.amount - event.amount) <= 0.01
            same_ref = (ev.ref_id or "") == (event.ref_id or "")
            same_reason = (ev.reason or "") == (event.reason or "")
            if same_amount and ((event.ref_id and same_ref) or (not event.ref_id and same_reason)):
                return True
            continue
        # BUY/SELL.
        if (
            (ev.ticker or "") == (event.ticker or "")
            and abs((ev.qtd or 0.0) - (event.qtd or 0.0)) <= _QTD_EPS
            and abs((ev.price or 0.0) - (event.price or 0.0)) <= 1e-6
            and abs(ev.amount - event.amount) <= 0.01
        ):
            return True
    return False


def _effective_events(
    as_of_date: date, *, extra_events: list[LedgerEvent] | None = None
) -> list[LedgerEvent]:
    all_events = [e for e in read_all_events() if e.exec_date <= as_of_date]
    if extra_events:
        all_events.extend(e for e in extra_events if e.exec_date <= as_of_date)
    all_events.sort(key=lambda e: (e.exec_date, e.created_at, e.id))
    cancelled = {e.ref_id for e in all_events if e.type == EventType.CORRECTION and e.ref_id}
    adjustments: dict[str, LedgerEvent] = {}
    for ev in all_events:
        if ev.type == EventType.RECON_ADJUST and ev.ref_id and ev.id not in cancelled:
            adjustments[ev.ref_id] = ev

    out: list[LedgerEvent] = []
    for ev in all_events:
        if ev.id in cancelled:
            continue
        if ev.type in {EventType.CORRECTION, EventType.RECON_ADJUST}:
            continue
        adj = adjustments.get(ev.id)
        if adj is not None:
            out.append(
                replace(
                    ev,
                    qtd=adj.qtd if adj.qtd is not None else ev.qtd,
                    price=adj.price if adj.price is not None else ev.price,
                )
            )
        else:
            out.append(ev)
    return out


def compute_positions(
    as_of_date: date, *, extra_events: list[LedgerEvent] | None = None
) -> dict[str, list[dict[str, Any]]]:
    lots: dict[str, list[dict[str, Any]]] = {}
    events = _effective_events(as_of_date, extra_events=extra_events)
    for ev in events:
        if ev.type == EventType.BUY and ev.ticker and float(ev.qtd or 0.0) > _QTD_EPS and (ev.price or 0.0) > 0:
            lots.setdefault(ev.ticker, []).append(
                {
                    "ticker": ev.ticker,
                    "buy_date": ev.exec_date.isoformat(),
                    "qtd": float(ev.qtd or 0.0),
                    "buy_price": float(ev.price or 0.0),
                }
            )
            continue
        if ev.type == EventType.SELL and ev.ticker and float(ev.qtd or 0.0) > _QTD_EPS:
            remain = float(ev.qtd or 0.0)
            queue = lots.get(ev.ticker, [])
            i = 0
            while i < len(queue) and remain > _QTD_EPS:
                take = min(remain, float(queue[i]["qtd"]))
                queue[i]["qtd"] = float(queue[i]["qtd"]) - take
                remain -= take
                if float(queue[i]["qtd"]) <= _QTD_EPS:
                    i += 1
            lots[ev.ticker] = [x for x in queue if float(x["qtd"]) > _QTD_EPS]
    out = {}
    for tk in sorted(lots.keys()):
        if lots[tk]:
            out[tk] = lots[tk]
    return out


def _settled_amounts(events: list[LedgerEvent], as_of_date: date) -> tuple[dict[str, float], float]:
    settled: dict[str, float] = {}
    unmatched_total = 0.0
    for ev in events:
        if ev.type != EventType.SETTLEMENT:
            continue
        if ev.exec_date > as_of_date:
            continue
        if not ev.ref_id:
            unmatched_total += float(ev.amount)
            continue
        settled[ev.ref_id] = settled.get(ev.ref_id, 0.0) + float(ev.amount)
    return settled, unmatched_total


def _settled_by_ref(events: list[LedgerEvent], as_of_date: date) -> dict[str, float]:
    settled, _ = _settled_amounts(events, as_of_date)
    return settled


def pending_settlements(as_of_date: date) -> list[dict[str, Any]]:
    events = _effective_events(as_of_date)
    settled, unmatched_total = _settled_amounts(events, as_of_date)
    out: list[dict[str, Any]] = []
    pending_rows: list[dict[str, Any]] = []
    for ev in events:
        if ev.type != EventType.SELL:
            continue
        if ev.settle_date and ev.settle_date > as_of_date:
            continue
        already = settled.get(ev.id, 0.0)
        remain = float(ev.amount) - already
        if remain > 0.50:
            pending_rows.append(
                {
                    "sell_id": ev.id,
                    "sale_date": ev.exec_date.isoformat(),
                    "ticker": ev.ticker or "",
                    "qtd": round(float(ev.qtd or 0.0), 8),
                    "preco": float(ev.price or 0.0),
                    "valor_venda": float(ev.amount),
                    "ja_transferido": already,
                    "pendente": remain,
                    "ref": ev.id,
                }
            )

    # Liquidacoes sem ref_id sao aplicadas em FIFO para reduzir pendencias antigas.
    pending_rows.sort(key=lambda x: (x["sale_date"], x["ticker"]))
    remaining_unmatched = float(unmatched_total)
    for row in pending_rows:
        if remaining_unmatched <= 0.50:
            break
        pending_amount = float(row["pendente"])
        take = min(pending_amount, remaining_unmatched)
        row["ja_transferido"] = float(row["ja_transferido"]) + take
        row["pendente"] = pending_amount - take
        remaining_unmatched -= take

    out = [row for row in pending_rows if float(row["pendente"]) > 0.50]
    out.sort(key=lambda x: (x["sale_date"], x["ticker"]))
    return out


def sells_in_settlement(as_of_date: date) -> list[dict[str, Any]]:
    """Vendas executadas com settle_date > as_of_date: inaptas a transferencia."""
    events = _effective_events(as_of_date)
    settled, _ = _settled_amounts(events, as_of_date)
    out: list[dict[str, Any]] = []
    for ev in events:
        if ev.type != EventType.SELL:
            continue
        if not ev.settle_date or ev.settle_date <= as_of_date:
            continue
        already = settled.get(ev.id, 0.0)
        remain = float(ev.amount) - already
        if remain > 0.50:
            out.append(
                {
                    "sell_id": ev.id,
                    "sale_date": ev.exec_date.isoformat(),
                    "ticker": ev.ticker or "",
                    "qtd": round(float(ev.qtd or 0.0), 8),
                    "preco": float(ev.price or 0.0),
                    "valor_venda": float(ev.amount),
                    "ja_transferido": already,
                    "pendente": remain,
                    "settle_date": ev.settle_date.isoformat(),
                }
            )
    out.sort(key=lambda x: (x["sale_date"], x["ticker"]))
    return out


def compute_cash(
    as_of_date: date, *, extra_events: list[LedgerEvent] | None = None
) -> dict[str, float]:
    events = _effective_events(as_of_date, extra_events=extra_events)
    free = 0.0
    for ev in events:
        if ev.type in {EventType.APORTE, EventType.DIVIDENDO, EventType.SETTLEMENT}:
            free += float(ev.amount)
        elif ev.type in {EventType.RETIRADA, EventType.BUY, EventType.FEE}:
            free -= float(ev.amount)

    settled, unmatched_total = _settled_amounts(events, as_of_date)
    accounting = 0.0
    for ev in events:
        if ev.type != EventType.SELL:
            continue
        # Sempre reconciliar SELL por "amount - settled", mesmo quando
        # settle_date futuro. Isso evita dupla contagem quando existe
        # SETTLEMENT same-day (ex.: liquidacao JA_NO_CAIXA).
        remain = float(ev.amount) - settled.get(ev.id, 0.0)
        accounting += max(remain, 0.0)
    accounting = max(accounting - float(unmatched_total), 0.0)
    return {"cash_free": free, "cash_accounting": accounting}


def compute_daily_cash_flow(
    exec_day: date, prev_day: date, *, extra_events: list[LedgerEvent] | None = None
) -> dict[str, float]:
    cash_prev = compute_cash(prev_day, extra_events=extra_events)
    cash_today = compute_cash(exec_day, extra_events=extra_events)
    events = _effective_events(exec_day, extra_events=extra_events)
    day_events = [ev for ev in events if ev.exec_date == exec_day]

    settled_today_refs = {
        ev.ref_id
        for ev in day_events
        if ev.type == EventType.SETTLEMENT and ev.ref_id
    }
    sells_today = [ev for ev in day_events if ev.type == EventType.SELL]
    sell_ids_today = {ev.id for ev in sells_today}

    aportes_dia = sum(
        float(ev.amount) for ev in day_events if ev.type in {EventType.APORTE, EventType.DIVIDENDO}
    )
    retiradas_dia = sum(float(ev.amount) for ev in day_events if ev.type == EventType.RETIRADA)
    compras_dia = sum(float(ev.amount) for ev in day_events if ev.type == EventType.BUY)
    corretagem_dia = sum(float(ev.amount) for ev in day_events if ev.type == EventType.FEE)

    vendas_liquidadas_dia = sum(
        float(ev.amount) for ev in sells_today if ev.id in settled_today_refs
    )
    vendas_em_liquidacao_dia = sum(
        float(ev.amount) for ev in sells_today if ev.id not in settled_today_refs
    )

    settlements_today = [ev for ev in day_events if ev.type == EventType.SETTLEMENT]
    transferencias_liquidadas_dia = sum(
        float(ev.amount)
        for ev in settlements_today
        if ev.ref_id and ev.ref_id not in sell_ids_today
    )

    return {
        "caixa_livre_anterior": float(cash_prev.get("cash_free", 0.0)),
        "vendas_liquidadas_dia": float(vendas_liquidadas_dia),
        "aportes_dia": float(aportes_dia),
        "retiradas_dia": float(retiradas_dia),
        "compras_dia": float(compras_dia),
        "corretagem_dia": float(corretagem_dia),
        "caixa_livre_final": float(cash_today.get("cash_free", 0.0)),
        "caixa_contabil_anterior": float(cash_prev.get("cash_accounting", 0.0)),
        "vendas_em_liquidacao_dia": float(vendas_em_liquidacao_dia),
        "transferencias_liquidadas_dia": float(transferencias_liquidadas_dia),
        "caixa_contabil_final": float(cash_today.get("cash_accounting", 0.0)),
    }


def latest_informed_cash(as_of_date: date) -> dict[str, Any] | None:
    """Ultimo saldo de Caixa Livre Real informado pelo Owner.

    Este dado e observacional e nao altera o calculo de caixa contabil/de balanco.
    """
    informed = [
        ev
        for ev in read_all_events()
        if ev.type == EventType.CAIXA_REAL_INFORMADO and ev.exec_date <= as_of_date
    ]
    if not informed:
        return None
    latest = max(informed, key=lambda ev: (ev.exec_date, ev.created_at))
    return {
        "amount": float(latest.amount),
        "exec_date": latest.exec_date.isoformat(),
    }


def export_snapshot(
    as_of_date: date, *, extra_events: list[LedgerEvent] | None = None
) -> list[dict[str, Any]]:
    pos = compute_positions(as_of_date, extra_events=extra_events)
    out: list[dict[str, Any]] = []
    for tk in sorted(pos.keys()):
        for lot in pos[tk]:
            qtd = float(lot.get("qtd", 0.0))
            if qtd <= _QTD_EPS:
                continue
            out.append(
                {
                    "ticker": tk,
                    "data_compra": str(lot.get("buy_date", as_of_date.isoformat())),
                    "qtd": round(qtd, 8),
                    "preco_compra": float(lot.get("buy_price", 0.0)),
                }
            )
    return out


def build_operations_book(
    as_of_date: date, *, extra_events: list[LedgerEvent] | None = None
) -> dict[str, Any]:
    events = _effective_events(as_of_date, extra_events=extra_events)
    buys: dict[str, list[dict[str, Any]]] = {}
    sells: dict[str, list[dict[str, Any]]] = {}
    fifo_queue: dict[str, list[dict[str, Any]]] = {}
    realized: dict[str, float] = {}

    for ev in events:
        ticker = (ev.ticker or "").upper().strip()
        if not ticker:
            continue
        qtd = float(ev.qtd or 0.0)
        price = float(ev.price or 0.0)
        if qtd <= _QTD_EPS or price <= 0:
            continue

        if ev.type == EventType.BUY:
            buy_row = {
                "date": ev.exec_date.isoformat(),
                "qtd": round(qtd, 8),
                "preco": price,
                "valor": round(qtd * price, 2),
            }
            buys.setdefault(ticker, []).append(buy_row)
            fifo_queue.setdefault(ticker, []).append(
                {
                    "date": ev.exec_date.isoformat(),
                    "qtd": qtd,
                    "preco": price,
                }
            )
            realized.setdefault(ticker, 0.0)
            continue

        if ev.type == EventType.SELL:
            sell_row = {
                "date": ev.exec_date.isoformat(),
                "qtd": round(qtd, 8),
                "preco": price,
                "valor": round(qtd * price, 2),
            }
            sells.setdefault(ticker, []).append(sell_row)
            queue = fifo_queue.setdefault(ticker, [])
            remaining = qtd
            matched_cost = 0.0
            while remaining > _QTD_EPS and queue:
                lot = queue[0]
                lot_qtd = float(lot.get("qtd", 0.0))
                lot_price = float(lot.get("preco", 0.0))
                if lot_qtd <= _QTD_EPS or lot_price <= 0:
                    queue.pop(0)
                    continue
                take = min(remaining, lot_qtd)
                matched_cost += take * lot_price
                lot["qtd"] = lot_qtd - take
                remaining -= take
                if float(lot.get("qtd", 0.0)) <= _QTD_EPS:
                    queue.pop(0)
            proceeds = float(qtd * price)
            realized[ticker] = realized.get(ticker, 0.0) + (proceeds - matched_cost)

    out: dict[str, Any] = {}
    tickers = sorted(set(buys.keys()) | set(sells.keys()))
    for ticker in tickers:
        queue = fifo_queue.get(ticker, [])
        open_lots = [lot for lot in queue if float(lot.get("qtd", 0.0)) > _QTD_EPS]
        qtd_liquida = sum(float(lot.get("qtd", 0.0)) for lot in open_lots)
        investido = sum(float(lot.get("qtd", 0.0)) * float(lot.get("preco", 0.0)) for lot in open_lots)
        custo_medio = (investido / qtd_liquida) if qtd_liquida > _QTD_EPS else 0.0
        out[ticker] = {
            "ticker": ticker,
            "compras": buys.get(ticker, []),
            "vendas": sells.get(ticker, []),
            "qtd_liquida": round(qtd_liquida, 8),
            "custo_medio": round(custo_medio, 4),
            "investido": round(investido, 2),
            "realizado": round(realized.get(ticker, 0.0), 2),
        }
    return out


def total_fees(as_of_date: date) -> float:
    """Soma das corretagens (FEE) ate a data de corte."""
    events = _effective_events(as_of_date)
    return sum(float(ev.amount) for ev in events if ev.type == EventType.FEE)


def capital_em_uso(as_of_date: date) -> float:
    """Capital liquido em uso: aportes menos retiradas ate a data de corte."""
    events = _effective_events(as_of_date)
    aportes = sum(float(ev.amount) for ev in events if ev.type == EventType.APORTE)
    retiradas = sum(float(ev.amount) for ev in events if ev.type == EventType.RETIRADA)
    return aportes - retiradas


def _load_winner_cagr() -> float:
    path = ROOT / "config" / "winner_us.json"
    try:
        cfg = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0.421353
    holdout = cfg.get("holdout_metrics", {})
    if not isinstance(holdout, dict):
        holdout = {}
    cagr = holdout.get("cagr_pct")
    if cagr is None:
        acid_root = cfg.get("acid_test", {})
        if not isinstance(acid_root, dict):
            acid_root = {}
        acid_global = acid_root.get("global_holdout_metrics", {})
        if not isinstance(acid_global, dict):
            acid_global = {}
        c4 = acid_global.get("c4", {})
        if not isinstance(c4, dict):
            c4 = {}
        cagr = c4.get("cagr_pct", 42.1353)
    try:
        return float(cagr) / 100.0
    except Exception:
        return 0.421353


def build_real_base1_series(
    as_of_date: date,
    *,
    live_snapshot: list[dict[str, Any]] | None = None,
    live_cash_free: float | None = None,
    live_cash_accounting: float | None = None,
    price_window_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Serie Base 1 real por cotizacao plena (R-049).

    Nota: este calculo nao aplica ajuste por split_factor entre data de compra e
    data de referencia; usa close_operational por ticker na data (ou ultimo
    pregao anterior), com fallback para preco_compra quando necessario.
    """
    target_window = price_window_path or (ROOT / "data" / "ssot" / "operational_window.parquet")
    if not target_window.exists():
        return []

    records: list[dict[str, Any]] = []
    real_dir = LEDGER_PATH.parent
    for path in sorted(real_dir.glob("*.json")):
        try:
            file_day = date.fromisoformat(path.stem)
        except Exception:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        exec_day = _to_date(payload.get("exec_day")) or _to_date(payload.get("date")) or file_day
        ref_day = (
            _to_date(payload.get("market_day"))
            or _to_date(payload.get("reference_decision"))
            or exec_day
        )
        if ref_day > as_of_date:
            continue
        snapshot = payload.get("positions_snapshot")
        if not isinstance(snapshot, list):
            snapshot = []
        cash_free = _safe_float(payload.get("cash_free", payload.get("cash_balance", 0.0)), 0.0)
        cash_acc = _safe_float(payload.get("cash_accounting", payload.get("caixa_liquidando", 0.0)), 0.0)
        if not snapshot and abs(cash_free) <= 1e-9 and abs(cash_acc) <= 1e-9:
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

    by_ref_day: dict[date, dict[str, Any]] = {}
    for rec in records:
        current = by_ref_day.get(rec["ref_day"])
        if current is None or rec["exec_day"] > current["exec_day"]:
            by_ref_day[rec["ref_day"]] = rec
    ordered = [by_ref_day[d] for d in sorted(by_ref_day.keys())]

    if (
        isinstance(live_snapshot, list)
        and live_cash_free is not None
        and live_cash_accounting is not None
        and not any(rec["ref_day"] == as_of_date for rec in ordered)
    ):
        ordered.append(
            {
                "exec_day": as_of_date,
                "ref_day": as_of_date,
                "snapshot": live_snapshot,
                "cash_free": _safe_float(live_cash_free, 0.0),
                "cash_acc": _safe_float(live_cash_accounting, 0.0),
            }
        )

    if not ordered:
        return []

    tickers: set[str] = set()
    for rec in ordered:
        for pos in rec["snapshot"]:
            if not isinstance(pos, dict):
                continue
            ticker = str(pos.get("ticker", "")).upper().strip()
            if ticker:
                tickers.add(ticker)

    prices = pd.DataFrame(columns=["date", "ticker", "close_operational"])
    if tickers:
        try:
            prices = pd.read_parquet(target_window, columns=["date", "ticker", "close_operational"])
        except Exception:
            prices = pd.DataFrame(columns=["date", "ticker", "close_operational"])
    if prices.empty:
        return []
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["ticker"] = prices["ticker"].astype(str).str.upper().str.strip()
    prices["close_operational"] = pd.to_numeric(prices["close_operational"], errors="coerce")
    prices = prices.dropna(subset=["date", "ticker", "close_operational"])
    prices = prices[(prices["date"] <= pd.Timestamp(as_of_date)) & (prices["ticker"].isin(tickers))]
    if prices.empty:
        return []
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    by_ticker: dict[str, pd.DataFrame] = {}
    for ticker in prices["ticker"].unique():
        by_ticker[str(ticker)] = prices[prices["ticker"] == ticker][["date", "close_operational"]].copy()

    events = read_all_events()
    if not events:
        return []
    external_flow_cum: list[float] = []
    for rec in ordered:
        cutoff = rec["exec_day"]
        cum_aportes = sum(
            float(ev.amount)
            for ev in events
            if ev.type == EventType.APORTE and ev.exec_date <= cutoff
        )
        cum_retiradas = sum(
            float(ev.amount)
            for ev in events
            if ev.type == EventType.RETIRADA and ev.exec_date <= cutoff
        )
        external_flow_cum.append(cum_aportes - cum_retiradas)

    if not external_flow_cum or max(external_flow_cum) <= 1e-9:
        return []

    cagr = _load_winner_cagr()
    daily_cagr = (1.0 + cagr) ** (1.0 / 252.0) - 1.0
    cagr_expect = 1.0

    rows: list[dict[str, Any]] = []
    cota_price_prev: float | None = None
    cota_qty_prev = 0.0
    total_ativo_prev: float | None = None
    base1_prev: float | None = None

    for idx, rec in enumerate(ordered):
        ref_ts = pd.Timestamp(rec["ref_day"])
        total_mkt = 0.0
        for pos in rec["snapshot"]:
            if not isinstance(pos, dict):
                continue
            ticker = str(pos.get("ticker", "")).upper().strip()
            qtd = _safe_float(pos.get("qtd"), 0.0)
            if not ticker or qtd <= _QTD_EPS:
                continue
            price = _safe_float(pos.get("preco_compra", pos.get("buy_price", 0.0)), 0.0)
            sub = by_ticker.get(ticker)
            if sub is not None and not sub.empty:
                sub_until = sub[sub["date"] <= ref_ts]
                if not sub_until.empty:
                    price = _safe_float(sub_until.iloc[-1]["close_operational"], price)
            if price <= 0:
                continue
            total_mkt += qtd * price

        total_ativo = total_mkt + _safe_float(rec["cash_free"], 0.0) + _safe_float(rec["cash_acc"], 0.0)
        ext_flow_cum = external_flow_cum[idx]
        prev_ext_flow_cum = external_flow_cum[idx - 1] if idx > 0 else 0.0
        ext_flow_day = ext_flow_cum - prev_ext_flow_cum

        cota_price: float | None = None
        if cota_price_prev is None:
            if ext_flow_day > 1e-9:
                cota_qty_prev = ext_flow_day / 100.0
                if cota_qty_prev > 1e-9:
                    cota_price = total_ativo / cota_qty_prev
        elif total_ativo_prev is not None and total_ativo_prev > 1e-9:
            nav_pre_flow = total_ativo - ext_flow_day
            cota_price = cota_price_prev * (nav_pre_flow / total_ativo_prev)
            if cota_price > 1e-9:
                cota_qty_prev += ext_flow_day / cota_price

        if cota_price is None:
            continue

        cota_price_prev = cota_price
        total_ativo_prev = total_ativo
        base1 = cota_price / 100.0
        daily_var_pct = 0.0
        if base1_prev is not None and base1_prev > 1e-9:
            daily_var_pct = (base1 / base1_prev - 1.0) * 100.0
        base1_prev = base1

        if rows:
            cagr_expect *= 1.0 + daily_cagr
        rows.append(
            {
                "date": rec["ref_day"].isoformat(),
                "nav": round(total_ativo, 4),
                "base1": round(base1, 8),
                "daily_var_pct": round(daily_var_pct, 6),
                "cagr_expect": round(cagr_expect, 8),
            }
        )

    return rows

