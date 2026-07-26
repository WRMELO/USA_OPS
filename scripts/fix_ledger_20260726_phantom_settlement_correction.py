"""Remediacao append-only do evento fantasma SETTLEMENT em 2026-07-16.

Contexto:
- Evento fantasma criado por teste exploratorio indevido de auditoria:
  id=a3aff53f-9bc1-450f-9e93-f63930dab98f, amount=2500.68, ref_id=None,
  reason="some-sell-id".
- A remediacao NAO reescreve historico: adiciona evento CORRECTION e
  regenera somente os artefatos derivados contaminados do dia 2026-07-15.

Uso:
- Dry-run (padrao): apenas projeta e imprime resumo.
- Aplicacao real: adicionar --confirm.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pipeline.ledger as ledger_mod
from pipeline.ledger import EventType, LedgerEvent, append_event, compute_cash, create_event, export_snapshot, read_all_events

PHANTOM_EVENT_ID = "a3aff53f-9bc1-450f-9e93-f63930dab98f"
PHANTOM_REASON = "some-sell-id"
PHANTOM_AMOUNT = 2500.68
TARGET_EXEC_DAY = date(2026, 7, 16)
REAL_MARKET_DAY = "2026-07-15"
REAL_BOLETIM_PATH = ROOT / "data" / "real" / f"{REAL_MARKET_DAY}.json"
CYCLE_BOLETIM_PATH = ROOT / "data" / "cycles" / REAL_MARKET_DAY / "boletim_preenchido.json"
CORRECTION_REASON = (
    "Anulacao de evento fantasma SETTLEMENT criado por teste exploratorio indevido "
    "do Auditor-Kimi em 2026-07-26 (violacao de read-only da skill auditor-kimi). "
    "Ver DECISION_LOG D-164/D-167."
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_number(value: float, *, decimals: int = 8) -> float | int:
    rounded_int = round(value)
    if abs(value - rounded_int) <= 1e-9:
        return int(rounded_int)
    return round(float(value), decimals)


def _effective_events(as_of_date: date, *, extra_events: list[LedgerEvent] | None = None) -> list[LedgerEvent]:
    all_events = [ev for ev in read_all_events() if ev.exec_date <= as_of_date]
    if extra_events:
        all_events.extend(ev for ev in extra_events if ev.exec_date <= as_of_date)
    all_events.sort(key=lambda ev: (ev.exec_date, ev.created_at, ev.id))

    cancelled = {ev.ref_id for ev in all_events if ev.type == EventType.CORRECTION and ev.ref_id}
    out: list[LedgerEvent] = []
    for ev in all_events:
        if ev.id in cancelled:
            continue
        if ev.type in {EventType.CORRECTION, EventType.RECON_ADJUST}:
            continue
        out.append(ev)
    return out


def _build_operations(day_events: list[LedgerEvent]) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    for ev in day_events:
        if ev.type != EventType.BUY or not ev.ticker:
            continue
        operations.append(
            {
                "type": "COMPRA",
                "ticker": ev.ticker,
                "qtd": _normalize_number(float(ev.qtd or 0.0), decimals=8),
                "preco": _normalize_number(float(ev.price or 0.0), decimals=6),
            }
        )
    return operations


def _build_cash_transfers(day_events: list[LedgerEvent]) -> list[dict[str, Any]]:
    cash_transfers: list[dict[str, Any]] = []
    for ev in day_events:
        if ev.type != EventType.SETTLEMENT:
            continue
        if not ev.ref_id:
            continue
        cash_transfers.append(
            {
                "value": _normalize_number(float(ev.amount), decimals=2),
                "note": ev.ref_id,
            }
        )
    return cash_transfers


def _assert_precondition(all_events: list[LedgerEvent]) -> LedgerEvent:
    matches = [ev for ev in all_events if ev.id == PHANTOM_EVENT_ID]
    if len(matches) != 1:
        raise RuntimeError(
            f"Precondicao falhou: esperado exatamente 1 evento fantasma, obtido {len(matches)}."
        )
    phantom = matches[0]
    if phantom.type != EventType.SETTLEMENT:
        raise RuntimeError(
            f"Precondicao falhou: evento alvo {PHANTOM_EVENT_ID} nao e SETTLEMENT."
        )
    if abs(float(phantom.amount) - PHANTOM_AMOUNT) > 0.01:
        raise RuntimeError(
            "Precondicao falhou: amount do evento fantasma divergente "
            f"(esperado {PHANTOM_AMOUNT:.2f}, obtido {float(phantom.amount):.2f})."
        )
    if phantom.ref_id is not None:
        raise RuntimeError(
            "Precondicao falhou: ref_id do evento fantasma deveria ser None."
        )
    if (phantom.reason or "") != PHANTOM_REASON:
        raise RuntimeError(
            "Precondicao falhou: reason do evento fantasma divergente "
            f"(esperado {PHANTOM_REASON!r}, obtido {(phantom.reason or '')!r})."
        )
    return phantom


def _rewrite_boletim(
    path: Path,
    *,
    operations: list[dict[str, Any]],
    cash_transfers: list[dict[str, Any]],
    cash_state: dict[str, float],
    snapshot: list[dict[str, Any]],
) -> None:
    payload = _read_json(path)
    payload["operations"] = operations
    payload["cash_transfers"] = cash_transfers
    payload["cash_free"] = float(cash_state.get("cash_free", 0.0))
    payload["cash_accounting"] = float(cash_state.get("cash_accounting", 0.0))
    payload["positions_snapshot"] = snapshot
    payload["cash_balance"] = float(cash_state.get("cash_free", 0.0))
    payload["caixa_liquidando"] = float(cash_state.get("cash_accounting", 0.0))
    _write_json(path, payload)


def _line_count(path: Path) -> int:
    return len([line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()])


def run(*, confirm: bool) -> dict[str, Any]:
    ledger_path = ledger_mod.LEDGER_PATH
    all_events_before = read_all_events()
    _assert_precondition(all_events_before)
    correction_exists_before = any(
        ev.type == EventType.CORRECTION and (ev.ref_id or "") == PHANTOM_EVENT_ID
        for ev in all_events_before
    )

    correction_event = create_event(
        EventType.CORRECTION,
        TARGET_EXEC_DAY,
        PHANTOM_AMOUNT,
        ref_id=PHANTOM_EVENT_ID,
        reason=CORRECTION_REASON,
    )

    cash_before = compute_cash(TARGET_EXEC_DAY)
    ledger_lines_before = _line_count(ledger_path)

    projected_extra = []
    if not correction_exists_before:
        projected_extra = [correction_event]

    if confirm and not correction_exists_before:
        append_event(correction_event)
        cash_after = compute_cash(TARGET_EXEC_DAY)
        snapshot_after = export_snapshot(TARGET_EXEC_DAY)
        effective_after = _effective_events(TARGET_EXEC_DAY)
        status = "APPLIED"
    else:
        cash_after = compute_cash(TARGET_EXEC_DAY, extra_events=projected_extra)
        snapshot_after = export_snapshot(TARGET_EXEC_DAY, extra_events=projected_extra)
        effective_after = _effective_events(TARGET_EXEC_DAY, extra_events=projected_extra)
        status = "NOOP_ALREADY_CORRECTED" if correction_exists_before else "DRY_RUN"

    day_events_after = [ev for ev in effective_after if ev.exec_date == TARGET_EXEC_DAY]
    operations_after = _build_operations(day_events_after)
    transfers_after = _build_cash_transfers(day_events_after)

    rewritten_files: list[str] = []
    if confirm:
        _rewrite_boletim(
            REAL_BOLETIM_PATH,
            operations=operations_after,
            cash_transfers=transfers_after,
            cash_state=cash_after,
            snapshot=snapshot_after,
        )
        _rewrite_boletim(
            CYCLE_BOLETIM_PATH,
            operations=operations_after,
            cash_transfers=transfers_after,
            cash_state=cash_after,
            snapshot=snapshot_after,
        )
        rewritten_files = [str(REAL_BOLETIM_PATH), str(CYCLE_BOLETIM_PATH)]

    ledger_lines_after = _line_count(ledger_path)
    if not confirm and not correction_exists_before:
        ledger_lines_after = ledger_lines_before + 1

    return {
        "ok": True,
        "status": status,
        "confirm": bool(confirm),
        "target_exec_day": TARGET_EXEC_DAY.isoformat(),
        "phantom_event_id": PHANTOM_EVENT_ID,
        "correction_event_id": correction_event.id,
        "correction_exists_before": bool(correction_exists_before),
        "ledger_path": str(ledger_path),
        "ledger_lines_before": ledger_lines_before,
        "ledger_lines_after": ledger_lines_after,
        "cash_before": {
            "cash_free": float(cash_before.get("cash_free", 0.0)),
            "cash_accounting": float(cash_before.get("cash_accounting", 0.0)),
        },
        "cash_after": {
            "cash_free": float(cash_after.get("cash_free", 0.0)),
            "cash_accounting": float(cash_after.get("cash_accounting", 0.0)),
        },
        "operations_after_count": len(operations_after),
        "cash_transfers_after_count": len(transfers_after),
        "rewritten_files": rewritten_files,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remedia evento fantasma SETTLEMENT (append-only) e regenera boletins derivados."
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Aplica a correcao no ledger e reescreve os boletins. Sem --confirm roda em dry-run.",
    )
    args = parser.parse_args()

    result = run(confirm=bool(args.confirm))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
