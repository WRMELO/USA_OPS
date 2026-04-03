"""Migra data/real/*.json para SSOT ledger append-only (T-045 / D-045)."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.ledger import (  # noqa: E402
    EventType,
    append_event,
    compute_cash,
    export_snapshot,
    is_duplicate,
    pending_settlements,
    read_all_events,
    create_event,
)


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


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_ticker_date_from_note(note: str) -> tuple[str | None, date | None]:
    note = (note or "").strip().upper()
    # Ex: "VENDA LASR 2026-03-31"
    parts = note.split()
    if len(parts) >= 3 and parts[0] == "VENDA":
        tk = parts[1].strip()
        try:
            d = date.fromisoformat(parts[2])
            return tk, d
        except Exception:
            return tk, None
    return None, None


def _find_sell_ref(note: str, value: float) -> str | None:
    tk, d = _extract_ticker_date_from_note(note)
    candidates = []
    for ev in read_all_events():
        if ev.type != EventType.SELL:
            continue
        if tk and ev.ticker != tk:
            continue
        if d and ev.exec_date != d:
            continue
        if value > 0 and abs(float(ev.amount) - value) > 0.05 and abs(float(ev.amount)) > 0:
            # Permite parcial no caso de transferências fracionadas.
            pass
        candidates.append(ev)
    if not candidates:
        return None
    candidates.sort(key=lambda e: (e.exec_date, e.created_at, e.id))
    return candidates[0].id


def _append_if_needed(event) -> bool:
    if is_duplicate(event):
        return False
    append_event(event)
    return True


def _resolve_exec_day(path: Path, payload: dict[str, Any]) -> date:
    raw = str(payload.get("exec_day", payload.get("date", path.stem))).strip()
    try:
        return date.fromisoformat(raw)
    except Exception:
        return date.fromisoformat(path.stem)


def _migrate_one_boletim_core(path: Path) -> tuple[int, list[str]]:
    payload = _read_json(path)
    exec_day = _resolve_exec_day(path, payload)
    logs: list[str] = []
    created = 0

    for mv in payload.get("cash_movements", []):
        typ = str(mv.get("type", "")).upper().strip()
        val = _safe_float(mv.get("value", mv.get("valor", 0.0)), 0.0)
        if val <= 0:
            continue
        if typ in {"APORTE", "DEPOSITO"}:
            ev = create_event(EventType.APORTE, exec_day, val, reason=mv.get("description"))
        elif typ in {"DIVIDENDO", "JCP", "BONIFICACAO", "BONUS", "SUBSCRICAO"}:
            ev = create_event(EventType.DIVIDENDO, exec_day, val, reason=mv.get("description"))
        elif typ in {"RETIRADA", "SAQUE"}:
            ev = create_event(EventType.RETIRADA, exec_day, val, reason=mv.get("description"))
        else:
            continue
        if _append_if_needed(ev):
            created += 1
            logs.append(f"+ {ev.type.value} {val:.2f} ({exec_day.isoformat()})")

    for op in payload.get("operations", []):
        typ = str(op.get("type", "")).upper().strip()
        tk = str(op.get("ticker", "")).upper().strip()
        qtd = _safe_int(op.get("qtd"), 0)
        px = _safe_float(op.get("preco"), 0.0)
        if not tk or qtd <= 0 or px <= 0:
            continue
        amount = qtd * px
        if typ == "COMPRA":
            ev = create_event(EventType.BUY, exec_day, amount, ticker=tk, qtd=qtd, price=px)
        elif typ == "VENDA":
            ev = create_event(EventType.SELL, exec_day, amount, ticker=tk, qtd=qtd, price=px)
        else:
            continue
        if _append_if_needed(ev):
            created += 1
            logs.append(f"+ {ev.type.value} {tk} {qtd}x{px:.4f} ({exec_day.isoformat()})")

    return created, logs


def _migrate_one_boletim_transfers(path: Path) -> tuple[int, list[str]]:
    payload = _read_json(path)
    exec_day = _resolve_exec_day(path, payload)
    logs: list[str] = []
    created = 0
    for tr in payload.get("cash_transfers", []):
        value = _safe_float(tr.get("value", tr.get("valor", 0.0)), 0.0)
        if value <= 0:
            continue
        note = str(tr.get("note", tr.get("ref", ""))).strip()
        ref = _find_sell_ref(note=note, value=value)
        ev = create_event(
            EventType.SETTLEMENT,
            exec_day,
            value,
            ref_id=ref,
            reason=note or "cash_transfer",
            settle_date=exec_day,
        )
        if _append_if_needed(ev):
            created += 1
            logs.append(f"+ SETTLEMENT {value:.2f} ref={ref or 'N/A'} ({exec_day.isoformat()})")
    return created, logs


def _infer_gap_events() -> list:
    # Gap identificado entre snapshots 2026-03-31 e 2026-04-01.
    gap_day = date(2026, 4, 1)
    return [
        create_event(
            EventType.SETTLEMENT,
            gap_day,
            3780.0,
            ref_id=_find_sell_ref("VENDA LASR 2026-03-31", 3780.0),
            reason="GAP-01APR transfer VENDA LASR 2026-03-31",
            settle_date=gap_day,
        ),
        create_event(EventType.BUY, gap_day, 496 * 7.67, ticker="IBRX", qtd=496, price=7.67, reason="GAP-01APR"),
        create_event(EventType.SELL, gap_day, 35 * 57.02, ticker="LASR", qtd=35, price=57.02, reason="GAP-01APR"),
        create_event(EventType.SELL, gap_day, 2 * 161.00, ticker="VICR", qtd=2, price=161.00, reason="GAP-01APR"),
        create_event(EventType.SELL, gap_day, 50 * 4.01, ticker="ROMA", qtd=50, price=4.01, reason="GAP-01APR"),
    ]


def _confirm_gap_events(events: list, auto_yes: bool) -> bool:
    print("\n=== GAP 2026-04-01 DETECTADO — CONFIRMAÇÃO DO OWNER ===")
    for ev in events:
        print(
            f"{ev.type.value:10} exec={ev.exec_date.isoformat()} "
            f"ticker={ev.ticker or '-':6} qtd={ev.qtd or 0:4} price={(ev.price or 0):8.4f} amount={ev.amount:10.2f}"
        )
    if auto_yes:
        print("auto-confirm: YES")
        return True
    choice = input("Confirmar append desses eventos no ledger? [y/N]: ").strip().lower()
    return choice == "y"


def _validate() -> None:
    # Validação de snapshot e caixa para 2026-04-02.
    snap_expected = _read_json(ROOT / "data" / "real" / "2026-04-01.json").get("positions_snapshot", [])
    snap_expected_qty: dict[str, int] = {}
    for p in snap_expected:
        tk = str(p.get("ticker", "")).upper().strip()
        snap_expected_qty[tk] = snap_expected_qty.get(tk, 0) + _safe_int(p.get("qtd"), 0)

    snap_calc = export_snapshot(date(2026, 4, 2))
    snap_calc_qty: dict[str, int] = {}
    for p in snap_calc:
        tk = str(p.get("ticker", "")).upper().strip()
        snap_calc_qty[tk] = snap_calc_qty.get(tk, 0) + _safe_int(p.get("qtd"), 0)

    if snap_calc_qty != snap_expected_qty:
        print("FAIL: snapshot calculado != snapshot esperado (2026-04-01.json)")
        all_tk = sorted(set(snap_calc_qty) | set(snap_expected_qty))
        for tk in all_tk:
            a = snap_calc_qty.get(tk, 0)
            b = snap_expected_qty.get(tk, 0)
            if a != b:
                print(f"  {tk}: calc={a} expected={b}")
        raise SystemExit(1)

    cash = compute_cash(date(2026, 4, 2))
    if abs(cash["cash_free"] - 2525.34) >= 0.02 or abs(cash["cash_accounting"]) >= 0.01:
        print("FAIL: cash não confere para 2026-04-02")
        print(f"  cash_free={cash['cash_free']:.6f} expected=2525.340000")
        print(f"  cash_accounting={cash['cash_accounting']:.6f} expected~0")
        raise SystemExit(1)

    pending = pending_settlements(date(2026, 4, 3))
    if pending:
        print("FAIL: pending_settlements(2026-04-03) não está vazio")
        for p in pending[:10]:
            print(f"  pending={p}")
        raise SystemExit(1)

    print("VALIDAÇÃO PASS: snapshot, cash e pendências ok.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrar boletins para ledger SSOT.")
    parser.add_argument("--auto-yes-gap", action="store_true", help="Confirma automaticamente eventos do gap 2026-04-01.")
    args = parser.parse_args()

    real_dir = ROOT / "data" / "real"
    files = sorted(real_dir.glob("*.json"), key=lambda p: p.stem)
    if not files:
        print("Sem boletins para migrar.")
        return

    total_created = 0
    for p in files:
        created, logs = _migrate_one_boletim_core(p)
        total_created += created
        print(f"{p.name} [core]: {created} evento(s) novo(s)")
        for line in logs:
            print(f"  {line}")

    gap_events = _infer_gap_events()
    if _confirm_gap_events(gap_events, auto_yes=args.auto_yes_gap):
        for ev in gap_events:
            if _append_if_needed(ev):
                total_created += 1
                print(f"  + GAP {ev.type.value} {ev.ticker or '-'} amount={ev.amount:.2f}")
    else:
        print("ABORTADO: Owner não confirmou eventos do gap 2026-04-01.")
        raise SystemExit(1)

    for p in files:
        created, logs = _migrate_one_boletim_transfers(p)
        total_created += created
        print(f"{p.name} [transfers]: {created} evento(s) novo(s)")
        for line in logs:
            print(f"  {line}")

    print(f"Total de eventos adicionados: {total_created}")
    _validate()


if __name__ == "__main__":
    main()

