"""Conferencia de nota BTG/DriveWealth contra ledger real (modo propositivo).

Este script nunca escreve no ledger. Ele apenas:
1) Le a nota oficial em PDF;
2) Compara com o ledger_real.jsonl;
3) Registra propostas/decisoes em reconciliation_log.jsonl.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pdfplumber

import pipeline.ledger as ledger_mod
from pipeline.ledger import EventType

DEFAULT_LEDGER_DIR = ROOT / "data" / "live_real_test"
LOG_FILE = "reconciliation_log.jsonl"

_EXEC_RE = re.compile(
    r"^(?P<ticker>[A-Z][A-Z0-9]{0,9})\s+.+?\s+"
    r"(?P<action>Buy|Sell)\s+"
    r"\d{1,2}:\d{2}:\d{2}\s+(?:AM|PM)\s+"
    r"(?P<qty>\d+(?:\.\d+)?)\s+"
    r"(?P<price>\d+(?:\.\d+)?)\s+"
    r"(?P<trade_date>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?P<settle_date>\d{1,2}/\d{1,2}/\d{4})\s+\S+"
)
NOTE_FILENAME_RE = re.compile(r"^Confirm_.*\.pdf$", flags=re.IGNORECASE)


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _resolve_ledger_dir(ledger_dir: Path) -> Path:
    return ledger_dir if ledger_dir.is_absolute() else (ROOT / ledger_dir).resolve()


def _usd_from_line(line: str) -> float:
    m = re.search(r"\$([\-0-9,]+\.[0-9]{2})", line)
    if not m:
        return 0.0
    return float(m.group(1).replace(",", ""))


def _mdy_to_iso(raw: str) -> str:
    mm, dd, yyyy = raw.split("/")
    return date(int(yyyy), int(mm), int(dd)).isoformat()


def _mmddyyyy_from_filename(note_path: Path) -> str:
    m = re.search(r"(\d{2})(\d{2})(\d{4})\.pdf$", note_path.name, flags=re.IGNORECASE)
    if not m:
        raise ValueError(f"Nao foi possivel extrair MMDDYYYY do nome da nota: {note_path.name}")
    mm, dd, yyyy = m.groups()
    return date(int(yyyy), int(mm), int(dd)).isoformat()


def _read_log_entries(log_path: Path) -> list[dict[str, Any]]:
    if not log_path.exists():
        return []
    out: list[dict[str, Any]] = []
    for raw in log_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _append_log_entry(log_path: Path, payload: dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
        fp.flush()


def _build_key(note_path: Path, ticker: str, action: str, trade_date: str) -> str:
    return f"{note_path.name}|{ticker.upper().strip()}|{action.upper().strip()}|{trade_date}"


def parse_note(note_path: Path) -> list[dict[str, Any]]:
    if not note_path.exists():
        raise FileNotFoundError(f"Nota nao encontrada: {note_path}")

    lines: list[str] = []
    with pdfplumber.open(note_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw in text.splitlines():
                line = raw.strip()
                if line:
                    lines.append(line)

    records: list[dict[str, Any]] = []
    i = 0
    while i < len(lines):
        m = _EXEC_RE.match(lines[i])
        if not m:
            i += 1
            continue

        row = {
            "ticker": m.group("ticker").upper().strip(),
            "action": m.group("action").upper().strip(),
            "qty": float(m.group("qty")),
            "price": float(m.group("price")),
            "trade_date": _mdy_to_iso(m.group("trade_date")),
            "settle_date": _mdy_to_iso(m.group("settle_date")),
            "principal": 0.0,
            "commission": 0.0,
            "transaction_fee": 0.0,
            "other_fees": 0.0,
            "net": 0.0,
        }

        j = i + 1
        while j < len(lines) and not _EXEC_RE.match(lines[j]):
            line = lines[j]
            if re.match(r"^Principal Amount\s+\$", line):
                row["principal"] = _usd_from_line(line)
            elif re.match(r"^Commission\s+\$", line):
                row["commission"] = _usd_from_line(line)
            elif re.match(r"^Transaction Fee\s+\$", line):
                row["transaction_fee"] = _usd_from_line(line)
            elif re.match(r"^Other Fees / Credits\s+\$", line):
                row["other_fees"] = _usd_from_line(line)
            elif re.match(r"^Net Amount\s+\$", line):
                row["net"] = _usd_from_line(line)
            j += 1

        records.append(row)
        i = j

    return records


def aggregate_by_ticker_date(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in records:
        key = (row["ticker"], row["action"], row["trade_date"])
        target = grouped.setdefault(
            key,
            {
                "ticker": row["ticker"],
                "action": row["action"],
                "trade_date": row["trade_date"],
                "settle_date": row["settle_date"],
                "qty": 0.0,
                "principal": 0.0,
                "commission": 0.0,
                "transaction_fee": 0.0,
                "other_fees": 0.0,
                "net": 0.0,
            },
        )
        target["qty"] += float(row["qty"])
        target["principal"] += float(row["principal"])
        target["commission"] += float(row["commission"])
        target["transaction_fee"] += float(row["transaction_fee"])
        target["other_fees"] += float(row["other_fees"])
        target["net"] += float(row["net"])

    out: list[dict[str, Any]] = []
    for key in sorted(grouped.keys()):
        item = grouped[key]
        qty = float(item["qty"])
        principal = float(item["principal"])
        item["qty"] = round(qty, 8)
        item["principal"] = round(principal, 2)
        item["commission"] = round(float(item["commission"]), 2)
        item["transaction_fee"] = round(float(item["transaction_fee"]), 2)
        item["other_fees"] = round(float(item["other_fees"]), 2)
        item["net"] = round(float(item["net"]), 2)
        item["avg_price"] = round((principal / qty) if qty > 0 else 0.0, 6)
        out.append(item)
    return out


def _load_ledger_events(ledger_dir: Path) -> list[ledger_mod.LedgerEvent]:
    previous = ledger_mod.LEDGER_PATH
    try:
        ledger_mod.LEDGER_PATH = ledger_dir / "ledger_real.jsonl"
        return ledger_mod.read_all_events()
    finally:
        ledger_mod.LEDGER_PATH = previous


def _active_matching_events(
    events: list[ledger_mod.LedgerEvent],
    item: dict[str, Any],
) -> list[ledger_mod.LedgerEvent]:
    corrected_ids = {ev.ref_id for ev in events if ev.type == EventType.CORRECTION and ev.ref_id}
    matching = [
        ev
        for ev in events
        if ev.type.value == item["action"]
        and (ev.ticker or "").upper().strip() == item["ticker"]
        and ev.exec_date.isoformat() == item["trade_date"]
    ]
    return [ev for ev in matching if ev.id not in corrected_ids]


def propose(note_path: Path, ledger_dir: Path) -> dict[str, Any]:
    ledger_dir = _resolve_ledger_dir(ledger_dir)
    note_date = _mmddyyyy_from_filename(note_path)
    records = parse_note(note_path)
    aggregates = aggregate_by_ticker_date(records)
    events = _load_ledger_events(ledger_dir)

    log_path = ledger_dir / LOG_FILE
    log_entries = _read_log_entries(log_path)
    existing_proposal_keys = {
        str(entry.get("key", ""))
        for entry in log_entries
        if entry.get("type") == "PROPOSTA" and entry.get("key")
    }
    decisions_by_key: dict[str, str] = {}
    for entry in log_entries:
        if entry.get("type") != "DECISAO":
            continue
        key = str(entry.get("key", "")).strip()
        decision = str(entry.get("decision", "")).strip().lower()
        if not key or decision not in {"aceita", "rejeitada"}:
            continue
        decisions_by_key[key] = decision

    report_items: list[dict[str, Any]] = []
    proposals_created = 0

    for item in aggregates:
        key = _build_key(note_path, item["ticker"], item["action"], item["trade_date"])
        matching = _active_matching_events(events, item)
        matching_ids = {ev.id for ev in matching}
        ledger_qty = round(sum(float(ev.qtd or 0.0) for ev in matching), 8)
        ledger_amount = round(sum(float(ev.amount or 0.0) for ev in matching), 2)
        ledger_fee = round(
            sum(
                float(ev.amount or 0.0)
                for ev in events
                if ev.type == EventType.FEE and (ev.ref_id or "") in matching_ids
            ),
            2,
        )

        qty_diff = round(float(item["qty"]) - ledger_qty, 8)
        amount_diff = round(float(item["principal"]) - ledger_amount, 2)
        fee_diff = round(float(item["commission"]) - ledger_fee, 2)
        has_divergence = abs(qty_diff) > 1e-6 or abs(amount_diff) > 0.01 or abs(fee_diff) > 0.01

        status = "sem_divergencia"
        if key in decisions_by_key:
            status = f"ja_decidido_{decisions_by_key[key]}"
            has_divergence = False
        elif has_divergence and key in existing_proposal_keys:
            status = "divergencia_pendente"
            has_divergence = False
        elif has_divergence:
            status = "divergencia"

        report_row = {
            "key": key,
            "status": status,
            "ticker": item["ticker"],
            "action": item["action"],
            "trade_date": item["trade_date"],
            "note_qty": float(item["qty"]),
            "ledger_qty": ledger_qty,
            "qty_diff": qty_diff,
            "note_principal": float(item["principal"]),
            "ledger_amount": ledger_amount,
            "amount_diff": amount_diff,
            "note_commission": float(item["commission"]),
            "ledger_commission": ledger_fee,
            "commission_diff": fee_diff,
            "note_avg_price": float(item["avg_price"]),
        }
        report_items.append(report_row)

        if status == "divergencia":
            proposal = {
                "type": "PROPOSTA",
                "created_at": _now_iso(),
                "key": key,
                "note_file": note_path.name,
                "note_date": note_date,
                "ticker": item["ticker"],
                "action": item["action"],
                "trade_date": item["trade_date"],
                "proposal": {
                    "qty_diff": qty_diff,
                    "amount_diff": amount_diff,
                    "commission_diff": fee_diff,
                    "note_qty": float(item["qty"]),
                    "note_principal": float(item["principal"]),
                    "note_commission": float(item["commission"]),
                    "ledger_qty": ledger_qty,
                    "ledger_amount": ledger_amount,
                    "ledger_commission": ledger_fee,
                },
            }
            _append_log_entry(log_path, proposal)
            proposals_created += 1

    return {
        "note_file": note_path.name,
        "note_date": note_date,
        "ledger_path": str((ledger_dir / "ledger_real.jsonl").resolve()),
        "log_path": str(log_path.resolve()),
        "items": report_items,
        "proposals_created": proposals_created,
    }


def discover_notes(notes_dir: Path) -> list[Path]:
    if not notes_dir.exists():
        return []
    return sorted(
        path for path in notes_dir.iterdir() if path.is_file() and NOTE_FILENAME_RE.match(path.name)
    )


def propose_dir(notes_dir: Path, ledger_dir: Path) -> dict[str, Any]:
    resolved_notes_dir = _resolve_ledger_dir(notes_dir)
    notes = discover_notes(resolved_notes_dir)

    results: list[dict[str, Any]] = []
    divergent_items: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    proposals_created_total = 0

    for note_path in notes:
        try:
            result = propose(note_path, ledger_dir)
        except Exception as exc:
            parse_errors.append({"note_file": note_path.name, "error": str(exc)})
            continue

        results.append(result)
        proposals_created_total += int(result["proposals_created"])
        for item in result["items"]:
            if item["status"] in {"divergencia", "divergencia_pendente"}:
                divergent_items.append({**item, "note_file": result["note_file"]})

    return {
        "notes_dir": str(resolved_notes_dir),
        "notes_found": [str(note) for note in notes],
        "results": results,
        "proposals_created_total": proposals_created_total,
        "divergent_items": divergent_items,
        "parse_errors": parse_errors,
        "has_blocking_divergence": bool(divergent_items) or bool(parse_errors),
    }


def resolve(
    note_path: Path,
    ticker: str,
    trade_date: str,
    decision: str,
    comentario: str,
    action: str,
    ledger_dir: Path,
) -> dict[str, Any]:
    ledger_dir = _resolve_ledger_dir(ledger_dir)
    key = _build_key(note_path, ticker, action, trade_date)
    payload = {
        "type": "DECISAO",
        "created_at": _now_iso(),
        "key": key,
        "note_file": note_path.name,
        "ticker": ticker.upper().strip(),
        "action": action.upper().strip(),
        "trade_date": trade_date,
        "decision": decision,
        "comentario": comentario,
    }
    _append_log_entry(ledger_dir / LOG_FILE, payload)
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Conferencia de nota BTG/DriveWealth x ledger real (modo propositivo)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_propose = sub.add_parser("propose", help="Ler nota e propor divergencias")
    p_propose.add_argument("--note", required=True, type=Path)
    p_propose.add_argument("--ledger-dir", type=Path, default=Path("data/live_real_test"))

    p_propose_dir = sub.add_parser("propose-dir", help="Varrer pasta de notas oficiais e propor divergencias")
    p_propose_dir.add_argument("--notes-dir", required=True, type=Path)
    p_propose_dir.add_argument("--ledger-dir", type=Path, default=Path("data/live_real_test"))

    p_resolve = sub.add_parser("resolve", help="Registrar decisao sobre proposta")
    p_resolve.add_argument("--note", required=True, type=Path)
    p_resolve.add_argument("--ticker", required=True)
    p_resolve.add_argument("--trade-date", required=True, help="Formato YYYY-MM-DD")
    p_resolve.add_argument("--decision", required=True, choices=["aceita", "rejeitada"])
    p_resolve.add_argument("--comentario", default="")
    p_resolve.add_argument("--action", default="BUY", choices=["BUY", "SELL"])
    p_resolve.add_argument("--ledger-dir", type=Path, default=Path("data/live_real_test"))
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "propose":
        output = propose(args.note, args.ledger_dir)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    if args.command == "propose-dir":
        output = propose_dir(args.notes_dir, args.ledger_dir)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    if args.command == "resolve":
        output = resolve(
            note_path=args.note,
            ticker=args.ticker,
            trade_date=args.trade_date,
            decision=args.decision,
            comentario=args.comentario,
            action=args.action,
            ledger_dir=args.ledger_dir,
        )
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    raise SystemExit(f"Comando invalido: {args.command}")


if __name__ == "__main__":
    main()
