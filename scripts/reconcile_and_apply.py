"""Reconciliacao autonoma BTG x ledger real (D-123/USA D-145, R-058).

Difere de reconcile_broker_note.py (propositivo): esta ferramenta APLICA
correcoes no ledger real quando o impacto liquido em caixa (cash_free) for
menor que o limiar de materialidade (default US$ 1,00). Notas oficiais BTG
sao SSOT (R-056). Toda escrita e append-only via par CORRECTION + evento
reemitido (EventType.CORRECTION, ja suportado por pipeline/ledger.py e pelo
padrao R-038) -- nunca sobrescrita destrutiva de linha existente.

Checkpoint forward-only: nota com todos os itens resolvidos avanca o
checkpoint; nota com item bloqueado por materialidade NAO avanca e fica
sinalizada para o Owner via PROPOSTA em reconciliation_log.jsonl (R-056).
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pipeline.ledger as ledger_mod
from pipeline.ledger import EventType
from scripts.reconcile_broker_note import (
    DEFAULT_LEDGER_DIR,
    LOG_FILE,
    _append_log_entry,
    _build_key,
    _load_ledger_events,
    _mmddyyyy_from_filename,
    _now_iso,
    _read_log_entries,
    _resolve_ledger_dir,
    aggregate_by_ticker_date,
    discover_notes,
    parse_note,
)

CHECKPOINT_FILE = "reconciliation_checkpoint.json"
DEFAULT_CASH_MATERIALITY_USD = 1.00
DEFAULT_NOTES_DIR = Path("/home/wilson/SALA_DE_CONTROLE/dados_oficiais_btg")


def _checkpoint_path(ledger_dir: Path) -> Path:
    return ledger_dir / CHECKPOINT_FILE


def read_checkpoint(ledger_dir: Path) -> dict[str, Any]:
    path = _checkpoint_path(ledger_dir)
    if not path.exists():
        return {
            "last_fully_reconciled_note_date": None,
            "last_fully_reconciled_note_file": None,
        }
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "last_fully_reconciled_note_date": None,
            "last_fully_reconciled_note_file": None,
        }


def write_checkpoint(ledger_dir: Path, note_date: str, note_file: str) -> None:
    path = _checkpoint_path(ledger_dir)
    payload = {
        "last_fully_reconciled_note_date": note_date,
        "last_fully_reconciled_note_file": note_file,
        "updated_at": _now_iso(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _notes_sorted_by_date(notes_dir: Path) -> list[tuple[str, Path]]:
    notes = discover_notes(notes_dir)
    dated = [(_mmddyyyy_from_filename(path), path) for path in notes]
    dated.sort(key=lambda row: row[0])
    return dated


def _backup_ledger(ledger_path: Path, tag: str) -> Path:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_path = ledger_path.with_name(f"{ledger_path.name}.bak_pre_auto_reconcile_{tag}_{stamp}")
    shutil.copy2(ledger_path, backup_path)
    return backup_path


def _git_commit_ledger(ledger_dir: Path, message: str) -> dict[str, Any]:
    repo_root = ROOT
    rel_paths = [
        str((ledger_dir / "ledger_real.jsonl").resolve().relative_to(repo_root)),
        str((ledger_dir / LOG_FILE).resolve().relative_to(repo_root)),
        str(_checkpoint_path(ledger_dir).resolve().relative_to(repo_root)),
    ]
    result: dict[str, Any] = {"add_ok": False, "commit_ok": False, "push_ok": False, "message": message}
    try:
        add_run = subprocess.run(
            ["git", "-C", str(repo_root), "add", "-f", *rel_paths],
            capture_output=True,
            text=True,
            timeout=15,
        )
        result["add_ok"] = add_run.returncode == 0
        if not result["add_ok"]:
            result["add_stderr"] = (add_run.stderr or "").strip()
            return result

        commit_run = subprocess.run(
            ["git", "-C", str(repo_root), "commit", "-m", message],
            capture_output=True,
            text=True,
            timeout=15,
        )
        out = (commit_run.stdout or "").lower() + (commit_run.stderr or "").lower()
        result["commit_ok"] = commit_run.returncode == 0
        result["nothing_to_commit"] = "nothing to commit" in out
        if not result["commit_ok"] and not result["nothing_to_commit"]:
            result["commit_stderr"] = (commit_run.stderr or "").strip()
            return result

        push_run = subprocess.run(
            ["git", "-C", str(repo_root), "push"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        result["push_ok"] = push_run.returncode == 0
        if not result["push_ok"]:
            result["push_stderr"] = (push_run.stderr or "").strip()
    except Exception as exc:
        result["exception"] = str(exc)
    return result


def _apply_correction(
    ledger_dir: Path,
    matching_events: list[Any],
    all_events: list[Any],
    item: dict[str, Any],
    note_path: Path,
    cash_delta: float,
) -> dict[str, Any]:
    previous_ledger_path = ledger_mod.LEDGER_PATH
    ledger_mod.LEDGER_PATH = ledger_dir / "ledger_real.jsonl"
    try:
        buy_or_sell = [ev for ev in matching_events if ev.type.value == item["action"]]
        if len(buy_or_sell) != 1:
            return {
                "applied": False,
                "reason": f"esperado 1 evento {item['action']} correspondente, encontrado {len(buy_or_sell)}",
            }
        original = buy_or_sell[0]
        new_qty = round(float(item["qty"]), 8)
        new_price = round(float(item["avg_price"]), 6)
        new_amount = round(float(item["principal"]), 2)

        correction_event = ledger_mod.create_event(
            EventType.CORRECTION,
            original.exec_date,
            0.0,
            ticker=original.ticker,
            ref_id=original.id,
            reason=(
                f"Auto-reconciliacao BTG {note_path.name}: qtd/preco/amount alinhados a nota oficial "
                f"(Delta caixa=US$ {cash_delta:.2f} < US$ 1,00)"
            ),
        )
        replacement_event = ledger_mod.create_event(
            EventType(item["action"]),
            original.exec_date,
            new_amount,
            ticker=original.ticker,
            qtd=new_qty,
            price=new_price,
            settle_date=original.settle_date,
            reason=(
                f"{original.reason or ''} | auto-reconciliado via {note_path.name} "
                f"(Delta caixa=US$ {cash_delta:.2f} < US$ 1,00)"
            ).strip(" |"),
        )
        ledger_mod.append_event(correction_event)
        ledger_mod.append_event(replacement_event)

        fee_events = [ev for ev in all_events if ev.type == EventType.FEE and ev.ref_id == original.id]
        new_commission = float(item.get("commission", 0.0))
        old_commission = sum(float(fee_ev.amount) for fee_ev in fee_events)
        if fee_events and abs(new_commission - old_commission) > 0.01:
            for fee_ev in fee_events:
                ledger_mod.append_event(
                    ledger_mod.create_event(
                        EventType.CORRECTION,
                        fee_ev.exec_date,
                        0.0,
                        ticker=fee_ev.ticker,
                        ref_id=fee_ev.id,
                        reason=f"Auto-reconciliacao BTG {note_path.name}: corretagem alinhada a nota oficial",
                    )
                )
            ledger_mod.append_event(
                ledger_mod.create_event(
                    EventType.FEE,
                    original.exec_date,
                    round(new_commission, 2),
                    ticker=original.ticker,
                    ref_id=replacement_event.id,
                    reason=f"Corretagem auto-reconciliada via {note_path.name}",
                )
            )

        return {
            "applied": True,
            "original_event_id": original.id,
            "replacement_event_id": replacement_event.id,
        }
    finally:
        ledger_mod.LEDGER_PATH = previous_ledger_path


def apply_dir(
    notes_dir: Path,
    ledger_dir: Path,
    cash_materiality_usd: float = DEFAULT_CASH_MATERIALITY_USD,
    dry_run: bool = True,
) -> dict[str, Any]:
    ledger_dir = _resolve_ledger_dir(ledger_dir)
    notes_dir_resolved = notes_dir if notes_dir.is_absolute() else _resolve_ledger_dir(notes_dir)
    ledger_path = ledger_dir / "ledger_real.jsonl"
    checkpoint = read_checkpoint(ledger_dir)
    last_reconciled = checkpoint.get("last_fully_reconciled_note_date")

    dated_notes = _notes_sorted_by_date(notes_dir_resolved)
    pending_notes = [(d, p) for d, p in dated_notes if last_reconciled is None or d > last_reconciled]

    applied_items: list[dict[str, Any]] = []
    blocked_items: list[dict[str, Any]] = []
    processed_notes: list[dict[str, Any]] = []

    for note_date, note_path in pending_notes:
        records = parse_note(note_path)
        aggregates = aggregate_by_ticker_date(records)
        events = _load_ledger_events(ledger_dir)
        log_path = ledger_dir / LOG_FILE
        log_entries = _read_log_entries(log_path)
        decisions_by_key = {
            str(e.get("key", "")).strip(): str(e.get("decision", "")).strip().lower()
            for e in log_entries
            if e.get("type") == "DECISAO" and e.get("key")
        }
        existing_proposal_keys = {
            str(e.get("key", ""))
            for e in log_entries
            if e.get("type") == "PROPOSTA"
        }

        note_fully_resolved = True
        for item in aggregates:
            key = _build_key(note_path, item["ticker"], item["action"], item["trade_date"])
            if key in decisions_by_key:
                continue

            matching = [
                ev
                for ev in events
                if ev.type.value == item["action"]
                and (ev.ticker or "").upper().strip() == item["ticker"]
                and ev.exec_date.isoformat() == item["trade_date"]
            ]
            if not matching:
                blocked_items.append(
                    {
                        "key": key,
                        "note_file": note_path.name,
                        "ticker": item["ticker"],
                        "reason": "sem_evento_correspondente_no_ledger",
                    }
                )
                note_fully_resolved = False
                continue

            ledger_qty = round(sum(float(ev.qtd or 0.0) for ev in matching), 8)
            ledger_amount = round(sum(float(ev.amount or 0.0) for ev in matching), 2)
            matching_ids = {ev.id for ev in matching}
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
            cash_delta = round(amount_diff + fee_diff, 2)

            if abs(qty_diff) <= 1e-6 and abs(amount_diff) <= 0.01 and abs(fee_diff) <= 0.01:
                continue

            row = {
                "key": key,
                "note_file": note_path.name,
                "ticker": item["ticker"],
                "action": item["action"],
                "trade_date": item["trade_date"],
                "qty_diff": qty_diff,
                "amount_diff": amount_diff,
                "fee_diff": fee_diff,
                "cash_delta": cash_delta,
            }

            if abs(cash_delta) < cash_materiality_usd:
                if dry_run:
                    row["would_apply"] = True
                    applied_items.append(row)
                    continue
                backup_path = _backup_ledger(ledger_path, item["ticker"])
                result = _apply_correction(ledger_dir, matching, events, item, note_path, cash_delta)
                row["applied"] = result.get("applied", False)
                row["backup_path"] = str(backup_path)
                if not result.get("applied"):
                    row["error"] = result.get("reason")
                    note_fully_resolved = False
                applied_items.append(row)
            else:
                row["status"] = "divergencia_material_bloqueada"
                if key not in existing_proposal_keys:
                    _append_log_entry(
                        log_path,
                        {
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
                                "cash_delta": cash_delta,
                                "cash_materiality_usd": cash_materiality_usd,
                                "note_qty": item["qty"],
                                "note_principal": item["principal"],
                                "note_commission": item["commission"],
                                "ledger_qty": ledger_qty,
                                "ledger_amount": ledger_amount,
                                "ledger_commission": ledger_fee,
                            },
                        },
                    )
                blocked_items.append(row)
                note_fully_resolved = False

        processed_notes.append(
            {
                "note_file": note_path.name,
                "note_date": note_date,
                "fully_resolved": note_fully_resolved,
            }
        )
        if note_fully_resolved and not dry_run:
            write_checkpoint(ledger_dir, note_date, note_path.name)

    git_result = None
    if not dry_run and any(row.get("applied") for row in applied_items):
        tickers = ",".join(sorted({row["ticker"] for row in applied_items if row.get("applied")}))
        commit_msg = f"fix(ledger): auto-reconcile BTG {tickers} (max |Delta caixa| < US$ {cash_materiality_usd:.2f})"
        git_result = _git_commit_ledger(ledger_dir, commit_msg)

    return {
        "dry_run": dry_run,
        "cash_materiality_usd": cash_materiality_usd,
        "checkpoint_before": checkpoint,
        "notes_evaluated": [p.name for _, p in pending_notes],
        "processed_notes": processed_notes,
        "applied_items": applied_items,
        "blocked_items": blocked_items,
        "has_blocking_divergence": bool(blocked_items),
        "git_result": git_result,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconciliacao autonoma BTG x ledger real (aplicacao)")
    parser.add_argument("--notes-dir", type=Path, default=DEFAULT_NOTES_DIR)
    parser.add_argument("--ledger-dir", type=Path, default=DEFAULT_LEDGER_DIR)
    parser.add_argument("--cash-materiality-usd", type=float, default=DEFAULT_CASH_MATERIALITY_USD)
    parser.add_argument("--confirm", action="store_true", help="Aplica de fato (default = dry-run)")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    output = apply_dir(
        notes_dir=args.notes_dir,
        ledger_dir=args.ledger_dir,
        cash_materiality_usd=args.cash_materiality_usd,
        dry_run=not args.confirm,
    )
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
