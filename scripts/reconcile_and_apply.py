"""Reconciliacao autonoma BTG x ledger real (D-123/USA D-145, R-058/R-059).

Difere de reconcile_broker_note.py (propositivo): esta ferramenta APLICA
ajustes autonoma apenas para divergencia de quantidade/preco sem impacto de
caixa. Divergencia que mova `amount` ou `commission` permanece supervisonada
via PROPOSTA no log oficial (R-056). Notas oficiais BTG sao SSOT.
Toda escrita e append-only; para ajuste imaterial usa EventType.RECON_ADJUST
(`amount=0`) ancorado ao evento original, sem sobrescrita destrutiva.

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
    _active_matching_events,
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


def _apply_recon_adjust(
    ledger_dir: Path,
    matching_events: list[Any],
    item: dict[str, Any],
    note_path: Path,
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
        target = buy_or_sell[0]
        new_qty = round(float(item["qty"]), 8)
        new_price = round(float(item["avg_price"]), 6)
        adjust_event = ledger_mod.create_event(
            EventType.RECON_ADJUST,
            target.exec_date,
            0.0,
            ticker=target.ticker,
            qtd=new_qty,
            price=new_price,
            ref_id=target.id,
            reason=(
                f"Auto-reconciliacao BTG {note_path.name}: qtd/preco alinhados a nota oficial "
                "(sem alterar investido)"
            ),
        )
        ledger_mod.append_event(adjust_event)

        return {
            "applied": True,
            "target_event_id": target.id,
            "adjust_event_id": adjust_event.id,
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

            matching = _active_matching_events(events, item)
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

            qty_only_adjustable = abs(qty_diff) > 1e-6 and abs(amount_diff) <= 0.01 and abs(fee_diff) <= 0.01

            if qty_only_adjustable:
                if dry_run:
                    row["would_apply"] = True
                    row["mode"] = "recon_adjust"
                    applied_items.append(row)
                    continue
                backup_path = _backup_ledger(ledger_path, item["ticker"])
                result = _apply_recon_adjust(ledger_dir, matching, item, note_path)
                row["applied"] = result.get("applied", False)
                row["backup_path"] = str(backup_path)
                row["mode"] = "recon_adjust"
                if not result.get("applied"):
                    row["error"] = result.get("reason")
                    note_fully_resolved = False
                applied_items.append(row)
            else:
                row["status"] = "divergencia_caixa_requer_supervisao"
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
        commit_msg = f"fix(ledger): auto-reconcile BTG qty/price {tickers} (sem alterar investido)"
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
