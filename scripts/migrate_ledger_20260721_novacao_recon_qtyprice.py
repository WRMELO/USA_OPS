"""Novacao append-only da reconciliacao BTG de 17/07/2026.

Objetivo:
- Recuperar a base pre-reconciliacao de cada BUY afetado pela nota oficial;
- Reexpressar a reconciliacao no novo modelo (RECON_ADJUST: apenas qtd/preco);
- Preservar caixa/investido exatamente como esta hoje.

IMPORTANTE:
- Default = dry-run (nao escreve nada).
- Para escrever, use --confirm.
- Sempre cria backup antes de gravar.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pipeline.ledger as ledger_mod
from pipeline.ledger import EventType

NOTE_FILE = "Confirm_BTGP_001_BPXB000057_07172026.pdf"
NOVATION_TAG = "Novacao reconciliacao 17/07/2026"
TARGET_TICKERS = [
    "FCEL",
    "GH",
    "HNGE",
    "LQDA",
    "PGNY",
    "PRCH",
    "RDVT",
    "REPL",
    "RPD",
    "SLS",
    "SMWB",
    "SNOW",
    "TOI",
    "URGN",
    "VPG",
    "VRNS",
]


def _backup_ledger(ledger_path: Path) -> Path:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_path = ledger_path.with_name(f"{ledger_path.name}.bak_pre_novacao_{stamp}")
    shutil.copy2(ledger_path, backup_path)
    return backup_path


def _load_events(ledger_path: Path) -> list[ledger_mod.LedgerEvent]:
    previous = ledger_mod.LEDGER_PATH
    try:
        ledger_mod.LEDGER_PATH = ledger_path
        return ledger_mod.read_all_events()
    finally:
        ledger_mod.LEDGER_PATH = previous


def _build_plan(events: list[ledger_mod.LedgerEvent]) -> list[dict[str, Any]]:
    by_id = {ev.id: ev for ev in events}
    plan: list[dict[str, Any]] = []

    for ticker in TARGET_TICKERS:
        correction_candidates = [
            ev
            for ev in events
            if ev.type == EventType.CORRECTION
            and (ev.ticker or "").upper().strip() == ticker
            and NOTE_FILE in (ev.reason or "")
            and ev.ref_id
        ]
        if len(correction_candidates) != 1:
            raise SystemExit(
                f"[{ticker}] esperado 1 CORRECTION da nota {NOTE_FILE}, encontrado {len(correction_candidates)}"
            )
        correction_event = correction_candidates[0]
        original_buy = by_id.get(correction_event.ref_id or "")
        if original_buy is None or original_buy.type != EventType.BUY:
            raise SystemExit(f"[{ticker}] BUY original ausente/invalidado para ref_id={correction_event.ref_id}")
        if (original_buy.ticker or "").upper().strip() != ticker:
            raise SystemExit(f"[{ticker}] ticker do BUY original nao confere: {(original_buy.ticker or '').upper().strip()}")

        replacement_candidates = [
            ev
            for ev in events
            if ev.type == EventType.BUY
            and (ev.ticker or "").upper().strip() == ticker
            and ev.id != original_buy.id
            and ev.exec_date == original_buy.exec_date
            and NOTE_FILE in (ev.reason or "")
        ]
        if len(replacement_candidates) != 1:
            raise SystemExit(
                f"[{ticker}] esperado 1 BUY reemitido da nota {NOTE_FILE}, encontrado {len(replacement_candidates)}"
            )
        replacement_buy = replacement_candidates[0]

        fee_candidates = [
            ev
            for ev in events
            if ev.type == EventType.FEE
            and (ev.ticker or "").upper().strip() == ticker
            and (ev.ref_id or "") == original_buy.id
        ]
        if len(fee_candidates) != 1:
            raise SystemExit(f"[{ticker}] esperado 1 FEE ligado ao BUY original, encontrado {len(fee_candidates)}")
        old_fee = fee_candidates[0]

        already_novated = any(
            ev.type == EventType.CORRECTION and (ev.ref_id or "") == replacement_buy.id for ev in events
        )
        if already_novated:
            raise SystemExit(f"[{ticker}] BUY reemitido ja foi cancelado por CORRECTION; novacao parece ja aplicada.")

        plan.append(
            {
                "ticker": ticker,
                "original_buy": original_buy,
                "replacement_buy": replacement_buy,
                "old_fee": old_fee,
            }
        )

    return plan


def run_migration(ledger_path: Path, confirm: bool = False) -> dict[str, Any]:
    if not ledger_path.exists():
        raise FileNotFoundError(f"Ledger nao encontrado: {ledger_path}")

    events = _load_events(ledger_path)
    plan = _build_plan(events)
    details: list[dict[str, Any]] = []

    for row in plan:
        original_buy = row["original_buy"]
        replacement_buy = row["replacement_buy"]
        old_fee = row["old_fee"]
        print(
            f"[PLAN] {row['ticker']} "
            f"original={original_buy.id} replacement={replacement_buy.id} fee={old_fee.id}"
        )
        details.append(
            {
                "ticker": row["ticker"],
                "original_buy_id": original_buy.id,
                "replacement_buy_id": replacement_buy.id,
                "old_fee_id": old_fee.id,
            }
        )

    if not confirm:
        print("[DRY-RUN] Nenhuma alteracao escrita no ledger.")
        return {
            "ok": True,
            "dry_run": True,
            "ledger_path": str(ledger_path),
            "tickers_planned": len(plan),
            "events_appended": 0,
            "backup_path": None,
            "details": details,
        }

    backup_path = _backup_ledger(ledger_path)
    previous = ledger_mod.LEDGER_PATH
    appended_events = 0
    try:
        ledger_mod.LEDGER_PATH = ledger_path
        for row in plan:
            original_buy = row["original_buy"]
            replacement_buy = row["replacement_buy"]
            old_fee = row["old_fee"]
            ticker = row["ticker"]

            cancel_replacement = ledger_mod.create_event(
                EventType.CORRECTION,
                replacement_buy.exec_date,
                0.0,
                ticker=replacement_buy.ticker,
                ref_id=replacement_buy.id,
                reason=(
                    f"{NOVATION_TAG}: cancela BUY reemitido da auto-reconciliacao de {NOTE_FILE} "
                    f"(ticker {ticker})"
                ),
            )
            ledger_mod.append_event(cancel_replacement)
            appended_events += 1

            restored_buy = ledger_mod.create_event(
                EventType.BUY,
                original_buy.exec_date,
                float(original_buy.amount),
                ticker=original_buy.ticker,
                qtd=float(original_buy.qtd or 0.0),
                price=float(original_buy.price or 0.0),
                settle_date=original_buy.settle_date,
                reason=(
                    f"{(original_buy.reason or '').strip()} | {NOVATION_TAG}: restaura base pre-reconciliacao"
                ).strip(" |"),
            )
            ledger_mod.append_event(restored_buy)
            appended_events += 1

            recon_adjust = ledger_mod.create_event(
                EventType.RECON_ADJUST,
                restored_buy.exec_date,
                0.0,
                ticker=restored_buy.ticker,
                qtd=float(replacement_buy.qtd or 0.0),
                price=float(replacement_buy.price or 0.0),
                ref_id=restored_buy.id,
                reason=(
                    f"{NOVATION_TAG}: aplica qtd/preco reconciliados da nota {NOTE_FILE} "
                    "(sem alterar investido)"
                ),
            )
            ledger_mod.append_event(recon_adjust)
            appended_events += 1

            cancel_fee = ledger_mod.create_event(
                EventType.CORRECTION,
                old_fee.exec_date,
                0.0,
                ticker=old_fee.ticker,
                ref_id=old_fee.id,
                reason=f"{NOVATION_TAG}: cancela FEE antigo para relink",
            )
            ledger_mod.append_event(cancel_fee)
            appended_events += 1

            relinked_fee = ledger_mod.create_event(
                EventType.FEE,
                old_fee.exec_date,
                float(old_fee.amount),
                ticker=old_fee.ticker,
                ref_id=restored_buy.id,
                reason=f"{(old_fee.reason or '').strip()} | {NOVATION_TAG}: relink para BUY restaurado".strip(" |"),
            )
            ledger_mod.append_event(relinked_fee)
            appended_events += 1

            details_row = next(item for item in details if item["ticker"] == ticker)
            details_row.update(
                {
                    "cancel_replacement_id": cancel_replacement.id,
                    "restored_buy_id": restored_buy.id,
                    "recon_adjust_id": recon_adjust.id,
                    "cancel_fee_id": cancel_fee.id,
                    "relinked_fee_id": relinked_fee.id,
                }
            )
            print(
                f"[APPLIED] {ticker} "
                f"cancel_replacement={cancel_replacement.id} restored_buy={restored_buy.id} "
                f"recon_adjust={recon_adjust.id} relinked_fee={relinked_fee.id}"
            )
    finally:
        ledger_mod.LEDGER_PATH = previous

    print(f"[CONFIRM] Backup criado em: {backup_path}")
    print(f"[CONFIRM] Ledger atualizado: {ledger_path}")
    return {
        "ok": True,
        "dry_run": False,
        "ledger_path": str(ledger_path),
        "tickers_planned": len(plan),
        "events_appended": appended_events,
        "backup_path": str(backup_path),
        "details": details,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Novacao append-only da reconciliacao de 17/07/2026")
    parser.add_argument(
        "--ledger-path",
        type=Path,
        default=ROOT / "data" / "live_real_test" / "ledger_real.jsonl",
        help="Caminho do ledger real",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--confirm", action="store_true", help="Aplica a novacao no ledger")
    mode.add_argument("--dry-run", action="store_true", help="Executa somente simulacao (default)")
    args = parser.parse_args()

    output = run_migration(args.ledger_path, confirm=args.confirm)
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
