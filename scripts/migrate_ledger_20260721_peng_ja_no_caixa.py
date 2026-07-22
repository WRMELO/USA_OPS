"""Remediacao append-only do SELL PENG 2026-07-21 (D-132 / D-150).

Cria um SETTLEMENT same-day referenciando o SELL existente para refletir
liquidacao JA_NO_CAIXA sem reescrever a linha historica do SELL.
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
from pipeline.ledger import EventType, append_event, create_event, is_duplicate, read_all_events
from scripts.live_real_cutover import build_boletim_payload

REAL_LEDGER_NAME = "ledger_real.jsonl"
TARGET_SELL_ID = "834a5aa1-bc64-4aad-93df-e35bfaa9a89e"
TARGET_SELL_AMOUNT = 930.4574376
TARGET_DAY = date(2026, 7, 21)
SETTLEMENT_REASON = (
    "Remediacao D-132/D-150: PENG 2026-07-21 liquidacao=JA_NO_CAIXA (append-only)"
)


def _resolve_ledger_dir(path: Path) -> Path:
    if path.is_absolute():
        return path
    return (ROOT / path).resolve()


def _find_target_sell() -> Any:
    for ev in read_all_events():
        if ev.id != TARGET_SELL_ID:
            continue
        if ev.type != EventType.SELL:
            raise ValueError(f"Evento alvo {TARGET_SELL_ID} existe, mas nao e SELL.")
        if abs(float(ev.amount) - TARGET_SELL_AMOUNT) > 0.01:
            raise ValueError(
                f"SELL alvo com amount divergente: esperado {TARGET_SELL_AMOUNT:.4f}, obtido {float(ev.amount):.4f}."
            )
        return ev
    raise ValueError(f"SELL alvo {TARGET_SELL_ID} nao encontrado no ledger.")


def apply_migration(
    ledger_dir: Path,
    *,
    confirm: bool = False,
    rebuild_boletim: bool = True,
) -> dict[str, Any]:
    resolved_ledger_dir = _resolve_ledger_dir(ledger_dir)
    resolved_ledger_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = resolved_ledger_dir / REAL_LEDGER_NAME

    previous_path = ledger_mod.LEDGER_PATH
    ledger_mod.LEDGER_PATH = ledger_path
    try:
        target_sell = _find_target_sell()
        settlement = create_event(
            EventType.SETTLEMENT,
            TARGET_DAY,
            float(target_sell.amount),
            settle_date=TARGET_DAY,
            ref_id=target_sell.id,
            reason=SETTLEMENT_REASON,
        )
        duplicate = is_duplicate(settlement)
        result: dict[str, Any] = {
            "ok": True,
            "confirm": confirm,
            "ledger_path": str(ledger_path),
            "target_sell_id": target_sell.id,
            "target_sell_amount": float(target_sell.amount),
            "settlement_duplicate": duplicate,
            "status": "NOOP_DUPLICATE" if duplicate else ("DRY_RUN" if not confirm else "APPLIED"),
        }
        if duplicate or not confirm:
            return result

        append_event(settlement)
        result["settlement_event_id"] = settlement.id
        result["settlement_reason"] = settlement.reason

        if rebuild_boletim:
            payload = build_boletim_payload(TARGET_DAY, ledger_dir=resolved_ledger_dir)
            boletim_path = resolved_ledger_dir / f"{TARGET_DAY.isoformat()}.json"
            boletim_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            result["boletim_rebuilt"] = str(boletim_path)
        else:
            result["boletim_rebuilt"] = "SKIPPED"

        return result
    finally:
        ledger_mod.LEDGER_PATH = previous_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migra SELL PENG 2026-07-21 para liquidacao JA_NO_CAIXA (append-only)."
    )
    parser.add_argument(
        "--ledger-dir",
        type=Path,
        default=ROOT / "data" / "live_real_test",
        help="Diretorio com ledger_real.jsonl",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Aplica alteracao no ledger. Sem --confirm roda em dry-run.",
    )
    parser.add_argument(
        "--no-rebuild-boletim",
        action="store_true",
        help="Nao regera {exec_day}.json apos aplicar.",
    )
    args = parser.parse_args()

    result = apply_migration(
        args.ledger_dir,
        confirm=bool(args.confirm),
        rebuild_boletim=not bool(args.no_rebuild_boletim),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
