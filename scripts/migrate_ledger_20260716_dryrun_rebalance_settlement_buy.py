"""Remediacao append-only do dry-run US a partir de 2026-07-16.

Objetivo:
- Congelar o plano de compra no rebalance de 2026-07-15;
- Aplicar automaticamente liquidacao + compras em 2026-07-16;
- Regerar os boletins derivados de 2026-07-15 em diante, sem reescrever ledger.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import dryrun_autosave, painel_diario, servidor
from pipeline.ledger import compute_cash, read_all_events

SEED_EXEC_DAY = date(2026, 7, 15)
FIRST_EXEC_DAY = date(2026, 7, 16)
FIRST_MARKET_DAY = date(2026, 7, 15)
AUTOSAVE_LOG_PATH = ROOT / "data" / "daily" / "autosave_log.jsonl"
PENDING_PLAN_PATH = ROOT / "data" / "daily" / "pending_rebalance_buy.json"


@dataclass
class PendingPlanSnapshot:
    existed: bool
    content: str | None


def _snapshot_pending_plan() -> PendingPlanSnapshot:
    if not PENDING_PLAN_PATH.exists():
        return PendingPlanSnapshot(existed=False, content=None)
    return PendingPlanSnapshot(existed=True, content=PENDING_PLAN_PATH.read_text(encoding="utf-8"))


def _restore_pending_plan(snapshot: PendingPlanSnapshot) -> None:
    if snapshot.existed:
        PENDING_PLAN_PATH.parent.mkdir(parents=True, exist_ok=True)
        PENDING_PLAN_PATH.write_text(snapshot.content or "", encoding="utf-8")
        return
    if PENDING_PLAN_PATH.exists():
        PENDING_PLAN_PATH.unlink()


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as fp:
        return sum(1 for _ in fp)


def _existing_market_days_from(start_day: date) -> list[date]:
    real_dir = ROOT / "data" / "real"
    if not real_dir.exists():
        return []
    out: list[date] = []
    for p in real_dir.glob("*.json"):
        try:
            market_day = date.fromisoformat(p.stem)
        except ValueError:
            continue
        if market_day >= start_day:
            out.append(market_day)
    out.sort()
    return out


def _build_payload_for_market_day(market_day: date, exec_day: date, computed: dict[str, Any]) -> dict[str, Any]:
    computed_market_day = str(computed.get("market_day", "")).strip()
    if computed_market_day and computed_market_day != market_day.isoformat():
        raise RuntimeError(
            "Autosave inconsistente na migracao: "
            f"market_day esperado={market_day.isoformat()} calculado={computed_market_day}"
        )
    return {
        "exec_day": str(computed.get("exec_day", exec_day.isoformat())),
        "market_day": market_day.isoformat(),
        "trade_day": str(computed.get("trade_day", exec_day.isoformat())),
        "operations": list(computed.get("operations", [])),
        "cash_movements": [],
        "cash_transfers": list(computed.get("cash_transfers", [])),
    }


def _append_remediation_log(payload: dict[str, Any]) -> None:
    AUTOSAVE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "source": "remediation",
        "market_day": str(payload.get("market_day", "")),
        "exec_day": str(payload.get("exec_day", "")),
        "trade_day": str(payload.get("trade_day", "")),
        "n_operations": len(payload.get("operations", [])),
        "n_cash_transfers": len(payload.get("cash_transfers", [])),
    }
    with AUTOSAVE_LOG_PATH.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(entry, ensure_ascii=False) + "\n")


def run_migration(confirm: bool = False) -> dict[str, Any]:
    ledger_path = ROOT / "data" / "ssot" / "ledger.jsonl"
    before_events = read_all_events()
    before_ids = {ev.id for ev in before_events}
    before_lines = _line_count(ledger_path)
    cash_before = compute_cash(FIRST_EXEC_DAY)

    market_days = _existing_market_days_from(FIRST_MARKET_DAY)
    if not market_days:
        raise RuntimeError("Nenhum market_day encontrado em data/real >= 2026-07-15.")

    pending_snapshot = _snapshot_pending_plan()
    applied_days: list[str] = []
    preview_rows: list[dict[str, Any]] = []

    try:
        seed_preview = painel_diario.compute_dryrun_autosave_operations(SEED_EXEC_DAY)
        preview_rows.append(
            {
                "market_day": seed_preview.get("market_day"),
                "exec_day": seed_preview.get("exec_day"),
                "n_operations": len(seed_preview.get("operations", [])),
                "n_cash_transfers": len(seed_preview.get("cash_transfers", [])),
                "note": "seed_rebalance_plan_only",
            }
        )

        first_computed = painel_diario.compute_dryrun_autosave_operations(FIRST_EXEC_DAY)
        first_payload = _build_payload_for_market_day(FIRST_MARKET_DAY, FIRST_EXEC_DAY, first_computed)
        preview_rows.append(
            {
                "market_day": first_payload["market_day"],
                "exec_day": first_payload["exec_day"],
                "n_operations": len(first_payload["operations"]),
                "n_cash_transfers": len(first_payload["cash_transfers"]),
                "note": "first_rebuild_day",
            }
        )

        if confirm:
            servidor.apply_boletim_operations(first_payload)
            _append_remediation_log(first_payload)
            applied_days.append(first_payload["market_day"])

        for market_day in market_days:
            if market_day <= FIRST_MARKET_DAY:
                continue
            exec_day = dryrun_autosave._market_day_to_exec_day(market_day)
            computed = painel_diario.compute_dryrun_autosave_operations(exec_day)
            payload = _build_payload_for_market_day(market_day, exec_day, computed)
            preview_rows.append(
                {
                    "market_day": payload["market_day"],
                    "exec_day": payload["exec_day"],
                    "n_operations": len(payload["operations"]),
                    "n_cash_transfers": len(payload["cash_transfers"]),
                }
            )
            if confirm:
                servidor.apply_boletim_operations(payload)
                _append_remediation_log(payload)
                applied_days.append(payload["market_day"])
    finally:
        if not confirm:
            _restore_pending_plan(pending_snapshot)

    after_events = read_all_events()
    after_lines = _line_count(ledger_path)
    after_ids = {ev.id for ev in after_events}
    added_ids = sorted(after_ids - before_ids)
    cash_after = compute_cash(FIRST_EXEC_DAY)

    status = "APPLIED" if confirm else "DRY_RUN"
    if confirm and not added_ids:
        status = "NOOP_DUPLICATE"

    return {
        "ok": True,
        "status": status,
        "confirm": confirm,
        "seed_exec_day": SEED_EXEC_DAY.isoformat(),
        "first_exec_day": FIRST_EXEC_DAY.isoformat(),
        "first_market_day": FIRST_MARKET_DAY.isoformat(),
        "market_days_scope": [d.isoformat() for d in market_days],
        "applied_market_days": applied_days,
        "preview": preview_rows,
        "ledger_path": str(ledger_path),
        "ledger_lines_before": before_lines,
        "ledger_lines_after": after_lines,
        "events_added": len(added_ids),
        "added_event_ids": added_ids,
        "cash_before_first_exec_day": cash_before,
        "cash_after_first_exec_day": cash_after,
        "pending_plan_exists_after": PENDING_PLAN_PATH.exists(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Remediacao append-only do dry-run US (2026-07-16+).")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Aplica alteracoes no ledger/boletins. Sem --confirm roda em dry-run.",
    )
    args = parser.parse_args()
    result = run_migration(confirm=bool(args.confirm))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
