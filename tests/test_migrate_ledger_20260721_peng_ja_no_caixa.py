from __future__ import annotations

import json
from pathlib import Path

from scripts import migrate_ledger_20260721_peng_ja_no_caixa as migration


def _seed_target_sell(ledger_path: Path) -> None:
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    sell_event = {
        "id": migration.TARGET_SELL_ID,
        "type": "SELL",
        "exec_date": migration.TARGET_DAY.isoformat(),
        "created_at": "2026-07-21T13:40:49.971794+00:00",
        "ticker": "PENG",
        "qtd": 16.734846,
        "price": 55.6,
        "amount": migration.TARGET_SELL_AMOUNT,
        "settle_date": "2026-07-22",
        "ref_id": None,
        "reason": "LIVE-REAL-TEST web close",
    }
    ledger_path.write_text(json.dumps(sell_event, ensure_ascii=False) + "\n", encoding="utf-8")


def _read_events(ledger_path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in ledger_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_migration_dry_run_does_not_write(tmp_path):
    ledger_dir = tmp_path / "live_real_test"
    ledger_path = ledger_dir / migration.REAL_LEDGER_NAME
    _seed_target_sell(ledger_path)
    before = _read_events(ledger_path)

    out = migration.apply_migration(ledger_dir, confirm=False, rebuild_boletim=False)
    after = _read_events(ledger_path)

    assert out["status"] == "DRY_RUN"
    assert len(after) == len(before)


def test_migration_confirm_appends_settlement(tmp_path):
    ledger_dir = tmp_path / "live_real_test"
    ledger_path = ledger_dir / migration.REAL_LEDGER_NAME
    _seed_target_sell(ledger_path)

    out = migration.apply_migration(ledger_dir, confirm=True, rebuild_boletim=False)
    events = _read_events(ledger_path)
    settlements = [ev for ev in events if ev.get("type") == "SETTLEMENT"]

    assert out["status"] == "APPLIED"
    assert len(events) == 2
    assert len(settlements) == 1
    assert settlements[0]["ref_id"] == migration.TARGET_SELL_ID
    assert abs(float(settlements[0]["amount"]) - migration.TARGET_SELL_AMOUNT) < 0.01


def test_migration_is_idempotent(tmp_path):
    ledger_dir = tmp_path / "live_real_test"
    ledger_path = ledger_dir / migration.REAL_LEDGER_NAME
    _seed_target_sell(ledger_path)

    first = migration.apply_migration(ledger_dir, confirm=True, rebuild_boletim=False)
    second = migration.apply_migration(ledger_dir, confirm=True, rebuild_boletim=False)

    events = _read_events(ledger_path)
    settlements = [ev for ev in events if ev.get("type") == "SETTLEMENT"]

    assert first["status"] == "APPLIED"
    assert second["status"] == "NOOP_DUPLICATE"
    assert len(settlements) == 1
