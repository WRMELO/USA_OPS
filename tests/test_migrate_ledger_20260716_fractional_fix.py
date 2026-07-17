from __future__ import annotations

import json
from pathlib import Path

from scripts import migrate_ledger_20260716_fractional_fix as migration_mod


def _seed_ledger(path: Path) -> None:
    rows = [
        {
            "id": "22b7a015-4ffd-478d-9613-61c7261acb1b",
            "type": "APORTE",
            "exec_date": "2026-07-16",
            "created_at": "2026-07-16T14:48:03.629838+00:00",
            "ticker": None,
            "qtd": None,
            "price": None,
            "amount": 20008.72,
            "settle_date": None,
            "ref_id": None,
            "reason": "LIVE-REAL-TEST cutover C0",
        },
        {
            "id": "38086527-e7e0-48d8-aaba-0b586c54af36",
            "type": "BUY",
            "exec_date": "2026-07-16",
            "created_at": "2026-07-16T19:13:07.552563+00:00",
            "ticker": "MRVI",
            "qtd": 142,
            "price": 7.0,
            "amount": 994.0,
            "settle_date": "2026-07-17",
            "ref_id": None,
            "reason": "LIVE-REAL-TEST manual buy",
        },
        {
            "id": "46870306-b1d4-4e63-b0da-856e61358b75",
            "type": "BUY",
            "exec_date": "2026-07-16",
            "created_at": "2026-07-16T19:13:49.780347+00:00",
            "ticker": "HPP",
            "qtd": 62,
            "price": 15.99,
            "amount": 991.38,
            "settle_date": "2026-07-17",
            "ref_id": None,
            "reason": "LIVE-REAL-TEST manual buy",
        },
        {
            "id": "3572e8ac-155b-4a81-858d-dda4bf54fdba",
            "type": "FEE",
            "exec_date": "2026-07-16",
            "created_at": "2026-07-16T20:43:39.633970+00:00",
            "ticker": "MRVI",
            "qtd": None,
            "price": None,
            "amount": 2.5,
            "settle_date": None,
            "ref_id": "38086527-e7e0-48d8-aaba-0b586c54af36",
            "reason": "Corretagem retroativa 2026-07-16",
        },
        {
            "id": "99bbdd6d-09ec-4fed-9e2e-1c1fd01263cb",
            "type": "FEE",
            "exec_date": "2026-07-16",
            "created_at": "2026-07-16T20:43:40.100908+00:00",
            "ticker": "HPP",
            "qtd": None,
            "price": None,
            "amount": 2.5,
            "settle_date": None,
            "ref_id": "46870306-b1d4-4e63-b0da-856e61358b75",
            "reason": "Corretagem retroativa 2026-07-16",
        },
    ]

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def test_migration_dry_run_does_not_modify_file(tmp_path):
    ledger_path = tmp_path / "ledger_real.jsonl"
    _seed_ledger(ledger_path)
    before = ledger_path.read_text(encoding="utf-8")

    result = migration_mod.run_migration(ledger_path, confirm=False)

    assert result["dry_run"] is True
    assert result["updated_count"] == 2
    assert ledger_path.read_text(encoding="utf-8") == before


def test_migration_confirm_creates_backup_and_updates_two_buy_rows(tmp_path):
    ledger_path = tmp_path / "ledger_real.jsonl"
    _seed_ledger(ledger_path)

    result = migration_mod.run_migration(ledger_path, confirm=True)

    assert result["dry_run"] is False
    backup = Path(str(result["backup_path"]))
    assert backup.exists()

    rows = [
        json.loads(line)
        for line in ledger_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 5

    by_id = {row["id"]: row for row in rows}
    mrvi = by_id["38086527-e7e0-48d8-aaba-0b586c54af36"]
    hpp = by_id["46870306-b1d4-4e63-b0da-856e61358b75"]
    aporte = by_id["22b7a015-4ffd-478d-9613-61c7261acb1b"]
    fee_1 = by_id["3572e8ac-155b-4a81-858d-dda4bf54fdba"]
    fee_2 = by_id["99bbdd6d-09ec-4fed-9e2e-1c1fd01263cb"]

    assert abs(float(mrvi["qtd"]) - 142.88572) < 1e-6
    assert abs(float(mrvi["price"]) - 6.9986) < 1e-6
    assert abs(float(mrvi["amount"]) - 1000.0) < 0.01
    assert migration_mod.MIGRATION_REASON_SUFFIX in str(mrvi.get("reason", ""))

    assert abs(float(hpp["qtd"]) - 62.55004003) < 1e-6
    assert abs(float(hpp["price"]) - 15.9872) < 1e-6
    assert abs(float(hpp["amount"]) - 1000.0) < 0.01
    assert migration_mod.MIGRATION_REASON_SUFFIX in str(hpp.get("reason", ""))

    assert abs(float(aporte["amount"]) - 20008.72) < 1e-9
    assert abs(float(fee_1["amount"]) - 2.5) < 1e-9
    assert abs(float(fee_2["amount"]) - 2.5) < 1e-9
