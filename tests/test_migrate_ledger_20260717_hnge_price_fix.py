from __future__ import annotations

import json
from pathlib import Path

from scripts import migrate_ledger_20260717_hnge_price_fix as migration_mod


def _seed_ledger(path: Path) -> None:
    rows = [
        {
            "id": "b510ad0e-6218-4acd-aa49-57e83a78a0df",
            "type": "BUY",
            "exec_date": "2026-07-17",
            "created_at": "2026-07-17T21:31:26.239598+00:00",
            "ticker": "FCEL",
            "qtd": 53.87931034,
            "price": 18.56,
            "amount": 999.9999999104,
            "settle_date": "2026-07-20",
            "ref_id": None,
            "reason": "LIVE-REAL-TEST web close",
        },
        {
            "id": "fedd1bfa-44d8-496e-9197-a42f9dd6a03b",
            "type": "BUY",
            "exec_date": "2026-07-17",
            "created_at": "2026-07-17T21:31:26.567974+00:00",
            "ticker": "HNGE",
            "qtd": 53.87931034,
            "price": 18.56,
            "amount": 999.9999999104,
            "settle_date": "2026-07-20",
            "ref_id": None,
            "reason": "LIVE-REAL-TEST web close",
        },
        {
            "id": "1ffe27d5-a9fc-4048-8c92-14ac0bc348da",
            "type": "FEE",
            "exec_date": "2026-07-17",
            "created_at": "2026-07-17T21:31:26.568421+00:00",
            "ticker": "HNGE",
            "qtd": None,
            "price": None,
            "amount": 2.5,
            "settle_date": None,
            "ref_id": "fedd1bfa-44d8-496e-9197-a42f9dd6a03b",
            "reason": "Corretagem registrada via boletim web",
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
    assert result["updated_count"] == 1
    assert ledger_path.read_text(encoding="utf-8") == before


def test_migration_confirm_creates_backup_and_updates_hnge_row(tmp_path):
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
    assert len(rows) == 3

    by_id = {row["id"]: row for row in rows}
    hnge = by_id["fedd1bfa-44d8-496e-9197-a42f9dd6a03b"]
    fcel = by_id["b510ad0e-6218-4acd-aa49-57e83a78a0df"]
    fee = by_id["1ffe27d5-a9fc-4048-8c92-14ac0bc348da"]

    assert abs(float(hnge["qtd"]) - 11.52604887) < 1e-6
    assert abs(float(hnge["price"]) - 86.76) < 1e-6
    assert abs(float(hnge["amount"]) - 1000.0) < 0.01
    assert migration_mod.MIGRATION_REASON_SUFFIX in str(hnge.get("reason", ""))

    assert abs(float(fcel["qtd"]) - 53.87931034) < 1e-6
    assert abs(float(fcel["price"]) - 18.56) < 1e-6

    assert abs(float(fee["amount"]) - 2.5) < 1e-9
