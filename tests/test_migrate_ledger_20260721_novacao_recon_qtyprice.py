from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pipeline.ledger as ledger
from scripts import migrate_ledger_20260721_novacao_recon_qtyprice as migration_mod


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _seed_ledger(path: Path) -> None:
    rows = [
        {
            "id": "AP1",
            "type": "APORTE",
            "exec_date": "2026-07-16",
            "created_at": "2026-07-16T14:48:03+00:00",
            "ticker": None,
            "qtd": None,
            "price": None,
            "amount": 1200.0,
            "settle_date": None,
            "ref_id": None,
            "reason": "seed",
        },
        {
            "id": "BUY-ORIG",
            "type": "BUY",
            "exec_date": "2026-07-17",
            "created_at": "2026-07-17T21:31:00+00:00",
            "ticker": "FCEL",
            "qtd": 100.0,
            "price": 10.0,
            "amount": 1000.0,
            "settle_date": "2026-07-20",
            "ref_id": None,
            "reason": "original",
        },
        {
            "id": "FEE-ORIG",
            "type": "FEE",
            "exec_date": "2026-07-17",
            "created_at": "2026-07-17T21:31:10+00:00",
            "ticker": "FCEL",
            "qtd": None,
            "price": None,
            "amount": 2.5,
            "settle_date": None,
            "ref_id": "BUY-ORIG",
            "reason": "fee",
        },
        {
            "id": "CORR-ORIG",
            "type": "CORRECTION",
            "exec_date": "2026-07-17",
            "created_at": "2026-07-20T23:58:46+00:00",
            "ticker": "FCEL",
            "qtd": None,
            "price": None,
            "amount": 0.0,
            "settle_date": None,
            "ref_id": "BUY-ORIG",
            "reason": f"Auto-reconciliacao BTG {migration_mod.NOTE_FILE}: qtd/preco/amount alinhados a nota oficial",
        },
        {
            "id": "BUY-REPL",
            "type": "BUY",
            "exec_date": "2026-07-17",
            "created_at": "2026-07-20T23:58:47+00:00",
            "ticker": "FCEL",
            "qtd": 95.0,
            "price": 10.526315,
            "amount": 1000.0,
            "settle_date": "2026-07-20",
            "ref_id": None,
            "reason": f"auto-reconciliado via {migration_mod.NOTE_FILE}",
        },
    ]
    _write_jsonl(path, rows)


def _metrics(ledger_path: Path, as_of: date) -> dict:
    previous = ledger.LEDGER_PATH
    ledger.LEDGER_PATH = ledger_path
    try:
        return {
            "pos": ledger.compute_positions(as_of),
            "cash": ledger.compute_cash(as_of),
            "fees": ledger.total_fees(as_of),
            "book": ledger.build_operations_book(as_of),
        }
    finally:
        ledger.LEDGER_PATH = previous


def test_novacao_dry_run_does_not_modify_file(tmp_path):
    ledger_path = tmp_path / "ledger_real.jsonl"
    _seed_ledger(ledger_path)
    before = ledger_path.read_text(encoding="utf-8")

    original_tickers = list(migration_mod.TARGET_TICKERS)
    migration_mod.TARGET_TICKERS = ["FCEL"]
    try:
        result = migration_mod.run_migration(ledger_path, confirm=False)
    finally:
        migration_mod.TARGET_TICKERS = original_tickers

    assert result["dry_run"] is True
    assert result["tickers_planned"] == 1
    assert result["events_appended"] == 0
    assert ledger_path.read_text(encoding="utf-8") == before


def test_novacao_confirm_is_append_only_and_preserves_metrics(tmp_path):
    ledger_path = tmp_path / "ledger_real.jsonl"
    _seed_ledger(ledger_path)
    before_text = ledger_path.read_text(encoding="utf-8")
    before_rows = [line for line in before_text.splitlines() if line.strip()]
    before_metrics = _metrics(ledger_path, date(2026, 7, 21))

    original_tickers = list(migration_mod.TARGET_TICKERS)
    migration_mod.TARGET_TICKERS = ["FCEL"]
    try:
        result = migration_mod.run_migration(ledger_path, confirm=True)
    finally:
        migration_mod.TARGET_TICKERS = original_tickers

    assert result["dry_run"] is False
    assert result["events_appended"] == 5
    backup = Path(str(result["backup_path"]))
    assert backup.exists()

    after_text = ledger_path.read_text(encoding="utf-8")
    assert after_text.startswith(before_text)
    after_rows = [line for line in after_text.splitlines() if line.strip()]
    assert len(after_rows) == len(before_rows) + 5

    details = result["details"][0]
    rows = [json.loads(line) for line in after_rows]
    by_id = {row["id"]: row for row in rows}

    cancel_repl = by_id[details["cancel_replacement_id"]]
    restored_buy = by_id[details["restored_buy_id"]]
    recon_adjust = by_id[details["recon_adjust_id"]]
    cancel_fee = by_id[details["cancel_fee_id"]]
    relink_fee = by_id[details["relinked_fee_id"]]

    assert cancel_repl["type"] == "CORRECTION"
    assert cancel_repl["ref_id"] == "BUY-REPL"
    assert restored_buy["type"] == "BUY"
    assert abs(float(restored_buy["amount"]) - 1000.0) < 0.01
    assert recon_adjust["type"] == "RECON_ADJUST"
    assert recon_adjust["ref_id"] == restored_buy["id"]
    assert abs(float(recon_adjust["amount"])) < 1e-12
    assert cancel_fee["type"] == "CORRECTION"
    assert cancel_fee["ref_id"] == "FEE-ORIG"
    assert relink_fee["type"] == "FEE"
    assert relink_fee["ref_id"] == restored_buy["id"]
    assert abs(float(relink_fee["amount"]) - 2.5) < 1e-9

    after_metrics = _metrics(ledger_path, date(2026, 7, 21))
    assert before_metrics["pos"] == after_metrics["pos"]
    assert before_metrics["book"] == after_metrics["book"]
    assert abs(before_metrics["cash"]["cash_free"] - after_metrics["cash"]["cash_free"]) < 1e-9
    assert abs(before_metrics["cash"]["cash_accounting"] - after_metrics["cash"]["cash_accounting"]) < 1e-9
    assert abs(before_metrics["fees"] - after_metrics["fees"]) < 1e-9
