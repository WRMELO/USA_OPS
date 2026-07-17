from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import reconcile_broker_note as reconcile_mod

NOTE_PATH = Path("/home/wilson/SALA_DE_CONTROLE/dados_oficiais_btg/Confirm_BTGP_001_BPXB000057_07162026.pdf")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


@pytest.mark.skipif(not NOTE_PATH.exists(), reason="nota BTG real nao encontrada neste ambiente")
def test_parse_real_note_and_aggregate_expected_totals():
    rows = reconcile_mod.parse_note(NOTE_PATH)
    agg = reconcile_mod.aggregate_by_ticker_date(rows)
    by_ticker = {row["ticker"]: row for row in agg if row["action"] == "BUY"}

    mrvi = by_ticker["MRVI"]
    hpp = by_ticker["HPP"]

    assert abs(float(mrvi["qty"]) - 142.88572) < 1e-4
    assert abs(float(mrvi["principal"]) - 1000.0) < 0.01
    assert abs(float(hpp["qty"]) - 62.55004003) < 1e-4
    assert abs(float(hpp["principal"]) - 1000.0) < 0.01


def test_mmddyyyy_from_filename_extracts_trade_day():
    note = Path("Confirm_BTGP_001_BPXB000057_07162026.pdf")
    assert reconcile_mod._mmddyyyy_from_filename(note) == "2026-07-16"


def test_propose_and_resolve_are_idempotent_for_accepted_keys(tmp_path, monkeypatch):
    note_path = tmp_path / "Confirm_BTGP_001_BPXB000057_07162026.pdf"
    note_path.write_text("placeholder", encoding="utf-8")

    synthetic_rows = [
        {
            "ticker": "MRVI",
            "action": "BUY",
            "qty": 142.88572,
            "price": 6.9986,
            "trade_date": "2026-07-16",
            "settle_date": "2026-07-17",
            "principal": 1000.0,
            "commission": 2.5,
            "transaction_fee": 0.0,
            "other_fees": 0.0,
            "net": 1002.5,
        }
    ]
    monkeypatch.setattr(reconcile_mod, "parse_note", lambda _path: synthetic_rows)

    ledger_dir = tmp_path / "live_real"
    ledger_path = ledger_dir / "ledger_real.jsonl"
    _write_jsonl(
        ledger_path,
        [
            {
                "id": "BUY-1",
                "type": "BUY",
                "exec_date": "2026-07-16",
                "created_at": "2026-07-16T19:13:07.552563+00:00",
                "ticker": "MRVI",
                "qtd": 142,
                "price": 7.0,
                "amount": 994.0,
                "settle_date": "2026-07-17",
                "ref_id": None,
                "reason": "manual buy",
            },
            {
                "id": "FEE-1",
                "type": "FEE",
                "exec_date": "2026-07-16",
                "created_at": "2026-07-16T20:43:39.633970+00:00",
                "ticker": "MRVI",
                "qtd": None,
                "price": None,
                "amount": 2.5,
                "settle_date": None,
                "ref_id": "BUY-1",
                "reason": "fee",
            },
        ],
    )

    first = reconcile_mod.propose(note_path, ledger_dir)
    assert first["proposals_created"] == 1
    assert first["items"][0]["status"] == "divergencia"

    second_without_decision = reconcile_mod.propose(note_path, ledger_dir)
    assert second_without_decision["proposals_created"] == 0
    assert second_without_decision["items"][0]["status"] == "divergencia_pendente"

    reconcile_mod.resolve(
        note_path=note_path,
        ticker="MRVI",
        trade_date="2026-07-16",
        decision="aceita",
        comentario="validado pelo owner",
        action="BUY",
        ledger_dir=ledger_dir,
    )

    third_after_decision = reconcile_mod.propose(note_path, ledger_dir)
    assert third_after_decision["proposals_created"] == 0
    assert third_after_decision["items"][0]["status"] == "ja_decidido_aceita"

    log_entries = [
        json.loads(line)
        for line in (ledger_dir / "reconciliation_log.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    proposals = [entry for entry in log_entries if entry.get("type") == "PROPOSTA"]
    assert len(proposals) == 1
