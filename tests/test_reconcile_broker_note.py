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


def test_discover_notes_filters_confirm_pdfs_and_ignores_noise(tmp_path):
    note_a = tmp_path / "Confirm_BTGP_001_BPXB000001_07162026.pdf"
    note_b = tmp_path / "Confirm_BTGP_001_BPXB000002_07172026.PDF"
    noise_image = tmp_path / "WhatsApp Image 2026-07-15 at 14.06.52.jpeg"
    noise_pdf = tmp_path / "nota_qualquer.pdf"

    note_a.write_text("pdf a", encoding="utf-8")
    note_b.write_text("pdf b", encoding="utf-8")
    noise_image.write_text("image", encoding="utf-8")
    noise_pdf.write_text("pdf noise", encoding="utf-8")

    found = reconcile_mod.discover_notes(tmp_path)
    assert [path.name for path in found] == [note_a.name, note_b.name]


def test_discover_notes_returns_empty_list_for_missing_dir(tmp_path):
    missing = tmp_path / "nao_existe"
    assert reconcile_mod.discover_notes(missing) == []


def test_active_matching_events_ignores_cancelled_buy(tmp_path):
    ledger_dir = tmp_path / "live_real"
    _write_jsonl(
        ledger_dir / "ledger_real.jsonl",
        [
            {
                "id": "BUY-OLD",
                "type": "BUY",
                "exec_date": "2026-07-17",
                "created_at": "2026-07-17T12:00:00+00:00",
                "ticker": "FCEL",
                "qtd": 53.87931034,
                "price": 18.56,
                "amount": 1000.0,
                "settle_date": "2026-07-20",
                "ref_id": None,
                "reason": "original",
            },
            {
                "id": "CORR-1",
                "type": "CORRECTION",
                "exec_date": "2026-07-17",
                "created_at": "2026-07-20T23:58:46+00:00",
                "ticker": "FCEL",
                "qtd": None,
                "price": None,
                "amount": 0.0,
                "settle_date": None,
                "ref_id": "BUY-OLD",
                "reason": "corrige original",
            },
            {
                "id": "BUY-NEW",
                "type": "BUY",
                "exec_date": "2026-07-17",
                "created_at": "2026-07-20T23:58:47+00:00",
                "ticker": "FCEL",
                "qtd": 53.89150562,
                "price": 18.5558,
                "amount": 1000.0,
                "settle_date": "2026-07-20",
                "ref_id": None,
                "reason": "replacement",
            },
        ],
    )
    events = reconcile_mod._load_ledger_events(ledger_dir)
    item = {"ticker": "FCEL", "action": "BUY", "trade_date": "2026-07-17"}

    matching = reconcile_mod._active_matching_events(events, item)

    assert [ev.id for ev in matching] == ["BUY-NEW"]


def test_propose_uses_only_active_event_after_correction(tmp_path, monkeypatch):
    note_path = tmp_path / "Confirm_BTGP_001_BPXB000057_07172026.pdf"
    note_path.write_text("placeholder", encoding="utf-8")

    synthetic_rows = [
        {
            "ticker": "FCEL",
            "action": "BUY",
            "qty": 53.89150562,
            "price": 18.5558,
            "trade_date": "2026-07-17",
            "settle_date": "2026-07-20",
            "principal": 1000.0,
            "commission": 0.0,
            "transaction_fee": 0.0,
            "other_fees": 0.0,
            "net": 1000.0,
        }
    ]
    monkeypatch.setattr(reconcile_mod, "parse_note", lambda _path: synthetic_rows)

    ledger_dir = tmp_path / "live_real"
    _write_jsonl(
        ledger_dir / "ledger_real.jsonl",
        [
            {
                "id": "BUY-OLD",
                "type": "BUY",
                "exec_date": "2026-07-17",
                "created_at": "2026-07-17T12:00:00+00:00",
                "ticker": "FCEL",
                "qtd": 53.87931034,
                "price": 18.56,
                "amount": 1000.0,
                "settle_date": "2026-07-20",
                "ref_id": None,
                "reason": "original",
            },
            {
                "id": "CORR-1",
                "type": "CORRECTION",
                "exec_date": "2026-07-17",
                "created_at": "2026-07-20T23:58:46+00:00",
                "ticker": "FCEL",
                "qtd": None,
                "price": None,
                "amount": 0.0,
                "settle_date": None,
                "ref_id": "BUY-OLD",
                "reason": "corrige original",
            },
            {
                "id": "BUY-NEW",
                "type": "BUY",
                "exec_date": "2026-07-17",
                "created_at": "2026-07-20T23:58:47+00:00",
                "ticker": "FCEL",
                "qtd": 53.89150562,
                "price": 18.5558,
                "amount": 1000.0,
                "settle_date": "2026-07-20",
                "ref_id": None,
                "reason": "replacement",
            },
        ],
    )

    result = reconcile_mod.propose(note_path, ledger_dir)

    assert result["proposals_created"] == 0
    assert result["items"][0]["status"] == "sem_divergencia"
    assert abs(float(result["items"][0]["ledger_amount"]) - 1000.0) < 0.01


def test_propose_dir_aggregates_multiple_notes_and_flags_blocking_divergence(tmp_path, monkeypatch):
    notes_dir = tmp_path / "notes"
    notes_dir.mkdir(parents=True, exist_ok=True)
    note_mrvi = notes_dir / "Confirm_BTGP_001_BPXB000001_07162026.pdf"
    note_hpp = notes_dir / "Confirm_BTGP_001_BPXB000002_07172026.pdf"
    noise = notes_dir / "WhatsApp Image 2026-07-15 at 14.06.52.jpeg"
    note_mrvi.write_text("mrvi", encoding="utf-8")
    note_hpp.write_text("hpp", encoding="utf-8")
    noise.write_text("noise", encoding="utf-8")

    rows_by_note = {
        note_mrvi.name: [
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
        ],
        note_hpp.name: [
            {
                "ticker": "HPP",
                "action": "BUY",
                "qty": 62.55004003,
                "price": 15.9872,
                "trade_date": "2026-07-17",
                "settle_date": "2026-07-18",
                "principal": 1000.0,
                "commission": 2.5,
                "transaction_fee": 0.0,
                "other_fees": 0.0,
                "net": 1002.5,
            }
        ],
    }

    def _fake_parse(note_path: Path) -> list[dict]:
        return rows_by_note[note_path.name]

    monkeypatch.setattr(reconcile_mod, "parse_note", _fake_parse)

    ledger_dir = tmp_path / "live_real"
    _write_jsonl(ledger_dir / "ledger_real.jsonl", [])

    result = reconcile_mod.propose_dir(notes_dir, ledger_dir)
    assert result["has_blocking_divergence"] is True
    assert result["parse_errors"] == []
    assert {Path(path).name for path in result["notes_found"]} == {note_mrvi.name, note_hpp.name}
    assert {item["ticker"] for item in result["divergent_items"]} == {"MRVI", "HPP"}


def test_propose_dir_returns_no_blocking_divergence_when_notes_dir_is_empty(tmp_path):
    notes_dir = tmp_path / "notes"
    notes_dir.mkdir(parents=True, exist_ok=True)

    result = reconcile_mod.propose_dir(notes_dir, tmp_path / "live_real")
    assert result["notes_found"] == []
    assert result["divergent_items"] == []
    assert result["parse_errors"] == []
    assert result["has_blocking_divergence"] is False


def test_propose_dir_collects_parse_errors_without_raising(tmp_path, monkeypatch):
    notes_dir = tmp_path / "notes"
    notes_dir.mkdir(parents=True, exist_ok=True)
    note = notes_dir / "Confirm_BTGP_001_BPXB000057_07162026.pdf"
    note.write_text("invalid", encoding="utf-8")

    def _broken_parse(_note_path: Path) -> list[dict]:
        raise ValueError("pdf corrompido")

    monkeypatch.setattr(reconcile_mod, "parse_note", _broken_parse)
    _write_jsonl((tmp_path / "live_real" / "ledger_real.jsonl"), [])

    result = reconcile_mod.propose_dir(notes_dir, tmp_path / "live_real")
    assert result["divergent_items"] == []
    assert len(result["parse_errors"]) == 1
    assert result["parse_errors"][0]["note_file"] == note.name
    assert "pdf corrompido" in result["parse_errors"][0]["error"]
    assert result["has_blocking_divergence"] is True
