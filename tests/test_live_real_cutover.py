from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path


def _load_cutover_module():
    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "live_real_cutover.py"
    spec = importlib.util.spec_from_file_location("live_real_cutover", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cutover = _load_cutover_module()


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def test_init_cutover_creates_single_aporte(tmp_path):
    ledger_dir = tmp_path / "live_real"
    rc = cutover.main(
        [
            "init-cutover",
            "--exec-date",
            "2026-07-16",
            "--aporte",
            "12345.67",
            "--ledger-dir",
            str(ledger_dir),
            "--confirm",
        ]
    )
    assert rc == 0

    rows = _read_jsonl(ledger_dir / "ledger_real.jsonl")
    assert len(rows) == 1
    ev = rows[0]
    assert ev["type"] == "APORTE"
    assert ev["exec_date"] == "2026-07-16"
    assert abs(float(ev["amount"]) - 12345.67) < 1e-9


def test_init_cutover_blocks_second_call(tmp_path):
    ledger_dir = tmp_path / "live_real"
    rc1 = cutover.main(
        [
            "init-cutover",
            "--exec-date",
            "2026-07-16",
            "--aporte",
            "1000.0",
            "--ledger-dir",
            str(ledger_dir),
            "--confirm",
        ]
    )
    assert rc1 == 0

    rc2 = cutover.main(
        [
            "init-cutover",
            "--exec-date",
            "2026-07-17",
            "--aporte",
            "2000.0",
            "--ledger-dir",
            str(ledger_dir),
            "--confirm",
        ]
    )
    assert rc2 != 0

    rows = _read_jsonl(ledger_dir / "ledger_real.jsonl")
    assert len(rows) == 1
    assert rows[0]["type"] == "APORTE"


def test_emit_boletim_contains_required_fields_after_buy(tmp_path):
    ledger_dir = tmp_path / "live_real"

    rc_init = cutover.main(
        [
            "init-cutover",
            "--exec-date",
            "2026-07-16",
            "--aporte",
            "1000.0",
            "--ledger-dir",
            str(ledger_dir),
            "--confirm",
        ]
    )
    assert rc_init == 0

    rc_buy = cutover.main(
        [
            "record-buy",
            "--ticker",
            "FLEX",
            "--qtd",
            "2",
            "--preco",
            "50.0",
            "--exec-date",
            "2026-07-16",
            "--ledger-dir",
            str(ledger_dir),
        ]
    )
    assert rc_buy == 0

    rc_emit = cutover.main(
        [
            "emit-boletim",
            "--exec-date",
            "2026-07-16",
            "--ledger-dir",
            str(ledger_dir),
        ]
    )
    assert rc_emit == 0

    boletim_path = ledger_dir / "2026-07-16.json"
    payload = json.loads(boletim_path.read_text(encoding="utf-8"))

    assert payload["exec_day"] == "2026-07-16"
    assert "positions_snapshot" in payload
    assert "cash_free" in payload
    assert "cash_accounting" in payload
    assert isinstance(payload["positions_snapshot"], list)
    assert len(payload["positions_snapshot"]) == 1
    assert payload["positions_snapshot"][0]["ticker"] == "FLEX"
    assert payload["positions_snapshot"][0]["qtd"] == 2
    assert abs(float(payload["cash_free"]) - 900.0) < 1e-9
    assert abs(float(payload["cash_accounting"]) - 0.0) < 1e-9


def test_freeze_dryrun_generates_sha256_manifest(tmp_path):
    ledger_dir = tmp_path / "live_real"
    real_dir = tmp_path / "real"
    real_dir.mkdir(parents=True, exist_ok=True)

    source_payload = {
        "exec_day": "2026-07-15",
        "positions_snapshot": [],
        "cash_free": 0.0,
        "cash_accounting": 0.0,
    }
    source_file = real_dir / "2026-07-15.json"
    source_file.write_text(json.dumps(source_payload, ensure_ascii=False), encoding="utf-8")

    rc = cutover.main(
        [
            "freeze-dryrun",
            "--market-day",
            "2026-07-15",
            "--ledger-dir",
            str(ledger_dir),
            "--real-dir",
            str(real_dir),
        ]
    )
    assert rc == 0

    manifest_file = ledger_dir / "dryrun_freeze_2026-07-15.json"
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    expected_sha = hashlib.sha256(source_file.read_bytes()).hexdigest()

    assert manifest["market_day"] == "2026-07-15"
    assert manifest["sha256"] == expected_sha
    assert manifest["snapshot"] == source_payload


def test_init_cutover_without_confirm_writes_nothing(tmp_path):
    ledger_dir = tmp_path / "live_real"
    rc = cutover.main(
        [
            "init-cutover",
            "--exec-date",
            "2026-07-16",
            "--aporte",
            "777.77",
            "--ledger-dir",
            str(ledger_dir),
        ]
    )
    assert rc == 0
    assert not (ledger_dir / "ledger_real.jsonl").exists()
