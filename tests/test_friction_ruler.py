from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path


def _load_friction_module():
    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "friction_ruler.py"
    spec = importlib.util.spec_from_file_location("friction_ruler", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


friction = _load_friction_module()


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


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _append_real_buy(ledger_dir: Path, *, ticker: str, qtd: int, preco: float, exec_date: str) -> None:
    friction._configure_real_ledger(ledger_dir)
    event = friction.create_event(
        friction.EventType.BUY,
        date.fromisoformat(exec_date),
        float(qtd * preco),
        ticker=ticker,
        qtd=qtd,
        price=preco,
        reason="TEST real buy",
    )
    friction.append_event(event)


def test_record_shadow_buy_creates_event(tmp_path):
    ledger_dir = tmp_path / "live_real"
    rc = friction.main(
        [
            "record-shadow-buy",
            "--ticker",
            "FLEX",
            "--qtd",
            "10",
            "--preco",
            "100.0",
            "--exec-date",
            "2026-07-16",
            "--ledger-dir",
            str(ledger_dir),
        ]
    )
    assert rc == 0

    rows = _read_jsonl(ledger_dir / "ledger_shadow.jsonl")
    assert len(rows) == 1
    ev = rows[0]
    assert ev["type"] == "BUY"
    assert ev["ticker"] == "FLEX"
    assert ev["qtd"] == 10
    assert abs(float(ev["price"]) - 100.0) < 1e-9


def test_record_shadow_buy_is_idempotent(tmp_path):
    ledger_dir = tmp_path / "live_real"
    args = [
        "record-shadow-buy",
        "--ticker",
        "FLEX",
        "--qtd",
        "10",
        "--preco",
        "100.0",
        "--exec-date",
        "2026-07-16",
        "--ledger-dir",
        str(ledger_dir),
    ]

    rc1 = friction.main(args)
    rc2 = friction.main(args)
    assert rc1 == 0
    assert rc2 == 0

    rows = _read_jsonl(ledger_dir / "ledger_shadow.jsonl")
    assert len(rows) == 1


def test_emit_friction_report_computes_slippage_for_matched_trade(tmp_path):
    ledger_dir = tmp_path / "live_real"
    _append_real_buy(ledger_dir, ticker="FLEX", qtd=10, preco=101.0, exec_date="2026-07-16")

    rc_shadow = friction.main(
        [
            "record-shadow-buy",
            "--ticker",
            "FLEX",
            "--qtd",
            "10",
            "--preco",
            "100.0",
            "--exec-date",
            "2026-07-16",
            "--ledger-dir",
            str(ledger_dir),
        ]
    )
    assert rc_shadow == 0

    rc_emit = friction.main(
        [
            "emit-friction-report",
            "--as-of-date",
            "2026-07-16",
            "--ledger-dir",
            str(ledger_dir),
        ]
    )
    assert rc_emit == 0

    payload = _read_json(ledger_dir / "friction_report_2026-07-16.json")
    matches = payload["execution_friction"]["trades_matched"]
    assert len(matches) == 1
    assert abs(float(matches[0]["slippage_amount"]) - 10.0) < 1e-9
    assert abs(float(matches[0]["slippage_bps"]) - 100.0) < 1e-9
    assert payload["execution_friction"]["unmatched_real_buys"] == []
    assert payload["execution_friction"]["unmatched_shadow_buys"] == []
    assert payload["statistically_meaningful_return_comparison"] is False
    assert isinstance(payload["methodology_note"], str)
    assert payload["methodology_note"]


def test_emit_friction_report_flags_unmatched_buys(tmp_path):
    ledger_dir = tmp_path / "live_real"
    _append_real_buy(ledger_dir, ticker="FLEX", qtd=7, preco=99.0, exec_date="2026-07-16")

    rc_emit = friction.main(
        [
            "emit-friction-report",
            "--as-of-date",
            "2026-07-16",
            "--ledger-dir",
            str(ledger_dir),
        ]
    )
    assert rc_emit == 0

    payload = _read_json(ledger_dir / "friction_report_2026-07-16.json")
    assert payload["execution_friction"]["trades_matched"] == []
    assert len(payload["execution_friction"]["unmatched_real_buys"]) == 1
    assert payload["execution_friction"]["unmatched_real_buys"][0]["ticker"] == "FLEX"
    assert payload["execution_friction"]["total_slippage_amount"] == 0.0


def test_emit_friction_report_marks_dryrun_as_retired(tmp_path):
    ledger_dir = tmp_path / "live_real"
    real_dir = tmp_path / "real"
    real_dir.mkdir(parents=True, exist_ok=True)

    dryrun_payload = {
        "exec_day": "2026-07-15",
        "market_day": "2026-07-15",
        "positions_snapshot": [{"ticker": "FLEX", "qtd": 1, "preco_compra": 100.0}],
        "cash_free": 123.45,
        "cash_accounting": 67.89,
    }
    (real_dir / "2026-07-15.json").write_text(
        json.dumps(dryrun_payload, ensure_ascii=False),
        encoding="utf-8",
    )

    rc_emit = friction.main(
        [
            "emit-friction-report",
            "--as-of-date",
            "2026-07-16",
            "--ledger-dir",
            str(ledger_dir),
            "--real-dir",
            str(real_dir),
        ]
    )
    assert rc_emit == 0

    payload = _read_json(ledger_dir / "friction_report_2026-07-16.json")
    cross = payload["operational_crosscheck"]
    assert abs(float(cross["real_cash_free"])) < 1e-9
    assert abs(float(cross["real_cash_accounting"])) < 1e-9
    assert cross["real_n_positions"] == 0
    assert cross["dryrun_retired"] is True
    assert cross["dryrun_retirement_ref"] == "SALA D-194 / USA D-171"
    assert "dryrun_source_missing" not in cross
    assert "dryrun_source_file" not in cross
    assert "dryrun_cash_free" not in cross
    assert "dryrun_cash_accounting" not in cross
    assert "dryrun_n_positions" not in cross
