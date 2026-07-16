from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd


def _load_module():
    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "lookup_shadow_price.py"
    spec = importlib.util.spec_from_file_location("lookup_shadow_price", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


lookup = _load_module()


def _write_window(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


def test_lookup_close_found(tmp_path, capsys):
    window_path = tmp_path / "operational_window.parquet"
    _write_window(
        window_path,
        [
            {"date": "2026-07-15", "ticker": "FLEX", "close_operational": 42.5},
            {"date": "2026-07-14", "ticker": "FLEX", "close_operational": 41.0},
        ],
    )

    rc = lookup.main(
        ["--ticker", "flex", "--exec-date", "2026-07-16", "--window-path", str(window_path)]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip())
    assert out["found"] is True
    assert out["ticker"] == "FLEX"
    assert out["market_day"] == "2026-07-15"
    assert abs(float(out["close"]) - 42.5) < 1e-9


def test_lookup_close_not_found_returns_null(tmp_path, capsys):
    window_path = tmp_path / "operational_window.parquet"
    _write_window(window_path, [{"date": "2026-07-15", "ticker": "OTHER", "close_operational": 10.0}])

    rc = lookup.main(
        ["--ticker", "FLEX", "--exec-date", "2026-07-16", "--window-path", str(window_path)]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip())
    assert out["found"] is False
    assert out["close"] is None


def test_lookup_close_missing_window_file(tmp_path, capsys):
    window_path = tmp_path / "nao_existe.parquet"

    rc = lookup.main(
        ["--ticker", "FLEX", "--exec-date", "2026-07-16", "--window-path", str(window_path)]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip())
    assert out["found"] is False
    assert out["close"] is None
