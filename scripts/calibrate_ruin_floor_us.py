from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
HOLDOUT_CSV = ROOT / "backtest" / "results" / "curve_C4_K10.csv"
OUTPUT_JSON = ROOT / "data" / "live_real_test" / "ruin_floor_us.json"

START_DATE = "2022-12-31"
B = 20_000
BLOCK_LEN = 21
SEED = 42
HORIZON = 504


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_returns() -> np.ndarray:
    frame = pd.read_csv(HOLDOUT_CSV, parse_dates=["date"])
    frame = frame[frame["date"] >= START_DATE].sort_values("date").reset_index(drop=True)
    equity = frame["equity_base100"].to_numpy(dtype=float)
    if equity.size < (BLOCK_LEN + 1):
        raise ValueError(
            f"Holdout insuficiente: {equity.size} pontos para bloco={BLOCK_LEN}. "
            "Necessario ao menos block_len+1."
        )
    returns = np.diff(equity) / equity[:-1]
    if returns.size < BLOCK_LEN:
        raise ValueError(
            f"Retornos insuficientes: {returns.size} para bloco={BLOCK_LEN}."
        )
    return returns


def _bootstrap_floor_percentiles(
    returns: np.ndarray,
    *,
    n_paths: int,
    block_len: int,
    horizon: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    n_starts = returns.size - block_len + 1
    if n_starts <= 0:
        raise ValueError("Serie de retornos menor que o tamanho do bloco.")

    blocks_per_path = int(np.ceil(horizon / block_len))
    starts = rng.integers(0, n_starts, size=(n_paths, blocks_per_path), endpoint=False)
    block_offsets = np.arange(block_len, dtype=int)[None, None, :]
    take_idx = starts[:, :, None] + block_offsets
    sampled = returns[take_idx].reshape(n_paths, blocks_per_path * block_len)[:, :horizon]

    equity_paths = np.cumprod(1.0 + sampled, axis=1)
    running_min = np.minimum.accumulate(equity_paths, axis=1)
    worst_loss_pct = (running_min - 1.0) * 100.0

    p1 = np.percentile(worst_loss_pct, 1, axis=0)
    p5 = np.percentile(worst_loss_pct, 5, axis=0)
    return p1, p5


def _round4(values: np.ndarray) -> list[float]:
    return [round(float(v), 4) for v in values]


def _load_existing_payload(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _stable_computed_at(
    existing: dict | None,
    *,
    p1_pct: list[float],
    p5_pct: list[float],
) -> str:
    if not isinstance(existing, dict):
        return _utc_now_iso()
    floors = existing.get("floors", {}) if isinstance(existing.get("floors"), dict) else {}
    existing_p1 = floors.get("p1_pct")
    existing_p5 = floors.get("p5_pct")
    if existing_p1 == p1_pct and existing_p5 == p5_pct:
        params = existing.get("params", {}) if isinstance(existing.get("params"), dict) else {}
        previous = params.get("computed_at")
        if isinstance(previous, str) and previous.strip():
            return previous
    return _utc_now_iso()


def main() -> None:
    returns = _load_returns()
    p1, p5 = _bootstrap_floor_percentiles(
        returns,
        n_paths=B,
        block_len=BLOCK_LEN,
        horizon=HORIZON,
        seed=SEED,
    )
    p1_pct = _round4(p1)
    p5_pct = _round4(p5)

    existing = _load_existing_payload(OUTPUT_JSON)
    computed_at = _stable_computed_at(existing, p1_pct=p1_pct, p5_pct=p5_pct)

    payload = {
        "decision_ref": "SALA D-185 / USA D-169",
        "params": {
            "source_csv": "backtest/results/curve_C4_K10.csv",
            "holdout_start_date": START_DATE,
            "bootstrap_paths": B,
            "block_len": BLOCK_LEN,
            "seed": SEED,
            "horizon_max": HORIZON,
            "method": "block bootstrap + prefix-min (perda vs C0 em %)",
            "n_retornos_holdout": int(returns.size),
            "computed_at": computed_at,
        },
        "floors": {
            "horizons": list(range(1, HORIZON + 1)),
            "p1_pct": p1_pct,
            "p5_pct": p5_pct,
        },
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print("Pisos de ruina calibrados (perda vs C0, pior ponto ate h):")
    print("  h |      p5 |      p1")
    for h in (9, 21, 42, 63, 126, 252):
        print(f"{h:3d} | {p5_pct[h - 1]:7.2f}% | {p1_pct[h - 1]:7.2f}%")
    print(f"\nArquivo gerado: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
