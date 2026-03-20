"""Orquestrador diário USA_OPS — executa steps 04-12 (ou 01-12 com --full)."""
from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import sys
import traceback
from collections.abc import Callable
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load_step(name: str):
    path = ROOT / "pipeline" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"pipeline.{name}", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def setup_logging(log_date: date) -> logging.Logger:
    log_dir = ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"run_{log_date}.log"

    logger = logging.getLogger("usa_ops")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def run(
    target_date: date | None = None,
    full: bool = False,
    on_step: Callable[[int, int, str], None] | None = None,
) -> dict:
    run_date = target_date or date.today()
    logger = setup_logging(run_date)
    logger.info("=== USA_OPS daily pipeline started (date=%s, mode=%s) ===", run_date, "FULL" if full else "DAILY")
    total_steps = 13 if full else 9

    def _step(n: int, label: str) -> None:
        logger.info(label)
        if on_step:
            on_step(n, total_steps, label)

    try:
        if full:
            _step(1, "Step 01: Ingest macro US...")
            _load_step("01_ingest_macro").run(end_date=run_date)

            _step(2, "Step 02: Ingest prices US...")
            _load_step("02_ingest_prices_us").run(end_date=run_date)

            _step(3, "Step 03: Ingest reference/index US...")
            _load_step("03_ingest_reference_us").run(end_date=run_date)

            _step(4, "Step 04: Build canonical US...")
            _load_step("04_build_canonical").run(end_date=run_date)

            _step(5, "Step 05: Rebuild operational window (D-026)...")
            _load_step("rebuild_operational_window").run(end_date=run_date)
            base_n = 6
        else:
            _step(1, "Step 00: Incremental ingest + rebuild operational window (D-026)...")
            _load_step("00_incremental_ingest").run(target_date=run_date)
            base_n = 2

        # A partir daqui, opera sobre a janela operacional (não toca no SSOT full).
        os.environ["USA_OPS_CANONICAL_PATH"] = "data/ssot/operational_window.parquet"
        os.environ["USA_OPS_RAW_PATH"] = "data/ssot/operational_market_data_raw.parquet"
        os.environ["USA_OPS_BLACKLIST_PATH"] = "data/ssot/blacklist_window_us.json"

        _step(base_n, "Step 05: Build macro expanded features...")
        _load_step("05_build_macro_expanded").run(end_date=run_date)

        _step(base_n + 1, "Step 06: Compute M3-US scores...")
        _load_step("06_compute_scores").run(end_date=run_date)

        _step(base_n + 2, "Step 07: Build feature dataset US...")
        _load_step("07_build_features").run(end_date=run_date)

        _step(base_n + 3, "Step 08: Predict (stub sem ML trigger)...")
        _load_step("08_predict").run(end_date=run_date)

        _step(base_n + 4, "Step 09: Decide carteira C4 pura...")
        decision = _load_step("09_decide").run(target_date=run_date)
        logger.info(
            "Decision: action=%s n_tickers=%s",
            decision.get("action"),
            len(decision.get("portfolio", [])),
        )

        _step(base_n + 5, "Step 10: Extend winner curve...")
        _load_step("10_extend_curve").run(target_date=run_date)

        _step(base_n + 6, "Step 11: Reconcile metrics...")
        recon = _load_step("11_reconcile_metrics").run()
        if recon.get("status") != "PASS":
            logger.warning("Metrics reconciliation FAIL — check logs/metrics_reconciliation.json")

        _step(base_n + 7, "Step 12: Build minimal daily panel...")
        panel_path = _load_step("painel_diario").run(target_date=run_date)
        logger.info("Daily panel generated at: %s", panel_path)

        logger.info("=== Pipeline completed successfully ===")
        return decision
    except Exception as exc:  # noqa: BLE001
        logger.error("Pipeline FAILED: %s", exc)
        logger.error(traceback.format_exc())
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="USA_OPS daily pipeline")
    parser.add_argument("--full", action="store_true", help="Run full pipeline including ingestion steps 01-03")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else None
    run(target_date=target, full=args.full)


if __name__ == "__main__":
    main()
