"""Orquestrador diário USA_OPS — executa steps 04-12 (ou 01-12 com --full)."""
from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import sys
import traceback
from collections.abc import Callable
from datetime import date, timedelta
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


def _ssot_date_max_us() -> date | None:
    import pandas as pd

    path = ROOT / "data" / "ssot" / "operational_window.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=["date"])
        if df.empty:
            return None
        dt_max = pd.to_datetime(df["date"], errors="coerce").max()
        if pd.isna(dt_max):
            return None
        return dt_max.date()
    except Exception:
        return None


def _expected_ssot_min_date(run_date: date) -> date:
    wd = run_date.weekday()
    if wd == 0:
        delta = 3
    elif wd == 6:
        delta = 2
    elif wd == 1:
        delta = 4
    else:
        delta = 2
    return run_date - timedelta(days=delta)


def _assert_ssot_fresh_us(run_date: date) -> None:
    dt_max = _ssot_date_max_us()
    expected = _expected_ssot_min_date(run_date)
    if dt_max is None:
        raise RuntimeError(
            f"SSOT desatualizado: operational_window sem datas. Esperado >= {expected.isoformat()}. "
            "Rode --ingest-only primeiro."
        )
    if dt_max < expected:
        raise RuntimeError(
            f"SSOT desatualizado: última data={dt_max.isoformat()}, esperado >= {expected.isoformat()}. "
            "Rode --ingest-only primeiro."
        )


def run(
    target_date: date | None = None,
    full: bool = False,
    ingest_only: bool = False,
    decision_only: bool = False,
    dry_run: bool = False,
    on_step: Callable[[int, int, str], None] | None = None,
) -> dict:
    run_date = target_date or date.today()
    logger = setup_logging(run_date)
    if ingest_only and decision_only:
        raise ValueError("--ingest-only e --decision-only são mutuamente exclusivos.")
    mode = "FULL" if full else "DAILY"
    if ingest_only:
        mode = "INGEST_ONLY"
    elif decision_only:
        mode = "DECISION_ONLY"
    if dry_run:
        mode = f"{mode}+DRY_RUN"
    logger.info("=== USA_OPS daily pipeline started (date=%s, mode=%s) ===", run_date, mode)
    if ingest_only:
        total_steps = 5
    elif decision_only or full:
        total_steps = 13
    else:
        total_steps = 9

    def _step(n: int, label: str) -> None:
        logger.info(label)
        if on_step:
            on_step(n, total_steps, label)

    def _run_step(n: int, label: str, fn) -> object | None:
        _step(n, label)
        if dry_run:
            logger.info("[DRY-RUN] %s", label)
            return None
        return fn()

    try:
        run_ingest = bool(full or ingest_only)
        run_daily_incremental = bool((not full) and (not ingest_only) and (not decision_only))

        if run_ingest:
            _run_step(1, "Step 01: Ingest macro US...", lambda: _load_step("01_ingest_macro").run(end_date=run_date))
            _run_step(2, "Step 02: Ingest prices US...", lambda: _load_step("02_ingest_prices_us").run(end_date=run_date))
            _run_step(3, "Step 03: Ingest reference/index US...", lambda: _load_step("03_ingest_reference_us").run(end_date=run_date))
            _run_step(4, "Step 04: Build canonical US...", lambda: _load_step("04_build_canonical").run(end_date=run_date))
            _run_step(5, "Step 05: Rebuild operational window (D-026)...", lambda: _load_step("rebuild_operational_window").run(end_date=run_date))
            dt_max = _ssot_date_max_us()
            logger.info("SSOT operational_window date_max=%s", dt_max.isoformat() if dt_max else "N/A")
            if ingest_only:
                logger.info("=== Pipeline ingest-only concluído ===")
                return {"mode": "INGEST_ONLY", "ssot_date_max": dt_max.isoformat() if dt_max else None}
            base_n = 6
        elif run_daily_incremental:
            _run_step(1, "Step 00: Incremental ingest + rebuild operational window (D-026)...", lambda: _load_step("00_incremental_ingest").run(target_date=run_date))
            base_n = 2
        else:
            base_n = 6

        if decision_only:
            _assert_ssot_fresh_us(run_date)
            logger.info("SSOT freshness check PASS (operational_window)")

        # A partir daqui, opera sobre a janela operacional (não toca no SSOT full).
        os.environ["USA_OPS_CANONICAL_PATH"] = "data/ssot/operational_window.parquet"
        os.environ["USA_OPS_RAW_PATH"] = "data/ssot/operational_market_data_raw.parquet"
        os.environ["USA_OPS_BLACKLIST_PATH"] = "data/ssot/blacklist_window_us.json"

        _run_step(base_n, "Step 05: Build macro expanded features...", lambda: _load_step("05_build_macro_expanded").run(end_date=run_date))

        _run_step(base_n + 1, "Step 06: Compute M3-US scores...", lambda: _load_step("06_compute_scores").run(end_date=run_date))

        _run_step(base_n + 2, "Step 07: Build feature dataset US...", lambda: _load_step("07_build_features").run(end_date=run_date))

        _run_step(base_n + 3, "Step 08: Predict (stub sem ML trigger)...", lambda: _load_step("08_predict").run(end_date=run_date))

        _step(base_n + 4, "Step 09: Decide carteira C4 pura...")
        if dry_run:
            logger.info("[DRY-RUN] Step 09: Decide carteira C4 pura...")
            decision = {"action": "DRY_RUN", "portfolio": []}
        else:
            decision = _load_step("09_decide").run(target_date=run_date)
        logger.info(
            "Decision: action=%s n_tickers=%s",
            decision.get("action"),
            len(decision.get("portfolio", [])),
        )

        _run_step(base_n + 5, "Step 10: Extend winner curve...", lambda: _load_step("10_extend_curve").run(target_date=run_date))

        _step(base_n + 6, "Step 11: Reconcile metrics...")
        if dry_run:
            logger.info("[DRY-RUN] Step 11: Reconcile metrics...")
        else:
            recon = _load_step("11_reconcile_metrics").run()
            if recon.get("status") != "PASS":
                logger.warning("Metrics reconciliation FAIL — check logs/metrics_reconciliation.json")

        _step(base_n + 7, "Step 12: Build minimal daily panel...")
        if dry_run:
            logger.info("[DRY-RUN] Step 12: Build minimal daily panel...")
        else:
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
    parser.add_argument("--ingest-only", action="store_true", help="Run only ingestion/SSOT steps")
    parser.add_argument("--decision-only", action="store_true", help="Run only decision/panel steps")
    parser.add_argument("--dry-run", action="store_true", help="Execute flow without writing outputs")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    args = parser.parse_args()
    if args.ingest_only and args.decision_only:
        parser.error("--ingest-only e --decision-only não podem ser usados juntos")

    target = date.fromisoformat(args.date) if args.date else None
    run(
        target_date=target,
        full=args.full,
        ingest_only=bool(args.ingest_only),
        decision_only=bool(args.decision_only),
        dry_run=bool(args.dry_run),
    )


if __name__ == "__main__":
    main()
