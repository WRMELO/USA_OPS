"""Ferramentas de regua de friccao para o regime LIVE-REAL-TEST (F-17).

Este script NAO executa o corte real automaticamente.
Ele fornece comandos operacionais auditaveis para:
  - registrar BUY do gemeo sombra (preco ideal),
  - emitir relatorio de friccao de execucao (real vs sombra),
  - comparar cross-check operacional contra o dry-run paralelo.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict, deque
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pipeline.ledger as ledger  # noqa: E402
from pipeline.ledger import (  # noqa: E402
    EventType,
    append_event,
    compute_cash,
    create_event,
    export_snapshot,
    is_duplicate,
    read_all_events,
)

DEFAULT_LEDGER_DIR = ROOT / "data" / "live_real_test"
DEFAULT_REAL_DIR = ROOT / "data" / "real"


def _parse_iso_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except Exception as exc:  # pragma: no cover - argparse ja cobre boa parte
        raise argparse.ArgumentTypeError(f"Data invalida: {raw!r}. Use YYYY-MM-DD.") from exc


def _resolve_dir(raw: str | None, default_dir: Path) -> Path:
    if raw is None:
        return default_dir
    p = Path(raw)
    if not p.is_absolute():
        p = (ROOT / p).resolve()
    return p


def _configure_target_ledger(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    ledger.LEDGER_PATH = path
    return path


def _configure_real_ledger(ledger_dir: Path) -> Path:
    ledger_dir.mkdir(parents=True, exist_ok=True)
    target = ledger_dir / "ledger_real.jsonl"
    return _configure_target_ledger(target)


def _configure_shadow_ledger(ledger_dir: Path) -> Path:
    ledger_dir.mkdir(parents=True, exist_ok=True)
    target = ledger_dir / "ledger_shadow.jsonl"
    return _configure_target_ledger(target)


def _json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _safe_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _buy_events_until(ledger_path: Path, as_of: date) -> list[Any]:
    _configure_target_ledger(ledger_path)
    rows = [
        ev
        for ev in read_all_events()
        if ev.type == EventType.BUY and ev.exec_date <= as_of
    ]
    rows.sort(key=lambda ev: (ev.exec_date, ev.created_at, ev.id))
    return rows


def _match_key(ev: Any) -> tuple[str, str, int]:
    return ((ev.ticker or "").upper(), ev.exec_date.isoformat(), int(ev.qtd or 0))


def _event_buy_payload(ev: Any) -> dict[str, Any]:
    return {
        "ticker": (ev.ticker or "").upper(),
        "exec_date": ev.exec_date.isoformat(),
        "qtd": int(ev.qtd or 0),
        "preco": float(ev.price or 0.0),
        "amount": float(ev.amount or 0.0),
    }


def _find_latest_dryrun_boletim(real_dir: Path, as_of: date) -> Path | None:
    if not real_dir.exists():
        return None

    pat = re.compile(r"^\d{4}-\d{2}-\d{2}\.json$")
    candidates: list[tuple[date, Path]] = []
    for file in real_dir.glob("*.json"):
        if not file.is_file() or not pat.match(file.name):
            continue
        try:
            file_date = date.fromisoformat(file.stem)
        except Exception:
            continue
        if file_date <= as_of:
            candidates.append((file_date, file))

    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def cmd_record_shadow_buy(args: argparse.Namespace) -> int:
    ledger_dir = _resolve_dir(args.ledger_dir, DEFAULT_LEDGER_DIR)
    _configure_shadow_ledger(ledger_dir)

    ticker = str(args.ticker or "").upper().strip()
    if not ticker:
        print("ERRO: --ticker obrigatorio.", file=sys.stderr)
        return 2
    if int(args.qtd) <= 0:
        print("ERRO: --qtd deve ser maior que zero.", file=sys.stderr)
        return 2
    if float(args.preco) <= 0:
        print("ERRO: --preco deve ser maior que zero.", file=sys.stderr)
        return 2

    qtd = int(args.qtd)
    preco = float(args.preco)
    amount = float(qtd * preco)

    event = create_event(
        EventType.BUY,
        args.exec_date,
        amount,
        ticker=ticker,
        qtd=qtd,
        price=preco,
        reason="LIVE-REAL-TEST shadow buy (gemeo sombra, preco ideal)",
    )

    if is_duplicate(event):
        print("SKIP: BUY sombra duplicado detectado; nenhum evento novo gravado.")
        return 0

    append_event(event)
    print(
        f"OK: BUY sombra registrado ticker={ticker} qtd={qtd} preco={preco:.6f} amount={amount:.2f}"
    )
    return 0


def build_friction_report_payload(
    as_of: date,
    ledger_dir: Path | None = None,
    real_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_ledger_dir = ledger_dir if ledger_dir is not None else DEFAULT_LEDGER_DIR
    resolved_real_dir = real_dir if real_dir is not None else DEFAULT_REAL_DIR

    real_ledger_path = _configure_real_ledger(resolved_ledger_dir)
    shadow_ledger_path = _configure_shadow_ledger(resolved_ledger_dir)

    real_buys = _buy_events_until(real_ledger_path, as_of)
    shadow_buys = _buy_events_until(shadow_ledger_path, as_of)

    shadow_by_key: dict[tuple[str, str, int], deque[Any]] = defaultdict(deque)
    for ev in shadow_buys:
        shadow_by_key[_match_key(ev)].append(ev)

    trades_matched: list[dict[str, Any]] = []
    unmatched_real_buys: list[dict[str, Any]] = []
    for real_ev in real_buys:
        key = _match_key(real_ev)
        queue = shadow_by_key.get(key)
        if queue and queue:
            shadow_ev = queue.popleft()
            price_real = float(real_ev.price or 0.0)
            price_shadow = float(shadow_ev.price or 0.0)
            qtd = int(real_ev.qtd or 0)
            slippage_amount = float((price_real - price_shadow) * qtd)
            slippage_bps = (
                0.0
                if price_shadow <= 0
                else float(((price_real - price_shadow) / price_shadow) * 10_000.0)
            )
            trades_matched.append(
                {
                    "ticker": (real_ev.ticker or "").upper(),
                    "exec_date": real_ev.exec_date.isoformat(),
                    "qtd": qtd,
                    "preco_real": price_real,
                    "preco_sombra": price_shadow,
                    "amount_real": float(real_ev.amount or 0.0),
                    "amount_sombra": float(shadow_ev.amount or 0.0),
                    "slippage_amount": slippage_amount,
                    "slippage_bps": slippage_bps,
                }
            )
        else:
            unmatched_real_buys.append(_event_buy_payload(real_ev))

    unmatched_shadow_buys: list[dict[str, Any]] = []
    for queue in shadow_by_key.values():
        while queue:
            unmatched_shadow_buys.append(_event_buy_payload(queue.popleft()))

    trades_matched.sort(key=lambda row: (row["exec_date"], row["ticker"], row["qtd"]))
    unmatched_real_buys.sort(key=lambda row: (row["exec_date"], row["ticker"], row["qtd"]))
    unmatched_shadow_buys.sort(key=lambda row: (row["exec_date"], row["ticker"], row["qtd"]))

    total_real_invested = float(sum(float(ev.amount or 0.0) for ev in real_buys))
    total_shadow_invested = float(sum(float(ev.amount or 0.0) for ev in shadow_buys))
    total_slippage_amount = float(sum(float(row["slippage_amount"]) for row in trades_matched))
    slippage_pct = (
        None if total_shadow_invested <= 0 else float(total_slippage_amount / total_shadow_invested)
    )

    _configure_target_ledger(real_ledger_path)
    real_positions = export_snapshot(as_of)
    real_cash = compute_cash(as_of)

    dryrun_file = _find_latest_dryrun_boletim(resolved_real_dir, as_of)
    dryrun_source_missing = dryrun_file is None
    dryrun_source_file: str | None = None
    dryrun_cash_free: float | None = None
    dryrun_cash_accounting: float | None = None
    dryrun_n_positions: int | None = None

    if dryrun_file is not None:
        dryrun_payload = json.loads(dryrun_file.read_text(encoding="utf-8"))
        dryrun_source_file = str(dryrun_file)
        dryrun_cash_free = _safe_float_or_none(dryrun_payload.get("cash_free"))
        dryrun_cash_accounting = _safe_float_or_none(dryrun_payload.get("cash_accounting"))
        positions_snapshot = dryrun_payload.get("positions_snapshot")
        if isinstance(positions_snapshot, list):
            dryrun_n_positions = len(positions_snapshot)

    return {
        "as_of_date": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "n_real_buy_events": len(real_buys),
        "n_shadow_buy_events": len(shadow_buys),
        "execution_friction": {
            "trades_matched": trades_matched,
            "unmatched_real_buys": unmatched_real_buys,
            "unmatched_shadow_buys": unmatched_shadow_buys,
            "total_real_invested": total_real_invested,
            "total_shadow_invested": total_shadow_invested,
            "total_slippage_amount": total_slippage_amount,
            "slippage_pct_of_shadow_invested": slippage_pct,
        },
        "operational_crosscheck": {
            "real_cash_free": float(real_cash.get("cash_free", 0.0)),
            "real_cash_accounting": float(real_cash.get("cash_accounting", 0.0)),
            "real_n_positions": len(real_positions),
            "dryrun_cash_free": dryrun_cash_free,
            "dryrun_cash_accounting": dryrun_cash_accounting,
            "dryrun_n_positions": dryrun_n_positions,
            "dryrun_source_file": dryrun_source_file,
            "dryrun_source_missing": dryrun_source_missing,
        },
        "statistically_meaningful_return_comparison": False,
        "methodology_note": (
            "Comparacao formal de Sharpe/retorno da serie real fica fora deste relatorio inicial: "
            "exige acumulacao de observacoes (n_live_real) apos o dia-D. "
            "Este artefato mede friccao de execucao (real vs gemeo sombra) e cross-check operacional "
            "(real vs dry-run paralelo), em linha com D-103 e R-049."
        ),
    }

def cmd_emit_friction_report(args: argparse.Namespace) -> int:
    ledger_dir = _resolve_dir(args.ledger_dir, DEFAULT_LEDGER_DIR)
    real_dir = _resolve_dir(args.real_dir, DEFAULT_REAL_DIR)
    as_of = args.as_of_date

    payload = build_friction_report_payload(as_of, ledger_dir=ledger_dir, real_dir=real_dir)
    out_file = ledger_dir / f"friction_report_{as_of.isoformat()}.json"
    _json_dump(out_file, payload)
    print(f"OK: relatorio de friccao gravado em {out_file}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ferramentas de regua de friccao LIVE-REAL-TEST (sem execucao automatica)."
    )
    sub = parser.add_subparsers(dest="command")

    p_shadow = sub.add_parser(
        "record-shadow-buy",
        help="Registra BUY do gemeo sombra (preco ideal) no ledger dedicado.",
    )
    p_shadow.add_argument("--ticker", type=str, required=True)
    p_shadow.add_argument("--qtd", type=int, required=True)
    p_shadow.add_argument("--preco", type=float, required=True)
    p_shadow.add_argument("--exec-date", type=_parse_iso_date, required=True, help="Data YYYY-MM-DD.")
    p_shadow.add_argument(
        "--ledger-dir",
        type=str,
        default=None,
        help="Diretorio do ledger LIVE-REAL-TEST (default: data/live_real_test).",
    )
    p_shadow.set_defaults(handler=cmd_record_shadow_buy)

    p_emit = sub.add_parser(
        "emit-friction-report",
        help="Emite relatorio de friccao de execucao e cross-check operacional.",
    )
    p_emit.add_argument("--as-of-date", type=_parse_iso_date, required=True, help="Data YYYY-MM-DD.")
    p_emit.add_argument(
        "--ledger-dir",
        type=str,
        default=None,
        help="Diretorio do ledger LIVE-REAL-TEST (default: data/live_real_test).",
    )
    p_emit.add_argument(
        "--real-dir",
        type=str,
        default=None,
        help="Diretorio dos boletins dry-run paralelos (default: data/real).",
    )
    p_emit.set_defaults(handler=cmd_emit_friction_report)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1
    return int(handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
