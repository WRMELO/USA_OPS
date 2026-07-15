"""Runbook utilitario para corte LIVE-REAL-TEST (F-16).

Este script NAO executa o corte real automaticamente.
Ele apenas fornece comandos operacionais auditaveis para:
  - abrir ledger real limpo com APORTE inicial (C0),
  - congelar snapshot do dry-run no market_day de corte,
  - registrar compras reais,
  - emitir boletim real-only no formato esperado pelo monitor B+C.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pipeline.ledger as ledger  # noqa: E402
from pipeline.ledger import EventType, append_event, create_event, export_snapshot, is_duplicate  # noqa: E402

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


def _configure_target_ledger(ledger_dir: Path) -> Path:
    ledger_dir.mkdir(parents=True, exist_ok=True)
    target = ledger_dir / "ledger_real.jsonl"
    ledger.LEDGER_PATH = target
    return target


def _json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def cmd_init_cutover(args: argparse.Namespace) -> int:
    ledger_dir = _resolve_dir(args.ledger_dir, DEFAULT_LEDGER_DIR)
    ledger_path = _configure_target_ledger(ledger_dir)

    aporte = float(args.aporte)
    if aporte <= 0:
        print("ERRO: --aporte deve ser maior que zero.", file=sys.stderr)
        return 2

    if any(ev.type == EventType.APORTE for ev in ledger.read_all_events()):
        print(
            "ERRO: ledger de destino ja contem APORTE. Corte deve ser executado uma unica vez.",
            file=sys.stderr,
        )
        return 2

    event = create_event(
        EventType.APORTE,
        args.exec_date,
        aporte,
        reason="LIVE-REAL-TEST cutover C0",
    )

    if not args.confirm:
        print("SIMULACAO: nenhum dado gravado (faltou --confirm).")
        print(f"Ledger alvo: {ledger_path}")
        print(
            json.dumps(
                {
                    "event_type": event.type.value,
                    "exec_date": event.exec_date.isoformat(),
                    "amount": event.amount,
                    "reason": event.reason,
                },
                ensure_ascii=False,
            )
        )
        return 0

    append_event(event)
    print(f"OK: APORTE registrado em {ledger_path}")
    return 0


def cmd_freeze_dryrun(args: argparse.Namespace) -> int:
    ledger_dir = _resolve_dir(args.ledger_dir, DEFAULT_LEDGER_DIR)
    real_dir = _resolve_dir(args.real_dir, DEFAULT_REAL_DIR)

    market_day = args.market_day.isoformat()
    source_file = real_dir / f"{market_day}.json"
    if not source_file.exists():
        print(f"ERRO: boletim dry-run nao encontrado: {source_file}", file=sys.stderr)
        return 2

    raw = source_file.read_bytes()
    sha256 = hashlib.sha256(raw).hexdigest()
    snapshot = json.loads(raw.decode("utf-8"))

    payload = {
        "market_day": market_day,
        "source_file": str(source_file),
        "sha256": sha256,
        "frozen_at": datetime.now(tz=UTC).isoformat(),
        "snapshot": snapshot,
    }
    out_file = ledger_dir / f"dryrun_freeze_{market_day}.json"
    _json_dump(out_file, payload)
    print(f"OK: freeze gravado em {out_file}")
    return 0


def cmd_record_buy(args: argparse.Namespace) -> int:
    ledger_dir = _resolve_dir(args.ledger_dir, DEFAULT_LEDGER_DIR)
    _configure_target_ledger(ledger_dir)

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
        reason="LIVE-REAL-TEST manual buy",
    )

    if is_duplicate(event):
        print("SKIP: BUY duplicado detectado; nenhum evento novo gravado.")
        return 0

    append_event(event)
    print(
        f"OK: BUY registrado ticker={ticker} qtd={qtd} preco={preco:.6f} amount={amount:.2f}"
    )
    return 0


def cmd_emit_boletim(args: argparse.Namespace) -> int:
    ledger_dir = _resolve_dir(args.ledger_dir, DEFAULT_LEDGER_DIR)
    _configure_target_ledger(ledger_dir)

    exec_day = args.exec_date
    snapshot = export_snapshot(exec_day)
    cash = ledger.compute_cash(exec_day)

    payload: dict[str, Any] = {
        "exec_day": exec_day.isoformat(),
        "date": exec_day.isoformat(),
        "reference_decision": exec_day.isoformat(),
        "market_day": exec_day.isoformat(),
        "trade_day": exec_day.isoformat(),
        "operations": [],
        "cash_movements": [],
        "cash_transfers": [],
        "cash_free": float(cash.get("cash_free", 0.0)),
        "cash_accounting": float(cash.get("cash_accounting", 0.0)),
        "positions_snapshot": snapshot,
        "cash_balance": float(cash.get("cash_free", 0.0)),
        "caixa_liquidando": float(cash.get("cash_accounting", 0.0)),
    }

    out_file = ledger_dir / f"{exec_day.isoformat()}.json"
    _json_dump(out_file, payload)
    print(f"OK: boletim real-only gravado em {out_file}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ferramentas de corte LIVE-REAL-TEST (sem execucao automatica)."
    )
    sub = parser.add_subparsers(dest="command")

    p_init = sub.add_parser("init-cutover", help="Inicializa ledger real com APORTE inicial.")
    p_init.add_argument("--aporte", type=float, required=True, help="Valor do aporte inicial.")
    p_init.add_argument("--exec-date", type=_parse_iso_date, required=True, help="Data YYYY-MM-DD.")
    p_init.add_argument(
        "--ledger-dir",
        type=str,
        default=None,
        help="Diretorio do ledger real (default: data/live_real_test).",
    )
    p_init.add_argument(
        "--confirm",
        action="store_true",
        help="Sem essa flag o comando apenas simula e nao grava dados.",
    )
    p_init.set_defaults(handler=cmd_init_cutover)

    p_freeze = sub.add_parser(
        "freeze-dryrun",
        help="Congela snapshot do dry-run para o market_day informado.",
    )
    p_freeze.add_argument("--market-day", type=_parse_iso_date, required=True, help="Data YYYY-MM-DD.")
    p_freeze.add_argument(
        "--ledger-dir",
        type=str,
        default=None,
        help="Diretorio de destino do freeze (default: data/live_real_test).",
    )
    p_freeze.add_argument(
        "--real-dir",
        type=str,
        default=None,
        help="Diretorio de boletins dry-run (default: data/real).",
    )
    p_freeze.set_defaults(handler=cmd_freeze_dryrun)

    p_buy = sub.add_parser("record-buy", help="Registra BUY real no ledger de teste.")
    p_buy.add_argument("--ticker", type=str, required=True)
    p_buy.add_argument("--qtd", type=int, required=True)
    p_buy.add_argument("--preco", type=float, required=True)
    p_buy.add_argument("--exec-date", type=_parse_iso_date, required=True, help="Data YYYY-MM-DD.")
    p_buy.add_argument(
        "--ledger-dir",
        type=str,
        default=None,
        help="Diretorio do ledger real (default: data/live_real_test).",
    )
    p_buy.set_defaults(handler=cmd_record_buy)

    p_emit = sub.add_parser("emit-boletim", help="Emite boletim real-only do ledger de teste.")
    p_emit.add_argument("--exec-date", type=_parse_iso_date, required=True, help="Data YYYY-MM-DD.")
    p_emit.add_argument(
        "--ledger-dir",
        type=str,
        default=None,
        help="Diretorio do ledger real (default: data/live_real_test).",
    )
    p_emit.set_defaults(handler=cmd_emit_boletim)

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
