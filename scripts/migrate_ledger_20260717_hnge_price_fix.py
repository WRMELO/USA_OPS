"""Migracao supervisionada do ledger real de 2026-07-17 (HNGE, preco interino).

Objetivo: corrigir o evento BUY de HNGE em 17/07/2026, que foi registrado com
preco e quantidade copiados por engano do evento FCEL do mesmo lote. Usa como
fonte o preco interino informado pelo Owner (US$ 86,76), pendente de
reconciliacao com a nota oficial BTG de 17/07/2026 quando disponivel (R-056).

IMPORTANTE:
- Default = dry-run (nao escreve nada).
- Para escrever, use --confirm.
- Sempre cria backup antes de sobrescrever.
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

MIGRATION_REASON_SUFFIX = (
    "corrigido: preco interino informado pelo Owner em 2026-07-18 (US$86,76), "
    "pendente reconciliacao com nota oficial BTG de 17/07/2026 (R-056)"
)

CORRECTIONS: dict[str, dict[str, Any]] = {
    "fedd1bfa-44d8-496e-9197-a42f9dd6a03b": {
        "ticker": "HNGE",
        "old_qtd": 53.87931034,
        "old_price": 18.56,
        "old_amount": 999.9999999104,
        "new_qtd": 11.52604887,
        "new_price": 86.76,
        "new_amount": 1000.0,
    },
}


def _with_suffix_reason(reason: Any) -> str:
    base = str(reason or "").strip()
    if not base:
        return MIGRATION_REASON_SUFFIX
    if MIGRATION_REASON_SUFFIX in base:
        return base
    return f"{base} | {MIGRATION_REASON_SUFFIX}"


def run_migration(ledger_path: Path, confirm: bool = False) -> dict[str, Any]:
    if not ledger_path.exists():
        raise FileNotFoundError(f"Ledger nao encontrado: {ledger_path}")

    original_lines = ledger_path.read_text(encoding="utf-8").splitlines()
    new_lines: list[str] = []
    changed: list[dict[str, Any]] = []

    for raw in original_lines:
        payload = json.loads(raw)
        ev_id = str(payload.get("id", ""))
        correction = CORRECTIONS.get(ev_id)
        if correction is None:
            new_lines.append(raw)
            continue

        ticker = str(payload.get("ticker", "")).upper().strip()
        qtd = float(payload.get("qtd", 0.0) or 0.0)
        price = float(payload.get("price", 0.0) or 0.0)
        amount = float(payload.get("amount", 0.0) or 0.0)

        if ticker != correction["ticker"]:
            raise SystemExit(f"Ticker inesperado para {ev_id}: {ticker} != {correction['ticker']}")
        if abs(qtd - float(correction["old_qtd"])) > 1e-6:
            raise SystemExit(f"qtd inesperada para {ev_id}: {qtd} != {correction['old_qtd']}")
        if abs(price - float(correction["old_price"])) > 1e-6:
            raise SystemExit(f"price inesperado para {ev_id}: {price} != {correction['old_price']}")
        if abs(amount - float(correction["old_amount"])) > 0.01:
            raise SystemExit(f"amount inesperado para {ev_id}: {amount} != {correction['old_amount']}")

        before = dict(payload)
        payload["qtd"] = float(correction["new_qtd"])
        payload["price"] = float(correction["new_price"])
        payload["amount"] = float(correction["new_amount"])
        payload["reason"] = _with_suffix_reason(payload.get("reason"))
        changed.append({"id": ev_id, "before": before, "after": payload})
        new_lines.append(json.dumps(payload, ensure_ascii=False))

    if len(changed) != len(CORRECTIONS):
        raise SystemExit(f"Migracao invalida: corrigidos={len(changed)} esperado={len(CORRECTIONS)}")

    for row in changed:
        print(f"[MIGRATION] {row['id']}")
        print(f"  before qtd={row['before'].get('qtd')} price={row['before'].get('price')} amount={row['before'].get('amount')}")
        print(f"  after  qtd={row['after'].get('qtd')} price={row['after'].get('price')} amount={row['after'].get('amount')}")

    if not confirm:
        print("[DRY-RUN] Nenhuma alteracao escrita no arquivo.")
        return {
            "ok": True,
            "dry_run": True,
            "ledger_path": str(ledger_path),
            "updated_count": len(changed),
            "backup_path": None,
        }

    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_path = ledger_path.with_name(f"{ledger_path.name}.bak_pre_migration_{stamp}")
    shutil.copy2(ledger_path, backup_path)
    ledger_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    print(f"[CONFIRM] Backup criado em: {backup_path}")
    print(f"[CONFIRM] Ledger atualizado: {ledger_path}")
    return {
        "ok": True,
        "dry_run": False,
        "ledger_path": str(ledger_path),
        "updated_count": len(changed),
        "backup_path": str(backup_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Migracao supervisionada do ledger real 2026-07-17 (HNGE preco interino)")
    parser.add_argument(
        "--ledger-path",
        type=Path,
        default=ROOT / "data" / "live_real_test" / "ledger_real.jsonl",
        help="Caminho do ledger real a migrar",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--confirm", action="store_true", help="Aplica a migracao no ledger real")
    mode.add_argument("--dry-run", action="store_true", help="Executa somente simulacao (default)")
    args = parser.parse_args()

    result = run_migration(args.ledger_path, confirm=args.confirm)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
