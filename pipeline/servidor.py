"""Servidor/lançador local USA_OPS (T-031)."""
from __future__ import annotations

import argparse
import json
import sys
import threading
import webbrowser
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline import run_daily
from pipeline import painel_diario as _painel_diario_mod
from pipeline import real_boletim_web
from pipeline.ledger import (
    EventType,
    append_event,
    compute_cash,
    create_event,
    export_snapshot,
    is_duplicate,
    pending_settlements,
)


def _real_ledger_path() -> Path:
    return ROOT / "data" / "live_real_test" / "ledger_real.jsonl"


def _real_test_active() -> bool:
    ledger_path = _real_ledger_path()
    if not ledger_path.exists():
        return False
    try:
        with ledger_path.open("r", encoding="utf-8") as fp:
            for raw_line in fp:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if payload.get("type") == "APORTE":
                    return True
    except Exception:
        return False
    return False


@dataclass
class JobState:
    status: str = "IDLE"  # IDLE | RUNNING | OK | FAIL
    mode: str = ""
    day: str = ""
    message: str = ""
    error: str = ""
    progress_current: int = 0
    progress_total: int = 12
    progress_label: str = ""


JOB_LOCK = threading.Lock()
JOB_STATE = JobState()


def _panel_path(day: date) -> Path:
    return ROOT / "data" / "cycles" / day.isoformat() / "painel.html"


def _list_existing_panels() -> list[date]:
    cycles_dir = ROOT / "data" / "cycles"
    if not cycles_dir.exists():
        return []
    out: list[date] = []
    for p in cycles_dir.glob("*/painel.html"):
        try:
            out.append(date.fromisoformat(p.parent.name))
        except Exception:
            continue
    return sorted(set(out))


def _trading_days() -> list[date]:
    operational_window = ROOT / "data" / "ssot" / "operational_window.parquet"
    canonical = ROOT / "data" / "ssot" / "canonical_us.parquet"
    dataset = ROOT / "data" / "features" / "dataset_us.parquet"
    for p in (operational_window, canonical, dataset):
        if not p.exists():
            continue
        try:
            df = pd.read_parquet(p, columns=["date"])
            if df.empty:
                continue
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            days = sorted({d for d in df["date"].dropna().tolist()})
            if days:
                return days
        except Exception:
            continue
    return []


def _missing_days_for_catchup(target_day: date) -> list[date]:
    trading = [d for d in _trading_days() if d <= target_day]
    if not trading:
        return [target_day]
    existing = set(_list_existing_panels())
    if not existing:
        return [target_day]
    last_done = max(existing)
    return [d for d in trading if d > last_done and d not in existing]


def _start_job(mode: str, target_day: date) -> bool:
    with JOB_LOCK:
        if JOB_STATE.status == "RUNNING":
            return False
        JOB_STATE.status = "RUNNING"
        JOB_STATE.mode = mode
        JOB_STATE.day = target_day.isoformat()
        JOB_STATE.message = "Job iniciado."
        JOB_STATE.error = ""
        JOB_STATE.progress_current = 0
        JOB_STATE.progress_total = 12
        JOB_STATE.progress_label = "Inicializando"

    def _runner() -> None:
        try:
            if mode == "single":
                days = [target_day]
            else:
                days = _missing_days_for_catchup(target_day)
                if not days:
                    days = []

            if not days:
                with JOB_LOCK:
                    JOB_STATE.status = "OK"
                    JOB_STATE.message = "Catch-up sem pendencias."
                    JOB_STATE.progress_current = JOB_STATE.progress_total
                    JOB_STATE.progress_label = "Concluido"
                return

            total_steps = max(len(days) * 12, 1)
            with JOB_LOCK:
                JOB_STATE.progress_total = total_steps
                JOB_STATE.message = f"Executando {len(days)} dia(s)."

            for i, day in enumerate(days):
                offset = i * 12

                def _on_step(cur: int, _tot: int, label: str) -> None:
                    with JOB_LOCK:
                        JOB_STATE.progress_current = offset + cur
                        JOB_STATE.progress_label = f"{day.isoformat()} - {label}"

                run_daily.run(target_date=day, full=False, on_step=_on_step)

            with JOB_LOCK:
                JOB_STATE.status = "OK"
                JOB_STATE.message = "Pipeline concluido com sucesso."
                JOB_STATE.error = ""
                JOB_STATE.progress_current = JOB_STATE.progress_total
                JOB_STATE.progress_label = "Concluido"
        except Exception as exc:  # noqa: BLE001
            with JOB_LOCK:
                JOB_STATE.status = "FAIL"
                JOB_STATE.message = "Falha na execucao do pipeline."
                JOB_STATE.error = str(exc)

    threading.Thread(target=_runner, daemon=True).start()
    return True


def apply_boletim_operations(payload: dict[str, Any]) -> dict[str, Any]:
    payload_date = str(payload.get("exec_day", payload.get("date", ""))).strip()
    try:
        save_day = date.fromisoformat(payload_date)
    except ValueError as exc:
        raise ValueError("Campo 'exec_day/date' invalido") from exc

    real_dir = ROOT / "data" / "real"
    market_day_raw = str(payload.get("market_day", "")).strip()
    if market_day_raw:
        try:
            market_day = date.fromisoformat(market_day_raw)
        except ValueError:
            market_day = save_day
    else:
        market_day = save_day

    cycle_dir = ROOT / "data" / "cycles" / market_day.isoformat()
    real_dir.mkdir(parents=True, exist_ok=True)
    cycle_dir.mkdir(parents=True, exist_ok=True)

    # SSOT imutavel: primeiro grava eventos no ledger (D-045).
    ops = payload.get("operations", [])
    for op in ops:
        typ = str(op.get("type", "")).upper().strip()
        tk = str(op.get("ticker", "")).upper().strip()
        qtd = int(op.get("qtd", 0) or 0)
        preco = float(op.get("preco", 0.0) or 0.0)
        if not tk or qtd <= 0 or preco <= 0:
            continue
        amount = qtd * preco
        if typ == "COMPRA":
            ev = create_event(EventType.BUY, exec_date=save_day, amount=amount, ticker=tk, qtd=qtd, price=preco)
        elif typ == "VENDA":
            ev = create_event(EventType.SELL, exec_date=save_day, amount=amount, ticker=tk, qtd=qtd, price=preco)
        else:
            continue
        if not is_duplicate(ev):
            append_event(ev)

    cash_movements = payload.get("cash_movements", [])
    for mv in cash_movements:
        typ = str(mv.get("type", "")).upper().strip()
        val = float(mv.get("value", mv.get("valor", 0.0)) or 0.0)
        desc = str(mv.get("description", "")).strip() or None
        if val <= 0:
            continue
        if typ in {"APORTE", "DEPOSITO"}:
            ev = create_event(EventType.APORTE, exec_date=save_day, amount=val, reason=desc)
            if not is_duplicate(ev):
                append_event(ev)
        elif typ in {"DIVIDENDO", "JCP", "BONIFICACAO", "BONUS", "SUBSCRICAO"}:
            ev = create_event(EventType.DIVIDENDO, exec_date=save_day, amount=val, reason=desc)
            if not is_duplicate(ev):
                append_event(ev)
        elif typ in {"RETIRADA", "SAQUE"}:
            ev = create_event(EventType.RETIRADA, exec_date=save_day, amount=val, reason=desc)
            if not is_duplicate(ev):
                append_event(ev)

    # Transferências usam ref_id quando possível.
    pend_by_ref = {p.get("ref"): p for p in pending_settlements(save_day)}
    cash_transfers = payload.get("cash_transfers", [])
    for tr in cash_transfers:
        val = float(tr.get("value", tr.get("valor", 0.0)) or 0.0)
        note = str(tr.get("note", tr.get("ref", ""))).strip()
        if val <= 0:
            continue
        ref_id = note if note in pend_by_ref else None
        ev = create_event(
            EventType.SETTLEMENT,
            exec_date=save_day,
            settle_date=save_day,
            amount=val,
            ref_id=ref_id,
            reason=note or "cash_transfer",
        )
        if not is_duplicate(ev):
            append_event(ev)

    # Boletim salvo vira artefato derivado do ledger.
    cash = compute_cash(save_day)
    derived_payload = {
        "date": payload.get("date", save_day.isoformat()),
        "reference_decision": payload.get("reference_decision", market_day.isoformat()),
        "exec_day": save_day.isoformat(),
        "market_day": market_day.isoformat(),
        "trade_day": str(payload.get("trade_day", save_day.isoformat())),
        "operations": ops,
        "cash_movements": cash_movements,
        "cash_transfers": cash_transfers,
        "cash_free": float(cash.get("cash_free", 0.0)),
        "cash_accounting": float(cash.get("cash_accounting", 0.0)),
        "caixa_liquido_real": payload.get("caixa_liquido_real", None),
        "positions_snapshot": export_snapshot(save_day),
        "cash_balance": float(cash.get("cash_free", 0.0)),
        "caixa_liquidando": float(cash.get("cash_accounting", 0.0)),
    }

    out_real = real_dir / f"{market_day.isoformat()}.json"
    out_cycle = cycle_dir / "boletim_preenchido.json"
    content = json.dumps(derived_payload, ensure_ascii=False, indent=2)
    out_real.write_text(content, encoding="utf-8")
    out_cycle.write_text(content, encoding="utf-8")
    try:
        decision_path = ROOT / "data" / "daily" / f"decision_{save_day.isoformat()}.json"
        if decision_path.exists():
            decision_payload = json.loads(decision_path.read_text(encoding="utf-8"))
            if bool(decision_payload.get("is_rebalance_day", False)):
                rebalance_dt_raw = str(decision_payload.get("scores_reference_date_d_minus_1", "")).strip()
                if rebalance_dt_raw:
                    date.fromisoformat(rebalance_dt_raw)
                    last_rebalance_path = ROOT / "data" / "daily" / "last_rebalance.json"
                    last_rebalance_path.parent.mkdir(parents=True, exist_ok=True)
                    last_rebalance_payload = {
                        "last_rebalance_dt": rebalance_dt_raw,
                        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                    }
                    last_rebalance_path.write_text(
                        json.dumps(last_rebalance_payload, indent=2, ensure_ascii=False),
                        encoding="utf-8",
                    )
    except Exception:
        pass
    paths = [str(out_cycle.relative_to(ROOT)), str(out_real.relative_to(ROOT))]
    # Auto-commit e auto-push do ledger SSOT apos salvar o boletim (D-037, R-030).
    # Falha no commit/push NAO bloqueia o 200 OK: registra no log e continua.
    try:
        import logging
        import subprocess

        ledger_rel = "data/ssot/ledger.jsonl"
        add_cmd = ["git", "-C", str(ROOT), "add", "-f", ledger_rel]
        add_run = subprocess.run(add_cmd, capture_output=True, text=True, timeout=15)
        if add_run.returncode == 0:
            commit_msg = f"feat(ledger): auto-commit boletim {save_day.isoformat()}"
            commit_cmd = ["git", "-C", str(ROOT), "commit", "-m", commit_msg]
            commit_run = subprocess.run(commit_cmd, capture_output=True, text=True, timeout=15)
            commit_stdout = (commit_run.stdout or "").lower()
            commit_stderr = (commit_run.stderr or "").lower()
            if commit_run.returncode == 0:
                push_cmd = ["git", "-C", str(ROOT), "push"]
                push_run = subprocess.run(push_cmd, capture_output=True, text=True, timeout=30)
                if push_run.returncode != 0:
                    logging.warning("[ledger-autocommit] git push falhou: %s", (push_run.stderr or "").strip())
            elif "nothing to commit" not in commit_stdout and "nothing to commit" not in commit_stderr:
                logging.warning("[ledger-autocommit] git commit falhou: %s", (commit_run.stderr or "").strip())
        else:
            logging.warning("[ledger-autocommit] git add falhou: %s", (add_run.stderr or "").strip())
    except Exception as exc:
        try:
            import logging as _logging

            _logging.warning("[ledger-autocommit] excecao inesperada: %s", exc)
        except Exception:
            pass
    return {"ok": True, "paths": paths}


def serve(host: str = "127.0.0.1", port: int = 8788, auto_open: bool = True, override_date: date | None = None) -> None:
    import http.server

    class Handler(http.server.BaseHTTPRequestHandler):
        def _today(self) -> date:
            return override_date if override_date is not None else date.today()

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            today = self._today()

            if path == "/":
                self._respond_html(self._render_home(today))
                return
            if path == "/rodar":
                _start_job("single", today)
                self._redirect("/status")
                return
            if path == "/catchup":
                _start_job("catchup", today)
                self._redirect("/status")
                return
            if path == "/status":
                self._respond_html(self._render_status(today))
                return
            if path == "/painel":
                if _real_test_active():
                    real_boletim_web.close_stale_drafts(today, ROOT / "data" / "live_real_test")
                    view = real_boletim_web.load_live_view(today, ROOT / "data" / "live_real_test")
                    html = real_boletim_web.render_live_html(view)
                    self._respond_html(html, code=200)
                    return
                panel = _panel_path(_painel_diario_mod.get_d_minus_1(today))
                if not panel.exists():
                    self._respond_html("<h3>Painel do dia nao encontrado.</h3>", code=404)
                    return
                self._respond_bytes("text/html", panel.read_bytes(), code=200)
                return
            if path.startswith("/painel/"):
                token = path.replace("/painel/", "", 1).strip("/")
                try:
                    day = date.fromisoformat(token)
                except ValueError:
                    self._respond_html("<h3>Data invalida.</h3>", code=400)
                    return
                panel = _panel_path(day)
                if not panel.exists():
                    self._respond_html("<h3>Painel historico nao encontrado.</h3>", code=404)
                    return
                self._respond_bytes("text/html", panel.read_bytes(), code=200)
                return
            if path == "/healthz":
                with JOB_LOCK:
                    self._respond_json(
                        {
                            "ok": True,
                            "status": JOB_STATE.status,
                            "mode": JOB_STATE.mode,
                            "day": JOB_STATE.day,
                            "progress": JOB_STATE.progress_current,
                            "total": JOB_STATE.progress_total,
                            "label": JOB_STATE.progress_label,
                        }
                    )
                return

            self._respond_html("<h3>Rota nao encontrada.</h3>", code=404)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            today = self._today()

            def _read_form_payload() -> dict[str, str]:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length).decode("utf-8", errors="ignore")
                parsed_form = parse_qs(body, keep_blank_values=True)
                return {k: (v[0] if v else "") for k, v in parsed_form.items()}

            if path == "/painel/rascunho":
                form = _read_form_payload()
                raw_exec_day = str(form.get("exec_day", "")).strip()
                try:
                    exec_day = date.fromisoformat(raw_exec_day)
                except ValueError:
                    self._respond_html("<h3>exec_day invalido.</h3>", code=400)
                    return
                if exec_day != today:
                    self._respond_html("<h3>Somente o painel do dia atual pode salvar rascunho.</h3>", code=403)
                    return

                tipo = str(form.get("tipo", "")).upper().strip()
                ticker = str(form.get("ticker", "")).upper().strip()
                liquidacao = str(form.get("liquidacao", "")).upper().strip()
                try:
                    raw_qtd = str(form.get("qtd", "")).strip()
                    qtd = float(raw_qtd) if raw_qtd else None
                    raw_valor_investido = str(form.get("valor_investido", "")).strip()
                    valor_investido = float(raw_valor_investido) if raw_valor_investido else None
                    preco = float(str(form.get("preco", "0") or "0"))
                    raw_corretagem = str(form.get("corretagem", "")).strip()
                    corretagem = float(raw_corretagem) if raw_corretagem else 2.50
                    raw_preco_sombra = str(form.get("preco_sombra", "")).strip()
                    preco_sombra = float(raw_preco_sombra) if raw_preco_sombra else None
                except ValueError:
                    self._respond_html("<h3>Campos numericos invalidos.</h3>", code=400)
                    return

                if tipo not in {"COMPRA", "VENDA"} or not ticker or preco <= 0:
                    self._respond_html("<h3>Dados da operacao invalidos.</h3>", code=400)
                    return
                if not valor_investido and (qtd is None or qtd <= 0):
                    self._respond_html("<h3>Informe Valor investido ou Quantidade.</h3>", code=400)
                    return
                if corretagem < 0:
                    self._respond_html("<h3>Corretagem invalida.</h3>", code=400)
                    return
                if tipo == "VENDA" and liquidacao not in {"JA_NO_CAIXA", "EM_LIQUIDACAO"}:
                    self._respond_html(
                        "<h3>Liquidacao invalida para VENDA (use JA_NO_CAIXA ou EM_LIQUIDACAO).</h3>",
                        code=400,
                    )
                    return

                try:
                    real_boletim_web.add_operation(
                        exec_day,
                        tipo=tipo,
                        ticker=ticker,
                        qtd=qtd,
                        preco=preco,
                        corretagem=corretagem,
                        preco_sombra=preco_sombra,
                        valor_investido=valor_investido,
                        liquidacao=liquidacao if tipo == "VENDA" else None,
                    )
                except ValueError as exc:
                    self._respond_html(f"<h3>{str(exc)}</h3>", code=400)
                    return
                self._redirect("/painel")
                return

            if path == "/painel/rascunho/remover":
                form = _read_form_payload()
                raw_exec_day = str(form.get("exec_day", "")).strip()
                row_id = str(form.get("row_id", "")).strip()
                try:
                    exec_day = date.fromisoformat(raw_exec_day)
                except ValueError:
                    self._respond_html("<h3>exec_day invalido.</h3>", code=400)
                    return
                if exec_day != today:
                    self._respond_html("<h3>Somente o painel do dia atual pode alterar rascunho.</h3>", code=403)
                    return
                if not row_id:
                    self._respond_html("<h3>row_id obrigatorio.</h3>", code=400)
                    return
                real_boletim_web.remove_operation(exec_day, row_id)
                self._redirect("/painel")
                return

            if path == "/painel/encerrar":
                form = _read_form_payload()
                raw_exec_day = str(form.get("exec_day", "")).strip()
                confirmar = str(form.get("confirmar", "")).strip().lower()
                raw_caixa_real = str(form.get("caixa_real", "")).strip()
                try:
                    exec_day = date.fromisoformat(raw_exec_day)
                except ValueError:
                    self._respond_html("<h3>exec_day invalido.</h3>", code=400)
                    return
                if exec_day != today:
                    self._respond_html("<h3>Somente o painel do dia atual pode encerrar.</h3>", code=403)
                    return
                if confirmar != "sim":
                    self._respond_html("<h3>Confirmacao obrigatoria para encerrar o dia.</h3>", code=400)
                    return
                caixa_real: float | None = None
                if raw_caixa_real:
                    try:
                        caixa_real = float(raw_caixa_real)
                    except ValueError:
                        self._respond_html("<h3>caixa_real invalido.</h3>", code=400)
                        return
                    if caixa_real < 0:
                        self._respond_html("<h3>caixa_real deve ser maior ou igual a zero.</h3>", code=400)
                        return
                real_boletim_web.close_day(exec_day, ROOT / "data" / "live_real_test", caixa_real=caixa_real)
                self._redirect("/painel")
                return

            if path != "/salvar":
                self._respond_json({"ok": False, "error": "Rota nao encontrada"}, code=404)
                return

            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            try:
                payload = json.loads(body)
            except Exception:
                self._respond_json({"ok": False, "error": "Payload JSON invalido"}, code=400)
                return

            payload_date = str(payload.get("exec_day", payload.get("date", ""))).strip()
            try:
                save_day = date.fromisoformat(payload_date)
            except ValueError:
                self._respond_json({"ok": False, "error": "Campo 'exec_day/date' invalido"}, code=400)
                return

            # Bloqueia salvamento de paineis historicos.
            if save_day != today:
                self._respond_json(
                    {"ok": False, "error": "Somente o painel do dia atual pode salvar boletim."},
                    code=403,
                )
                return
            try:
                result = apply_boletim_operations(payload)
            except ValueError as exc:
                self._respond_json({"ok": False, "error": str(exc)}, code=400)
                return
            self._respond_json(result, code=200)

        def _render_home(self, today: date) -> str:
            hist = _list_existing_panels()
            items = [
                f"<li><a href='/painel/{d.isoformat()}'>{d.isoformat()}</a>{' (hoje)' if d == today else ''}</li>"
                for d in reversed(hist[-60:])
            ]
            history_html = "<ul>" + "".join(items) + "</ul>" if items else "<p>Nenhum painel encontrado.</p>"
            return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>USA_OPS - Lancador Diario</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 18px; color: #111827; background: #f9fafb; }}
    .card {{ background: #fff; border: 1px solid #d1d5db; border-radius: 8px; padding: 12px; margin: 10px 0; }}
    .btn {{ display: inline-block; margin-right: 8px; text-decoration: none; background: #1d4ed8; color: #fff; padding: 8px 12px; border-radius: 6px; }}
    .muted {{ color: #4b5563; }}
  </style>
</head>
<body>
  <h1>USA_OPS - Lancador Diario</h1>
  <p class="muted">Porta 8788 | NYSE | Rodar sem Cursor</p>
  <div class="card">
    <h3>Dia atual: {today.isoformat()}</h3>
    <a class="btn" href="/rodar">Rodar ciclo do dia</a>
    <a class="btn" href="/catchup">Rodar catch-up</a>
    <a class="btn" href="/status">Ver status</a>
    <p><a href="/painel">Abrir painel do dia</a></p>
  </div>
  <div class="card">
    <h3>Paineis historicos</h3>
    {history_html}
  </div>
</body>
</html>"""

        def _render_status(self, today: date) -> str:
            with JOB_LOCK:
                st = JOB_STATE.status
                mode = JOB_STATE.mode
                day = JOB_STATE.day or today.isoformat()
                msg = JOB_STATE.message
                err = JOB_STATE.error
                cur = JOB_STATE.progress_current
                tot = JOB_STATE.progress_total
                label = JOB_STATE.progress_label

            refresh = "<meta http-equiv='refresh' content='2'>" if st == "RUNNING" else ""
            pct = int((cur / tot) * 100) if tot > 0 else 0
            error_html = f"<p style='color:#991b1b;'><b>Erro:</b> {err}</p>" if err else ""
            return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>USA_OPS - Status</title>
  {refresh}
</head>
<body style="font-family: Arial, sans-serif; margin: 18px;">
  <h2>Status do ciclo</h2>
  <p><b>Status:</b> {st}</p>
  <p><b>Modo:</b> {mode or 'N/A'}</p>
  <p><b>Data alvo:</b> {day}</p>
  <p><b>Mensagem:</b> {msg}</p>
  <p><b>Progresso:</b> {cur}/{tot} ({pct}%)</p>
  <p><b>Step:</b> {label}</p>
  {error_html}
  <p><a href="/">Voltar</a> | <a href="/painel">Painel do dia</a></p>
</body>
</html>"""

        def _redirect(self, location: str) -> None:
            self.send_response(303)
            self.send_header("Location", location)
            self.end_headers()

        def _respond_html(self, html: str, code: int = 200) -> None:
            self._respond_bytes("text/html", html.encode("utf-8"), code=code)

        def _respond_json(self, payload: dict[str, Any], code: int = 200) -> None:
            self._respond_bytes("application/json", json.dumps(payload, ensure_ascii=False).encode("utf-8"), code=code)

        def _respond_bytes(self, ctype: str, body: bytes, code: int = 200) -> None:
            self.send_response(code)
            self.send_header("Content-Type", f"{ctype}; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            return

    server = http.server.ThreadingHTTPServer((host, port), Handler)
    url = f"http://{host}:{port}"
    print(f"Lancador USA_OPS ativo em {url}")
    print("Pressione Ctrl+C para encerrar.")
    if auto_open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor autonomo USA_OPS")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8788)
    parser.add_argument("--no-open", action="store_true", help="Nao abrir navegador automaticamente")
    parser.add_argument("--override-date", type=str, default=None, help="Simular data (YYYY-MM-DD)")
    args = parser.parse_args()
    od = date.fromisoformat(args.override_date) if args.override_date else None
    serve(host=args.host, port=args.port, auto_open=not args.no_open, override_date=od)


if __name__ == "__main__":
    main()
