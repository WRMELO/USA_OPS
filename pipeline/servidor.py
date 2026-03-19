"""Servidor/lançador local USA_OPS (T-031)."""
from __future__ import annotations

import argparse
import json
import sys
import threading
import webbrowser
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline import run_daily


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
    return ROOT / "data" / "daily" / f"painel_{day.isoformat()}.html"


def _list_existing_panels() -> list[date]:
    daily = ROOT / "data" / "daily"
    if not daily.exists():
        return []
    out: list[date] = []
    for p in sorted(daily.glob("painel_*.html")):
        token = p.stem.replace("painel_", "", 1)
        try:
            out.append(date.fromisoformat(token))
        except ValueError:
            continue
    return sorted(set(out))


def _trading_days() -> list[date]:
    canonical = ROOT / "data" / "ssot" / "canonical_us.parquet"
    dataset = ROOT / "data" / "features" / "dataset_us.parquet"
    for p in (canonical, dataset):
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
                panel = _panel_path(today)
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

            payload_date = str(payload.get("date", "")).strip()
            try:
                save_day = date.fromisoformat(payload_date)
            except ValueError:
                self._respond_json({"ok": False, "error": "Campo 'date' invalido"}, code=400)
                return

            # Bloqueia salvamento de paineis historicos.
            if save_day != today:
                self._respond_json(
                    {"ok": False, "error": "Somente o painel do dia atual pode salvar boletim."},
                    code=403,
                )
                return

            real_dir = ROOT / "data" / "real"
            real_dir.mkdir(parents=True, exist_ok=True)
            out_path = real_dir / f"{save_day.isoformat()}.json"
            out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            self._respond_json({"ok": True, "path": str(out_path.relative_to(ROOT))}, code=200)

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
