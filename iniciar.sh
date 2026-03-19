#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

HOST="127.0.0.1"
PORT="8788"
BASE_URL="http://${HOST}:${PORT}"
LOG_FILE="/tmp/usa_ops_server.log"

DESKTOP_DIR="$(xdg-user-dir DESKTOP 2>/dev/null || true)"
if [[ -z "${DESKTOP_DIR}" ]]; then
  DESKTOP_DIR="${HOME}/Desktop"
fi
SHORTCUT_PATH="${DESKTOP_DIR}/USA_OPS.desktop"

install_shortcut() {
  mkdir -p "${DESKTOP_DIR}"
  cat > "${SHORTCUT_PATH}" <<EOF
[Desktop Entry]
Type=Application
Version=1.0
Name=USA_OPS
Comment=Abrir lancador da Fabrica US (porta 8788)
Exec=${ROOT_DIR}/iniciar.sh
Icon=utilities-terminal
Terminal=false
Categories=Finance;Utility;
StartupNotify=true
EOF
  chmod +x "${SHORTCUT_PATH}"
  if command -v gio >/dev/null 2>&1; then
    gio set "${SHORTCUT_PATH}" metadata::trusted true >/dev/null 2>&1 || true
  fi
  echo "Atalho criado/atualizado: ${SHORTCUT_PATH}"
}

if [[ "${1:-}" == "--install-shortcut" ]]; then
  install_shortcut
  exit 0
fi

# Garante atalho idempotente sem falhar inicializacao.
if [[ ! -f "${SHORTCUT_PATH}" ]]; then
  install_shortcut || true
fi

# Se servidor ja estiver no ar, apenas abre a UI e encerra.
if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
  xdg-open "${BASE_URL}" >/dev/null 2>&1 || true
  disown >/dev/null 2>&1 || true
  exit 0
fi

# Porta ocupada por outro processo: notifica e encerra.
if ss -ltn "sport = :${PORT}" | rg -q LISTEN 2>/dev/null; then
  notify-send "USA OPS" "Porta ${PORT} ocupada por outro processo." --icon=dialog-error 2>/dev/null || true
  exit 1
fi

# Sobe o servidor em background, com log.
nohup .venv/bin/python pipeline/servidor.py --host "${HOST}" --port "${PORT}" > "${LOG_FILE}" 2>&1 &
disown >/dev/null 2>&1 || true

# Aguarda o servidor ficar pronto (ate 15s).
for _i in $(seq 1 30); do
  if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
    xdg-open "${BASE_URL}" >/dev/null 2>&1 || true
    disown >/dev/null 2>&1 || true
    exit 0
  fi
  sleep 0.5
done

# Timeout: servidor nao subiu.
notify-send "USA OPS" "Servidor nao iniciou em 15s. Veja ${LOG_FILE}" --icon=dialog-error 2>/dev/null || true
exit 1
