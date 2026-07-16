#!/usr/bin/env bash
# encerrar_dia_real.sh
# Atalho operacional da fase LIVE-REAL-TEST:
# emite boletim real-only + relatorio de friccao do dia e reporta n_live_real.
set -uo pipefail
trap '' PIPE

ROOT_US="/home/wilson/USA_OPS"
ROOT_SALA="/home/wilson/SALA_DE_CONTROLE"
PY_US="${ROOT_US}/.venv/bin/python"
PY_SALA="${ROOT_SALA}/.venv/bin/python"
CUTOVER_SCRIPT="${ROOT_US}/scripts/live_real_cutover.py"
FRICTION_SCRIPT="${ROOT_US}/scripts/friction_ruler.py"
DRYRUN_DIR="${ROOT_SALA}/analise_interfabricas/dryrun_to_live"
LOG_FILE="/tmp/usa_encerrar_dia_real.log"

EXEC_DATE=""
LEDGER_DIR=""
REAL_DIR=""

usage() {
  cat <<'EOF'
Uso: ./encerrar_dia_real.sh [opcoes]
  --exec-date YYYY-MM-DD   Data de execucao (default: hoje).
  --ledger-dir DIR         Diretorio do ledger LIVE-REAL-TEST (default: data/live_real_test).
  --real-dir DIR           Diretorio dos boletins dry-run paralelos (default: data/real).
  -h, --help               Esta ajuda.
EOF
}

log() { echo "$1" | tee -a "${LOG_FILE}"; }
fail() { log "ERRO: $1"; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exec-date)
      EXEC_DATE="$2"
      shift 2
      ;;
    --ledger-dir)
      LEDGER_DIR="$2"
      shift 2
      ;;
    --real-dir)
      REAL_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      fail "Parametro invalido: $1"
      ;;
  esac
done

[[ -n "${EXEC_DATE}" ]] || EXEC_DATE="$(date +%F)"
echo "=== $(date '+%F %T') encerrar-dia-real (exec_date=${EXEC_DATE}) ===" > "${LOG_FILE}"

[[ -x "${PY_US}" ]] || fail "Python da USA_OPS nao encontrado em ${PY_US}"
"${PY_US}" --version >>"${LOG_FILE}" 2>&1 || fail "Health-check do venv USA_OPS falhou. Veja ${LOG_FILE}"
[[ -x "${PY_SALA}" ]] || fail "Python da SALA_DE_CONTROLE nao encontrado em ${PY_SALA}"
"${PY_SALA}" --version >>"${LOG_FILE}" 2>&1 || fail "Health-check do venv SALA_DE_CONTROLE falhou. Veja ${LOG_FILE}"

LEDGER_DIR_ARGS=()
if [[ -n "${LEDGER_DIR}" ]]; then
  LEDGER_DIR_ARGS=(--ledger-dir "${LEDGER_DIR}")
fi

REAL_DIR_ARGS=()
if [[ -n "${REAL_DIR}" ]]; then
  REAL_DIR_ARGS=(--real-dir "${REAL_DIR}")
fi

RESOLVED_LEDGER_DIR="${LEDGER_DIR:-${ROOT_US}/data/live_real_test}"
REAL_LEDGER_FILE="${RESOLVED_LEDGER_DIR}/ledger_real.jsonl"

has_aporte() {
  "${PY_US}" - "${REAL_LEDGER_FILE}" <<'PY'
import sys
from pathlib import Path

sys.path.insert(0, "/home/wilson/USA_OPS")
import pipeline.ledger as ledger

ledger.LEDGER_PATH = Path(sys.argv[1])
events = ledger.read_all_events()
has = any(ev.type.value == "APORTE" for ev in events)
print("1" if has else "0")
PY
}

count_buys_today() {
  "${PY_US}" - "${REAL_LEDGER_FILE}" "${EXEC_DATE}" <<'PY'
import sys
from pathlib import Path
from datetime import date

sys.path.insert(0, "/home/wilson/USA_OPS")
import pipeline.ledger as ledger

ledger.LEDGER_PATH = Path(sys.argv[1])
target = date.fromisoformat(sys.argv[2])
events = ledger.read_all_events()
n = sum(1 for e in events if e.type.value == "BUY" and e.exec_date == target)
print(n)
PY
}

APORTE_FLAG="$(has_aporte)"
if [[ "${APORTE_FLAG}" != "1" ]]; then
  fail "Corte real (F-16) ainda nao foi executado: ${REAL_LEDGER_FILE} nao tem evento APORTE. Nada a encerrar."
fi

N_BUYS_TODAY="$(count_buys_today)"
if [[ "${N_BUYS_TODAY}" == "0" ]]; then
  log "AVISO: nenhuma compra real registrada em ${EXEC_DATE}. Emitindo boletim/friccao mesmo assim (posicoes/caixa carregados do ledger)."
fi

log ">> Emitindo boletim real-only..."
if ! "${PY_US}" "${CUTOVER_SCRIPT}" emit-boletim --exec-date "${EXEC_DATE}" "${LEDGER_DIR_ARGS[@]}" 2>&1 | tee -a "${LOG_FILE}"; then
  fail "emit-boletim falhou. Veja ${LOG_FILE}"
fi

log ">> Emitindo relatorio de friccao..."
if ! "${PY_US}" "${FRICTION_SCRIPT}" emit-friction-report --as-of-date "${EXEC_DATE}" "${LEDGER_DIR_ARGS[@]}" "${REAL_DIR_ARGS[@]}" 2>&1 | tee -a "${LOG_FILE}"; then
  fail "emit-friction-report falhou. Veja ${LOG_FILE}"
fi

BOLETIM_FILE="${RESOLVED_LEDGER_DIR}/${EXEC_DATE}.json"
FRICTION_FILE="${RESOLVED_LEDGER_DIR}/friction_report_${EXEC_DATE}.json"

N_LIVE_INFO="$("${PY_SALA}" - <<PY
import sys
sys.path.insert(0, "${DRYRUN_DIR}")
from dryrun_decision_b import _count_n_live_real_us
n, status = _count_n_live_real_us()
print(f"{n}|{status}")
PY
)"
N_LIVE_REAL="${N_LIVE_INFO%%|*}"
REAL_TEST_STATUS="${N_LIVE_INFO##*|}"

log ""
log "=== Resumo do encerramento (${EXEC_DATE}) ==="
log "Boletim real-only : ${BOLETIM_FILE}"
log "Relatorio friccao : ${FRICTION_FILE}"
log "n_live_real       : ${N_LIVE_REAL} (${REAL_TEST_STATUS})"

if [[ -f "${FRICTION_FILE}" ]]; then
  "${PY_US}" - "${FRICTION_FILE}" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
fric = payload.get("execution_friction", {})
cross = payload.get("operational_crosscheck", {})
print(f"Friccao total       : {fric.get('total_slippage_amount')} ({fric.get('slippage_pct_of_shadow_invested')})")
print(f"Trades casados      : {len(fric.get('trades_matched', []))}")
print(f"BUYs reais sem par  : {len(fric.get('unmatched_real_buys', []))}")
print(f"BUYs sombra sem par : {len(fric.get('unmatched_shadow_buys', []))}")
print(f"Dry-run missing     : {cross.get('dryrun_source_missing')}")
PY
fi

log ""
log "Encerramento concluido. Log completo em: ${LOG_FILE}"
if command -v notify-send >/dev/null 2>&1; then
  timeout 5s notify-send "USA Encerrar Dia" "n_live_real=${N_LIVE_REAL}. Log: ${LOG_FILE}" 2>/dev/null || true
fi
exit 0
