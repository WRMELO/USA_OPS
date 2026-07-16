#!/usr/bin/env bash
# registrar_ordem_real.sh
# Atalho operacional da fase LIVE-REAL-TEST:
# registra o par BUY real + BUY sombra para cada fill.
set -uo pipefail
trap '' PIPE

ROOT_US="/home/wilson/USA_OPS"
PY_US="${ROOT_US}/.venv/bin/python"
CUTOVER_SCRIPT="${ROOT_US}/scripts/live_real_cutover.py"
FRICTION_SCRIPT="${ROOT_US}/scripts/friction_ruler.py"
LOOKUP_SCRIPT="${ROOT_US}/scripts/lookup_shadow_price.py"
LOG_FILE="/tmp/usa_registrar_ordem_real.log"

EXEC_DATE=""
LEDGER_DIR=""
TICKER=""
QTD=""
PRECO=""
PRECO_SOMBRA=""
AUTO_YES=0
N_OK=0
MODE_INTERACTIVE=1

usage() {
  cat <<'EOF'
Uso: ./registrar_ordem_real.sh [opcoes]
  --exec-date YYYY-MM-DD   Data de execucao (default: hoje).
  --ledger-dir DIR         Diretorio do ledger LIVE-REAL-TEST (default: data/live_real_test).
  --ticker TICKER          Modo nao-interativo: ticker da ordem.
  --qtd N                  Modo nao-interativo: quantidade.
  --preco P                Modo nao-interativo: preco real de execucao.
  --preco-sombra S         Modo nao-interativo: preco sombra (default: auto-lookup).
  --yes                    Modo nao-interativo: grava sem pedir confirmacao.
  -h, --help               Esta ajuda.
Sem --ticker/--qtd/--preco, o script entra em loop interativo.
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
    --ticker)
      TICKER="$2"
      shift 2
      ;;
    --qtd)
      QTD="$2"
      shift 2
      ;;
    --preco)
      PRECO="$2"
      shift 2
      ;;
    --preco-sombra)
      PRECO_SOMBRA="$2"
      shift 2
      ;;
    --yes)
      AUTO_YES=1
      shift
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
echo "=== $(date '+%F %T') registrar-ordem-real (exec_date=${EXEC_DATE}) ===" > "${LOG_FILE}"

[[ -x "${PY_US}" ]] || fail "Python da USA_OPS nao encontrado em ${PY_US}"
"${PY_US}" --version >>"${LOG_FILE}" 2>&1 || fail "Health-check do venv USA_OPS falhou. Veja ${LOG_FILE}"

LEDGER_DIR_ARGS=()
if [[ -n "${LEDGER_DIR}" ]]; then
  LEDGER_DIR_ARGS=(--ledger-dir "${LEDGER_DIR}")
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

APORTE_FLAG="$(has_aporte)"
if [[ "${APORTE_FLAG}" != "1" ]]; then
  fail "Corte real (F-16) ainda nao foi executado: ${REAL_LEDGER_FILE} nao tem evento APORTE. Rode o corte pelo ciclo formal antes de registrar ordens."
fi

lookup_shadow_price_json() {
  local ticker="$1"
  "${PY_US}" "${LOOKUP_SCRIPT}" --ticker "${ticker}" --exec-date "${EXEC_DATE}" 2>>"${LOG_FILE}"
}

record_pair() {
  local ticker="$1"
  local qtd="$2"
  local preco="$3"
  local preco_sombra="$4"

  log ">> Gravando BUY real: ${ticker} qtd=${qtd} preco=${preco} exec_date=${EXEC_DATE}"
  if ! "${PY_US}" "${CUTOVER_SCRIPT}" record-buy \
      --ticker "${ticker}" --qtd "${qtd}" --preco "${preco}" \
      --exec-date "${EXEC_DATE}" "${LEDGER_DIR_ARGS[@]}" 2>&1 | tee -a "${LOG_FILE}"; then
    log "FALHA: BUY real nao foi gravado para ${ticker}. Nenhum BUY sombra sera gravado para esta ordem."
    return 1
  fi

  log ">> Gravando BUY sombra: ${ticker} qtd=${qtd} preco=${preco_sombra} exec_date=${EXEC_DATE}"
  if ! "${PY_US}" "${FRICTION_SCRIPT}" record-shadow-buy \
      --ticker "${ticker}" --qtd "${qtd}" --preco "${preco_sombra}" \
      --exec-date "${EXEC_DATE}" "${LEDGER_DIR_ARGS[@]}" 2>&1 | tee -a "${LOG_FILE}"; then
    log "ALERTA CRITICO: BUY real de ${ticker} foi gravado, mas o BUY sombra FALHOU."
    log "Corrija manualmente antes de encerrar o dia, rodando:"
    log "  ${PY_US} ${FRICTION_SCRIPT} record-shadow-buy --ticker ${ticker} --qtd ${qtd} --preco ${preco_sombra} --exec-date ${EXEC_DATE} ${LEDGER_DIR_ARGS[*]}"
    return 1
  fi

  log "OK: par real+sombra gravado para ${ticker}."
  return 0
}

process_one_order() {
  local raw_ticker="$1"
  local qtd="$2"
  local preco="$3"
  local preco_sombra="$4"
  local auto_confirm="$5"
  local ticker lookup_json lookup_found lookup_market_day lookup_close
  ticker="$(echo "${raw_ticker}" | tr '[:lower:]' '[:upper:]' | xargs)"
  lookup_market_day=""

  if [[ -z "${ticker}" || -z "${qtd}" || -z "${preco}" ]]; then
    log "ERRO: ticker, quantidade e preco real sao obrigatorios. Ordem ignorada."
    return 1
  fi

  if [[ -z "${preco_sombra}" ]]; then
    lookup_json="$(lookup_shadow_price_json "${ticker}")"
    lookup_found="$(printf '%s' "${lookup_json}" | "${PY_US}" -c 'import json,sys; d=json.loads(sys.stdin.read() or "{}"); print("1" if d.get("found") else "0")' 2>/dev/null || echo "0")"
    if [[ "${lookup_found}" == "1" ]]; then
      lookup_close="$(printf '%s' "${lookup_json}" | "${PY_US}" -c 'import json,sys; print(json.loads(sys.stdin.read())["close"])' 2>/dev/null || true)"
      lookup_market_day="$(printf '%s' "${lookup_json}" | "${PY_US}" -c 'import json,sys; print(json.loads(sys.stdin.read())["market_day"])' 2>/dev/null || true)"
      preco_sombra="${lookup_close}"
    elif [[ "${MODE_INTERACTIVE}" == "1" ]]; then
      read -r -p "Preco sombra nao encontrado automaticamente. Informe manualmente: " preco_sombra
    else
      log "ERRO: preco sombra ausente e sem fechamento auto disponivel para ${ticker}."
      return 1
    fi
  fi

  if [[ -z "${preco_sombra}" ]]; then
    log "ERRO: preco sombra ausente para ${ticker}. Ordem ignorada."
    return 1
  fi

  echo ""
  echo "Resumo da ordem:"
  echo "  ticker        = ${ticker}"
  echo "  qtd           = ${qtd}"
  echo "  preco real    = ${preco}"
  if [[ -n "${lookup_market_day}" ]]; then
    echo "  preco sombra  = ${preco_sombra} (auto-lookup fechamento ${lookup_market_day})"
  else
    echo "  preco sombra  = ${preco_sombra} (manual)"
  fi
  echo "  exec_date     = ${EXEC_DATE}"

  if [[ "${auto_confirm}" != "1" ]]; then
    read -r -p "Confirma gravacao deste par (real+sombra)? [s/N]: " resp
    if [[ ! "${resp}" =~ ^[sS]$ ]]; then
      log "Ordem descartada pelo Owner (sem confirmacao)."
      return 1
    fi
  fi

  record_pair "${ticker}" "${qtd}" "${preco}" "${preco_sombra}"
}

if [[ -n "${TICKER}" || -n "${QTD}" || -n "${PRECO}" ]]; then
  MODE_INTERACTIVE=0
  [[ -n "${TICKER}" && -n "${QTD}" && -n "${PRECO}" ]] || fail "Modo nao-interativo requer --ticker, --qtd e --preco juntos."
  if process_one_order "${TICKER}" "${QTD}" "${PRECO}" "${PRECO_SOMBRA}" "${AUTO_YES}"; then
    N_OK=1
    log ""
    log "Resumo da sessao: ${N_OK} par(es) real+sombra gravado(s)."
    exit 0
  fi
  log ""
  log "Resumo da sessao: 0 par(es) real+sombra gravado(s)."
  exit 1
fi

MODE_INTERACTIVE=1
echo "=== USA - Registrar Ordem (real + sombra) — LIVE-REAL-TEST ==="
echo "exec_date = ${EXEC_DATE}"

while true; do
  echo ""
  read -r -p "Ticker (ou vazio para terminar): " in_ticker
  [[ -z "${in_ticker}" ]] && break
  read -r -p "Quantidade: " in_qtd
  read -r -p "Preco real de execucao (BTG): " in_preco
  read -r -p "Preco sombra (Enter para usar fechamento auto, se disponivel): " in_preco_sombra

  if process_one_order "${in_ticker}" "${in_qtd}" "${in_preco}" "${in_preco_sombra}" "0"; then
    N_OK=$((N_OK + 1))
  fi

  read -r -p "Registrar outra ordem? [s/N]: " resp_again
  [[ "${resp_again}" =~ ^[sS]$ ]] || break
done

echo ""
log "Resumo da sessao: ${N_OK} par(es) real+sombra gravado(s) com sucesso."
log "Log completo em: ${LOG_FILE}"
if command -v notify-send >/dev/null 2>&1; then
  timeout 5s notify-send "USA Registrar Ordem" "${N_OK} par(es) gravado(s). Log: ${LOG_FILE}" 2>/dev/null || true
fi
exit 0
