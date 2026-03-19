#!/usr/bin/env bash
set -euo pipefail

PROTECTED_PREFIXES=(
  "lib/"
  "pipeline/"
  "scripts/"
)
PROTECTED_FILES=(
  "config/winner_us.json"
)

if ! command -v git >/dev/null 2>&1; then
  exit 0
fi

STAGED="$(git diff --cached --name-only --diff-filter=ACMR || true)"
if [[ -z "${STAGED}" ]]; then
  exit 0
fi

needs_guard=0
while IFS= read -r f; do
  [[ -z "${f}" ]] && continue
  for p in "${PROTECTED_PREFIXES[@]}"; do
    if [[ "${f}" == "${p}"* ]]; then
      needs_guard=1
      break
    fi
  done
  for pf in "${PROTECTED_FILES[@]}"; do
    if [[ "${f}" == "${pf}" ]]; then
      needs_guard=1
      break
    fi
  done
  [[ "${needs_guard}" -eq 1 ]] && break
done <<< "${STAGED}"

if [[ "${needs_guard}" -eq 0 ]]; then
  exit 0
fi

if [[ "${MOTOR_OVERRIDE:-0}" != "1" ]]; then
  echo "ERROR: Arquivos do motor estão protegidos."
  echo "Você tentou commitar alterações em: lib/, pipeline/, scripts/ ou config/winner_us.json"
  echo
  echo "Para prosseguir, rode o commit com a variável:"
  echo "  MOTOR_OVERRIDE=1 git commit ..."
  echo
  exit 1
fi

exit 0

