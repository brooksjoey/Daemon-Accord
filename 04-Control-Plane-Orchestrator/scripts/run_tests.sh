#!/usr/bin/env bash
set -euo pipefail

# run_tests.sh
# Purpose: Run pytest suite with the local venv.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"

VENV_DIR="${VENV_DIR:-.venv}"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "[test] venv not found at ${VENV_DIR}. Run scripts/setup_environment.sh first." >&2
  exit 1
fi

echo "[test] Running pytest..."
"${VENV_DIR}/bin/python" -m pytest "${@:-tests}"
