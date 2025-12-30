#!/usr/bin/env bash
set -euo pipefail

# lint.sh
# Purpose: Run the repo's configured lint checks (black, pylint, mypy).

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"

VENV_DIR="${VENV_DIR:-.venv}"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "[lint] venv not found at ${VENV_DIR}. Run scripts/setup_environment.sh first." >&2
  exit 1
fi

echo "[lint] black --check src/ tests/"
"${VENV_DIR}/bin/python" -m black --check src/ tests/

echo "[lint] pylint src/ tests/ --disable=C,R,W0613"
"${VENV_DIR}/bin/python" -m pylint src/ tests/ --disable=C,R,W0613

echo "[lint] mypy src/ --ignore-missing-imports"
"${VENV_DIR}/bin/python" -m mypy src/ --ignore-missing-imports
