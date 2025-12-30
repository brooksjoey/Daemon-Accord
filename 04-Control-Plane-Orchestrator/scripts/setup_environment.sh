#!/usr/bin/env bash
set -euo pipefail

# setup_environment.sh
# Purpose: Bootstrap local dev environment for 04-Control-Plane-Orchestrator.
# NOTE: This script is intended to be run manually. It does not start long-lived services.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-.venv}"

echo "[setup] Project: ${PROJECT_DIR}"

echo "[setup] Checking python..."
command -v "${PYTHON_BIN}" >/dev/null 2>&1 || {
  echo "[error] '${PYTHON_BIN}' not found in PATH" >&2
  exit 1
}

echo "[setup] Creating venv: ${VENV_DIR}"
"${PYTHON_BIN}" -m venv "${VENV_DIR}"

echo "[setup] Upgrading pip..."
"${VENV_DIR}/bin/python" -m pip install --upgrade pip

echo "[setup] Installing python requirements..."
"${VENV_DIR}/bin/pip" install -r requirements.txt

if [[ -f ".env.example" ]]; then
  if [[ ! -f ".env" ]]; then
    echo "[setup] Creating .env from .env.example"
    cp ".env.example" ".env"
  else
    echo "[setup] .env already exists; leaving unchanged"
  fi
else
  echo "[setup] .env.example not found; skipping .env creation"
fi

echo
echo "[setup] Done."
echo
echo "Next steps (manual):"
echo "- Activate venv:"
echo "    source \"${VENV_DIR}/bin/activate\""
echo "- Start dependencies (examples):"
echo "    docker compose up -d postgres redis minio"
echo "- Run the app (example):"
echo "    python main.py"
echo "- Run tests (examples):"
echo "    pytest"
