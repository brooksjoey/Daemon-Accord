#!/usr/bin/env bash
set -euo pipefail

# stop_dependencies.sh
# Purpose: Stop local dependency services via docker compose.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"

COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"

echo "[deps] Project: ${PROJECT_DIR}"
echo "[deps] Stopping services..."

${COMPOSE_BIN} down

echo "[deps] Done."
