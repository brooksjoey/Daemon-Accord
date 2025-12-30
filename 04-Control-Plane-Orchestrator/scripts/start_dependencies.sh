#!/usr/bin/env bash
set -euo pipefail

# start_dependencies.sh
# Purpose: Start local dependency services (Postgres/Redis/MinIO) via docker compose.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"

COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"
SERVICES=("${@:-postgres redis minio}")

echo "[deps] Project: ${PROJECT_DIR}"
echo "[deps] Starting services: ${SERVICES[*]}"

${COMPOSE_BIN} up -d "${SERVICES[@]}"

echo "[deps] Done."
