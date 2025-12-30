#!/usr/bin/env bash
set -euo pipefail

# reset_database.sh
# Purpose: Apply database schema from database/init.sql to a Postgres instance.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"

INIT_SQL="${INIT_SQL:-database/init.sql}"
DATABASE_URL="${DATABASE_URL:-postgresql://user:password@localhost/browser_automation}"

if [[ ! -f "${INIT_SQL}" ]]; then
  echo "[db] init.sql not found: ${INIT_SQL}" >&2
  exit 1
fi

echo "[db] Applying schema: ${INIT_SQL}"
echo "[db] Target: ${DATABASE_URL}"

# Requires: psql installed and DATABASE_URL reachable
psql "${DATABASE_URL}" -f "${INIT_SQL}"

echo "[db] Done."
