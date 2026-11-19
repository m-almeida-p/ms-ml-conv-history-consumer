#!/usr/bin/env bash
# Shared helpers for the Karavan scripts. Sourced, not executed.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${REPO_DIR}/docker-compose.yml"

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

require_docker() {
  if ! docker info >/dev/null 2>&1; then
    echo "❌  Docker is not running." >&2
    exit 1
  fi
}

# The compose file joins the network that the shared cluster creates.
require_cluster_network() {
  if ! docker network inspect yape-local >/dev/null 2>&1; then
    echo "❌  Network yape-local not found. Start the shared cluster first:" >&2
    echo "    ${REPO_DIR}/../cluster/scripts/up.sh" >&2
    exit 1
  fi
}

# Karavan needs the GitHub token to read and write its projects. Only checks
# that the value is set; it is never printed.
warn_if_no_token() {
  if ! grep -qE '^GITHUB_MALMEIDAP_PAT=.+' "${REPO_DIR}/.env" 2>/dev/null; then
    echo "⚠️   GITHUB_MALMEIDAP_PAT is empty or .env is missing." >&2
    echo "    Karavan will start but cannot reach the Git repository." >&2
    echo "    Copy .env.example to .env and set the token." >&2
  fi
}
