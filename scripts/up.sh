#!/usr/bin/env bash
# Starts Karavan (Karavan, its Postgres and the image registry).
# Needs the shared cluster first: ../cluster/scripts/up.sh (network yape-local).
# Usage: ./scripts/up.sh

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_docker
require_cluster_network
warn_if_no_token

echo "🚀  Starting Karavan..."
compose up -d

PORT="$(grep -E '^KARAVAN_PORT=' "${REPO_DIR}/.env" 2>/dev/null | cut -d= -f2- || true)"
echo "✅  Karavan is starting: http://localhost:${PORT:-9998}"
echo "    First start pulls images and may take a minute. Follow it with ./scripts/logs.sh -f"
