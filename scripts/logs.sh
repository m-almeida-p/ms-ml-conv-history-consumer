#!/usr/bin/env bash
# Karavan logs.
#   ./scripts/logs.sh        saves them to logs_<timestamp>.txt in the repo root
#   ./scripts/logs.sh -f     follows them in the terminal
# logs_*.txt is git-ignored.

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_docker

if [ "${1:-}" = "-f" ]; then
  compose logs -f
  exit 0
fi

OUT="${REPO_DIR}/logs_$(date +%Y%m%d_%H%M%S).txt"
compose logs > "${OUT}"
echo "📝  Logs saved to ${OUT}"
