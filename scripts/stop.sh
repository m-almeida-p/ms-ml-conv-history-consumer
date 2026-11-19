#!/usr/bin/env bash
# Stops the Karavan containers without removing them or their data.
# Usage: ./scripts/stop.sh

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_docker
compose stop
