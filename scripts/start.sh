#!/usr/bin/env bash
# Starts the already created Karavan containers (after stop.sh).
# Usage: ./scripts/start.sh

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_docker
compose start
