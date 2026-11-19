#!/usr/bin/env bash
# Removes the Karavan containers AND the karavan_postgres_data volume.
# Karavan's projects are safe: they live in the Git repository.
# Usage: ./scripts/clean.sh

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_docker
compose down -v
