#!/bin/bash
# Clear node logs.
#
# logs/ is bind-mounted into every container as /app/logs, so cleaning the
# local directory clears the cluster's logs too.
#
# Usage: ./scripts/common/clean_logs.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Cleaning logs/..."
mkdir -p "$PROJECT_ROOT/logs"
rm -f "$PROJECT_ROOT/logs"/*.log
echo "Log cleanup complete."
