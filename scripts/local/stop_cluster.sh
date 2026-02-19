#!/bin/bash
# Stop the RainStorm cluster and remove containers.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/../.."

docker compose down
echo "Cluster stopped."
