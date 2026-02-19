#!/bin/bash
# Start the 10-node RainStorm cluster locally using Docker Compose.
#
# Usage:  ./scripts/local/start_cluster.sh [--build]
#   --build   Rebuild the Docker image before starting

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT"

if [[ "${1:-}" == "--build" ]]; then
    echo "Building Docker image..."
    docker compose build
fi

echo "Starting 10-node cluster..."
docker compose up -d

echo ""
echo "Cluster is starting. Waiting for SWIM membership to converge (~5s)..."
sleep 5

echo ""
echo "Container status:"
docker compose ps

echo ""
echo "Usage:"
echo "  View logs:        docker compose logs -f"
echo "  Attach to node1:  docker attach node1"
echo "  Submit a job:     scripts/local/run_demo.sh"
echo "  Stop cluster:     scripts/local/stop_cluster.sh"
