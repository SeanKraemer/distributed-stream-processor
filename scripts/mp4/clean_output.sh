#!/bin/bash
# Clean RainStorm output files locally and inside the cluster containers.
# Usage: ./scripts/mp4/clean_output.sh [local|remote|all]

set -e

MODE="${1:-all}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "RainStorm Output Cleanup"
echo "========================"

clean_local() {
    echo "Cleaning local rainstorm_outputs/..."
    mkdir -p "$PROJECT_ROOT/rainstorm_outputs"
    find "$PROJECT_ROOT/rainstorm_outputs" -mindepth 1 -type f ! -name '.gitkeep' -delete 2>/dev/null || true
    find "$PROJECT_ROOT/rainstorm_outputs" -mindepth 1 -type d -exec rm -rf {} + 2>/dev/null || true
    echo "   done"
}

clean_remote() {
    echo "Cleaning /app/rainstorm_outputs on cluster containers..."
    for n in $(seq 1 10); do
        NODE="node$n"
        echo -n "   $NODE: "
        if docker exec "$NODE" sh -c 'rm -rf /app/rainstorm_outputs/* 2>/dev/null; true' 2>/dev/null; then
            echo "cleaned"
        else
            echo "skipped (not running)"
        fi
    done
}

case "$MODE" in
    local)  clean_local ;;
    remote) clean_remote ;;
    all)    clean_local; clean_remote ;;
    *)
        echo "Invalid mode: $MODE"
        echo "Usage: $0 [local|remote|all]"
        exit 1
        ;;
esac

echo ""
echo "Output cleanup complete."
