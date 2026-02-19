#!/bin/bash
# Script to clean RainStorm output files both locally and on VMs
# Usage: ./scripts/mp4/clean_output.sh [local|remote|all]

set -e

MODE="${1:-all}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "🧹 RainStorm Output Cleanup Script"
echo "==================================="

CLUSTER_USER=""
VMS=(
    "node1"
    "node1"
    "node1"
    "node1"
    "node1"
    "node1"
    "node1"
    "node1"
    "node1"
    "node1"
)

# Function to clean local output
clean_local() {
    echo "🗑️  Cleaning local rainstorm_outputs..."
    if [ -d "$PROJECT_ROOT/rainstorm_outputs" ]; then
        # Remove all files and subdirectories except .gitkeep
        find "$PROJECT_ROOT/rainstorm_outputs" -mindepth 1 -type f ! -name '.gitkeep' -delete 2>/dev/null || true
        find "$PROJECT_ROOT/rainstorm_outputs" -mindepth 1 -type d -exec rm -rf {} + 2>/dev/null || true
        echo "   ✅ Local rainstorm_outputs directory cleared"
    else
        echo "   ⚠️  Local rainstorm_outputs directory not found, creating..."
        mkdir -p "$PROJECT_ROOT/rainstorm_outputs"
        touch "$PROJECT_ROOT/rainstorm_outputs/.gitkeep"
    fi
}

# Function to clean remote VM output
clean_remote() {
    echo "🗑️  Cleaning rainstorm_outputs on remote VMs..."

    for vm in "${VMS[@]}"; do
        VM_SHORT=$(echo "$vm" | sed 's/node1/VM/' | # removed domain stripping)
        echo -n "   $VM_SHORT: "

        # Clean /app/rainstorm_outputs/ directory (our rainstorm output and logs)
        # Use 'true' to ensure success even if directory doesn't exist
        if ssh -q -o ConnectTimeout=5 ${NETID}@${vm} "rm -rf /app/rainstorm_outputs/* 2>/dev/null; true" 2>/dev/null; then
            echo "✅ cleaned"
        else
            echo "⚠️  failed (VM might not be accessible)"
        fi
    done
}

# Main execution
case "$MODE" in
    local)
        clean_local
        ;;
    remote)
        clean_remote
        ;;
    all)
        clean_local
        clean_remote
        ;;
    *)
        echo "❌ Invalid mode: $MODE"
        echo "Usage: $0 [local|remote|all]"
        exit 1
        ;;
esac

echo ""
echo "✅ Output cleanup complete!"
