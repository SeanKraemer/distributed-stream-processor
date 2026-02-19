#!/bin/bash
# Script to clear all logs both locally and on VMs
# Usage: ./scripts/clean_logs.sh [local|remote|all]

set -e

MODE="${1:-all}"

echo "🧹 Log Cleanup Script"
echo "===================="

# Function to clean local logs
clean_local() {
    echo "🗑️  Cleaning local logs..."
    if [ -d "logs" ]; then
        rm -f logs/*.log
        echo "✅ Local logs directory cleared"
    else
        echo "⚠️  Local logs directory not found"
    fi
}

# Function to clean remote VM logs
clean_remote() {
    echo "🗑️  Cleaning logs on remote VMs..."

    CLUSTER_USER=""
    # Read VM hostnames from config.json or use default list
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

    for vm in "${VMS[@]}"; do
        echo "  Cleaning logs on $vm..."
        ssh ${NETID}@${vm} "cd /app && rm -f logs/*.log" 2>/dev/null && \
            echo "    ✅ $vm cleaned" || \
            echo "    ⚠️  Failed to clean $vm (might not exist or not accessible)"
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
echo "✅ Log cleanup complete!"
