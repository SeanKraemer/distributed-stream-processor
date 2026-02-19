#!/bin/bash
# Pre-upload datasets to HyDFS before running experiments
# This avoids repeated large file uploads during experiments

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "📤 Uploading Datasets to HyDFS"
echo "==============================="
echo ""

# Start cluster if not running
if ! tmux has-session -t rainstorm 2>/dev/null; then
    echo "🚀 Starting RainStorm cluster..."
    "$PROJECT_ROOT/scripts/common/start_cluster.sh"
    sleep 30
else
    echo "✅ RainStorm cluster already running"
fi

# Upload medium dataset
echo ""
echo "📁 Uploading synthetic_medium.csv (127 MB)..."
docker exec node1 "
    cd /app
    echo 'delete synthetic_medium.csv' | nc localhost 8003
    sleep 2
    echo 'create data/synthetic_medium.csv synthetic_medium.csv' | nc localhost 8003
"
echo "   ✅ Medium dataset uploaded"

sleep 5

# Upload large dataset
echo ""
echo "📁 Uploading synthetic_large.csv (254 MB)..."
echo "   ⚠️  This may take 30-60 seconds..."
docker exec node1 "
    cd /app
    echo 'delete synthetic_large.csv' | nc localhost 8003
    sleep 2
    echo 'create data/synthetic_large.csv synthetic_large.csv' | nc localhost 8003
"
echo "   ✅ Large dataset uploaded"

echo ""
echo "✅ All datasets uploaded to HyDFS!"
echo ""
echo "📋 Verify uploads:"
echo "   docker exec node1 'echo ls | nc localhost 8003'"
echo ""
echo "📌 Next step:"
echo "   Run experiments: nohup ./scripts/experiments/run_experiments.sh > experiments.log 2>&1 &"
echo ""
