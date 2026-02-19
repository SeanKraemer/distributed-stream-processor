#!/bin/bash
# Upload test data to HyDFS
# Uses netcat to send commands to VM1's client port (same pattern as send_cmd.sh)
#
# Usage: ./scripts/mp4/upload_test_data.sh [--hydfs-only]
#
# This script uploads datasets needed for demo tests.
# The source task will try HyDFS first, then fall back to local filesystem (data/).
#
# NOTE: dataset2.csv (2.3MB) may fail HyDFS upload due to buffer limits.
#       The local filesystem fallback will handle this automatically.

CLUSTER_USER=""
VM1="node1"
CLIENT_PORT=8003

# Function to send HyDFS command (same pattern as send_cmd.sh)
send_hydfs_cmd() {
    local cmd="$1"
    echo "   -> $cmd"
    echo "$cmd" | ssh -q ${NETID}@${VM1} "nc localhost ${CLIENT_PORT}"
}

echo "📤 Uploading test data to HyDFS..."
echo ""
echo "💡 Note: Source tasks will fall back to local data/ directory if HyDFS fails."
echo ""

# Check if VM1 is reachable
echo "🔍 Checking connection to VM1..."
if ! ssh -q -o ConnectTimeout=5 ${NETID}@${VM1} "echo 'Connection OK'" > /dev/null 2>&1; then
    echo "❌ Cannot connect to VM1. Is the cluster running?"
    exit 1
fi

echo ""
echo "📁 Uploading dataset1.csv (735 KB - for Test 0, 1, 2)..."
send_hydfs_cmd "create data/dataset1.csv dataset1.csv"
sleep 10

# dataset2.csv is large (2.3MB) and may hit buffer limits
# The source task will automatically fall back to local filesystem
echo ""
echo "📁 Attempting dataset2.csv (2.3 MB - may use local fallback)..."
send_hydfs_cmd "create data/dataset2.csv dataset2.csv"

echo ""
echo "✅ Upload complete!"
echo ""
echo "📋 Files available for RainStorm:"
echo "   - dataset1.csv (735 KB) - Test 0, 1, 2"
echo "   - dataset2.csv (2.3 MB) - Test 1, 2 (uses local fallback if HyDFS fails)"
echo ""
echo "💡 Verify HyDFS files: ./scripts/common/send_cmd.sh 1 ls"
echo "💡 Local fallback uses: data/<filename> on each VM"
