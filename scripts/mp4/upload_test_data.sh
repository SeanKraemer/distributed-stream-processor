#!/bin/bash
# Upload test datasets to HyDFS on the local Docker cluster.
#
# Usage: ./scripts/mp4/upload_test_data.sh
#
# Sends `create` commands to node1's client port. Source tasks fall back to
# the local data/ directory (bind-mounted read-only into every container) if
# a file is missing from HyDFS.

set -uo pipefail

CLIENT_PORT=8003

send_hydfs_cmd() {
    local cmd="$1"
    echo "   -> $cmd"
    docker exec node1 sh -c "echo '$cmd' | nc -w 10 localhost $CLIENT_PORT"
}

if ! docker ps --filter "name=node1" --filter "status=running" -q | grep -q .; then
    echo "ERROR: node1 is not running. Start the cluster first:  make up"
    exit 1
fi

echo "Uploading test data to HyDFS..."
echo ""

echo "dataset1.csv (735 KB — Tests 0, 1, 2)..."
send_hydfs_cmd "create /app/data/dataset1.csv dataset1.csv"
sleep 2

# dataset2.csv is larger (2.3 MB); source tasks fall back to the bind-mounted
# data/ directory automatically if the HyDFS upload hits buffer limits.
echo ""
echo "dataset2.csv (2.3 MB — Test 3)..."
send_hydfs_cmd "create /app/data/dataset2.csv dataset2.csv"
sleep 2

echo ""
echo "Upload complete."
echo "Verify:  docker exec node1 sh -c \"echo 'ls dataset1.csv' | nc -w 3 localhost $CLIENT_PORT\""
