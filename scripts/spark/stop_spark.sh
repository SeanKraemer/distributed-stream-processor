#!/bin/bash
# Stop Spark cluster on all VMs
#
# Usage:
#   ./scripts/spark/stop_spark.sh

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

echo "🛑 Stopping Spark cluster..."

# Stop master
docker exec node1 "/opt/spark/sbin/stop-master.sh 2>/dev/null || true"

# Stop all workers
for vm in "${VMS[@]:1}"; do
    ssh -q "$vm" "/opt/spark/sbin/stop-worker.sh 2>/dev/null || true" &
done
wait

echo "✅ Spark cluster stopped"
