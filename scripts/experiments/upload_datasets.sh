#!/bin/bash
#
# NOTE: VM-era script. This targeted the original 10-VM deployment used for
# the RainStorm-vs-Spark benchmarking experiments and is retained to document
# that methodology. It is NOT runnable against the local Docker cluster.
#
# Upload synthetic datasets to all VMs
#
# Usage:
#   ./scripts/experiments/upload_datasets.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

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

echo "📤 Uploading Synthetic Datasets to VMs"
echo "======================================="
echo ""

# Upload synthetic datasets
for dataset in "synthetic_medium.csv" "synthetic_large.csv"; do
    if [ ! -f "$PROJECT_ROOT/data/$dataset" ]; then
        echo "⚠️  $dataset not found, generating..."
        python3 "$PROJECT_ROOT/scripts/experiments/generate_datasets.py"
        break
    fi
done

for dataset in "synthetic_medium.csv" "synthetic_large.csv"; do
    echo "📁 Uploading $dataset..."
    for vm in "${VMS[@]}"; do
        scp -q "$PROJECT_ROOT/data/$dataset" "$vm:/app/data/" &
    done
    wait
    echo "   ✅ $dataset uploaded to all VMs"
done

# Also upload Spark apps
echo ""
echo "📁 Uploading Spark applications..."
for vm in "${VMS[@]}"; do
    ssh -q "$vm" "mkdir -p /app/spark_apps" &
done
wait

for vm in "${VMS[@]}"; do
    scp -q "$PROJECT_ROOT/spark_apps"/*.py "$vm:/app/spark_apps/" &
done
wait
echo "   ✅ Spark apps uploaded to all VMs"

echo ""
echo "✅ All uploads complete!"
echo ""
