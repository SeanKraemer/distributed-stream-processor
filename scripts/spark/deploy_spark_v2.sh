#!/bin/bash
#
# NOTE: VM-era script. This targeted the original 10-VM deployment used for
# the RainStorm-vs-Spark benchmarking experiments and is retained to document
# that methodology. It is NOT runnable against the local Docker cluster.
#
# Deploy Spark Streaming to all VMs (Fixed version)
#
# Usage:
#   ./scripts/spark/deploy_spark_v2.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "🚀 Spark Deployment Script (v2 - Fixed)"
echo "=========================================="
echo ""

# VM configuration
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

MASTER_VM="node1"
SPARK_VERSION="spark-4.0.1-bin-hadoop3"
SPARK_ARCHIVE="${SPARK_VERSION}.tgz"
SPARK_HOME="/opt/spark"

echo "📦 Step 1: Uploading Spark archive to all VMs..."
echo "------------------------------------------------"

for vm in "${VMS[@]}"; do
    echo "   Uploading to $vm..."
    scp -q "$PROJECT_ROOT/$SPARK_ARCHIVE" "$vm:~/" &
done
wait
echo "   ✅ Upload complete"
echo ""

echo "📦 Step 2: Extracting Spark on all VMs..."
echo "------------------------------------------"

for vm in "${VMS[@]}"; do
    echo "   Extracting on $vm..."
    ssh -q "$vm" "
        rm -rf /opt/spark
        tar -xzf ~/$SPARK_ARCHIVE
        mv ~/$SPARK_VERSION /opt/spark
        chmod +x /opt/spark/bin/*
        chmod +x /opt/spark/sbin/*
    " &
done
wait
echo "   ✅ Extraction complete"
echo ""

echo "⚙️  Step 3: Configuring Spark on all VMs..."
echo "--------------------------------------------"

# Configure Spark environment on ALL VMs (not just master)
for vm in "${VMS[@]}"; do
    ssh -q "$vm" "cat > /opt/spark/conf/spark-env.sh << 'EOF'
#!/usr/bin/env bash

# Spark master configuration
export SPARK_MASTER_HOST=node1
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Worker configuration
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081

# Java configuration (Java 17 on these VMs)
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-17.0.17.0.10-1.el9.x86_64

# Logging
export SPARK_LOG_DIR=/opt/spark/logs
export SPARK_PID_DIR=/opt/spark/run

EOF
chmod +x /opt/spark/conf/spark-env.sh
mkdir -p /opt/spark/logs /opt/spark/run
" &
done
wait

echo "   ✅ Configuration complete"
echo ""

echo "🚀 Step 4: Starting Spark cluster..."
echo "-------------------------------------"

# Start master on VM01
echo "   Starting master on $MASTER_VM..."
ssh -q "$MASTER_VM" "
    /opt/spark/sbin/stop-master.sh 2>/dev/null || true
    /opt/spark/sbin/start-master.sh
"
sleep 5

# Start workers on VM02-VM10 (individually, no inter-VM SSH needed)
echo "   Starting workers on VMs 02-10..."
for vm in "${VMS[@]:1}"; do
    echo "      Starting worker on $vm..."
    ssh -q "$vm" "
        /opt/spark/sbin/stop-worker.sh 2>/dev/null || true
        /opt/spark/sbin/start-worker.sh spark://$MASTER_VM:7077
    " &
done
wait

sleep 5
echo "   ✅ Spark cluster started"
echo ""

echo "📊 Step 5: Verifying cluster status..."
echo "---------------------------------------"

# Check master
MASTER_RUNNING=$(ssh -q "$MASTER_VM" "ps aux | grep -v grep | grep -c 'org.apache.spark.deploy.master.Master'" || echo "0")
if [ "$MASTER_RUNNING" -gt 0 ]; then
    echo "   ✅ Master running on $MASTER_VM"
else
    echo "   ⚠️  Master NOT running on $MASTER_VM"
fi

# Check workers
WORKER_COUNT=0
for vm in "${VMS[@]:1}"; do
    WORKER_RUNNING=$(ssh -q "$vm" "ps aux | grep -v grep | grep -c 'org.apache.spark.deploy.worker.Worker'" || echo "0")
    if [ "$WORKER_RUNNING" -gt 0 ]; then
        ((WORKER_COUNT++))
    fi
done

echo "   Workers running: $WORKER_COUNT / 9"
echo ""

if [ "$MASTER_RUNNING" -gt 0 ] && [ "$WORKER_COUNT" -ge 5 ]; then
    echo "✅ Spark Deployment Successful!"
else
    echo "⚠️  Spark Deployment Partial - Some components may not be running"
fi

echo ""
echo "📌 Cluster Information:"
echo "   Master URL:  spark://node1:7077"
echo "   Master Web UI:      http://node1:8080"
echo "   Workers:     $WORKER_COUNT / 9 active"
echo ""
echo "📌 Verification Commands:"
echo "   Check master: docker exec $MASTER_VM 'ps aux | grep Master'"
echo "   Check worker: docker exec node1 'ps aux | grep Worker'"
echo ""
echo "📌 Management Commands:"
echo "   Stop all:  ./scripts/spark/stop_spark.sh"
echo "   Restart:   ./scripts/spark/deploy_spark_v2.sh (this script)"
echo ""
echo "📌 Next Steps:"
echo "   1. Upload datasets: ./scripts/experiments/upload_datasets.sh"
echo "   2. Run experiments: ./scripts/experiments/run_experiments.sh"
echo ""
