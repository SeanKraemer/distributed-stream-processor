#!/bin/bash
#
# NOTE: VM-era script. This targeted the original 10-VM deployment used for
# the RainStorm-vs-Spark benchmarking experiments and is retained to document
# that methodology. It is NOT runnable against the local Docker cluster.
#
# Deploy Spark Streaming to all VMs
#
# Usage:
#   ./scripts/spark/deploy_spark.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "🚀 Spark Deployment Script"
echo "============================"
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

echo "⚙️  Step 3: Configuring Spark cluster..."
echo "-----------------------------------------"

# Create workers file (all VMs except master)
WORKERS_FILE=$(mktemp)
for vm in "${VMS[@]:1}"; do
    echo "$vm" >> "$WORKERS_FILE"
done

# Upload workers file to master
scp -q "$WORKERS_FILE" "$MASTER_VM:/opt/spark/conf/workers"
rm "$WORKERS_FILE"

# Configure Spark environment
ssh -q "$MASTER_VM" "cat > /opt/spark/conf/spark-env.sh << 'EOF'
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

# Java configuration
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-11.0.25.0.9-3.el8.x86_64

# Logging
export SPARK_LOG_DIR=/opt/spark/logs
export SPARK_PID_DIR=/opt/spark/run

EOF
chmod +x /opt/spark/conf/spark-env.sh
"

# Copy spark-env.sh to all workers
for vm in "${VMS[@]:1}"; do
    # scp removed: "$vm:/opt/spark/conf/" &
done
wait

# Create log and run directories on all VMs
for vm in "${VMS[@]}"; do
    ssh -q "$vm" "mkdir -p /opt/spark/logs /opt/spark/run" &
done
wait

echo "   ✅ Configuration complete"
echo ""

echo "🚀 Step 4: Starting Spark cluster..."
echo "-------------------------------------"

# Start master
echo "   Starting master on $MASTER_VM..."
ssh -q "$MASTER_VM" "/opt/spark/sbin/start-master.sh"
sleep 5

# Start workers
echo "   Starting workers on all VMs..."
ssh -q "$MASTER_VM" "/opt/spark/sbin/start-workers.sh"
sleep 5

echo "   ✅ Spark cluster started"
echo ""

echo "📊 Step 5: Verifying cluster status..."
echo "---------------------------------------"

ssh -q "$MASTER_VM" "
    echo '   Master process:'
    jps | grep Master || echo '   ⚠️  Master not running'
    echo ''
    echo '   Worker processes on master:'
    jps | grep Worker || echo '   (No worker on master - OK)'
"

WORKER_COUNT=0
for vm in "${VMS[@]:1}"; do
    WORKER_RUNNING=$(ssh -q "$vm" "jps | grep -c Worker" || echo "0")
    if [ "$WORKER_RUNNING" -gt 0 ]; then
        ((WORKER_COUNT++))
    fi
done

echo "   Workers running: $WORKER_COUNT / 9"
echo ""

echo "✅ Spark Deployment Complete!"
echo ""
echo "📌 Cluster Information:"
echo "   Master URL:  spark://node1:7077"
echo "   Web UI:      http://node1:8080"
echo "   Workers:     $WORKER_COUNT / 9 active"
echo ""
echo "📌 Management Commands:"
echo "   Stop cluster:  docker exec $MASTER_VM '/opt/spark/sbin/stop-all.sh'"
echo "   Start cluster: docker exec $MASTER_VM '/opt/spark/sbin/start-all.sh'"
echo "   Check status:  docker exec $MASTER_VM 'jps'"
echo ""
echo "📌 Next Steps:"
echo "   1. Upload datasets to all VMs"
echo "   2. Run experiments with ./scripts/spark/run_spark_experiment.sh"
echo ""
