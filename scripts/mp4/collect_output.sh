#!/bin/bash
# Collect RainStorm output files from all VMs to local rainstorm_outputs folder
# Usage: ./collect_output.sh [timestamp]
#   timestamp: Optional - specific job timestamp folder to collect (e.g., 20251203_143500)
#              If not provided, collects all output folders

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TIMESTAMP="${1:-}"

echo "📦 RainStorm Output Collection Script"
echo "======================================"

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

LOCAL_OUTPUT_DIR="$PROJECT_ROOT/rainstorm_outputs"

# Ensure local output directory exists
mkdir -p "$LOCAL_OUTPUT_DIR"

echo "📂 Collecting outputs to: $LOCAL_OUTPUT_DIR"
echo ""

for vm in "${VMS[@]}"; do
    VM_NUM=$(echo "$vm" | grep -oE '61[0-9]{2}' | sed 's/61//')
    VM_SHORT="vm${VM_NUM}"

    echo -n "   $VM_SHORT: "

    # Check if VM has any output
    if [ -n "$TIMESTAMP" ]; then
        # Collect specific timestamp folder
        REMOTE_PATH="rainstorm_outputs/${TIMESTAMP}"
    else
        # Collect all timestamp folders
        REMOTE_PATH="rainstorm_outputs"
    fi

    # Check if remote has any output directories (now in /app/rainstorm_outputs/)
    HAS_OUTPUT=$(ssh -q -o ConnectTimeout=5 ${NETID}@${vm} "ls -d /app/${REMOTE_PATH}/*/ 2>/dev/null || ls /app/${REMOTE_PATH}/*.txt 2>/dev/null || ls /app/${REMOTE_PATH}/*.log 2>/dev/null" 2>/dev/null || echo "")

    if [ -z "$HAS_OUTPUT" ]; then
        echo "no output"
        continue
    fi

    # Collect all timestamp directories from this VM (now in /app/rainstorm_outputs/)
    TIMESTAMPS=$(ssh -q -o ConnectTimeout=5 ${NETID}@${vm} "ls -1 /app/rainstorm_outputs/ 2>/dev/null" 2>/dev/null || echo "")

    if [ -z "$TIMESTAMPS" ]; then
        echo "no output"
        continue
    fi

    COLLECTED=0
    for ts in $TIMESTAMPS; do
        # Skip if looking for specific timestamp and this isn't it
        if [ -n "$TIMESTAMP" ] && [ "$ts" != "$TIMESTAMP" ]; then
            continue
        fi

        # Create local directory
        mkdir -p "$LOCAL_OUTPUT_DIR/${ts}"

        # Get list of files in this timestamp directory
        FILES=$(ssh -q -o ConnectTimeout=5 ${NETID}@${vm} "ls /app/rainstorm_outputs/${ts}/ 2>/dev/null" 2>/dev/null || echo "")

        for filename in $FILES; do
            # Copy file with VM prefix
            scp -q -o ConnectTimeout=5 "${NETID}@${vm}:/app/rainstorm_outputs/${ts}/${filename}" \
                "$LOCAL_OUTPUT_DIR/${ts}/${VM_SHORT}_${filename}" 2>/dev/null && ((COLLECTED++)) || true
        done
    done

    if [ $COLLECTED -gt 0 ]; then
        echo "✅ $COLLECTED file(s)"
    else
        echo "no output"
    fi
done

echo ""

# List what was collected
if [ -d "$LOCAL_OUTPUT_DIR" ]; then
    echo "📋 Collected outputs:"
    find "$LOCAL_OUTPUT_DIR" -type f ! -name '.gitkeep' -exec ls -lh {} \; 2>/dev/null | head -20

    TOTAL_FILES=$(find "$LOCAL_OUTPUT_DIR" -type f ! -name '.gitkeep' 2>/dev/null | wc -l | tr -d ' ')
    echo ""
    echo "   Total files: $TOTAL_FILES"

    # List timestamp directories
    echo ""
    echo "📁 Output directories:"
    ls -d "$LOCAL_OUTPUT_DIR"/*/ 2>/dev/null | while read dir; do
        dirname=$(basename "$dir")
        count=$(find "$dir" -type f 2>/dev/null | wc -l | tr -d ' ')
        echo "   $dirname/ ($count files)"
    done
fi

echo ""
echo "✅ Output collection complete!"
