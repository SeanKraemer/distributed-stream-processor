#!/bin/bash
# Run the end-to-end RainStorm demo against the local Docker cluster.
#
# Demonstrates a 2-stage streaming pipeline:
#   Stage 1: grep — filter tuples matching a pattern
#   Stage 2: count — aggregate by key
#
# Prerequisites: cluster must be running (scripts/local/start_cluster.sh)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

INPUT_FILE="dataset1.csv"
OUTPUT_FILE="demo_output.txt"
PATTERN="SEVERE"
NODES=3          # tasks per stage
EXACTLY_ONCE=true

echo "========================================================"
echo "  RainStorm Demo — Filter + Count Pipeline"
echo "========================================================"
echo "  Input:   $INPUT_FILE"
echo "  Filter:  rows matching '$PATTERN'"
echo "  Output:  $OUTPUT_FILE (in HyDFS)"
echo "  Exactly-once: $EXACTLY_ONCE"
echo "========================================================"
echo ""

# Check cluster is up
if ! docker ps --filter "name=node1" --filter "status=running" -q | grep -q .; then
    echo "ERROR: node1 is not running. Start the cluster first:"
    echo "  ./scripts/local/start_cluster.sh"
    exit 1
fi

echo "Step 1: Uploading input dataset to HyDFS..."
docker exec node1 sh -c "echo 'create /app/data/$INPUT_FILE $INPUT_FILE' | ./rainstorm" 2>/dev/null || true

echo ""
echo "Step 2: Submitting RainStorm job..."
echo "  (The job will run and write results to HyDFS file '$OUTPUT_FILE')"
echo ""

# Submit via the CLI binary. The CLI connects to node1's client port.
docker exec node1 ./rainstorm-cli \
    --stages=2 \
    --tasks=3 \
    --op1=grep --op1-args="--pattern=$PATTERN --column=4" \
    --op2=count \
    --src="$INPUT_FILE" \
    --dest="$OUTPUT_FILE" \
    --exactly-once="$EXACTLY_ONCE" \
    --autoscale=false \
    --input-rate=100 \
    --lw=10 --hw=50 2>&1 || {
        echo ""
        echo "Note: For interactive submission, attach to node1:"
        echo "  docker attach node1"
        echo "  Then at the prompt run the RainStorm command shown in the README."
    }

echo ""
echo "Step 3: Collecting output from HyDFS..."
docker exec node1 sh -c "echo 'get $OUTPUT_FILE /tmp/$OUTPUT_FILE' | ./rainstorm" 2>/dev/null || true
docker cp "node1:/tmp/$OUTPUT_FILE" "$REPO_ROOT/$OUTPUT_FILE" 2>/dev/null && \
    echo "Output written to: $REPO_ROOT/$OUTPUT_FILE" || \
    echo "Output is in HyDFS on node1 as: $OUTPUT_FILE"

echo ""
echo "Demo complete."
