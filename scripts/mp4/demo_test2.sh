#!/bin/bash
# Test 2: Exactly-once under failures
# This is the same as Test 1 (Application 1: grep+count) but we'll kill a task mid-run
#
# Requirements:
# - Run Application 1 on Dataset 1
# - Exactly-once enabled
# - INPUT_RATE = 100 tuples/sec
# - Kill a grep task (stage1) after ~10 seconds
# - Results should match failure-free run (no duplicates, no missing)

PATTERN="${1:-STOP}"
COLUMN="${2:-4}"

echo "🧪 Test 2: Exactly-once under failures"
echo "Pattern: $PATTERN"
echo "Column: $COLUMN"
echo ""
echo "⚠️  This test runs the same job as Test 1"
echo "   A task will be killed mid-run to test exactly-once semantics"
echo ""

# Demo spec requires Ntasks_per_stage = 3
# Submit the job
go run cmd/cli/main.go \
    2 \
    3 \
    grep \
    --pattern="$PATTERN" --column=$COLUMN \
    count \
    dataset1.csv \
    output2.txt \
    true \
    false \
    100 \
    10 \
    50

echo ""
echo "✅ Job submitted!"
echo ""

# Wait for tasks to start (need longer wait - tasks take ~20 seconds to start after job submission)
echo "⏳ Waiting 25 seconds for tasks to start and begin processing..."
sleep 25

# Get task list and find a grep task to kill
echo ""
echo "📋 Getting task list..."

LEADER_HOST="node1"
RAINSTORM_PORT=8002

# Retry loop to find a grep task
MAX_RETRIES=5
RETRY_DELAY=5
GREP_TASK=""

for i in $(seq 1 $MAX_RETRIES); do
    # Get list of tasks
    TASK_LIST=$(echo '{"type":"list_tasks","sender":"client","payload":{}}' | nc -w 5 "$LEADER_HOST" "$RAINSTORM_PORT" 2>/dev/null)

    if [ -z "$TASK_LIST" ]; then
        echo "   Attempt $i/$MAX_RETRIES: Failed to connect to leader"
        if [ $i -lt $MAX_RETRIES ]; then
            sleep $RETRY_DELAY
            continue
        fi
    fi

    echo "   Attempt $i/$MAX_RETRIES - Task list: $TASK_LIST"

    # Parse the task list to find a grep task
    GREP_TASK=$(echo "$TASK_LIST" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    tasks = data.get('tasks', [])
    if tasks is None:
        tasks = []
    for t in tasks:
        op_exe = t.get('op_exe', '')
        state = t.get('state', '')
        vm = t.get('vm', '')
        pid = t.get('pid', 0)
        if 'grep' in op_exe and state == 'running' and pid > 0:
            print(f'{vm}|{pid}')
            break
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
" 2>/dev/null)

    if [ -n "$GREP_TASK" ]; then
        echo "   ✅ Found grep task!"
        break
    fi

    echo "   No running grep task found yet, retrying in ${RETRY_DELAY}s..."
    if [ $i -lt $MAX_RETRIES ]; then
        sleep $RETRY_DELAY
    fi
done

if [ -z "$GREP_TASK" ]; then
    echo "❌ Failed to find a running grep task after $MAX_RETRIES attempts"
    echo ""
    echo "📋 Manual steps for Test 2:"
    echo "   1. Use list_tasks to get task info"
    echo "   2. Kill a grep task (stage1) using kill_task"
    echo "   3. Verify task restarts and results match Test 1"
    exit 0
fi

KILL_VM=$(echo "$GREP_TASK" | cut -d'|' -f1)
KILL_PID=$(echo "$GREP_TASK" | cut -d'|' -f2)

echo ""
echo "💀 Killing grep task:"
echo "   VM: $KILL_VM"
echo "   PID: $KILL_PID"

# Send kill command
echo "{\"type\":\"kill_task\",\"sender\":\"client\",\"payload\":{\"vm\":\"$KILL_VM\",\"pid\":$KILL_PID}}" | \
    nc -w 5 "$LEADER_HOST" "$RAINSTORM_PORT" 2>/dev/null

echo ""
echo "✅ Kill command sent!"
echo "   The leader should detect the failure and restart the task"
echo ""
echo "📋 Post-test verification:"
echo "   1. Check logs for 'TASK FAILED' and 'TASK START' messages"
echo "   2. Compare output2.txt with output1.txt (Test 1 baseline)"
echo "   3. Results should be identical (exactly-once semantics)"
