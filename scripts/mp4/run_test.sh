#!/bin/bash
# Master orchestration script for RainStorm testing
#
# Usage:
#   ./scripts/mp4/run_test.sh 0                          # Test 0: Identity
#   ./scripts/mp4/run_test.sh 1 [PATTERN] [COLUMN]       # Test 1: Filter & Count
#   ./scripts/mp4/run_test.sh 2                          # Test 2: Exactly-once with failure
#   ./scripts/mp4/run_test.sh 3 [PATTERN] [RATE] [LW] [HW]  # Test 3: Autoscaling
#
# Examples:
#   ./scripts/mp4/run_test.sh 0                    # Run Test 0 (identity on dataset1)
#   ./scripts/mp4/run_test.sh 1                    # Run Test 1 with defaults (STOP, column 4)
#   ./scripts/mp4/run_test.sh 1 YIELD 4            # Run Test 1 with YIELD pattern
#   ./scripts/mp4/run_test.sh 2                    # Run Test 2 (exactly-once + task kill)
#   ./scripts/mp4/run_test.sh 3                    # Run Test 3 with defaults (YIELD, 100, 50, 150)
#   ./scripts/mp4/run_test.sh 3 STOP 200 30 80    # Run Test 3 with custom params
#
# This script:
# 1. Pre-cluster: Clean logs, storage, and output
# 2. Start the cluster
# 3. Upload test data
# 4. Run the specified demo test
# 5. Post-cluster: Kill tmux and collect logs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TEST_NUM="${1:-0}"

# Track total runtime
START_TIME=$(date +%s)

# Auto-capture output to demo_test_terminal_output.txt
# This runs when script is NOT already being piped through tee
if [ -z "$RUN_TEST_CAPTURING" ]; then
    export RUN_TEST_CAPTURING=1
    OUTPUT_FILE="$PROJECT_ROOT/demo_test_terminal_output.txt"
    exec > >(tee "$OUTPUT_FILE") 2>&1
    echo "📝 Output being captured to: demo_test_terminal_output.txt"
    echo ""
fi

echo "🚀 RainStorm Test Orchestrator"
echo "==============================="
echo "Test: demo_test${TEST_NUM}"
echo "Project: $PROJECT_ROOT"
echo ""

# Function to cleanup on exit (collect logs)
cleanup() {
    echo ""
    echo "🧹 Post-test cleanup..."

    # Kill tmux session if it exists
    echo "   Stopping cluster..."
    tmux kill-session -t rainstorm 2>/dev/null || true

    # Wait a moment for processes to terminate
    sleep 2

    # Collect logs (skip if already collected for Test 3)
    if [ "${LOGS_COLLECTED:-0}" != "1" ]; then
        echo "   Collecting logs..."
        "$PROJECT_ROOT/scripts/common/collect_logs.sh"
    else
        echo "   Logs already collected during test."
    fi

    # Outputs were already collected in Step 6
    echo "   Outputs already collected during test."

    # Archive to test-specific directory for preservation
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    ARCHIVE_BASE="$PROJECT_ROOT/demo_test_${TEST_NUM}_artifacts"
    mkdir -p "$ARCHIVE_BASE/logs"
    mkdir -p "$ARCHIVE_BASE/rainstorm_outputs"

    # Archive logs
    if [ -d "$PROJECT_ROOT/logs" ] && [ "$(ls -A $PROJECT_ROOT/logs 2>/dev/null)" ]; then
        echo "   Archiving logs to demo_test_${TEST_NUM}_artifacts/logs/..."
        cp -r "$PROJECT_ROOT/logs/"* "$ARCHIVE_BASE/logs/" 2>/dev/null || true
    fi

    # Archive outputs
    if [ -d "$PROJECT_ROOT/rainstorm_outputs" ] && [ "$(ls -A $PROJECT_ROOT/rainstorm_outputs 2>/dev/null)" ]; then
        echo "   Archiving outputs to demo_test_${TEST_NUM}_artifacts/rainstorm_outputs/..."
        cp -r "$PROJECT_ROOT/rainstorm_outputs/"* "$ARCHIVE_BASE/rainstorm_outputs/" 2>/dev/null || true
    fi

    echo "   ✅ Logs and outputs archived for Test ${TEST_NUM}"

    echo ""
    echo "✅ Cleanup complete!"
    echo "   Logs saved to: $PROJECT_ROOT/logs/"
    echo "   Outputs saved to: $PROJECT_ROOT/rainstorm_outputs/"
    echo "   Archived to: $ARCHIVE_BASE/"

    # Final runtime summary (including cleanup time)
    if [ -n "$START_TIME" ]; then
        FINAL_END=$(date +%s)
        FINAL_ELAPSED=$((FINAL_END - START_TIME))
        FINAL_MINUTES=$((FINAL_ELAPSED / 60))
        FINAL_SECONDS=$((FINAL_ELAPSED % 60))
        echo ""
        echo "⏱️  Total runtime (including cleanup): ${FINAL_MINUTES}m ${FINAL_SECONDS}s"
    fi
}

# Set trap to run cleanup on exit (including Ctrl+C)
trap cleanup EXIT

# Step 1: Pre-cluster cleanup (run concurrently for speed)
echo "📋 Step 1: Pre-cluster cleanup (concurrent)"
echo "--------------------------------------------"

echo "   Starting cleanup tasks in parallel..."
"$PROJECT_ROOT/scripts/common/clean_logs.sh" all &
PID_LOGS=$!
"$PROJECT_ROOT/scripts/mp3/clean_storage.sh" &
PID_STORAGE=$!
"$PROJECT_ROOT/scripts/mp4/clean_output.sh" &
PID_OUTPUT=$!

# Wait for all cleanup tasks to complete
echo "   Waiting for cleanup to complete..."
wait $PID_LOGS $PID_STORAGE $PID_OUTPUT
echo "   ✅ All cleanup tasks complete"

echo ""

# Step 2: Start the cluster
echo "📋 Step 2: Starting cluster"
echo "----------------------------"

# For Test 3, use the autoscaling commit; otherwise use current branch
if [ "$TEST_NUM" = "3" ]; then
    echo "   🔄 Test 3: Switching VMs to autoscaling commit (4f733db9)"
    "$PROJECT_ROOT/scripts/common/start_cluster.sh" "4f733db9"
else
    "$PROJECT_ROOT/scripts/common/start_cluster.sh"
fi

# Wait for cluster to stabilize (VMs need time to build, start, and join)
echo "   Waiting 25 seconds for cluster to fully stabilize..."
for i in {25..1}; do
    echo -ne "   ⏳ $i seconds remaining...\r"
    sleep 1
done
echo "   ✅ Cluster should be ready                    "

echo ""

# Step 3: Upload test data
echo "📋 Step 3: Uploading test data"
echo "-------------------------------"
"$PROJECT_ROOT/scripts/mp4/upload_test_data.sh"

# Wait for replication to complete across all nodes
echo "   Waiting 10 seconds for replication to complete..."
for i in {10..1}; do
    echo -ne "   ⏳ $i seconds remaining...\r"
    sleep 1
done
echo "   ✅ Data should be replicated                  "

echo ""

# Step 4: Run the demo test
echo "📋 Step 4: Running demo_test${TEST_NUM}"
echo "----------------------------------------"

# Special handling for Test 3: Use working Test 3 commit (4f733db9)
if [ "$TEST_NUM" = "3" ]; then
    echo "🔄 Test 3: Switching to working commit 4f733db9..."

    # Save current branch/commit
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
    CURRENT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "")

    # Checkout working Test 3 commit
    cd "$PROJECT_ROOT"
    git checkout 4f733db9 2>&1 | head -5

    echo "✅ Switched to Test 3 working commit"
    echo ""
fi

# Handle Test 2 specially - it uses demo_test2.sh
if [ "$TEST_NUM" = "2" ]; then
    TEST_SCRIPT="$PROJECT_ROOT/scripts/mp4/demo_test2.sh"
else
    TEST_SCRIPT="$PROJECT_ROOT/scripts/mp4/demo_test${TEST_NUM}.sh"
fi

if [ -f "$TEST_SCRIPT" ]; then
    case "$TEST_NUM" in
        1)
            # Test 1: grep+count with pattern and column
            PATTERN="${2:-STOP}"
            COLUMN="${3:-4}"
            echo "   Pattern: $PATTERN, Column: $COLUMN"
            "$TEST_SCRIPT" "$PATTERN" "$COLUMN"
            ;;
        2)
            # Test 2: Exactly-once with failure (same as Test 1 but with task kill)
            # First run Test 1 as baseline if not already done
            PATTERN="${2:-STOP}"
            COLUMN="${3:-4}"
            echo "   Test 2: Exactly-once semantics under failure"
            echo "   (This test uses Application 1 with a task kill mid-execution)"
            echo "   Pattern: $PATTERN, Column: $COLUMN"
            "$TEST_SCRIPT" "$PATTERN" "$COLUMN"
            ;;
        3)
            # Test 3: Autoscaling with grep+transform on dataset2.csv
            # Usage: ./run_test.sh 3 [PATTERN] [INPUT_RATE] [LW] [HW]
            # Defaults optimized for demonstrating autoscaling
            PATTERN="${2:-SIGN_REGULATORY}"
            INPUT_RATE="${3:-450}"
            LW="${4:-35}"
            HW="${5:-50}"
            echo "   Pattern: $PATTERN"
            echo "   Input Rate: $INPUT_RATE, Low Watermark: $LW, High Watermark: $HW"
            "$TEST_SCRIPT" "$PATTERN" "$INPUT_RATE" "$LW" "$HW"
            ;;
        *)
            "$TEST_SCRIPT"
            ;;
    esac
else
    echo "❌ Test script not found: $TEST_SCRIPT"
    exit 1
fi

# Restore original branch/commit after Test 3
if [ "$TEST_NUM" = "3" ]; then
    echo ""
    echo "🔄 Restoring to main branch..."
    cd "$PROJECT_ROOT"
    # Always return to main branch after Test 3
    git checkout main 2>&1 | head -5
    git pull origin main 2>&1 | head -5
    echo "✅ Restored to: main"
    echo ""
fi

# Wait for test to complete - different tests have different durations
echo ""
case "$TEST_NUM" in
    0|1)
        WAIT_TIME=45
        ;;
    2)
        # Test 2 involves failure recovery, give it more time
        WAIT_TIME=75
        ;;
    3)
        # Test 3 with dataset2.csv (9000 lines at 450 tuples/sec = ~20 sec + processing buffer)
        WAIT_TIME=75
        ;;
    *)
        WAIT_TIME=60
        ;;
esac
echo "   Waiting $WAIT_TIME seconds for test to complete..."
sleep $WAIT_TIME

# Check output files
echo ""
echo "📋 Step 5: Checking output files"
echo "---------------------------------"
for vm in 01 02 03 04 05 06 07 08 09 10; do
    result=$(docker exec node${vm} "ls -laR /app/rainstorm_outputs/ 2>/dev/null" 2>/dev/null || echo "No files")
    if [ "$result" != "No files" ]; then
        echo "=== VM$vm ==="
        echo "$result"
        echo ""
    fi
done

# Step 6: Collect outputs from VMs
echo ""
echo "📋 Step 6: Collecting outputs from VMs"
echo "---------------------------------------"
"$PROJECT_ROOT/scripts/mp4/collect_output.sh"

# Step 7: Verify HyDFS output (applies to all tests)
echo ""
echo "📋 Step 7: Verifying HyDFS output"
echo "----------------------------------"

# Determine the HyDFS destination filename based on test
case "$TEST_NUM" in
    0) HYDFS_DEST="output0.txt" ;;
    1) HYDFS_DEST="output1.txt" ;;
    2) HYDFS_DEST="output2.txt" ;;
    3) HYDFS_DEST="output3.txt" ;;
    *) HYDFS_DEST="output.txt" ;;
esac

echo "   HyDFS destination file: $HYDFS_DEST"

# First, run merge on the output file to ensure all replicas are consistent
echo "   Running merge on HyDFS file..."
docker exec node1 \
    "cd /app && echo 'merge $HYDFS_DEST' | nc localhost 8003" 2>/dev/null || true
sleep 2

# Get the file from HyDFS to hydfs_local (standard MP3 behavior)
echo "   Fetching HyDFS output file..."
HYDFS_LOCAL="$PROJECT_ROOT/rainstorm_outputs/hydfs_${HYDFS_DEST}"
# Use relative path so GET deposits to hydfs_local/<filename>
# Then cat from hydfs_local to our local collection folder
docker exec node1 \
    "cd /app && echo 'get $HYDFS_DEST $HYDFS_DEST' | nc localhost 8003 > /dev/null && cat hydfs_local/$HYDFS_DEST" 2>/dev/null > "$HYDFS_LOCAL" || true

if [ -s "$HYDFS_LOCAL" ]; then
    HYDFS_LINES=$(wc -l < "$HYDFS_LOCAL" | tr -d ' ')
    echo "   ✅ HyDFS output file has $HYDFS_LINES lines"
    echo "   First 5 lines:"
    head -5 "$HYDFS_LOCAL" | sed 's/^/      /'
else
    echo "   ⚠️  HyDFS output file is empty or not found"
    echo "   (This may be OK if HyDFS append is still propagating)"
fi

# Step 8: Test-specific verification
echo ""
echo "📋 Step 8: Verifying Test $TEST_NUM results"
echo "--------------------------------------------"

# For Test 3, we need to collect logs BEFORE verification to check autoscaling events
# Other tests don't need logs for their core verification
if [ "$TEST_NUM" = "3" ]; then
    echo "   (Collecting logs first for autoscaling verification...)"
    "$PROJECT_ROOT/scripts/common/collect_logs.sh"
    LOGS_COLLECTED=1
    echo ""
fi

case "$TEST_NUM" in
    0)
        # Test 0: Identity - compare line counts
        SRC_LINES=$(docker exec node1 "wc -l < /app/data/dataset1.csv" 2>/dev/null | tr -d ' ')
        echo "   Source file (dataset1.csv): $SRC_LINES lines"

        # Find the output directory that contains actual output files
        LATEST_OUTPUT=""
        for dir in $(ls -td "$PROJECT_ROOT/rainstorm_outputs"/*/ 2>/dev/null); do
            if ls "$dir"vm*_output_*.txt >/dev/null 2>&1; then
                LATEST_OUTPUT="$dir"
                break
            fi
        done

        if [ -n "$LATEST_OUTPUT" ]; then
            LOCAL_LINES=$(cat "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null | wc -l | tr -d ' ')
            echo "   Local output files: $LOCAL_LINES lines"

            if [ "$LOCAL_LINES" -eq "$SRC_LINES" ]; then
                echo "   ✅ PASS: Output line count matches source!"
            else
                echo "   ⚠️  Line count mismatch (expected $SRC_LINES, got $LOCAL_LINES)"
            fi
        fi

        if [ -s "$HYDFS_LOCAL" ]; then
            echo "   HyDFS output: $HYDFS_LINES lines"
            if [ "$HYDFS_LINES" -eq "$SRC_LINES" ]; then
                echo "   ✅ PASS: HyDFS output matches source!"
            else
                echo "   ⚠️  HyDFS line count mismatch"
            fi
        fi
        ;;
    1)
        # Test 1: grep+count
        PATTERN="${2:-STOP}"
        COLUMN="${3:-4}"
        "$PROJECT_ROOT/scripts/mp4/verify_test1.sh" "$PATTERN" "$COLUMN"
        ;;
    2)
        # Test 2: Exactly-once under failure - compare against Test 1 baseline
        "$PROJECT_ROOT/scripts/mp4/verify_test2.sh"
        ;;
    3)
        # Test 3: Autoscaling with transform
        PATTERN="${2:-SIGN_REGULATORY}"
        "$PROJECT_ROOT/scripts/mp4/verify_test3.sh" "$PATTERN"
        ;;
esac

echo ""
echo "🎉 Test $TEST_NUM complete!"

# Calculate and display total runtime
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo "⏱️  Total runtime: ${MINUTES}m ${SECONDS}s"
echo ""

# Cleanup happens automatically via trap - no need to wait for user input
