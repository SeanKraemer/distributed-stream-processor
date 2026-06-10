#!/bin/bash
# Orchestrate an end-to-end RainStorm test against the local Docker cluster.
#
# Usage:
#   ./scripts/mp4/run_test.sh 0                           # Test 0: identity pass-through
#   ./scripts/mp4/run_test.sh 1 [PATTERN] [COLUMN]        # Test 1: filter & count
#   ./scripts/mp4/run_test.sh 2 [PATTERN] [COLUMN]        # Test 2: exactly-once under task failure
#   ./scripts/mp4/run_test.sh 3 [PATTERN] [RATE] [LW] [HW]  # Test 3: autoscaling
#
# Flow:
#   1. Reset the cluster (wipes HyDFS volumes — exactly-once state is keyed by
#      task ID and would collide with a previous run)
#   2. Clean local logs/ and rainstorm_outputs/
#   3. Upload test datasets to HyDFS
#   4. Run the requested demo test
#   5. Wait for the leader to log RUN END
#   6. Collect outputs from all containers
#   7. Run the matching verify script

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TEST_NUM="${1:-1}"
START_TIME=$(date +%s)
WAIT_TIMEOUT=240

echo "RainStorm Test Orchestrator — Test $TEST_NUM"
echo "============================================"
echo ""

# ── Step 1: reset the cluster ────────────────────────────────────────────────
echo "Step 1: Resetting cluster (clean HyDFS state)..."
cd "$PROJECT_ROOT"
docker compose down -v >/dev/null 2>&1
docker compose up -d >/dev/null 2>&1
echo "   Waiting 10s for SWIM membership to converge..."
sleep 10

# ── Step 2: clean local artifacts ────────────────────────────────────────────
echo ""
echo "Step 2: Cleaning local logs and outputs..."
"$PROJECT_ROOT/scripts/common/clean_logs.sh"
"$PROJECT_ROOT/scripts/mp4/clean_output.sh" local

# ── Step 3: upload test data ─────────────────────────────────────────────────
echo ""
echo "Step 3: Uploading test data..."
"$PROJECT_ROOT/scripts/mp4/upload_test_data.sh"

# ── Step 4: run the demo test ────────────────────────────────────────────────
echo ""
echo "Step 4: Running demo_test${TEST_NUM}..."
# Trailing Z required — without it docker reads the timestamp as local time.
SUBMIT_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

TEST_SCRIPT="$PROJECT_ROOT/scripts/mp4/demo_test${TEST_NUM}.sh"
if [ ! -f "$TEST_SCRIPT" ]; then
    echo "ERROR: test script not found: $TEST_SCRIPT"
    exit 1
fi

case "$TEST_NUM" in
    1|2)
        PATTERN="${2:-STOP}"
        COLUMN="${3:-4}"
        "$TEST_SCRIPT" "$PATTERN" "$COLUMN"
        ;;
    3)
        PATTERN="${2:-SIGN_REGULATORY}"
        INPUT_RATE="${3:-450}"
        LW="${4:-35}"
        HW="${5:-50}"
        "$TEST_SCRIPT" "$PATTERN" "$INPUT_RATE" "$LW" "$HW"
        ;;
    *)
        "$TEST_SCRIPT"
        ;;
esac

# ── Step 5: wait for job completion ──────────────────────────────────────────
echo ""
echo "Step 5: Waiting for the job to finish (leader logs 'RUN END')..."
elapsed=0
until docker logs node1 --since "$SUBMIT_TS" 2>&1 | grep -q "RUN END"; do
    if [ "$elapsed" -ge "$WAIT_TIMEOUT" ]; then
        echo "ERROR: job did not finish within ${WAIT_TIMEOUT}s. Check logs: make logs"
        exit 1
    fi
    sleep 5
    elapsed=$((elapsed + 5))
done
echo "   Job finished after ~${elapsed}s."

# ── Step 6: collect outputs ──────────────────────────────────────────────────
echo ""
echo "Step 6: Collecting outputs..."
"$PROJECT_ROOT/scripts/mp4/collect_output.sh"

# ── Step 7: verify ───────────────────────────────────────────────────────────
echo ""
echo "Step 7: Verifying Test $TEST_NUM results..."
case "$TEST_NUM" in
    1)
        "$PROJECT_ROOT/scripts/mp4/verify_test1.sh" "${2:-STOP}" "${3:-4}"
        ;;
    2)
        "$PROJECT_ROOT/scripts/mp4/verify_test2.sh" "${2:-STOP}" "${3:-4}"
        ;;
    3)
        "$PROJECT_ROOT/scripts/mp4/verify_test3.sh" "${2:-SIGN_REGULATORY}"
        ;;
    *)
        echo "   (no verify script for test $TEST_NUM — inspect rainstorm_outputs/ manually)"
        ;;
esac

ELAPSED=$(($(date +%s) - START_TIME))
echo ""
echo "Test $TEST_NUM complete in $((ELAPSED / 60))m $((ELAPSED % 60))s."
