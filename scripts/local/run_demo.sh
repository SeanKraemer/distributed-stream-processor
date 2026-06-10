#!/bin/bash
# Run the end-to-end RainStorm demo against the local Docker cluster.
#
# Demonstrates a 2-stage streaming pipeline with exactly-once semantics:
#   Stage 1: grep  — filter tuples matching a pattern
#   Stage 2: count — aggregate by key (CSV column)
#
# Usage: ./scripts/local/run_demo.sh [PATTERN] [COLUMN]
#   PATTERN  filter pattern (default: STOP — matches 34 rows in dataset1.csv)
#   COLUMN   1-indexed CSV column used as the aggregation key (default: 4, the sign message)
#
# Prerequisites: cluster must be running (make up)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PATTERN="${1:-STOP}"
COLUMN="${2:-4}"
INPUT_FILE="dataset1.csv"
OUTPUT_FILE="demo_output.txt"
TASKS_PER_STAGE=3
INPUT_RATE=100
WAIT_TIMEOUT=120

echo "========================================================"
echo "  RainStorm Demo — Filter + Count Pipeline"
echo "========================================================"
echo "  Input:        $INPUT_FILE"
echo "  Filter:       rows matching '$PATTERN'"
echo "  Group by:     CSV column $COLUMN"
echo "  Tasks/stage:  $TASKS_PER_STAGE"
echo "  Exactly-once: true"
echo "========================================================"
echo ""

# ── Preflight ────────────────────────────────────────────────────────────────

if ! docker ps --filter "name=node1" --filter "status=running" -q | grep -q .; then
    echo "ERROR: node1 is not running. Start the cluster first:  make up"
    exit 1
fi

# Exactly-once state files are keyed by task ID and persist in the HyDFS
# volumes. A second job submission would recover the previous run's
# processed-tuple IDs and reject everything as duplicates, producing empty
# output. Require a clean cluster instead of confusing the user.
for n in $(seq 1 10); do
    if docker exec "node$n" sh -c 'ls /app/hydfs_storage 2>/dev/null | grep -qE "task-"'; then
        echo "ERROR: HyDFS contains state from a previous job (exactly-once dedup"
        echo "       would suppress all output on a re-run)."
        echo ""
        echo "Reset the cluster first:  make reset"
        exit 1
    fi
done

# ── Step 1: upload input ─────────────────────────────────────────────────────

echo "Step 1: Uploading $INPUT_FILE to HyDFS..."
docker exec node1 sh -c "echo 'create /app/data/$INPUT_FILE $INPUT_FILE' | nc -w 5 localhost 8003"

# ── Step 2: submit the job ───────────────────────────────────────────────────

echo ""
echo "Step 2: Submitting RainStorm job..."
# CLI args: <Nstages> <Ntasks_per_stage> <op1> [op1 args] <op2> [op2 args]
#           <hydfs_src> <hydfs_dest> <exactly_once> <autoscale> <input_rate> <lw> <hw>
docker exec node1 ./rainstorm-cli \
    2 "$TASKS_PER_STAGE" \
    grep --pattern="$PATTERN" --column="$COLUMN" \
    count \
    "$INPUT_FILE" "$OUTPUT_FILE" \
    true false "$INPUT_RATE" 10 50

# ── Step 3: wait for completion ──────────────────────────────────────────────

echo ""
echo "Step 3: Waiting for the job to finish (leader logs 'RUN END')..."
# The trailing Z matters: without it, docker interprets the timestamp as
# host-local time and --since silently filters out everything.
SUBMIT_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
elapsed=0
until docker logs node1 --since "$SUBMIT_TS" 2>&1 | grep -q "RUN END"; do
    if [ "$elapsed" -ge "$WAIT_TIMEOUT" ]; then
        echo "ERROR: job did not finish within ${WAIT_TIMEOUT}s. Check logs: make logs"
        exit 1
    fi
    sleep 3
    elapsed=$((elapsed + 3))
done
echo "Job finished after ~${elapsed}s."

# ── Step 4: collect results ──────────────────────────────────────────────────

echo ""
echo "Step 4: Collecting results from sink tasks..."
echo ""
RESULTS=""
for n in $(seq 1 10); do
    NODE_OUT=$(docker exec "node$n" sh -c \
        'for f in /app/rainstorm_outputs/*/output_*.txt; do [ -s "$f" ] && cat "$f"; done' 2>/dev/null || true)
    [ -n "$NODE_OUT" ] && RESULTS="${RESULTS}${NODE_OUT}"$'\n'
done

RESULTS=$(printf '%s' "$RESULTS" | sed '/^$/d')
if [ -z "$RESULTS" ]; then
    echo "WARNING: no output produced. Does '$PATTERN' appear in data/$INPUT_FILE?"
    exit 1
fi

echo "  count  key"
echo "  -----  ---"
printf '%s\n' "$RESULTS" | awk -F',' '{count=$NF; NF--; printf "  %5d  %s\n", count, $0}' OFS=',' | sort -rn

TOTAL=$(printf '%s\n' "$RESULTS" | awk -F',' '{sum += $NF} END {print sum}')
UNIQUE=$(printf '%s\n' "$RESULTS" | wc -l | tr -d ' ')
echo ""
echo "  Total matches: $TOTAL across $UNIQUE unique keys"

if [ "$PATTERN" = "STOP" ] && [ "$COLUMN" = "4" ]; then
    if [ "$TOTAL" -eq 34 ] && [ "$UNIQUE" -eq 14 ]; then
        echo "  PASS: matches ground truth (34 rows, 14 unique sign messages)"
    else
        echo "  FAIL: expected 34 rows / 14 unique keys for pattern STOP, column 4"
        exit 1
    fi
fi

echo ""
echo "Demo complete. To re-run:  make reset && make demo"
