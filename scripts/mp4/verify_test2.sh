#!/bin/bash
# Verify Test 2 results (exactly-once under failures)
# Compares against Test 1 baseline to ensure no duplicates/missing
#
# Usage: ./scripts/mp4/verify_test2.sh [PATTERN] [COLUMN]

PATTERN="${1:-STOP}"
COLUMN="${2:-4}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "🔍 Verifying Test 2 Results (Exactly-once under failures)"
echo "   Pattern: $PATTERN"
echo "   Column: $COLUMN"
echo ""

# Step 1: Find output files
echo "📥 Step 1: Finding output files..."

# Find the output directory that contains actual output files (not just logs)
LATEST_OUTPUT=""
for dir in $(ls -td "$PROJECT_ROOT/rainstorm_outputs"/*/ 2>/dev/null); do
    if ls "$dir"vm*_output_*.txt >/dev/null 2>&1; then
        LATEST_OUTPUT="$dir"
        break
    fi
done

if [ -z "$LATEST_OUTPUT" ]; then
    echo "❌ No output directory with output files found in rainstorm_outputs/"
    exit 1
fi

echo "   Found output: $LATEST_OUTPUT"

# Find all output files
OUTPUT_FILES=$(ls "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null)
if [ -z "$OUTPUT_FILES" ]; then
    echo "❌ No output files found"
    exit 1
fi

echo "   Output files:"
for f in $OUTPUT_FILES; do
    echo "      - $(basename "$f")"
done
echo ""

# Step 2: Check for duplicates using sort | uniq -c
echo "📊 Step 2: Checking for duplicates..."

DUPLICATES=$(cat "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null | sort | uniq -c | awk '$1 > 1')
if [ -n "$DUPLICATES" ]; then
    echo "   ❌ DUPLICATES FOUND:"
    echo "$DUPLICATES" | sed 's/^/      /'
    echo ""
    DUPLICATE_COUNT=$(echo "$DUPLICATES" | wc -l | tr -d ' ')
    echo "   Total duplicate entries: $DUPLICATE_COUNT"
else
    echo "   ✅ No duplicates found"
fi
echo ""

# Step 3: Calculate expected values (same as Test 1)
echo "📊 Step 3: Calculating expected values..."

GROUND_TRUTH=$(python3 << EOF
import csv
from collections import Counter

pattern = "$PATTERN"
column = $COLUMN - 1  # Convert to 0-indexed

counts = Counter()
with open("$PROJECT_ROOT/data/dataset1.csv", 'r') as f:
    reader = csv.reader(f)
    next(reader)  # skip header
    for row in reader:
        line = ','.join(row)
        if pattern in line:
            key = row[column] if len(row) > column else ''
            counts[key] += 1

print(f"{sum(counts.values())} {len(counts)}")
EOF
)

EXPECTED_TOTAL=$(echo "$GROUND_TRUTH" | cut -d' ' -f1)
EXPECTED_UNIQUE=$(echo "$GROUND_TRUTH" | cut -d' ' -f2)

echo "   Expected total: $EXPECTED_TOTAL"
echo "   Expected unique keys: $EXPECTED_UNIQUE"
echo ""

# Step 4: Parse actual output
echo "📋 Step 4: Parsing actual output..."

ACTUAL_LINES=$(cat "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null | wc -l | tr -d ' ')
ACTUAL_TOTAL=$(cat "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null | sed 's/.*,//' | awk '{sum += $1} END {print sum}')

# Handle empty output case to avoid integer expression errors
ACTUAL_LINES=${ACTUAL_LINES:-0}
ACTUAL_TOTAL=${ACTUAL_TOTAL:-0}

echo "   Actual unique keys: $ACTUAL_LINES"
echo "   Actual total count: $ACTUAL_TOTAL"
echo ""

# Step 5: Compare
echo "📝 Step 5: Comparison"
echo "   ┌─────────────────┬──────────┬──────────┐"
echo "   │ Metric          │ Expected │ Actual   │"
echo "   ├─────────────────┼──────────┼──────────┤"
printf "   │ Total count     │ %8s │ %8s │\n" "$EXPECTED_TOTAL" "$ACTUAL_TOTAL"
printf "   │ Unique keys     │ %8s │ %8s │\n" "$EXPECTED_UNIQUE" "$ACTUAL_LINES"
echo "   └─────────────────┴──────────┴──────────┘"
echo ""

# Step 6: Check leader logs for task restart (SSH to VM01 since logs aren't collected yet)
echo "📋 Step 6: Checking for task restart in logs..."

# First try local logs (if already collected), then SSH to VM01
RESTART_LOGS=$(grep -iE "TASK RESTART|TASK FAILED" "$PROJECT_ROOT"/logs/vm01*.log 2>/dev/null | tail -10)
if [ -z "$RESTART_LOGS" ]; then
    # Logs not collected yet - SSH to VM01 to check the log directly
    RESTART_LOGS=$(docker exec node1 \
        "grep -iE 'TASK RESTART|TASK FAILED' /app/logs/vm01*.log 2>/dev/null | tail -10" 2>/dev/null)
fi

if [ -n "$RESTART_LOGS" ]; then
    echo "   ✅ Task restart events found:"
    echo "$RESTART_LOGS" | sed 's/^/      /'
else
    echo "   ⚠️  No restart events found in leader logs"
    echo "   (Check logs manually: grep -iE 'TASK RESTART|TASK FAILED' logs/vm01*.log)"
fi
echo ""

# Step 7: Final verdict
echo "════════════════════════════════════════"
if [ -z "$DUPLICATES" ] && [ "$ACTUAL_TOTAL" -eq "$EXPECTED_TOTAL" ] && [ "$ACTUAL_LINES" -eq "$EXPECTED_UNIQUE" ]; then
    echo "✅ PASS: Exactly-once semantics verified!"
    echo ""
    echo "   ✓ No duplicates"
    echo "   ✓ Total count: $ACTUAL_TOTAL = $EXPECTED_TOTAL"
    echo "   ✓ Unique keys: $ACTUAL_LINES = $EXPECTED_UNIQUE"
    exit 0
else
    echo "❌ FAIL: Exactly-once verification failed!"
    echo ""
    if [ -n "$DUPLICATES" ]; then
        echo "   ✗ Duplicates found"
    else
        echo "   ✓ No duplicates"
    fi
    if [ "$ACTUAL_TOTAL" -ne "$EXPECTED_TOTAL" ]; then
        echo "   ✗ Total count mismatch: expected $EXPECTED_TOTAL, got $ACTUAL_TOTAL"
    else
        echo "   ✓ Total count: $ACTUAL_TOTAL"
    fi
    if [ "$ACTUAL_LINES" -ne "$EXPECTED_UNIQUE" ]; then
        echo "   ✗ Unique keys mismatch: expected $EXPECTED_UNIQUE, got $ACTUAL_LINES"
    else
        echo "   ✓ Unique keys: $ACTUAL_LINES"
    fi
    exit 1
fi
