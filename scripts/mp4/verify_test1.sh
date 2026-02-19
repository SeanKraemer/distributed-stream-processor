#!/bin/bash
# Verify Test 1 results
# Usage: ./scripts/mp4/verify_test1.sh [PATTERN] [COLUMN]
#
# This script:
# 1. Finds the output directory containing output files from rainstorm_outputs/
# 2. Compares against ground truth from dataset1.csv (using Python for proper CSV parsing)
# 3. Reports pass/fail

PATTERN="${1:-STOP}"
COLUMN="${2:-4}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "🔍 Verifying Test 1 Results"
echo "   Pattern: $PATTERN"
echo "   Column: $COLUMN"
echo ""

# Step 1: Find output files
echo "📥 Step 1: Finding output files..."

# Find the output directory that contains actual output files (not just logs)
# There may be multiple timestamp directories; we need the one with vm*_output_*.txt files
LATEST_OUTPUT=""
for dir in $(ls -td "$PROJECT_ROOT/rainstorm_outputs"/*/ 2>/dev/null); do
    if ls "$dir"vm*_output_*.txt >/dev/null 2>&1; then
        LATEST_OUTPUT="$dir"
        break
    fi
done

if [ -z "$LATEST_OUTPUT" ]; then
    echo "❌ No output directory with output files found in rainstorm_outputs/"
    echo "   Did you run ./scripts/mp4/collect_output.sh?"
    echo "   Available directories:"
    ls -la "$PROJECT_ROOT/rainstorm_outputs"/ 2>/dev/null | head -10
    exit 1
fi

echo "   Found output: $LATEST_OUTPUT"

# Find all output files (format: vm*_output_*.txt)
OUTPUT_FILES=$(ls "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null)
if [ -z "$OUTPUT_FILES" ]; then
    echo "❌ No output files found in $LATEST_OUTPUT"
    exit 1
fi

echo "   Found output files:"
for f in $OUTPUT_FILES; do
    echo "      - $(basename "$f")"
done
echo ""

# Step 2: Calculate ground truth using Python (handles CSV properly)
echo "📊 Step 2: Calculating ground truth from dataset1.csv..."

# Use Python to properly parse CSV with quoted fields
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

GROUND_TRUTH_TOTAL=$(echo "$GROUND_TRUTH" | cut -d' ' -f1)
GROUND_TRUTH_UNIQUE=$(echo "$GROUND_TRUTH" | cut -d' ' -f2)

echo "   Total lines matching '$PATTERN': $GROUND_TRUTH_TOTAL"
echo "   Unique values in column $COLUMN: $GROUND_TRUTH_UNIQUE"
echo ""

# Step 3: Parse actual output
# Note: Output format is "KEY,COUNT" but KEY may contain commas
# So we extract the count from the LAST comma-separated field
echo "📋 Step 3: Parsing actual output..."

ACTUAL_LINES=$(cat "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null | wc -l | tr -d ' ')

# Sum counts by extracting the last field (after final comma)
ACTUAL_TOTAL=$(cat "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null | sed 's/.*,//' | awk '{sum += $1} END {print sum}')

echo "   Unique keys in output: $ACTUAL_LINES"
echo "   Total count (sum of values): $ACTUAL_TOTAL"
echo ""

# Step 4: Compare
echo "📝 Step 4: Comparison"
echo "   ┌─────────────────┬──────────┬──────────┐"
echo "   │ Metric          │ Expected │ Actual   │"
echo "   ├─────────────────┼──────────┼──────────┤"
printf "   │ Total lines     │ %8s │ %8s │\n" "$GROUND_TRUTH_TOTAL" "$ACTUAL_TOTAL"
printf "   │ Unique keys     │ %8s │ %8s │\n" "$GROUND_TRUTH_UNIQUE" "$ACTUAL_LINES"
echo "   └─────────────────┴──────────┴──────────┘"
echo ""

# Step 5: Detailed comparison
echo "📊 Step 5: Detailed breakdown"
echo ""
echo "   Expected (from grep):"
echo "$GROUND_TRUTH" | head -10 | while read count key; do
    printf "      %3s  %s\n" "$count" "$key"
done
if [ "$GROUND_TRUTH_UNIQUE" -gt 10 ]; then
    echo "      ... and $((GROUND_TRUTH_UNIQUE - 10)) more"
fi
echo ""

echo "   Actual (from RainStorm):"
cat "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null | sort -t',' -k2 -rn | head -10 | while IFS=',' read key count; do
    printf "      %3s  %s\n" "$count" "$key"
done
if [ "$ACTUAL_LINES" -gt 10 ]; then
    echo "      ... and $((ACTUAL_LINES - 10)) more"
fi
echo ""

# Step 6: Pass/Fail
echo "════════════════════════════════════════"
if [ "$ACTUAL_TOTAL" -eq "$GROUND_TRUTH_TOTAL" ] && [ "$ACTUAL_LINES" -eq "$GROUND_TRUTH_UNIQUE" ]; then
    echo "✅ PASS: Results match expected values!"
    echo ""
    echo "   ✓ Total lines: $ACTUAL_TOTAL = $GROUND_TRUTH_TOTAL"
    echo "   ✓ Unique keys: $ACTUAL_LINES = $GROUND_TRUTH_UNIQUE"
    exit 0
else
    echo "❌ FAIL: Results do not match!"
    echo ""
    if [ "$ACTUAL_TOTAL" -ne "$GROUND_TRUTH_TOTAL" ]; then
        DIFF=$((GROUND_TRUTH_TOTAL - ACTUAL_TOTAL))
        echo "   ✗ Total lines: expected $GROUND_TRUTH_TOTAL, got $ACTUAL_TOTAL (diff: $DIFF)"
    else
        echo "   ✓ Total lines: $ACTUAL_TOTAL"
    fi
    if [ "$ACTUAL_LINES" -ne "$GROUND_TRUTH_UNIQUE" ]; then
        DIFF=$((GROUND_TRUTH_UNIQUE - ACTUAL_LINES))
        echo "   ✗ Unique keys: expected $GROUND_TRUTH_UNIQUE, got $ACTUAL_LINES (diff: $DIFF)"
    else
        echo "   ✓ Unique keys: $ACTUAL_LINES"
    fi
    echo ""
    echo "   Possible causes:"
    echo "     - Not all count tasks received EOF"
    echo "     - Tuple loss during transmission"
    echo "     - Tasks still running (job not complete)"
    exit 1
fi
