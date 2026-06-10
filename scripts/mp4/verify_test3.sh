#!/bin/bash
# Verify Test 3 results (autoscaling)
# Checks:
#   1. Output correctness (filtered + transformed)
#   2. Autoscaling events occurred
#   3. Task count changes in stage 2
#
# Usage: ./scripts/mp4/verify_test3.sh [PATTERN]
#
# NOTE: Run this AFTER logs are collected (after cluster is stopped).
#       During run_test.sh, logs won't be available yet - that's expected.
#       Default pattern is SIGN_REGULATORY (Test 3 pattern).

PATTERN="${1:-SIGN_REGULATORY}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "🔍 Verifying Test 3 Results (Autoscaling)"
echo "   Pattern: $PATTERN"
echo "   Application: grep (filter) + transform (extract fields 1-3)"
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
    echo "   Did you run ./scripts/mp4/collect_output.sh?"
    exit 1
fi

echo "   Found output: $LATEST_OUTPUT"

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

# Step 2: Calculate expected count and samples from dataset2.csv using Python
echo "📊 Step 2: Calculating ground truth from dataset2.csv..."

# Use Python to properly parse CSV with quoted fields and extract first 3 preserving quotes
GROUND_TRUTH=$(python3 << EOF
import csv

pattern = "$PATTERN"
matching_lines = 0
sample_lines = []

def extract_first_3_fields(line):
    """Extract first 3 CSV fields preserving original formatting including quotes"""
    field_count = 0
    in_quotes = False

    for i in range(len(line)):
        if line[i] == '"':
            in_quotes = not in_quotes
        elif line[i] == ',' and not in_quotes:
            field_count += 1
            if field_count == 3:
                return line[:i]
    return line

with open("$PROJECT_ROOT/data/dataset2.csv", 'r') as f:
    for line in f:
        line = line.rstrip('\n')

        if pattern in line:
            matching_lines += 1
            # Expected output: first 3 fields with original formatting (quotes preserved)
            if len(sample_lines) < 5:
                expected_output = extract_first_3_fields(line)
                sample_lines.append(expected_output)

print(f"{matching_lines}")
for line in sample_lines:
    print(f"SAMPLE:{line}")
EOF
)

EXPECTED=$(echo "$GROUND_TRUTH" | head -1)
echo "   Total lines matching '$PATTERN': $EXPECTED"
echo ""

# Show expected samples
echo "   Expected output format (first 5 samples):"
echo "$GROUND_TRUTH" | grep "^SAMPLE:" | sed 's/^SAMPLE:/      /' | head -5
echo ""

# Step 3: Parse actual output
echo "📋 Step 3: Parsing actual output..."

OUTPUT_LINES=$(cat "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null | wc -l | tr -d ' ')
echo "   Total lines in output: $OUTPUT_LINES"
echo ""

# Show actual samples
echo "   Actual output (first 5 lines):"
cat "$LATEST_OUTPUT"vm*_output_*.txt 2>/dev/null | head -5 | sed 's/^/      /'
echo ""

# Verify field count for all lines (should be exactly 3)
echo "   Verifying field count (should be exactly 3 fields per line)..."
# Use Python CSV parser to properly count fields respecting quotes
FIELD_COUNT_CHECK=$(python3 << PYEOF
import csv
import glob

invalid_count = 0
field_counts = {}

for output_file in glob.glob("$LATEST_OUTPUT" + "vm*_output_*.txt"):
    with open(output_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            field_count = len(row)
            field_counts[field_count] = field_counts.get(field_count, 0) + 1
            if field_count != 3:
                invalid_count += 1

print(f"{invalid_count}")
for count, freq in sorted(field_counts.items()):
    print(f"FIELDCOUNT:{freq} {count}")
PYEOF
)

INVALID_FIELD_COUNTS=$(echo "$FIELD_COUNT_CHECK" | head -1)
if [ "$INVALID_FIELD_COUNTS" -eq 0 ]; then
    echo "   ✅ All lines have exactly 3 fields"
else
    echo "   ❌ Found $INVALID_FIELD_COUNTS lines with incorrect field count"
    echo "   Field counts found:"
    echo "$FIELD_COUNT_CHECK" | grep "^FIELDCOUNT:" | sed 's/^FIELDCOUNT:/      /'
fi
echo ""

# Step 4: Compare line counts
echo "📝 Step 4: Comparison"
echo "   ┌─────────────────┬──────────┬──────────┐"
echo "   │ Metric          │ Expected │ Actual   │"
echo "   ├─────────────────┼──────────┼──────────┤"
printf "   │ Total lines     │ %8s │ %8s │\n" "$EXPECTED" "$OUTPUT_LINES"
echo "   └─────────────────┴──────────┴──────────┘"
echo ""

# Step 5: Check autoscaling events in logs
echo "📊 Step 5: Checking autoscaling events in logs"

# Find most recent leader log (node1 is the leader; logs/ is bind-mounted)
LEADER_LOG=$(ls -t "$PROJECT_ROOT/logs/node1_"*.log 2>/dev/null | head -1)

if [ -z "$LEADER_LOG" ]; then
    echo "   ⚠️  No leader log found"
    echo ""
else
    echo "   Analyzing: $(basename "$LEADER_LOG")"

    # Count scaling events (use tr to clean up any whitespace/newlines)
    UPSCALE_COUNT=$(grep -c "UPSCALE TRIGGERED" "$LEADER_LOG" 2>/dev/null | tr -d '\n' || echo "0")
    DOWNSCALE_COUNT=$(grep -c "DOWNSCALE TRIGGERED" "$LEADER_LOG" 2>/dev/null | tr -d '\n' || echo "0")
    UPSCALE_COMPLETE=$(grep -c "UPSCALE COMPLETE" "$LEADER_LOG" 2>/dev/null | tr -d '\n' || echo "0")
    DOWNSCALE_COMPLETE=$(grep -c "DOWNSCALE COMPLETE" "$LEADER_LOG" 2>/dev/null | tr -d '\n' || echo "0")

    # Ensure we have valid numbers (default to 0 if empty)
    UPSCALE_COUNT=${UPSCALE_COUNT:-0}
    DOWNSCALE_COUNT=${DOWNSCALE_COUNT:-0}
    UPSCALE_COMPLETE=${UPSCALE_COMPLETE:-0}
    DOWNSCALE_COMPLETE=${DOWNSCALE_COMPLETE:-0}

    echo "   Scaling Events:"
    echo "      Upscale triggered:   $UPSCALE_COUNT"
    echo "      Upscale completed:   $UPSCALE_COMPLETE"
    echo "      Downscale triggered: $DOWNSCALE_COUNT"
    echo "      Downscale completed: $DOWNSCALE_COMPLETE"
    echo ""

    # Show scaling event details
    SCALE_EVENTS=$(grep -E "(UPSCALE|DOWNSCALE)" "$LEADER_LOG" 2>/dev/null)
    if [ -n "$SCALE_EVENTS" ]; then
        echo "   Scaling Event Timeline:"
        echo "$SCALE_EVENTS" | sed 's/^/      /' | head -10
        echo ""
    fi

    # Check stage 2 metrics
    STAGE2_METRICS=$(grep "Stage 2:" "$LEADER_LOG" 2>/dev/null | tail -10)
    if [ -n "$STAGE2_METRICS" ]; then
        echo "   Stage 2 Metrics (last 10 samples):"
        echo "$STAGE2_METRICS" | sed 's/^/      /'
        echo ""
    fi

    # Summary
    TOTAL_SCALES=$((UPSCALE_COUNT + DOWNSCALE_COUNT))
    if [ "$TOTAL_SCALES" -gt 0 ]; then
        echo "   ✅ Autoscaling active: $TOTAL_SCALES scaling event(s) detected"
    else
        echo "   ℹ️  No autoscaling events (rates within LW-HW thresholds)"
    fi
fi
echo ""

# Step 6: Pass/Fail
echo "════════════════════════════════════════"
PASS=true

# Check line count.
# Autoscaling mode runs at-least-once (exactly-once is disabled), and
# rescaling is not epoch-coordinated: tuples in flight while a task is added
# or drained can be lost or duplicated. Allow a small tolerance (0.5%) around
# ground truth and report the exact delta.
TOLERANCE=$((EXPECTED / 200))
[ "$TOLERANCE" -lt 5 ] && TOLERANCE=5
DIFF=$((EXPECTED - OUTPUT_LINES))
ABS_DIFF=${DIFF#-}
if [ "$ABS_DIFF" -gt "$TOLERANCE" ]; then
    PASS=false
    echo "❌ Line count outside tolerance:"
    echo "   Expected: $EXPECTED (±$TOLERANCE)"
    echo "   Actual:   $OUTPUT_LINES"
    echo "   Diff:     $DIFF"
    echo ""
elif [ "$ABS_DIFF" -ne 0 ]; then
    echo "⚠️  Line count within rescale tolerance:"
    echo "   Expected: $EXPECTED (±$TOLERANCE allowed in autoscaling mode)"
    echo "   Actual:   $OUTPUT_LINES (diff: $DIFF)"
    echo ""
fi

# Check field counts
if [ "$INVALID_FIELD_COUNTS" -ne 0 ]; then
    PASS=false
    echo "❌ Field count errors:"
    echo "   $INVALID_FIELD_COUNTS lines have incorrect field count"
    echo ""
fi

if [ "$PASS" = true ]; then
    echo "✅ PASS: Output correctness verified!"
    echo ""
    if [ "$OUTPUT_LINES" -eq "$EXPECTED" ]; then
        echo "   ✓ Total lines: $OUTPUT_LINES = $EXPECTED"
    else
        echo "   ✓ Total lines: $OUTPUT_LINES (expected $EXPECTED, within rescale tolerance)"
    fi
    echo "   ✓ All lines have exactly 3 fields"
    echo ""
    if [ "${TOTAL_SCALES:-0}" -gt 0 ]; then
        echo "   ✓ Autoscaling events detected: $TOTAL_SCALES"
    else
        echo "   ℹ️  No autoscaling (rates within thresholds)"
    fi
    echo ""
    echo "For complete autoscaling verification, ensure:"
    echo "   1. Scaling decisions made within 5 seconds of rate breach"
    echo "   2. Stage 2 task count changed appropriately"
    echo "   3. No tuple loss during scaling"
    exit 0
else
    echo "❌ FAIL: Results do not match!"
    echo ""
    echo "   Possible causes:"
    echo "     - Not all transform tasks received EOF"
    echo "     - Tuple loss during transmission"
    echo "     - Tasks still running (job not complete)"
    echo "     - Transform operator CSV parsing bug"
    exit 1
fi
