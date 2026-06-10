#!/bin/bash
# Test 3: Autoscaling under no failures
# Application 2: Filter by pattern, extract fields 1-3
#
# Requirements:
# - Run Application 2 on Dataset 2
# - Autoscaling enabled
# - TA provides PATTERN, INPUT_RATE, LW, HW during demo
# - Autoscaling decisions within 5 seconds of threshold breach
# - Check for adjustments in task count for stage 2 (transform)
#
# Usage: ./demo_test3.sh [PATTERN] [INPUT_RATE] [LW] [HW]

# Default parameters (OPTIMIZED for clear autoscaling demonstration)
# Using SIGN_REGULATORY (38.6% match rate) + tuned thresholds for guaranteed upscale event
PATTERN="${1:-SIGN_REGULATORY}"
INPUT_RATE="${2:-450}"
LW="${3:-35}"
HW="${4:-50}"

echo "🧪 Test 3: Autoscaling under no failures"
echo "Application: grep (filter) + transform (extract fields 1-3)"
echo "Dataset: dataset2.csv (9000 lines)"
echo ""
echo "Parameters:"
echo "   Pattern: $PATTERN"
echo "   Input Rate: $INPUT_RATE tuples/sec"
echo "   Low Watermark: $LW tuples/sec per task"
echo "   High Watermark: $HW tuples/sec per task"
echo ""

# Calculate expected lines for verification
EXPECTED_LINES=$(grep -c "$PATTERN" data/dataset2.csv 2>/dev/null || echo "unknown")
echo "   Expected output lines: $EXPECTED_LINES (lines containing '$PATTERN')"
echo ""
echo "💡 Autoscaling Scenario:"
echo "   - Pattern '$PATTERN' appears in ~$EXPECTED_LINES lines ($(echo "scale=1; $EXPECTED_LINES*100/9000" | bc)% of dataset)"
echo "   - Initial: 3 tasks per stage"
echo "   - Stage 1 (grep): filters to $EXPECTED_LINES tuples → $(echo "$EXPECTED_LINES/$INPUT_RATE" | bc) sec processing"
echo "   - Stage 2 (transform): receives filtered tuples"
echo ""
echo "   Expected behavior based on pattern frequency:"
if [ "$EXPECTED_LINES" -gt 2000 ]; then
    echo "   ✅ High frequency pattern → Stage 2 may UPSCALE (rate > $HW/task)"
elif [ "$EXPECTED_LINES" -lt 500 ]; then
    echo "   ✅ Low frequency pattern → Stage 2 may DOWNSCALE (rate < $LW/task)"
else
    echo "   ✅ Medium frequency pattern → Stage 2 may remain stable"
fi
echo ""

# Demo spec requires Ntasks_per_stage = 3 initially
# Application 2: grep (filter) + transform (extract fields 1-3)
# Autoscaling enabled (exactly_once disabled per spec)
echo "🚀 Submitting job with AUTOSCALING ENABLED..."
docker exec node1 ./rainstorm-cli \
    2 \
    3 \
    grep \
    --pattern="$PATTERN" \
    transform \
    dataset2.csv \
    output3.txt \
    false \
    true \
    $INPUT_RATE \
    $LW \
    $HW

echo ""
echo "✅ Job submitted with autoscaling enabled!"
echo ""
echo "📊 Monitor autoscaling events:"
echo "   Watch for '🔼 [RM] UPSCALE TRIGGERED' or '🔽 [RM] DOWNSCALE TRIGGERED' in leader logs"
echo ""
echo "📋 Monitor progress:"
echo "   # Leader logs (autoscaling decisions):"
echo "   docker exec node1 'tail -f /app/logs/rainstorm_*.log | grep -E \"(RM|UPSCALE|DOWNSCALE|Stage)\"'"
echo ""
echo "   # All logs:"
echo "   docker exec node1 'tail -f /app/logs/*.log'"
echo ""
echo "🔍 Verify autoscaling:"
echo "   1. Check leader logs for scaling events (should occur within 5 seconds of rate breach)"
echo "   2. Use 'list_tasks' command to verify task count changes in stage 2"
echo ""
echo "Suggested patterns for testing:"
echo "   - High frequency (upscale): SIGN_REGULATORY (3473 lines, 38.6%)"
echo "   - Medium frequency: SIGN_GUIDE (2141 lines, 23.8%)"
echo "   - Low frequency (downscale): SIGN_WARNING (568 lines, 6.3%)"
echo ""

