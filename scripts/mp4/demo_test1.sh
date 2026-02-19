#!/bin/bash
# Application 1: Filter by pattern, count by CSV column
#
# Expected results for "STOP" pattern with column 4:
#   - 34 total lines containing "STOP"
#   - 14 unique sign messages
#   - Top: STOP AHEAD (17), STOP HERE ON RED (4)

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <pattern> <column_num>"
    echo ""
    echo "Example: $0 'STOP' 4    # Column 4 = Sign Message"
    echo ""
    echo "Recommended columns for dataset1.csv:"
    echo "  Column 4 = SIGN_MESSAGE (sign text like 'STOP AHEAD')"
    echo "  Column 5 = SIGN_MUT_CD (sign code)"
    exit 1
fi

PATTERN=$1
COLUMN=$2

echo "🧪 Application 1: Filter & Count"
echo "Pattern: $PATTERN"
echo "Column: $COLUMN"
echo ""

# Demo spec requires Ntasks_per_stage = 3
go run cmd/cli/main.go \
    2 \
    3 \
    grep \
    --pattern="$PATTERN" --column=$COLUMN \
    count \
    dataset1.csv \
    output1.txt \
    true \
    false \
    100 \
    10 \
    50

echo ""
echo "✅ Job submitted!"
echo ""
echo "📋 Verification steps:"
echo "   1. Monitor: docker exec node1 'tail -f /app/logs/*.log'"
echo "   2. Collect: ./scripts/mp4/collect_output.sh"
echo "   3. Verify:  ./scripts/mp4/verify_test1.sh $PATTERN $COLUMN"
