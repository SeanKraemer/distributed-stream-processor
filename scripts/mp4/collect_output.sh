#!/bin/bash
# Collect RainStorm output files from all cluster containers into the local
# rainstorm_outputs/ folder.
#
# Usage: ./scripts/mp4/collect_output.sh [timestamp]
#   timestamp: optional — collect only that job's timestamp folder
#              (e.g. 20260610_174500); default collects everything.
#
# Files are prefixed vm01_..vm10_ by node number; the verify scripts glob
# for vm*_output_*.txt.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TIMESTAMP="${1:-}"
LOCAL_OUTPUT_DIR="$PROJECT_ROOT/rainstorm_outputs"

mkdir -p "$LOCAL_OUTPUT_DIR"

echo "RainStorm Output Collection"
echo "==========================="
echo "Collecting to: $LOCAL_OUTPUT_DIR"
echo ""

TOTAL_COLLECTED=0

for n in $(seq 1 10); do
    NODE="node$n"
    VM_SHORT=$(printf 'vm%02d' "$n")
    echo -n "   $NODE: "

    if ! docker ps --filter "name=$NODE" --filter "status=running" -q | grep -q .; then
        echo "not running"
        continue
    fi

    TIMESTAMPS=$(docker exec "$NODE" sh -c 'ls /app/rainstorm_outputs/ 2>/dev/null' || true)
    if [ -z "$TIMESTAMPS" ]; then
        echo "no output"
        continue
    fi

    COLLECTED=0
    for ts in $TIMESTAMPS; do
        if [ -n "$TIMESTAMP" ] && [ "$ts" != "$TIMESTAMP" ]; then
            continue
        fi
        mkdir -p "$LOCAL_OUTPUT_DIR/$ts"

        FILES=$(docker exec "$NODE" sh -c "ls /app/rainstorm_outputs/$ts/ 2>/dev/null" || true)
        for filename in $FILES; do
            docker cp -q "$NODE:/app/rainstorm_outputs/$ts/$filename" \
                "$LOCAL_OUTPUT_DIR/$ts/${VM_SHORT}_${filename}" 2>/dev/null && COLLECTED=$((COLLECTED+1)) || true
        done
    done

    if [ "$COLLECTED" -gt 0 ]; then
        echo "$COLLECTED file(s)"
        TOTAL_COLLECTED=$((TOTAL_COLLECTED+COLLECTED))
    else
        echo "no output"
    fi
done

echo ""
echo "Output directories:"
ls -d "$LOCAL_OUTPUT_DIR"/*/ 2>/dev/null | while read -r dir; do
    count=$(find "$dir" -type f 2>/dev/null | wc -l | tr -d ' ')
    echo "   $(basename "$dir")/ ($count files)"
done

echo ""
echo "Collection complete: $TOTAL_COLLECTED file(s)."
