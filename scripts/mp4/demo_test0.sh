#!/bin/bash
# Test 0: Identity operation with rate limiting (Pre-test)

echo "🧪 Test 0: Identity Operation (Pre-test)"
echo "Testing: 100 tuples/sec rate limiting"
echo ""

# Demo spec requires Ntasks_per_stage = 3
docker exec node1 ./rainstorm-cli \
    1 \
    3 \
    identity \
    dataset1.csv \
    output0.txt \
    true \
    false \
    100 \
    10 \
    50

echo ""
echo "✅ Job submitted. Check output with:"
echo "   get output0.txt local_output0.txt"
echo ""
echo "Verify output matches input (order may differ)"
