package rainstorm

import (
	"fmt"
	"strings"
	"testing"
)

// TestHashStringNonNegative verifies that hashString never returns a negative
// value, even for inputs whose intermediate accumulation overflows int64 and
// goes negative before the final sign fixup.
func TestHashStringNonNegative(t *testing.T) {
	inputs := []string{
		"",
		"a",
		"EOF",
		"file.txt:1",
		"trafficsigns.csv:123456",
		strings.Repeat("z", 1),
		strings.Repeat("z", 10),
		// Long inputs force repeated (hash<<16) shifts that overflow the
		// signed accumulator into negative territory mid-computation.
		strings.Repeat("z", 100),
		strings.Repeat("ÿ", 500),
		strings.Repeat("overflow-driver-", 1000),
		// Multi-byte runes (int(c) can be large).
		"世界\U0001F600",
	}
	// Plus many generated keys mimicking real tuple keys.
	for i := 0; i < 1000; i++ {
		inputs = append(inputs, fmt.Sprintf("dataset.csv:%d", i*7919))
	}

	for _, in := range inputs {
		h := hashString(in)
		if h < 0 {
			t.Errorf("hashString(%q) = %d, want non-negative", truncate(in), h)
		}
	}
}

// TestHashStringDeterministic verifies the same input always yields the same
// hash (the partitioner relies on this for stable routing).
func TestHashStringDeterministic(t *testing.T) {
	inputs := []string{"", "a", "file.txt:42", strings.Repeat("key-", 200)}
	for _, in := range inputs {
		first := hashString(in)
		for i := 0; i < 10; i++ {
			if got := hashString(in); got != first {
				t.Fatalf("hashString(%q) not deterministic: got %d then %d", truncate(in), first, got)
			}
		}
	}
}

// TestHashStringDistribution is a loose sanity check that hash partitioning
// spreads realistic keys across buckets: with 10k keys, no bucket should
// receive fewer than 50% of its fair share. This documents (not proves) that
// the partitioner is usable for load balancing.
func TestHashStringDistribution(t *testing.T) {
	const numKeys = 10000
	keys := make([]string, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		// Mimics the source tuple key format "<filename>:<lineNumber>".
		keys = append(keys, fmt.Sprintf("input_file.csv:%d", i+1))
	}

	for _, numBuckets := range []int{3, 5, 8} {
		buckets := make([]int, numBuckets)
		for _, k := range keys {
			buckets[hashString(k)%numBuckets]++
		}

		fairShare := numKeys / numBuckets
		minAllowed := fairShare / 2
		for b, count := range buckets {
			if count < minAllowed {
				t.Errorf("buckets=%d: bucket %d got %d keys, want >= %d (50%% of fair share %d); full distribution: %v",
					numBuckets, b, count, minAllowed, fairShare, buckets)
			}
		}
	}
}

func truncate(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}
