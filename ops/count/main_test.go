package main

import (
	"testing"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
)

func newCountOp() *CountOp {
	return &CountOp{counts: make(map[string]int)}
}

func collectEmissions() (func(rainstorm.Tuple), *[]rainstorm.Tuple) {
	var emitted []rainstorm.Tuple
	return func(t rainstorm.Tuple) {
		emitted = append(emitted, t)
	}, &emitted
}

func TestCountAggregatesByKey(t *testing.T) {
	op := newCountOp()
	emit, emitted := collectEmissions()

	for i, key := range []string{"apple", "banana", "apple", "apple", "banana", "cherry"} {
		op.Process(rainstorm.Tuple{Key: key, Value: "line", ID: string(rune('a' + i))}, emit)
	}

	// Nothing is emitted until EOF triggers the flush.
	if len(*emitted) != 0 {
		t.Fatalf("emitted %d tuples before EOF, want 0", len(*emitted))
	}

	op.Process(rainstorm.Tuple{Key: "EOF", Value: "end-of-stream", ID: "eof-1", IsEOF: true}, emit)

	// Flush emits one count tuple per unique key, then forwards the EOF.
	if len(*emitted) != 4 {
		t.Fatalf("emitted %d tuples after EOF, want 4 (3 counts + 1 EOF)", len(*emitted))
	}

	last := (*emitted)[len(*emitted)-1]
	if !last.IsEOF {
		t.Errorf("last emitted tuple IsEOF = false, want EOF forwarded after counts")
	}

	// Map iteration order is nondeterministic, so collect into a map.
	gotCounts := map[string]interface{}{}
	for _, e := range (*emitted)[:3] {
		if e.IsEOF {
			t.Fatalf("unexpected EOF among count tuples: %+v", e)
		}
		gotCounts[e.Key] = e.Value
		if e.ID != "count-"+e.Key {
			t.Errorf("count tuple ID = %q, want %q", e.ID, "count-"+e.Key)
		}
	}
	wantCounts := map[string]interface{}{
		"apple":  "apple,3",
		"banana": "banana,2",
		"cherry": "cherry,1",
	}
	for key, wantVal := range wantCounts {
		if gotCounts[key] != wantVal {
			t.Errorf("count for %q = %v, want %v", key, gotCounts[key], wantVal)
		}
	}
}

func TestCountFlushesExactlyOnceOnMultipleEOFs(t *testing.T) {
	op := newCountOp()
	emit, emitted := collectEmissions()

	op.Process(rainstorm.Tuple{Key: "x", Value: "v", ID: "t1"}, emit)
	op.Process(rainstorm.Tuple{Key: "x", Value: "v", ID: "t2"}, emit)

	// First EOF flushes counts and forwards EOF.
	op.Process(rainstorm.Tuple{Key: "EOF", ID: "eof-1", IsEOF: true}, emit)
	if len(*emitted) != 2 {
		t.Fatalf("after first EOF: emitted %d tuples, want 2 (1 count + 1 EOF)", len(*emitted))
	}
	if (*emitted)[0].Value != "x,2" {
		t.Errorf("count tuple Value = %v, want %q", (*emitted)[0].Value, "x,2")
	}
	if !(*emitted)[1].IsEOF {
		t.Error("second emission after first EOF should be the forwarded EOF")
	}

	// Subsequent EOFs (from multiple upstream sources) must NOT re-flush,
	// but each EOF is still forwarded downstream.
	op.Process(rainstorm.Tuple{Key: "EOF", ID: "eof-2", IsEOF: true}, emit)
	op.Process(rainstorm.Tuple{Key: "EOF", ID: "eof-3", IsEOF: true}, emit)

	if len(*emitted) != 4 {
		t.Fatalf("after three EOFs: emitted %d tuples, want 4 (counts flushed exactly once)", len(*emitted))
	}
	if !(*emitted)[2].IsEOF || (*emitted)[2].ID != "eof-2" {
		t.Errorf("emitted[2] = %+v, want forwarded eof-2", (*emitted)[2])
	}
	if !(*emitted)[3].IsEOF || (*emitted)[3].ID != "eof-3" {
		t.Errorf("emitted[3] = %+v, want forwarded eof-3", (*emitted)[3])
	}
	if !op.flushed {
		t.Error("flushed guard = false after EOF, want true")
	}
}

func TestCountFlushOnEmptyState(t *testing.T) {
	// EOF with no prior tuples: no count tuples, just the forwarded EOF.
	op := newCountOp()
	emit, emitted := collectEmissions()

	op.Process(rainstorm.Tuple{Key: "EOF", ID: "eof-1", IsEOF: true}, emit)

	if len(*emitted) != 1 {
		t.Fatalf("emitted %d tuples, want 1 (just the EOF)", len(*emitted))
	}
	if !(*emitted)[0].IsEOF {
		t.Error("emitted tuple IsEOF = false, want true")
	}
}
