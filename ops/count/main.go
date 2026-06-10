package main

import (
	"fmt"
	"github.com/SeanKraemer/distributed-stream-processor/internal/debuglog"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
	"log"
	"sync"
)

// CountOp counts occurrences by tuple Key
type CountOp struct {
	counts     map[string]int
	mu         sync.Mutex
	tupleCount int  // Counter for debug logging
	flushed    bool // Track if we've already flushed (to handle multiple EOFs)
}

func (o *CountOp) Process(t rainstorm.Tuple, emit func(rainstorm.Tuple)) {
	// Handle EOF - flush counts ONCE and forward EOF
	if t.IsEOF {
		o.mu.Lock()
		alreadyFlushed := o.flushed
		if !o.flushed {
			o.flushed = true
		}
		o.mu.Unlock()

		if !alreadyFlushed {
			log.Printf("[COUNT] Received first EOF, flushing %d counts", len(o.counts))
			o.FlushCounts(emit)
		} else {
			log.Printf("[COUNT] Received additional EOF, skipping flush (already flushed)")
		}
		emit(t) // Always forward EOF
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	// Count by tuple Key (which upstream operators like grep set to extracted column)
	// The Key represents the grouping dimension (e.g., column value from grep)
	key := t.Key

	// Debug logging for first 10 tuples
	if o.tupleCount < 10 {
		o.tupleCount++
		debuglog.Debugf("[COUNT DEBUG #%d] Key: %q", o.tupleCount, key)
	}

	o.counts[key]++
} // FlushCounts emits all accumulated counts (should be called at end of stream)
func (o *CountOp) FlushCounts(emit func(rainstorm.Tuple)) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for word, count := range o.counts {
		// Format as "KEY,COUNT" for output operator
		// Output operator will write just the Value, so we format it here
		formattedOutput := fmt.Sprintf("%s,%d", word, count)
		countTuple := rainstorm.Tuple{
			Key:   word, // Keep key for potential downstream aggregation
			Value: formattedOutput,
			ID:    fmt.Sprintf("count-%s", word),
		}
		emit(countTuple)
	}

	log.Printf("[COUNT] Flushed %d unique keys", len(o.counts))
}

// CountOpWrapper wraps CountOp and handles initialization after flag parsing
type CountOpWrapper struct {
	actualOp    *CountOp
	initialized bool
}

func (w *CountOpWrapper) Process(t rainstorm.Tuple, emit func(rainstorm.Tuple)) {
	// Initialize on first call (after flag.Parse has been called by StartOperation)
	if !w.initialized {
		log.Printf("[COUNT] Starting count operation (grouping by tuple Key)")

		w.actualOp = &CountOp{
			counts: make(map[string]int),
		}
		w.initialized = true
	}

	// Delegate to actual operation
	w.actualOp.Process(t, emit)
}

func main() {
	// Create wrapper that will initialize after StartOperation calls flag.Parse()
	op := &CountOpWrapper{}

	// StartOperation will call flag.Parse() with all flags defined
	rainstorm.StartOperation(op)
}
