package main

import (
	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
	"log"
)

// IdentityOp passes through tuples unchanged
type IdentityOp struct{}

func (o *IdentityOp) Process(t rainstorm.Tuple, emit func(rainstorm.Tuple)) {
	// Handle EOF - forward it downstream
	if t.IsEOF {
		log.Printf("[IDENTITY] Received EOF, forwarding to next stage")
		emit(t)
		return
	}

	// Simply pass through the tuple unchanged
	emit(t)
}

func main() {
	log.Printf("[IDENTITY] Starting identity operation (pass-through)")

	op := &IdentityOp{}
	rainstorm.StartOperation(op)
}
