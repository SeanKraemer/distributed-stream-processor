package main

import (
	"fmt"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
)

// EchoOp just passes the tuple through, optionally modifying the value
type EchoOp struct{}

func (o *EchoOp) Process(t rainstorm.Tuple, emit func(rainstorm.Tuple)) {
	// Example transform: append " processed"
	valStr := fmt.Sprintf("%v processed", t.Value)

	newTuple := rainstorm.Tuple{
		Key:   t.Key,
		Value: valStr,
		ID:    t.ID,
	}

	emit(newTuple)
}

func main() {
	op := &EchoOp{}
	rainstorm.StartOperation(op)
}
