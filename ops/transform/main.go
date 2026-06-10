package main

import (
	"fmt"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
	"log"
)

// TransformOp extracts fields 1-3 from CSV lines
type TransformOp struct{}

// extractFirst3Fields extracts the first 3 CSV fields while preserving quotes
func extractFirst3Fields(line string) string {
	// Track field boundaries while respecting quotes
	fieldCount := 0
	inQuotes := false

	for i := 0; i < len(line); i++ {
		c := line[i]

		if c == '"' {
			inQuotes = !inQuotes
		} else if c == ',' && !inQuotes {
			fieldCount++
			if fieldCount == 3 {
				// Found the 3rd comma (end of 3rd field)
				return line[:i]
			}
		}
	}

	// If we have fewer than 3 fields, return the whole line
	return line
}

func (o *TransformOp) Process(t rainstorm.Tuple, emit func(rainstorm.Tuple)) {
	// Handle EOF - forward it downstream
	if t.IsEOF {
		log.Printf("[TRANSFORM] Received EOF, forwarding to next stage")
		emit(t)
		return
	}

	// Convert tuple value to string
	valStr := fmt.Sprintf("%v", t.Value)

	// Extract first 3 fields while preserving original formatting (including quotes)
	transformedValue := extractFirst3Fields(valStr)

	// Create new tuple with transformed value
	transformedTuple := rainstorm.Tuple{
		Key:   t.Key, // Keep original key for partitioning
		Value: transformedValue,
		ID:    t.ID,
	}

	emit(transformedTuple)
}

func main() {
	log.Printf("[TRANSFORM] Starting field extraction operation (fields 1-3)")

	op := &TransformOp{}
	rainstorm.StartOperation(op)
}
