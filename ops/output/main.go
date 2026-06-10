package main

import (
	"fmt"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
	"log"
	"os"
	"sync"
)

// OutputOp collects all tuples and writes them to a file
type OutputOp struct {
	outputFile string
	buffer     []string
	mu         sync.Mutex
}

func (o *OutputOp) Process(t rainstorm.Tuple, emit func(rainstorm.Tuple)) {
	// Handle EOF - flush buffer to file
	if t.IsEOF {
		log.Printf("[OUTPUT] Received EOF, flushing %d lines to %s", len(o.buffer), o.outputFile)
		if err := o.Flush(); err != nil {
			log.Printf("[OUTPUT] Failed to flush: %v", err)
		} else {
			log.Printf("[OUTPUT] Successfully wrote output file")
		}
		// HyDFS upload is handled outside this operator: the framework sink path
		// appends via -hydfs-dest, and scripts/mp4/collect_output.sh gathers the
		// per-task files.
		return
	}

	// Collect tuple in buffer
	// For Application 2 (grep+transform), we only want the Value (transformed CSV fields)
	// For Application 1 (grep+count), the Key is the aggregation key and Value is the count
	// Output format: "VALUE" only (not "KEY: VALUE")
	o.mu.Lock()
	line := fmt.Sprintf("%v", t.Value)
	o.buffer = append(o.buffer, line)
	o.mu.Unlock()

	// No downstream, so don't emit
	if len(o.buffer) <= 5 {
		log.Printf("[OUTPUT] Collected: %s", line)
	}
} // Flush writes all buffered output to file
func (o *OutputOp) Flush() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.buffer) == 0 {
		log.Printf("[OUTPUT] No data to write")
		return nil
	}

	// Write to local file (will be uploaded to HyDFS separately)
	f, err := os.Create(o.outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer f.Close()

	for _, line := range o.buffer {
		fmt.Fprintln(f, line)
	}

	log.Printf("[OUTPUT] Wrote %d lines to %s", len(o.buffer), o.outputFile)
	return nil
}

func main() {
	// Get output filename from environment or command line
	outputFile := os.Getenv("RAINSTORM_OUTPUT")
	if outputFile == "" {
		outputFile = "rainstorm_output.txt"
	}

	log.Printf("[OUTPUT] Starting output sink to %s", outputFile)

	op := &OutputOp{
		outputFile: outputFile,
		buffer:     make([]string, 0),
	}

	// Process flushes the buffer when the EOF tuple arrives.
	rainstorm.StartOperation(op)
}
