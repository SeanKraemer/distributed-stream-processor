package main

import (
	"flag"
	"fmt"
	"github.com/SeanKraemer/distributed-stream-processor/internal/debuglog"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
	"log"
	"regexp"
	"strings"
)

// GrepOp filters tuples based on a regex pattern
type GrepOp struct {
	pattern      *regexp.Regexp
	matchCount   int
	filterCount  int
	columnNum    int  // Optional: CSV column to extract as new key (0 = keep original key)
	eofForwarded bool // Track if we've already forwarded EOF
}

// GrepOpWrapper wraps GrepOp and handles initialization after flag parsing
type GrepOpWrapper struct {
	patternFlag *string
	columnNum   *int
	actualOp    *GrepOp
	initialized bool
}

func (w *GrepOpWrapper) Process(t rainstorm.Tuple, emit func(rainstorm.Tuple)) {
	// Initialize on first call (after flag.Parse has been called by StartOperation)
	if !w.initialized {
		patternStr := *w.patternFlag

		// Check for positional args if flag wasn't set
		if patternStr == "" && len(flag.Args()) > 0 {
			patternStr = flag.Args()[0]
		}

		if patternStr == "" {
			log.Fatal("Usage: grep <pattern> [column]  OR  grep --pattern=<pattern> [--column=N]")
		}

		// Compile regex pattern
		pattern, err := regexp.Compile(patternStr)
		if err != nil {
			log.Fatalf("Invalid regex pattern '%s': %v", patternStr, err)
		}

		columnNum := *w.columnNum
		// Check for positional column arg if flag wasn't set
		if columnNum == 0 && len(flag.Args()) > 1 {
			fmt.Sscanf(flag.Args()[1], "%d", &columnNum)
		}

		if columnNum > 0 {
			log.Printf("[GREP] Starting with pattern: %s, extracting column %d as key", patternStr, columnNum)
		} else {
			log.Printf("[GREP] Starting with pattern: %s", patternStr)
		}

		w.actualOp = &GrepOp{
			pattern:   pattern,
			columnNum: columnNum,
		}
		w.initialized = true
	}

	// Delegate to actual operation
	w.actualOp.Process(t, emit)
}

func (o *GrepOp) Process(t rainstorm.Tuple, emit func(rainstorm.Tuple)) {
	// Handle EOF - forward it downstream ONCE, but always call emit for EOF tracking
	if t.IsEOF {
		if !o.eofForwarded {
			log.Printf("[GREP] Received first EOF, forwarding to next stage")
			o.eofForwarded = true
		} else {
			log.Printf("[GREP] Received additional EOF (tracking for shutdown)")
		}
		// Always call emit for EOF so the framework can track shutdown
		emit(t)
		return
	}

	// Convert tuple value to string
	valStr := fmt.Sprintf("%v", t.Value)

	// Debug: Log first 3 tuples
	if o.matchCount+o.filterCount < 3 {
		debuglog.Debugf("[GREP DEBUG] Tuple: %q", valStr[:min(100, len(valStr))])
		debuglog.Debugf("[GREP DEBUG] Pattern: %q, Matches: %v", o.pattern.String(), o.pattern.MatchString(valStr))
	}

	// Check if pattern matches
	if o.pattern.MatchString(valStr) {
		o.matchCount++

		// If columnNum is set, extract that CSV column and use it as the new key
		// This enables proper partitioning for downstream aggregation
		newTuple := t
		if o.columnNum > 0 {
			// Parse CSV and extract column
			fields := parseCSVLine(valStr)
			if o.columnNum <= len(fields) {
				newTuple.Key = fields[o.columnNum-1] // 1-indexed to 0-indexed
			} else {
				// Column doesn't exist, use empty string
				newTuple.Key = ""
			}

			if o.matchCount <= 3 {
				debuglog.Debugf("[GREP DEBUG] Extracted column %d as key: %q", o.columnNum, newTuple.Key)
			}
		}

		// Emit the tuple if it matches
		emit(newTuple)
	} else {
		o.filterCount++
	}
}

// parseCSVLine parses a CSV line into fields, handling quoted fields
func parseCSVLine(line string) []string {
	// Simple CSV parser - split by comma, handle quotes
	var fields []string
	var current strings.Builder
	inQuotes := false

	for i := 0; i < len(line); i++ {
		c := line[i]
		switch c {
		case '"':
			inQuotes = !inQuotes
		case ',':
			if inQuotes {
				current.WriteByte(c)
			} else {
				fields = append(fields, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(c)
		}
	}
	fields = append(fields, current.String())

	return fields
}

// Helper for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	// Parse command-line arguments
	// Expected: ./grep --pattern=<pattern> [--column=N] --port=<port> --targets=<targets>

	// Define flags (but don't parse yet - StartOperation will do that)
	patternFlag := flag.String("pattern", "", "Regex pattern to match (alternative to positional arg)")
	columnNum := flag.Int("column", 0, "CSV column to extract as new key for partitioning (1-indexed, 0 = keep original key)")

	// Create a custom wrapper to extract pattern after flag.Parse()
	op := &GrepOpWrapper{
		patternFlag: patternFlag,
		columnNum:   columnNum,
	}
	rainstorm.StartOperation(op)
}
