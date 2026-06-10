// Package debuglog gates chatty per-tuple and per-chain diagnostics behind
// LOG_LEVEL=debug. It delegates to the standard logger, so gated output goes
// to the same destinations (stdout + the node's log file) as everything else.
package debuglog

import (
	"log"
	"os"
	"sync"
)

var enabled = sync.OnceValue(func() bool {
	return os.Getenv("LOG_LEVEL") == "debug"
})

// Enabled reports whether debug logging is on (LOG_LEVEL=debug).
func Enabled() bool {
	return enabled()
}

// Debugf logs through the default logger when debug logging is enabled.
func Debugf(format string, args ...any) {
	if enabled() {
		log.Printf(format, args...)
	}
}
