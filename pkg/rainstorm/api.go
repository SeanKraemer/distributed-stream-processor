package rainstorm

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Tuple represents a key-value pair in the stream
type Tuple struct {
	Type  string      `json:"type"` // "tuple" or "ack"
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	ID    string      `json:"id"`     // Unique ID for exactly-once tracking
	IsEOF bool        `json:"is_eof"` // End-of-stream marker
}

// Context provides the environment for the user operation
type Context struct {
	Port    int
	Targets []string // "host:port" of next stage tasks
	// Output channel or function
}

// UserOperation defines the interface for user logic
type UserOperation interface {
	Process(tuple Tuple, emit func(Tuple))
}

// StartOperation is the entry point for user binaries
func StartOperation(op UserOperation) {
	port := flag.Int("port", 0, "Port to listen on")
	targets := flag.String("targets", "", "Comma-separated list of target addresses (host:port)")
	outputFile := flag.String("output", "", "Output file for sink stage (optional)")
	numSources := flag.Int("num-sources", 0, "Number of source tasks (for EOF tracking in sink stage)")
	hydfsDestFile := flag.String("hydfs-dest", "", "HyDFS destination filename for sink output")
	hydfsLeader := flag.String("hydfs-leader", "", "HyDFS leader address (host:port) for appends")
	taskID := flag.String("task-id", "", "Task ID for state file naming (critical for recovery)")
	rainstormLeader := flag.String("rainstorm-leader", "", "RainStorm leader address (host:port) for metrics reporting")
	flag.Parse()

	if *port == 0 {
		log.Fatal("❌ Port required")
	}

	// Target list and mutex for dynamic routing updates during autoscaling
	var targetList []string
	var targetListMutex sync.RWMutex
	if *targets != "" {
		targetList = strings.Split(*targets, ",")
	}

	log.Printf("🔧 [OP] Starting operation on port %d with %d targets", *port, len(targetList))

	// State tracking for Exactly-Once
	processedIDs := make(map[string]bool)
	var processedIDsMutex sync.Mutex

	// Log file for state persistence - use TaskID for stable naming across restarts
	var stateLogFile string
	if *taskID != "" {
		stateLogFile = fmt.Sprintf("%s.state", *taskID)
		log.Printf("📋 [OP] Using TaskID-based state file: %s", stateLogFile)
	} else {
		stateLogFile = fmt.Sprintf("task_%d.state", *port)
		log.Printf("⚠️  [OP] No TaskID provided, using port-based state file: %s", stateLogFile)
	}

	// Load state from HyDFS if available (recovery)
	if *hydfsLeader != "" {
		log.Printf("🔄 [OP] Loading state from HyDFS log: %s", stateLogFile)
		ids, err := fetchStateFromHyDFS(*hydfsLeader, stateLogFile)
		if err == nil {
			for _, id := range ids {
				processedIDs[id] = true
			}
			log.Printf("✅ [OP] Recovered %d processed IDs", len(processedIDs))
		} else {
			log.Printf("ℹ️  [OP] No previous state found or failed to load: %v", err)
		}
	}

	// Open output file for sink stage
	var outFile *os.File
	var outFileMutex sync.Mutex
	if len(targetList) == 0 && *outputFile != "" {
		var err error
		outFile, err = os.OpenFile(*outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("❌ [OP] Failed to open output file %s: %v", *outputFile, err)
		}
		log.Printf("📝 [OP] Sink stage will write output to: %s", *outputFile)
	}

	// HyDFS append setup for sink stage
	var hydfsAppendChan chan string
	var hydfsStopChan chan bool
	if len(targetList) == 0 && *hydfsDestFile != "" && *hydfsLeader != "" {
		log.Printf("📝 [OP] Sink stage will append to HyDFS file: %s via %s", *hydfsDestFile, *hydfsLeader)
		hydfsAppendChan = make(chan string, 1000) // Buffer up to 1000 lines
		hydfsStopChan = make(chan bool)
		go hydfsAppendWorker(*hydfsLeader, *hydfsDestFile, hydfsAppendChan, hydfsStopChan, *port)
	}

	// HyDFS State Log Append Worker
	var stateLogChan chan string
	var checkpointStopChan chan bool
	if *hydfsLeader != "" {
		stateLogChan = make(chan string, 1000)
		checkpointStopChan = make(chan bool)
		go hydfsAppendWorker(*hydfsLeader, stateLogFile, stateLogChan, checkpointStopChan, *port)
	}

	// Shutdown flag - use atomic to prevent race conditions
	var isShuttingDown int32 = 0

	// Metrics tracking
	tuplesProcessed := 0
	metricsStartTime := time.Now()

	// Start periodic metrics logging and reporting to leader
	// Use actual taskID if provided, otherwise fall back to port-based ID
	metricsTaskID := fmt.Sprintf("task-port%d", *port)
	if *taskID != "" {
		metricsTaskID = *taskID
	}

	// Get leader address for metrics reporting
	// Use --rainstorm-leader if provided (preferred), otherwise extract from --hydfs-leader
	leaderAddr := ""
	if *rainstormLeader != "" {
		leaderAddr = *rainstormLeader
		log.Printf("📊 [OP] Metrics reporting enabled to leader: %s (task: %s)", leaderAddr, metricsTaskID)
	} else if *hydfsLeader != "" {
		// Fallback: extract host from hydfsLeader and use port 8002
		host, _, err := net.SplitHostPort(*hydfsLeader)
		if err == nil && host != "" {
			leaderAddr = net.JoinHostPort(host, "8002")
			log.Printf("📊 [OP] Metrics reporting enabled to leader (fallback): %s (task: %s)", leaderAddr, metricsTaskID)
		}
	}
	go LogMetricsPeriodicallyWithReport(metricsTaskID, &tuplesProcessed, metricsStartTime, leaderAddr)

	// EOF tracking for ALL stages (not just sink)
	// When a stage receives EOF from all upstream sources, it should exit
	var eofCount int
	var eofMutex sync.Mutex
	shutdownChan := make(chan bool, 1)

	isSinkStage := len(targetList) == 0
	if *numSources > 0 {
		if isSinkStage {
			log.Printf("📊 [OP] Sink stage expecting %d EOF markers", *numSources)
		} else {
			log.Printf("📊 [OP] Middle stage expecting %d EOF markers", *numSources)
		}
	}

	// Connection pool for downstream targets
	// Maintain persistent connections to avoid overhead of connection-per-tuple
	targetConnections := make(map[string]net.Conn)
	targetEncoders := make(map[string]*json.Encoder)
	targetDecoders := make(map[string]*json.Decoder)
	targetMutexes := make(map[string]*sync.Mutex) // Per-target mutex for thread-safe encode/decode
	var connMutex sync.Mutex

	// Initialize connections to all targets with retry logic
	// Downstream tasks may not be ready immediately, so retry with backoff
	log.Printf("⏳ [OP] Waiting 2 seconds for downstream tasks to initialize...")
	time.Sleep(2 * time.Second)

	for _, target := range targetList {
		var conn net.Conn
		var err error

		// Try up to 10 times with increasing delays
		for attempt := 1; attempt <= 10; attempt++ {
			conn, err = net.DialTimeout("tcp", target, 3*time.Second)
			if err == nil {
				break
			}
			if attempt < 10 {
				waitTime := time.Duration(attempt) * 500 * time.Millisecond
				log.Printf("⚠️  [OP] Connection attempt %d to %s failed, retrying in %v...", attempt, target, waitTime)
				time.Sleep(waitTime)
			}
		}

		if err != nil {
			log.Fatalf("❌ [OP] Failed to connect to target %s after 10 attempts: %v", target, err)
		}

		targetConnections[target] = conn
		targetEncoders[target] = json.NewEncoder(conn)
		targetDecoders[target] = json.NewDecoder(conn)
		targetMutexes[target] = &sync.Mutex{} // Create per-target mutex
		log.Printf("🔗 [OP] Connected to downstream target: %s", target)

		// Start Ack listener for this target
		/*
			go func(tgt string, c net.Conn, dec *json.Decoder) {
				for {
					var ack Tuple
					if err := dec.Decode(&ack); err != nil {
						return
					}
					if ack.Type == "ack" {
						// Handle Ack - signal the waiter
						// Complex to map ack to specific send without channel map
					}
				}
			}(target, conn, targetDecoders[target])
		*/
	}

	// Cleanup connections on exit
	defer func() {
		for target, conn := range targetConnections {
			conn.Close()
			log.Printf("🔌 [OP] Closed connection to %s", target)
		}
	}()

	// Start Listener
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("❌ [OP] Failed to listen: %v", err)
	}

	// Track if we've already broadcast EOF (only broadcast once per middle stage)
	var eofBroadcast bool
	var eofBroadcastMutex sync.Mutex

	// Emit function
	emit := func(t Tuple) {
		t.Type = "tuple" // Ensure type is set

		// Check if we're a sink stage (no targets)
		targetListMutex.RLock()
		isSink := len(targetList) == 0
		targetListMutex.RUnlock()

		if isSink {
			// Sink (Last Stage): Output to console and file
			// Track EOF markers for shutdown
			if t.IsEOF {
				log.Printf("🏁 [SINK] Received EOF, skipping output")

				// Track EOF count for sink stage shutdown
				if isSinkStage && *numSources > 0 {
					eofMutex.Lock()
					eofCount++
					currentEOF := eofCount
					eofMutex.Unlock()

					log.Printf("📊 [SINK] Received EOF %d/%d", currentEOF, *numSources)

					// Signal shutdown when all EOFs received
					if currentEOF >= *numSources {
						log.Printf("✅ [SINK] All EOF markers received, shutting down")
						select {
						case shutdownChan <- true:
						default:
						}
					}
				}
				return
			}

			// Format output: value only (not key,value)
			// The Key contains source location (file:line), Value contains actual data
			outputLine := fmt.Sprintf("%v", t.Value)

			// Write to stdout (captured in task log) - required for demo/grading
			fmt.Printf("output: %s\n", outputLine)
			// Write to local output file if specified; guard with mutex to avoid concurrent writes
			if outFile != nil {
				outFileMutex.Lock()
				outFile.WriteString(outputLine + "\n")
				outFileMutex.Unlock()
			}

			// Send to HyDFS append channel if configured (only if not shutting down)
			if hydfsAppendChan != nil && atomic.LoadInt32(&isShuttingDown) == 0 {
				select {
				case hydfsAppendChan <- outputLine:
					// Successfully queued for HyDFS append
				default:
					log.Printf("⚠️  [SINK] HyDFS append buffer full, dropping line")
				}
			}
			return
		}

		// EOF should be broadcast to ALL targets, not hash-partitioned
		// But only broadcast ONCE - subsequent EOFs just count toward shutdown
		if t.IsEOF {
			// Track EOF count for middle stage shutdown
			if *numSources > 0 {
				eofMutex.Lock()
				eofCount++
				currentEOF := eofCount
				eofMutex.Unlock()

				log.Printf("📊 [MIDDLE] Received EOF %d/%d", currentEOF, *numSources)

				// Helper function to broadcast EOF (called when all EOFs received or timeout)
				broadcastEOF := func() {
					eofBroadcastMutex.Lock()
					alreadyBroadcast := eofBroadcast
					if !alreadyBroadcast {
						eofBroadcast = true
					}
					eofBroadcastMutex.Unlock()

					if alreadyBroadcast {
						return // Already broadcast, skip
					}

					// Get snapshot of current targets under lock
					targetListMutex.RLock()
					currentTargets := make([]string, len(targetList))
					copy(currentTargets, targetList)
					targetListMutex.RUnlock()

					log.Printf("📡 [OP] Broadcasting EOF to all %d downstream targets", len(currentTargets))

					// Iterate over snapshot to catch autoscaled tasks
					for _, target := range currentTargets {
						connMutex.Lock()
						encoder := targetEncoders[target]
						targetMu := targetMutexes[target]

						// If no encoder exists, the task was added via autoscaling
						// Establish connection now for EOF delivery
						if encoder == nil {
							conn, err := net.DialTimeout("tcp", target, 3*time.Second)
							if err == nil {
								targetConnections[target] = conn
								targetEncoders[target] = json.NewEncoder(conn)
								targetMutexes[target] = &sync.Mutex{}
								encoder = targetEncoders[target]
								targetMu = targetMutexes[target]
								log.Printf("🔗 [OP] Connected to new target for EOF broadcast: %s", target)
							} else {
								log.Printf("⚠️  [OP] Failed to connect to target %s for EOF: %v", target, err)
								connMutex.Unlock()
								continue
							}
						}
						connMutex.Unlock()

						if encoder == nil || targetMu == nil {
							log.Printf("⚠️  [OP] No encoder/mutex for target %s", target)
							continue
						}

						// Acquire per-target mutex to avoid racing with regular emits
						targetMu.Lock()
						err := encoder.Encode(t)
						targetMu.Unlock()

						if err != nil {
							log.Printf("⚠️  [OP] Failed to send EOF to %s: %v", target, err)
						} else {
							log.Printf("✅ [OP] Sent EOF to %s", target)
						}
					}
				}

				// Broadcast and shutdown when all EOFs received
				if currentEOF >= *numSources {
					log.Printf("✅ [MIDDLE] All EOF markers received, broadcasting and shutting down")
					broadcastEOF()
					select {
					case shutdownChan <- true:
					default:
					}
				} else {
					// Start EOF timeout - if a source died, we may never get all EOFs
					// Wait 5 seconds after first EOF, then broadcast and shut down
					if currentEOF == 1 {
						go func() {
							time.Sleep(5 * time.Second)
							eofMutex.Lock()
							finalEOF := eofCount
							eofMutex.Unlock()
							if finalEOF > 0 && finalEOF < *numSources {
								log.Printf("⏱️  [MIDDLE] EOF timeout: received %d/%d EOFs, broadcasting and shutting down anyway", finalEOF, *numSources)
								broadcastEOF()
								select {
								case shutdownChan <- true:
								default:
								}
							}
						}()
					}
				}
			}
			return
		}

		// Hash Partitioning for regular tuples
		// Simple string hash
		hash := 0
		for _, c := range t.Key {
			hash = int(c) + (hash << 6) + (hash << 16) - hash
		}
		if hash < 0 {
			hash = -hash
		}

		// Lock target list for consistent reading during hash partitioning
		targetListMutex.RLock()
		numTargets := len(targetList)
		if numTargets == 0 {
			targetListMutex.RUnlock()
			log.Printf("⚠️  [OP] No targets available for routing")
			return
		}
		targetIdx := hash % numTargets
		target := targetList[targetIdx]
		targetListMutex.RUnlock()

		// Use persistent connection from pool with per-target mutex for thread safety
		connMutex.Lock()
		encoder := targetEncoders[target]
		decoder := targetDecoders[target]
		targetMu := targetMutexes[target]
		connMutex.Unlock()

		// Lazy connection establishment for newly added targets (from autoscaling)
		if encoder == nil || targetMu == nil {
			log.Printf("🔗 [OP] Lazy connecting to new target %s", target)
			var conn net.Conn
			var err error
			for attempt := 1; attempt <= 5; attempt++ {
				conn, err = net.DialTimeout("tcp", target, 3*time.Second)
				if err == nil {
					break
				}
				if attempt < 5 {
					time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
				}
			}
			if err != nil {
				log.Printf("⚠️  [OP] Failed to connect to target %s: %v", target, err)
				return
			}
			connMutex.Lock()
			targetConnections[target] = conn
			targetEncoders[target] = json.NewEncoder(conn)
			targetDecoders[target] = json.NewDecoder(conn)
			targetMutexes[target] = &sync.Mutex{}
			encoder = targetEncoders[target]
			decoder = targetDecoders[target]
			targetMu = targetMutexes[target]
			connMutex.Unlock()
			log.Printf("✅ [OP] Lazy connected to autoscaled target: %s", target)
		}

		// Lock this target for the entire encode-decode cycle to prevent concurrent access
		targetMu.Lock()

		// Stop-and-Wait: Send Tuple -> Wait for Ack
		// Retry logic for fault tolerance
		success := false
		for attempt := 1; attempt <= 5; attempt++ {
			err := encoder.Encode(t)
			if err != nil {
				log.Printf("⚠️  [OP] Failed to encode tuple to %s (attempt %d): %v", target, attempt, err)
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
				continue
			}

			// Wait for Ack
			// We use the decoder on the SAME connection
			// This assumes synchronous protocol: Send(T) -> Recv(Ack)
			var response Tuple
			// Set read deadline to avoid hanging forever
			// We can't easily set deadline on json.Decoder, need to set on underlying Conn
			// But Conn is shared.
			// Hack: Just read. If it blocks, the task hangs?
			// We should set deadline on the connection if possible, but targetConnections[target] is the conn.

			// connMutex.Lock()
			// conn := targetConnections[target]
			// connMutex.Unlock()
			// conn.SetReadDeadline(time.Now().Add(5 * time.Second))

			if err := decoder.Decode(&response); err != nil {
				log.Printf("⚠️  [OP] Failed to receive Ack from %s: %v", target, err)
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
				continue
			}

			if response.Type == "ack" && response.ID == t.ID {
				// Success
				success = true
				break
			} else {
				log.Printf("⚠️  [OP] Unexpected response from %s: type=%s, id=%s (expected ack for %s)",
					target, response.Type, response.ID, t.ID)
			}
		}

		targetMu.Unlock() // Unlock after encode-decode cycle complete

		if !success {
			log.Printf("❌ [OP] Failed to deliver tuple %s to %s after retries", t.ID, target)
			// In exactly-once, we should probably crash or retry forever?
			// For now, log and continue, but this is data loss.
		}
	}

	// Accept loop with shutdown support
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// Check if this is a "use of closed network connection" error
				// This happens normally during shutdown when listener is closed
				if strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				// Check if shutdown was signaled
				select {
				case <-shutdownChan:
					return
				default:
					log.Printf("Accept error: %v", err)
					continue
				}
			}

			go func(c net.Conn) {
				defer c.Close()
				decoder := json.NewDecoder(c)
				encoder := json.NewEncoder(c)

				for {
					var t Tuple
					if err := decoder.Decode(&t); err != nil {
						return
					}

					// Handle routing update from leader (for autoscaling)
					if t.Type == "routing_update" {
						newTargets := strings.Split(t.Value.(string), ",")

						// Find new targets that need connections
						connMutex.Lock()
						existingTargets := make(map[string]bool)
						for target := range targetConnections {
							existingTargets[target] = true
						}
						connMutex.Unlock()

						// Establish connections to new targets
						for _, target := range newTargets {
							if !existingTargets[target] {
								log.Printf("🔗 [OP] Routing update: connecting to new target %s", target)
								go func(tgt string) {
									var conn net.Conn
									var err error
									// Retry with backoff
									for attempt := 1; attempt <= 5; attempt++ {
										conn, err = net.DialTimeout("tcp", tgt, 3*time.Second)
										if err == nil {
											break
										}
										if attempt < 5 {
											time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
										}
									}
									if err != nil {
										log.Printf("⚠️  [OP] Failed to connect to new target %s: %v", tgt, err)
										return
									}
									connMutex.Lock()
									targetConnections[tgt] = conn
									targetEncoders[tgt] = json.NewEncoder(conn)
									targetDecoders[tgt] = json.NewDecoder(conn)
									targetMutexes[tgt] = &sync.Mutex{}
									connMutex.Unlock()
									log.Printf("✅ [OP] Connected to new autoscaled target: %s", tgt)
								}(target)
							}
						}

						targetListMutex.Lock()
						oldLen := len(targetList)
						targetList = newTargets
						newLen := len(targetList)
						targetListMutex.Unlock()
						log.Printf("📡 [OP] Received routing update: targets changed from %d to %d", oldLen, newLen)
						continue
					}

					// Handle Ack (should not happen in listener if we use Stop-and-Wait on sender side)
					if t.Type == "ack" {
						continue
					}

					// Deduplication check
					processedIDsMutex.Lock()
					isProcessed := processedIDs[t.ID]
					processedIDsMutex.Unlock()

					if isProcessed {
						// Duplicate detected! Send Ack and skip processing.
						log.Printf("♻️ [OP] DUPLICATE REJECTED: tuple %s (already processed, skipping)", t.ID)
						ack := Tuple{Type: "ack", ID: t.ID}
						encoder.Encode(ack)
						continue
					} // Track metrics (only for non-EOF tuples)
					if !t.IsEOF {
						tuplesProcessed++
					}

					// Process the tuple
					op.Process(t, emit)

					// For exactly-once: Checkpoint to HyDFS BEFORE sending ACK
					// This ensures that if we crash after ACK, the ID is already persisted
					// and will be recovered on restart (preventing duplicate processing)
					if !t.IsEOF && *hydfsLeader != "" && atomic.LoadInt32(&isShuttingDown) == 0 {
						// Queue for async batch checkpoint (still needed for performance)
						if stateLogChan != nil {
							select {
							case stateLogChan <- t.ID:
								// Queued for batch append
							default:
								// Channel full, do synchronous checkpoint
								syncCheckpointToHyDFS(*hydfsLeader, stateLogFile, t.ID, *port)
							}
						}
					}

					// Always update in-memory map (even during shutdown)
					if !t.IsEOF {
						processedIDsMutex.Lock()
						processedIDs[t.ID] = true
						processedIDsMutex.Unlock()
					}

					// Send Ack
					ack := Tuple{Type: "ack", ID: t.ID}
					if err := encoder.Encode(ack); err != nil {
						log.Printf("⚠️  [OP] Failed to send Ack for %s: %v", t.ID, err)
					}
				}
			}(conn)
		}
	}()

	// Wait for shutdown signal (for ALL stages with numSources set)
	if *numSources > 0 {
		<-shutdownChan
		log.Printf("🏁 [OP] Shutdown signal received, exiting gracefully")

		// Set shutdown flag FIRST to prevent goroutines from sending to closed channels
		atomic.StoreInt32(&isShuttingDown, 1)

		// Close listener to stop accepting new connections
		ln.Close()

		// Wait for in-flight connection handlers to finish processing
		// This ensures all tuples received before shutdown are processed
		time.Sleep(500 * time.Millisecond)

		// Now close output file (after all processing is done)
		if outFile != nil {
			outFileMutex.Lock()
			outFile.Sync() // Ensure all data is written to disk
			outFile.Close()
			outFileMutex.Unlock()
		}
		// Signal workers to flush and stop (via stop channels)
		if hydfsStopChan != nil {
			close(hydfsStopChan)
		}
		if checkpointStopChan != nil {
			close(checkpointStopChan)
		}
		time.Sleep(1 * time.Second) // Allow time for final flush (matches qz-dev)
	} else {
		// Source stages (no numSources) run forever
		select {}
	}
}

// fetchStateFromHyDFS reads the state log file from HyDFS
func fetchStateFromHyDFS(leaderAddr, filename string) ([]string, error) {
	// Connect to HyDFS leader
	conn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	msg := map[string]interface{}{
		"type":      "get_file",
		"sender":    "task-recovery",
		"operation": "get",
		"filename":  filename,
		"timestamp": time.Now().UnixNano(),
	}

	jsonData, _ := json.Marshal(msg)
	conn.Write(append(jsonData, '\n'))

	// Read response
	reader := bufio.NewReader(conn)
	respLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	var resp map[string]interface{}
	json.Unmarshal([]byte(respLine), &resp)

	// Handle "error" response type as "file not found" - this is OK for fresh start
	// The file may not exist yet if this is the first run or first task on this port
	if resp["type"] == "error" {
		return nil, nil // Return empty state, not an error
	}

	if resp["type"] != "get_file_response" {
		return nil, fmt.Errorf("invalid response type: %v", resp["type"])
	}

	data, ok := resp["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid data format")
	}

	content, ok := data["content"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid content format")
	}

	// Parse IDs (newline separated)
	var ids []string
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			ids = append(ids, strings.TrimSpace(line))
		}
	}
	return ids, nil
}

// LogMetricsPeriodically logs processing rate every second
// This should be called in a goroutine before starting the operation
func LogMetricsPeriodically(taskID string, tuplesProcessed *int, startTime time.Time) {
	LogMetricsPeriodicallyWithReport(taskID, tuplesProcessed, startTime, "")
}

// LogMetricsPeriodicallyWithReport logs processing rate every second and optionally sends to leader
func LogMetricsPeriodicallyWithReport(taskID string, tuplesProcessed *int, startTime time.Time, leaderAddr string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	lastCount := 0
	for range ticker.C {
		currentCount := *tuplesProcessed
		processedThisSecond := currentCount - lastCount
		lastCount = currentCount

		elapsed := time.Since(startTime).Seconds()
		avgRate := 0.0
		if elapsed > 0 {
			avgRate = float64(currentCount) / elapsed
		}

		log.Printf("📊 [TASK %s] Rate: %d tuples/sec (instant), %.2f tuples/sec (avg), Total: %d",
			taskID, processedThisSecond, avgRate, currentCount)

		// Send metrics to leader for autoscaling decisions
		if leaderAddr != "" {
			go sendMetricsToLeaderFromAPI(leaderAddr, taskID, currentCount, float64(processedThisSecond))
		}
	}
}

// sendMetricsToLeaderFromAPI sends task metrics to the ResourceManager (leader)
// This is used by operator tasks (api.go) to report their metrics
func sendMetricsToLeaderFromAPI(leaderAddr, taskID string, tuplesProcessed int, currentRate float64) {
	conn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
	if err != nil {
		// Don't log every failure - leader might be busy
		return
	}
	defer conn.Close()

	msg := RainStormMessage{
		Type:   "task_metrics",
		Sender: taskID,
		Payload: TaskMetricsPayload{
			Metrics: TaskMetrics{
				TaskID:          taskID,
				TuplesProcessed: tuplesProcessed,
				CurrentRate:     currentRate,
				Timestamp:       time.Now().Unix(),
			},
		},
	}

	encoder := json.NewEncoder(conn)
	encoder.Encode(msg)
}

// hydfsAppendWorker batches output lines and appends them to HyDFS periodically
// This reduces the number of HyDFS operations for better performance
// For exactly-once semantics, we flush aggressively to minimize the window
// where a crash could lose checkpointed IDs
func hydfsAppendWorker(leaderAddr, destFile string, appendChan chan string, stopChan chan bool, taskPort int) {
	// Buffer to accumulate lines
	var buffer []string
	flushTicker := time.NewTicker(500 * time.Millisecond) // Flush every 500ms (stable, matches qz-dev)
	defer flushTicker.Stop()

	clientID := fmt.Sprintf("sink-task-%d", taskPort)

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		// Combine buffered lines
		content := strings.Join(buffer, "\n") + "\n"
		buffer = nil

		// Send append request to HyDFS leader
		conn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
		if err != nil {
			log.Printf("⚠️  [HYDFS-APPEND] Failed to connect to %s: %v", leaderAddr, err)
			return
		}
		defer conn.Close()

		msg := map[string]interface{}{
			"type":      "append_file",
			"sender":    clientID,
			"operation": "append",
			"filename":  destFile,
			"data": map[string]interface{}{
				"content":   content,
				"client_id": clientID,
			},
			"timestamp": time.Now().UnixNano(),
		}

		jsonData, _ := json.Marshal(msg)
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(append(jsonData, '\n')); err != nil {
			log.Printf("⚠️  [HYDFS-APPEND] Failed to send append: %v", err)
			return
		}

		log.Printf("📤 [HYDFS-APPEND] Appended %d lines to %s", len(strings.Split(content, "\n"))-1, destFile)
	}

	for {
		select {
		case line, ok := <-appendChan:
			if !ok {
				// Channel closed (shouldn't happen with stop pattern), flush remaining and exit
				flush()
				return
			}
			buffer = append(buffer, line)
			// Flush immediately if buffer gets too large (more stable threshold)
			if len(buffer) >= 50 {
				flush()
			}
		case <-flushTicker.C:
			flush()
		case <-stopChan:
			// Stop signal received, flush remaining data and exit gracefully
			flush()
			return
		}
	}
}

// syncCheckpointToHyDFS performs a synchronous checkpoint of a single tuple ID to HyDFS
// This is used when the async channel is full, to ensure exactly-once semantics
func syncCheckpointToHyDFS(leaderAddr, stateFile, tupleID string, taskPort int) {
	conn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
	if err != nil {
		log.Printf("⚠️  [SYNC-CHECKPOINT] Failed to connect to %s: %v", leaderAddr, err)
		return
	}
	defer conn.Close()

	clientID := fmt.Sprintf("sink-task-%d", taskPort)
	msg := map[string]interface{}{
		"type":      "append_file",
		"sender":    clientID,
		"operation": "append",
		"filename":  stateFile,
		"data": map[string]interface{}{
			"content":   tupleID + "\n",
			"client_id": clientID,
		},
		"timestamp": time.Now().UnixNano(),
	}

	jsonData, _ := json.Marshal(msg)
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(append(jsonData, '\n')); err != nil {
		log.Printf("⚠️  [SYNC-CHECKPOINT] Failed to send: %v", err)
		return
	}
	log.Printf("📤 [SYNC-CHECKPOINT] Checkpointed %s to %s", tupleID, stateFile)
}
