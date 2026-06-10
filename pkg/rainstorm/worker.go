package rainstorm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/hashing"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/membership"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// HandleScheduleTask processes a task assignment
func (s *Server) HandleScheduleTask(msg RainStormMessage) {
	// 1. Parse Payload
	data, _ := json.Marshal(msg.Payload)
	var payload ScheduleTaskPayload
	json.Unmarshal(data, &payload)
	task := payload.Task
	routingTable := payload.RoutingTable

	log.Printf("👷 [WORKER] Received Task: %s (Op: %s, Port: %d)", task.ID, task.OpExecutable, task.Port)
	log.Printf("🔍 [WORKER DEBUG] Task details: TaskIndex=%d, TotalTasks=%d, InputRate=%d", task.TaskIndex, task.TotalTasks, task.InputRate)

	// 2. Update Local State
	s.ActiveTasksMutex.Lock()
	s.ActiveTasks[task.ID] = &task
	s.ActiveTasksMutex.Unlock()

	// 3. Launch Task Process
	go s.runTaskProcess(&task, routingTable)
}

// runTaskProcess executes the operation binary or internal handler
func (s *Server) runTaskProcess(task *Task, routingTable map[int][]string) {
	// Determine targets for next stage
	nextStageID := task.StageID + 1
	nextStageTargets := routingTable[nextStageID]
	targetsStr := strings.Join(nextStageTargets, ",")

	if task.OpExecutable == "internal_source" {
		s.runSourceTask(task, nextStageTargets)
		return
	}

	// For user-defined operations (binaries)
	// Operations are in ops/<op_name>/<op_name> directory structure
	// If the executable path is already provided (contains /), use it.
	// Otherwise, assume it's a name like "grep" and look in ops/grep/grep
	cmdPath := task.OpExecutable
	if !strings.Contains(cmdPath, "/") {
		cmdPath = fmt.Sprintf("ops/%s/%s", cmdPath, cmdPath)
	}

	if _, err := os.Stat(cmdPath); os.IsNotExist(err) {
		log.Printf("❌ [WORKER] Executable %s not found!", cmdPath)
		return
	}

	// Prepare arguments
	// IMPORTANT: Framework flags (--port, --targets, --output) must come BEFORE operator args
	// because Go's flag package stops parsing after the first non-flag argument
	// Order: --port=<port> --task-id=<id> --targets=<t1,t2,...> --output=<file> <op_args...>
	var args []string

	// Add framework flags first
	args = append(args, fmt.Sprintf("--port=%d", task.Port))
	args = append(args, fmt.Sprintf("--task-id=%s", task.ID)) // Critical for state recovery
	if len(nextStageTargets) > 0 {
		args = append(args, fmt.Sprintf("--targets=%s", targetsStr))
	}
	if task.OutputFile != "" {
		// Expand relative path to absolute path using current working directory
		outputPath := task.OutputFile
		if !strings.HasPrefix(outputPath, "/") {
			cwd, err := os.Getwd()
			if err != nil {
				cwd = "."
			}
			outputPath = fmt.Sprintf("%s/%s", cwd, outputPath)
			// Ensure output directory exists
			outputDir := outputPath[:strings.LastIndex(outputPath, "/")]
			os.MkdirAll(outputDir, 0755)
		}
		args = append(args, fmt.Sprintf("--output=%s", outputPath))
	}
	if task.NumSources > 0 {
		args = append(args, fmt.Sprintf("--num-sources=%d", task.NumSources))
	}

	// Add HyDFS destination flags for sink tasks
	if task.HyDFSDestFile != "" {
		args = append(args, fmt.Sprintf("--hydfs-dest=%s", task.HyDFSDestFile))
	}
	if task.HyDFSLeader != "" {
		args = append(args, fmt.Sprintf("--hydfs-leader=%s", task.HyDFSLeader))
	}
	// Add RainStorm leader address for metrics reporting
	if task.RainStormLeader != "" {
		args = append(args, fmt.Sprintf("--rainstorm-leader=%s", task.RainStormLeader))
	}

	// Then add operator-specific arguments (pass as-is, don't split on whitespace)
	args = append(args, task.OpArgs...)

	cmd := exec.Command(cmdPath, args...)

	// Setup environment variables
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("RAINSTORM_PORT=%d", task.Port))

	// Determine output directory - use job timestamp if available, else current time
	var outputDir string
	if task.JobTimestamp != "" {
		outputDir = fmt.Sprintf("rainstorm_outputs/%s", task.JobTimestamp)
	} else {
		outputDir = fmt.Sprintf("rainstorm_outputs/%s", time.Now().Format("20060102_150405"))
	}

	// Get working directory for output path resolution
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}
	fullOutputDir := fmt.Sprintf("%s/%s", cwd, outputDir)

	// Ensure output directory exists for logs and output
	os.MkdirAll(fullOutputDir, 0755)

	// Generate log file path
	logFile := fmt.Sprintf("%s/task_%s.log", fullOutputDir, task.ID)
	cmd.Stdout, _ = os.Create(logFile)
	cmd.Stderr = cmd.Stdout

	log.Printf("🚀 [WORKER] Starting process: %s %v", cmdPath, args)
	err = cmd.Start()
	if err != nil {
		log.Printf("❌ [WORKER] Failed to start task %s: %v", task.ID, err)
		return
	}

	// Track process
	pid := cmd.Process.Pid
	task.PID = pid
	task.LogFile = logFile
	task.State = TaskStateRunning

	s.TaskProcessesMutex.Lock()
	s.TaskProcesses[pid] = cmd
	s.TaskProcessesMutex.Unlock()

	log.Printf("📍 [WORKER] TASK START: TaskID=%s, VM=%s, PID=%d, OpExe=%s, LogFile=%s",
		task.ID, task.AssignedWorker, pid, task.OpExecutable, logFile)

	// Report to leader
	s.ReportTaskStarted(task)

	// Monitor process for unexpected death (Snitch Pattern)
	// Use a channel to track if this is a normal shutdown or unexpected death
	shutdownChan := make(chan bool, 1)
	waitDoneChan := make(chan error, 1)

	go func() {
		// Wait for process to exit
		processErr := cmd.Wait()
		waitDoneChan <- processErr

		// Check if this was a normal shutdown or unexpected death
		select {
		case <-shutdownChan:
			// Normal shutdown - main routine will handle reporting
			return
		default:
			// Unexpected death - notify leader for restart
			if processErr != nil {
				log.Printf("💀 [WORKER] TASK FAILED: %s on %s (PID=%d, exit: %v)",
					task.ID, task.AssignedWorker, pid, processErr)
				s.NotifyLeaderTaskFailed(task, processErr.Error())
			}
		}
	}()

	// Wait for process to complete
	err = <-waitDoneChan

	// If the process exited cleanly, signal that this was a normal shutdown.
	// If it exited with an error, the snitch goroutine will handle reporting the failure.
	if err == nil {
		shutdownChan <- true
	}

	// Log and report completion
	if err != nil {
		log.Printf("🏁 [WORKER] TASK END: TaskID=%s, VM=%s, PID=%d, Status=FAILED, Error=%v",
			task.ID, task.AssignedWorker, pid, err)
		task.State = TaskStateFailed
		s.ReportTaskCompleted(task, false, err.Error())
	} else {
		log.Printf("🏁 [WORKER] TASK END: TaskID=%s, VM=%s, PID=%d, Status=SUCCESS",
			task.ID, task.AssignedWorker, pid)
		task.State = TaskStateCompleted
		s.ReportTaskCompleted(task, true, "")
	}
}

// fetchFileFromHyDFS fetches a file from HyDFS using distributed GET
func (s *Server) fetchFileFromHyDFS(hydfsFilename string) ([]byte, error) {
	log.Printf("🌐 [DISTRIBUTED GET] Fetching %s from HyDFS network...", hydfsFilename)

	// Calculate file ID using consistent hashing
	fileID := hashing.HashString(hydfsFilename)

	// Find successor nodes (replicas) for this file
	infoMap := s.Node.Membership.GetInfoMap()
	successors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

	if len(successors) == 0 {
		return nil, fmt.Errorf("no alive replicas found for %s", hydfsFilename)
	}

	// Try each replica in order until one succeeds
	for _, successorNodeID := range successors {
		replicaInfo, ok := infoMap[successorNodeID]
		if !ok || replicaInfo.State != membership.Alive {
			continue
		}

		log.Printf("📍 [DISTRIBUTED GET] Trying replica at %s:%d (NodeID=%020d)",
			replicaInfo.Hostname, replicaInfo.Port, successorNodeID)

		// Send GET request to replica
		type NodeMessage struct {
			Type      string                 `json:"type"`
			Sender    string                 `json:"sender"`
			Operation string                 `json:"operation"`
			Filename  string                 `json:"filename"`
			Data      map[string]interface{} `json:"data,omitempty"`
			Timestamp int64                  `json:"timestamp"`
		}

		msg := NodeMessage{
			Type:      "get_file",
			Sender:    infoMap[s.Node.RingID].Hostname,
			Operation: "get",
			Filename:  hydfsFilename,
			Data:      map[string]interface{}{},
			Timestamp: time.Now().UnixNano(),
		}

		jsonData, _ := json.Marshal(msg)

		// Connect to replica
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", replicaInfo.Hostname, replicaInfo.Port), 5*time.Second)
		if err != nil {
			log.Printf("⚠️  [DISTRIBUTED GET] Failed to connect to %s:%d: %v", replicaInfo.Hostname, replicaInfo.Port, err)
			continue
		}

		// Send request
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(append(jsonData, '\n')); err != nil {
			conn.Close()
			log.Printf("⚠️  [DISTRIBUTED GET] Failed to send request to %s:%d: %v", replicaInfo.Hostname, replicaInfo.Port, err)
			continue
		}

		// Read response
		reader := bufio.NewReader(conn)
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		responseLine, err := reader.ReadString('\n')
		conn.Close()

		if err != nil {
			log.Printf("⚠️  [DISTRIBUTED GET] Failed to receive response from %s:%d: %v", replicaInfo.Hostname, replicaInfo.Port, err)
			continue
		}

		var response NodeMessage
		if err := json.Unmarshal([]byte(responseLine), &response); err != nil {
			log.Printf("⚠️  [DISTRIBUTED GET] Failed to parse response from %s:%d: %v", replicaInfo.Hostname, replicaInfo.Port, err)
			continue
		}

		if response.Type == "get_file_response" {
			contentStr, ok := response.Data["content"].(string)
			if !ok {
				log.Printf("⚠️  [DISTRIBUTED GET] Invalid content in response from %s:%d", replicaInfo.Hostname, replicaInfo.Port)
				continue
			}

			content := []byte(contentStr)
			log.Printf("✅ [DISTRIBUTED GET] Successfully fetched %d bytes from %s:%d", len(content), replicaInfo.Hostname, replicaInfo.Port)
			return content, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch file from any replica")
}

// runSourceTask handles the source stream generation (Internal Go routine)
func (s *Server) runSourceTask(task *Task, targets []string) {
	// Calculate per-task rate (divide total INPUT_RATE among all source tasks)
	perTaskRate := task.InputRate
	if task.TotalTasks > 0 {
		perTaskRate = task.InputRate / task.TotalTasks
	}

	log.Printf("📍 [SOURCE] TASK START: TaskID=%s, VM=%s, OpExe=internal_source, SourceFile=%s",
		task.ID, task.AssignedWorker, task.OpArgs[0])
	log.Printf("    -> Per-task rate: %d tuples/sec (total INPUT_RATE: %d / %d tasks)",
		perTaskRate, task.InputRate, task.TotalTasks)
	log.Printf("    -> Targeting %d downstream tasks: %v", len(targets), targets)

	// Report task start to leader
	task.OpExecutable = "internal_source"
	s.ReportTaskStarted(task)

	hydfsFilename := task.OpArgs[0]

	// 1. Read file - try multiple sources in order:
	//    a) Local BlockStore (HyDFS local replica)
	//    b) Distributed GET from other HyDFS replicas
	//    c) Local filesystem (data/ directory) - fallback per demo instructions
	log.Printf("📥 [SOURCE] Fetching %s...", hydfsFilename)

	var content []byte
	var err error

	// Try BlockStore first (local HyDFS replica)
	content, err = s.BlockStore.ReadFile(hydfsFilename)
	if err != nil {
		// File not local - try distributed GET from other replicas
		log.Printf("⚠️  [SOURCE] File %s not in local BlockStore, trying distributed GET...", hydfsFilename)
		content, err = s.fetchFileFromHyDFS(hydfsFilename)
		if err != nil {
			// Fallback: try local filesystem (data/ directory)
			// This is explicitly allowed by demo instructions for source files
			localPath := fmt.Sprintf("data/%s", hydfsFilename)
			log.Printf("⚠️  [SOURCE] Distributed GET failed, trying local filesystem: %s", localPath)
			content, err = os.ReadFile(localPath)
			if err != nil {
				log.Printf("❌ [SOURCE] Failed to fetch %s from any source: %v", hydfsFilename, err)
				task.State = TaskStateFailed
				return
			}
			log.Printf("✅ [SOURCE] Successfully read %d bytes from local filesystem: %s", len(content), localPath)
		} else {
			log.Printf("✅ [SOURCE] Successfully read %d bytes via distributed GET", len(content))
		}
	} else {
		log.Printf("✅ [SOURCE] Successfully read %d bytes from local BlockStore", len(content))
	}

	// 2. Split content into lines and partition among source tasks
	allLines := strings.Split(string(content), "\n")

	// Partition: each task processes lines where (lineIndex % TotalTasks == TaskIndex)
	// Store both line content and original line number
	type lineWithNum struct {
		content     string
		origLineNum int
	}
	var lines []lineWithNum
	for i, line := range allLines {
		if i%task.TotalTasks == task.TaskIndex {
			lines = append(lines, lineWithNum{content: line, origLineNum: i + 1}) // 1-indexed line numbers
		}
	}
	log.Printf("📄 [SOURCE] Task %s processing %d/%d lines (partition %d/%d)",
		task.ID, len(lines), len(allLines), task.TaskIndex, task.TotalTasks)

	// 3. Create connection pool to downstream tasks
	if len(targets) == 0 {
		log.Printf("⚠️  [SOURCE] No downstream targets configured!")
		task.State = TaskStateCompleted
		return
	}

	// Open persistent connections to all targets with retry logic
	connections := make(map[string]net.Conn)
	encoders := make(map[string]*json.Encoder)
	decoders := make(map[string]*json.Decoder)

	// Wait a bit for downstream tasks to start listening
	log.Printf("⏳ [SOURCE] Waiting 2 seconds for downstream tasks to initialize...")
	time.Sleep(2 * time.Second)

	// Retry connection with exponential backoff
	for _, target := range targets {
		var conn net.Conn
		var err error

		// Try up to 5 times with increasing delays
		for attempt := 1; attempt <= 5; attempt++ {
			conn, err = net.DialTimeout("tcp", target, 3*time.Second)
			if err == nil {
				break
			}
			if attempt < 5 {
				waitTime := time.Duration(attempt) * 500 * time.Millisecond
				log.Printf("⚠️  [SOURCE] Connection attempt %d to %s failed, retrying in %v...", attempt, target, waitTime)
				time.Sleep(waitTime)
			}
		}

		if err != nil {
			log.Printf("❌ [SOURCE] Failed to connect to %s after 5 attempts: %v", target, err)
			continue
		}

		log.Printf("✅ [SOURCE] Connected to %s", target)
		connections[target] = conn
		encoders[target] = json.NewEncoder(conn)
		decoders[target] = json.NewDecoder(conn)
	}

	// Cleanup connections when done
	defer func() {
		for target, conn := range connections {
			conn.Close()
			log.Printf("🔌 [SOURCE] Closed connection to %s", target)
		}
	}()

	// 4. Emit tuples with hash partitioning and rate limiting
	tuplesSent := 0
	metricsStartTime := time.Now()

	// Start metrics reporting goroutine
	metricsStopChan := make(chan bool)
	go s.reportSourceMetrics(task, &tuplesSent, metricsStartTime, metricsStopChan)
	defer func() { metricsStopChan <- true }()

	// Calculate rate limiting interval based on per-task rate
	var rateLimitTicker *time.Ticker
	taskRate := task.InputRate
	if task.TotalTasks > 0 {
		taskRate = task.InputRate / task.TotalTasks
	}
	if taskRate > 0 {
		interval := time.Second / time.Duration(taskRate)
		rateLimitTicker = time.NewTicker(interval)
		defer rateLimitTicker.Stop()
		log.Printf("⏱️  [SOURCE] Rate limiting: %d tuples/sec (interval: %v)", taskRate, interval)
	}

	for _, lineInfo := range lines {
		// Skip empty lines
		lineContent := strings.TrimSpace(lineInfo.content)
		if lineContent == "" {
			continue
		}

		// Rate limiting
		if rateLimitTicker != nil {
			<-rateLimitTicker.C // Wait for next tick
		}

		// Create tuple with unique ID using original line number
		tuple := Tuple{
			Type:  "tuple",
			Key:   fmt.Sprintf("%s:%d", hydfsFilename, lineInfo.origLineNum),
			Value: lineContent,
			ID:    fmt.Sprintf("%s-tuple-%d", task.ID, lineInfo.origLineNum),
		}

		// Hash partition to target
		hash := hashString(tuple.Key)
		targetIdx := hash % len(targets)
		target := targets[targetIdx]

		// Check connection
		if _, ok := encoders[target]; !ok {
			log.Printf("⚠️  [SOURCE] No connection to target %s, attempting reconnect...", target)
			// Connection logic handled in retry loop below
		}

		// Retry loop for sending AND waiting for Ack
		success := false
		for retryAttempt := 1; retryAttempt <= 10; retryAttempt++ {
			encoder := encoders[target]
			decoder := decoders[target]
			conn := connections[target]

			// If connection missing, try to reconnect
			if encoder == nil || decoder == nil || conn == nil {
				// Reconnect logic...
				// Query leader for updated task locations (nextStageID = task.StageID + 1)
				queryStageID := task.StageID + 1
				leaderAddr := net.JoinHostPort(s.Config.VMs[0], fmt.Sprintf("%d", s.Config.RainStormPort))
				leaderConn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
				if err == nil {
					// Ask leader where tasks are
					queryMsg := RainStormMessage{
						Type:    "get_task_location",
						Sender:  s.Node.Membership.GetInfoMap()[s.Node.RingID].Hostname,
						Payload: GetTaskLocationPayload{StageID: queryStageID},
					}
					json.NewEncoder(leaderConn).Encode(queryMsg)
					var locationResp GetTaskLocationResponse
					if err := json.NewDecoder(leaderConn).Decode(&locationResp); err == nil {
						if len(locationResp.Addresses) > 0 {
							targets = locationResp.Addresses // Update all targets
							// Recalculate target for this tuple
							targetIdx = hash % len(targets)
							target = targets[targetIdx]
							log.Printf("✅ [SOURCE] Updated target list from leader")
						}
					}
					leaderConn.Close()
				}

				// Re-establish connection to specific target
				newConn, err := net.DialTimeout("tcp", target, 3*time.Second)
				if err == nil {
					connections[target] = newConn
					encoders[target] = json.NewEncoder(newConn)
					decoders[target] = json.NewDecoder(newConn)
					log.Printf("✅ [SOURCE] Reconnected to %s", target)
					encoder = encoders[target]
					decoder = decoders[target]
					conn = newConn
				} else {
					log.Printf("⚠️  [SOURCE] Reconnect failed to %s: %v", target, err)
					time.Sleep(time.Duration(retryAttempt) * 500 * time.Millisecond)
					continue
				}
			}

			// Send Tuple
			if err := encoder.Encode(tuple); err != nil {
				log.Printf("⚠️  [SOURCE] Failed to send to %s: %v", target, err)
				// Force reconnect next time
				connections[target].Close()
				delete(connections, target)
				delete(encoders, target)
				delete(decoders, target)
				time.Sleep(time.Duration(retryAttempt) * 500 * time.Millisecond)
				continue
			}

			// Wait for Ack
			// Need to set read deadline on underlying conn, but json.Decoder buffers...
			// Assuming standard Go net/rpc behavior where blocking read is fine if we trust liveness
			// But we need timeout.
			// conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			// Note: Setting deadline might break Decoder if it buffers and we timeout mid-read?
			// Usually safe for full JSON object.

			// Since connection is shared in map, careful with concurrency (Source is single threaded per task)
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))

			var resp Tuple
			if err := decoder.Decode(&resp); err != nil {
				log.Printf("⚠️  [SOURCE] Failed to receive Ack from %s: %v", target, err)
				// Force reconnect
				conn.Close()
				delete(connections, target)
				delete(encoders, target)
				delete(decoders, target)
				time.Sleep(time.Duration(retryAttempt) * 500 * time.Millisecond)
				continue
			}

			// Clear deadline
			conn.SetReadDeadline(time.Time{})

			if resp.Type == "ack" && resp.ID == tuple.ID {
				success = true
				break
			} else {
				log.Printf("⚠️  [SOURCE] Unexpected response from %s: type=%s, id=%s (expected ack for %s)",
					target, resp.Type, resp.ID, tuple.ID)
			}
		}

		if !success {
			log.Printf("❌ [SOURCE] CRITICAL: Failed to deliver tuple %s to %s after retries", tuple.ID, target)
		} else {
			tuplesSent++
			// Log progress every 100 tuples
			if tuplesSent%100 == 0 {
				log.Printf("📊 [SOURCE] Sent %d tuples...", tuplesSent)
			}
		}
	}

	log.Printf("✅ [SOURCE] Task %s sent %d tuples, now sending EOF markers",
		task.ID, tuplesSent)

	// 5. Send EOF markers to all targets
	eofTuple := Tuple{
		Type:  "tuple",
		Key:   "EOF",
		Value: "end-of-stream",
		ID:    fmt.Sprintf("%s-eof", task.ID),
		IsEOF: true,
	}

	for target, encoder := range encoders {
		if err := encoder.Encode(eofTuple); err != nil {
			log.Printf("⚠️  [SOURCE] Failed to send EOF to %s: %v", target, err)
		} else {
			log.Printf("📡 [SOURCE] Sent EOF to %s", target)
		}
	}

	log.Printf("🏁 [SOURCE] TASK END: TaskID=%s, VM=%s, Status=SUCCESS, TuplesSent=%d",
		task.ID, task.AssignedWorker, tuplesSent)
	task.State = TaskStateCompleted

	// Report completion to leader
	s.ReportTaskCompleted(task, true, "")
}

// hashString computes a simple hash of a string for partitioning
func hashString(s string) int {
	hash := 0
	for _, c := range s {
		hash = int(c) + (hash << 6) + (hash << 16) - hash
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}

// HandleKillTaskWorker kills a task process on this worker
func (s *Server) HandleKillTaskWorker(msg RainStormMessage) {
	data, _ := json.Marshal(msg.Payload)
	var payload KillTaskPayload
	json.Unmarshal(data, &payload)

	log.Printf("💀 [WORKER] Kill request for PID %d", payload.PID)

	s.TaskProcessesMutex.Lock()
	cmd, exists := s.TaskProcesses[payload.PID]
	s.TaskProcessesMutex.Unlock()

	if !exists || cmd.Process == nil {
		log.Printf("⚠️  [WORKER] PID %d not found in active processes", payload.PID)
		// Try to kill anyway using OS signal
		process, err := os.FindProcess(payload.PID)
		if err == nil {
			process.Signal(syscall.SIGKILL)
			log.Printf("✅ [WORKER] Sent SIGKILL to PID %d", payload.PID)
		}
		return
	}

	// Kill the process
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		log.Printf("❌ [WORKER] Failed to kill PID %d: %v", payload.PID, err)
	} else {
		log.Printf("✅ [WORKER] Killed PID %d", payload.PID)
	}
}

// ReportTaskStarted notifies the leader that a task has started
func (s *Server) ReportTaskStarted(task *Task) {
	if s.Role == "worker" {
		msg := RainStormMessage{
			Type:    "task_started",
			Sender:  s.Node.Membership.GetInfoMap()[s.Node.RingID].Hostname,
			Payload: task,
		}

		// Get leader address (assume VM1 is leader)
		leaderAddr := net.JoinHostPort(s.Config.VMs[0], fmt.Sprintf("%d", s.Config.RainStormPort))

		conn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
		if err != nil {
			log.Printf("⚠️  [WORKER] Failed to report task start to leader: %v", err)
			return
		}
		defer conn.Close()

		encoder := json.NewEncoder(conn)
		if err := encoder.Encode(msg); err != nil {
			log.Printf("⚠️  [WORKER] Failed to send task_started: %v", err)
		}
	}
}

// ReportTaskCompleted notifies the leader that a task has finished
func (s *Server) ReportTaskCompleted(task *Task, success bool, errorMsg string) {
	payload := TaskCompletedPayload{
		TaskID:  task.ID,
		VM:      task.AssignedWorker,
		PID:     task.PID,
		OpExe:   task.OpExecutable,
		LogFile: task.LogFile,
		Success: success,
		Error:   errorMsg,
	}

	// If we ARE the leader, call HandleTaskCompleted directly
	if s.Role == "leader" {
		msg := RainStormMessage{
			Type:    "task_completed",
			Sender:  s.Node.Membership.GetInfoMap()[s.Node.RingID].Hostname,
			Payload: payload,
		}
		s.HandleTaskCompleted(msg)
		log.Printf("📤 [LEADER-WORKER] Reported task %s completion locally", task.ID)
		return
	}

	// Otherwise, send to leader over network
	msg := RainStormMessage{
		Type:    "task_completed",
		Sender:  s.Node.Membership.GetInfoMap()[s.Node.RingID].Hostname,
		Payload: payload,
	}

	// Get leader address (assume VM1 is leader)
	leaderAddr := net.JoinHostPort(s.Config.VMs[0], fmt.Sprintf("%d", s.Config.RainStormPort))

	conn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
	if err != nil {
		log.Printf("⚠️  [WORKER] Failed to report task completion to leader: %v", err)
		return
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		log.Printf("⚠️  [WORKER] Failed to send task_completed: %v", err)
	} else {
		log.Printf("📤 [WORKER] Reported task %s completion to leader", task.ID)
	}
}

// NotifyLeaderTaskFailed notifies the leader that a task has failed unexpectedly
// This is called by the process monitor goroutine when a task dies unexpectedly (e.g., kill -9)
func (s *Server) NotifyLeaderTaskFailed(task *Task, errorMsg string) {
	payload := TaskFailurePayload{
		TaskID:  task.ID,
		VM:      task.AssignedWorker,
		PID:     task.PID,
		OpExe:   task.OpExecutable,
		LogFile: task.LogFile,
		Error:   errorMsg,
	}

	msg := RainStormMessage{
		Type:    "task_failed",
		Sender:  s.Node.Membership.GetInfoMap()[s.Node.RingID].Hostname,
		Payload: payload,
	}

	// Get leader address (assume VM1 is leader)
	leaderAddr := net.JoinHostPort(s.Config.VMs[0], fmt.Sprintf("%d", s.Config.RainStormPort))

	conn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
	if err != nil {
		log.Printf("⚠️  [WORKER] Failed to notify leader of task failure: %v", err)
		return
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		log.Printf("⚠️  [WORKER] Failed to send task_failed: %v", err)
	} else {
		log.Printf("📤 [WORKER] Notified leader of task %s failure", task.ID)
	}
}

// reportSourceMetrics periodically reports processing rate to leader for source tasks
func (s *Server) reportSourceMetrics(task *Task, tuplesProcessed *int, startTime time.Time, stopChan chan bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	lastCount := 0
	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			currentCount := *tuplesProcessed
			processedThisSecond := currentCount - lastCount
			lastCount = currentCount

			elapsed := time.Since(startTime).Seconds()
			avgRate := 0.0
			if elapsed > 0 {
				avgRate = float64(currentCount) / elapsed
			}

			metrics := TaskMetrics{
				TaskID:          task.ID,
				TuplesProcessed: currentCount,
				CurrentRate:     float64(processedThisSecond),
				Timestamp:       time.Now().Unix(),
			}

			// Log locally
			log.Printf("📊 [TASK %s] Rate: %.2f tuples/sec (instant), %.2f tuples/sec (avg), Total: %d",
				task.ID, metrics.CurrentRate, avgRate, currentCount)

			// Report to leader
			s.sendMetricsToLeader(metrics)
		}
	}
}

// sendMetricsToLeader sends task metrics to the ResourceManager (leader)
func (s *Server) sendMetricsToLeader(metrics TaskMetrics) {
	// If we are the leader, handle metrics directly
	if s.Role == "leader" {
		s.MetricsMutex.Lock()
		existing, exists := s.TaskMetrics[metrics.TaskID]
		if !exists || metrics.Timestamp > existing.Timestamp {
			s.TaskMetrics[metrics.TaskID] = &metrics
		}
		s.MetricsMutex.Unlock()
		return
	}

	msg := RainStormMessage{
		Type:    "task_metrics",
		Sender:  s.Node.Membership.GetInfoMap()[s.Node.RingID].Hostname,
		Payload: TaskMetricsPayload{Metrics: metrics},
	}

	leaderAddr := net.JoinHostPort(s.Config.VMs[0], fmt.Sprintf("%d", s.Config.RainStormPort))
	conn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
	if err != nil {
		// Don't spam logs with connection failures
		return
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		// Silent failure for metrics reporting
	}
}
