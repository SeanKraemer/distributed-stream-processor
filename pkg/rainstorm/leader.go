package rainstorm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/membership"
	"net"
	"os"
	"strings"
	"time"
)

// HandleSubmitJob processes a job submission request
func (s *Server) HandleSubmitJob(msg RainStormMessage) {
	// 1. Parse Payload
	data, _ := json.Marshal(msg.Payload)
	var payload JobSubmitPayload
	json.Unmarshal(data, &payload)

	// Generate job timestamp for output directory
	jobTimestamp := time.Now().Format("20060102_150405")
	log.Printf("👑 [LEADER] ========== RUN START: %s ========== Time: %s",
		payload.App, time.Now().Format("2006-01-02 15:04:05"))
	log.Printf("👑 [LEADER] Received Job: App=%s, Src=%s, Dest=%s, Tasks=%d, OutputDir=rainstorm_outputs/%s",
		payload.App, payload.SrcFile, payload.DestFile, payload.NumTasks, jobTimestamp)

	// Clear old task metrics from previous jobs
	s.MetricsMutex.Lock()
	s.TaskMetrics = make(map[string]*TaskMetrics)
	s.MetricsMutex.Unlock()
	log.Printf("🧹 [LEADER] Cleared old task metrics for new job")

	// Create HyDFS destination file for output (required before appends)
	if payload.DestFile != "" {
		log.Printf("📝 [LEADER] Creating HyDFS output file: %s", payload.DestFile)
		// Use distributed HyDFS create via TCP to the local HyDFS handler
		// This ensures proper replication to the correct nodes based on consistent hashing
		err := s.createHyDFSFile(payload.DestFile)
		if err != nil {
			// File might already exist - that's OK for demos where we might rerun
			log.Printf("⚠️  [LEADER] HyDFS create returned: %v (continuing anyway)", err)
		} else {
			log.Printf("✅ [LEADER] Created HyDFS output file: %s", payload.DestFile)
		}
	}

	// 2. Validate: RainStorm supports no more than three processing stages
	// (Plus implicit Source stage 0 and any output handling)
	if len(payload.Stages) > 3 {
		log.Printf("❌ [LEADER] Job rejected: Too many stages (%d). RainStorm supports maximum 3 processing stages.", len(payload.Stages))
		return
	}

	// 2.1 Create Tasks for each stage
	// RainStorm supports specific stages. For the demo/spec, it's usually Source -> Op1 -> Op2 -> ... -> Output
	// The Spec says: "RainStorm supports no more than three processing stages."
	// Plus Source and Output.

	// Let's generate a list of tasks.
	var tasks []Task
	numTasks := payload.NumTasks

	// We need to assign ports. Let's assume a base range.
	// Ideally, we track used ports. For now, simple increment.
	basePort := 10000

	// Identify Alive Workers from MP2
	workers := s.GetAliveWorkers()
	if len(workers) == 0 {
		log.Printf("❌ [LEADER] No alive workers found! Cannot schedule job.")
		return
	}
	log.Printf("👑 [LEADER] Found %d alive workers", len(workers))

	workerIndex := 0

	// Stage 1: Source (Implicit or Explicit?)
	// Spec: "RainStorm must read input files from HyDFS and use them to produce the source stream... for the first stage"
	// So we create "Source Tasks".
	// Let's create `numTasks` Source Tasks.
	for i := 0; i < numTasks; i++ {
		worker := workers[workerIndex%len(workers)]
		workerIndex++

		task := Task{
			ID:             fmt.Sprintf("src-task-%d", i),
			StageID:        0,
			OpType:         OpSource,
			OpExecutable:   "internal_source",         // Special handler in worker
			OpArgs:         []string{payload.SrcFile}, // Arg is the source file
			Port:           basePort + i,
			AssignedWorker: worker,
			State:          TaskStateIdle,
			InputRate:      payload.InputRate, // Total INPUT_RATE (will be divided among tasks)
			TaskIndex:      i,
			TotalTasks:     numTasks,
			JobTimestamp:   jobTimestamp, // All tasks use same job folder
		}
		// Set RainStorm leader for metrics reporting (introducer = VMs[0])
		if len(s.Config.VMs) > 0 {
			task.RainStormLeader = net.JoinHostPort(s.Config.VMs[0], fmt.Sprintf("%d", s.Config.RainStormPort))
		}
		log.Printf("🔍 [LEADER DEBUG] Created source task: ID=%s, TaskIndex=%d, TotalTasks=%d", task.ID, task.TaskIndex, task.TotalTasks)
		tasks = append(tasks, task)
		basePort++
	}

	// User Stages
	lastStageID := len(payload.Stages)
	for stageIdx, opExe := range payload.Stages {
		// actualStageID is stageIdx + 1 (since 0 is Source)
		actualStageID := stageIdx + 1
		opArgs := []string{}
		if stageIdx < len(payload.StageArgs) {
			opArgs = payload.StageArgs[stageIdx]
		}

		isLastStage := actualStageID == lastStageID

		for i := 0; i < numTasks; i++ {
			worker := workers[workerIndex%len(workers)]
			workerIndex++

			task := Task{
				ID:             fmt.Sprintf("stage%d-task-%d", actualStageID, i),
				StageID:        actualStageID,
				OpType:         OpTransform, // Default, could be Filter/Agg based on config?
				OpExecutable:   opExe,
				OpArgs:         opArgs,
				Port:           basePort + i,
				AssignedWorker: worker,
				State:          TaskStateIdle,
				NumSources:     numTasks,     // All stages need to track EOF from upstream tasks
				JobTimestamp:   jobTimestamp, // All tasks use same job folder
			}

			// ALL stages need HyDFS leader for exactly-once checkpointing
			// This enables state recovery on task restart
			// Use NodePort (8000) which accepts JSON protocol for get_file
			if len(workers) > 0 {
				task.HyDFSLeader = net.JoinHostPort(workers[0], fmt.Sprintf("%d", s.Config.NodePort))
			}

			// Set RainStorm leader for metrics reporting (introducer = VMs[0])
			if len(s.Config.VMs) > 0 {
				task.RainStormLeader = net.JoinHostPort(s.Config.VMs[0], fmt.Sprintf("%d", s.Config.RainStormPort))
			}

			// Set output file and HyDFS destination for last stage tasks (sink stage)
			// Use rainstorm_outputs/{timestamp}/ for organized local output
			if isLastStage && payload.DestFile != "" {
				task.OutputFile = fmt.Sprintf("rainstorm_outputs/%s/output_%s_task%d.txt", jobTimestamp, payload.App, i)
				task.HyDFSDestFile = payload.DestFile
			}

			tasks = append(tasks, task)
			basePort++
		}
	}

	// 2.5 Build Routing Table
	// Map StageID -> List of "Host:Port" addresses
	routingTable := make(map[int][]string)

	for _, task := range tasks {
		addr := net.JoinHostPort(task.AssignedWorker, fmt.Sprintf("%d", task.Port))
		routingTable[task.StageID] = append(routingTable[task.StageID], addr)
	}

	// Track job info for completion detection
	s.JobMutex.Lock()
	s.CurrentJobName = payload.App
	s.CurrentJobDestFile = payload.DestFile
	s.JobTimestamp = jobTimestamp
	s.JobStartTime = time.Now()
	s.ExpectedTasks = len(tasks)
	s.OriginalTaskCount = len(tasks) // Track original count for autoscaling EOF logic
	s.CompletedTasks = 0
	s.CompletedTaskIDs = make(map[string]bool) // Reset completed task tracking for new job
	s.UpscaledTaskIDs = make(map[string]bool)  // Track upscaled tasks separately
	s.OriginalTasksDone = false                // Reset for new job
	s.JobEnded = false                         // Reset job ended flag for new job
	s.OutputTaskFiles = make(map[string]string)
	// Track which tasks produce output (sink tasks with OutputFile set)
	for _, task := range tasks {
		if task.OutputFile != "" {
			s.OutputTaskFiles[task.ID] = task.OutputFile
		}
	}
	// Store routing table for task restart on failure
	s.CurrentRoutingTable = routingTable
	s.JobMutex.Unlock()

	// Store autoscaling configuration
	s.AutoScaleMutex.Lock()
	s.AutoScaleEnabled = payload.AutoScale
	s.LowWatermark = payload.LowWatermark
	s.HighWatermark = payload.HighWatermark
	s.StageTaskCount = make(map[int]int)
	s.LastScaleTime = make(map[int]time.Time)
	s.LowRateStart = make(map[int]time.Time) // Track sustained low rates per stage

	// Initialize stage task counts and store stage configs for spawning new tasks
	s.StageConfigs = make([]StageConfig, 0)
	for stageIdx, opExe := range payload.Stages {
		actualStageID := stageIdx + 1
		s.StageTaskCount[actualStageID] = numTasks

		opArgs := []string{}
		if stageIdx < len(payload.StageArgs) {
			opArgs = payload.StageArgs[stageIdx]
		}

		isLastStage := actualStageID == len(payload.Stages)

		s.StageConfigs = append(s.StageConfigs, StageConfig{
			StageID:       actualStageID,
			OpType:        OpTransform,
			OpExecutable:  opExe,
			OpArgs:        opArgs,
			BasePort:      basePort + (stageIdx * 100), // Reserve port range per stage
			IsSink:        isLastStage,
			HyDFSDestFile: payload.DestFile,
		})
	}

	// Stop any previous autoscale monitor
	if s.AutoScaleStopChan != nil {
		close(s.AutoScaleStopChan)
	}
	s.AutoScaleStopChan = make(chan struct{})
	s.AutoScaleMutex.Unlock()

	// Start autoscale monitor if enabled
	if payload.AutoScale {
		log.Printf("⚖️  [RM] Autoscaling ENABLED: LW=%d, HW=%d tuples/sec per task",
			payload.LowWatermark, payload.HighWatermark)
		go s.autoscaleMonitor()
	}

	// 3. Send Schedule Commands to Workers
	for _, task := range tasks {
		s.sendScheduleTask(task, routingTable)
	}

	log.Printf("👑 [LEADER] Scheduled %d tasks across %d workers", len(tasks), len(workers))
	// Note: RUN END will be logged when all tasks complete (see HandleTaskCompleted)
}

// GetAliveWorkers returns a list of hostnames of alive worker nodes (excluding the leader)
// Per MP4 spec: "One of them is the leader, and the remaining N-1 are worker servers.
// The tasks are scheduled on a worker machine."
func (s *Server) GetAliveWorkers() []string {
	infoMap := s.Node.Membership.GetInfoMap()
	leaderHostname := infoMap[s.Node.RingID].Hostname // This node is the leader
	var workers []string
	for _, info := range infoMap {
		if info.State == membership.Alive && info.Hostname != leaderHostname {
			// Exclude the leader - only include worker VMs
			workers = append(workers, info.Hostname)
		}
	}
	return workers
}

// sendScheduleTask sends the task to the assigned worker
func (s *Server) sendScheduleTask(task Task, routingTable map[int][]string) {
	msg := RainStormMessage{
		Type:   "schedule_task",
		Sender: s.Node.Membership.GetInfoMap()[s.Node.RingID].Hostname,
		Payload: ScheduleTaskPayload{
			Task:         task,
			RoutingTable: routingTable,
		},
	}

	// Connect to Worker's RainStorm port
	// We need to find the port. Config has ranges, but here we need the *server* port of that worker.
	// Assumption: All nodes run on the same configured RainStormPort.
	targetAddr := net.JoinHostPort(task.AssignedWorker, fmt.Sprintf("%d", s.Config.RainStormPort))

	conn, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
	if err != nil {
		log.Printf("❌ [LEADER] Failed to connect to worker %s: %v", targetAddr, err)
		return
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		log.Printf("❌ [LEADER] Failed to send task %s to %s: %v", task.ID, task.AssignedWorker, err)
	} else {
		log.Printf("📤 [LEADER] Sent task %s to %s", task.ID, task.AssignedWorker)

		// Track task on leader
		s.AllTasksMutex.Lock()
		taskCopy := task
		s.AllTasks[task.ID] = &taskCopy
		s.AllTasksMutex.Unlock()
	}
}

// HandleListTasks returns information about all tasks
func (s *Server) HandleListTasks(conn net.Conn, msg RainStormMessage) {
	s.AllTasksMutex.RLock()
	defer s.AllTasksMutex.RUnlock()

	var tasks []TaskInfo
	for _, task := range s.AllTasks {
		stateStr := "unknown"
		switch task.State {
		case TaskStateIdle:
			stateStr = "idle"
		case TaskStateRunning:
			stateStr = "running"
		case TaskStateFailed:
			stateStr = "failed"
		case TaskStateCompleted:
			stateStr = "completed"
		}

		tasks = append(tasks, TaskInfo{
			TaskID:  task.ID,
			VM:      task.AssignedWorker,
			PID:     task.PID,
			OpExe:   task.OpExecutable,
			LogFile: task.LogFile,
			State:   stateStr,
		})
	}

	response := ListTasksResponse{Tasks: tasks}
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(response); err != nil {
		log.Printf("❌ [LEADER] Failed to send list_tasks response: %v", err)
	} else {
		log.Printf("📋 [LEADER] Sent list of %d tasks", len(tasks))
	}
}

// HandleKillTask kills a specific task by VM and PID
func (s *Server) HandleKillTask(msg RainStormMessage) {
	data, _ := json.Marshal(msg.Payload)
	var payload KillTaskPayload
	json.Unmarshal(data, &payload)

	log.Printf("💀 [LEADER] Kill task request: VM=%s, PID=%d", payload.VM, payload.PID)

	// Forward kill command to the worker
	killMsg := RainStormMessage{
		Type:    "kill_task_worker",
		Sender:  s.Node.Membership.GetInfoMap()[s.Node.RingID].Hostname,
		Payload: payload,
	}

	targetAddr := net.JoinHostPort(payload.VM, fmt.Sprintf("%d", s.Config.RainStormPort))
	conn, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
	if err != nil {
		log.Printf("❌ [LEADER] Failed to connect to worker %s: %v", targetAddr, err)
		return
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(killMsg); err != nil {
		log.Printf("❌ [LEADER] Failed to send kill command to %s: %v", payload.VM, err)
	} else {
		log.Printf("✅ [LEADER] Sent kill command to %s for PID %d", payload.VM, payload.PID)
	}
}

// forwardKillToWorker sends a kill command to a specific worker for a given PID
func (s *Server) forwardKillToWorker(vm string, pid int) {
	killMsg := RainStormMessage{
		Type:   "kill_task_worker",
		Sender: s.Node.Membership.GetInfoMap()[s.Node.RingID].Hostname,
		Payload: KillTaskPayload{
			VM:  vm,
			PID: pid,
		},
	}

	targetAddr := net.JoinHostPort(vm, fmt.Sprintf("%d", s.Config.RainStormPort))
	conn, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
	if err != nil {
		log.Printf("❌ [RM] Failed to connect to worker %s: %v", targetAddr, err)
		return
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(killMsg); err != nil {
		log.Printf("❌ [RM] Failed to send kill command to %s: %v", vm, err)
	} else {
		log.Printf("✅ [RM] Sent kill command to %s for PID %d", vm, pid)
	}
}

// HandleTaskStarted updates task info when worker reports task has started
func (s *Server) HandleTaskStarted(msg RainStormMessage) {
	data, _ := json.Marshal(msg.Payload)
	var task Task
	json.Unmarshal(data, &task)

	s.AllTasksMutex.Lock()
	if existingTask, ok := s.AllTasks[task.ID]; ok {
		existingTask.PID = task.PID
		existingTask.LogFile = task.LogFile
		existingTask.State = TaskStateRunning
		log.Printf("📍 [LEADER] TASK START: TaskID=%s, VM=%s, PID=%d, OpExe=%s, LogFile=%s, Time=%s",
			task.ID, existingTask.AssignedWorker, task.PID, existingTask.OpExecutable,
			task.LogFile, time.Now().Format("2006-01-02 15:04:05"))
	}
	s.AllTasksMutex.Unlock()
}

// HandleTaskMetrics receives and aggregates metrics from worker tasks
func (s *Server) HandleTaskMetrics(msg RainStormMessage) {
	data, _ := json.Marshal(msg.Payload)
	var payload TaskMetricsPayload
	json.Unmarshal(data, &payload)

	metrics := payload.Metrics

	// Update metrics map
	s.MetricsMutex.Lock()
	s.TaskMetrics[metrics.TaskID] = &metrics
	s.MetricsMutex.Unlock()

	// Log individual task rate
	log.Printf("📊 [RM] Task %s: %.2f tuples/sec, Total: %d",
		metrics.TaskID, metrics.CurrentRate, metrics.TuplesProcessed)

	// Aggregate by stage and log
	s.aggregateAndLogStageMetrics()
}

// aggregateAndLogStageMetrics calculates total throughput per stage
func (s *Server) aggregateAndLogStageMetrics() {
	s.MetricsMutex.RLock()
	defer s.MetricsMutex.RUnlock()

	// Group metrics by stage
	stageMetrics := make(map[int][]TaskMetrics)

	s.AllTasksMutex.RLock()
	for taskID, metrics := range s.TaskMetrics {
		if task, exists := s.AllTasks[taskID]; exists {
			stageMetrics[task.StageID] = append(stageMetrics[task.StageID], *metrics)
		}
	}
	s.AllTasksMutex.RUnlock()

	// Log aggregated rates per stage
	for stageID, metricsSlice := range stageMetrics {
		if len(metricsSlice) == 0 {
			continue
		}

		totalRate := 0.0
		totalTuples := 0
		for _, m := range metricsSlice {
			totalRate += m.CurrentRate
			totalTuples += m.TuplesProcessed
		}

		log.Printf("📊 [RM] Stage %d: %.2f tuples/sec total (%d tasks, %d total tuples)",
			stageID, totalRate, len(metricsSlice), totalTuples)
	}
}

// autoscaleMonitor runs in the background and checks stage rates every second
// It scales up if avg rate per task > HW, scales down if < LW
func (s *Server) autoscaleMonitor() {
	log.Printf("⚖️  [RM] Autoscale monitor started")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Cooldown period between scale events (5 seconds as per spec)
	scaleCooldown := 5 * time.Second

	for {
		select {
		case <-s.AutoScaleStopChan:
			log.Printf("⚖️  [RM] Autoscale monitor stopped")
			return
		case <-ticker.C:
			s.checkAndScale(scaleCooldown)
		}
	}
}

// checkAndScale evaluates each stage and triggers scaling if needed
func (s *Server) checkAndScale(cooldown time.Duration) {
	s.AutoScaleMutex.RLock()
	if !s.AutoScaleEnabled {
		s.AutoScaleMutex.RUnlock()
		return
	}
	lw := s.LowWatermark
	hw := s.HighWatermark
	s.AutoScaleMutex.RUnlock()

	// WARMUP: Don't make scaling decisions in the first 10 seconds after job start
	// This allows tasks to initialize, connect to downstream, and start processing
	s.JobMutex.RLock()
	jobStartTime := s.JobStartTime
	s.JobMutex.RUnlock()

	warmupPeriod := 10 * time.Second
	if time.Since(jobStartTime) < warmupPeriod {
		return // Still in warmup period
	}

	// Collect metrics per stage
	s.MetricsMutex.RLock()
	stageRates := make(map[int]float64) // stageID -> total rate
	stageTasks := make(map[int]int)     // stageID -> task count with metrics
	stageTuples := make(map[int]int)    // stageID -> total tuples processed

	s.AllTasksMutex.RLock()
	for taskID, metrics := range s.TaskMetrics {
		if task, exists := s.AllTasks[taskID]; exists {
			// Only consider running user stages (not source stage 0)
			// Also require that the task has actually processed some data
			if task.StageID > 0 && task.State == TaskStateRunning && metrics.TuplesProcessed > 0 {
				stageRates[task.StageID] += metrics.CurrentRate
				stageTasks[task.StageID]++
				stageTuples[task.StageID] += metrics.TuplesProcessed
			}
		}
	}
	s.AllTasksMutex.RUnlock()
	s.MetricsMutex.RUnlock()

	// Check each stage for scaling
	for stageID, totalRate := range stageRates {
		taskCount := stageTasks[stageID]
		if taskCount == 0 {
			continue
		}

		// Require minimum tuples processed before making scaling decisions
		// This prevents scaling based on initial warmup data
		if stageTuples[stageID] < 50 {
			continue // Not enough data to make a decision
		}

		avgRatePerTask := totalRate / float64(taskCount)

		// Check cooldown
		s.AutoScaleMutex.RLock()
		lastScale, hasLastScale := s.LastScaleTime[stageID]
		currentTaskCount := s.StageTaskCount[stageID]
		s.AutoScaleMutex.RUnlock()

		if hasLastScale && time.Since(lastScale) < cooldown {
			continue // Still in cooldown
		}

		// Scale UP: avg rate per task > HW
		// NOTE: Don't upscale Stage 1 - source has already distributed work to original tasks
		// New Stage 1 tasks would have no input and fail endlessly
		if avgRatePerTask > float64(hw) && stageID > 1 {
			log.Printf("⚖️  [RM] UPSCALE TRIGGERED: Stage %d avgRate=%.2f > HW=%d, tasks=%d->%d",
				stageID, avgRatePerTask, hw, currentTaskCount, currentTaskCount+1)
			s.scaleStage(stageID, 1) // Add 1 task
			// Reset low rate tracking since we're scaling up
			s.AutoScaleMutex.Lock()
			delete(s.LowRateStart, stageID)
			s.AutoScaleMutex.Unlock()
			return // Only one scaling action per check
		} else if avgRatePerTask > float64(hw) && stageID == 1 {
			log.Printf("ℹ️  [RM] Skipping upscale for Stage 1 (source already distributed work)")
		}

		// Scale DOWN: avg rate per task < LW (sustained for 3+ seconds)
		// Require sustained low rate to prevent aggressive downscaling during transient dips
		// NOTE: Don't downscale Stage 1 either - same reasoning
		if avgRatePerTask < float64(lw) && currentTaskCount > 1 && totalRate > 0.1 && stageID > 1 {
			s.AutoScaleMutex.Lock()
			lowRateStart, exists := s.LowRateStart[stageID]
			if !exists {
				// First time below threshold - start tracking
				s.LowRateStart[stageID] = time.Now()
				s.AutoScaleMutex.Unlock()
				log.Printf("⏱️  [RM] Stage %d below LW (avgRate=%.2f < %d), starting sustained check", stageID, avgRatePerTask, lw)
			} else {
				// Check if sustained for at least 3 seconds
				if time.Since(lowRateStart) >= 3*time.Second {
					delete(s.LowRateStart, stageID) // Reset for next downscale
					s.AutoScaleMutex.Unlock()
					log.Printf("⚖️  [RM] DOWNSCALE TRIGGERED: Stage %d avgRate=%.2f < LW=%d (sustained 3+ sec), tasks=%d->%d",
						stageID, avgRatePerTask, lw, currentTaskCount, currentTaskCount-1)
					s.scaleStage(stageID, -1) // Remove 1 task
					return                    // Only one scaling action per check
				}
				s.AutoScaleMutex.Unlock()
			}
		} else {
			// Rate is above threshold or only 1 task - reset tracking
			s.AutoScaleMutex.Lock()
			delete(s.LowRateStart, stageID)
			s.AutoScaleMutex.Unlock()
		}
	}
}

// scaleStage adds or removes tasks from a stage
// delta: +1 to add a task, -1 to remove a task
func (s *Server) scaleStage(stageID int, delta int) {
	s.AutoScaleMutex.Lock()
	defer s.AutoScaleMutex.Unlock()

	currentCount := s.StageTaskCount[stageID]
	newCount := currentCount + delta

	if newCount < 1 {
		log.Printf("⚖️  [RM] Cannot scale Stage %d below 1 task", stageID)
		return
	}

	s.StageTaskCount[stageID] = newCount
	s.LastScaleTime[stageID] = time.Now()

	if delta > 0 {
		// Add a new task
		go s.addTaskToStage(stageID, newCount-1) // Task index is newCount-1
	} else {
		// Remove a task (kill the last one)
		go s.removeTaskFromStage(stageID, currentCount-1) // Remove task with highest index
	}
}

// addTaskToStage spawns a new task for the given stage
func (s *Server) addTaskToStage(stageID int, taskIndex int) {
	// Find stage config
	var stageConfig *StageConfig
	for i := range s.StageConfigs {
		if s.StageConfigs[i].StageID == stageID {
			stageConfig = &s.StageConfigs[i]
			break
		}
	}

	if stageConfig == nil {
		log.Printf("❌ [RM] Cannot find config for stage %d", stageID)
		return
	}

	// Get a worker to assign the task
	workers := s.GetAliveWorkers()
	if len(workers) == 0 {
		log.Printf("❌ [RM] No alive workers to add task")
		return
	}

	// Pick worker with fewest tasks (simple load balancing)
	worker := workers[taskIndex%len(workers)]

	// Get job info
	s.JobMutex.RLock()
	jobTimestamp := s.JobTimestamp
	routingTable := s.CurrentRoutingTable
	numSources := len(routingTable[0]) // Source stage task count
	s.JobMutex.RUnlock()

	// Create the new task
	taskPort := stageConfig.BasePort + taskIndex + 1000 // Offset to avoid conflicts
	task := Task{
		ID:             fmt.Sprintf("stage%d-task-%d", stageID, taskIndex),
		StageID:        stageID,
		OpType:         stageConfig.OpType,
		OpExecutable:   stageConfig.OpExecutable,
		OpArgs:         stageConfig.OpArgs,
		Port:           taskPort,
		AssignedWorker: worker,
		State:          TaskStateIdle,
		NumSources:     numSources,
		JobTimestamp:   jobTimestamp,
	}

	// Set HyDFS leader for checkpointing
	if len(workers) > 0 {
		task.HyDFSLeader = net.JoinHostPort(workers[0], fmt.Sprintf("%d", s.Config.NodePort))
	}

	// Set RainStorm leader for metrics reporting (introducer = VMs[0])
	if len(s.Config.VMs) > 0 {
		task.RainStormLeader = net.JoinHostPort(s.Config.VMs[0], fmt.Sprintf("%d", s.Config.RainStormPort))
	}

	// Set output file for sink stage
	if stageConfig.IsSink && stageConfig.HyDFSDestFile != "" {
		task.OutputFile = fmt.Sprintf("rainstorm_outputs/%s/output_main_task%d.txt", jobTimestamp, taskIndex)
		task.HyDFSDestFile = stageConfig.HyDFSDestFile
	}

	// Add to task tracking
	s.AllTasksMutex.Lock()
	s.AllTasks[task.ID] = &task
	s.AllTasksMutex.Unlock()

	// Increment expected task count and mark as upscaled
	s.JobMutex.Lock()
	s.ExpectedTasks++
	if s.UpscaledTaskIDs == nil {
		s.UpscaledTaskIDs = make(map[string]bool)
	}
	s.UpscaledTaskIDs[task.ID] = true
	log.Printf("📝 [RM] Incremented ExpectedTasks to %d for upscaled task %s", s.ExpectedTasks, task.ID)
	s.JobMutex.Unlock()

	// Update routing table
	s.JobMutex.Lock()
	newAddr := net.JoinHostPort(worker, fmt.Sprintf("%d", taskPort))
	s.CurrentRoutingTable[stageID] = append(s.CurrentRoutingTable[stageID], newAddr)
	updatedRouting := s.CurrentRoutingTable
	s.JobMutex.Unlock()

	// Notify upstream tasks about new downstream target
	s.broadcastRoutingUpdate(stageID, updatedRouting)

	// Schedule the new task
	s.sendScheduleTask(task, updatedRouting)

	log.Printf("⚖️  [RM] ADDED Task %s on %s:%d", task.ID, worker, taskPort)
}

// removeTaskFromStage gracefully stops a task from the given stage
// FIXED: Now updates routing table FIRST, sends EOF, waits, then kills
func (s *Server) removeTaskFromStage(stageID int, taskIndex int) {
	taskID := fmt.Sprintf("stage%d-task-%d", stageID, taskIndex)

	s.AllTasksMutex.RLock()
	task, exists := s.AllTasks[taskID]
	if !exists {
		s.AllTasksMutex.RUnlock()
		log.Printf("⚖️  [RM] Task %s not found for removal", taskID)
		return
	}
	worker := task.AssignedWorker
	taskPort := task.Port
	numSources := task.NumSources
	s.AllTasksMutex.RUnlock()

	// Step 1: Remove from routing table FIRST (before killing)
	s.JobMutex.Lock()
	addrs := s.CurrentRoutingTable[stageID]
	targetAddr := net.JoinHostPort(worker, fmt.Sprintf("%d", taskPort))
	newAddrs := make([]string, 0)
	for _, addr := range addrs {
		if addr != targetAddr {
			newAddrs = append(newAddrs, addr)
		}
	}
	s.CurrentRoutingTable[stageID] = newAddrs
	updatedRouting := make(map[int][]string)
	for k, v := range s.CurrentRoutingTable {
		updatedRouting[k] = v
	}
	// Mark task as completed preemptively to avoid waiting for it
	if !s.CompletedTaskIDs[taskID] {
		s.CompletedTaskIDs[taskID] = true
		s.CompletedTasks++
		log.Printf("📝 [RM] Marking killed task %s as completed (%d/%d)", taskID, s.CompletedTasks, s.ExpectedTasks)
	}
	s.JobMutex.Unlock()

	// Step 2: Notify upstream tasks about removed downstream target BEFORE sending EOF
	s.broadcastRoutingUpdate(stageID, updatedRouting)

	// Step 3: Wait for routing updates to propagate
	time.Sleep(2000 * time.Millisecond)

	// Step 4: Send artificial EOF markers to gracefully shutdown the task
	s.sendEOFToTask(worker, taskPort, numSources, taskID)

	// Step 5: Wait for task to exit gracefully, force kill after timeout
	go func() {
		time.Sleep(15 * time.Second) // Grace period (matches qz-dev stable implementation)
		if !s.isTaskCompleted(taskID) {
			log.Printf("⚠️  [RM] Task %s didn't exit after EOF, force killing", taskID)
			s.AllTasksMutex.RLock()
			t, exists := s.AllTasks[taskID]
			pid := 0
			if exists {
				pid = t.PID
			}
			s.AllTasksMutex.RUnlock()
			if pid > 0 {
				s.forwardKillToWorker(worker, pid)
			}
		}
		// Remove from task tracking after grace period
		s.AllTasksMutex.Lock()
		delete(s.AllTasks, taskID)
		s.AllTasksMutex.Unlock()
	}()

	log.Printf("⚖️  [RM] REMOVED Task %s from %s (graceful shutdown initiated)", taskID, worker)
}

// sendEOFToTask sends artificial EOF markers to a task to trigger graceful shutdown
func (s *Server) sendEOFToTask(worker string, port int, numSources int, taskID string) {
	log.Printf("📨 [RM] Sending %d artificial EOF markers to task %s for graceful shutdown", numSources, taskID)

	taskAddr := net.JoinHostPort(worker, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", taskAddr, 3*time.Second)
	if err != nil {
		log.Printf("⚠️  [RM] Failed to connect to task %s for EOF: %v", taskID, err)
		return
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)

	// Send NumSources EOF tuples (task expects one from each upstream task)
	for i := 0; i < numSources; i++ {
		eofTuple := map[string]interface{}{
			"key":    fmt.Sprintf("shutdown-%d", i),
			"value":  "EOF",
			"id":     fmt.Sprintf("eof-%s-%d-%d", taskID, i, time.Now().UnixNano()),
			"is_eof": true,
		}

		if err := encoder.Encode(eofTuple); err != nil {
			log.Printf("⚠️  [RM] Failed to send EOF %d/%d to task %s: %v", i+1, numSources, taskID, err)
		}
	}

	log.Printf("✅ [RM] Sent %d EOF markers to task %s (expecting graceful shutdown)", numSources, taskID)
}

// isTaskCompleted checks if a task has reported completion
func (s *Server) isTaskCompleted(taskID string) bool {
	s.JobMutex.RLock()
	defer s.JobMutex.RUnlock()
	return s.CompletedTaskIDs[taskID]
}

// broadcastRoutingUpdate notifies upstream tasks about routing changes
// This ensures tuples are redistributed to include/exclude the scaled task
func (s *Server) broadcastRoutingUpdate(changedStageID int, routingTable map[int][]string) {
	upstreamStageID := changedStageID - 1
	if upstreamStageID < 0 {
		return // No upstream for source stage
	}

	s.AllTasksMutex.RLock()
	defer s.AllTasksMutex.RUnlock()

	for _, task := range s.AllTasks {
		if task.StageID == upstreamStageID && task.State == TaskStateRunning {
			// Send routing update to this task
			go s.sendRoutingUpdate(task, routingTable)
		}
	}
}

// sendRoutingUpdate sends updated routing table to a task
// Sends as a Tuple with type "routing_update" so operator can handle it
func (s *Server) sendRoutingUpdate(task *Task, routingTable map[int][]string) {
	addr := net.JoinHostPort(task.AssignedWorker, fmt.Sprintf("%d", task.Port))
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		log.Printf("⚠️  [RM] Failed to send routing update to %s: %v", task.ID, err)
		return
	}
	defer conn.Close()

	// Get the downstream targets for this task's next stage
	downstreamStageID := task.StageID + 1
	targets := routingTable[downstreamStageID]

	// Send as a Tuple (same format operator expects)
	tuple := Tuple{
		Type:  "routing_update",
		Key:   fmt.Sprintf("%d", downstreamStageID),
		Value: strings.Join(targets, ","),
		ID:    fmt.Sprintf("routing-%d-%d", downstreamStageID, time.Now().UnixNano()),
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(tuple); err != nil {
		log.Printf("⚠️  [RM] Failed to encode routing update for %s: %v", task.ID, err)
	} else {
		log.Printf("📡 [RM] Sent routing update to %s: %d targets for stage %d", task.ID, len(targets), downstreamStageID)
	}
}

// TaskCompletedPayload is the payload for task_completed messages
type TaskCompletedPayload struct {
	TaskID  string `json:"task_id"`
	VM      string `json:"vm"`
	PID     int    `json:"pid"`
	OpExe   string `json:"op_exe"`
	LogFile string `json:"log_file"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// HandleTaskCompleted processes task completion notifications from workers
func (s *Server) HandleTaskCompleted(msg RainStormMessage) {
	data, _ := json.Marshal(msg.Payload)
	var payload TaskCompletedPayload
	json.Unmarshal(data, &payload)

	// Log TASK END
	status := "SUCCESS"
	if !payload.Success {
		status = fmt.Sprintf("FAILED: %s", payload.Error)
	}
	log.Printf("🏁 [LEADER] TASK END: TaskID=%s, VM=%s, PID=%d, OpExe=%s, LogFile=%s, Status=%s, Time=%s",
		payload.TaskID, payload.VM, payload.PID, payload.OpExe, payload.LogFile, status,
		time.Now().Format("2006-01-02 15:04:05"))

	// Update task state
	s.AllTasksMutex.Lock()
	if task, ok := s.AllTasks[payload.TaskID]; ok {
		if payload.Success {
			task.State = TaskStateCompleted
		} else {
			task.State = TaskStateFailed
		}
	}
	s.AllTasksMutex.Unlock()

	// Track completion for RUN END (with duplicate detection)
	s.JobMutex.Lock()

	// Check if this task was already counted
	if s.CompletedTaskIDs == nil {
		s.CompletedTaskIDs = make(map[string]bool)
	}

	alreadyCounted := s.CompletedTaskIDs[payload.TaskID]
	isUpscaledTask := s.UpscaledTaskIDs[payload.TaskID]
	if !alreadyCounted {
		s.CompletedTaskIDs[payload.TaskID] = true
		s.CompletedTasks++
	}

	completed := s.CompletedTasks
	expected := s.ExpectedTasks
	originalCount := s.OriginalTaskCount
	originalDone := s.OriginalTasksDone
	jobName := s.CurrentJobName
	destFile := s.CurrentJobDestFile
	outputFiles := make(map[string]string)
	for k, v := range s.OutputTaskFiles {
		outputFiles[k] = v
	}
	startTime := s.JobStartTime

	// Count how many original (non-upscaled) tasks have completed
	originalCompleted := 0
	for taskID := range s.CompletedTaskIDs {
		if !s.UpscaledTaskIDs[taskID] {
			originalCompleted++
		}
	}

	// Check if all original tasks just completed (trigger EOF to upscaled tasks)
	shouldSendEOFToUpscaled := false
	if originalCompleted >= originalCount && !originalDone {
		s.OriginalTasksDone = true
		shouldSendEOFToUpscaled = true
	}

	// Collect upscaled task info for EOF sending
	var upscaledTasks []Task
	if shouldSendEOFToUpscaled {
		s.AllTasksMutex.RLock()
		for taskID := range s.UpscaledTaskIDs {
			if task, ok := s.AllTasks[taskID]; ok && task.State != TaskStateCompleted {
				upscaledTasks = append(upscaledTasks, *task)
			}
		}
		s.AllTasksMutex.RUnlock()
	}

	s.JobMutex.Unlock()

	if !alreadyCounted {
		log.Printf("📈 [LEADER] Task completion: %d/%d tasks done (original: %d/%d, upscaled: %v)",
			completed, expected, originalCompleted, originalCount, isUpscaledTask)
	} else {
		log.Printf("⚠️  [LEADER] Duplicate completion for task %s (already counted)", payload.TaskID)
	}

	// Send EOF to upscaled tasks when all original tasks complete
	if shouldSendEOFToUpscaled && len(upscaledTasks) > 0 {
		log.Printf("📨 [LEADER] All %d original tasks done, sending EOF to %d upscaled tasks",
			originalCount, len(upscaledTasks))
		for _, task := range upscaledTasks {
			go s.sendEOFToTask(task.AssignedWorker, task.Port, task.NumSources, task.ID)
		}
	}

	// Check if all tasks are done (only log RUN END once)
	s.JobMutex.Lock()
	alreadyEnded := s.JobEnded
	if completed >= expected && !alreadyEnded {
		s.JobEnded = true
	}
	s.JobMutex.Unlock()

	if completed >= expected && !alreadyEnded {
		duration := time.Since(startTime)
		log.Printf("🏁 [LEADER] ========== RUN END: %s ========== Time: %s, Duration: %s",
			jobName, time.Now().Format("2006-01-02 15:04:05"), duration.Round(time.Second))

		// Collect output from sink tasks and merge to HyDFS
		if destFile != "" && len(outputFiles) > 0 {
			go s.collectAndMergeOutput(destFile, outputFiles)
		}
	}
}

// HandleTaskFailure processes task failure notifications from workers
// This implements the "Snitch Pattern" for task-level failure detection
func (s *Server) HandleTaskFailure(msg RainStormMessage) {
	data, _ := json.Marshal(msg.Payload)
	var payload TaskFailurePayload
	json.Unmarshal(data, &payload)

	log.Printf("💀 [LEADER] TASK FAILED: %s on %s (PID=%d, Error=%s)",
		payload.TaskID, payload.VM, payload.PID, payload.Error)

	// Check if job has already ended - don't restart tasks after job completion
	s.JobMutex.RLock()
	jobEnded := s.JobEnded
	s.JobMutex.RUnlock()

	if jobEnded {
		log.Printf("⚠️  [LEADER] Ignoring task failure for %s - job already ended", payload.TaskID)
		return
	}

	// Update task state
	s.AllTasksMutex.Lock()
	task, exists := s.AllTasks[payload.TaskID]
	if !exists {
		s.AllTasksMutex.Unlock()
		log.Printf("⚠️  [LEADER] Failed task %s not found in AllTasks", payload.TaskID)
		return
	}
	task.State = TaskStateFailed
	s.AllTasksMutex.Unlock()

	// Get routing table for restart
	s.JobMutex.RLock()
	routingTable := s.CurrentRoutingTable
	s.JobMutex.RUnlock()

	if routingTable == nil {
		log.Printf("❌ [LEADER] No routing table available for task restart")
		return
	}

	// Select a healthy VM for restart (can be same VM - process is dead anyway)
	workers := s.GetAliveWorkers()
	if len(workers) == 0 {
		log.Printf("❌ [LEADER] No alive workers available for task restart")
		return
	}

	// Use the first available worker (simple strategy)
	newWorker := workers[0]

	// CRITICAL: Preserve task identity (same TaskID) so it can find its checkpoint
	// Create a new Task struct with the same ID and configuration
	restartTask := Task{
		ID:              task.ID, // SAME TaskID - critical for checkpoint recovery
		StageID:         task.StageID,
		OpType:          task.OpType,
		OpExecutable:    task.OpExecutable,
		OpArgs:          task.OpArgs,
		Port:            task.Port, // Keep same port for routing table
		AssignedWorker:  newWorker, // New worker (may be same as before)
		State:           TaskStateIdle,
		InputRate:       task.InputRate,
		TaskIndex:       task.TaskIndex,
		TotalTasks:      task.TotalTasks,
		OutputFile:      task.OutputFile,
		NumSources:      task.NumSources,
		JobTimestamp:    task.JobTimestamp,
		HyDFSDestFile:   task.HyDFSDestFile,
		HyDFSLeader:     task.HyDFSLeader,
		RainStormLeader: task.RainStormLeader,
		ExactlyOnce:     task.ExactlyOnce,
	}

	log.Printf("🔄 [LEADER] TASK RESTART: %s on %s (was on %s)",
		restartTask.ID, newWorker, payload.VM)

	// Update task in AllTasks with new worker assignment
	s.AllTasksMutex.Lock()
	s.AllTasks[restartTask.ID] = &restartTask
	s.AllTasksMutex.Unlock()

	// CRITICAL: Update routing table with new task location
	// Find and replace the old address with the new address in the routing table
	s.JobMutex.Lock()
	oldAddr := net.JoinHostPort(payload.VM, fmt.Sprintf("%d", task.Port))
	newAddr := net.JoinHostPort(newWorker, fmt.Sprintf("%d", task.Port))

	if targets, exists := s.CurrentRoutingTable[task.StageID]; exists {
		for i, addr := range targets {
			if addr == oldAddr {
				s.CurrentRoutingTable[task.StageID][i] = newAddr
				log.Printf("🔄 [LEADER] Updated routing table: %s -> %s", oldAddr, newAddr)
				break
			}
		}
	}

	// Get the updated routing table for the restart
	updatedRoutingTable := s.CurrentRoutingTable
	s.JobMutex.Unlock()

	// Send schedule task to the new worker with updated routing table
	s.sendScheduleTask(restartTask, updatedRoutingTable)
}

// HandleGetTaskLocation returns the current addresses for tasks in a stage
// This allows source tasks to discover new task locations after failures
func (s *Server) HandleGetTaskLocation(conn net.Conn, msg RainStormMessage) {
	data, _ := json.Marshal(msg.Payload)
	var payload GetTaskLocationPayload
	json.Unmarshal(data, &payload)

	// Get current routing table
	s.JobMutex.RLock()
	addresses := s.CurrentRoutingTable[payload.StageID]
	s.JobMutex.RUnlock()

	response := GetTaskLocationResponse{
		Addresses: addresses,
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(response); err != nil {
		log.Printf("❌ [LEADER] Failed to send task location response: %v", err)
	}
}

// collectAndMergeOutput collects output files from sink tasks and merges them to HyDFS
// This runs asynchronously after RUN END is detected
func (s *Server) collectAndMergeOutput(destFile string, outputFiles map[string]string) {
	log.Printf("📦 [LEADER] Starting output collection for HyDFS file: %s", destFile)
	log.Printf("📦 [LEADER] Output files to collect: %v", outputFiles)

	// TODO: Implement Phase 2 - collect rainstorm_outputs/{timestamp}/ from workers
	// and merge into HyDFS destFile
	// For now, just log that we would do this
	s.JobMutex.RLock()
	jobTs := s.JobTimestamp
	s.JobMutex.RUnlock()
	log.Printf("📦 [LEADER] Output collection placeholder - outputs are in rainstorm_outputs/%s/ on worker nodes", jobTs)
}

// createHyDFSFile creates an empty file in HyDFS using the distributed protocol
// This sends a create command to the local HyDFS handler which properly replicates
// to the correct nodes based on consistent hashing
func (s *Server) createHyDFSFile(filename string) error {
	// Create a temporary empty file in the working directory
	cwd, _ := os.Getwd()
	tmpDir := fmt.Sprintf("%s/rainstorm_outputs", cwd)
	os.MkdirAll(tmpDir, 0755)
	tmpPath := fmt.Sprintf("%s/hydfs_empty_%d.txt", tmpDir, time.Now().UnixNano())
	err := ioutil.WriteFile(tmpPath, []byte{}, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpPath)

	// Connect to local HyDFS CLI handler (ClientPort, not NodePort)
	clientPort := s.Config.ClientPort // CLI commands go to ClientPort (8003)
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", clientPort), 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to local HyDFS: %v", err)
	}
	defer conn.Close()

	// Send create command
	cmd := fmt.Sprintf("create %s %s\n", tmpPath, filename)
	log.Printf("📝 [LEADER] Sending HyDFS create command: %s", strings.TrimSpace(cmd))

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return fmt.Errorf("failed to send create command: %v", err)
	}

	// Read response with longer timeout (create involves 3s sleep for replication)
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		// If we timeout, wait a bit and assume it worked (the file create is async)
		log.Printf("⚠️  [LEADER] Response timeout, waiting for create to complete...")
		time.Sleep(5 * time.Second)
		return nil
	}

	response = strings.TrimSpace(response)
	log.Printf("📝 [LEADER] HyDFS create response: %s", response)

	if strings.HasPrefix(response, "OK") {
		return nil
	}
	return fmt.Errorf("create failed: %s", response)
}
