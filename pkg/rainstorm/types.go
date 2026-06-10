// Package rainstorm implements the stream-processing framework: a leader
// that schedules and monitors tasks across workers, workers that run
// operator binaries and route tuples by key hash, and HyDFS-backed state
// logs that provide exactly-once semantics under task failure.
package rainstorm

// OperatorType defines the type of operation a task performs
type OperatorType string

const (
	OpTransform OperatorType = "transform"
	OpFilter    OperatorType = "filter"
	OpAggregate OperatorType = "aggregate"
	OpSource    OperatorType = "source" // Special internal type
)

// TaskState represents the state of a task
type TaskState int

const (
	TaskStateIdle TaskState = iota
	TaskStateRunning
	TaskStateFailed
	TaskStateCompleted
)

// Task represents a single unit of work scheduled on a worker
type Task struct {
	ID              string       // Unique Task ID (e.g., "stage1-task3")
	StageID         int          // Stage number (0-indexed)
	OpType          OperatorType // Type of operation
	OpExecutable    string       // Path to user-defined binary (e.g., "grep_op")
	OpArgs          []string     // Arguments for the binary (e.g., ["--pattern=ERROR"])
	Port            int          // Port this task listens on for incoming tuples
	AssignedWorker  string       // Hostname of the worker running this task
	State           TaskState    // Current state
	PID             int          // Process ID of running task (0 if not started)
	LogFile         string       // Path to local log file for this task
	InputRate       int          // Tuples/sec for source tasks (0 = unlimited)
	TaskIndex       int          // Task index within stage (for partitioning)
	TotalTasks      int          // Total tasks in stage (for partitioning)
	OutputFile      string       // Output file path for sink tasks (empty for non-sink)
	NumSources      int          // Number of source tasks (for EOF tracking in sink stages)
	JobTimestamp    string       // Timestamp for job output directory (YYYYMMDD_HHMMSS)
	HyDFSDestFile   string       // HyDFS destination filename for sink output (empty for non-sink)
	HyDFSLeader     string       // HyDFS leader address (host:port) for appends
	RainStormLeader string       // RainStorm leader address (host:port) for metrics reporting
	ExactlyOnce     bool         // Enable exactly-once semantics with checkpointing
}

// RainStormMessage represents the communication envelope for the RainStorm system
// (Separate from MP2/MP3 NodeMessage to keep things clean)
type RainStormMessage struct {
	Type    string      `json:"type"`    // "schedule_task", "start_job", "ack", etc.
	Sender  string      `json:"sender"`  // Sender Hostname
	Payload interface{} `json:"payload"` // Dynamic payload based on Type
}

// Payload types for specific messages

type ScheduleTaskPayload struct {
	Task         Task             `json:"task"`
	RoutingTable map[int][]string `json:"routing_table"` // StageID -> []WorkerAddr (e.g. "host:port")
}

// StageConfig stores configuration for a stage (used for autoscaling new tasks)
type StageConfig struct {
	StageID       int          // Stage number
	OpType        OperatorType // Type of operation
	OpExecutable  string       // Path to user-defined binary
	OpArgs        []string     // Arguments for the binary
	BasePort      int          // Base port for tasks in this stage
	IsSink        bool         // Whether this is the final stage
	HyDFSDestFile string       // Output file for sink stage
}

type JobSubmitPayload struct {
	App           string     `json:"app"`
	SrcFile       string     `json:"src_file"`
	DestFile      string     `json:"dest_file"`
	NumTasks      int        `json:"num_tasks"`      // Tasks per stage
	Stages        []string   `json:"stages"`         // List of binaries for each stage
	StageArgs     [][]string `json:"stage_args"`     // Args for each stage
	ExactlyOnce   bool       `json:"exactly_once"`   // Enable exactly-once semantics
	AutoScale     bool       `json:"autoscale"`      // Enable autoscaling
	InputRate     int        `json:"input_rate"`     // Tuples/sec from source
	LowWatermark  int        `json:"low_watermark"`  // Scale down threshold
	HighWatermark int        `json:"high_watermark"` // Scale up threshold
}

type ListTasksPayload struct {
	// Empty payload - just query all tasks
}

type KillTaskPayload struct {
	VM  string `json:"vm"`  // Hostname of VM running the task
	PID int    `json:"pid"` // Process ID to kill
}

type TaskInfo struct {
	TaskID  string `json:"task_id"`
	VM      string `json:"vm"`
	PID     int    `json:"pid"`
	OpExe   string `json:"op_exe"`
	LogFile string `json:"log_file"`
	State   string `json:"state"`
}

type ListTasksResponse struct {
	Tasks []TaskInfo `json:"tasks"`
}

// TaskMetrics tracks processing rate for a task
type TaskMetrics struct {
	TaskID          string  `json:"task_id"`
	TuplesProcessed int     `json:"tuples_processed"`
	CurrentRate     float64 `json:"current_rate"` // tuples/sec
	Timestamp       int64   `json:"timestamp"`    // Unix timestamp
}

type TaskMetricsPayload struct {
	Metrics TaskMetrics `json:"metrics"`
}

// TaskFailurePayload is the payload for task_failed messages
// Sent from worker to leader when a task process dies unexpectedly
type TaskFailurePayload struct {
	TaskID  string `json:"task_id"`
	VM      string `json:"vm"`
	PID     int    `json:"pid"`
	OpExe   string `json:"op_exe"`
	LogFile string `json:"log_file"`
	Error   string `json:"error"`
}

// GetTaskLocationPayload is the payload for querying task locations
type GetTaskLocationPayload struct {
	StageID int `json:"stage_id"` // Query all tasks in a stage
}

// GetTaskLocationResponse returns the current addresses for a stage
type GetTaskLocationResponse struct {
	Addresses []string `json:"addresses"` // List of "host:port" for all tasks in stage
}
