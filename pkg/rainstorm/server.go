package rainstorm

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os/exec"
	"sync"
	"time"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/common"
)

// Server represents a RainStorm node (can act as Leader or Worker)
type Server struct {
	Node       *common.Node   // Underlying MP2/MP3 node
	Config     *common.Config // Configuration
	Role       string         // "leader" or "worker"
	BlockStore interface {    // BlockStore for reading/writing HyDFS files
		ReadFile(filename string) ([]byte, error)
		CreateFile(filename string, content []byte, fileID uint64, primaryNodeID uint64, clientID string) error
		AppendFile(filename string, content []byte, clientID string) error
		DeleteFile(filename string) error
	}

	// Worker specific fields
	ActiveTasks        map[string]*Task // TaskID -> Task
	ActiveTasksMutex   sync.RWMutex
	TaskProcesses      map[int]*exec.Cmd // PID -> *exec.Cmd
	TaskProcessesMutex sync.RWMutex

	// Leader specific fields
	// (To be expanded in leader.go)
	Workers       map[string]bool // Set of active workers
	WorkersMutex  sync.RWMutex
	AllTasks      map[string]*Task // All tasks tracked by leader (TaskID -> Task)
	AllTasksMutex sync.RWMutex
	TaskMetrics   map[string]*TaskMetrics // TaskID -> Metrics for rate tracking
	MetricsMutex  sync.RWMutex

	// Job tracking
	CurrentJobName      string            // Name of the current running job
	CurrentJobDestFile  string            // HyDFS destination filename for output
	JobTimestamp        string            // Timestamp for job output directory (YYYYMMDD_HHMMSS)
	JobStartTime        time.Time         // When the job started
	ExpectedTasks       int               // Total number of tasks expected to complete
	OriginalTaskCount   int               // Number of original tasks (before autoscaling)
	CompletedTasks      int               // Number of tasks that have completed
	CompletedTaskIDs    map[string]bool   // Set of TaskIDs that have been counted (prevents double counting)
	UpscaledTaskIDs     map[string]bool   // Set of TaskIDs that were added via autoscaling
	OriginalTasksDone   bool              // Flag: all original tasks have completed
	OutputTaskFiles     map[string]string // TaskID -> local output file path (for sink tasks)
	CurrentRoutingTable map[int][]string  // Routing table for current job (for task restart)
	JobEnded            bool              // Flag to prevent multiple RUN END logs
	JobMutex            sync.RWMutex

	// Autoscaling configuration
	AutoScaleEnabled  bool              // Whether autoscaling is enabled for current job
	LowWatermark      int               // Scale down if avg rate per task < LW
	HighWatermark     int               // Scale up if avg rate per task > HW
	StageTaskCount    map[int]int       // Current number of tasks per stage
	LastScaleTime     map[int]time.Time // Last time we scaled each stage (cooldown)
	LowRateStart      map[int]time.Time // Track when each stage first dropped below LW (sustained downscaling)
	AutoScaleMutex    sync.RWMutex
	AutoScaleStopChan chan struct{} // Channel to stop autoscale monitor goroutine

	// Stage configuration (stored from job submission for spawning new tasks)
	StageConfigs []StageConfig // Configuration for each stage
}

// NewServer creates a new RainStorm server instance
func NewServer(node *common.Node, cfg *common.Config, blockStore interface {
	ReadFile(filename string) ([]byte, error)
	CreateFile(filename string, content []byte, fileID uint64, primaryNodeID uint64, clientID string) error
	AppendFile(filename string, content []byte, clientID string) error
	DeleteFile(filename string) error
}) *Server {
	return &Server{
		Node:          node,
		Config:        cfg,
		BlockStore:    blockStore,
		TaskMetrics:   make(map[string]*TaskMetrics),
		ActiveTasks:   make(map[string]*Task),
		Workers:       make(map[string]bool),
		AllTasks:      make(map[string]*Task),
		TaskProcesses: make(map[int]*exec.Cmd),
	}
}

// Start launches the RainStorm server listener
func (s *Server) Start() {
	port := s.Config.RainStormPort
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("[RAINSTORM] Failed to listen on port %d: %v", port, err)
	}

	log.Printf("[RAINSTORM] Server listening on port %d as %s", port, s.Role)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[RAINSTORM] Accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

// handleConnection processes incoming RainStorm messages
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	var msg RainStormMessage
	if err := decoder.Decode(&msg); err != nil {
		log.Printf("[RAINSTORM] Failed to decode message: %v", err)
		return
	}

	switch msg.Type {
	case "submit_job":
		if s.Role == "leader" {
			s.HandleSubmitJob(msg)
		} else {
			log.Printf("[RAINSTORM] Worker received submit_job (ignoring)")
		}
	case "schedule_task":
		// Both leader and workers can execute tasks
		s.HandleScheduleTask(msg)
	case "kill_task_worker":
		if s.Role == "worker" {
			s.HandleKillTaskWorker(msg)
		}
	case "list_tasks":
		if s.Role == "leader" {
			s.HandleListTasks(conn, msg)
		}
	case "kill_task":
		if s.Role == "leader" {
			s.HandleKillTask(msg)
		}
	case "task_started":
		if s.Role == "leader" {
			s.HandleTaskStarted(msg)
		}
	case "task_metrics":
		if s.Role == "leader" {
			s.HandleTaskMetrics(msg)
		}
	case "task_completed":
		if s.Role == "leader" {
			s.HandleTaskCompleted(msg)
		}
	case "task_failed":
		if s.Role == "leader" {
			s.HandleTaskFailure(msg)
		}
	case "get_task_location":
		if s.Role == "leader" {
			s.HandleGetTaskLocation(conn, msg)
		}
	default:
		log.Printf("[RAINSTORM] Unknown message type: %s", msg.Type)
	}
}
