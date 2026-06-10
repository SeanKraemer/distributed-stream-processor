package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/common"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
)

func main() {
	// Command format: RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> ...
	//                 <hydfs_src_directory> <hydfs_dest_filename> <exactly_once>
	//                 <autoscale_enabled> <INPUT_RATE> <LW> <HW>

	args := os.Args[1:]
	if len(args) < 8 {
		log.Fatal("Usage: RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> [op1_args...] ... <hydfs_src> <hydfs_dest> <exactly_once> <autoscale> <input_rate> <lw> <hw>")
	}

	// Load Config for Leader Port
	cfg, err := common.LoadConfig("config.json")
	if err != nil {
		log.Printf("⚠️  Failed to load config.json: %v. Using default port 8002.", err)
		cfg = &common.Config{RainStormPort: 8002}
	}

	// Parse basic parameters
	nStages := 0
	fmt.Sscanf(args[0], "%d", &nStages)

	nTasks := 0
	fmt.Sscanf(args[1], "%d", &nTasks)

	if nStages < 1 || nStages > 3 {
		log.Fatalf("❌ Nstages must be 1-3, got %d", nStages)
	}

	// Parse operations: Need to find where stage definitions end
	// Last 7 args are: <hydfs_src> <hydfs_dest> <exactly_once> <autoscale> <input_rate> <lw> <hw>
	// Format: <op1_exe> <op1_args> <op2_exe> <op2_args> ... <src> <dest> <exactly_once> <autoscale> <input_rate> <lw> <hw>
	// Each stage has: executable followed by 0 or more arguments (arguments start with "--" or are quoted strings)

	if len(args) < 2+nStages+7 {
		log.Fatal("❌ Not enough arguments. Need at least Nstages operation executables plus 7 fixed params.")
	}

	// Parse stages and their arguments
	// Format: <op1_exe> <op1_args...> <op2_exe> <op2_args...> ... <src> <dest> <exactly_once> ...
	// Strategy: Find all operator names first (known keywords), then collect args between them

	stages := []string{}
	opArgs := [][]string{}

	// Known operator names (simple names without paths)
	knownOps := map[string]bool{
		"grep": true, "count": true, "filter": true, "transform": true,
		"echo": true, "identity": true, "output": true,
	}

	// First pass: identify operator positions
	idx := 2 // Start after Nstages and Ntasks
	opPositions := []int{}

	for i := idx; i < len(args)-7; i++ {
		arg := args[i]
		// Check if this is a known operator name OR looks like an operator path
		if knownOps[arg] || strings.Contains(arg, "/") {
			opPositions = append(opPositions, i)
			if len(opPositions) == nStages {
				break
			}
		}
	}

	if len(opPositions) != nStages {
		log.Fatalf("❌ Could not find %d operators (found %d at positions %v)", nStages, len(opPositions), opPositions)
	}

	// Second pass: extract operators and their arguments
	for i := 0; i < len(opPositions); i++ {
		opIdx := opPositions[i]
		stages = append(stages, args[opIdx])

		// Arguments are between this operator and the next (or fixed params)
		startArgs := opIdx + 1
		var endArgs int
		if i < len(opPositions)-1 {
			endArgs = opPositions[i+1]
		} else {
			endArgs = len(args) - 7
		}

		stageArgs := args[startArgs:endArgs]
		opArgs = append(opArgs, stageArgs)
	}

	idx = len(args) - 7 // Parse fixed parameters (remaining arguments after all stages)
	fixedParams := args[idx:]
	hydfsSource := fixedParams[0]
	hydfsDest := fixedParams[1]
	exactlyOnce := fixedParams[2] == "true" || fixedParams[2] == "1"
	autoscale := fixedParams[3] == "true" || fixedParams[3] == "1"

	inputRate := 0
	fmt.Sscanf(fixedParams[4], "%d", &inputRate)

	lw := 0
	fmt.Sscanf(fixedParams[5], "%d", &lw)

	hw := 0
	fmt.Sscanf(fixedParams[6], "%d", &hw)

	log.Printf("📋 RainStorm Job Submission:")
	log.Printf("   Stages: %d, Tasks/Stage: %d", nStages, nTasks)
	log.Printf("   Source: %s, Dest: %s", hydfsSource, hydfsDest)
	log.Printf("   ExactlyOnce: %v, Autoscale: %v", exactlyOnce, autoscale)
	log.Printf("   InputRate: %d, LW: %d, HW: %d", inputRate, lw, hw)

	payload := rainstorm.JobSubmitPayload{
		App:           filepath.Base(os.Args[0]),
		SrcFile:       hydfsSource,
		DestFile:      hydfsDest,
		NumTasks:      nTasks,
		Stages:        stages,
		StageArgs:     opArgs,
		ExactlyOnce:   exactlyOnce,
		AutoScale:     autoscale,
		InputRate:     inputRate,
		LowWatermark:  lw,
		HighWatermark: hw,
	}

	msg := rainstorm.RainStormMessage{
		Type:    "submit_job",
		Sender:  "client",
		Payload: payload,
	}

	// Leader is always node1 (first in the nodes list)
	leaderHost := "node1"
	if len(cfg.Nodes) > 0 {
		leaderHost = cfg.Nodes[0]
	}
	leaderAddr := net.JoinHostPort(leaderHost, strconv.Itoa(cfg.RainStormPort))

	conn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
	if err != nil {
		log.Fatalf("❌ Failed to connect to leader %s: %v", leaderAddr, err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		log.Fatalf("❌ Failed to send job: %v", err)
	}

	fmt.Println("✅ Job submitted successfully!")
}
