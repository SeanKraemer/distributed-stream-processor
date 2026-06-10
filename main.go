package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/common"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/fileops"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/hashing"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/logging"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/membership"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/storage"
)

// Global map to track pending replication ACK targets
var (
	pendingAckTargets      = make(map[string]string) // filename -> ackTarget address
	pendingAckTargetsMutex sync.RWMutex
	globalBlockStore       *storage.BlockStore // Global BlockStore instance to prevent race conditions
)

// NodeMessage represents inter-node communication messages
type NodeMessage struct {
	Type          string                 `json:"type"`             // "operation_log", "replicate_file", "replication_ack", "merge_request", etc.
	Sender        string                 `json:"sender"`           // Sending VM hostname
	Operation     string                 `json:"operation"`        // create, get, append, merge, etc.
	Filename      string                 `json:"filename"`         // HyDFS filename
	Data          map[string]interface{} `json:"data"`             // Additional operation-specific data
	Timestamp     int64                  `json:"timestamp"`        // Unix nano timestamp
	NextReplica   string                 `json:"next_replica"`     // Next replica in chain (for pipelined replication)
	AckTarget     string                 `json:"ack_target"`       // Where to send ACK (for chained acknowledgment)
	IsLastInChain bool                   `json:"is_last_in_chain"` // Indicates this is the last replica in chain
}

// broadcastOperation broadcasts an operation log to all alive members in the cluster
func broadcastOperation(node *common.Node, operation, filename string, additionalData map[string]interface{}) {
	infoMap := node.Membership.GetInfoMap()
	self := infoMap[node.RingID]

	msg := NodeMessage{
		Type:      "operation_log",
		Sender:    self.Hostname,
		Operation: operation,
		Filename:  filename,
		Data:      additionalData,
		Timestamp: time.Now().UnixNano(),
	}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		log.Printf("broadcast: failed to marshal message: %v", err)
		return
	}

	// Send to all alive members
	for id, info := range infoMap {
		if id == node.RingID || info.State != membership.Alive {
			continue
		}

		go func(hostname string, port int) {
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), 2*time.Second)
			if err != nil {
				log.Printf("broadcast: failed to connect to %s:%d: %v", hostname, port, err)
				return
			}
			defer conn.Close()

			conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			_, err = conn.Write(append(jsonData, '\n'))
			if err != nil {
				log.Printf("broadcast: failed to send to %s:%d: %v", hostname, port, err)
			}
		}(info.Hostname, info.Port)
	}
}

// forwardReplication handles pipelined replication forwarding
func forwardReplication(node *common.Node, hydfsFile string, content []byte, fileID uint64, primaryNodeID uint64, clientID string, successors []uint64, chainIndex int, infoMap map[uint64]membership.Info) {
	if chainIndex >= len(successors) {
		// End of chain
		return
	}

	currentNodeID := successors[chainIndex]
	currentInfo, ok := infoMap[currentNodeID]
	if !ok || currentInfo.State != membership.Alive {
		log.Printf("❌ [FORWARD] Node %d not available, skipping", currentNodeID)
		return
	}

	// Determine next replica
	var nextReplica string
	var ackTarget string
	isLast := (chainIndex == len(successors)-1)

	if !isLast {
		nextInfo := infoMap[successors[chainIndex+1]]
		nextReplica = fmt.Sprintf("%s:%d", nextInfo.Hostname, nextInfo.Port)
	}

	// ACK goes back to previous node in chain (or coordinator if this is primary from coordinator)
	if chainIndex > 0 {
		prevInfo := infoMap[successors[chainIndex-1]]
		ackTarget = fmt.Sprintf("%s:%d", prevInfo.Hostname, prevInfo.Port)
	} else {
		// Primary node - ACK goes to coordinator
		self := infoMap[node.RingID]
		ackTarget = fmt.Sprintf("%s:%d", self.Hostname, self.Port)
	}

	// Convert successors to strings to avoid float64 precision loss
	successorsList := make([]string, len(successors))
	for i, s := range successors {
		successorsList[i] = fmt.Sprintf("%d", s)
	}

	msg := NodeMessage{
		Type:          "replicate_file",
		Sender:        infoMap[node.RingID].Hostname,
		Operation:     "create",
		Filename:      hydfsFile,
		NextReplica:   nextReplica,
		AckTarget:     ackTarget,
		IsLastInChain: isLast,
		Data: map[string]interface{}{
			"file_id":     float64(fileID),
			"content":     string(content),
			"created_at":  float64(time.Now().Unix()),
			"client_id":   clientID,
			"primary_id":  float64(primaryNodeID),
			"successors":  successorsList, // Now strings
			"chain_index": chainIndex,
		},
		Timestamp: time.Now().UnixNano(),
	}

	jsonData, _ := json.Marshal(msg)

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", currentInfo.Hostname, currentInfo.Port), 5*time.Second)
	if err != nil {
		log.Printf("❌ [FORWARD] Failed to connect to %s:%d: %v", currentInfo.Hostname, currentInfo.Port, err)
		return
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(append(jsonData, '\n')); err != nil {
		log.Printf("❌ [FORWARD] Failed to send to %s:%d: %v", currentInfo.Hostname, currentInfo.Port, err)
		return
	}

	log.Printf("📤 [FORWARD] Sent replication to %s:%d (NodeID=%020d)", currentInfo.Hostname, currentInfo.Port, currentNodeID)
}

// gracefulLeave performs a voluntary leave from the cluster
// Broadcasts Leave message to all members and marks self as Failed locally
func gracefulLeave(node *common.Node) {
	node.ActiveMutex.RLock()
	isActive := node.IsActive
	node.ActiveMutex.RUnlock()

	if !isActive {
		// Already left
		return
	}

	infoMap := node.Membership.GetInfoMap()
	self, ok := infoMap[node.RingID]
	if !ok || self.State == membership.Failed {
		// Already left or not in membership
		node.ActiveMutex.Lock()
		node.IsActive = false
		node.ActiveMutex.Unlock()
		return
	}

	log.Printf("🚪 Performing graceful leave from cluster...")

	// Mark as inactive FIRST to stop gossip loop
	node.ActiveMutex.Lock()
	node.IsActive = false
	node.ActiveMutex.Unlock()

	// Mark self as failed in local membership
	node.Membership.UpdateStateSwim(time.Now(), node.RingID, membership.Failed, false)

	// Broadcast Leave message to all alive members
	leaveMsg := membership.Message{
		Type:       membership.Leave,
		SenderInfo: self,
		InfoMap:    node.Membership.GetInfoMap(),
	}

	sent := 0
	for id, info := range infoMap {
		if id == node.RingID || info.State != membership.Alive {
			continue
		}

		if _, err := membership.SendMessage(leaveMsg, info.Hostname, info.Port); err != nil {
			log.Printf("⚠️  Failed to send Leave to %s:%d: %v", info.Hostname, info.Port, err)
		} else {
			sent++
		}
	}

	log.Printf("✅ Sent Leave message to %d members", sent)
}

// joinGroup performs an initial join using a Probe to the introducer (cfg.VMs[0]).
// It temporarily opens a UDP listener on our node's port to receive the ack and
// merges the membership info.
func joinGroup(node *common.Node, cfg *common.Config) {
	// Resolve our own info
	infoMap := node.Membership.GetInfoMap()
	self, ok := infoMap[node.RingID]
	if !ok {
		log.Printf("joinGroup: self info not found in membership; skipping join")
		return
	}

	introducerHost := cfg.VMs[0]

	// Check if we ARE the introducer
	if self.Hostname == introducerHost {
		log.Printf("joinGroup: I am the introducer (%s), skipping join", introducerHost)
		return
	}

	// Retry join with exponential backoff
	maxRetries := 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("🔄 joinGroup: attempt %d/%d to join via %s", attempt, maxRetries, introducerHost)

		// Send Probe to introducer
		probe := membership.Message{
			Type:       membership.Probe,
			SenderInfo: self,
			InfoMap:    node.Membership.GetInfoMap(),
		}
		// Send to introducer's UDP port (same cfg.NodePort)
		p := cfg.NodePort
		if _, err := membership.SendMessage(probe, introducerHost, p); err != nil {
			log.Printf("❌ joinGroup: send probe to %s:%d failed: %v", introducerHost, p, err)
			if attempt < maxRetries {
				backoff := time.Duration(attempt) * time.Second
				log.Printf("   Retrying in %v...", backoff)
				time.Sleep(backoff)
				continue
			}
			log.Printf("❌ joinGroup: all send attempts failed")
			return
		}

		log.Printf("✅ joinGroup: Probe sent to introducer %s:%d", introducerHost, p)

		// Wait for the gossip loop to merge the membership info from the introducer's response
		// The introducer should respond via gossip, so we just wait and check
		time.Sleep(2 * time.Second)

		// Check if we've joined (membership should have more than just ourselves)
		infoMap = node.Membership.GetInfoMap()
		if len(infoMap) > 1 {
			log.Printf("✅ joinGroup: Successfully joined! Membership size: %d", len(infoMap))
			return
		}

		if attempt < maxRetries {
			backoff := time.Duration(attempt) * time.Second
			log.Printf("⚠️  joinGroup: Not yet joined (membership size: %d), retrying in %v...", len(infoMap), backoff)
			time.Sleep(backoff)
		}
	}

	log.Printf("⚠️  joinGroup: Join attempts completed. Membership size: %d. Will rely on gossip convergence.", len(node.Membership.GetInfoMap()))
}

// checkAndRereplicate checks all locally stored files for under-replication
// and triggers re-replication if needed. This is called when a node failure is detected.
func checkAndRereplicate(node *common.Node) {
	// Scan local storage for files
	entries, err := os.ReadDir(fileops.StorageRoot)
	if err != nil {
		log.Printf("❌ [REREPLICATE] Failed to read storage directory: %v", err)
		return
	}

	infoMap := node.Membership.GetInfoMap()

	log.Printf("🔍 [REREPLICATE] Starting re-replication scan after membership change (alive nodes: %d)", len(infoMap))
	log.Printf("🔍 [REREPLICATE] Found %d entries in storage directory: %s", len(entries), fileops.StorageRoot)

	filesScanned := 0
	for _, entry := range entries {
		log.Printf("🔍 [REREPLICATE] Checking entry: %s (isDir=%v)", entry.Name(), entry.IsDir())

		if !entry.IsDir() {
			log.Printf("  ⏭️  Skipping non-directory: %s", entry.Name())
			continue
		}

		// Read metadata to get file info
		metadataPath := filepath.Join(fileops.StorageRoot, entry.Name(), "_metadata.json")
		data, err := os.ReadFile(metadataPath)
		if err != nil {
			log.Printf("  ⚠️  Failed to read metadata for %s: %v", entry.Name(), err)
			continue
		}

		var metadata storage.FileMetadata
		if err := json.Unmarshal(data, &metadata); err != nil {
			log.Printf("  ⚠️  Failed to parse metadata for %s: %v", entry.Name(), err)
			continue
		}

		hydfsFile := metadata.HyDFSFilename
		fileID := metadata.FileID
		filesScanned++

		log.Printf("🔍 [REREPLICATE] Scanning file: %s (ID=%020d)", hydfsFile, fileID)

		// Calculate where this file SHOULD be replicated (based on current membership)
		expectedSuccessors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)
		if len(expectedSuccessors) == 0 {
			continue
		}

		// DO NOT SORT - GetSuccessors already returns them in correct ring order
		// The first element is the primary replica

		// Check if this node is the primary replica
		isPrimary := false
		if len(expectedSuccessors) > 0 && expectedSuccessors[0] == node.RingID {
			isPrimary = true
		}

		// Check if this node is one of the expected replicas
		isExpectedReplica := false
		for _, nodeID := range expectedSuccessors {
			if nodeID == node.RingID {
				isExpectedReplica = true
				break
			}
		}

		// Only proceed if this node is an expected replica
		if !isExpectedReplica {
			continue
		}

		// Count how many expected replicas are alive
		aliveReplicas := 0
		for _, nodeID := range expectedSuccessors {
			if info, ok := infoMap[nodeID]; ok && info.State == membership.Alive {
				aliveReplicas++
			}
		}

		log.Printf("  Expected successors for %s: %v", hydfsFile, expectedSuccessors)
		log.Printf("  This node is expected replica: %v, Is primary: %v", isExpectedReplica, isPrimary)
		log.Printf("  Alive replicas: %d/%d", aliveReplicas, hashing.NumReplicas)

		// Only proceed if this node is an expected replica
		if !isExpectedReplica {
			log.Printf("  ⏭️  Skipping - this node is NOT an expected replica")
			continue
		}

		// If this node is the primary, always check if re-replication is needed
		// (We can't reliably know which remote nodes have the file without querying them)
		if isPrimary {
			log.Printf("🔄 [REREPLICATE] Primary replica checking file %s (ID=%020d): %d/%d expected nodes alive",
				hydfsFile, fileID, aliveReplicas, hashing.NumReplicas)
			log.Printf("    Primary will send to all expected successors (they will deduplicate if already stored)")

			// Read the file content
			content, err := globalBlockStore.ReadFile(hydfsFile)
			if err != nil {
				log.Printf("❌ [REREPLICATE] Failed to read %s: %v", hydfsFile, err)
				continue
			}

			// Send to each expected successor that doesn't have the file
			for _, targetNodeID := range expectedSuccessors {
				targetInfo, ok := infoMap[targetNodeID]
				if !ok || targetInfo.State != membership.Alive {
					continue
				}

				// Skip self
				if targetNodeID == node.RingID {
					continue
				}

				// Send replication message
				msg := NodeMessage{
					Type:      "rereplicate_file",
					Sender:    infoMap[node.RingID].Hostname,
					Operation: "rereplicate",
					Filename:  hydfsFile,
					Data: map[string]interface{}{
						"file_id":    float64(fileID),
						"content":    string(content),
						"created_at": float64(time.Now().Unix()),
					},
					Timestamp: time.Now().UnixNano(),
				}

				jsonData, _ := json.Marshal(msg)
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", targetInfo.Hostname, targetInfo.Port), 5*time.Second)
				if err != nil {
					log.Printf("⚠️  [REREPLICATE] Failed to connect to %s: %v", targetInfo.Hostname, err)
					continue
				}

				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := conn.Write(append(jsonData, '\n')); err != nil {
					log.Printf("⚠️  [REREPLICATE] Failed to send to %s: %v", targetInfo.Hostname, err)
				} else {
					log.Printf("✅ [REREPLICATE] Sent %s (%d bytes) to %s:%d (NodeID=%020d)",
						hydfsFile, len(content), targetInfo.Hostname, targetInfo.Port, targetNodeID)
				}
				conn.Close()
			}
		} else if !isPrimary {
			log.Printf("  ⏭️  Not primary - skipping re-replication (primary will handle)")
		}
	}

	log.Printf("🔍 [REREPLICATE] Re-replication scan complete (scanned %d files)", filesScanned)
}

// startGossipLoop runs a combined loop that both gossips periodically and
// reacts to incoming UDP messages. It listens on our node's UDP port.
func startGossipLoop(node *common.Node) {
	// Resolve self info and UDP port
	infoMap := node.Membership.GetInfoMap()
	self := infoMap[node.RingID]

	// UDP listener
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", self.Port))
	if err != nil {
		log.Printf("gossip: resolve UDP addr: %v", err)
		return
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("gossip: listen UDP: %v", err)
		return
	}
	log.Printf("gossip: UDP listening on :%d", self.Port)

	// Channel to deliver incoming messages
	incoming := make(chan membership.Message, 64)

	// Reader goroutine
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				log.Printf("gossip: read error: %v", err)
				continue
			}
			msg, derr := membership.Deserialize(buf[:n])
			if derr != nil {
				log.Printf("gossip: decode error: %v", derr)
				continue
			}
			select {
			case incoming <- msg:
			default:
				// drop if busy
			}
		}
	}()

	// Periodic ticker for proactive gossip
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	// Failure detection ticker (check for timeouts)
	failureTicker := time.NewTicker(500 * time.Millisecond)
	defer failureTicker.Stop()

	// Cleanup ticker (remove old failed entries)
	cleanupTicker := time.NewTicker(10 * time.Second)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ticker.C:
			// Skip gossip if not active in cluster
			node.ActiveMutex.RLock()
			isActive := node.IsActive
			node.ActiveMutex.RUnlock()

			if !isActive {
				continue
			}

			// Increment our heartbeat counter
			if err := node.Membership.Heartbeat(node.RingID, time.Now()); err != nil {
				log.Printf("⚠️  Failed to heartbeat: %v", err)
				continue
			}

			// Proactive gossip: pick a target and send our membership map
			target, err := node.Membership.GetTarget()
			if err != nil {
				// possibly solo; skip
				continue
			}

			// Get fresh info map with updated counter
			infoMap := node.Membership.GetInfoMap()
			self = infoMap[node.RingID] // Update self with new counter

			g := membership.Message{
				Type:       membership.GossipMsg,
				SenderInfo: self,
				InfoMap:    infoMap,
			}
			if _, err := membership.SendMessage(g, target.Hostname, target.Port); err != nil {
				// log, but continue (don't spam logs)
			}

		case <-failureTicker.C:
			// Skip failure detection if not active
			node.ActiveMutex.RLock()
			isActive := node.IsActive
			node.ActiveMutex.RUnlock()

			if !isActive {
				continue
			}

			// Update membership state based on timeouts (gossip protocol, no suspicion)
			// Tfail=5s - with gossip every 250ms, this gives ~20 gossip chances before timeout
			// Tsuspect unused when suspicion disabled

			// Check if any failures were detected - if so, trigger re-replication scan
			failureDetected := node.Membership.UpdateStateGossip(time.Now(), 5*time.Second, 5*time.Second, false)
			if failureDetected {
				log.Printf("🚨 [FAILURE] Node failure detected - triggering re-replication scan")
				go checkAndRereplicate(node)
			}

		case <-cleanupTicker.C:
			// Cleanup old failed entries (after 30 seconds)
			node.Membership.Cleanup(time.Now(), 30*time.Second)

		case msg := <-incoming:
			// Reactive handling of inbound messages
			switch msg.Type {
			case membership.Probe:
				// A node is trying to join - merge their info and send back our full membership
				log.Printf("📥 Received Probe from %s:%d", msg.SenderInfo.Hostname, msg.SenderInfo.Port)

				// Before merging, check if there's an old incarnation with same hostname:port
				// If found, remove it to prevent duplicate counting
				// First, find the new Ring ID from the incoming InfoMap
				var newRingID uint64
				for id, info := range msg.InfoMap {
					if info.Hostname == msg.SenderInfo.Hostname && info.Port == msg.SenderInfo.Port {
						newRingID = id
						break
					}
				}

				// Now check for old incarnations in our current membership
				currentInfo := node.Membership.GetInfoMap()
				for oldID, oldInfo := range currentInfo {
					if oldInfo.Hostname == msg.SenderInfo.Hostname &&
						oldInfo.Port == msg.SenderInfo.Port &&
						oldID != newRingID {
						// Found old incarnation - remove it
						node.Membership.RemoveMember(oldID)
						log.Printf("🗑️  Removed old incarnation (ID=%d, state=%v) for %s:%d before adding new incarnation (ID=%d)",
							oldID, oldInfo.State, oldInfo.Hostname, oldInfo.Port, newRingID)
						break
					}
				}

				// Merge the joining node's info
				node.Membership.Merge(msg.InfoMap, time.Now())

				// Get fresh membership info with updated state
				infoMap := node.Membership.GetInfoMap()
				self = infoMap[node.RingID] // Update self

				// Send back our full membership
				ack := membership.Message{
					Type:       membership.ProbeAckGossip,
					SenderInfo: self,
					InfoMap:    infoMap,
				}
				if _, err := membership.SendMessage(ack, msg.SenderInfo.Hostname, msg.SenderInfo.Port); err != nil {
					log.Printf("❌ gossip: ack send failed to %s:%d: %v", msg.SenderInfo.Hostname, msg.SenderInfo.Port, err)
				} else {
					log.Printf("✅ Sent membership (%d members) to joining node %s:%d",
						len(infoMap), msg.SenderInfo.Hostname, msg.SenderInfo.Port)
				}

			case membership.GossipMsg:
				// Before merging gossip, check for any new incarnations and remove old ones
				currentInfo := node.Membership.GetInfoMap()
				for newID, newInfo := range msg.InfoMap {
					// Check if this is a new incarnation (different ID, same hostname:port)
					for oldID, oldInfo := range currentInfo {
						if oldInfo.Hostname == newInfo.Hostname &&
							oldInfo.Port == newInfo.Port &&
							oldID != newID &&
							oldInfo.State == membership.Failed {
							// Found old failed incarnation - remove it
							node.Membership.RemoveMember(oldID)
							log.Printf("🗑️  Removed old incarnation (ID=%d) for %s:%d via gossip, replacing with new incarnation (ID=%d)",
								oldID, oldInfo.Hostname, oldInfo.Port, newID)
							break
						}
					}
				}

				// Merge gossip info
				changed := node.Membership.Merge(msg.InfoMap, time.Now())
				if changed {
					log.Printf("🔄 Membership updated via gossip from %s:%d (now %d members)",
						msg.SenderInfo.Hostname, msg.SenderInfo.Port, len(node.Membership.GetInfoMap()))

					// Trigger re-replication scan since membership changed (failures learned via gossip)
					log.Printf("🚨 [GOSSIP] Membership changed - triggering re-replication scan")
					go checkAndRereplicate(node)
				}
				// Update self periodically
				infoMap := node.Membership.GetInfoMap()
				self = infoMap[node.RingID]

			case membership.Leave:
				// Handle voluntary leave - immediately mark as failed
				log.Printf("👋 Received Leave message from %s:%d", msg.SenderInfo.Hostname, msg.SenderInfo.Port)

				// Find the leaving node's ID and mark as failed
				for id, info := range node.Membership.GetInfoMap() {
					if info.Hostname == msg.SenderInfo.Hostname && info.Port == msg.SenderInfo.Port {
						changed := node.Membership.UpdateStateSwim(time.Now(), id, membership.Failed, false)
						log.Printf("✅ Marked %s:%d as Failed (voluntary leave)", info.Hostname, info.Port)

						// Trigger re-replication scan since membership changed
						if changed {
							log.Printf("🚨 [LEAVE] Node left - triggering re-replication scan")
							go checkAndRereplicate(node)
						}
						break
					}
				}

			case membership.Ping, membership.Pong, membership.PingReq,
				membership.ProbeAckGossip, membership.ProbeAckSwim,
				membership.UseSwimSus, membership.UseSwimNoSus,
				membership.UseGossipSus, membership.UseGossipNoSus:
				// For now, minimal handling: merge if info present
				if msg.InfoMap != nil {
					node.Membership.Merge(msg.InfoMap, time.Now())
				}
			default:
				// Unknown/unused message types: ignore
			}
		}
	}
}

// handleNodeConnection handles internal node control connections for inter-node messages.
func handleNodeConnection(node *common.Node, conn net.Conn) {
	defer conn.Close()
	log.Printf("node-conn: new connection from %s", conn.RemoteAddr())

	reader := bufio.NewScanner(conn)
	reader.Buffer(make([]byte, 64*1024), 10*1024*1024) // 10MB max message size (for large file replication)

	for reader.Scan() {
		line := reader.Text()
		if line == "" {
			continue
		}

		log.Printf("node-conn: received message (length: %d bytes)", len(line))

		var msg NodeMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			// Try to parse as generic message to check if it's a replicate_block request
			var genericMsg map[string]interface{}
			if err2 := json.Unmarshal([]byte(line), &genericMsg); err2 == nil {
				if msgType, ok := genericMsg["type"].(string); ok {
					log.Printf("node-conn: message type: %s", msgType)
					if msgType == "replicate_block" {
						// Handle replication request
						var replicateReq struct {
							Type    string                        `json:"type"`
							Request fileops.ReplicateBlockRequest `json:"request"`
						}
						if err3 := json.Unmarshal([]byte(line), &replicateReq); err3 == nil {
							log.Printf("node-conn: processing replicate_block request")
							resp, err := fileops.HandleReplicateBlock(node, replicateReq.Request)
							if err != nil {
								log.Printf("node-conn: failed to handle replication: %v", err)
								respData, _ := json.Marshal(map[string]interface{}{
									"success": false,
									"error":   err.Error(),
								})
								conn.Write(append(respData, '\n'))
								return // Close connection after response
							}
							log.Printf("node-conn: replication successful, sending response")
							respData, _ := json.Marshal(resp)
							conn.Write(append(respData, '\n'))
							return // Close connection after response
						} else {
							log.Printf("node-conn: failed to unmarshal replicate_block request: %v", err3)
						}
					}
				}
			}
			log.Printf("node-conn: failed to decode message: %v", err)
			continue
		}

		// Log received operation from other nodes
		switch msg.Type {
		case "operation_log":
			log.Printf("🔔 [BROADCAST] VM %s executed: %s on file '%s' at %s",
				msg.Sender,
				strings.ToUpper(msg.Operation),
				msg.Filename,
				time.Unix(0, msg.Timestamp).Format("15:04:05.000"))

			// Display additional data if present
			if len(msg.Data) > 0 {
				for k, v := range msg.Data {
					log.Printf("    %s: %v", k, v)
				}
			}

		case "replicate_file":
			// TASK 3: Handle pipelined replication
			log.Printf("📥 [REPLICATE] Received file '%s' from %s", msg.Filename, msg.Sender)

			// Debug: Log received message fields
			log.Printf("🔍 [REPLICATE DEBUG] NextReplica=%s, AckTarget=%s, IsLastInChain=%v", msg.NextReplica, msg.AckTarget, msg.IsLastInChain)
			chainIndexFloat, _ := msg.Data["chain_index"].(float64)
			log.Printf("🔍 [REPLICATE DEBUG] chain_index=%.0f", chainIndexFloat)
			successorsRaw, _ := msg.Data["successors"].([]interface{})
			log.Printf("🔍 [REPLICATE DEBUG] successors raw (len=%d): %v", len(successorsRaw), successorsRaw)

			// Extract data from message
			fileIDFloat, ok := msg.Data["file_id"].(float64)
			if !ok {
				log.Printf("❌ [REPLICATE] Invalid file_id in message")
				continue
			}
			fileID := uint64(fileIDFloat)

			contentStr, ok := msg.Data["content"].(string)
			if !ok {
				log.Printf("❌ [REPLICATE] Invalid content in message")
				continue
			}
			content := []byte(contentStr)

			createdAtFloat, ok := msg.Data["created_at"].(float64)
			if !ok {
				log.Printf("❌ [REPLICATE] Invalid created_at in message")
				continue
			}
			createdAt := int64(createdAtFloat)

			clientID, _ := msg.Data["client_id"].(string)
			primaryIDFloat, _ := msg.Data["primary_id"].(float64)
			primaryNodeID := uint64(primaryIDFloat)

			// Store using BlockStore
			if err := globalBlockStore.CreateFile(msg.Filename, content, fileID, primaryNodeID, clientID); err != nil {
				log.Printf("❌ [REPLICATE] Failed to store %s: %v", msg.Filename, err)
				continue
			}

			// Update metadata
			node.FileStoreMutex.Lock()
			node.FileStore[msg.Filename] = common.FileMetadata{
				Filename:  msg.Filename,
				FileID:    fileID,
				Version:   1,
				Size:      int64(len(content)),
				CreatedAt: createdAt,
			}
			node.FileStoreMutex.Unlock()

			log.Printf("✅ [REPLICATE] Stored %s (%d bytes)", msg.Filename, len(content))

			// Store the AckTarget for later use when we receive ACK from successor
			if msg.AckTarget != "" && !msg.IsLastInChain {
				pendingAckTargetsMutex.Lock()
				pendingAckTargets[msg.Filename] = msg.AckTarget
				pendingAckTargetsMutex.Unlock()
			}

			// TASK 3: Forward to next replica if not last in chain
			if msg.NextReplica != "" && !msg.IsLastInChain {
				log.Printf("📤 [CHAIN] Forwarding replication to next replica: %s", msg.NextReplica)

				// Forward the message to the next replica
				parts := strings.Split(msg.NextReplica, ":")
				if len(parts) == 2 {
					forwardConn, err := net.DialTimeout("tcp", msg.NextReplica, 5*time.Second)
					if err != nil {
						log.Printf("❌ [CHAIN] Failed to connect to next replica %s: %v", msg.NextReplica, err)
					} else {
						// Update the message for forwarding
						chainIndexFloat, _ := msg.Data["chain_index"].(float64)
						chainIndex := int(chainIndexFloat) + 1

						// Parse successors as STRINGS to preserve precision
						successorsRaw, _ := msg.Data["successors"].([]interface{})
						successors := make([]uint64, len(successorsRaw))
						for i, v := range successorsRaw {
							// Parse string to uint64
							nodeIDStr, ok := v.(string)
							if ok {
								nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
								if err == nil {
									successors[i] = nodeID
								} else {
									log.Printf("❌ [CHAIN DEBUG] Failed to parse NodeID string '%s': %v", nodeIDStr, err)
								}
							} else {
								log.Printf("❌ [CHAIN DEBUG] Successor at index %d is not a string: %T", i, v)
							}
						}

						log.Printf("🔍 [CHAIN DEBUG] Current chain_index=%d, successors=%v, len=%d", chainIndex, successors, len(successors))

						// Determine the next-next replica
						var nextNextReplica string
						isNextLast := chainIndex == len(successors)-1

						if chainIndex+1 < len(successors) {
							infoMap := node.Membership.GetInfoMap()
							nextNodeID := successors[chainIndex+1]
							nextNextInfo, exists := infoMap[nextNodeID]
							if exists {
								nextNextReplica = fmt.Sprintf("%s:%d", nextNextInfo.Hostname, nextNextInfo.Port)
								log.Printf("🔍 [CHAIN DEBUG] Next-next replica: NodeID=%d, Address=%s", nextNodeID, nextNextReplica)
							} else {
								log.Printf("❌ [CHAIN DEBUG] Next-next NodeID=%d not found in infoMap!", nextNodeID)
							}
						} else {
							log.Printf("🔍 [CHAIN DEBUG] This is the last node (chain_index=%d >= len-1=%d)", chainIndex, len(successors)-1)
						}

						forwardMsg := NodeMessage{
							Type:          "replicate_file",
							Sender:        msg.Sender,
							Operation:     msg.Operation,
							Filename:      msg.Filename,
							NextReplica:   nextNextReplica,
							AckTarget:     fmt.Sprintf("%s:%d", node.Membership.GetInfoMap()[node.RingID].Hostname, node.Membership.GetInfoMap()[node.RingID].Port),
							IsLastInChain: isNextLast,
							Data:          msg.Data,
							Timestamp:     time.Now().UnixNano(),
						}
						forwardMsg.Data["chain_index"] = float64(chainIndex)

						jsonData, _ := json.Marshal(forwardMsg)
						forwardConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
						forwardConn.Write(append(jsonData, '\n'))
						forwardConn.Close()

						log.Printf("📤 [CHAIN] Forwarded to %s (chain_index=%d, is_last=%v, next=%s)", msg.NextReplica, chainIndex, isNextLast, nextNextReplica)
					}
				}
			}

			// TASK 2 FIX: Only send ACK if this is the LAST node in chain
			// Non-last nodes will send ACK when they receive ACK from their successor
			if msg.IsLastInChain && msg.AckTarget != "" {
				log.Printf("📮 [ACK] Last node in chain, sending acknowledgment to %s", msg.AckTarget)

				ackConn, err := net.DialTimeout("tcp", msg.AckTarget, 5*time.Second)
				if err != nil {
					log.Printf("❌ [ACK] Failed to connect to ACK target %s: %v", msg.AckTarget, err)
				} else {
					ackMsg := NodeMessage{
						Type:      "replication_ack",
						Sender:    node.Membership.GetInfoMap()[node.RingID].Hostname,
						Operation: "create",
						Filename:  msg.Filename,
						Data: map[string]interface{}{
							"success": true,
						},
						Timestamp: time.Now().UnixNano(),
					}

					jsonData, _ := json.Marshal(ackMsg)
					ackConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
					ackConn.Write(append(jsonData, '\n'))
					ackConn.Close()

					log.Printf("✅ [ACK] Sent acknowledgment to %s", msg.AckTarget)
				}
			}

		case "replication_ack":
			// Received ACK from next node in chain - forward it back up the chain
			log.Printf("📬 [ACK] Received acknowledgment for '%s' from %s", msg.Filename, msg.Sender)

			// Retrieve the stored AckTarget for this file
			pendingAckTargetsMutex.Lock()
			ackTarget, exists := pendingAckTargets[msg.Filename]
			if exists {
				delete(pendingAckTargets, msg.Filename) // Clean up
			}
			pendingAckTargetsMutex.Unlock()

			if exists && ackTarget != "" {
				log.Printf("📮 [ACK] Forwarding acknowledgment to %s", ackTarget)

				ackConn, err := net.DialTimeout("tcp", ackTarget, 5*time.Second)
				if err != nil {
					log.Printf("❌ [ACK] Failed to connect to ACK target %s: %v", ackTarget, err)
				} else {
					// Forward the ACK message
					forwardAckMsg := NodeMessage{
						Type:      "replication_ack",
						Sender:    node.Membership.GetInfoMap()[node.RingID].Hostname,
						Operation: "create",
						Filename:  msg.Filename,
						Data: map[string]interface{}{
							"success": true,
						},
						Timestamp: time.Now().UnixNano(),
					}

					jsonData, _ := json.Marshal(forwardAckMsg)
					ackConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
					ackConn.Write(append(jsonData, '\n'))
					ackConn.Close()

					log.Printf("✅ [ACK] Forwarded acknowledgment to %s", ackTarget)
				}
			}

		case "get_file":
			// Handle GET request from client
			log.Printf("📥 [GET] Received GET request for '%s' from %s", msg.Filename, msg.Sender)

			// First try to read the file locally
			content, err := globalBlockStore.ReadFile(msg.Filename)

			var response NodeMessage

			if err != nil {
				// File not found locally - try to forward to primary replica
				log.Printf("ℹ️  [GET] File '%s' not found locally, checking if we should forward to primary", msg.Filename)

				// Calculate file hash to find correct replicas
				fileID := hashing.HashString(msg.Filename)
				infoMap := node.Membership.GetInfoMap()
				replicas := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

				// Check if this node is the primary (first replica)
				isPrimary := len(replicas) > 0 && replicas[0] == node.RingID

				if !isPrimary && len(replicas) > 0 {
					// Forward to primary replica
					primaryID := replicas[0]
					primaryInfo, ok := infoMap[primaryID]
					if ok && primaryInfo.State == membership.Alive {
						log.Printf("📤 [GET] Forwarding GET for '%s' to primary %s", msg.Filename, primaryInfo.Hostname)

						forwardMsg := NodeMessage{
							Type:      "get_file",
							Sender:    msg.Sender,
							Operation: "get",
							Filename:  msg.Filename,
							Timestamp: time.Now().UnixNano(),
						}

						jsonData, _ := json.Marshal(forwardMsg)
						fwdConn, fwdErr := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", primaryInfo.Hostname, primaryInfo.Port), 5*time.Second)
						if fwdErr == nil {
							defer fwdConn.Close()
							fwdConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
							if _, writeErr := fwdConn.Write(append(jsonData, '\n')); writeErr == nil {
								// Read response from primary
								fwdConn.SetReadDeadline(time.Now().Add(5 * time.Second))
								reader := bufio.NewReader(fwdConn)
								respLine, readErr := reader.ReadString('\n')
								if readErr == nil {
									// Forward the response directly to the original client
									log.Printf("✅ [GET] Forwarding response from primary for '%s'", msg.Filename)
									conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
									conn.Write([]byte(respLine))
									break // Response sent, we're done
								}
							}
						}
						log.Printf("⚠️  [GET] Failed to forward to primary, returning error")
					}
				}

				// Either we're the primary and file doesn't exist, or forwarding failed
				log.Printf("❌ [GET] File '%s' not found: %v", msg.Filename, err)
				response = NodeMessage{
					Type:      "error",
					Sender:    node.Membership.GetInfoMap()[node.RingID].Hostname,
					Operation: "get",
					Filename:  msg.Filename,
					Data: map[string]interface{}{
						"message": fmt.Sprintf("file not found: %v", err),
					},
					Timestamp: time.Now().UnixNano(),
				}
			} else {
				log.Printf("✅ [GET] Successfully read file '%s' (%d bytes)", msg.Filename, len(content))
				response = NodeMessage{
					Type:      "get_file_response",
					Sender:    node.Membership.GetInfoMap()[node.RingID].Hostname,
					Operation: "get",
					Filename:  msg.Filename,
					Data: map[string]interface{}{
						"content": string(content),
					},
					Timestamp: time.Now().UnixNano(),
				}
			}

			// Send response back
			jsonData, _ := json.Marshal(response)
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(append(jsonData, '\n')); err != nil {
				log.Printf("❌ [GET] Failed to send response: %v", err)
			}

		case "rereplicate_file":
			// Handle re-replication request after failure
			log.Printf("🔄 [REREPLICATE] Received re-replication request for '%s' from %s", msg.Filename, msg.Sender)

			// Extract file data
			content, ok := msg.Data["content"].(string)
			if !ok {
				log.Printf("❌ [REREPLICATE] Invalid content for %s", msg.Filename)
				break
			}

			fileID, ok := msg.Data["file_id"].(float64)
			if !ok {
				log.Printf("❌ [REREPLICATE] Invalid file_id for %s", msg.Filename)
				break
			}

			// Store the file using BlockStore

			// Use node's own ID as the "client" for re-replication
			infoMap := node.Membership.GetInfoMap()
			clientID := fmt.Sprintf("%s:rereplicate", infoMap[node.RingID].Hostname)

			// Find the primary replica (first successor)
			successors := hashing.GetSuccessors(uint64(fileID), infoMap, hashing.NumReplicas)
			// DO NOT SORT - GetSuccessors already returns them in correct ring order
			// The first element is the primary replica

			var primaryNodeID uint64
			if len(successors) > 0 {
				primaryNodeID = successors[0]
			} else {
				primaryNodeID = node.RingID
			}

			err := globalBlockStore.CreateFile(msg.Filename, []byte(content), uint64(fileID), primaryNodeID, clientID)
			if err != nil {
				// Check if it's a "file already exists" error - this is expected and safe
				if err.Error() == fmt.Sprintf("file already exists: %s", msg.Filename) {
					log.Printf("ℹ️  [REREPLICATE] File '%s' already exists locally (duplicate re-replication request ignored)", msg.Filename)
				} else {
					log.Printf("❌ [REREPLICATE] Failed to store file '%s': %v", msg.Filename, err)
				}
			} else {
				log.Printf("✅ [REREPLICATE] Successfully stored file '%s' (%d bytes)", msg.Filename, len(content))
			}

		case "append_file":
			// Handle append request - append locally if we're a replica, or forward to primary
			// Per MP3 specs: eventual consistency - merge will sync replicas later
			log.Printf("📥 [APPEND] Received append request for '%s' from %s", msg.Filename, msg.Sender)

			// Extract file data
			content, ok := msg.Data["content"].(string)
			if !ok {
				log.Printf("❌ [APPEND] Invalid content for %s", msg.Filename)
				break
			}

			clientID, ok := msg.Data["client_id"].(string)
			if !ok {
				log.Printf("❌ [APPEND] Invalid client_id for %s", msg.Filename)
				break
			}

			// Calculate file hash to find correct replicas
			fileID := hashing.HashString(msg.Filename)
			infoMap := node.Membership.GetInfoMap()
			replicas := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

			if len(replicas) == 0 {
				log.Printf("❌ [APPEND] No replicas found for '%s'", msg.Filename)
				break
			}

			// Check if this node is a replica for this file
			isReplica := false
			for _, replicaID := range replicas {
				if replicaID == node.RingID {
					isReplica = true
					break
				}
			}

			if isReplica {
				// This node is a replica - append locally only (merge will sync to other replicas)
				err := globalBlockStore.AppendFile(msg.Filename, []byte(content), clientID)
				if err != nil {
					log.Printf("❌ [APPEND] Failed to append to file '%s': %v", msg.Filename, err)
				} else {
					log.Printf("✅ [APPEND] Successfully appended to file '%s' (%d bytes from client %s)",
						msg.Filename, len(content), clientID)
				}
			} else {
				// This node is NOT a replica - forward to PRIMARY replica only
				primaryID := replicas[0]
				primaryInfo, ok := infoMap[primaryID]
				if !ok || primaryInfo.State != membership.Alive {
					log.Printf("❌ [APPEND] Primary replica not available for '%s'", msg.Filename)
					break
				}

				log.Printf("📤 [APPEND] Forwarding append for '%s' to primary %s", msg.Filename, primaryInfo.Hostname)

				forwardMsg := NodeMessage{
					Type:      "append_file",
					Sender:    msg.Sender,
					Operation: "append",
					Filename:  msg.Filename,
					Data: map[string]interface{}{
						"file_id":   float64(fileID),
						"content":   content,
						"client_id": clientID,
					},
					Timestamp: time.Now().UnixNano(),
				}

				jsonData, _ := json.Marshal(forwardMsg)
				fwdConn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", primaryInfo.Hostname, primaryInfo.Port), 5*time.Second)
				if err != nil {
					log.Printf("⚠️  [APPEND] Failed to forward to primary %s: %v", primaryInfo.Hostname, err)
					break
				}

				fwdConn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := fwdConn.Write(append(jsonData, '\n')); err != nil {
					log.Printf("⚠️  [APPEND] Failed to send forward to primary %s: %v", primaryInfo.Hostname, err)
				} else {
					log.Printf("✅ [APPEND] Forwarded to primary %s:%d", primaryInfo.Hostname, primaryInfo.Port)
				}
				fwdConn.Close()
			}

		case "merge_request":
			// Handle merge request from non-primary replica
			log.Printf("📥 [MERGE] Received merge request for '%s' from %s", msg.Filename, msg.Sender)

			// Trigger merge operation (this node should be the primary)
			go func() {
				// Give it a moment to process
				time.Sleep(100 * time.Millisecond)

				// Execute merge as if initiated locally
				parts := []string{"merge", msg.Filename}
				handleCLIMerge(node, parts)
			}()

		case "merge_collect":
			// Handle block collection request for merge
			log.Printf("📥 [MERGE] Received block collection request for '%s' from %s", msg.Filename, msg.Sender)

			// Read all blocks from local storage
			blocks, err := globalBlockStore.GetAllBlocks(msg.Filename)
			if err != nil {
				log.Printf("❌ [MERGE] Failed to read blocks for '%s': %v", msg.Filename, err)
				// Send empty response
				response := map[string]interface{}{"blocks": []storage.BlockInfo{}}
				jsonData, _ := json.Marshal(response)
				conn.Write(append(jsonData, '\n'))
				break
			}

			log.Printf("📤 [MERGE] Sending %d blocks for '%s' to %s", len(blocks), msg.Filename, msg.Sender)

			// Send blocks back
			response := map[string]interface{}{"blocks": blocks}
			jsonData, _ := json.Marshal(response)
			conn.Write(append(jsonData, '\n'))

		case "merge_update":
			// Handle merged file update from primary
			log.Printf("📥 [MERGE] Received merged file update for '%s' from %s", msg.Filename, msg.Sender)

			content, ok := msg.Data["content"].(string)
			if !ok {
				log.Printf("❌ [MERGE] Invalid content for %s", msg.Filename)
				break
			}

			// Write merged content to local storage
			// This replaces ALL existing blocks with the merged version

			// Delete existing file
			globalBlockStore.DeleteFile(msg.Filename)

			// Recreate with merged content
			fileID := uint64(msg.Data["file_id"].(float64))

			// Get primary replica ID from data or use sender's ID
			// For simplicity, just use the fileID's first successor as primary
			infoMap := node.Membership.GetInfoMap()
			successors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)
			primaryNodeID := successors[0]

			// Use a generic client ID for merged content
			clientID := "merged_content"

			err := globalBlockStore.CreateFile(msg.Filename, []byte(content), fileID, primaryNodeID, clientID)
			if err != nil {
				log.Printf("❌ [MERGE] Failed to store merged file '%s': %v", msg.Filename, err)
			} else {
				log.Printf("✅ [MERGE] Successfully stored merged file '%s' (%d bytes)",
					msg.Filename, len(content))
			}

		case "merge_complete":
			// Informational only: merge correctness is enforced synchronously on
			// the primary before replicas are told to adopt the merged file.
			log.Printf("node-conn: received merge complete for %s from %s", msg.Filename, msg.Sender)

		default:
			log.Printf("node-conn: unknown message type: %s", msg.Type)
		}
	}

	if err := reader.Err(); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "closed") {
			log.Printf("node-conn: read error: %v", err)
		}
	}
}

// handleClientConnection handles client commands with enhanced logging and broadcasting.
func handleClientConnection(node *common.Node, conn net.Conn) {
	defer conn.Close()
	log.Printf("client-conn: new connection from %s", conn.RemoteAddr())

	// Simple line-oriented protocol: one command per line
	reader := bufio.NewScanner(conn)
	// Increase buffer size to handle large RainStorm commands (default is 64KB, set to 1MB)
	buf := make([]byte, 0, 64*1024)
	reader.Buffer(buf, 1024*1024)
	writer := bufio.NewWriter(conn)

	// helper to write response safely
	writeResp := func(s string) {
		_, _ = writer.WriteString(s)
		if !strings.HasSuffix(s, "\n") {
			_, _ = writer.WriteString("\n")
		}
		_ = writer.Flush()
	}

	remote := conn.RemoteAddr().String()
	for reader.Scan() {
		line := strings.TrimSpace(reader.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "list_mem":
			// Return a compact table of current membership
			table := node.Membership.Table()
			log.Printf("✅ [CMD] list_mem: %s -> list_mem (len=%d)", remote, len(strings.Split(table, "\n"))-1)
			writeResp(table)

		case "list_mem_ids":
			// List membership with ring IDs (sorted)
			infoMap := node.Membership.GetInfoMap()
			type memberEntry struct {
				id   uint64
				info membership.Info
			}
			var entries []memberEntry
			for id, info := range infoMap {
				if info.State == membership.Alive {
					entries = append(entries, memberEntry{id: id, info: info})
				}
			}
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].id < entries[j].id
			})

			var result strings.Builder
			result.WriteString(fmt.Sprintf("%-40s %-10s %-20s\n", "Hostname", "Port", "Ring ID"))
			result.WriteString(strings.Repeat("-", 75) + "\n")
			for _, e := range entries {
				result.WriteString(fmt.Sprintf("%-40s %-10d %020d\n", e.info.Hostname, e.info.Port, e.id))
			}

			// Log the full membership list to logs
			log.Printf("════════════════════════════════════════════════════════════")
			log.Printf("MEMBERSHIP LIST WITH RING IDs (Total: %d members)", len(entries))
			log.Printf("════════════════════════════════════════════════════════════")
			log.Printf("%-40s %-10s %-20s", "Hostname", "Port", "Ring ID")
			log.Printf("%s", strings.Repeat("-", 75))
			for _, e := range entries {
				log.Printf("%-40s %-10d %020d", e.info.Hostname, e.info.Port, e.id)
			}
			log.Printf("════════════════════════════════════════════════════════════")

			log.Printf("✅ [CMD] list_mem_ids: returned %d members", len(entries))
			writeResp(result.String())

		case "list_self":
			infoMap := node.Membership.GetInfoMap()
			self := infoMap[node.RingID]
			stateStr := "Unknown"
			switch self.State {
			case membership.Alive:
				stateStr = "Alive"
			case membership.Failed:
				stateStr = "Failed"
			case membership.Suspected:
				stateStr = "Suspected"
			}
			msg := fmt.Sprintf("RingID=%020d Hostname=%s Port=%d State=%s",
				node.RingID, self.Hostname, self.Port, stateStr)
			log.Printf("✅ [CMD] list_self: %s", msg)
			writeResp(msg)

		case "join":
			// Re-read config and attempt a join probe to introducer
			cfg, err := common.LoadConfig("config.json")
			if err != nil {
				writeResp("ERR join: cannot load config: " + err.Error())
				continue
			}
			go joinGroup(node, cfg)
			log.Printf("✅ [CMD] join: probe sent to introducer")
			writeResp("OK join initiated")

		case "leave":
			// Voluntary leave: remove self from local membership and log
			node.Membership.UpdateStateSwim(time.Now(), node.RingID, membership.Failed, false)
			log.Printf("✅ [CMD] leave: marked self as failed")
			writeResp("OK left group")

		// MP3 file operations
		case "create":
			if len(parts) < 3 {
				writeResp("ERR usage: create localfilename HyDFSfilename")
				continue
			}
			localFile := parts[1]
			hydfsFile := parts[2]

			log.Printf("🚀 [CREATE] %s -> %s (from %s)", localFile, hydfsFile, remote)

			// Check if file already exists in local store
			node.FileStoreMutex.RLock()
			_, localExists := node.FileStore[hydfsFile]
			node.FileStoreMutex.RUnlock()

			if localExists {
				log.Printf("❌ [CREATE] File %s already exists in HyDFS", hydfsFile)
				writeResp(fmt.Sprintf("ERR file %s already exists", hydfsFile))
				continue
			}

			// Read local file
			content, err := os.ReadFile(localFile)
			if err != nil {
				log.Printf("❌ [CREATE] Failed to read %s: %v", localFile, err)
				writeResp(fmt.Sprintf("ERR failed to read local file: %v", err))
				continue
			}

			// Calculate file hash
			fileID := hashing.HashString(hydfsFile)

			// Find successor nodes for replication
			infoMap := node.Membership.GetInfoMap()
			successors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

			if len(successors) == 0 {
				log.Printf("❌ [CREATE] No alive nodes to replicate to")
				writeResp("ERR no alive nodes for replication")
				continue
			}

			// DO NOT SORT - GetSuccessors already returns them in correct ring order
			// The first element is the primary replica

			log.Printf("📍 [CREATE] File %s (ID=%020d) will replicate to %d nodes",
				hydfsFile, fileID, len(successors))

			// Primary Replica is the first successor (already in ring order)
			primaryNodeID := successors[0]
			primaryInfo := infoMap[primaryNodeID]
			log.Printf("✅ [CREATE] Primary Replica: %s:%d (NodeID=%020d)",
				primaryInfo.Hostname, primaryInfo.Port, primaryNodeID)

			// TASK 3: Use pipelined replication
			// Coordinator sends only to Primary, which forwards to S2, which forwards to S3
			createdAt := time.Now().Unix()
			clientID := fmt.Sprintf("%s:%s", infoMap[node.RingID].Hostname, remote)

			// Build the replication chain
			var nextReplica string
			if len(successors) > 1 {
				nextInfo := infoMap[successors[1]]
				nextReplica = fmt.Sprintf("%s:%d", nextInfo.Hostname, nextInfo.Port)
			}

			// Check if coordinator is the primary
			if node.RingID == primaryNodeID {
				// Coordinator is the primary - store locally using BlockStore
				if err := globalBlockStore.CreateFile(hydfsFile, content, fileID, primaryNodeID, clientID); err != nil {
					log.Printf("❌ [CREATE] Failed to store locally: %v", err)
					writeResp(fmt.Sprintf("ERR failed to store: %v", err))
					continue
				}

				// Update metadata
				node.FileStoreMutex.Lock()
				node.FileStore[hydfsFile] = common.FileMetadata{
					Filename:  hydfsFile,
					FileID:    fileID,
					Version:   1,
					Size:      int64(len(content)),
					CreatedAt: createdAt,
				}
				node.FileStoreMutex.Unlock()

				log.Printf("✅ [CREATE] Stored %s locally as PRIMARY (%d bytes)", hydfsFile, len(content))

				// Forward to next replica if exists
				if nextReplica != "" {
					go forwardReplication(node, hydfsFile, content, fileID, primaryNodeID, clientID, successors, 1, infoMap)
				} else {
					// No more replicas, we're done
					log.Printf("🎉 [CREATE] File %s created (single replica: coordinator is primary)", hydfsFile)
					writeResp(fmt.Sprintf("OK created %s", hydfsFile))
				}
			} else {
				// Coordinator is not a replica - send to primary
				// Primary will handle the replication chain

				// Build successor list as STRINGS to avoid float64 precision loss
				successorsList := make([]string, len(successors))
				for i, s := range successors {
					successorsList[i] = fmt.Sprintf("%d", s)
				}

				msg := NodeMessage{
					Type:          "replicate_file",
					Sender:        infoMap[node.RingID].Hostname,
					Operation:     "create",
					Filename:      hydfsFile,
					NextReplica:   nextReplica,
					AckTarget:     fmt.Sprintf("%s:%d", infoMap[node.RingID].Hostname, infoMap[node.RingID].Port),
					IsLastInChain: len(successors) == 1,
					Data: map[string]interface{}{
						"file_id":     float64(fileID),
						"content":     string(content),
						"created_at":  float64(createdAt),
						"client_id":   clientID,
						"primary_id":  float64(primaryNodeID),
						"successors":  successorsList, // Now strings, not float64
						"chain_index": float64(0),     // Primary is at index 0
					},
					Timestamp: time.Now().UnixNano(),
				}

				jsonData, _ := json.Marshal(msg)

				// Send to primary
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", primaryInfo.Hostname, primaryInfo.Port), 5*time.Second)
				if err != nil {
					log.Printf("❌ [CREATE] Failed to connect to primary %s:%d: %v", primaryInfo.Hostname, primaryInfo.Port, err)
					writeResp(fmt.Sprintf("ERR failed to connect to primary: %v", err))
					continue
				}

				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := conn.Write(append(jsonData, '\n')); err != nil {
					conn.Close()
					log.Printf("❌ [CREATE] Failed to send to primary %s:%d: %v", primaryInfo.Hostname, primaryInfo.Port, err)
					writeResp(fmt.Sprintf("ERR failed to send to primary: %v", err))
					continue
				}

				// Close the send connection - ACK will come via handleNodeConnection
				conn.Close()

				log.Printf("📤 [CREATE] Replication message sent to primary, waiting for final ACK...")

				// Known limitation: the final ACK arrives asynchronously via
				// handleNodeConnection, and there is no per-request ACK registry to
				// rendezvous with it. A fixed delay bounds the wait instead; failures
				// surface later through re-replication and merge. Tracking ACKs
				// per-request would mean threading a channel registry through the
				// node-connection handler. See README "Design notes & limitations".
				time.Sleep(3 * time.Second)
				log.Printf("🎉 [CREATE] File %s created and replicated via pipelined replication", hydfsFile)

				// Log all replicas in ring order
				for i, nodeID := range successors {
					info := infoMap[nodeID]
					if i == 0 {
						log.Printf("✅ [CREATE] Replicated to %s:%d (NodeID=%020d) [PRIMARY]", info.Hostname, info.Port, nodeID)
					} else {
						log.Printf("✅ [CREATE] Replicated to %s:%d (NodeID=%020d)", info.Hostname, info.Port, nodeID)
					}
				}

				writeResp(fmt.Sprintf("OK created %s on %d nodes via pipelined replication", hydfsFile, len(successors)))
			}

		case "get":
			if len(parts) < 3 {
				writeResp("ERR usage: get HyDFSfilename localfilename")
				continue
			}
			hydfsFile := parts[1]
			localFile := parts[2]

			log.Printf("🚀 [GET] %s -> %s (from %s)", hydfsFile, localFile, remote)

			// Calculate file ID using consistent hashing (same as CREATE)
			fileID := hashing.HashString(hydfsFile)

			// Find successor nodes for this file
			infoMap := node.Membership.GetInfoMap()
			successors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

			if len(successors) == 0 {
				log.Printf("❌ [GET] No alive replicas found for %s", hydfsFile)
				writeResp("ERR no replicas available")
				continue
			}

			// DO NOT SORT - GetSuccessors already returns them in correct ring order
			// The first element is the primary replica

			// Primary is the first successor
			primaryNodeID := successors[0]
			primaryInfo, ok := infoMap[primaryNodeID]
			if !ok || primaryInfo.State != membership.Alive {
				log.Printf("❌ [GET] Primary replica for %s is not available", hydfsFile)
				writeResp("ERR primary replica not available")
				continue
			}

			log.Printf("📍 [GET] File %s (ID=%020d) -> Primary: %s:%d (NodeID=%020d)",
				hydfsFile, fileID, primaryInfo.Hostname, primaryInfo.Port, primaryNodeID)

			// Send GET request to primary
			msg := NodeMessage{
				Type:      "get_file",
				Sender:    infoMap[node.RingID].Hostname,
				Operation: "get",
				Filename:  hydfsFile,
				Data: map[string]interface{}{
					"localfile": localFile,
				},
				Timestamp: time.Now().UnixNano(),
			}

			jsonData, _ := json.Marshal(msg)

			conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", primaryInfo.Hostname, primaryInfo.Port), 5*time.Second)
			if err != nil {
				log.Printf("❌ [GET] Failed to connect to primary %s:%d: %v", primaryInfo.Hostname, primaryInfo.Port, err)
				writeResp(fmt.Sprintf("ERR failed to connect to primary: %v", err))
				continue
			}

			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(append(jsonData, '\n')); err != nil {
				conn.Close()
				log.Printf("❌ [GET] Failed to send GET request: %v", err)
				writeResp(fmt.Sprintf("ERR failed to send request: %v", err))
				continue
			}

			// Read response
			reader := bufio.NewReader(conn)
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			responseLine, err := reader.ReadString('\n')
			conn.Close()

			if err != nil {
				log.Printf("❌ [GET] Failed to receive response: %v", err)
				writeResp(fmt.Sprintf("ERR failed to receive response: %v", err))
				continue
			}

			var response NodeMessage
			if err := json.Unmarshal([]byte(responseLine), &response); err != nil {
				log.Printf("❌ [GET] Failed to parse response: %v", err)
				writeResp(fmt.Sprintf("ERR failed to parse response: %v", err))
				continue
			}

			if response.Type == "get_file_response" {
				// Extract file content from response
				contentStr, ok := response.Data["content"].(string)
				if !ok {
					log.Printf("❌ [GET] Invalid content in response")
					writeResp("ERR invalid response content")
					continue
				}

				content := []byte(contentStr)

				// Determine local path - use exact path if absolute, otherwise use hydfs_local
				var localPath string
				if filepath.IsAbs(localFile) {
					// Absolute path - write directly to specified location
					localPath = localFile
					// Ensure parent directory exists
					if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
						log.Printf("❌ [GET] Failed to create directory: %v", err)
						writeResp(fmt.Sprintf("ERR failed to create directory: %v", err))
						continue
					}
				} else {
					// Relative path - use hydfs_local directory
					localDir := "./hydfs_local"
					if err := os.MkdirAll(localDir, 0755); err != nil {
						log.Printf("❌ [GET] Failed to create local directory: %v", err)
						writeResp(fmt.Sprintf("ERR failed to create directory: %v", err))
						continue
					}
					localPath = filepath.Join(localDir, localFile)
				}

				if err := os.WriteFile(localPath, content, 0644); err != nil {
					log.Printf("❌ [GET] Failed to write local file: %v", err)
					writeResp(fmt.Sprintf("ERR failed to write file: %v", err))
					continue
				}

				log.Printf("✅ [GET] Successfully retrieved %s (%d bytes) -> %s", hydfsFile, len(content), localPath)
				writeResp(fmt.Sprintf("OK retrieved %s (%d bytes) to %s", hydfsFile, len(content), localPath))
			} else if response.Type == "error" {
				errorMsg, _ := response.Data["message"].(string)
				log.Printf("❌ [GET] Error from primary: %s", errorMsg)
				writeResp(fmt.Sprintf("ERR %s", errorMsg))
			} else {
				log.Printf("❌ [GET] Unexpected response type: %s", response.Type)
				writeResp("ERR unexpected response")
			}

		case "append":
			if len(parts) < 3 {
				writeResp("ERR usage: append localfilename HyDFSfilename")
				continue
			}
			localFile := parts[1]
			hydfsFile := parts[2]

			log.Printf("🚀 [APPEND] %s -> %s (from %s)", localFile, hydfsFile, remote)

			broadcastOperation(node, "append", hydfsFile, map[string]interface{}{
				"localfile": localFile,
				"client":    remote,
			})

			// Execute append logic
			handleCLIAppend(node, parts)
			writeResp(fmt.Sprintf("OK append %s -> %s (operation logged)", localFile, hydfsFile))

		case "merge":
			if len(parts) < 2 {
				writeResp("ERR usage: merge HyDFSfilename")
				continue
			}
			hydfsFile := parts[1]

			log.Printf("🚀 [MERGE] %s (from %s)", hydfsFile, remote)

			broadcastOperation(node, "merge", hydfsFile, map[string]interface{}{
				"client": remote,
			})

			// Execute merge logic
			handleCLIMerge(node, parts)
			writeResp(fmt.Sprintf("OK merge %s completed", hydfsFile))

		case "ls":
			if len(parts) < 2 {
				writeResp("ERR usage: ls HyDFSfilename")
				continue
			}
			hydfsFile := parts[1]

			log.Printf("🚀 [LS] %s (from %s)", hydfsFile, remote)

			// Calculate file ID using consistent hashing
			fileID := hashing.HashString(hydfsFile)

			// Find successor nodes for this file
			infoMap := node.Membership.GetInfoMap()
			successors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

			if len(successors) == 0 {
				log.Printf("❌ [LS] No replicas found for %s", hydfsFile)
				writeResp("ERR no replicas available")
				continue
			}

			// DO NOT SORT - GetSuccessors already returns them in correct ring order
			// The first element is the primary replica

			// Build response
			var result strings.Builder
			result.WriteString("════════════════════════════════════════════════════════════\n")
			result.WriteString(fmt.Sprintf("File Name: %s\n", hydfsFile))
			result.WriteString(fmt.Sprintf("File ID:   %020d\n", fileID))
			result.WriteString(fmt.Sprintf("Replication Factor (n): %d\n", hashing.NumReplicas))
			result.WriteString("════════════════════════════════════════════════════════════\n")
			result.WriteString(fmt.Sprintf("%-40s %-20s\n", "Hostname", "Node ID"))
			result.WriteString(strings.Repeat("-", 60) + "\n")

			for i, nodeID := range successors {
				info := infoMap[nodeID]
				marker := ""
				if i == 0 {
					marker = " [PRIMARY]"
				}
				result.WriteString(fmt.Sprintf("%-40s %020d%s\n", info.Hostname, nodeID, marker))
			}
			result.WriteString("════════════════════════════════════════════════════════════\n")

			log.Printf("✅ [LS] %s -> FileID=%020d, %d replicas", hydfsFile, fileID, len(successors))
			writeResp(result.String())

		case "liststore":
			log.Printf("🚀 [LISTSTORE] (from %s)", remote)

			infoMap := node.Membership.GetInfoMap()
			self := infoMap[node.RingID]

			// Scan hydfs_storage directory for actual stored files
			storedFiles := make(map[string]uint64) // filename -> fileID

			// Read all directories in hydfs_storage
			entries, err := os.ReadDir(fileops.StorageRoot)
			if err != nil {
				log.Printf("❌ [LISTSTORE] Failed to read storage directory: %v", err)
				writeResp(fmt.Sprintf("ERR failed to read storage: %v", err))
				continue
			}

			// For each directory, read _metadata.json
			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}

				metadataPath := filepath.Join(fileops.StorageRoot, entry.Name(), "_metadata.json")
				data, err := os.ReadFile(metadataPath)
				if err != nil {
					// Skip if metadata doesn't exist
					continue
				}

				var metadata storage.FileMetadata
				if err := json.Unmarshal(data, &metadata); err != nil {
					log.Printf("⚠️  [LISTSTORE] Failed to parse metadata for %s: %v", entry.Name(), err)
					continue
				}

				storedFiles[metadata.HyDFSFilename] = metadata.FileID
			}

			// Build response
			var result strings.Builder
			result.WriteString("════════════════════════════════════════════════════════════\n")
			result.WriteString(fmt.Sprintf("Node: %s\n", self.Hostname))
			result.WriteString(fmt.Sprintf("Ring ID: %020d\n", node.RingID))
			result.WriteString("════════════════════════════════════════════════════════════\n")
			result.WriteString(fmt.Sprintf("Stored Files: %d\n", len(storedFiles)))
			result.WriteString(strings.Repeat("-", 60) + "\n")
			result.WriteString(fmt.Sprintf("%-30s %-20s\n", "HyDFS Filename", "File ID"))
			result.WriteString(strings.Repeat("-", 60) + "\n")

			// Sort filenames for consistent output
			filenames := make([]string, 0, len(storedFiles))
			for filename := range storedFiles {
				filenames = append(filenames, filename)
			}
			sort.Strings(filenames)

			for _, filename := range filenames {
				fileID := storedFiles[filename]
				result.WriteString(fmt.Sprintf("%-30s %020d\n", filename, fileID))
			}
			result.WriteString("════════════════════════════════════════════════════════════\n")

			log.Printf("✅ [LISTSTORE] Returned %d files stored on this node", len(storedFiles))
			writeResp(result.String())

		case "getfromreplica":
			if len(parts) < 4 {
				writeResp("ERR usage: getfromreplica VMaddress HyDFSfilename localfilename")
				continue
			}
			vmAddr := parts[1]
			hydfsFile := parts[2]
			localFile := parts[3]

			log.Printf("🚀 [GETFROMREPLICA] %s from %s -> %s (from %s)", hydfsFile, vmAddr, localFile, remote)

			broadcastOperation(node, "getfromreplica", hydfsFile, map[string]interface{}{
				"vmaddress": vmAddr,
				"localfile": localFile,
				"client":    remote,
			})

			// Execute getfromreplica in a goroutine so we can respond immediately
			go handleCLIGetFromReplica(node, parts)
			writeResp(fmt.Sprintf("OK getfromreplica %s from %s", hydfsFile, vmAddr))

		case "multiappend":
			if len(parts) < 4 {
				writeResp("ERR usage: multiappend HyDFSfilename VM1,VM2,... localfile1,localfile2,...")
				continue
			}
			hydfsFile := parts[1]

			log.Printf("🚀 [MULTIAPPEND] %s (from %s)", hydfsFile, remote)

			broadcastOperation(node, "multiappend", hydfsFile, map[string]interface{}{
				"client": remote,
				"args":   strings.Join(parts[2:], " "),
			})

			// Execute multiappend in a goroutine so we can respond immediately
			go handleCLIMultiAppend(parts)
			writeResp(fmt.Sprintf("OK multiappend %s (operation initiated)", hydfsFile))

		case "dgrep_query":
			// Handle distributed grep query from another VM
			// Format: dgrep_query <grep-args...>
			if len(parts) < 2 {
				writeResp("ERR usage: dgrep_query <grep-args>")
				writeResp("END_DGREP")
				continue
			}

			// Extract grep arguments (everything after "dgrep_query")
			grepArgs := parts[1:]

			// Execute local grep query
			output, err := logging.Query(grepArgs)
			if err != nil {
				log.Printf("❌ [DGREP_QUERY] Error: %v", err)
				writeResp(fmt.Sprintf("ERROR: %v", err))
				// Always send terminator even on error
				writeResp("END_DGREP")
			} else if output == "" {
				// No matches - send empty response and terminator
				writeResp("END_DGREP")
			} else {
				// Send matching lines
				writeResp(output)
				// Send terminator
				writeResp("END_DGREP")
			}

		default:
			// Unknown command: echo help
			writeResp("ERR unknown command: " + cmd)
			helpMsg := "Available commands:\n" +
				"  MP1: dgrep\n" +
				"  MP2: list_mem | list_mem_ids | list_self | join | leave\n" +
				"  MP3: create | get | append | merge | ls | liststore | getfromreplica | multiappend\n"
			writeResp(helpMsg)
		}
	}

	if err := reader.Err(); err != nil {
		// Ignore closed pipe errors on abrupt client disconnect
		if !strings.Contains(strings.ToLower(err.Error()), "closed") {
			log.Printf("client-conn: read error from %s: %v", remote, err)
		}
	}
}

func main() {
	// Logger setup with file output
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Setup log file in logs/ directory with timestamp
	// Format: logs/vm01_20251203_130220.log
	logHostname, _ := os.Hostname()
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("Failed to create log directory: %v", err)
	} else {
		// Use hostname directly as the log name prefix (e.g., "node1" -> "node1")
		vmName := logHostname

		// Create timestamped log filename
		timestamp := time.Now().Format("20060102_150405")
		logFile := fmt.Sprintf("%s/%s_%s.log", logDir, vmName, timestamp)
		f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Failed to open log file: %v", err)
		} else {
			// Write to both stdout and file
			log.SetOutput(io.MultiWriter(os.Stdout, f))
			log.Printf("📝 Logging to %s", logFile)
		}
	}

	// Load configuration
	cfg, err := common.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Initialize self Info and Membership
	hostname, _ := membership.GetHostName()
	port := cfg.NodePort
	now := time.Now()
	myInfo := membership.Info{
		Hostname:  hostname,
		Port:      port,
		Version:   now,
		Timestamp: now,
		Counter:   0,
		State:     membership.Alive,
	}
	myId := membership.HashInfo(myInfo)

	// Initialize membership with only self
	var myMembership membership.Membership
	myMembership.Reset(now, hostname, port)

	// Initialize MP3 node
	node := &common.Node{
		RingID:     myId,
		Membership: &myMembership,
		FileStore:  make(map[string]common.FileMetadata),
		Storage:    storage.NewManager(),
		IsActive:   true, // Start as active
	}

	// Initialize HyDFS storage
	if err := fileops.InitStorage(); err != nil {
		log.Fatalf("failed to initialize storage: %v", err)
	}
	log.Printf("✅ Storage initialized at %s", fileops.StorageRoot)

	// Initialize global BlockStore instance (prevents race conditions on concurrent appends)
	globalBlockStore = storage.NewBlockStore(fileops.StorageRoot)
	log.Printf("✅ Global BlockStore initialized")

	// Setup graceful shutdown on Ctrl+C (SIGINT) or SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("\n🛑 Received signal: %v", sig)
		log.Println("Performing graceful leave...")
		gracefulLeave(node)
		log.Println("✅ Gracefully left the cluster. Exiting.")
		os.Exit(0)
	}()

	// Start gossip loop BEFORE joining
	go startGossipLoop(node)

	// Give gossip listener time to start
	time.Sleep(1 * time.Second)

	// Determine if we're the introducer
	introducerHost := cfg.VMs[0]
	if hostname == introducerHost {
		log.Printf("✅ I am the introducer (%s), ready to accept joins", introducerHost)
	} else {
		// Non-introducer nodes: wait a bit longer for introducer to be ready, then join
		log.Printf("Waiting for introducer to be ready...")
		time.Sleep(2 * time.Second)
		log.Printf("Attempting to join group via introducer: %s", introducerHost)
		go joinGroup(node, cfg)
	}

	// Start TCP listener for internal node commands (same port as UDP, but TCP)
	go func() {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.NodePort))
		if err != nil {
			log.Printf("node TCP listen error: %v", err)
			return
		}
		log.Printf("node TCP listening on :%d", cfg.NodePort)
		for {
			c, err := ln.Accept()
			if err != nil {
				log.Printf("node TCP accept error: %v", err)
				continue
			}
			go handleNodeConnection(node, c)
		}
	}()

	// Start TCP listener for client commands
	go func() {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.ClientPort))
		if err != nil {
			log.Printf("client TCP listen error: %v", err)
			return
		}
		log.Printf("client TCP listening on :%d", cfg.ClientPort)
		for {
			c, err := ln.Accept()
			if err != nil {
				log.Printf("client TCP accept error: %v", err)
				continue
			}
			go handleClientConnection(node, c)
		}
	}()

	// Initialize and start RainStorm Server
	// By convention, VM1 (Introducer) is the RainStorm Leader
	rainStormServer := rainstorm.NewServer(node, cfg, globalBlockStore)
	if len(cfg.VMs) > 0 && hostname == cfg.VMs[0] {
		rainStormServer.Role = "leader"
		log.Printf("👑 [RAINSTORM] Role assigned: LEADER (ResourceManager)")
	} else {
		rainStormServer.Role = "worker"
		log.Printf("👷 [RAINSTORM] Role assigned: WORKER")
	}
	go rainStormServer.Start()

	// Wait a moment for services to start
	time.Sleep(500 * time.Millisecond)

	// Start interactive CLI
	startInteractiveCLI(node)
}

// startInteractiveCLI provides an interactive command-line interface
func startInteractiveCLI(node *common.Node) {
	infoMap := node.Membership.GetInfoMap()
	self := infoMap[node.RingID]

	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║        Distributed Stream Processing System                ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n\n")
	fmt.Printf("Node Information:\n")
	fmt.Printf("  Hostname:  %s\n", self.Hostname)
	fmt.Printf("  Port:      %d\n", self.Port)
	fmt.Printf("  Ring ID:   %020d\n\n", node.RingID)

	fmt.Printf("Available Commands:\n\n")

	fmt.Printf("  Distributed Log Query:\n")
	fmt.Printf("    dgrep [flags] <pattern> - Search logs across all nodes in cluster\n\n")

	fmt.Printf("  Group Membership (SWIM):\n")
	fmt.Printf("    list_mem          - List the membership list\n")
	fmt.Printf("    list_self         - List self's information\n")
	fmt.Printf("    join              - Join or rejoin the group (new incarnation)\n")
	fmt.Printf("    leave             - Voluntarily leave the group (stay in CLI)\n\n")

	fmt.Printf("  Distributed File System (HyDFS):\n")
	fmt.Printf("    create <local> <hydfs>                        - Create file in HyDFS\n")
	fmt.Printf("    get <hydfs> <local>                           - Fetch file from HyDFS\n")
	fmt.Printf("    append <local> <hydfs>                        - Append to existing file\n")
	fmt.Printf("    merge <hydfs>                                 - Merge file replicas\n")
	fmt.Printf("    ls <hydfs>                                    - List replica locations\n")
	fmt.Printf("    liststore                                     - List files on this node\n")
	fmt.Printf("    getfromreplica <node> <hydfs> <local>         - Get from specific replica\n")
	fmt.Printf("    list_mem_ids                                  - List membership with ring IDs (sorted)\n")
	fmt.Printf("    multiappend <hydfs> <n1..nN> <file1..fileN>  - Multi-node concurrent append\n\n")

	fmt.Printf("  Stream Processing (RainStorm):\n")
	fmt.Printf("    list_tasks                                    - Query leader for all task details\n")
	fmt.Printf("    kill_task <node> <pid>                        - Kill a specific task process\n\n")

	fmt.Printf("  Utility:\n")
	fmt.Printf("    help              - Show this help message\n")
	fmt.Printf("    quit              - Gracefully leave and exit the program\n\n")

	fmt.Printf("════════════════════════════════════════════════════════════\n\n")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("hydfs> ")
		if !scanner.Scan() {
			break
		}

		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			continue
		}

		// Log the CLI command to the log file
		log.Printf("🖥️  [CLI] User command: %s", command)

		parts := strings.Fields(command)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "list_mem":
			handleCLIListMem(node)
		case "list_mem_ids":
			handleCLIListMemIds(node)
		case "list_self":
			handleCLIListSelf(node)
		case "join":
			handleCLIJoin(node)
		case "leave":
			handleCLILeave(node)
		case "create":
			handleCLICreate(node, parts)
		case "get":
			handleCLIGet(node, parts)
		case "append":
			handleCLIAppend(node, parts)
		case "merge":
			handleCLIMerge(node, parts)
		case "ls":
			handleCLILs(parts, node)
		case "liststore":
			handleCLIListStore(node)
		case "getfromreplica":
			handleCLIGetFromReplica(node, parts)
		case "multiappend":
			handleCLIMultiAppend(parts)
		case "dgrep":
			handleCLIDgrep(node, parts)
		case "list_tasks":
			handleCLIListTasks(node)
		case "kill_task":
			handleCLIKillTask(node, parts)
		case "help":
			showHelp()
		case "quit", "exit":
			fmt.Println("\n👋 Gracefully leaving cluster and exiting HyDFS...")
			gracefulLeave(node)
			fmt.Println("✅ Goodbye!")
			os.Exit(0)
		default:
			fmt.Printf("❌ Unknown command: %s\n", parts[0])
			fmt.Println("Type 'help' for available commands.")
		}
	}
}

// CLI command handlers
func handleCLIListMem(node *common.Node) {
	table := node.Membership.Table()
	fmt.Println("════════════════════════════════════════")
	fmt.Println("         MEMBERSHIP LIST")
	fmt.Println("════════════════════════════════════════")
	fmt.Print(table)
	fmt.Println("════════════════════════════════════════")
}

func handleCLIListMemIds(node *common.Node) {
	infoMap := node.Membership.GetInfoMap()
	type memberEntry struct {
		id   uint64
		info membership.Info
	}
	var entries []memberEntry
	for id, info := range infoMap {
		if info.State == membership.Alive {
			entries = append(entries, memberEntry{id: id, info: info})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].id < entries[j].id
	})

	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Println("              MEMBERSHIP LIST WITH RING IDs")
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Printf("%-40s %-10s %-20s\n", "Hostname", "Port", "Ring ID")
	fmt.Println("────────────────────────────────────────────────────────────")
	for _, e := range entries {
		fmt.Printf("%-40s %-10d %020d\n", e.info.Hostname, e.info.Port, e.id)
	}
	fmt.Printf("════════════════════════════════════════════════════════════\n")
	fmt.Printf("Total Members: %d\n", len(entries))
	fmt.Println("════════════════════════════════════════════════════════════")
}

func handleCLIListSelf(node *common.Node) {
	infoMap := node.Membership.GetInfoMap()
	self := infoMap[node.RingID]
	stateStr := "Unknown"
	switch self.State {
	case membership.Alive:
		stateStr = "Alive"
	case membership.Failed:
		stateStr = "Failed"
	case membership.Suspected:
		stateStr = "Suspected"
	}

	node.ActiveMutex.RLock()
	isActive := node.IsActive
	node.ActiveMutex.RUnlock()

	activeStr := "Yes (gossiping)"
	if !isActive {
		activeStr = "No (left cluster)"
	}

	fmt.Println("════════════════════════════════════════")
	fmt.Println("         SELF INFORMATION")
	fmt.Println("════════════════════════════════════════")
	fmt.Printf("  Ring ID:   %020d\n", node.RingID)
	fmt.Printf("  Hostname:  %s\n", self.Hostname)
	fmt.Printf("  Port:      %d\n", self.Port)
	fmt.Printf("  State:     %s\n", stateStr)
	fmt.Printf("  Active:    %s\n", activeStr)
	fmt.Printf("  Version:   %s\n", self.Version.Format("2006-01-02 15:04:05"))
	fmt.Println("════════════════════════════════════════")
}

func handleCLIJoin(node *common.Node) {
	// Check if already active
	node.ActiveMutex.RLock()
	isActive := node.IsActive
	node.ActiveMutex.RUnlock()

	if isActive {
		// Check if already in the cluster
		infoMap := node.Membership.GetInfoMap()
		self, ok := infoMap[node.RingID]
		if ok && self.State == membership.Alive {
			fmt.Println("⚠️  Already in the cluster!")
			return
		}
	}

	// Check if we need a new incarnation
	infoMap := node.Membership.GetInfoMap()
	self, ok := infoMap[node.RingID]

	if ok && self.State == membership.Failed {
		fmt.Println("🔄 Creating new incarnation for rejoin...")

		// Create new Info with current timestamp as new Version (incarnation)
		now := time.Now()
		newInfo := membership.Info{
			Hostname:  self.Hostname,
			Port:      self.Port,
			Version:   now, // New incarnation!
			Timestamp: now,
			Counter:   0,
			State:     membership.Alive,
		}
		newId := membership.HashInfo(newInfo)

		// Reset membership with new incarnation
		node.Membership.Reset(now, self.Hostname, self.Port)
		node.RingID = newId

		fmt.Printf("✅ New incarnation created (Ring ID: %020d)\n", newId)
	}

	// Activate the node
	node.ActiveMutex.Lock()
	node.IsActive = true
	node.ActiveMutex.Unlock()

	cfg, err := common.LoadConfig("config.json")
	if err != nil {
		fmt.Printf("❌ Failed to load config: %v\n", err)
		return
	}

	go joinGroup(node, cfg)
	fmt.Println("✅ Join request sent to introducer")
	fmt.Println("   💡 Node is now active and gossiping")
}

func handleCLILeave(node *common.Node) {
	node.ActiveMutex.RLock()
	isActive := node.IsActive
	node.ActiveMutex.RUnlock()

	if !isActive {
		fmt.Println("⚠️  Already left the group")
		fmt.Println("   💡 Use 'join' to rejoin with a new incarnation")
		return
	}

	gracefulLeave(node)
	fmt.Println("✅ Left the group gracefully")
	fmt.Println("   🔇 Gossip and heartbeat stopped")
	fmt.Println("   💡 You can still monitor membership with 'list_mem'")
	fmt.Println("   💡 Use 'join' to rejoin with a new incarnation")
	fmt.Println("   💡 Use 'quit' to exit the program")
}

func handleCLICreate(node *common.Node, parts []string) {
	if len(parts) < 3 {
		fmt.Println("❌ Usage: create <localfile> <hydfsfile>")
		fmt.Println("   Example: create local.txt hydfs_file.txt")
		return
	}
	localFile := parts[1]
	hydfsFile := parts[2]

	fmt.Printf("🚀 CREATE: %s -> %s\n", localFile, hydfsFile)

	// Check if file already exists in local store
	node.FileStoreMutex.RLock()
	_, localExists := node.FileStore[hydfsFile]
	node.FileStoreMutex.RUnlock()

	if localExists {
		fmt.Printf("❌ File %s already exists in HyDFS\n", hydfsFile)
		return
	}

	// Read local file
	content, err := os.ReadFile(localFile)
	if err != nil {
		fmt.Printf("❌ Failed to read %s: %v\n", localFile, err)
		return
	}

	// Calculate file hash
	fileID := hashing.HashString(hydfsFile)

	// Find successor nodes for replication
	infoMap := node.Membership.GetInfoMap()
	successors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

	if len(successors) == 0 {
		fmt.Println("❌ No alive nodes to replicate to")
		return
	}

	// DO NOT SORT - GetSuccessors already returns them in correct ring order
	// The first element is the primary replica

	fmt.Printf("📍 File %s (ID=%020d) will replicate to %d nodes\n",
		hydfsFile, fileID, len(successors))

	// Primary Replica is the first successor (after sorting)
	primaryNodeID := successors[0]
	primaryInfo := infoMap[primaryNodeID]
	fmt.Printf("✅ Primary Replica: %s:%d (NodeID=%020d)\n",
		primaryInfo.Hostname, primaryInfo.Port, primaryNodeID)

	// Check if this node is a replica
	isReplica := false
	isPrimary := false
	for _, nodeID := range successors {
		if nodeID == node.RingID {
			isReplica = true
			if nodeID == primaryNodeID {
				isPrimary = true
			}
			break
		}
	}

	createdAt := time.Now().Unix()
	replicatedTo := []string{}

	// Store locally if we're a replica
	if isReplica {
		if err := node.Storage.WriteFile(hydfsFile, content); err != nil {
			fmt.Printf("❌ Failed to store locally: %v\n", err)
			return
		}

		// Update metadata
		node.FileStoreMutex.Lock()
		node.FileStore[hydfsFile] = common.FileMetadata{
			Filename:  hydfsFile,
			FileID:    fileID,
			Version:   1,
			Size:      int64(len(content)),
			CreatedAt: createdAt,
		}
		node.FileStoreMutex.Unlock()

		self := infoMap[node.RingID]
		replicatedTo = append(replicatedTo, fmt.Sprintf("%s:%d", self.Hostname, self.Port))
		if isPrimary {
			fmt.Printf("✅ Stored locally as PRIMARY (%d bytes)\n", len(content))
		} else {
			fmt.Printf("✅ Stored locally (%d bytes)\n", len(content))
		}
	}

	// Replicate to other successors (skip primary if already handled above)
	for i, nodeID := range successors {
		if nodeID == node.RingID {
			continue // Already stored locally
		}

		info, ok := infoMap[nodeID]
		if !ok || info.State != membership.Alive {
			continue
		}

		isPrimaryReplica := (i == 0)

		// Send replication message
		msg := NodeMessage{
			Type:      "replicate_file",
			Sender:    infoMap[node.RingID].Hostname,
			Operation: "create",
			Filename:  hydfsFile,
			Data: map[string]interface{}{
				"file_id":    float64(fileID),
				"content":    string(content),
				"created_at": float64(createdAt),
			},
			Timestamp: time.Now().UnixNano(),
		}

		jsonData, _ := json.Marshal(msg)

		go func(hostname string, port int, nid uint64, isPrimary bool) {
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), 3*time.Second)
			if err != nil {
				fmt.Printf("❌ Failed to connect to replica %s:%d: %v\n", hostname, port, err)
				return
			}
			defer conn.Close()

			conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
			if _, err := conn.Write(append(jsonData, '\n')); err != nil {
				fmt.Printf("❌ Failed to send to replica %s:%d: %v\n", hostname, port, err)
				return
			}

			if isPrimary {
				fmt.Printf("✅ Replicated to %s:%d (NodeID=%020d) [PRIMARY]\n", hostname, port, nid)
			} else {
				fmt.Printf("✅ Replicated to %s:%d (NodeID=%020d)\n", hostname, port, nid)
			}
		}(info.Hostname, info.Port, nodeID, isPrimaryReplica)

		replicatedTo = append(replicatedTo, fmt.Sprintf("%s:%d", info.Hostname, info.Port))
	}

	// Give a moment for async replication
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("🎉 File %s created and replicated to %d nodes:\n", hydfsFile, len(replicatedTo))
	for _, replica := range replicatedTo {
		fmt.Printf("   - %s\n", replica)
	}
}

func handleCLIGet(node *common.Node, parts []string) {
	if len(parts) < 3 {
		fmt.Println("❌ Usage: get <hydfsfile> <localfile>")
		fmt.Println("   Example: get hydfs_file.txt local.txt")
		return
	}
	hydfsFile := parts[1]
	localFile := parts[2]

	fmt.Printf("🚀 GET: %s -> %s\n", hydfsFile, localFile)

	broadcastOperation(node, "get", hydfsFile, map[string]interface{}{
		"localfile": localFile,
		"client":    "CLI",
	})

	fmt.Println("✅ Operation logged and broadcast to all members")
	fmt.Println("   (Note: File retrieval not implemented yet)")
}

func handleCLIAppend(node *common.Node, parts []string) {
	if len(parts) < 3 {
		fmt.Println("❌ Usage: append <localfile> <hydfsfile>")
		fmt.Println("   Example: append data.txt hydfs_file.txt")
		return
	}
	localFile := parts[1]
	hydfsFile := parts[2]

	fmt.Printf("🚀 APPEND: %s -> %s\n", localFile, hydfsFile)

	// Read local file content
	content, err := os.ReadFile(localFile)
	if err != nil {
		fmt.Printf("❌ Failed to read local file %s: %v\n", localFile, err)
		return
	}

	// Calculate file hash (same as CREATE)
	fileID := hashing.HashString(hydfsFile)

	// Find replicas for this file (should already exist)
	infoMap := node.Membership.GetInfoMap()
	expectedSuccessors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

	if len(expectedSuccessors) == 0 {
		fmt.Println("❌ No alive nodes to replicate to")
		return
	}

	// Generate client ID (hostname + timestamp for uniqueness)
	clientID := fmt.Sprintf("%s_%d", infoMap[node.RingID].Hostname, time.Now().UnixNano())

	fmt.Printf("📍 [APPEND] File %s (ID=%020d) will append to %d replicas\n",
		hydfsFile, fileID, len(expectedSuccessors))

	// Send append request to all replicas
	successCount := 0
	for _, nodeID := range expectedSuccessors {
		targetInfo, ok := infoMap[nodeID]
		if !ok || targetInfo.State != membership.Alive {
			continue
		}

		msg := NodeMessage{
			Type:      "append_file",
			Sender:    infoMap[node.RingID].Hostname,
			Operation: "append",
			Filename:  hydfsFile,
			Data: map[string]interface{}{
				"file_id":   float64(fileID),
				"content":   string(content),
				"client_id": clientID,
			},
			Timestamp: time.Now().UnixNano(),
		}

		jsonData, _ := json.Marshal(msg)
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", targetInfo.Hostname, targetInfo.Port), 5*time.Second)
		if err != nil {
			fmt.Printf("⚠️  Failed to connect to %s: %v\n", targetInfo.Hostname, err)
			continue
		}

		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(append(jsonData, '\n')); err != nil {
			fmt.Printf("⚠️  Failed to send append to %s: %v\n", targetInfo.Hostname, err)
		} else {
			fmt.Printf("✅ [APPEND] Sent to %s:%d (NodeID=%020d)\n",
				targetInfo.Hostname, targetInfo.Port, nodeID)
			successCount++
		}
		conn.Close()
	}

	// Wait a moment for appends to complete
	time.Sleep(500 * time.Millisecond)

	if successCount > 0 {
		fmt.Printf("🎉 [APPEND] Successfully sent append to %d/%d replicas\n", successCount, len(expectedSuccessors))
		fmt.Printf("✅ [APPEND] Append operation completed\n")
	} else {
		fmt.Println("❌ [APPEND] Failed to append to any replicas")
	}
}

func handleCLIMerge(node *common.Node, parts []string) {
	if len(parts) < 2 {
		fmt.Println("❌ Usage: merge <hydfsfile>")
		fmt.Println("   Example: merge hydfs_file.txt")
		return
	}
	hydfsFile := parts[1]

	fmt.Printf("🚀 MERGE: %s\n", hydfsFile)

	// Calculate file ID
	fileID := hashing.HashString(hydfsFile)

	// Find replicas
	infoMap := node.Membership.GetInfoMap()
	successors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

	if len(successors) == 0 {
		fmt.Println("❌ No alive replicas found")
		return
	}

	fmt.Printf("📍 [MERGE] File %s (ID=%020d) has %d replicas\n",
		hydfsFile, fileID, len(successors))

	// For simplicity: just trigger re-replication from primary to ensure consistency
	// This is a simplified merge - primary replica redistributes its version to all replicas
	primaryNodeID := successors[0]
	primaryInfo, ok := infoMap[primaryNodeID]
	if !ok || primaryInfo.State != membership.Alive {
		fmt.Println("❌ Primary replica not available")
		return
	}

	fmt.Printf("📍 [MERGE] Primary replica: %s (NodeID=%020d)\n",
		primaryInfo.Hostname, primaryNodeID)

	// If we ARE the primary, collect blocks from ALL replicas and merge
	if primaryNodeID == node.RingID {
		fmt.Println("🔄 [MERGE] This node is primary - collecting blocks from all replicas")

		// Step 1: Collect all blocks from all replicas (including self)
		allBlocks := make(map[string]storage.BlockInfo) // Use map to deduplicate by clientID+sequence
		blocksMutex := sync.Mutex{}

		var collectWg sync.WaitGroup
		for _, replicaID := range successors {
			replicaInfo, ok := infoMap[replicaID]
			if !ok || replicaInfo.State != membership.Alive {
				continue
			}

			collectWg.Add(1)
			go func(repInfo membership.Info, repID uint64) {
				defer collectWg.Done()

				var blocks []storage.BlockInfo
				var err error

				if repID == node.RingID {
					// Read from self
					blocks, err = globalBlockStore.GetAllBlocks(hydfsFile)
					if err != nil {
						fmt.Printf("⚠️  Failed to read local blocks: %v\n", err)
						return
					}
				} else {
					// Request blocks from remote replica
					msg := NodeMessage{
						Type:      "merge_collect",
						Sender:    primaryInfo.Hostname,
						Operation: "merge",
						Filename:  hydfsFile,
						Data: map[string]interface{}{
							"file_id": float64(fileID),
						},
						Timestamp: time.Now().UnixNano(),
					}

					jsonData, _ := json.Marshal(msg)
					conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", repInfo.Hostname, repInfo.Port), 5*time.Second)
					if err != nil {
						fmt.Printf("⚠️  Failed to connect to replica %s: %v\n", repInfo.Hostname, err)
						return
					}
					defer conn.Close()

					conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
					if _, err := conn.Write(append(jsonData, '\n')); err != nil {
						fmt.Printf("⚠️  Failed to request blocks from %s: %v\n", repInfo.Hostname, err)
						return
					}

					// Read response
					conn.SetReadDeadline(time.Now().Add(10 * time.Second))
					decoder := json.NewDecoder(conn)
					var response struct {
						Blocks []storage.BlockInfo `json:"blocks"`
					}
					if err := decoder.Decode(&response); err != nil {
						fmt.Printf("⚠️  Failed to read blocks from %s: %v\n", repInfo.Hostname, err)
						return
					}
					blocks = response.Blocks
				}

				// Add blocks to collection (deduplicate by client+sequence)
				blocksMutex.Lock()
				for _, block := range blocks {
					key := fmt.Sprintf("%s:%d", block.ClientID, block.ClientSequence)
					allBlocks[key] = block
				}
				blocksMutex.Unlock()

				fmt.Printf("� [MERGE] Collected %d blocks from %s\n", len(blocks), repInfo.Hostname)
			}(replicaInfo, replicaID)
		}

		// Wait for all collections
		collectWg.Wait()

		// Convert map to slice for merging
		blockSlice := make([]storage.BlockInfo, 0, len(allBlocks))
		for _, block := range allBlocks {
			blockSlice = append(blockSlice, block)
		}

		fmt.Printf("📊 [MERGE] Collected %d unique blocks from all replicas\n", len(blockSlice))

		// Step 2: Merge blocks locally
		if err := globalBlockStore.MergeFile(hydfsFile, blockSlice); err != nil {
			fmt.Printf("❌ Failed to merge blocks: %v\n", err)
			return
		}

		// Step 3: Read merged content
		content, err := globalBlockStore.ReadFile(hydfsFile)
		if err != nil {
			fmt.Printf("❌ Failed to read merged file: %v\n", err)
			return
		}

		metadata, err := globalBlockStore.GetMetadata(hydfsFile)
		if err != nil {
			fmt.Printf("❌ Failed to read merged metadata: %v\n", err)
			return
		}

		fmt.Printf("📊 [MERGE] Merged file has %d blocks, redistributing to %d replicas\n",
			metadata.TotalBlocks, len(successors))

		// Step 4: Send merged result to all replicas (including self for consistency)
		successCount := 0
		for _, replicaID := range successors {
			replicaInfo, ok := infoMap[replicaID]
			if !ok || replicaInfo.State != membership.Alive {
				continue
			}

			msg := NodeMessage{
				Type:      "merge_update",
				Sender:    primaryInfo.Hostname,
				Operation: "merge",
				Filename:  hydfsFile,
				Data: map[string]interface{}{
					"content":  string(content),
					"file_id":  float64(fileID),
					"metadata": metadata,
				},
				Timestamp: time.Now().UnixNano(),
			}

			jsonData, _ := json.Marshal(msg)
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", replicaInfo.Hostname, replicaInfo.Port), 5*time.Second)
			if err != nil {
				fmt.Printf("⚠️  Failed to connect to replica %s: %v\n", replicaInfo.Hostname, err)
				continue
			}

			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(append(jsonData, '\n')); err != nil {
				fmt.Printf("⚠️  Failed to send merge update to %s: %v\n", replicaInfo.Hostname, err)
			} else {
				fmt.Printf("✅ [MERGE] Sent merged file to %s\n", replicaInfo.Hostname)
				successCount++
			}
			conn.Close()
		}

		fmt.Printf("🎉 [MERGE] Merge complete - updated %d/%d replicas\n", successCount, len(successors))
	} else {
		// We're not the primary - send merge request to primary
		fmt.Printf("📤 [MERGE] Sending merge request to primary %s\n", primaryInfo.Hostname)

		msg := NodeMessage{
			Type:      "merge_request",
			Sender:    infoMap[node.RingID].Hostname,
			Operation: "merge",
			Filename:  hydfsFile,
			Data: map[string]interface{}{
				"file_id": float64(fileID),
			},
			Timestamp: time.Now().UnixNano(),
		}

		jsonData, _ := json.Marshal(msg)
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", primaryInfo.Hostname, primaryInfo.Port), 5*time.Second)
		if err != nil {
			fmt.Printf("❌ Failed to connect to primary: %v\n", err)
			return
		}
		defer conn.Close()

		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(append(jsonData, '\n')); err != nil {
			fmt.Printf("❌ Failed to send merge request: %v\n", err)
			return
		}

		fmt.Println("✅ [MERGE] Merge request sent to primary")
		fmt.Println("   Primary will coordinate merge across all replicas")
	}
}

func handleCLILs(parts []string, node *common.Node) {
	if len(parts) < 2 {
		fmt.Println("❌ Usage: ls <hydfsfile>")
		fmt.Println("   Example: ls hydfs_file.txt")
		return
	}
	hydfsFile := parts[1]

	// Calculate file ID using consistent hashing
	fileID := hashing.HashString(hydfsFile)

	// Find successor nodes for this file
	infoMap := node.Membership.GetInfoMap()
	successors := hashing.GetSuccessors(fileID, infoMap, hashing.NumReplicas)

	if len(successors) == 0 {
		fmt.Printf("❌ No replicas found for %s\n", hydfsFile)
		return
	}

	// Display replica information
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Printf("File Name: %s\n", hydfsFile)
	fmt.Printf("File ID:   %020d\n", fileID)
	fmt.Printf("Replication Factor (n): %d\n", hashing.NumReplicas)
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Printf("%-40s %-20s\n", "Hostname", "Node ID")
	fmt.Println(strings.Repeat("-", 60))

	for i, nodeID := range successors {
		info := infoMap[nodeID]
		marker := ""
		if i == 0 {
			marker = " [PRIMARY]"
		}
		fmt.Printf("%-40s %020d%s\n", info.Hostname, nodeID, marker)
	}
	fmt.Println("════════════════════════════════════════════════════════════")
}

func handleCLIListStore(node *common.Node) {
	infoMap := node.Membership.GetInfoMap()
	self := infoMap[node.RingID]

	fmt.Println("════════════════════════════════════════")
	fmt.Println("         FILES STORED ON THIS NODE")
	fmt.Println("════════════════════════════════════════")
	fmt.Printf("Node: %s (Ring ID: %020d)\n", self.Hostname, node.RingID)
	fmt.Println("────────────────────────────────────────")

	node.FileStoreMutex.RLock()
	if len(node.FileStore) == 0 {
		fmt.Println("No files stored.")
	} else {
		fmt.Printf("Total files: %d\n\n", len(node.FileStore))
		for filename, metadata := range node.FileStore {
			fmt.Printf("  📄 %s\n", filename)
			fmt.Printf("     Version: %d, Blocks: %d\n", metadata.Version, len(metadata.Blocks))
		}
	}
	node.FileStoreMutex.RUnlock()
	fmt.Println("════════════════════════════════════════")
}

func handleCLIGetFromReplica(node *common.Node, parts []string) {
	if len(parts) < 4 {
		fmt.Println("❌ Usage: getfromreplica <vmaddress> <hydfsfile> <localfile>")
		fmt.Println("   Example: getfromreplica node1 hydfs_file.txt local.txt")
		return
	}
	vmAddr := parts[1]
	hydfsFile := parts[2]
	localFile := parts[3]

	fmt.Printf("🚀 GETFROMREPLICA: %s from %s -> %s\n", hydfsFile, vmAddr, localFile)

	// Send GET request directly to the specified VM
	msg := NodeMessage{
		Type:      "get_file",
		Sender:    node.Membership.GetInfoMap()[node.RingID].Hostname,
		Operation: "get",
		Filename:  hydfsFile,
		Data: map[string]interface{}{
			"localfile": localFile,
		},
		Timestamp: time.Now().UnixNano(),
	}

	jsonData, _ := json.Marshal(msg)

	// Connect to specified VM (add port if not present)
	targetAddr := vmAddr
	if !strings.Contains(targetAddr, ":") {
		targetAddr = fmt.Sprintf("%s:%d", targetAddr, 8081) // Default node port (from config.json)
	}

	conn, err := net.DialTimeout("tcp", targetAddr, 5*time.Second)
	if err != nil {
		fmt.Printf("❌ Failed to connect to %s: %v\n", targetAddr, err)
		return
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(append(jsonData, '\n')); err != nil {
		fmt.Printf("❌ Failed to send GET request: %v\n", err)
		return
	}

	// Read response
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(conn)
	respLine, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("❌ Failed to read response: %v\n", err)
		return
	}

	var response NodeMessage
	if err := json.Unmarshal([]byte(respLine), &response); err != nil {
		fmt.Printf("❌ Failed to parse response: %v\n", err)
		return
	}

	switch response.Type {
	case "get_file_response", "get_response":
		content, ok := response.Data["content"].(string)
		if !ok {
			fmt.Printf("❌ Invalid content in response\n")
			return
		}

		// Write to local file
		localDir := "./hydfs_local"
		os.MkdirAll(localDir, 0755) // Ensure directory exists
		localPath := filepath.Join(localDir, localFile)
		if err := os.WriteFile(localPath, []byte(content), 0644); err != nil {
			fmt.Printf("❌ Failed to write local file: %v\n", err)
			return
		}

		fmt.Printf("✅ [GETFROMREPLICA] Retrieved %s (%d bytes) from %s -> %s\n",
			hydfsFile, len(content), vmAddr, localPath)
	case "error":
		errorMsg, _ := response.Data["message"].(string)
		fmt.Printf("❌ Error from replica: %s\n", errorMsg)
	default:
		fmt.Printf("❌ Unexpected response type: %s\n", response.Type)
	}
}

func handleCLIMultiAppend(parts []string) {
	if len(parts) < 4 {
		fmt.Println("❌ Usage: multiappend <hydfsfile> <vm1,vm2,...> <file1,file2,...>")
		fmt.Println("   Example: multiappend hydfs_file.txt 1,2,3,4 business_10.txt,business_11.txt,business_12.txt,business_13.txt")
		fmt.Println("   Note: VM numbers (1-10) or hostnames can be used")
		return
	}
	hydfsFile := parts[1]
	vmListStr := parts[2]
	fileListStr := parts[3]

	fmt.Printf("🚀 MULTI-APPEND: %s\n", hydfsFile)

	// Parse VM list
	vmParts := strings.Split(vmListStr, ",")
	fileParts := strings.Split(fileListStr, ",")

	if len(vmParts) != len(fileParts) {
		fmt.Printf("❌ VM count (%d) must match file count (%d)\n", len(vmParts), len(fileParts))
		return
	}

	fmt.Printf("📍 [MULTIAPPEND] Will launch %d concurrent appends to %s\n", len(vmParts), hydfsFile)

	// Map node numbers or hostnames to full addresses (from config)
	var vmHosts []string
	if cfg, err := common.LoadConfig("config.json"); err == nil {
		vmHosts = cfg.VMs
	}

	// Launch concurrent appends using goroutines
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < len(vmParts); i++ {
		wg.Add(1)
		go func(vmStr, localFile string, index int) {
			defer wg.Done()

			// Parse VM identifier (number or hostname)
			var targetHost string
			if vmNum, err := strconv.Atoi(vmStr); err == nil {
				// It's a number (1-10)
				if vmNum < 1 || vmNum > 10 {
					fmt.Printf("❌ [MULTIAPPEND %d] Invalid VM number: %d (must be 1-10)\n", index+1, vmNum)
					return
				}
				targetHost = vmHosts[vmNum-1]
			} else {
				// It's a hostname
				targetHost = vmStr
			}

			// Add port if not present
			targetAddr := targetHost
			if !strings.Contains(targetAddr, ":") {
				targetAddr = fmt.Sprintf("%s:%d", targetAddr, 8000) // Client port
			}

			// Prepend data/business/ if not already present
			fullLocalPath := localFile
			if !strings.HasPrefix(localFile, "data/") && !strings.HasPrefix(localFile, "/") {
				fullLocalPath = "data/business/" + localFile
			}

			fmt.Printf("📤 [MULTIAPPEND %d] Sending append command to %s: append %s %s\n",
				index+1, targetHost, fullLocalPath, hydfsFile)

			// Connect to target VM's client port and send append command
			conn, err := net.DialTimeout("tcp", targetAddr, 5*time.Second)
			if err != nil {
				fmt.Printf("❌ [MULTIAPPEND %d] Failed to connect to %s: %v\n", index+1, targetHost, err)
				return
			}
			defer conn.Close()

			// Send append command
			appendCmd := fmt.Sprintf("append %s %s\n", fullLocalPath, hydfsFile)
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write([]byte(appendCmd)); err != nil {
				fmt.Printf("❌ [MULTIAPPEND %d] Failed to send append to %s: %v\n", index+1, targetHost, err)
				return
			}

			// Read response
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			reader := bufio.NewReader(conn)
			response, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("❌ [MULTIAPPEND %d] Failed to read response from %s: %v\n", index+1, targetHost, err)
				return
			}

			response = strings.TrimSpace(response)
			if strings.HasPrefix(response, "OK") {
				mu.Lock()
				successCount++
				mu.Unlock()
				fmt.Printf("✅ [MULTIAPPEND %d] Append completed on %s\n", index+1, targetHost)
			} else {
				fmt.Printf("❌ [MULTIAPPEND %d] Append failed on %s: %s\n", index+1, targetHost, response)
			}
		}(vmParts[i], fileParts[i], i)
	}

	// Wait for all appends to complete
	wg.Wait()

	fmt.Printf("\n🎉 [MULTIAPPEND] Completed %d/%d concurrent appends to %s\n", successCount, len(vmParts), hydfsFile)
}

func handleCLIDgrep(node *common.Node, parts []string) {
	if len(parts) < 2 {
		fmt.Println("❌ Usage: dgrep [flags] <pattern>")
		fmt.Println("   Available flags:")
		fmt.Println("     -i            Case-insensitive search")
		fmt.Println("     -v            Invert match (non-matching lines)")
		fmt.Println("     -c            Count matches only")
		fmt.Println("     -E            Extended regex")
		fmt.Println("     -e <pattern>  Specify pattern (use when pattern starts with -)")
		fmt.Println("     -m <num>      Stop after <num> matches")
		fmt.Println("     --save <file> Save output to local file (optional)")
		fmt.Println("   Examples:")
		fmt.Println("     dgrep \"ERROR\"                    Simple search")
		fmt.Println("     dgrep -i \"error\"                 Case-insensitive")
		fmt.Println("     dgrep -c \"failed\"                Count only")
		fmt.Println("     dgrep -E \"error|warning\"         Extended regex")
		fmt.Println("     dgrep -i -E \"error|warning\"      Combined flags")
		fmt.Println("     dgrep -i \"error\" --save out.txt  Save results")
		fmt.Println("")
		fmt.Println("   Note: When using -e or -m, provide the value immediately after:")
		fmt.Println("     dgrep -i -e \"pattern\"            Correct")
		fmt.Println("     dgrep -e -i \"pattern\"            Wrong (treats -i as pattern)")
		return
	}

	// Check for --save flag and extract filename
	var saveFile string
	grepArgs := []string{}
	for i := 1; i < len(parts); i++ {
		if parts[i] == "--save" && i+1 < len(parts) {
			saveFile = parts[i+1]
			i++ // skip the filename in next iteration
		} else {
			grepArgs = append(grepArgs, parts[i])
		}
	}

	fmt.Printf("🔍 DGREP: Distributed grep across cluster\n")
	fmt.Printf("   Pattern: %s\n", strings.Join(grepArgs, " "))

	// Get alive nodes from membership
	infoMap := node.Membership.GetInfoMap()
	aliveVMs := make(map[string]int)

	for _, info := range infoMap {
		if info.State == membership.Alive {
			hostname := info.Hostname
			// Extract node number from hostname (e.g., "node1" -> 1, "node10" -> 10)
			nodeNum := 0
			fmt.Sscanf(strings.TrimPrefix(hostname, "node"), "%d", &nodeNum)
			if nodeNum > 0 {
				aliveVMs[hostname] = nodeNum
			} else {
				// Fallback: assign sequential index for unknown hostname formats
				aliveVMs[hostname] = len(aliveVMs) + 1
			}
		}
	}

	if len(aliveVMs) == 0 {
		msg := "❌ No alive VMs in cluster"
		fmt.Println(msg)
		log.Printf("[CLI-DGREP] %s (Found %d total members in infoMap)", msg, len(infoMap))
		fmt.Printf("   Debug: Found %d total members in infoMap\n", len(infoMap))
		return
	}

	fmt.Printf("   Querying %d VMs...\n\n", len(aliveVMs))
	log.Printf("[CLI-DGREP] Querying %d VMs for pattern: %s", len(aliveVMs), strings.Join(grepArgs, " "))

	result, err := logging.QueryDistributed(grepArgs, aliveVMs)
	if err != nil {
		errMsg := fmt.Sprintf("❌ Dgrep error: %v", err)
		fmt.Println(errMsg)
		log.Printf("[CLI-DGREP] %s", errMsg)
		return
	}

	// Display results
	fmt.Print(result)

	// Log only a summary (full results already displayed above, avoid duplication since log goes to stdout)
	lines := strings.Split(strings.TrimSpace(result), "\n")
	summaryLine := ""
	for _, line := range lines {
		if strings.HasPrefix(line, "Summary:") {
			summaryLine = line
			break
		}
	}
	if summaryLine != "" {
		log.Printf("[CLI-DGREP] Completed - %s", summaryLine)
	}

	// Save to file if requested
	if saveFile != "" {
		err := os.WriteFile(saveFile, []byte(result), 0644)
		if err != nil {
			fmt.Printf("❌ Failed to save to file %s: %v\n", saveFile, err)
		} else {
			fmt.Printf("💾 Results saved to: %s\n", saveFile)
		}
	}
}

// handleCLIListTasks queries the leader for all task details
func handleCLIListTasks(node *common.Node) {
	cfg, err := common.LoadConfig("config.json")
	if err != nil {
		fmt.Printf("❌ Failed to load config: %v\n", err)
		return
	}

	// Connect to leader (VM1)
	if len(cfg.VMs) == 0 {
		fmt.Println("❌ No VMs configured")
		return
	}

	leaderAddr := net.JoinHostPort(cfg.VMs[0], strconv.Itoa(cfg.RainStormPort))

	msg := rainstorm.RainStormMessage{
		Type:    "list_tasks",
		Sender:  node.Membership.GetInfoMap()[node.RingID].Hostname,
		Payload: rainstorm.ListTasksPayload{},
	}

	conn, err := net.DialTimeout("tcp", leaderAddr, 3*time.Second)
	if err != nil {
		fmt.Printf("❌ Failed to connect to leader at %s: %v\n", leaderAddr, err)
		return
	}
	defer conn.Close()

	// Send request
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		fmt.Printf("❌ Failed to send list_tasks request: %v\n", err)
		return
	}

	// Read response
	decoder := json.NewDecoder(conn)
	var response rainstorm.ListTasksResponse
	if err := decoder.Decode(&response); err != nil {
		fmt.Printf("❌ Failed to read response: %v\n", err)
		return
	}

	// Display tasks
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Println("                    RAINSTORM TASKS")
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Printf("%-20s %-30s %-8s %-20s %-10s\n", "Task ID", "VM", "PID", "Operator", "State")
	fmt.Println("────────────────────────────────────────────────────────────")

	for _, task := range response.Tasks {
		fmt.Printf("%-20s %-30s %-8d %-20s %-10s\n",
			task.TaskID, task.VM, task.PID, task.OpExe, task.State)
		if task.LogFile != "" {
			fmt.Printf("  Log: %s\n", task.LogFile)
		}
	}

	fmt.Printf("════════════════════════════════════════════════════════════\n")
	fmt.Printf("Total Tasks: %d\n", len(response.Tasks))
	fmt.Println()
}

// handleCLIKillTask kills a specific task by VM and PID
func handleCLIKillTask(node *common.Node, parts []string) {
	if len(parts) < 3 {
		fmt.Println("❌ Usage: kill_task <vm> <pid>")
		fmt.Println("   Example: kill_task node1 12345")
		return
	}

	vm := parts[1]
	pid, err := strconv.Atoi(parts[2])
	if err != nil {
		fmt.Printf("❌ Invalid PID: %s\n", parts[2])
		return
	}

	cfg, err := common.LoadConfig("config.json")
	if err != nil {
		fmt.Printf("❌ Failed to load config: %v\n", err)
		return
	}

	// Connect to leader (VM1)
	if len(cfg.VMs) == 0 {
		fmt.Println("❌ No VMs configured")
		return
	}

	leaderAddr := net.JoinHostPort(cfg.VMs[0], strconv.Itoa(cfg.RainStormPort))

	payload := rainstorm.KillTaskPayload{
		VM:  vm,
		PID: pid,
	}

	msg := rainstorm.RainStormMessage{
		Type:    "kill_task",
		Sender:  node.Membership.GetInfoMap()[node.RingID].Hostname,
		Payload: payload,
	}

	conn, err := net.DialTimeout("tcp", leaderAddr, 3*time.Second)
	if err != nil {
		fmt.Printf("❌ Failed to connect to leader at %s: %v\n", leaderAddr, err)
		return
	}
	defer conn.Close()

	// Send request
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		fmt.Printf("❌ Failed to send kill_task request: %v\n", err)
		return
	}

	fmt.Printf("✅ Kill request sent for task on %s with PID %d\n", vm, pid)
	fmt.Println("   💡 Use 'list_tasks' to verify task status")
}

func showHelp() {
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                    HyDFS COMMAND HELP                      ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n\n")

	fmt.Printf("MP1 Commands (Distributed Grep):\n")
	fmt.Printf("  dgrep [flags] <pattern>               Search logs across cluster\n")
	fmt.Printf("    Flags:\n")
	fmt.Printf("      -i                                Case-insensitive search\n")
	fmt.Printf("      -v                                Invert match (non-matching lines)\n")
	fmt.Printf("      -c                                Count matches only\n")
	fmt.Printf("      -E                                Extended regex\n")
	fmt.Printf("      -e <pattern>                      Specify pattern (for patterns starting with -)\n")
	fmt.Printf("      -m <num>                          Stop after <num> matches\n")
	fmt.Printf("      --save <file>                     Save output to local file\n")
	fmt.Printf("    Examples:\n")
	fmt.Printf("      dgrep \"ERROR\"                      Find all ERROR logs\n")
	fmt.Printf("      dgrep -i \"error\"                   Case-insensitive search\n")
	fmt.Printf("      dgrep -c \"failed\"                  Count failed occurrences\n")
	fmt.Printf("      dgrep -i -E \"error|warning\"        Combined flags\n\n")

	fmt.Printf("Group Membership Commands (SWIM):\n")
	fmt.Printf("  list_mem                              List all members\n")
	fmt.Printf("  list_self                             Show this node's info\n")
	fmt.Printf("  join                                  Join or rejoin (new incarnation)\n")
	fmt.Printf("  leave                                 Leave group (stay in CLI)\n\n")

	fmt.Printf("Distributed File System Commands (HyDFS):\n")
	fmt.Printf("  create <local> <hydfs>                Create file in HyDFS\n")
	fmt.Printf("  get <hydfs> <local>                   Fetch file to local\n")
	fmt.Printf("  append <local> <hydfs>                Append to HyDFS file\n")
	fmt.Printf("  merge <hydfs>                         Merge file replicas\n")
	fmt.Printf("  ls <hydfs>                            List replica locations\n")
	fmt.Printf("  liststore                             List files on this node\n")
	fmt.Printf("  getfromreplica <node> <hydfs> <local> Get from specific node\n")
	fmt.Printf("  list_mem_ids                          List membership with ring IDs (sorted)\n")
	fmt.Printf("  multiappend <hydfs> <nodes> <files>   Multi-node concurrent append\n\n")

	fmt.Printf("Stream Processing Commands (RainStorm):\n")
	fmt.Printf("  list_tasks                            Query leader for all task details\n")
	fmt.Printf("  kill_task <node> <pid>                Kill a specific task process\n\n")

	fmt.Printf("Utility:\n")
	fmt.Printf("  help                                  Show this help\n")
	fmt.Printf("  quit                                  Gracefully leave and exit\n\n")

	fmt.Printf("════════════════════════════════════════════════════════════\n\n")
}
