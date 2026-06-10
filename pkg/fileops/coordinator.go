// Package fileops implements HyDFS file operations: it maps files to their
// coordinator and replicas on the consistent-hash ring and orchestrates
// create/get/append/merge against the block store.
package fileops

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/common"
)

// CreateFileRequest represents a create file operation request
type CreateFileRequest struct {
	LocalFilePath string `json:"local_file_path"`
	HyDFSFilename string `json:"hydfs_filename"`
	ClientID      string `json:"client_id"`
}

// CreateFileResponse represents the response to a create file operation
type CreateFileResponse struct {
	Success       bool     `json:"success"`
	Error         string   `json:"error,omitempty"`
	Replicas      []string `json:"replicas"`       // List of "hostname:port" where file was stored
	FileID        uint64   `json:"file_id"`        // Ring ID of the file
	CoordinatorID uint64   `json:"coordinator_id"` // Ring ID of the coordinator
}

// ReplicateBlockRequest is sent from coordinator to replicas to store a block
type ReplicateBlockRequest struct {
	HyDFSFilename string                 `json:"hydfs_filename"`
	BlockID       common.BlockIdentifier `json:"block_id"`
	Data          []byte                 `json:"data"`
	IsCreate      bool                   `json:"is_create"` // true if this is the initial create operation
}

// ReplicateBlockResponse is the reply from a replica after storing a block
type ReplicateBlockResponse struct {
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
}

// HandleCreateFile handles a create file operation
// This should be called on the coordinator node
func HandleCreateFile(node *common.Node, req CreateFileRequest) (*CreateFileResponse, error) {
	log.Printf("CREATE: Starting create operation for %s (local: %s, client: %s)",
		req.HyDFSFilename, req.LocalFilePath, req.ClientID)

	// Check if file already exists locally
	if FileExists(req.HyDFSFilename) {
		log.Printf("CREATE: File %s already exists", req.HyDFSFilename)
		return &CreateFileResponse{
			Success: false,
			Error:   fmt.Sprintf("file %s already exists", req.HyDFSFilename),
		}, nil
	}

	// Determine replicas using consistent hashing
	replicas, err := GetCoordinatorAndReplicas(req.HyDFSFilename, node.Membership)
	if err != nil {
		return nil, fmt.Errorf("failed to get replicas: %w", err)
	}

	// Get file ID for detailed logging
	fileID := GetFileID(req.HyDFSFilename)

	// Get self info
	infoMap := node.Membership.GetInfoMap()
	selfInfo := infoMap[node.RingID]

	log.Printf("CREATE: File %s (ID: %d) will be stored on 3 replicas", req.HyDFSFilename, fileID)
	log.Printf("COORDINATOR: %s (RingID: %020d - THIS NODE) is coordinating the create operation", selfInfo.Hostname, node.RingID)
	log.Printf("REPLICA PLAN (based on consistent hashing):")
	for i, r := range replicas {
		if i == 0 {
			log.Printf("[1/3] %s (RingID: %020d) - PRIMARY REPLICA (first successor to file hash)", r.Hostname, r.RingID)
		} else {
			log.Printf("[%d/3] %s (RingID: %020d) - REPLICA (successor %d)", i+1, r.Hostname, r.RingID, i)
		}
	}

	// Check if coordinator node is also one of the replicas
	isSelfReplica := false
	for _, replica := range replicas {
		if replica.Hostname == selfInfo.Hostname && replica.Port == selfInfo.Port {
			isSelfReplica = true
			log.Printf("CREATE: Coordinator node IS also a replica (will save locally)")
			break
		}
	}
	if !isSelfReplica {
		log.Printf("CREATE: Coordinator node is NOT a replica (will only forward requests)")
	}

	// Create the initial block (block 0)
	blockMeta, blockID, blockData, err := CreateBlock(req.HyDFSFilename, req.LocalFilePath, req.ClientID)
	if err != nil {
		log.Printf("CREATE: Failed to create block: %v", err)
		return nil, fmt.Errorf("failed to create block: %w", err)
	}

	log.Printf("CREATE: Created block %d for %s (size: %d bytes)",
		blockID.Timestamp, req.HyDFSFilename, blockMeta.Size)

	// Initialize metadata with the first block
	metadata := &FileBlockMetadata{
		HyDFSFilename: req.HyDFSFilename,
		Blocks:        []BlockMetadata{*blockMeta},
		Version:       1,
	}

	// Check if we (the coordinator) are one of the replica nodes
	isSelfReplica = false
	for _, replica := range replicas {
		if replica.Hostname == selfInfo.Hostname && replica.Port == selfInfo.Port {
			isSelfReplica = true
			break
		}
	}

	// Only save metadata locally if we're one of the replicas
	if isSelfReplica {
		// Save metadata locally
		if err := SaveMetadata(metadata); err != nil {
			log.Printf("CREATE: Failed to save metadata: %v", err)
			return nil, fmt.Errorf("failed to save metadata: %w", err)
		}
		log.Printf("CREATE: Saved metadata for %s on THIS NODE (coordinator is also a replica)", req.HyDFSFilename)
	}

	// Replicate to all replica nodes
	var successfulReplicas []string

	for _, replica := range replicas {
		log.Printf("CREATE: Replicating to %s:%d", replica.Hostname, replica.Port)

		// Send replication request
		replicaReq := ReplicateBlockRequest{
			HyDFSFilename: req.HyDFSFilename,
			BlockID:       blockID,
			Data:          blockData,
			IsCreate:      true,
		}

		resp, err := sendReplicateRequest(replica.Hostname, replica.Port, replicaReq)
		if err != nil {
			log.Printf("CREATE: Failed to replicate to %s:%d: %v", replica.Hostname, replica.Port, err)
			continue
		}

		if !resp.Success {
			log.Printf("CREATE: Replication to %s:%d failed: %s", replica.Hostname, replica.Port, resp.Error)
			continue
		}

		successfulReplicas = append(successfulReplicas, fmt.Sprintf("%s:%d", replica.Hostname, replica.Port))
		log.Printf("CREATE: Successfully replicated to %s:%d", replica.Hostname, replica.Port)
	}

	log.Printf("CREATE: File %s created successfully on %d replicas: %v",
		req.HyDFSFilename, len(successfulReplicas), successfulReplicas)

	return &CreateFileResponse{
		Success:       true,
		Replicas:      successfulReplicas,
		FileID:        GetFileID(req.HyDFSFilename),
		CoordinatorID: replicas[0].RingID,
	}, nil
}

// HandleReplicateBlock handles a replication request from a coordinator
// This is called on replica nodes to store a block
func HandleReplicateBlock(node *common.Node, req ReplicateBlockRequest) (*ReplicateBlockResponse, error) {
	infoMap := node.Membership.GetInfoMap()
	selfInfo := infoMap[node.RingID]

	log.Printf("REPLICATE: THIS NODE (%s, RingID: %020d) received replication request", selfInfo.Hostname, node.RingID)
	log.Printf("REPLICATE: File: %s, Block: %d, IsCreate: %v, Data Size: %d bytes",
		req.HyDFSFilename, req.BlockID.Timestamp, req.IsCreate, len(req.Data))

	// If this is a create operation, check if file already exists
	if req.IsCreate && FileExists(req.HyDFSFilename) {
		log.Printf("REPLICATE: File %s already exists on this node", req.HyDFSFilename)
		return &ReplicateBlockResponse{
			Success:  false,
			Error:    fmt.Sprintf("file %s already exists", req.HyDFSFilename),
			Hostname: selfInfo.Hostname,
			Port:     selfInfo.Port,
		}, nil
	}

	// Write the block to storage
	blockMeta, err := WriteBlock(req.HyDFSFilename, req.BlockID, req.Data)
	if err != nil {
		log.Printf("REPLICATE: Failed to write block: %v", err)
		return &ReplicateBlockResponse{
			Success:  false,
			Error:    fmt.Sprintf("failed to write block: %v", err),
			Hostname: selfInfo.Hostname,
			Port:     selfInfo.Port,
		}, nil
	}

	// Load or create metadata
	var metadata *FileBlockMetadata
	if FileExists(req.HyDFSFilename) {
		metadata, err = LoadMetadata(req.HyDFSFilename)
		if err != nil {
			log.Printf("REPLICATE: Failed to load metadata: %v", err)
			return &ReplicateBlockResponse{
				Success:  false,
				Error:    fmt.Sprintf("failed to load metadata: %v", err),
				Hostname: selfInfo.Hostname,
				Port:     selfInfo.Port,
			}, nil
		}
		// Add new block to metadata
		metadata.Blocks = append(metadata.Blocks, *blockMeta)
		metadata.Version++
	} else {
		// Create new metadata for this file
		metadata = &FileBlockMetadata{
			HyDFSFilename: req.HyDFSFilename,
			Blocks:        []BlockMetadata{*blockMeta},
			Version:       1,
		}
	}

	// Save updated metadata
	if err := SaveMetadata(metadata); err != nil {
		log.Printf("REPLICATE: Failed to save metadata: %v", err)
		return &ReplicateBlockResponse{
			Success:  false,
			Error:    fmt.Sprintf("failed to save metadata: %v", err),
			Hostname: selfInfo.Hostname,
			Port:     selfInfo.Port,
		}, nil
	}

	log.Printf("REPLICATE: Successfully stored block for %s (size: %d bytes)",
		req.HyDFSFilename, blockMeta.Size)

	return &ReplicateBlockResponse{
		Success:  true,
		Hostname: selfInfo.Hostname,
		Port:     selfInfo.Port,
	}, nil
}

// sendReplicateRequest sends a replication request to a replica node
func sendReplicateRequest(hostname string, port int, req ReplicateBlockRequest) (*ReplicateBlockResponse, error) {
	// Connect to replica node
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Set write deadline
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// Create request message
	reqMsg := map[string]interface{}{
		"type":    "replicate_block",
		"request": req,
	}

	reqData, err := json.Marshal(reqMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	log.Printf("CREATE: Sending replication request (size: %d bytes) to %s:%d", len(reqData), hostname, port)

	// Send request
	if _, err := conn.Write(append(reqData, '\n')); err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	log.Printf("CREATE: Request sent, waiting for response from %s:%d", hostname, port)

	// Read response using buffered reader
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(conn)
	respLine, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("CREATE: Received response from %s:%d (size: %d bytes)", hostname, port, len(respLine))

	var resp ReplicateBlockResponse
	if err := json.Unmarshal(respLine, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &resp, nil
}
