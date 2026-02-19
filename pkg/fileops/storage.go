package fileops

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/common"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/membership"
)

const (
	// StorageRoot is the base directory for HyDFS storage on each VM
	StorageRoot = "./hydfs_storage"
)

// ReplicaNode represents a node that should store a file replica
type ReplicaNode struct {
	RingID   uint64
	Hostname string
	Port     int
}

// BlockMetadata represents the metadata for a single block in a file
type BlockMetadata struct {
	BlockID   common.BlockIdentifier `json:"block_id"`
	Size      int64                  `json:"size"`
	Checksum  string                 `json:"checksum"` // SHA1 checksum for integrity
	CreatedAt time.Time              `json:"created_at"`
}

// FileBlockMetadata represents the complete metadata for a file's blocks
type FileBlockMetadata struct {
	HyDFSFilename string          `json:"hydfs_filename"`
	Blocks        []BlockMetadata `json:"blocks"`
	Version       int             `json:"version"`
}

// InitStorage ensures the storage directory exists
func InitStorage() error {
	return os.MkdirAll(StorageRoot, 0755)
}

// GetFileStoragePath returns the directory path for a given HyDFS file
func GetFileStoragePath(hydfsFilename string) string {
	return filepath.Join(StorageRoot, hydfsFilename)
}

// GetBlockPath returns the full path to a block file
func GetBlockPath(hydfsFilename string, blockID common.BlockIdentifier) string {
	blockFilename := fmt.Sprintf("block_%d_%s", blockID.Timestamp, blockID.ClientID)
	return filepath.Join(GetFileStoragePath(hydfsFilename), "blocks", blockFilename)
}

// GetMetadataPath returns the path to the metadata file for a HyDFS file
func GetMetadataPath(hydfsFilename string) string {
	return filepath.Join(GetFileStoragePath(hydfsFilename), "metadata.json")
}

// FileExists checks if a HyDFS file exists in local storage
func FileExists(hydfsFilename string) bool {
	metadataPath := GetMetadataPath(hydfsFilename)
	_, err := os.Stat(metadataPath)
	return err == nil
}

// LoadMetadata loads the metadata for a HyDFS file from disk
func LoadMetadata(hydfsFilename string) (*FileBlockMetadata, error) {
	metadataPath := GetMetadataPath(hydfsFilename)
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata FileBlockMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

// SaveMetadata saves the metadata for a HyDFS file to disk
func SaveMetadata(metadata *FileBlockMetadata) error {
	metadataPath := GetMetadataPath(metadata.HyDFSFilename)

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(metadataPath), 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// WriteBlock writes a block's data to disk and returns its metadata
func WriteBlock(hydfsFilename string, blockID common.BlockIdentifier, data []byte) (*BlockMetadata, error) {
	blockPath := GetBlockPath(hydfsFilename, blockID)

	// Ensure blocks directory exists
	if err := os.MkdirAll(filepath.Dir(blockPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create blocks directory: %w", err)
	}

	// Write the block data
	if err := os.WriteFile(blockPath, data, 0644); err != nil {
		return nil, fmt.Errorf("failed to write block: %w", err)
	}

	// Calculate checksum
	hash := sha1.Sum(data)
	checksum := fmt.Sprintf("%x", hash)

	metadata := &BlockMetadata{
		BlockID:   blockID,
		Size:      int64(len(data)),
		Checksum:  checksum,
		CreatedAt: time.Now(),
	}

	return metadata, nil
}

// ReadBlock reads a block's data from disk
func ReadBlock(hydfsFilename string, blockID common.BlockIdentifier) ([]byte, error) {
	blockPath := GetBlockPath(hydfsFilename, blockID)
	data, err := os.ReadFile(blockPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read block: %w", err)
	}
	return data, nil
}

// ReadFullFile reconstructs and returns the full file content by reading all blocks in order
func ReadFullFile(hydfsFilename string) ([]byte, error) {
	metadata, err := LoadMetadata(hydfsFilename)
	if err != nil {
		return nil, err
	}

	var fullContent []byte
	for _, blockMeta := range metadata.Blocks {
		blockData, err := ReadBlock(hydfsFilename, blockMeta.BlockID)
		if err != nil {
			return nil, fmt.Errorf("failed to read block %v: %w", blockMeta.BlockID, err)
		}
		fullContent = append(fullContent, blockData...)
	}

	return fullContent, nil
}

// DeleteFile removes all blocks and metadata for a HyDFS file
func DeleteFile(hydfsFilename string) error {
	filePath := GetFileStoragePath(hydfsFilename)
	return os.RemoveAll(filePath)
}

// ListLocalFiles returns a list of all HyDFS files stored on this node
func ListLocalFiles() ([]string, error) {
	entries, err := os.ReadDir(StorageRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			// Check if it has metadata.json
			metadataPath := filepath.Join(StorageRoot, entry.Name(), "metadata.json")
			if _, err := os.Stat(metadataPath); err == nil {
				files = append(files, entry.Name())
			}
		}
	}

	return files, nil
}

// GetFileID computes the hash (ring ID) for a HyDFS filename
func GetFileID(hydfsFilename string) uint64 {
	hash := sha1.Sum([]byte(hydfsFilename))
	// Use first 8 bytes of SHA1 hash as uint64
	return binary.BigEndian.Uint64(hash[:8])
}

// GetCoordinatorAndReplicas returns the coordinator and replica nodes for a file
// Returns up to 3 nodes (coordinator + 2 successors)
func GetCoordinatorAndReplicas(hydfsFilename string, m *membership.Membership) ([]ReplicaNode, error) {
	fileID := GetFileID(hydfsFilename)
	infoMap := m.GetInfoMap()

	// Get sorted list of alive nodes by checking the membership
	var nodeIDs []uint64
	for id := range infoMap {
		nodeIDs = append(nodeIDs, id)
	}

	if len(nodeIDs) == 0 {
		return nil, fmt.Errorf("no alive nodes in membership")
	}

	// Sort node IDs to create the ring order
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})

	// Find the coordinator (first node >= fileID)
	coordinatorIdx := -1
	for i, nodeID := range nodeIDs {
		if nodeID >= fileID {
			coordinatorIdx = i
			break
		}
	}

	// If no node >= fileID, wrap around to first node
	if coordinatorIdx == -1 {
		coordinatorIdx = 0
	}

	// Get coordinator + up to 2 successors (total 3 replicas for 2-failure tolerance)
	var replicas []ReplicaNode
	numNodes := len(nodeIDs)
	for i := 0; i < 3 && i < numNodes; i++ {
		idx := (coordinatorIdx + i) % numNodes
		nodeID := nodeIDs[idx]
		info := infoMap[nodeID]

		replicas = append(replicas, ReplicaNode{
			RingID:   nodeID,
			Hostname: info.Hostname,
			Port:     info.Port,
		})
	}

	return replicas, nil
}

// CreateBlock creates a new block for a file (used by create and append operations)
func CreateBlock(hydfsFilename string, localFilePath string, clientID string) (*BlockMetadata, common.BlockIdentifier, []byte, error) {
	// Read local file content
	data, err := os.ReadFile(localFilePath)
	if err != nil {
		return nil, common.BlockIdentifier{}, nil, fmt.Errorf("failed to read local file: %w", err)
	}

	// Create block identifier
	blockID := common.BlockIdentifier{
		Timestamp: time.Now().UnixNano(),
		ClientID:  clientID,
	}

	// Write block to disk
	blockMeta, err := WriteBlock(hydfsFilename, blockID, data)
	if err != nil {
		return nil, common.BlockIdentifier{}, nil, err
	}

	return blockMeta, blockID, data, nil
}

// AssembleFileFromBlocks reads all blocks and assembles them into a single file
func AssembleFileFromBlocks(hydfsFilename string, outputPath string) error {
	metadata, err := LoadMetadata(hydfsFilename)
	if err != nil {
		return err
	}

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Write each block in order
	for _, blockMeta := range metadata.Blocks {
		blockData, err := ReadBlock(hydfsFilename, blockMeta.BlockID)
		if err != nil {
			return fmt.Errorf("failed to read block %v: %w", blockMeta.BlockID, err)
		}

		if _, err := outFile.Write(blockData); err != nil {
			return fmt.Errorf("failed to write to output file: %w", err)
		}
	}

	return nil
}

// CopyLocalFileToStorage copies a local file directly to storage (for simple operations)
func CopyLocalFileToStorage(localPath, storagePath string) error {
	srcFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	if err := os.MkdirAll(filepath.Dir(storagePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	dstFile, err := os.Create(storagePath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	return nil
}
