// Package storage provides HyDFS's on-disk layer: an append-only block
// store with per-client sequencing, checksums, and deterministic merge
// ordering by (client, sequence).
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// FileMetadata represents the metadata stored in _metadata.json
type FileMetadata struct {
	FileID               uint64         `json:"file_id"`                 // 20-digit padded hash ID
	HyDFSFilename        string         `json:"hydfs_filename"`          // Original filename
	PrimaryReplicaNodeID uint64         `json:"primary_replica_node_id"` // Primary replica's node ID
	TotalBlocks          int            `json:"total_blocks"`            // Number of blocks
	ClientSequenceMap    map[string]int `json:"client_sequence_map"`     // CSN map for each client
	BlockMetadata        []BlockMeta    `json:"block_metadata"`          // Per-block metadata
}

// BlockMeta stores metadata for individual blocks
type BlockMeta struct {
	BlockNumber    int    `json:"block_number"`
	ClientID       string `json:"client_id"`
	ClientSequence int    `json:"client_sequence"`
}

// BlockStore handles block-based file storage with metadata
type BlockStore struct {
	baseDir string
	mu      sync.RWMutex
}

// NewBlockStore creates a new block storage manager
func NewBlockStore(baseDir string) *BlockStore {
	os.MkdirAll(baseDir, 0755)
	return &BlockStore{
		baseDir: baseDir,
	}
}

// getFileDir returns the directory path for a given HyDFS filename
func (bs *BlockStore) getFileDir(hydfsFilename string) string {
	// Remove extension to create directory name
	name := strings.TrimSuffix(hydfsFilename, filepath.Ext(hydfsFilename))
	return filepath.Join(bs.baseDir, name)
}

// CreateFile creates a new file with block-based storage
func (bs *BlockStore) CreateFile(hydfsFilename string, content []byte, fileID uint64, primaryNodeID uint64, clientID string) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	fileDir := bs.getFileDir(hydfsFilename)

	// Check if directory already exists
	if _, err := os.Stat(fileDir); err == nil {
		return fmt.Errorf("file already exists: %s", hydfsFilename)
	}

	// Create directory
	if err := os.MkdirAll(fileDir, 0755); err != nil {
		return fmt.Errorf("failed to create file directory: %v", err)
	}

	// Write first block
	blockPath := filepath.Join(fileDir, "block_01.txt")
	if err := os.WriteFile(blockPath, content, 0644); err != nil {
		os.RemoveAll(fileDir) // Clean up on failure
		return fmt.Errorf("failed to write block: %v", err)
	}

	// Create metadata
	metadata := FileMetadata{
		FileID:               fileID,
		HyDFSFilename:        hydfsFilename,
		PrimaryReplicaNodeID: primaryNodeID,
		TotalBlocks:          1,
		ClientSequenceMap:    map[string]int{clientID: 1},
		BlockMetadata: []BlockMeta{
			{BlockNumber: 1, ClientID: clientID, ClientSequence: 1},
		},
	}

	// Write metadata
	metadataPath := filepath.Join(fileDir, "_metadata.json")
	metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		os.RemoveAll(fileDir) // Clean up on failure
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	if err := os.WriteFile(metadataPath, metadataBytes, 0644); err != nil {
		os.RemoveAll(fileDir) // Clean up on failure
		return fmt.Errorf("failed to write metadata: %v", err)
	}

	return nil
}

// ReadFile reads the entire file by concatenating all blocks
func (bs *BlockStore) ReadFile(hydfsFilename string) ([]byte, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	fileDir := bs.getFileDir(hydfsFilename)

	// Read metadata to get total blocks
	metadata, err := bs.readMetadata(fileDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}

	// Concatenate all blocks - blocks already contain proper line endings
	var content []byte
	for i := 1; i <= metadata.TotalBlocks; i++ {
		blockPath := filepath.Join(fileDir, fmt.Sprintf("block_%02d.txt", i))
		blockData, err := os.ReadFile(blockPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read block %d: %v", i, err)
		}
		content = append(content, blockData...)
	}

	return content, nil
}

// readMetadata reads the metadata file
func (bs *BlockStore) readMetadata(fileDir string) (*FileMetadata, error) {
	metadataPath := filepath.Join(fileDir, "_metadata.json")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, err
	}

	var metadata FileMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// BlockInfo represents a block with its client information for merging
type BlockInfo struct {
	Content        []byte
	ClientID       string
	ClientSequence int
	BlockNumber    int
}

// MergeFile reconciles divergent replicas by merging all blocks
// It returns the merged content and updated metadata
func (bs *BlockStore) MergeFile(hydfsFilename string, allBlocksData []BlockInfo) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	fileDir := bs.getFileDir(hydfsFilename)

	// Read existing metadata
	metadata, err := bs.readMetadata(fileDir)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %v", err)
	}

	// Sort blocks by (ClientID, ClientSequence) to preserve per-client ordering
	// This ensures appends from same client appear in order
	sortedBlocks := make([]BlockInfo, len(allBlocksData))
	copy(sortedBlocks, allBlocksData)

	// Simple bubble sort (sufficient for demo purposes)
	for i := 0; i < len(sortedBlocks); i++ {
		for j := i + 1; j < len(sortedBlocks); j++ {
			// Sort by client ID first, then by client sequence
			if sortedBlocks[i].ClientID > sortedBlocks[j].ClientID ||
				(sortedBlocks[i].ClientID == sortedBlocks[j].ClientID &&
					sortedBlocks[i].ClientSequence > sortedBlocks[j].ClientSequence) {
				sortedBlocks[i], sortedBlocks[j] = sortedBlocks[j], sortedBlocks[i]
			}
		}
	}

	// Remove old block files
	files, _ := os.ReadDir(fileDir)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "block_") {
			os.Remove(filepath.Join(fileDir, f.Name()))
		}
	}

	// Write sorted blocks as new blocks
	newBlockMetadata := []BlockMeta{}
	for i, block := range sortedBlocks {
		blockPath := filepath.Join(fileDir, fmt.Sprintf("block_%02d.txt", i+1))
		if err := os.WriteFile(blockPath, block.Content, 0644); err != nil {
			return fmt.Errorf("failed to write merged block %d: %v", i+1, err)
		}

		// Track block metadata
		newBlockMetadata = append(newBlockMetadata, BlockMeta{
			BlockNumber:    i + 1,
			ClientID:       block.ClientID,
			ClientSequence: block.ClientSequence,
		})
	}

	// Update metadata
	metadata.TotalBlocks = len(sortedBlocks)
	metadata.BlockMetadata = newBlockMetadata

	// Rebuild client sequence map
	metadata.ClientSequenceMap = make(map[string]int)
	for _, block := range sortedBlocks {
		if block.ClientSequence > metadata.ClientSequenceMap[block.ClientID] {
			metadata.ClientSequenceMap[block.ClientID] = block.ClientSequence
		}
	}

	// Write updated metadata
	metadataPath := filepath.Join(fileDir, "_metadata.json")
	metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	if err := os.WriteFile(metadataPath, metadataBytes, 0644); err != nil {
		return fmt.Errorf("failed to update metadata: %v", err)
	}

	return nil
}

// GetAllBlocks returns all blocks with their metadata for merging
func (bs *BlockStore) GetAllBlocks(hydfsFilename string) ([]BlockInfo, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	fileDir := bs.getFileDir(hydfsFilename)

	// Read metadata
	metadata, err := bs.readMetadata(fileDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}

	var blocks []BlockInfo

	// Read all blocks
	for i := 1; i <= metadata.TotalBlocks; i++ {
		blockPath := filepath.Join(fileDir, fmt.Sprintf("block_%02d.txt", i))
		content, err := os.ReadFile(blockPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read block %d: %v", i, err)
		}

		// Find corresponding block metadata
		var clientID string
		var clientSeq int
		if i <= len(metadata.BlockMetadata) {
			blockMeta := metadata.BlockMetadata[i-1]
			clientID = blockMeta.ClientID
			clientSeq = blockMeta.ClientSequence
		} else {
			// Fallback for old files without block metadata
			clientID = fmt.Sprintf("unknown_block_%d", i)
			clientSeq = i
		}

		blocks = append(blocks, BlockInfo{
			Content:        content,
			ClientID:       clientID,
			ClientSequence: clientSeq,
			BlockNumber:    i,
		})
	}

	return blocks, nil
}

// FileExists checks if a file exists
func (bs *BlockStore) FileExists(hydfsFilename string) bool {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	fileDir := bs.getFileDir(hydfsFilename)
	_, err := os.Stat(fileDir)
	return err == nil
}

// DeleteFile removes a file and all its blocks
func (bs *BlockStore) DeleteFile(hydfsFilename string) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	fileDir := bs.getFileDir(hydfsFilename)
	return os.RemoveAll(fileDir)
}

// ListFiles returns all stored filenames
func (bs *BlockStore) ListFiles() ([]string, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	files, err := os.ReadDir(bs.baseDir)
	if err != nil {
		return nil, err
	}

	var filenames []string
	for _, f := range files {
		if f.IsDir() && !strings.HasPrefix(f.Name(), ".") {
			// Try to read metadata to get the original filename
			fileDir := filepath.Join(bs.baseDir, f.Name())
			metadata, err := bs.readMetadata(fileDir)
			if err == nil {
				filenames = append(filenames, metadata.HyDFSFilename)
			}
		}
	}
	return filenames, nil
}

// GetMetadata returns the metadata for a file
func (bs *BlockStore) GetMetadata(hydfsFilename string) (*FileMetadata, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	fileDir := bs.getFileDir(hydfsFilename)
	return bs.readMetadata(fileDir)
}

// AppendFile appends content to an existing file as a new block
// If the file doesn't exist, it creates it first (for exactly-once state logs)
func (bs *BlockStore) AppendFile(hydfsFilename string, content []byte, clientID string) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	fileDir := bs.getFileDir(hydfsFilename)

	// Check if file exists - if not, create it first
	if _, err := os.Stat(fileDir); os.IsNotExist(err) {
		// Create directory structure
		if err := os.MkdirAll(fileDir, 0755); err != nil {
			return fmt.Errorf("failed to create file directory: %v", err)
		}

		// Initialize empty metadata
		metadata := &FileMetadata{
			HyDFSFilename:     hydfsFilename,
			FileID:            0, // Will be set properly if needed
			TotalBlocks:       0,
			ClientSequenceMap: make(map[string]int),
			BlockMetadata:     []BlockMeta{},
		}

		metadataPath := filepath.Join(fileDir, "_metadata.json")
		metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal initial metadata: %v", err)
		}

		if err := os.WriteFile(metadataPath, metadataBytes, 0644); err != nil {
			return fmt.Errorf("failed to write initial metadata: %v", err)
		}
	}

	// Read existing metadata
	metadata, err := bs.readMetadata(fileDir)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %v", err)
	}

	// Increment block count and client sequence number
	metadata.TotalBlocks++
	if metadata.ClientSequenceMap == nil {
		metadata.ClientSequenceMap = make(map[string]int)
	}
	metadata.ClientSequenceMap[clientID]++

	// Add block metadata
	if metadata.BlockMetadata == nil {
		metadata.BlockMetadata = []BlockMeta{}
	}
	metadata.BlockMetadata = append(metadata.BlockMetadata, BlockMeta{
		BlockNumber:    metadata.TotalBlocks,
		ClientID:       clientID,
		ClientSequence: metadata.ClientSequenceMap[clientID],
	})

	// Write new block
	blockPath := filepath.Join(fileDir, fmt.Sprintf("block_%02d.txt", metadata.TotalBlocks))
	if err := os.WriteFile(blockPath, content, 0644); err != nil {
		return fmt.Errorf("failed to write append block: %v", err)
	}

	// Update metadata
	metadataPath := filepath.Join(fileDir, "_metadata.json")
	metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	if err := os.WriteFile(metadataPath, metadataBytes, 0644); err != nil {
		return fmt.Errorf("failed to update metadata: %v", err)
	}

	return nil
}
