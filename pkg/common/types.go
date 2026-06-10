// Package common holds the shared cluster configuration and node types used
// by the membership, HyDFS, and RainStorm subsystems.
package common

import (
	"sync"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/membership"
)

// BlockIdentifier uniquely identifies and orders an appended block.
// Sort primarily by Timestamp, then by ClientID to break ties.
type BlockIdentifier struct {
	Timestamp int64  // UnixNano precision
	ClientID  string // Unique client/node identifier
}

// FileMetadata tracks file information including replicas and blocks
type FileMetadata struct {
	Filename  string            // HyDFS filename
	FileID    uint64            // Hash of filename for ring placement
	Version   int               // Version number
	Size      int64             // File size in bytes
	Blocks    []BlockIdentifier // Sorted list of blocks (for append tracking)
	CreatedAt int64             // Unix timestamp
}

// Node represents a HyDFS server instance.
// FileStore is guarded by FileStoreMutex for concurrent access.
type Node struct {
	RingID         uint64
	Membership     *membership.Membership
	FileStore      map[string]FileMetadata // map[HyDFSFilename]FileMetadata
	FileStoreMutex sync.RWMutex
	Storage        StorageManager // Storage backend for file I/O
	IsActive       bool           // Whether node is actively participating in cluster
	ActiveMutex    sync.RWMutex
}

// StorageManager interface for file storage operations
type StorageManager interface {
	WriteFile(filename string, data []byte) error
	ReadFile(filename string) ([]byte, error)
	FileExists(filename string) bool
	DeleteFile(filename string) error
	ListFiles() ([]string, error)
}

// AppendReplicaMsg is sent from coordinator to replicas to append a new block.
type AppendReplicaMsg struct {
	HyDFSFilename string
	BlockID       BlockIdentifier
	Data          []byte
}

// GetMetadataMsg requests metadata for a given HyDFS file.
type GetMetadataMsg struct {
	HyDFSFilename string
}

// MetadataRespMsg responds with the block list for a HyDFS file.
type MetadataRespMsg struct {
	HyDFSFilename string
	BlockList     []BlockIdentifier
}
