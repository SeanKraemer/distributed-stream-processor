package hashing

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/membership"
)

const NumReplicas = 3 // 3 replicas minimum for tolerating 2 failures

// HashString hashes a filename to uint64 using SHA256
// This places the file on the consistent hashing ring
func HashString(s string) uint64 {
	h := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(h[:8])
}

// GetSuccessors returns n successor nodes for a given file hash
// These are the nodes responsible for storing replicas of the file
func GetSuccessors(fileHash uint64, infoMap map[uint64]membership.Info, n int) []uint64 {
	if len(infoMap) == 0 {
		return nil
	}

	// Get all alive node IDs
	var nodeIDs []uint64
	for id, info := range infoMap {
		if info.State == membership.Alive {
			nodeIDs = append(nodeIDs, id)
		}
	}

	if len(nodeIDs) == 0 {
		return nil
	}

	// Sort node IDs to form the ring
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})

	// Find first successor (first node >= fileHash, or wrap to first node)
	idx := sort.Search(len(nodeIDs), func(i int) bool {
		return nodeIDs[i] >= fileHash
	})

	// Wrap around if needed
	if idx == len(nodeIDs) {
		idx = 0
	}

	// Collect n successors (with wrap-around)
	result := make([]uint64, 0, n)
	for i := 0; i < n && i < len(nodeIDs); i++ {
		result = append(result, nodeIDs[(idx+i)%len(nodeIDs)])
	}

	return result
}
