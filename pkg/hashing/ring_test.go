package hashing

import (
	"fmt"
	"testing"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/membership"
)

// aliveInfo builds a minimal membership.Info in a given state.
// Note: GetSuccessors uses the *map keys* of infoMap as the ring positions
// directly (node IDs are not re-hashed inside GetSuccessors), so tests can
// hand-pick ring positions by choosing the map keys.
func infoWithState(name string, state membership.MemberState) membership.Info {
	return membership.Info{
		Hostname: name,
		Port:     9000,
		State:    state,
	}
}

func TestHashStringDeterminism(t *testing.T) {
	a1 := HashString("hello")
	a2 := HashString("hello")
	if a1 != a2 {
		t.Errorf("HashString not deterministic: %d != %d", a1, a2)
	}

	// Pin the actual algorithm: first 8 bytes of SHA256("hello"), big-endian.
	const want = uint64(3238736544897475342)
	if a1 != want {
		t.Errorf("HashString(\"hello\") = %d, want %d", a1, want)
	}

	b := HashString("world")
	if a1 == b {
		t.Errorf("HashString collision for distinct inputs: %q and %q both hash to %d", "hello", "world", a1)
	}
	if HashString("") == HashString("hello") {
		t.Error("HashString(\"\") unexpectedly equals HashString(\"hello\")")
	}
}

func TestGetSuccessorsOrdering(t *testing.T) {
	// Ring positions are the map keys, so we control them exactly.
	infoMap := map[uint64]membership.Info{
		100: infoWithState("n100", membership.Alive),
		200: infoWithState("n200", membership.Alive),
		300: infoWithState("n300", membership.Alive),
	}

	tests := []struct {
		name     string
		fileHash uint64
		n        int
		want     []uint64
	}{
		{"normal successor lookup", 150, 3, []uint64{200, 300, 100}},
		{"wrap-around past highest position", 350, 3, []uint64{100, 200, 300}},
		{"fileHash exactly equal to a node position is its own successor", 200, 3, []uint64{200, 300, 100}},
		{"n=1 returns only first successor", 150, 1, []uint64{200}},
		{"fileHash below lowest position", 50, 2, []uint64{100, 200}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetSuccessors(tt.fileHash, infoMap, tt.n)
			if len(got) != len(tt.want) {
				t.Fatalf("GetSuccessors(%d, n=%d) = %v, want %v", tt.fileHash, tt.n, got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("GetSuccessors(%d, n=%d) = %v, want %v", tt.fileHash, tt.n, got, tt.want)
				}
			}
		})
	}
}

func TestGetSuccessorsExcludesNonAlive(t *testing.T) {
	infoMap := map[uint64]membership.Info{
		100: infoWithState("n100", membership.Alive),
		200: infoWithState("n200", membership.Suspected),
		300: infoWithState("n300", membership.Failed),
		400: infoWithState("n400", membership.Alive),
	}

	got := GetSuccessors(150, infoMap, 3)
	// Only 100 and 400 are Alive; first node >= 150 among them is 400.
	want := []uint64{400, 100}
	if len(got) != len(want) {
		t.Fatalf("GetSuccessors = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("GetSuccessors = %v, want %v", got, want)
		}
	}
	for _, id := range got {
		if id == 200 || id == 300 {
			t.Errorf("GetSuccessors returned non-Alive node %d", id)
		}
	}
}

func TestGetSuccessorsFewerAliveThanReplicationFactor(t *testing.T) {
	infoMap := map[uint64]membership.Info{
		100: infoWithState("n100", membership.Alive),
		200: infoWithState("n200", membership.Alive),
	}

	got := GetSuccessors(500, infoMap, NumReplicas)
	if len(got) != 2 {
		t.Fatalf("expected all 2 alive nodes, got %v", got)
	}
	seen := map[uint64]bool{}
	for _, id := range got {
		if seen[id] {
			t.Errorf("duplicate node %d in result %v", id, got)
		}
		seen[id] = true
	}
	if !seen[100] || !seen[200] {
		t.Errorf("expected both alive nodes 100 and 200, got %v", got)
	}
}

func TestGetSuccessorsEmptyMap(t *testing.T) {
	got := GetSuccessors(123, map[uint64]membership.Info{}, NumReplicas)
	if got != nil {
		t.Errorf("expected nil for empty infoMap, got %v", got)
	}
	got = GetSuccessors(123, nil, NumReplicas)
	if got != nil {
		t.Errorf("expected nil for nil infoMap, got %v", got)
	}
}

func TestGetSuccessorsAllFailed(t *testing.T) {
	infoMap := map[uint64]membership.Info{
		100: infoWithState("n100", membership.Failed),
		200: infoWithState("n200", membership.Suspected),
	}
	got := GetSuccessors(123, infoMap, NumReplicas)
	if got != nil {
		t.Errorf("expected nil when no node is Alive, got %v", got)
	}
}

// replicaSet returns the replica set for a key as a lookup map.
func replicaSet(key uint64, infoMap map[uint64]membership.Info) map[uint64]bool {
	set := map[uint64]bool{}
	for _, id := range GetSuccessors(key, infoMap, NumReplicas) {
		set[id] = true
	}
	return set
}

func sameSet(a, b map[uint64]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if !b[id] {
			return false
		}
	}
	return true
}

// TestGetSuccessorsRedistribution verifies the minimal-movement property of
// consistent hashing: removing a node only changes the replica sets of keys
// that were replicated on it, and adding a node only changes replica sets
// that now include the new node.
func TestGetSuccessorsRedistribution(t *testing.T) {
	const numNodes = 10
	const numKeys = 1000

	// Build 10 synthetic nodes whose ring positions are spread out by
	// hashing their names (positions are the map keys).
	baseline := map[uint64]membership.Info{}
	var nodeIDs []uint64
	for i := 0; i < numNodes; i++ {
		id := HashString(fmt.Sprintf("node-%d", i))
		baseline[id] = infoWithState(fmt.Sprintf("node-%d", i), membership.Alive)
		nodeIDs = append(nodeIDs, id)
	}
	if len(baseline) != numNodes {
		t.Fatalf("hash collision among synthetic node IDs; have %d unique", len(baseline))
	}

	keys := make([]uint64, numKeys)
	for i := range keys {
		keys[i] = HashString(fmt.Sprintf("key-%d", i))
	}

	baselineSets := make([]map[uint64]bool, numKeys)
	for i, k := range keys {
		baselineSets[i] = replicaSet(k, baseline)
		if len(baselineSets[i]) != NumReplicas {
			t.Fatalf("key %d: expected %d replicas, got %v", k, NumReplicas, baselineSets[i])
		}
	}

	// Phase 1: remove one node. Only keys whose replica set contained the
	// removed node may change; surviving replicas must be retained.
	removedID := nodeIDs[3]
	afterRemove := map[uint64]membership.Info{}
	for id, info := range baseline {
		if id != removedID {
			afterRemove[id] = info
		}
	}

	changedOnRemove := 0
	for i, k := range keys {
		newSet := replicaSet(k, afterRemove)
		if baselineSets[i][removedID] {
			changedOnRemove++
			if sameSet(newSet, baselineSets[i]) {
				t.Errorf("key %d: replica set contained removed node but did not change", k)
			}
			if newSet[removedID] {
				t.Errorf("key %d: removed node %d still in replica set %v", k, removedID, newSet)
			}
			// Minimal movement: every surviving old replica keeps the key.
			for id := range baselineSets[i] {
				if id != removedID && !newSet[id] {
					t.Errorf("key %d: surviving replica %d dropped after removal; old=%v new=%v",
						k, id, baselineSets[i], newSet)
				}
			}
		} else {
			if !sameSet(newSet, baselineSets[i]) {
				t.Errorf("key %d: replica set changed despite not containing removed node; old=%v new=%v",
					k, baselineSets[i], newSet)
			}
		}
	}
	if changedOnRemove == 0 {
		t.Error("sanity: no key had the removed node in its replica set")
	}
	if changedOnRemove == numKeys {
		t.Error("sanity: every key had the removed node in its replica set")
	}

	// Phase 2: add a brand-new 11th node to the original 10-node ring.
	// Every replica set that changes must now contain the new node.
	newID := HashString("node-new")
	if _, exists := baseline[newID]; exists {
		t.Fatal("hash collision for new node ID")
	}
	afterAdd := map[uint64]membership.Info{newID: infoWithState("node-new", membership.Alive)}
	for id, info := range baseline {
		afterAdd[id] = info
	}

	changedOnAdd := 0
	for i, k := range keys {
		newSet := replicaSet(k, afterAdd)
		if sameSet(newSet, baselineSets[i]) {
			continue
		}
		changedOnAdd++
		if !newSet[newID] {
			t.Errorf("key %d: replica set changed after adding node but does not contain it; old=%v new=%v",
				k, baselineSets[i], newSet)
		}
		// All other members of the new set must have been replicas before.
		for id := range newSet {
			if id != newID && !baselineSets[i][id] {
				t.Errorf("key %d: unexpected new replica %d (not the added node); old=%v new=%v",
					k, id, baselineSets[i], newSet)
			}
		}
	}
	if changedOnAdd == 0 {
		t.Error("sanity: adding a node changed no replica sets")
	}
	if changedOnAdd == numKeys {
		t.Error("sanity: adding a node changed every replica set")
	}
}
