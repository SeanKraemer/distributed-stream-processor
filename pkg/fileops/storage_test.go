package fileops

import (
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/common"
	"github.com/SeanKraemer/distributed-stream-processor/pkg/membership"
)

// Note: GetCoordinatorAndReplicas uses the *map keys* of the membership's
// InfoMap as the ring positions directly (node IDs are not re-hashed), so
// tests can hand-pick ring positions. Only the file ID is hashed (SHA1,
// unlike pkg/hashing which uses SHA256).

func newMembership(infos map[uint64]membership.Info) *membership.Membership {
	members := make([]uint64, 0, len(infos))
	for id := range infos {
		members = append(members, id)
	}
	return &membership.Membership{
		Members: members,
		InfoMap: infos,
	}
}

func nodeInfo(name string, port int, state membership.MemberState) membership.Info {
	return membership.Info{
		Hostname: name,
		Port:     port,
		State:    state,
	}
}

func TestGetFileIDDeterminism(t *testing.T) {
	a1 := GetFileID("testfile.txt")
	a2 := GetFileID("testfile.txt")
	if a1 != a2 {
		t.Errorf("GetFileID not deterministic: %d != %d", a1, a2)
	}

	// Pin the actual algorithm: first 8 bytes of SHA1(filename), big-endian.
	const want = uint64(1475598485413821722)
	if a1 != want {
		t.Errorf("GetFileID(\"testfile.txt\") = %d, want %d", a1, want)
	}

	if GetFileID("testfile.txt") == GetFileID("otherfile.txt") {
		t.Error("GetFileID collision for distinct filenames")
	}
}

func ringIDs(replicas []ReplicaNode) []uint64 {
	ids := make([]uint64, len(replicas))
	for i, r := range replicas {
		ids[i] = r.RingID
	}
	return ids
}

func TestGetCoordinatorAndReplicasOrdering(t *testing.T) {
	const filename = "testfile.txt"
	fileID := GetFileID(filename) // 1475598485413821722, mid-range so +/- offsets are safe

	// Coordinator is the first node ID >= fileID, then the next two in
	// sorted ring order.
	infos := map[uint64]membership.Info{
		fileID - 10:  nodeInfo("below", 8000, membership.Alive),
		fileID + 5:   nodeInfo("coord", 8001, membership.Alive),
		fileID + 20:  nodeInfo("succ1", 8002, membership.Alive),
		fileID + 100: nodeInfo("succ2", 8003, membership.Alive),
	}
	m := newMembership(infos)

	replicas, err := GetCoordinatorAndReplicas(filename, m)
	if err != nil {
		t.Fatalf("GetCoordinatorAndReplicas: %v", err)
	}

	want := []uint64{fileID + 5, fileID + 20, fileID + 100}
	got := ringIDs(replicas)
	if len(got) != len(want) {
		t.Fatalf("replicas = %v, want ring IDs %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replicas = %v, want ring IDs %v", got, want)
		}
	}

	// Hostname/Port are copied from the membership info.
	if replicas[0].Hostname != "coord" || replicas[0].Port != 8001 {
		t.Errorf("coordinator = %+v, want Hostname=coord Port=8001", replicas[0])
	}
	if replicas[2].Hostname != "succ2" || replicas[2].Port != 8003 {
		t.Errorf("third replica = %+v, want Hostname=succ2 Port=8003", replicas[2])
	}
}

func TestGetCoordinatorAndReplicasWrapAround(t *testing.T) {
	const filename = "wraparound.txt"
	fileID := GetFileID(filename)

	// All node IDs are below fileID, so the coordinator wraps to the
	// lowest ring position.
	infos := map[uint64]membership.Info{
		fileID - 30: nodeInfo("a", 8000, membership.Alive),
		fileID - 20: nodeInfo("b", 8001, membership.Alive),
		fileID - 10: nodeInfo("c", 8002, membership.Alive),
	}
	m := newMembership(infos)

	replicas, err := GetCoordinatorAndReplicas(filename, m)
	if err != nil {
		t.Fatalf("GetCoordinatorAndReplicas: %v", err)
	}

	want := []uint64{fileID - 30, fileID - 20, fileID - 10}
	got := ringIDs(replicas)
	if len(got) != len(want) {
		t.Fatalf("replicas = %v, want ring IDs %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replicas = %v, want ring IDs %v", got, want)
		}
	}
}

func TestGetCoordinatorAndReplicasExactMatch(t *testing.T) {
	const filename = "exact.txt"
	fileID := GetFileID(filename)

	// A node sitting exactly at the file's ring position is the coordinator.
	infos := map[uint64]membership.Info{
		fileID:      nodeInfo("exact", 8000, membership.Alive),
		fileID - 5:  nodeInfo("below", 8001, membership.Alive),
		fileID + 50: nodeInfo("above", 8002, membership.Alive),
	}
	m := newMembership(infos)

	replicas, err := GetCoordinatorAndReplicas(filename, m)
	if err != nil {
		t.Fatalf("GetCoordinatorAndReplicas: %v", err)
	}

	want := []uint64{fileID, fileID + 50, fileID - 5}
	got := ringIDs(replicas)
	if len(got) != len(want) {
		t.Fatalf("replicas = %v, want ring IDs %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replicas = %v, want ring IDs %v", got, want)
		}
	}
}

// TestGetCoordinatorAndReplicasDoesNotFilterByState pins the actual current
// behavior: unlike pkg/hashing.GetSuccessors, GetCoordinatorAndReplicas
// iterates over every entry in the InfoMap and does NOT exclude Suspected or
// Failed nodes, even though its error message says "no alive nodes". This
// test documents that behavior; it is not an endorsement of it.
func TestGetCoordinatorAndReplicasDoesNotFilterByState(t *testing.T) {
	const filename = "testfile.txt"
	fileID := GetFileID(filename)

	infos := map[uint64]membership.Info{
		fileID + 1: nodeInfo("failed", 8000, membership.Failed),
		fileID + 2: nodeInfo("suspected", 8001, membership.Suspected),
		fileID + 3: nodeInfo("alive", 8002, membership.Alive),
		fileID + 4: nodeInfo("spare", 8003, membership.Alive),
	}
	m := newMembership(infos)

	replicas, err := GetCoordinatorAndReplicas(filename, m)
	if err != nil {
		t.Fatalf("GetCoordinatorAndReplicas: %v", err)
	}

	// The Failed node at fileID+1 is chosen as coordinator and the
	// Suspected node at fileID+2 as first successor.
	want := []uint64{fileID + 1, fileID + 2, fileID + 3}
	got := ringIDs(replicas)
	if len(got) != len(want) {
		t.Fatalf("replicas = %v, want ring IDs %v (non-Alive nodes included)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replicas = %v, want ring IDs %v (non-Alive nodes included)", got, want)
		}
	}
}

func TestGetCoordinatorAndReplicasAtMostThree(t *testing.T) {
	const filename = "testfile.txt"
	fileID := GetFileID(filename)

	infos := map[uint64]membership.Info{}
	for i := uint64(1); i <= 5; i++ {
		infos[fileID+i] = nodeInfo(fmt.Sprintf("n%d", i), 8000+int(i), membership.Alive)
	}
	m := newMembership(infos)

	replicas, err := GetCoordinatorAndReplicas(filename, m)
	if err != nil {
		t.Fatalf("GetCoordinatorAndReplicas: %v", err)
	}
	if len(replicas) != 3 {
		t.Fatalf("expected at most 3 replicas, got %d: %v", len(replicas), ringIDs(replicas))
	}
}

func TestGetCoordinatorAndReplicasTwoNodes(t *testing.T) {
	const filename = "testfile.txt"
	fileID := GetFileID(filename)

	infos := map[uint64]membership.Info{
		fileID + 1: nodeInfo("a", 8000, membership.Alive),
		fileID + 2: nodeInfo("b", 8001, membership.Alive),
	}
	m := newMembership(infos)

	replicas, err := GetCoordinatorAndReplicas(filename, m)
	if err != nil {
		t.Fatalf("GetCoordinatorAndReplicas: %v", err)
	}
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas with 2-node membership, got %d: %v", len(replicas), ringIDs(replicas))
	}
	got := ringIDs(replicas)
	if got[0] != fileID+1 || got[1] != fileID+2 {
		t.Errorf("replicas = %v, want [%d %d]", got, fileID+1, fileID+2)
	}
}

func TestGetCoordinatorAndReplicasEmptyMembership(t *testing.T) {
	m := newMembership(map[uint64]membership.Info{})
	if _, err := GetCoordinatorAndReplicas("testfile.txt", m); err == nil {
		t.Error("expected error for empty membership, got nil")
	}
}

// --- Block storage helpers (sandboxed via t.Chdir because StorageRoot is a
// relative path "./hydfs_storage") ---

func TestWriteAndReadBlock(t *testing.T) {
	t.Chdir(t.TempDir())

	blockID := common.BlockIdentifier{Timestamp: 12345, ClientID: "client-a"}
	data := []byte("hello block data")

	meta, err := WriteBlock("file1.txt", blockID, data)
	if err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if meta.BlockID != blockID {
		t.Errorf("metadata BlockID = %+v, want %+v", meta.BlockID, blockID)
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("metadata Size = %d, want %d", meta.Size, len(data))
	}
	wantChecksum := fmt.Sprintf("%x", sha1.Sum(data))
	if meta.Checksum != wantChecksum {
		t.Errorf("metadata Checksum = %s, want %s", meta.Checksum, wantChecksum)
	}

	// The block lives under StorageRoot/<file>/blocks/block_<ts>_<client>.
	wantPath := filepath.Join(StorageRoot, "file1.txt", "blocks", "block_12345_client-a")
	if GetBlockPath("file1.txt", blockID) != wantPath {
		t.Errorf("GetBlockPath = %s, want %s", GetBlockPath("file1.txt", blockID), wantPath)
	}
	if _, err := os.Stat(wantPath); err != nil {
		t.Errorf("block file not written at %s: %v", wantPath, err)
	}

	got, err := ReadBlock("file1.txt", blockID)
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("ReadBlock = %q, want %q", got, data)
	}
}

func TestReadBlockMissing(t *testing.T) {
	t.Chdir(t.TempDir())

	_, err := ReadBlock("nope.txt", common.BlockIdentifier{Timestamp: 1, ClientID: "c"})
	if err == nil {
		t.Error("expected error reading missing block, got nil")
	}
}

func TestSaveAndLoadMetadataAndFileExists(t *testing.T) {
	t.Chdir(t.TempDir())

	if FileExists("file2.txt") {
		t.Error("FileExists returned true before any metadata was saved")
	}

	meta := &FileBlockMetadata{
		HyDFSFilename: "file2.txt",
		Version:       2,
		Blocks: []BlockMetadata{
			{BlockID: common.BlockIdentifier{Timestamp: 1, ClientID: "c1"}, Size: 3, Checksum: "abc"},
		},
	}
	if err := SaveMetadata(meta); err != nil {
		t.Fatalf("SaveMetadata: %v", err)
	}

	if !FileExists("file2.txt") {
		t.Error("FileExists returned false after SaveMetadata")
	}

	loaded, err := LoadMetadata("file2.txt")
	if err != nil {
		t.Fatalf("LoadMetadata: %v", err)
	}
	if loaded.HyDFSFilename != "file2.txt" || loaded.Version != 2 {
		t.Errorf("loaded metadata = %+v, want filename=file2.txt version=2", loaded)
	}
	if len(loaded.Blocks) != 1 || loaded.Blocks[0].BlockID != meta.Blocks[0].BlockID {
		t.Errorf("loaded blocks = %+v, want %+v", loaded.Blocks, meta.Blocks)
	}
}

func TestLoadMetadataMissing(t *testing.T) {
	t.Chdir(t.TempDir())

	if _, err := LoadMetadata("missing.txt"); err == nil {
		t.Error("expected error loading missing metadata, got nil")
	}
}

func TestReadFullFileConcatenatesBlocksInMetadataOrder(t *testing.T) {
	t.Chdir(t.TempDir())

	const filename = "multi.txt"
	id1 := common.BlockIdentifier{Timestamp: 1, ClientID: "c1"}
	id2 := common.BlockIdentifier{Timestamp: 2, ClientID: "c2"}

	m1, err := WriteBlock(filename, id1, []byte("hello "))
	if err != nil {
		t.Fatalf("WriteBlock 1: %v", err)
	}
	m2, err := WriteBlock(filename, id2, []byte("world"))
	if err != nil {
		t.Fatalf("WriteBlock 2: %v", err)
	}

	meta := &FileBlockMetadata{
		HyDFSFilename: filename,
		Blocks:        []BlockMetadata{*m1, *m2},
		Version:       1,
	}
	if err := SaveMetadata(meta); err != nil {
		t.Fatalf("SaveMetadata: %v", err)
	}

	got, err := ReadFullFile(filename)
	if err != nil {
		t.Fatalf("ReadFullFile: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("ReadFullFile = %q, want %q", got, "hello world")
	}
}

func TestDeleteFile(t *testing.T) {
	t.Chdir(t.TempDir())

	const filename = "doomed.txt"
	if _, err := WriteBlock(filename, common.BlockIdentifier{Timestamp: 1, ClientID: "c"}, []byte("x")); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := SaveMetadata(&FileBlockMetadata{HyDFSFilename: filename}); err != nil {
		t.Fatalf("SaveMetadata: %v", err)
	}

	if err := DeleteFile(filename); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if FileExists(filename) {
		t.Error("file still exists after DeleteFile")
	}
	if _, err := os.Stat(GetFileStoragePath(filename)); !os.IsNotExist(err) {
		t.Errorf("storage dir still present after DeleteFile (err=%v)", err)
	}

	// Deleting a nonexistent file is a no-op (os.RemoveAll semantics).
	if err := DeleteFile("never-existed.txt"); err != nil {
		t.Errorf("DeleteFile on missing file returned error: %v", err)
	}
}

func TestListLocalFiles(t *testing.T) {
	t.Chdir(t.TempDir())

	// Nonexistent storage root: returns an empty list, not an error.
	files, err := ListLocalFiles()
	if err != nil {
		t.Fatalf("ListLocalFiles with no storage root: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected no files, got %v", files)
	}

	if err := SaveMetadata(&FileBlockMetadata{HyDFSFilename: "a.txt"}); err != nil {
		t.Fatalf("SaveMetadata: %v", err)
	}
	if err := SaveMetadata(&FileBlockMetadata{HyDFSFilename: "b.txt"}); err != nil {
		t.Fatalf("SaveMetadata: %v", err)
	}
	// A directory without metadata.json is not listed as a file.
	if err := os.MkdirAll(filepath.Join(StorageRoot, "not-a-file"), 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	files, err = ListLocalFiles()
	if err != nil {
		t.Fatalf("ListLocalFiles: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %v", files)
	}
	seen := map[string]bool{}
	for _, f := range files {
		seen[f] = true
	}
	if !seen["a.txt"] || !seen["b.txt"] {
		t.Errorf("expected files a.txt and b.txt, got %v", files)
	}
}
