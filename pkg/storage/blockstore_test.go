package storage

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

func TestCreateAndReadFileRoundtrip(t *testing.T) {
	bs := NewBlockStore(t.TempDir())

	content := []byte("hello world\nsecond line\n")
	if err := bs.CreateFile("test.txt", content, 12345, 1, "clientA"); err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	got, err := bs.ReadFile("test.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("ReadFile = %q, want %q", got, content)
	}

	// Metadata after create: one block, clientA at sequence 1.
	meta, err := bs.GetMetadata("test.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}
	if meta.FileID != 12345 {
		t.Errorf("FileID = %d, want 12345", meta.FileID)
	}
	if meta.HyDFSFilename != "test.txt" {
		t.Errorf("HyDFSFilename = %q, want %q", meta.HyDFSFilename, "test.txt")
	}
	if meta.TotalBlocks != 1 {
		t.Errorf("TotalBlocks = %d, want 1", meta.TotalBlocks)
	}
	if meta.ClientSequenceMap["clientA"] != 1 {
		t.Errorf("ClientSequenceMap[clientA] = %d, want 1", meta.ClientSequenceMap["clientA"])
	}
}

func TestCreateDuplicateFails(t *testing.T) {
	bs := NewBlockStore(t.TempDir())

	if err := bs.CreateFile("dup.txt", []byte("first"), 1, 1, "clientA"); err != nil {
		t.Fatalf("first CreateFile failed: %v", err)
	}
	if err := bs.CreateFile("dup.txt", []byte("second"), 1, 1, "clientA"); err == nil {
		t.Fatal("duplicate CreateFile succeeded, want error")
	}

	// Original content must be untouched.
	got, err := bs.ReadFile("dup.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(got) != "first" {
		t.Errorf("ReadFile = %q, want %q", got, "first")
	}
}

func TestAppendIncrementsBlocksAndSequences(t *testing.T) {
	bs := NewBlockStore(t.TempDir())

	// CreateFile writes block 1 as clientA sequence 1.
	if err := bs.CreateFile("app.txt", []byte("A1\n"), 7, 1, "clientA"); err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	// Two appends from clientA, one from clientB.
	if err := bs.AppendFile("app.txt", []byte("A2\n"), "clientA"); err != nil {
		t.Fatalf("Append A2 failed: %v", err)
	}
	if err := bs.AppendFile("app.txt", []byte("A3\n"), "clientA"); err != nil {
		t.Fatalf("Append A3 failed: %v", err)
	}
	if err := bs.AppendFile("app.txt", []byte("B1\n"), "clientB"); err != nil {
		t.Fatalf("Append B1 failed: %v", err)
	}

	meta, err := bs.GetMetadata("app.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}
	if meta.TotalBlocks != 4 {
		t.Errorf("TotalBlocks = %d, want 4", meta.TotalBlocks)
	}
	if meta.ClientSequenceMap["clientA"] != 3 {
		t.Errorf("ClientSequenceMap[clientA] = %d, want 3", meta.ClientSequenceMap["clientA"])
	}
	if meta.ClientSequenceMap["clientB"] != 1 {
		t.Errorf("ClientSequenceMap[clientB] = %d, want 1", meta.ClientSequenceMap["clientB"])
	}

	wantBlockMeta := []BlockMeta{
		{BlockNumber: 1, ClientID: "clientA", ClientSequence: 1},
		{BlockNumber: 2, ClientID: "clientA", ClientSequence: 2},
		{BlockNumber: 3, ClientID: "clientA", ClientSequence: 3},
		{BlockNumber: 4, ClientID: "clientB", ClientSequence: 1},
	}
	if len(meta.BlockMetadata) != len(wantBlockMeta) {
		t.Fatalf("len(BlockMetadata) = %d, want %d", len(meta.BlockMetadata), len(wantBlockMeta))
	}
	for i, want := range wantBlockMeta {
		if meta.BlockMetadata[i] != want {
			t.Errorf("BlockMetadata[%d] = %+v, want %+v", i, meta.BlockMetadata[i], want)
		}
	}

	// ReadFile concatenates blocks in block-number order.
	got, err := bs.ReadFile("app.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	want := "A1\nA2\nA3\nB1\n"
	if string(got) != want {
		t.Errorf("ReadFile = %q, want %q", got, want)
	}
}

func TestAppendToNonexistentFileAutoCreates(t *testing.T) {
	// Pins the exactly-once state-log path: AppendFile auto-creates the file
	// with empty initial metadata instead of erroring.
	bs := NewBlockStore(t.TempDir())

	if bs.FileExists("statelog.txt") {
		t.Fatal("file should not exist before append")
	}
	if err := bs.AppendFile("statelog.txt", []byte("entry1\n"), "task1"); err != nil {
		t.Fatalf("AppendFile to nonexistent file failed: %v", err)
	}
	if !bs.FileExists("statelog.txt") {
		t.Error("FileExists = false after auto-creating append")
	}

	meta, err := bs.GetMetadata("statelog.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}
	if meta.TotalBlocks != 1 {
		t.Errorf("TotalBlocks = %d, want 1", meta.TotalBlocks)
	}
	if meta.FileID != 0 {
		t.Errorf("FileID = %d, want 0 (auto-created files get zero FileID)", meta.FileID)
	}
	if meta.HyDFSFilename != "statelog.txt" {
		t.Errorf("HyDFSFilename = %q, want %q", meta.HyDFSFilename, "statelog.txt")
	}
	if meta.ClientSequenceMap["task1"] != 1 {
		t.Errorf("ClientSequenceMap[task1] = %d, want 1", meta.ClientSequenceMap["task1"])
	}

	got, err := bs.ReadFile("statelog.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(got) != "entry1\n" {
		t.Errorf("ReadFile = %q, want %q", got, "entry1\n")
	}
}

func TestMergeFileSortsByClientIDThenSequence(t *testing.T) {
	bs := NewBlockStore(t.TempDir())

	// MergeFile requires existing metadata, so create the file first.
	if err := bs.CreateFile("merge.txt", []byte("seed\n"), 99, 1, "clientA"); err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Shuffled input: out of order across and within clients.
	shuffled := []BlockInfo{
		{Content: []byte("B2\n"), ClientID: "clientB", ClientSequence: 2},
		{Content: []byte("A2\n"), ClientID: "clientA", ClientSequence: 2},
		{Content: []byte("B1\n"), ClientID: "clientB", ClientSequence: 1},
		{Content: []byte("A1\n"), ClientID: "clientA", ClientSequence: 1},
		{Content: []byte("A3\n"), ClientID: "clientA", ClientSequence: 3},
	}
	if err := bs.MergeFile("merge.txt", shuffled); err != nil {
		t.Fatalf("MergeFile failed: %v", err)
	}

	// Content must be ordered by (ClientID, ClientSequence).
	got, err := bs.ReadFile("merge.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	want := "A1\nA2\nA3\nB1\nB2\n"
	if string(got) != want {
		t.Errorf("merged content = %q, want %q", got, want)
	}

	meta, err := bs.GetMetadata("merge.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}
	if meta.TotalBlocks != 5 {
		t.Errorf("TotalBlocks = %d, want 5", meta.TotalBlocks)
	}
	// Sequence map rebuilt to per-client max.
	if meta.ClientSequenceMap["clientA"] != 3 {
		t.Errorf("ClientSequenceMap[clientA] = %d, want 3", meta.ClientSequenceMap["clientA"])
	}
	if meta.ClientSequenceMap["clientB"] != 2 {
		t.Errorf("ClientSequenceMap[clientB] = %d, want 2", meta.ClientSequenceMap["clientB"])
	}
	if len(meta.ClientSequenceMap) != 2 {
		t.Errorf("ClientSequenceMap has %d entries, want 2 (rebuilt from merged blocks)", len(meta.ClientSequenceMap))
	}

	wantBlockMeta := []BlockMeta{
		{BlockNumber: 1, ClientID: "clientA", ClientSequence: 1},
		{BlockNumber: 2, ClientID: "clientA", ClientSequence: 2},
		{BlockNumber: 3, ClientID: "clientA", ClientSequence: 3},
		{BlockNumber: 4, ClientID: "clientB", ClientSequence: 1},
		{BlockNumber: 5, ClientID: "clientB", ClientSequence: 2},
	}
	if len(meta.BlockMetadata) != len(wantBlockMeta) {
		t.Fatalf("len(BlockMetadata) = %d, want %d", len(meta.BlockMetadata), len(wantBlockMeta))
	}
	for i, want := range wantBlockMeta {
		if meta.BlockMetadata[i] != want {
			t.Errorf("BlockMetadata[%d] = %+v, want %+v", i, meta.BlockMetadata[i], want)
		}
	}

	// MergeFile preserves the original FileID and filename.
	if meta.FileID != 99 {
		t.Errorf("FileID = %d, want 99 (preserved across merge)", meta.FileID)
	}
}

func TestGetAllBlocksMetadataMapping(t *testing.T) {
	bs := NewBlockStore(t.TempDir())

	if err := bs.CreateFile("blocks.txt", []byte("one"), 5, 1, "c1"); err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if err := bs.AppendFile("blocks.txt", []byte("two"), "c2"); err != nil {
		t.Fatalf("AppendFile failed: %v", err)
	}
	if err := bs.AppendFile("blocks.txt", []byte("three"), "c1"); err != nil {
		t.Fatalf("AppendFile failed: %v", err)
	}

	blocks, err := bs.GetAllBlocks("blocks.txt")
	if err != nil {
		t.Fatalf("GetAllBlocks failed: %v", err)
	}
	if len(blocks) != 3 {
		t.Fatalf("len(blocks) = %d, want 3", len(blocks))
	}

	type expect struct {
		content string
		client  string
		seq     int
		num     int
	}
	wants := []expect{
		{"one", "c1", 1, 1},
		{"two", "c2", 1, 2},
		{"three", "c1", 2, 3},
	}
	for i, w := range wants {
		b := blocks[i]
		if string(b.Content) != w.content {
			t.Errorf("blocks[%d].Content = %q, want %q", i, b.Content, w.content)
		}
		if b.ClientID != w.client {
			t.Errorf("blocks[%d].ClientID = %q, want %q", i, b.ClientID, w.client)
		}
		if b.ClientSequence != w.seq {
			t.Errorf("blocks[%d].ClientSequence = %d, want %d", i, b.ClientSequence, w.seq)
		}
		if b.BlockNumber != w.num {
			t.Errorf("blocks[%d].BlockNumber = %d, want %d", i, b.BlockNumber, w.num)
		}
	}
}

func TestDeleteFileAndFileExists(t *testing.T) {
	bs := NewBlockStore(t.TempDir())

	if bs.FileExists("gone.txt") {
		t.Error("FileExists = true for never-created file")
	}
	if err := bs.CreateFile("gone.txt", []byte("data"), 1, 1, "c1"); err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if !bs.FileExists("gone.txt") {
		t.Error("FileExists = false after create")
	}
	if err := bs.DeleteFile("gone.txt"); err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}
	if bs.FileExists("gone.txt") {
		t.Error("FileExists = true after delete")
	}
	if _, err := bs.ReadFile("gone.txt"); err == nil {
		t.Error("ReadFile succeeded after delete, want error")
	}

	// Deleting a nonexistent file is a no-op (os.RemoveAll semantics).
	if err := bs.DeleteFile("never-existed.txt"); err != nil {
		t.Errorf("DeleteFile on nonexistent file = %v, want nil", err)
	}
}

func TestListFiles(t *testing.T) {
	bs := NewBlockStore(t.TempDir())

	names, err := bs.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles failed: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("ListFiles on empty store = %v, want empty", names)
	}

	for i := 0; i < 3; i++ {
		fname := fmt.Sprintf("file%d.txt", i)
		if err := bs.CreateFile(fname, []byte("x"), uint64(i), 1, "c1"); err != nil {
			t.Fatalf("CreateFile %s failed: %v", fname, err)
		}
	}

	names, err = bs.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles failed: %v", err)
	}
	sort.Strings(names)
	want := []string{"file0.txt", "file1.txt", "file2.txt"}
	if len(names) != len(want) {
		t.Fatalf("ListFiles = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("ListFiles[%d] = %q, want %q", i, names[i], want[i])
		}
	}

	// Deleted files drop out of the listing.
	if err := bs.DeleteFile("file1.txt"); err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}
	names, err = bs.ListFiles()
	if err != nil {
		t.Fatalf("ListFiles failed: %v", err)
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "file0.txt" || names[1] != "file2.txt" {
		t.Errorf("ListFiles after delete = %v, want [file0.txt file2.txt]", names)
	}
}
