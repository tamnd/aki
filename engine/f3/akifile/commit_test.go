package akifile

import "testing"

// TestCommitRootsFlipsSlot commits a new root and confirms the open sequence now
// picks the freshly written slot: the stale slot B takes the commit, its sequence
// advances, and it carries the roots and durable file size the writer stamped.
func TestCommitRootsFlipsSlot(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if err := f.CommitRoots(MetaSlot{SRTOff: 0x40000, SRTLen: 80, SRTShardCount: prefix.ShardCount}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if f.CommitSeq() != 1 {
		t.Fatalf("commit seq = %d, want 1", f.CommitSeq())
	}

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Which != 1 {
		t.Fatalf("live slot = %d, want 1 (the flipped-to slot B)", st.Which)
	}
	if st.Meta.CommitSeq != 1 || st.Meta.SRTOff != 0x40000 {
		t.Fatalf("live root = seq %d srt %#x, want 1/0x40000", st.Meta.CommitSeq, st.Meta.SRTOff)
	}
	if st.Meta.FileSize != f.Cursor() {
		t.Fatalf("committed file size %d, want the durable cursor %d", st.Meta.FileSize, f.Cursor())
	}
	// No clean_shutdown was set, so a reopen would treat it as a crashed tail.
	if st.Outcome != OpenCrashed {
		t.Fatalf("outcome = %d, want OpenCrashed", st.Outcome)
	}
}

// TestCommitRootsAlternatesSlots confirms two commits take turns: the first flips
// to slot B, the second back to slot A, each with the next commit sequence.
func TestCommitRootsAlternatesSlots(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	if err := f.CommitRoots(MetaSlot{CleanShutdown: 1}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if err := f.CommitRoots(MetaSlot{CleanShutdown: 1}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if f.CommitSeq() != 2 {
		t.Fatalf("commit seq = %d, want 2", f.CommitSeq())
	}

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Which != 0 || st.Meta.CommitSeq != 2 {
		t.Fatalf("live = slot %d seq %d, want slot 0 seq 2", st.Which, st.Meta.CommitSeq)
	}
}

// TestCommitRootsSurvivesTornSlot tears the live slot after a commit and confirms
// the previous root, in the other slot, is still picked: the dual-slot flip keeps
// the last good root through a torn write.
func TestCommitRootsSurvivesTornSlot(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("data")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := f.CommitRoots(MetaSlot{CleanShutdown: 1, SRTOff: 0x50000}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The commit went to slot B; tearing it must not lose the file: slot A still
	// holds the create-time root at commit_seq 0.
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Which != 0 || st.Meta.CommitSeq != 0 {
		t.Fatalf("fell back to slot %d seq %d, want slot 0 seq 0 (the surviving root)", st.Which, st.Meta.CommitSeq)
	}
}

// TestOpenReseedsCommitSeq reopens a committed file and confirms the next commit
// continues the sequence: Open reads the live root's commit_seq so a flip does not
// resurrect an old slot.
func TestOpenReseedsCommitSeq(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	if err := f.CommitRoots(MetaSlot{CleanShutdown: 1}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Reopen: the writer must resume with commit_seq 1 live in slot B, so the next
	// commit is seq 2 into slot A.
	g, err := OpenOnDevice(dev, OpenOptions{Sync: SyncNo})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if g.CommitSeq() != 1 {
		t.Fatalf("reopened commit seq = %d, want 1", g.CommitSeq())
	}
	if err := g.CommitRoots(MetaSlot{CleanShutdown: 1}); err != nil {
		t.Fatalf("post-reopen commit: %v", err)
	}

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Which != 0 || st.Meta.CommitSeq != 2 {
		t.Fatalf("live = slot %d seq %d, want slot 0 seq 2", st.Which, st.Meta.CommitSeq)
	}
}
