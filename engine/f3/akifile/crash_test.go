package akifile

import (
	"errors"
	"testing"
)

// buildRecoverable lays down a file a crash matrix can tear: a checkpoint that
// commits the shard root table and extent map to slot B (the create-time root
// stays in slot A at commit_seq 0), then a durable log segment appended past the
// root as the un-checkpointed tail. It returns the device, the prefix, the live
// meta root, and the offset of that tail segment. The file opens crashed, so
// recovery would replay from the tail.
func buildRecoverable(t *testing.T) (*memDevice, *Prefix, *MetaSlot, uint64) {
	t.Helper()
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	rows := make([]SRTRow, prefix.ShardCount)
	extents := []Extent{{Kind: ExtentHeader, StartOff: 0, Length: PageSize}}
	if err := f.Checkpoint(&SRT{Gen: 1, Rows: rows}, extents, CheckpointStats{}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	offs, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("tail")}})
	if err != nil {
		t.Fatalf("append tail: %v", err)
	}
	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("baseline open state: %v", err)
	}
	return dev, prefix, st.Meta, offs[0]
}

// TestCrashMatrixBaseline confirms the untorn file recovers cleanly: it opens
// crashed (no clean shutdown was committed), with the live root in slot B and the
// tail replay starting exactly at the un-checkpointed tail segment.
func TestCrashMatrixBaseline(t *testing.T) {
	dev, _, meta, tailOff := buildRecoverable(t)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.State.Outcome != OpenCrashed || rec.State.Which != 1 {
		t.Fatalf("outcome/which = %d/%d, want crashed in slot B", rec.State.Outcome, rec.State.Which)
	}
	if tailOff != meta.FileSize || rec.TailFrom != tailOff {
		t.Fatalf("tail = %d, file size %d, replay from %d, want all equal", tailOff, meta.FileSize, rec.TailFrom)
	}
}

// TestCrashMatrixTornPrefix tears the format magic: recovery never guesses past a
// bad prefix, so both Recover and Inspect fail with ErrMagic.
func TestCrashMatrixTornPrefix(t *testing.T) {
	dev, _, _, _ := buildRecoverable(t)
	dev.buf[0] ^= 0xff

	if _, err := Recover(dev); !errors.Is(err, ErrMagic) {
		t.Fatalf("recover err = %v, want ErrMagic", err)
	}
	if _, err := Inspect(dev); !errors.Is(err, ErrMagic) {
		t.Fatalf("inspect err = %v, want ErrMagic", err)
	}
}

// TestCrashMatrixTornStaleSlot tears slot A, the stale create-time root: the live
// root in slot B is untouched, so recovery is unchanged. A torn stale slot costs
// nothing.
func TestCrashMatrixTornStaleSlot(t *testing.T) {
	dev, prefix, _, tailOff := buildRecoverable(t)
	dev.buf[prefix.MetaSlotAOff+3] ^= 0xff

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.State.Which != 1 || rec.State.Outcome != OpenCrashed || rec.TailFrom != tailOff {
		t.Fatalf("recover = which %d outcome %d tail %d, want slot B crashed tail %d",
			rec.State.Which, rec.State.Outcome, rec.TailFrom, tailOff)
	}
}

// TestCrashMatrixTornLiveSlot tears slot B, the freshly committed live root: the
// dual-slot flip keeps the previous root in slot A, so recovery falls back to it
// and replays the whole append space rather than losing the file.
func TestCrashMatrixTornLiveSlot(t *testing.T) {
	dev, prefix, _, _ := buildRecoverable(t)
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.State.Which != 0 || rec.State.Outcome != OpenClean {
		t.Fatalf("recover = which %d outcome %d, want the surviving slot A", rec.State.Which, rec.State.Outcome)
	}
	if rec.TailFrom != PageSize {
		t.Fatalf("replay from %d, want the whole append space at %d", rec.TailFrom, PageSize)
	}
}

// TestCrashMatrixTornBothSlots tears both roots: with no trusted meta slot recovery
// falls back to a full segment scan from the header page.
func TestCrashMatrixTornBothSlots(t *testing.T) {
	dev, prefix, _, _ := buildRecoverable(t)
	dev.buf[prefix.MetaSlotAOff+3] ^= 0xff
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.State.Outcome != OpenScanFallback || rec.SRT != nil || rec.TailFrom != PageSize {
		t.Fatalf("recover = outcome %d srt %v tail %d, want scan fallback from the header page",
			rec.State.Outcome, rec.SRT, rec.TailFrom)
	}
}

// TestCrashMatrixTornSRTRoot tears the shard root table the live slot names: the
// meta slot still validates but its root does not, so Recover surfaces the mismatch
// while Inspect degrades to a finding rather than failing.
func TestCrashMatrixTornSRTRoot(t *testing.T) {
	dev, _, meta, _ := buildRecoverable(t)
	dev.buf[meta.SRTOff+SRTHeaderLen+3] ^= 0xff // a row byte: passes the magic, fails the crc

	if _, err := Recover(dev); !errors.Is(err, ErrChecksum) {
		t.Fatalf("recover err = %v, want ErrChecksum on the torn root", err)
	}
	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect should degrade, not fail: %v", err)
	}
	if !errors.Is(rep.SRTErr, ErrChecksum) || rep.SRT != nil {
		t.Fatalf("inspect srt = %v/%v, want a torn-root finding", rep.SRT, rep.SRTErr)
	}
}

// TestCrashMatrixTornExtentRoot tears the extent map, the coarse shape hint. It is
// not a recovery source (recovery reaches every segment through the SRT roots and
// per-shard chains), so a torn extent map cannot break Recover: the file recovers
// exactly as it does untorn. The map carries no checksum by design, so a mid-data
// tear that keeps the length is silently absorbed rather than flagged.
func TestCrashMatrixTornExtentRoot(t *testing.T) {
	dev, _, _, tailOff := buildRecoverable(t)
	base, err := Recover(dev)
	if err != nil {
		t.Fatalf("baseline recover: %v", err)
	}

	dev2, _, meta, _ := buildRecoverable(t)
	dev2.buf[meta.ExtentTableOff+3] ^= 0xff
	rec, err := Recover(dev2)
	if err != nil {
		t.Fatalf("recover must ignore a torn extent map: %v", err)
	}
	if rec.State.Outcome != base.State.Outcome || rec.TailFrom != tailOff {
		t.Fatalf("torn-extent recovery = outcome %d tail %d, want the untorn %d/%d",
			rec.State.Outcome, rec.TailFrom, base.State.Outcome, tailOff)
	}
}

// TestCrashMatrixTornTail tears the un-checkpointed tail segment: recovery succeeds
// and the durable tail cuts at the torn segment, so the file keeps everything the
// crash left intact and drops only the torn write.
func TestCrashMatrixTornTail(t *testing.T) {
	dev, prefix, _, tailOff := buildRecoverable(t)
	dev.buf[tailOff+SegHeaderLen+1] ^= 0xff

	if _, err := Recover(dev); err != nil {
		t.Fatalf("recover: %v", err)
	}
	size, _ := dev.Size()
	tally, err := ScanSegments(dev, prefix, PageSize, uint64(size))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if tally.DurableTail != tailOff {
		t.Fatalf("durable tail = %d, want the cut at the torn segment %d", tally.DurableTail, tailOff)
	}
}
