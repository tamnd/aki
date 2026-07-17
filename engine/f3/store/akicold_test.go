package store

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// coldTestFrame builds a self-delimiting cold frame the store's codec shape asks
// for: a leading u32 total over the whole frame, then a body. The adapter treats
// the body as opaque, so a stand-in is enough to pin the round trip.
func coldTestFrame(body []byte) []byte {
	total := 4 + len(body)
	b := make([]byte, total)
	binary.LittleEndian.PutUint32(b, uint32(total))
	copy(b[4:], body)
	return b
}

// newTestAkiCold builds a cold adapter over a fresh .aki in the test's temp dir.
func newTestAkiCold(t *testing.T, shard uint16) *akiCold {
	t.Helper()
	f, err := akifile.Create(filepath.Join(t.TempDir(), "test.aki"), akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return newAkiCold(f, shard)
}

// TestAkiColdAppendReadRoundTrip appends a demote batch and reads each frame back
// whole by the offset the batch returned, then reads an interior sub-range: the
// cold re-home's write and read paths end to end over the .aki cold region.
func TestAkiColdAppendReadRoundTrip(t *testing.T) {
	c := newTestAkiCold(t, 3)

	frames := [][]byte{
		coldTestFrame([]byte("alpha")),
		coldTestFrame(bytes.Repeat([]byte("b"), 3000)),
		coldTestFrame([]byte("gamma")),
	}
	offs, err := c.appendBatch(frames)
	if err != nil {
		t.Fatalf("appendBatch: %v", err)
	}
	if len(offs) != len(frames) {
		t.Fatalf("got %d offsets, want %d", len(offs), len(frames))
	}
	for i, off := range offs {
		got, err := c.readFrame(off, nil)
		if err != nil {
			t.Fatalf("readFrame %d: %v", i, err)
		}
		if !bytes.Equal(got, frames[i]) {
			t.Fatalf("frame %d = %q, want %q", i, got, frames[i])
		}
	}

	// readInto serves a positioned sub-range: the leading u32 total of the first
	// frame, the header read the resolver takes.
	hdr, err := c.readInto(offs[0], 4, nil)
	if err != nil {
		t.Fatalf("readInto: %v", err)
	}
	if got := binary.LittleEndian.Uint32(hdr); got != uint32(len(frames[0])) {
		t.Fatalf("total field = %d, want %d", got, len(frames[0]))
	}
}

// TestAkiColdBatchesShareASegment cuts two batches and confirms the second lands
// past the first so both sets of offsets resolve, and the sequence advances only
// on a real cut.
func TestAkiColdBatchesShareASegment(t *testing.T) {
	c := newTestAkiCold(t, 2)

	first, err := c.appendBatch([][]byte{coldTestFrame([]byte("first"))})
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	// An empty batch writes nothing and holds the sequence.
	if offs, err := c.appendBatch(nil); err != nil || offs != nil {
		t.Fatalf("empty batch = %v/%v, want nil/nil", offs, err)
	}
	if c.seq != 1 {
		t.Fatalf("empty batch advanced seq to %d", c.seq)
	}
	second, err := c.appendBatch([][]byte{coldTestFrame([]byte("second"))})
	if err != nil {
		t.Fatalf("second batch: %v", err)
	}
	if second[0] <= first[0] {
		t.Fatalf("second batch at %d did not advance past the first at %d", second[0], first[0])
	}
	if c.seq != 2 {
		t.Fatalf("seq = %d after two real cuts, want 2", c.seq)
	}
}

// TestAkiColdLogBytesAccounting checks total counts every appended frame byte and
// unlink moves the dead subset, the pair a cold-region compaction weighs.
func TestAkiColdLogBytesAccounting(t *testing.T) {
	c := newTestAkiCold(t, 0)

	frames := [][]byte{coldTestFrame([]byte("one")), coldTestFrame([]byte("twotwo"))}
	var want uint64
	for _, fr := range frames {
		want += uint64(len(fr))
	}
	if _, err := c.appendBatch(frames); err != nil {
		t.Fatalf("appendBatch: %v", err)
	}
	if total, dead := c.logBytes(); total != want || dead != 0 {
		t.Fatalf("after append total/dead = %d/%d, want %d/0", total, dead, want)
	}
	c.unlink(uint64(len(frames[0])))
	if total, dead := c.logBytes(); total != want || dead != uint64(len(frames[0])) {
		t.Fatalf("after unlink total/dead = %d/%d, want %d/%d", total, dead, want, len(frames[0]))
	}
}

// TestOpenWiresAkiCold confirms Open builds the cold adapter for the given shard
// off the same handle as the value-log adapter, while a plain Open leaves it nil.
func TestOpenWiresAkiCold(t *testing.T) {
	f, err := akifile.Create(filepath.Join(t.TempDir(), "shared.aki"), akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 2})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if s.akicold == nil {
		t.Fatal("Open with AkiValueLog left the cold adapter nil")
	}
	if s.akicold.shard != 2 {
		t.Fatalf("cold adapter shard = %d, want 2", s.akicold.shard)
	}
	if s.akicold.f != s.akivlog.f {
		t.Fatal("cold adapter wraps a different file than the value-log adapter")
	}
	offs, err := s.akicold.appendBatch([][]byte{coldTestFrame([]byte("wired"))})
	if err != nil {
		t.Fatalf("appendBatch: %v", err)
	}
	got, err := s.akicold.readFrame(offs[0], nil)
	if err != nil || !bytes.Equal(got, coldTestFrame([]byte("wired"))) {
		t.Fatalf("read back = %q/%v", got, err)
	}

	plain, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("plain open: %v", err)
	}
	if plain.akicold != nil {
		t.Fatal("plain Open built a cold adapter")
	}
}
