package store

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// The value-log flip: a store opened over an .aki value region spills a
// separated or chunked value's bytes into the shared region through the akivlog
// adapter instead of a per-shard scratch log, reads them back through the same
// seam, and accounts their drop as dead in that region. These tests exercise the
// whole path end to end over a real .aki, the first consumer to tie the read
// seam, the drop seam, and the spill bookkeeping together.
//
// The flip is eager here: writeRun stages one frame and resolves it at once, so
// the word it returns is already absolute and no provisional word reaches the
// read seam. The batched group-boundary form through the spill ledger is the
// reactor-side follow-up.

// newFlipStore opens a store backed by a fresh .aki with a resident cap tiny
// enough that any separated or chunked value spills to the shared region rather
// than the arena. The arena is still sized for the records, headers, and keys,
// which always stay resident.
func newFlipStore(t *testing.T) *Store {
	t.Helper()
	f, err := akifile.Create(filepath.Join(t.TempDir(), "flip.aki"), akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	s, err := Open(Options{
		ArenaBytes:       8 << 20,
		SegBytes:         1 << 20,
		AkiValueLog:      f,
		Shard:            1,
		ResidentCapBytes: 128,
	})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestWriteRunFlipsSeparatedToAkiRegion stores a separated-band value past the
// resident cap and reads it back: the bytes spill to the .aki value region and
// resolve through the read seam. The run count and the region's total bytes both
// move, the evidence the value left the arena.
func TestWriteRunFlipsSeparatedToAkiRegion(t *testing.T) {
	s := newFlipStore(t)

	// 4000 bytes: past strInlineMax, under strChunkMin, so a single separated run.
	val := bytes.Repeat([]byte("s"), 4000)
	if err := s.Set([]byte("sep"), val); err != nil {
		t.Fatalf("set: %v", err)
	}
	if runs := s.Stats().LogRuns; runs != 1 {
		t.Fatalf("LogRuns = %d after one separated spill, want 1", runs)
	}
	total, dead := s.LogBytes()
	if total == 0 {
		t.Fatal("LogBytes total = 0, the value did not spill to the .aki region")
	}
	if dead != 0 {
		t.Fatalf("LogBytes dead = %d before any drop, want 0", dead)
	}

	got, ok := s.Get([]byte("sep"), nil)
	if !ok {
		t.Fatal("get missed the spilled key")
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("read back %d bytes, want %d", len(got), len(val))
	}
}

// TestWriteRunFlipsChunkedToAkiRegion stores a chunked-band value: every chunk
// spills as its own run to the .aki region and the whole value materializes back
// through the seam's per-chunk fills.
func TestWriteRunFlipsChunkedToAkiRegion(t *testing.T) {
	s := newFlipStore(t)

	// 200000 bytes at a 64KiB chunk width is four chunks, four separate runs.
	val := bytes.Repeat([]byte("abcd"), 50000)
	if err := s.Set([]byte("big"), val); err != nil {
		t.Fatalf("set: %v", err)
	}
	if runs := s.Stats().LogRuns; runs != 4 {
		t.Fatalf("LogRuns = %d, want 4 chunk runs", runs)
	}
	if total, _ := s.LogBytes(); total < uint64(len(val)) {
		t.Fatalf("LogBytes total = %d, want at least the value's %d bytes", total, len(val))
	}

	got, ok := s.Get([]byte("big"), nil)
	if !ok {
		t.Fatal("get missed the chunked key")
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("read back %d bytes, want %d", len(got), len(val))
	}
}

// TestWriteRunDropAccountsDeadInAkiRegion overwrites a spilled separated value
// and checks the superseded run's bytes land in the region's dead counter
// through the drop seam, the accounting a later region compaction reads.
func TestWriteRunDropAccountsDeadInAkiRegion(t *testing.T) {
	s := newFlipStore(t)

	first := bytes.Repeat([]byte("x"), 4000)
	if err := s.Set([]byte("k"), first); err != nil {
		t.Fatalf("first set: %v", err)
	}
	if _, dead := s.LogBytes(); dead != 0 {
		t.Fatalf("dead = %d before overwrite, want 0", dead)
	}

	second := bytes.Repeat([]byte("y"), 5000)
	if err := s.Set([]byte("k"), second); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	// The old 4000-byte run is now dead; the live run count stays one.
	if _, dead := s.LogBytes(); dead != uint64(len(first)) {
		t.Fatalf("dead = %d after overwrite, want %d", dead, len(first))
	}
	if runs := s.Stats().LogRuns; runs != 1 {
		t.Fatalf("LogRuns = %d after overwrite, want 1", runs)
	}

	got, ok := s.Get([]byte("k"), nil)
	if !ok || !bytes.Equal(got, second) {
		t.Fatalf("read back = %q/%v, want the second value", got, ok)
	}
}
