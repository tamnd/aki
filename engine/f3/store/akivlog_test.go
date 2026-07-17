package store

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// newTestAkiVlog builds a value log over a fresh .aki in the test's temp dir.
func newTestAkiVlog(t *testing.T, shard uint16) *akiVlog {
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
	return newAkiVlog(f, shard)
}

// TestAkiVlogStageReadFlushResolves stages a batch, reads each value back before
// the cut, flushes once, and resolves each published pointer by its bare offset
// and length: the store's spill contract end to end over the .aki value region.
func TestAkiVlogStageReadFlushResolves(t *testing.T) {
	l := newTestAkiVlog(t, 3)

	vals := [][]byte{[]byte("alpha"), bytes.Repeat([]byte("b"), 3000), []byte("gamma")}
	for i, v := range vals {
		if got := l.stage(v); got != i {
			t.Fatalf("stage %d returned index %d", i, got)
		}
		// Readable from the pending buffer before the segment is cut.
		got, err := l.readStaged(i)
		if err != nil {
			t.Fatalf("read staged %d: %v", i, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("staged read %d = %q, want %q", i, got, v)
		}
	}
	if l.staged() != len(vals) {
		t.Fatalf("staged = %d, want %d", l.staged(), len(vals))
	}

	ptrs, err := l.flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(ptrs) != len(vals) {
		t.Fatalf("got %d pointers, want %d", len(ptrs), len(vals))
	}
	var buf []byte
	for i, p := range ptrs {
		got, err := l.readAt(p.ValueOff, int(p.ValueLen), buf)
		if err != nil {
			t.Fatalf("read value %d: %v", i, err)
		}
		if !bytes.Equal(got, vals[i]) {
			t.Fatalf("value %d = %q, want %q", i, got, vals[i])
		}
		buf = got[:0]
	}
}

// TestAkiVlogReadFillAndInto stages a value, flushes it, and reads sub-ranges of
// the published bytes by absolute offset through readFill and readInto: the partial
// read the bitmap and chunked bands take against the re-homed value log, parity with
// the scratch log's readFill/readInto.
func TestAkiVlogReadFillAndInto(t *testing.T) {
	l := newTestAkiVlog(t, 3)

	val := []byte("0123456789abcdef")
	l.stage(val)
	ptrs, err := l.flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	off := ptrs[0].ValueOff

	// readFill fills a caller buffer with a window; readInto returns the bytes.
	fill := make([]byte, 5)
	if err := l.readFill(off+6, fill); err != nil {
		t.Fatalf("readFill: %v", err)
	}
	if want := val[6:11]; !bytes.Equal(fill, want) {
		t.Fatalf("readFill = %q, want %q", fill, want)
	}
	got, err := l.readInto(off, 4, nil)
	if err != nil {
		t.Fatalf("readInto: %v", err)
	}
	if want := val[:4]; !bytes.Equal(got, want) {
		t.Fatalf("readInto = %q, want %q", got, want)
	}
}

// TestAkiVlogLogBytesAccounting checks total counts every flushed value byte and
// unlink moves the dead subset, the pair a value-region compaction weighs.
func TestAkiVlogLogBytesAccounting(t *testing.T) {
	l := newTestAkiVlog(t, 0)

	vals := [][]byte{[]byte("one"), []byte("twotwo"), []byte("threethree")}
	var want uint64
	for _, v := range vals {
		l.stage(v)
		want += uint64(len(v))
	}
	if _, err := l.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if total, dead := l.logBytes(); total != want || dead != 0 {
		t.Fatalf("after flush total/dead = %d/%d, want %d/0", total, dead, want)
	}

	l.unlink(uint64(len(vals[0])))
	if total, dead := l.logBytes(); total != want || dead != uint64(len(vals[0])) {
		t.Fatalf("after unlink total/dead = %d/%d, want %d/%d", total, dead, want, len(vals[0]))
	}
}

// TestAkiVlogEmptyFlushHoldsSequence confirms a flush with nothing staged writes
// no segment and does not burn a shard_seq, so the sequence advances only on a
// real cut.
func TestAkiVlogEmptyFlushHoldsSequence(t *testing.T) {
	l := newTestAkiVlog(t, 1)

	ptrs, err := l.flush()
	if err != nil || ptrs != nil {
		t.Fatalf("empty flush = %v/%v, want nil/nil", ptrs, err)
	}
	if l.seq != 0 {
		t.Fatalf("empty flush advanced seq to %d", l.seq)
	}

	l.stage([]byte("real"))
	if _, err := l.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if l.seq != 1 {
		t.Fatalf("real flush left seq at %d, want 1", l.seq)
	}
}

// TestAkiVlogSeparatesFlushes stages across two flushes and confirms the second
// batch's pointer sits past the first, so pointers from both cuts resolve.
func TestAkiVlogSeparatesFlushes(t *testing.T) {
	l := newTestAkiVlog(t, 2)

	l.stage([]byte("first-batch"))
	first, err := l.flush()
	if err != nil {
		t.Fatalf("first flush: %v", err)
	}
	l.stage([]byte("second-batch"))
	second, err := l.flush()
	if err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if second[0].ValueOff <= first[0].ValueOff {
		t.Fatalf("second batch at %d did not advance past the first at %d", second[0].ValueOff, first[0].ValueOff)
	}
	for name, p := range map[string]akifile.ValuePointer{"first-batch": first[0], "second-batch": second[0]} {
		got, err := l.readAt(p.ValueOff, int(p.ValueLen), nil)
		if err != nil || string(got) != name {
			t.Fatalf("read %q = %q/%v", name, got, err)
		}
	}
}

// TestOpenWiresAkiValueLog opens a store with an .aki handle and confirms the
// adapter is constructed for the given shard and usable, while an ordinary Open
// leaves it nil so the scratch path is untouched.
func TestOpenWiresAkiValueLog(t *testing.T) {
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
	if s.akivlog == nil {
		t.Fatal("Open with AkiValueLog left the adapter nil")
	}
	if s.akivlog.shard != 2 {
		t.Fatalf("adapter shard = %d, want 2", s.akivlog.shard)
	}
	// The spill accumulator is built over the same adapter, ready for the flip.
	if s.akispill == nil {
		t.Fatal("Open with AkiValueLog left the spill accumulator nil")
	}
	if s.akispill.v != s.akivlog {
		t.Fatal("spill accumulator wraps a different adapter than akivlog")
	}
	// The wired adapter drives a real cut against the shared file.
	s.akivlog.stage([]byte("wired"))
	ptrs, err := s.akivlog.flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	got, err := s.akivlog.readAt(ptrs[0].ValueOff, int(ptrs[0].ValueLen), nil)
	if err != nil || string(got) != "wired" {
		t.Fatalf("read back = %q/%v", got, err)
	}
	// Close is store-narrow: it must not close the borrowed .aki handle, which
	// the shard runtime still owns.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := f.AppendValues(2, 99, [][]byte{[]byte("still-open")}); err != nil {
		t.Fatalf("shared file unusable after store Close: %v", err)
	}

	plain, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("plain open: %v", err)
	}
	if plain.akivlog != nil {
		t.Fatal("plain Open built an akivlog adapter")
	}
	if plain.akispill != nil {
		t.Fatal("plain Open built a spill accumulator")
	}
}
