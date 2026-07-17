package store

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// newTestAkiRlog builds a record log over a fresh .aki in the test's temp dir.
func newTestAkiRlog(t *testing.T, shard uint16) *akiRlog {
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
	return newAkiRlog(f, shard)
}

func recRow(key string, word uint64) akifile.RecordRow {
	return akifile.RecordRow{
		ValueWord: word,
		ValueLen:  uint32(len(key)) + 10,
		ExpireAt:  word + 3,
		Key:       []byte(key),
	}
}

func recEqual(a, b akifile.RecordRow) bool {
	return a.Flags == b.Flags && a.ValueWord == b.ValueWord && a.ValueLen == b.ValueLen &&
		a.ExpireAt == b.ExpireAt && bytes.Equal(a.Key, b.Key)
}

// TestAkiRlogStageReadFlushResolves stages a batch, reads each row back before the
// cut, flushes once, and resolves each published address: the store's record-log
// contract end to end over the .aki log region.
func TestAkiRlogStageReadFlushResolves(t *testing.T) {
	l := newTestAkiRlog(t, 3)

	rows := []akifile.RecordRow{
		recRow("alpha", 100),
		{Flags: akifile.RecFlagTombstone, Key: []byte("beta")},
		recRow("gamma", 200),
	}
	for i, r := range rows {
		if got := l.stage(r); got != i {
			t.Fatalf("stage %d returned index %d", i, got)
		}
		// Readable from the pending buffer before the segment is cut.
		got, err := l.readStaged(i)
		if err != nil {
			t.Fatalf("read staged %d: %v", i, err)
		}
		if !recEqual(got, r) {
			t.Fatalf("staged read %d = %+v, want %+v", i, got, r)
		}
	}
	if l.staged() != len(rows) {
		t.Fatalf("staged = %d, want %d", l.staged(), len(rows))
	}

	addrs, err := l.flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(addrs) != len(rows) {
		t.Fatalf("got %d addresses, want %d", len(addrs), len(rows))
	}
	for i, addr := range addrs {
		got, err := l.readAt(addr)
		if err != nil {
			t.Fatalf("read record %d at %d: %v", i, addr, err)
		}
		if !recEqual(got, rows[i]) {
			t.Fatalf("record %d = %+v, want %+v", i, got, rows[i])
		}
	}
}

// TestAkiRlogLogBytesAccounting checks the total moves by the flushed payload and
// the dead counter moves by an unlink, the pair a checkpoint persists and a
// compaction weighs.
func TestAkiRlogLogBytesAccounting(t *testing.T) {
	l := newTestAkiRlog(t, 0)

	l.stage(recRow("k0", 1))
	l.stage(recRow("k1", 2))
	want := uint64(l.pendingBytes())
	if _, err := l.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	total, dead := l.logBytes()
	if total != want {
		t.Fatalf("total = %d after flush, want the flushed payload %d", total, want)
	}
	if dead != 0 {
		t.Fatalf("dead = %d before any unlink, want 0", dead)
	}

	l.unlink(30)
	if _, dead := l.logBytes(); dead != 30 {
		t.Fatalf("dead = %d after unlink 30, want 30", dead)
	}
}

// TestAkiRlogEmptyFlushHoldsSeq flushes an empty batch: no segment is cut, no
// address is returned, the total does not move, and the shard sequence is held so
// a timer-driven flush mints no empty segment.
func TestAkiRlogEmptyFlushHoldsSeq(t *testing.T) {
	l := newTestAkiRlog(t, 1)

	addrs, err := l.flush()
	if err != nil {
		t.Fatalf("empty flush: %v", err)
	}
	if addrs != nil {
		t.Fatalf("empty flush returned %d addresses, want none", len(addrs))
	}
	if l.seq != 0 {
		t.Fatalf("seq = %d after an empty flush, want 0", l.seq)
	}
	if total, _ := l.logBytes(); total != 0 {
		t.Fatalf("total = %d after an empty flush, want 0", total)
	}
}

// TestAkiRlogTwoFlushesSeparate confirms a second batch lands in its own segment
// with its own addresses and both batches read back, so the sequence advances per
// real cut.
func TestAkiRlogTwoFlushesSeparate(t *testing.T) {
	l := newTestAkiRlog(t, 2)

	l.stage(recRow("first", 1))
	a1, err := l.flush()
	if err != nil {
		t.Fatalf("flush 1: %v", err)
	}
	l.stage(recRow("second", 2))
	a2, err := l.flush()
	if err != nil {
		t.Fatalf("flush 2: %v", err)
	}
	if a1[0] == a2[0] {
		t.Fatalf("two flushes shared an address %d", a1[0])
	}
	if l.seq != 2 {
		t.Fatalf("seq = %d after two flushes, want 2", l.seq)
	}
	r1, err := l.readAt(a1[0])
	if err != nil || string(r1.Key) != "first" {
		t.Fatalf("read first = %+v/%v", r1, err)
	}
	r2, err := l.readAt(a2[0])
	if err != nil || string(r2.Key) != "second" {
		t.Fatalf("read second = %+v/%v", r2, err)
	}
}

// TestOpenWiresAkiRecordLog confirms Open builds the record-log adapter when the
// handle is set and leaves it nil otherwise, and that a plain store still opens.
func TestOpenWiresAkiRecordLog(t *testing.T) {
	f, err := akifile.Create(filepath.Join(t.TempDir(), "wire.aki"), akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	s, err := Open(Options{
		ArenaBytes:       4 << 20,
		SegBytes:         1 << 20,
		AkiValueLog:      f,
		Shard:            1,
		ResidentCapBytes: 128,
	})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if s.akirlog == nil {
		t.Fatal("Open left akirlog nil with a handle set")
	}
	// The adapter drives a real cut through the shared file.
	s.akirlog.stage(recRow("wired", 1))
	if _, err := s.akirlog.flush(); err != nil {
		t.Fatalf("adapter flush: %v", err)
	}

	plain, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open plain store: %v", err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	if plain.akirlog != nil {
		t.Fatal("plain Open built an akirlog without a handle")
	}
}
