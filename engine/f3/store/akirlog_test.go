package store

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
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
	return newAkiRlog(f, shard, nil)
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

// TestAkiRlogSharedWriterSerializesShards is the slice-4a proof: many shard adapters
// share one .aki and one group-commit writer, and each flushes concurrently through
// the gw path rather than cutting the segment itself. The one writer must serialize
// every append onto the single-writer file, so a raced cursor would tear a record or
// collide two addresses. Each adapter stamps its records with its own shard number, so
// a swapped or dropped append shows up as a foreign key on a shard's own walk. It
// asserts every flush resolves absolute addresses that round-trip through readAt and
// that each shard's walk sees exactly its own records. Run under -race this proves the
// store-side seam routes S owners through the one writer with no direct-AppendGroup
// race, the same guarantee the akifile-level test makes one layer down.
func TestAkiRlogSharedWriterSerializesShards(t *testing.T) {
	const shards, batches, perBatch = 4, 40, 3
	f, err := akifile.Create(filepath.Join(t.TempDir(), "shared.aki"), akifile.CreateOptions{
		ShardCount:   shards,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	// A ring smaller than the batch count forces the backpressure spin in submitWait.
	gw := akifile.NewGroupWriter(f, shards, 8)
	defer gw.Stop()

	logs := make([]*akiRlog, shards)
	for s := range logs {
		logs[s] = newAkiRlog(f, uint16(s), gw)
	}

	type resolved struct {
		key  string
		addr uint64
	}
	var mu sync.Mutex
	got := make([]resolved, 0, shards*batches*perBatch)
	var wg sync.WaitGroup
	wg.Add(shards)
	for s := 0; s < shards; s++ {
		go func(shard int) {
			defer wg.Done()
			l := logs[shard]
			for b := 0; b < batches; b++ {
				keys := make([]string, perBatch)
				for i := 0; i < perBatch; i++ {
					keys[i] = fmt.Sprintf("s%02d-b%03d-k%d", shard, b, i)
					l.stage(recRow(keys[i], uint64(shard)<<40|uint64(b)<<8|uint64(i)))
				}
				addrs, err := l.flush()
				if err != nil {
					t.Errorf("shard %d flush %d: %v", shard, b, err)
					return
				}
				if len(addrs) != perBatch {
					t.Errorf("shard %d flush %d: %d addrs, want %d", shard, b, len(addrs), perBatch)
					return
				}
				mu.Lock()
				for i, a := range addrs {
					got = append(got, resolved{keys[i], a})
				}
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()

	if len(got) != shards*batches*perBatch {
		t.Fatalf("resolved %d records, want %d", len(got), shards*batches*perBatch)
	}
	seen := make(map[uint64]bool, len(got))
	for _, g := range got {
		if seen[g.addr] {
			t.Fatalf("duplicate address %#x for %s", g.addr, g.key)
		}
		seen[g.addr] = true
		row, err := f.ReadRecordAt(g.addr)
		if err != nil {
			t.Fatalf("read %s at %#x: %v", g.key, g.addr, err)
		}
		if string(row.Key) != g.key {
			t.Fatalf("address %#x decoded key %q, want %q", g.addr, row.Key, g.key)
		}
	}

	// Each shard's own walk must see exactly its own records and no neighbor's, the
	// per-shard segregation the shared writer preserves through the shard tag.
	for s := 0; s < shards; s++ {
		n := 0
		err := logs[s].walkShard(func(_ uint64, row akifile.RecordRow) error {
			if want := fmt.Sprintf("s%02d-", s); string(row.Key[:4]) != want {
				t.Errorf("shard %d walk saw foreign key %q", s, row.Key)
			}
			n++
			return nil
		})
		if err != nil {
			t.Fatalf("walk shard %d: %v", s, err)
		}
		if n != batches*perBatch {
			t.Fatalf("shard %d walk: %d records, want %d", s, n, batches*perBatch)
		}
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
