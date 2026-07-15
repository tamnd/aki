package sqlo1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestDrainCoalescesPerKey(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ms := NewMemStore()
	d := newDrainer(ht, ms)

	k := []byte("k1")
	for i := range 5 {
		ht.Put(k, fmt.Appendf(nil, "v%d", i), TagString)
	}
	if ht.dirtyN != 1 {
		t.Fatalf("queue holds %d entries after 5 writes to one key", ht.dirtyN)
	}
	n, err := d.drain(ctx)
	if err != nil || n != 1 {
		t.Fatalf("drain = %d %v, want 1 record", n, err)
	}
	rec, err := ms.Get(ctx, k)
	if err != nil || string(rec.Value) != "v4" {
		t.Fatalf("store holds %q %v, want the final value", rec.Value, err)
	}
	hd := &ht.hdrs[slotOf(t, ht, k)]
	if hd.state != stateResident || hd.vptr == 0 {
		t.Fatalf("after drain: state %d vptr %d", hd.state, hd.vptr)
	}
	if ht.dirtyN != 0 || ht.dirtyBytes != 0 {
		t.Fatalf("queue not empty after drain: n %d bytes %d", ht.dirtyN, ht.dirtyBytes)
	}
	if n, _ := d.drain(ctx); n != 0 {
		t.Fatalf("drain of a clean table moved %d records", n)
	}
}

func TestDrainOldestFirstAndBounded(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	d := newDrainer(ht, NewMemStore())
	d.maxOps = 2

	for _, k := range []string{"k1", "k2", "k3"} {
		ht.Put([]byte(k), []byte("v"), TagString)
	}
	// Rewriting the oldest key must not move it to the back of the queue.
	ht.Put([]byte("k1"), []byte("v-rewritten"), TagString)

	if n, err := d.drain(ctx); err != nil || n != 2 {
		t.Fatalf("bounded drain = %d %v, want 2", n, err)
	}
	if ht.hdrs[slotOf(t, ht, []byte("k1"))].state != stateResident {
		t.Fatal("k1 not drained first despite the rewrite")
	}
	if ht.hdrs[slotOf(t, ht, []byte("k2"))].state != stateResident {
		t.Fatal("k2 not drained in the first cycle")
	}
	if ht.hdrs[slotOf(t, ht, []byte("k3"))].state != stateDirty {
		t.Fatal("k3 drained past the op cap")
	}
	if n, err := d.drain(ctx); err != nil || n != 1 {
		t.Fatalf("second cycle = %d %v, want 1", n, err)
	}
}

func TestDrainTombstone(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ms := NewMemStore()
	d := newDrainer(ht, ms)

	k := []byte("k1")
	ht.Put(k, []byte("v"), TagString)
	if _, err := d.drain(ctx); err != nil {
		t.Fatal(err)
	}
	ht.Del(k)
	if ht.dirtyBytes != len(k) {
		t.Fatalf("tombstone dirty bytes %d, want klen %d", ht.dirtyBytes, len(k))
	}
	if n, err := d.drain(ctx); err != nil || n != 1 {
		t.Fatalf("tombstone drain = %d %v", n, err)
	}
	if _, err := ms.Get(ctx, k); !errors.Is(err, ErrNotFound) {
		t.Fatalf("store still holds a deleted key: %v", err)
	}
	if len(ht.freeSlots) != 1 || ht.Len() != 0 {
		t.Fatalf("tombstone slot not retired: %d free, len %d", len(ht.freeSlots), ht.Len())
	}
}

func TestDrainThreshold(t *testing.T) {
	ht := NewHotTable(64)
	d := newDrainer(ht, NewMemStore())
	d.threshold = 64

	ht.Put([]byte("small"), bytes.Repeat([]byte("v"), 8), TagString)
	if d.needsDrain() {
		t.Fatalf("threshold tripped at %d of %d bytes", ht.dirtyBytes, d.threshold)
	}
	ht.Put([]byte("large"), bytes.Repeat([]byte("v"), 64), TagString)
	if !d.needsDrain() {
		t.Fatalf("threshold quiet at %d of %d bytes", ht.dirtyBytes, d.threshold)
	}
	if _, err := d.drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.needsDrain() {
		t.Fatal("threshold still tripped on a clean table")
	}
}

// failStore counts batches and fails on demand, to prove a failed cycle
// keeps everything dirty and reuses its Seq.
type failStore struct {
	MemStore
	fail bool
	seqs []int64
}

func (s *failStore) ApplyBatch(ctx context.Context, b *DrainBatch) error {
	s.seqs = append(s.seqs, b.Seq)
	if s.fail {
		return errors.New("injected store failure")
	}
	return s.MemStore.ApplyBatch(ctx, b)
}

func TestDrainErrorKeepsDirtyAndSeq(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	fs := &failStore{MemStore: *NewMemStore(), fail: true}
	d := newDrainer(ht, fs)

	k := []byte("k1")
	ht.Put(k, []byte("v"), TagString)
	if n, err := d.drain(ctx); err == nil || n != 0 {
		t.Fatalf("failed drain = %d %v, want an error and 0", n, err)
	}
	if ht.hdrs[slotOf(t, ht, k)].state != stateDirty || ht.dirtyN != 1 {
		t.Fatal("failed drain did not keep the record dirty and queued")
	}
	fs.fail = false
	if n, err := d.drain(ctx); err != nil || n != 1 {
		t.Fatalf("retry drain = %d %v", n, err)
	}
	if len(fs.seqs) != 2 || fs.seqs[0] != fs.seqs[1] {
		t.Fatalf("retry must reuse the Seq: %v", fs.seqs)
	}
}

func TestMemStoreClonesBatchMemory(t *testing.T) {
	ctx := context.Background()
	ms := NewMemStore()
	key := []byte("k")
	val := []byte("aaaa")
	if err := ms.ApplyBatch(ctx, &DrainBatch{Seq: 1, Ops: []Op{{Rec: Record{Key: key, Value: val}}}}); err != nil {
		t.Fatal(err)
	}
	copy(val, "bbbb")
	rec, err := ms.Get(ctx, key)
	if err != nil || string(rec.Value) != "aaaa" {
		t.Fatalf("store aliased the caller's bytes: %q %v", rec.Value, err)
	}
}

// nullStore drops every batch, so it isolates the drain cycle's own
// allocation behavior from the placeholder store's map writes.
type nullStore struct{}

func (nullStore) Get(ctx context.Context, key []byte) (Record, error) { return Record{}, ErrNotFound }
func (nullStore) BatchGet(ctx context.Context, keys [][]byte) ([]Record, error) {
	return nil, nil
}
func (nullStore) ApplyBatch(ctx context.Context, b *DrainBatch) error { return nil }
func (nullStore) Scan(ctx context.Context, cur Cursor, fn func(Record) bool) (Cursor, error) {
	return nil, nil
}
func (nullStore) Stats() StoreStats { return StoreStats{} }

// TestDrainCycleZeroAlloc holds the alloczero bar on the scheduler: a
// warm re-dirty plus a full drain cycle reuses the ops and slots slices,
// the batch struct, and every hot-table structure.
func TestDrainCycleZeroAlloc(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	d := newDrainer(ht, nullStore{})
	key := []byte("cycle-key")
	val := bytes.Repeat([]byte("v"), 16)

	cycle := func() {
		if !ht.Put(key, val, TagString) {
			panic("put refused")
		}
		if n, err := d.drain(ctx); err != nil || n != 1 {
			panic("drain did not move the record")
		}
	}
	cycle()
	if allocs := testing.AllocsPerRun(1000, cycle); allocs != 0 {
		t.Errorf("drain cycle: %.1f allocs/op, want 0", allocs)
	}
}
