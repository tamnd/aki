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

// TestDrainCarriesRootFlag: the TagRoot header bit crosses the seam as
// Record.Root, and only for the keys that carry it.
func TestDrainCarriesRootFlag(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ms := NewMemStore()
	d := newDrainer(ht, ms)

	ht.Put([]byte("root"), []byte("root payload"), TagString|TagRoot)
	ht.Put([]byte("plain"), []byte("v"), TagString)
	if n, err := d.drain(ctx); err != nil || n != 2 {
		t.Fatalf("drain = %d %v", n, err)
	}
	rec, err := ms.Get(ctx, []byte("root"))
	if err != nil || !rec.Root {
		t.Fatalf("root record: root=%v err=%v", rec.Root, err)
	}
	rec, err = ms.Get(ctx, []byte("plain"))
	if err != nil || rec.Root {
		t.Fatalf("plain record: root=%v err=%v", rec.Root, err)
	}
}

// TestBumpRidesRootBatch: a registered bump leaves in the same DrainBatch
// as its root key's op, and only that one; a bump whose root has not been
// dirtied yet stays registered for a later cycle.
func TestBumpRidesRootBatch(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ms := NewMemStore()
	d := newDrainer(ht, ms)

	d.addBump([]byte("hot-root"), 7, 2)
	d.addBump([]byte("hot-root"), 7, 3)
	d.addBump([]byte("cold-root"), 9, 5)
	ht.Put([]byte("hot-root"), []byte("img"), TagString|TagRoot)
	if n, err := d.drain(ctx); err != nil || n != 1 {
		t.Fatalf("drain = %d %v", n, err)
	}
	if live, _ := ms.RootLive(7, 2); live {
		t.Fatal("both bumps for the drained root did not ride its batch")
	}
	if live, _ := ms.RootLive(9, 4); !live {
		t.Fatal("bump for an undirtied root left the drainer early")
	}
	if _, ok := d.pending["hot-root"]; ok {
		t.Fatal("carried bump still registered after a successful drain")
	}
	if _, ok := d.pending["cold-root"]; !ok {
		t.Fatal("uncarried bump dropped from the drainer")
	}

	ht.Put([]byte("cold-root"), []byte("img"), TagString|TagRoot)
	if n, err := d.drain(ctx); err != nil || n != 1 {
		t.Fatalf("second drain = %d %v", n, err)
	}
	if live, _ := ms.RootLive(9, 4); live {
		t.Fatal("held bump did not ride its root's later batch")
	}
	if len(d.pending) != 0 {
		t.Fatalf("%d bumps still registered after both roots drained", len(d.pending))
	}
}

// TestBumpSurvivesFailedDrain: a store error keeps the bump registered,
// and the retry re-attaches it under the reused Seq.
func TestBumpSurvivesFailedDrain(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	fs := &failStore{MemStore: *NewMemStore(), fail: true}
	d := newDrainer(ht, fs)

	d.addBump([]byte("r"), 7, 2)
	ht.Put([]byte("r"), []byte("img"), TagString|TagRoot)
	if _, err := d.drain(ctx); err == nil {
		t.Fatal("injected failure did not surface")
	}
	if _, ok := d.pending["r"]; !ok {
		t.Fatal("failed drain dropped the registered bump")
	}
	fs.fail = false
	if n, err := d.drain(ctx); err != nil || n != 1 {
		t.Fatalf("retry drain = %d %v", n, err)
	}
	if live, _ := fs.RootLive(7, 1); live {
		t.Fatal("retried batch did not carry the bump")
	}
	if len(fs.seqs) != 2 || fs.seqs[0] != fs.seqs[1] {
		t.Fatalf("retry must reuse the Seq: %v", fs.seqs)
	}
	if len(d.pending) != 0 {
		t.Fatal("bump still registered after the successful retry")
	}
}

// TestTieredBumpReachesStore: the type layer's entry point, register
// through Tiered.Bump then dirty the root, lands the bump in the store
// with the root's image on a Flush.
func TestTieredBumpReachesStore(t *testing.T) {
	ctx := context.Background()
	r := newTieredRig(t, 64, 1, 1)

	key := []byte("root")
	r.t.Bump(key, 7, 2)
	if err := r.t.Set(ctx, key, []byte("img"), TagString|TagRoot); err != nil {
		t.Fatal(err)
	}
	if live, _ := r.ms.RootLive(7, 1); !live {
		t.Fatal("bump reached the store before its root drained")
	}
	if err := r.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if live, _ := r.ms.RootLive(7, 1); live {
		t.Fatal("flushed root batch did not carry the bump")
	}
	if live, _ := r.ms.RootLive(7, 2); !live {
		t.Fatal("bump retired the generation it installed")
	}
}

// TestMemStoreRejectsRootGen: the seam contract says a root's generation
// lives in its payload; a Root op with a seam gen must reject the whole
// batch with nothing applied.
func TestMemStoreRejectsRootGen(t *testing.T) {
	ctx := context.Background()
	ms := NewMemStore()
	b := &DrainBatch{Seq: 1, Ops: []Op{
		{Rec: Record{Key: []byte("a"), Value: []byte("v")}},
		{Rec: Record{Key: []byte("r"), Value: []byte("p"), Root: true, Gen: 2}},
	}}
	if err := ms.ApplyBatch(ctx, b); err == nil {
		t.Fatal("root op with a seam gen applied")
	}
	if _, err := ms.Get(ctx, []byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("batch was partially applied: %v", err)
	}
	if hw := ms.Stats().HighWater; hw != 0 {
		t.Fatalf("high-water moved to %d on a rejected batch", hw)
	}
}
