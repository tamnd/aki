package sqlo1

// Eviction under tiering, doc 04 section 8: the composite's eviction
// path never performs IO (R-I5) and never victimizes dirty records
// (R-I3). Both are structural in the evictor, so these tests prove
// them from the outside: a tripwire store that fails on any call
// during eviction pressure, and a measured victim count over a mixed
// clean-and-dirty population.

import (
	"context"
	"fmt"
	"testing"
)

// tripwireStore fails the test on any store call while armed. Filling
// and flushing happen disarmed; the wire arms just before eviction
// pressure is applied, so a single BatchGet or ApplyBatch attributable
// to eviction trips it.
type tripwireStore struct {
	Store
	t     *testing.T
	armed bool
	calls int
}

func (w *tripwireStore) BatchGet(ctx context.Context, keys [][]byte) ([]Record, error) {
	if w.armed {
		w.calls++
		w.t.Errorf("BatchGet reached the store during eviction pressure")
	}
	return w.Store.BatchGet(ctx, keys)
}

func (w *tripwireStore) ApplyBatch(ctx context.Context, b *DrainBatch) error {
	if w.armed {
		w.calls++
		w.t.Errorf("ApplyBatch reached the store during eviction pressure")
	}
	return w.Store.ApplyBatch(ctx, b)
}

// TestTieredEvictionNeverDoesIO packs the tier with clean residents,
// arms the tripwire, and writes through the public path until room has
// to be made repeatedly. Every refused insert must be satisfied by
// eviction alone: zero store calls while the wire is armed.
func TestTieredEvictionNeverDoesIO(t *testing.T) {
	ctx := context.Background()
	ws := &tripwireStore{Store: NewMemStore(), t: t}
	now := int64(1) << 41
	tr := NewTiered(ws, TieredConfig{
		Budget:   Budget{Entries: 64, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     31,
		NowMs:    func() int64 { return now },
	})

	filled := 0
	for i := range 10_000 {
		if !tr.ht.Put(fmt.Appendf(nil, "res-%03d", i), []byte("value"), TagString) {
			break
		}
		filled++
	}
	if filled == 0 || filled >= 10_000 {
		t.Fatalf("fill count %d: the budget never refused", filled)
	}
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	ws.armed = true
	for i := range 32 {
		if err := tr.Set(ctx, fmt.Appendf(nil, "new-%03d", i), []byte("fresh"), TagString); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	ws.armed = false
	if ws.calls != 0 {
		t.Fatalf("eviction pressure reached the store %d times", ws.calls)
	}
	st := tr.Stats()
	if st.Evictions == 0 {
		t.Fatal("32 writes into a full tier evicted nothing")
	}

	// The writes all landed hot and dirty, exactly as written.
	for i := range 32 {
		v, hit, definitive := tr.ht.probeRead(fmt.Appendf(nil, "new-%03d", i))
		if !hit || !definitive || string(v) != "fresh" {
			t.Fatalf("new key %d not hot after eviction made room", i)
		}
	}
}

// TestTieredCleanFirstBiasMeasured builds a mixed population, 192
// clean residents and 48 dirty records, applies unbounded eviction
// pressure, and measures the victim split through the stats counters:
// every victim clean, every dirty record untouched, dirty bytes
// unmoved.
func TestTieredCleanFirstBiasMeasured(t *testing.T) {
	r := newTieredRig(t, 256, -1, 33)
	for i := range 192 {
		r.set(t, fmt.Sprintf("res-%03d", i), "value")
	}
	r.flush(t)
	for i := range 48 {
		r.set(t, fmt.Sprintf("dirty-%02d", i), "value")
	}
	before := r.t.Stats()

	residents := func() int {
		n := 0
		for i := range r.t.ht.hdrs {
			if r.t.ht.hdrs[i].state == stateResident {
				n++
			}
		}
		return n
	}
	for i := 0; residents() > 0; i++ {
		if i > 1000 {
			t.Fatalf("evictor left %d residents after %d rounds", residents(), i)
		}
		r.t.ev.evict(1 << 20)
	}

	after := r.t.Stats()
	victims := after.Evictions - before.Evictions
	if victims != 192 {
		t.Fatalf("%d victims, want exactly the 192 residents", victims)
	}
	if after.EvictedBytes <= before.EvictedBytes {
		t.Fatal("eviction freed no bytes by the counter")
	}
	if after.DirtyBytes != before.DirtyBytes {
		t.Fatalf("dirty bytes moved %d -> %d under pure eviction pressure", before.DirtyBytes, after.DirtyBytes)
	}
	survivors := 0
	for i := range 48 {
		if _, hit, definitive := r.t.ht.probeRead(fmt.Appendf(nil, "dirty-%02d", i)); hit && definitive {
			survivors++
		}
	}
	if survivors != 48 {
		t.Fatalf("%d of 48 dirty records survived unbounded eviction", survivors)
	}
	t.Logf("clean-first bias: %d/%d victims clean, %d/%d dirty survivors", victims, victims, survivors, 48)
}

// TestTieredAllDirtySetDrainsThenEvicts writes twice the tier's
// capacity with no flush between, so every refusal finds nothing clean
// and makeRoom must cool records through the store before eviction can
// free a slot: the doc 04 section 8 tail, end to end through Set.
func TestTieredAllDirtySetDrainsThenEvicts(t *testing.T) {
	r := newTieredRig(t, 16, -1, 35)
	for i := range 32 {
		r.set(t, fmt.Sprintf("k%02d", i), "value")
	}
	if r.cs.applyBatches == 0 {
		t.Fatal("an all-dirty tier made room without draining")
	}
	if st := r.t.Stats(); st.Evictions == 0 {
		t.Fatal("no evictions after drains cooled the tier")
	}
	for i := range 32 {
		if v, ok := r.get(t, fmt.Sprintf("k%02d", i)); !ok || string(v) != "value" {
			t.Fatalf("k%02d = %q %v after drain-then-evict churn", i, v, ok)
		}
	}
}
