package sqlo1

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

// fillStandard allocates class-1024 payloads until the budget refuses
// and returns the refs.
func fillStandard(t *testing.T, a *arena) []uint32 {
	t.Helper()
	var refs []uint32
	for i := 0; ; i++ {
		ref := a.alloc(bytes.Repeat([]byte{byte(i)}, 1000))
		if ref == 0 {
			break
		}
		refs = append(refs, ref)
		if i > arenaMaxRefs {
			t.Fatal("budget never refused")
		}
	}
	return refs
}

// TestArenaReclaimClassMigration is the class-migration deadlock in its
// smallest form: a saturated budget, every slot free in one class, and
// the first allocation of a class the arena has never served. Freelists
// recycle within a class and never return budget, so before reclaim
// this allocation could never succeed.
func TestArenaReclaimClassMigration(t *testing.T) {
	a := arena{budget: &arenaBudget{limit: 3 * arenaChunkSize}}
	refs := fillStandard(t, &a)
	if len(a.chunks) != 3 {
		t.Fatalf("fill built %d chunks, want 3", len(a.chunks))
	}
	for _, ref := range refs {
		a.release(ref)
	}

	big := bytes.Repeat([]byte("B"), 5000) // class 8192, first touch
	ref := a.alloc(big)
	if ref == 0 {
		t.Fatal("class-migration alloc refused with every slot free")
	}
	if !bytes.Equal(a.data(ref), big) {
		t.Fatal("migrated alloc read back wrong bytes")
	}
	if a.budget.reserved > a.budget.limit {
		t.Fatalf("reserved %d over limit %d", a.budget.reserved, a.budget.limit)
	}

	// The retired chunk's freelist refs must be gone: a stale ref would
	// hand out a slot in a nil or reused chunk. Refill the old class and
	// read everything back.
	var again []uint32
	for i := 0; i < 1023; i++ {
		r := a.alloc(bytes.Repeat([]byte{byte(i)}, 1000))
		if r == 0 {
			t.Fatalf("refill alloc %d refused", i)
		}
		again = append(again, r)
	}
	for i, r := range again {
		if !bytes.Equal(a.data(r), bytes.Repeat([]byte{byte(i)}, 1000)) {
			t.Fatalf("refill alloc %d read back wrong bytes", i)
		}
	}
	if !bytes.Equal(a.data(ref), big) {
		t.Fatal("refill churn corrupted the migrated alloc")
	}
}

// TestArenaOversizeAfterReclaim covers the oversize retry: an oversize
// chunk needs contiguous budget headroom, which only reclaim can
// recover once the standard chunks have soaked the limit.
func TestArenaOversizeAfterReclaim(t *testing.T) {
	a := arena{budget: &arenaBudget{limit: 4 * arenaChunkSize}}
	refs := fillStandard(t, &a)
	for _, ref := range refs {
		a.release(ref)
	}
	big := bytes.Repeat([]byte("z"), 600<<10) // oversize, needs two retired chunks
	ref := a.alloc(big)
	if ref == 0 {
		t.Fatal("oversize alloc refused with every slot free")
	}
	if !bytes.Equal(a.data(ref), big) {
		t.Fatal("oversize alloc read back wrong bytes")
	}
	if a.budget.reserved > a.budget.limit {
		t.Fatalf("reserved %d over limit %d", a.budget.reserved, a.budget.limit)
	}
}

// TestArenaReclaimKeepsChunkZero pins the ref-0 invariant: chunk 0's
// reserved pad slot counts as live footprint, so reclaim can never
// retire it and ref 0 stays dead forever.
func TestArenaReclaimKeepsChunkZero(t *testing.T) {
	a := arena{budget: &arenaBudget{limit: 3 * arenaChunkSize}}
	refs := fillStandard(t, &a)
	for _, ref := range refs {
		a.release(ref)
	}
	a.reclaim()
	if a.chunks[0] == nil {
		t.Fatal("reclaim retired chunk 0")
	}
	if a.liveFp[0] != arenaAlign {
		t.Fatalf("chunk 0 live footprint %d, want the pad's %d", a.liveFp[0], arenaAlign)
	}
}

// TestHotVacateClassMigration drives the deadlock through the hot
// table, where reclaim alone is not enough: the free slots are spread
// across chunks that all still hold clean residents, so some chunk's
// residents must be force-evicted before its bytes can rebudget. This
// is the shape the xcatchup lab died on at 756613 of 10M entries, with
// a paged stream root crossing the 4096 to 8192 class boundary against
// a saturated arena.
func TestHotVacateClassMigration(t *testing.T) {
	ht := NewHotTable(4096)
	shared := &arenaBudget{limit: 4 * arenaChunkSize} // 1 key chunk + 3 value chunks
	ht.keys.budget = shared
	ht.vals.budget = shared
	ht.SetTick(1)

	n := 0
	for ; ; n++ {
		key := fmt.Appendf(nil, "k%06d", n)
		if !ht.Put(key, bytes.Repeat([]byte{byte(n)}, 1000), TagString) {
			break
		}
	}
	if n == 0 {
		t.Fatal("fill never started")
	}
	// Cool everything: dirty never evicts (R-I3), so the vacate needs
	// the table drained, exactly like the ladder stage does it.
	for {
		s, ok := ht.popDirty()
		if !ok {
			break
		}
		ht.drained(s, 1)
	}

	before := ht.Len()
	if !ht.vacateValChunk() {
		t.Fatal("vacate found no chunk with every resident clean")
	}
	if ht.vacates != 1 {
		t.Fatalf("vacates = %d, want 1", ht.vacates)
	}
	evicted := before - ht.Len()
	if evicted <= 0 {
		t.Fatal("vacate evicted nothing")
	}

	big := bytes.Repeat([]byte("B"), 5000) // class 8192, first touch
	if !ht.Put([]byte("root"), big, TagString) {
		t.Fatal("class-migration Put refused after vacate")
	}
	if v, ok := ht.Get([]byte("root")); !ok || !bytes.Equal(v, big) {
		t.Fatal("migrated value read back wrong")
	}

	// Survivors must be intact and the evicted exactly account for the
	// difference; a vacate that corrupted refs would misread here.
	hits := 0
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "k%06d", i)
		if v, ok := ht.Get(key); ok {
			if !bytes.Equal(v, bytes.Repeat([]byte{byte(i)}, 1000)) {
				t.Fatalf("survivor %d read back wrong bytes", i)
			}
			hits++
		}
	}
	if hits != n-evicted {
		t.Fatalf("survivors = %d, want %d of %d after %d evictions",
			hits, n-evicted, n, evicted)
	}
}

// TestTieredClassMigrationDeadlock is the end-to-end regression: a
// Tiered under a small budget, filler churn to saturation, then one key
// growing across a size-class boundary. Before the vacate stage this
// returned errHotFull no matter how much the ladder evicted, because
// eviction fed class freelists and never the budget.
func TestTieredClassMigrationDeadlock(t *testing.T) {
	ctx := context.Background()
	tr := NewTiered(NewMemStore(), TieredConfig{
		Budget: Budget{Entries: 8192, Arenas: 4 << 20},
		Seed:   7,
	})

	filler := bytes.Repeat([]byte("f"), 4000)
	for i := 0; i < 2000; i++ {
		key := fmt.Appendf(nil, "fill%06d", i)
		if err := tr.Set(ctx, key, filler, TagString); err != nil {
			t.Fatalf("filler %d: %v", i, err)
		}
	}

	root := []byte("s:root")
	if err := tr.Set(ctx, root, bytes.Repeat([]byte("r"), 4000), TagString); err != nil {
		t.Fatalf("root at class 4096: %v", err)
	}
	grown := bytes.Repeat([]byte("R"), 5000) // crosses into class 8192
	if err := tr.Set(ctx, root, grown, TagString); err != nil {
		t.Fatalf("root crossing the class boundary: %v", err)
	}
	if v, ok, err := tr.Get(ctx, root); err != nil || !ok || !bytes.Equal(v, grown) {
		t.Fatalf("grown root read back: ok=%v err=%v", ok, err)
	}
}
