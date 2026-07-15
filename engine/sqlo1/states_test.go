package sqlo1

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"math/rand"
	"testing"
)

func slotOf(t *testing.T, ht *HotTable, key []byte) uint32 {
	t.Helper()
	s, ok := ht.lookup(maphash.Bytes(ht.seed, key), key)
	if !ok {
		t.Fatalf("no slot for %q", key)
	}
	return s
}

func TestGhostRingDirectMapped(t *testing.T) {
	g := newGhostRing(4)
	g.put(5, 10, 20)
	e, ok := g.take(5)
	if !ok || e.lastRead != 10 || e.lastWrite != 20 {
		t.Fatalf("take = %+v %v", e, ok)
	}
	if _, ok := g.take(5); ok {
		t.Fatal("take must clear the slot")
	}

	// Two hashes landing on the same slot: the later one wins.
	g.put(5, 1, 1)
	g.put(9, 2, 2)
	if _, ok := g.take(5); ok {
		t.Fatal("collided ghost survived")
	}
	if e, ok := g.take(9); !ok || e.lastRead != 2 {
		t.Fatalf("winning ghost lost: %+v %v", e, ok)
	}

	// Hash zero is the empty sentinel and is never stored.
	g.put(0, 3, 3)
	if _, ok := g.take(0); ok {
		t.Fatal("hash zero must not be remembered")
	}
}

func TestStampsShiftOnTickChange(t *testing.T) {
	ht := NewHotTable(8)
	ht.SetTick(3)
	k := []byte("k")
	ht.Put(k, []byte("v"), TagString)
	hd := &ht.hdrs[slotOf(t, ht, k)]
	if hd.lastWrite != 3 || hd.prevWrite != 0 {
		t.Fatalf("after insert: lastWrite %d prevWrite %d", hd.lastWrite, hd.prevWrite)
	}

	ht.Get(k)
	ht.Get(k)
	if hd.lastRead != 3 || hd.prevRead != 0 {
		t.Fatalf("same-tick reads must not shift: lastRead %d prevRead %d", hd.lastRead, hd.prevRead)
	}

	ht.SetTick(7)
	ht.Get(k)
	if hd.lastRead != 7 || hd.prevRead != 3 {
		t.Fatalf("cross-tick read: lastRead %d prevRead %d", hd.lastRead, hd.prevRead)
	}
	ht.Put(k, []byte("v2"), TagString)
	ht.Put(k, []byte("v3"), TagString)
	if hd.lastWrite != 7 || hd.prevWrite != 3 {
		t.Fatalf("cross-tick writes: lastWrite %d prevWrite %d", hd.lastWrite, hd.prevWrite)
	}
}

func TestTombstoneLifecycle(t *testing.T) {
	ht := NewHotTable(4)
	k := []byte("k1")
	ht.Put(k, []byte("v1"), TagString)
	s := slotOf(t, ht, k)

	if !ht.Del(k) || ht.Del(k) {
		t.Fatal("del must hit once then miss on the tombstone")
	}
	if _, ok := ht.Get(k); ok {
		t.Fatal("tombstone visible to Get")
	}
	if ht.Len() != 0 {
		t.Fatalf("len = %d with only a tombstone", ht.Len())
	}
	if hd := &ht.hdrs[s]; hd.state != stateDirty || hd.valRef != 0 {
		t.Fatalf("tombstone header: state %d valRef %d", hd.state, hd.valRef)
	}

	// Put revives the tombstone in place: same slot, back to a live dirty key.
	if !ht.Put(k, []byte("v2"), TagString) {
		t.Fatal("revive refused")
	}
	if s2 := slotOf(t, ht, k); s2 != s {
		t.Fatalf("revive moved the key from slot %d to %d", s, s2)
	}
	if v, ok := ht.Get(k); !ok || string(v) != "v2" {
		t.Fatalf("get after revive = %q %v", v, ok)
	}
	if ht.Len() != 1 {
		t.Fatalf("len = %d after revive", ht.Len())
	}

	// A drained tombstone retires the slot entirely.
	ht.Del(k)
	if !ht.drained(s, 0) {
		t.Fatal("tombstone drain refused")
	}
	if _, ok := ht.lookup(maphash.Bytes(ht.seed, k), k); ok {
		t.Fatal("drained tombstone still in the index")
	}
	if len(ht.freeSlots) != 1 || ht.freeSlots[0] != s {
		t.Fatalf("slot not freed: %v", ht.freeSlots)
	}
	if ht.drained(s, 0) {
		t.Fatal("drain of a freed slot accepted")
	}
}

func TestDrainAndEvict(t *testing.T) {
	ht := NewHotTable(16)
	ht.SetTick(2)
	kA := []byte("kA")
	ht.Put(kA, []byte("vA"), TagString)
	sA := slotOf(t, ht, kA)

	if ht.evict(sA, false) {
		t.Fatal("dirty evicted; it must drain first")
	}
	if !ht.drained(sA, 77) {
		t.Fatal("drain refused")
	}
	if hd := &ht.hdrs[sA]; hd.state != stateResident || hd.vptr != 77 {
		t.Fatalf("after drain: state %d vptr %d", hd.state, hd.vptr)
	}
	if ht.drained(sA, 78) {
		t.Fatal("double drain accepted")
	}
	if v, ok := ht.Get(kA); !ok || string(v) != "vA" {
		t.Fatalf("resident copy gone after drain: %q %v", v, ok)
	}

	// Overwrite re-dirties; vptr keeps pointing at the now-stale record.
	ht.Put(kA, []byte("vA2"), TagString)
	if hd := &ht.hdrs[sA]; hd.state != stateDirty || hd.vptr != 77 {
		t.Fatalf("after re-dirty: state %d vptr %d", hd.state, hd.vptr)
	}

	// Evict to cold: nothing kept, no ghost.
	ht.drained(sA, 99)
	if !ht.evict(sA, false) {
		t.Fatal("evict of resident refused")
	}
	if _, ok := ht.Get(kA); ok || ht.Len() != 0 {
		t.Fatal("cold key still readable from the hot tier")
	}
	if _, ok := ht.ghosts.take(maphash.Bytes(ht.seed, kA)); ok {
		t.Fatal("cold eviction left a ghost")
	}

	// Evict to ghost, then reinsert: the stamps come back as history.
	ht.SetTick(5)
	kB := []byte("kB")
	ht.Put(kB, []byte("vB"), TagString)
	sB := slotOf(t, ht, kB)
	ht.drained(sB, 11)
	if !ht.evict(sB, true) {
		t.Fatal("ghost evict refused")
	}
	ht.SetTick(9)
	ht.Put(kB, []byte("vB2"), TagString)
	hd := &ht.hdrs[slotOf(t, ht, kB)]
	if hd.lastWrite != 9 || hd.prevWrite != 5 {
		t.Fatalf("ghost stamps not restored: lastWrite %d prevWrite %d", hd.lastWrite, hd.prevWrite)
	}
	if _, ok := ht.ghosts.take(maphash.Bytes(ht.seed, kB)); ok {
		t.Fatal("ghost not consumed by the reinsert")
	}
}

// TestTransitionCycleZeroAlloc extends the alloczero discipline to the
// transitions the lab cannot reach from outside the package: a full
// dirty to resident to ghost to reinsert cycle and a tombstone drain
// cycle on a warm table recycle header slots, arena classes, index
// cells, and ghost slots, so neither touches the allocator.
func TestTransitionCycleZeroAlloc(t *testing.T) {
	ht := NewHotTable(64)
	val := bytes.Repeat([]byte("v"), 16)
	key := []byte("cycle-key")
	if !ht.Put(key, val, TagString) {
		t.Fatal("warm put refused")
	}
	h := maphash.Bytes(ht.seed, key)
	mustSlot := func() uint32 {
		s, ok := ht.lookup(h, key)
		if !ok {
			panic("key lost mid-cycle")
		}
		return s
	}
	evictCycle := func() {
		s := mustSlot()
		if !ht.drained(s, 1) || !ht.evict(s, true) {
			panic("transition refused")
		}
		if !ht.Put(key, val, TagString) {
			panic("reinsert refused")
		}
	}
	tombCycle := func() {
		if !ht.Del(key) {
			panic("del missed")
		}
		if !ht.drained(mustSlot(), 0) {
			panic("tombstone drain refused")
		}
		if !ht.Put(key, val, TagString) {
			panic("reinsert refused")
		}
	}
	evictCycle() // warm the free-slot stack and ghost slot
	tombCycle()
	if allocs := testing.AllocsPerRun(1000, evictCycle); allocs != 0 {
		t.Errorf("drain-evict-reinsert cycle: %.1f allocs/op, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, tombCycle); allocs != 0 {
		t.Errorf("tombstone drain cycle: %.1f allocs/op, want 0", allocs)
	}
}

// TestStateMachineProperty is the doc 04 section 4 transition table as
// an executable check: random writes, reads, deletes, drains, and
// evictions against a shadow state machine, with the full header scan
// asserting that only the table's legal states and transitions ever
// appear. Illegal moves (evicting dirty, draining non-dirty) must be
// refused, not just avoided.
func TestStateMachineProperty(t *testing.T) {
	const (
		sDirty = iota
		sTomb
		sResident
	)
	type rec struct {
		state int
		val   []byte
	}

	rng := rand.New(rand.NewSource(7))
	ht := NewHotTable(64)
	tick := uint32(1)
	ht.SetTick(tick)
	shadow := make(map[string]*rec)
	keyOf := func() []byte { return fmt.Appendf(nil, "pk-%02d", rng.Intn(48)) }
	pick := func(want int) (string, bool) {
		for k, r := range shadow {
			if r.state == want {
				return k, true
			}
		}
		return "", false
	}

	for op := range 20000 {
		switch n := rng.Intn(100); {
		case n < 10:
			tick++
			ht.SetTick(tick)
		case n < 30:
			key := keyOf()
			r := shadow[string(key)]
			want := r != nil && r.state != sTomb
			if ht.Del(key) != want {
				t.Fatalf("op %d: del %q disagreed with shadow", op, key)
			}
			if want {
				r.state, r.val = sTomb, nil
			}
		case n < 45:
			key := keyOf()
			v, ok := ht.Get(key)
			r := shadow[string(key)]
			visible := r != nil && r.state != sTomb
			if ok != visible || (visible && !bytes.Equal(v, r.val)) {
				t.Fatalf("op %d: get %q = %v, shadow visible %v", op, key, ok, visible)
			}
		case n < 75:
			key := keyOf()
			val := bytes.Repeat([]byte{byte(op)}, rng.Intn(200))
			if !ht.Put(key, val, TagString) {
				t.Fatalf("op %d: put %q refused", op, key)
			}
			shadow[string(key)] = &rec{state: sDirty, val: val}
		case n < 88:
			if k, ok := pick(sResident); ok {
				if ht.drained(slotOf(t, ht, []byte(k)), uint64(op)+1) {
					t.Fatalf("op %d: drain of resident %q accepted", op, k)
				}
			}
			k, ok := pick(sDirty)
			if !ok {
				if k, ok = pick(sTomb); !ok {
					continue
				}
			}
			if !ht.drained(slotOf(t, ht, []byte(k)), uint64(op)+1) {
				t.Fatalf("op %d: drain of dirty %q refused", op, k)
			}
			if shadow[k].state == sTomb {
				delete(shadow, k)
			} else {
				shadow[k].state = sResident
			}
		default:
			if k, ok := pick(sDirty); ok {
				if ht.evict(slotOf(t, ht, []byte(k)), true) {
					t.Fatalf("op %d: dirty %q evicted", op, k)
				}
			}
			k, ok := pick(sResident)
			if !ok {
				continue
			}
			if !ht.evict(slotOf(t, ht, []byte(k)), rng.Intn(2) == 0) {
				t.Fatalf("op %d: evict of resident %q refused", op, k)
			}
			delete(shadow, k)
		}

		if op%500 != 0 {
			continue
		}
		live := 0
		for k, r := range shadow {
			if r.state != sTomb {
				live++
			}
			hd := &ht.hdrs[slotOf(t, ht, []byte(k))]
			switch {
			case r.state == sResident && hd.state != stateResident:
				t.Fatalf("op %d: %q header state %d, shadow resident", op, k, hd.state)
			case r.state != sResident && hd.state != stateDirty:
				t.Fatalf("op %d: %q header state %d, shadow dirty", op, k, hd.state)
			case (r.state == sTomb) != (hd.valRef == 0):
				t.Fatalf("op %d: %q tombstone mismatch: shadow %d valRef %d", op, k, r.state, hd.valRef)
			}
		}
		if ht.Len() != live {
			t.Fatalf("op %d: len %d, shadow live %d", op, ht.Len(), live)
		}
		occupied := 0
		for i := range ht.hdrs {
			switch ht.hdrs[i].state {
			case 0:
			case stateDirty, stateResident:
				occupied++
			default:
				t.Fatalf("op %d: slot %d in impossible state %d", op, i, ht.hdrs[i].state)
			}
		}
		if occupied != len(shadow) {
			t.Fatalf("op %d: %d occupied headers, shadow holds %d", op, occupied, len(shadow))
		}
		sumDirty := 0
		for i := range ht.hdrs {
			hd := &ht.hdrs[i]
			if hd.state != stateDirty {
				continue
			}
			sumDirty += int(hd.klen)
			if hd.valRef != 0 {
				sumDirty += len(ht.vals.data(hd.valRef))
			}
		}
		if sumDirty != ht.dirtyBytes {
			t.Fatalf("op %d: dirtyBytes %d, headers sum to %d", op, ht.dirtyBytes, sumDirty)
		}
	}

	// Drain everything out, evict everything cold: the table must empty.
	for k, r := range shadow {
		if r.state != sResident {
			if !ht.drained(slotOf(t, ht, []byte(k)), 1) {
				t.Fatalf("final drain of %q refused", k)
			}
		}
		if r.state == sTomb {
			continue
		}
		if !ht.evict(slotOf(t, ht, []byte(k)), false) {
			t.Fatalf("final evict of %q refused", k)
		}
	}
	if ht.Len() != 0 || len(ht.freeSlots) != len(ht.hdrs) {
		t.Fatalf("table not empty at the end: len %d, %d free of %d", ht.Len(), len(ht.freeSlots), len(ht.hdrs))
	}
}
