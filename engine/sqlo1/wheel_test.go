package sqlo1

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
)

// wheelHolds reports whether the wheel holds a live entry for slot s
// filed under at, searching every level, the overflow, and the unreaped
// tail of the due queue.
func wheelHolds(w *wheel, s, at uint32) bool {
	match := func(es []wheelEntry) bool {
		for _, e := range es {
			if e.slot == s && e.expireLo == at {
				return true
			}
		}
		return false
	}
	for l := range w.levels {
		for b := range w.levels[l] {
			if match(w.levels[l][b]) {
				return true
			}
		}
	}
	return match(w.overflow) || match(w.due[w.dueHead:])
}

func advanceTo(ht *HotTable, w *wheel, tick uint32) {
	ht.SetTick(tick)
	w.advance()
}

// tickMs is the millisecond at the start of tick at: the wheel tests
// reason in ticks, and the start-of-tick stamp keeps the expireLo
// projection equal to at so entry assertions stay in tick units.
func tickMs(at uint32) int64 {
	return int64(at) << 10
}

func TestWheelLevelsAndCascade(t *testing.T) {
	ht := NewHotTable(64)
	ht.SetTick(1)
	w := newWheel(ht)

	// One key per level, walked in expiry order: level 0, level 1
	// (needs one cascade), level 2 (needs two).
	keys := []struct {
		k  string
		at uint32
	}{{"l0", 6}, {"l1", 300}, {"l2", 70000}}
	for _, kv := range keys {
		if !ht.Put([]byte(kv.k), []byte("v"), TagString) || !w.expire([]byte(kv.k), tickMs(kv.at)) {
			t.Fatalf("seeding %s failed", kv.k)
		}
	}

	for _, kv := range keys {
		k, at := kv.k, kv.at
		advanceTo(ht, w, at-1)
		w.reap(1 << 30)
		if _, ok := ht.Get([]byte(k)); !ok {
			t.Fatalf("%s gone before its tick", k)
		}
		advanceTo(ht, w, at)
		if _, ok := ht.Get([]byte(k)); ok {
			t.Fatalf("%s visible at its expiry tick before reap (lazy layer broken)", k)
		}
		before := ht.Len()
		if n := w.reap(1 << 30); n != 1 {
			t.Fatalf("reap at %s's tick killed %d keys, want 1", k, n)
		}
		if ht.Len() != before-1 {
			t.Fatalf("%s not reaped at its tick", k)
		}
	}
}

func TestWheelLazyInvalidation(t *testing.T) {
	ht := NewHotTable(64)
	ht.SetTick(1)
	w := newWheel(ht)
	key := []byte("moved")

	ht.Put(key, []byte("v"), TagString)
	w.expire(key, tickMs(10))
	w.expire(key, tickMs(20)) // GT-style rewrite; the entry for 10 stays filed

	advanceTo(ht, w, 10)
	if n := w.reap(1 << 30); n != 0 {
		t.Fatalf("stale entry reaped %d keys", n)
	}
	if _, ok := ht.Get(key); !ok {
		t.Fatal("key died at its rewritten-away expiry")
	}
	advanceTo(ht, w, 20)
	if n := w.reap(1 << 30); n != 1 {
		t.Fatalf("reap at the rewritten expiry killed %d, want 1", n)
	}

	// Refiling the same expiry must not file a duplicate entry.
	ht.Put(key, []byte("v"), TagString)
	w.expire(key, tickMs(30))
	w.expire(key, tickMs(30))
	s := slotOf(t, ht, key)
	n := 0
	for _, e := range w.levels[0][30&(wheelBuckets-1)] {
		if e.slot == s && e.expireLo == 30 {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("unchanged EXPIRE filed %d entries, want 1", n)
	}
}

func TestWheelPersistAndDelInvalidate(t *testing.T) {
	ht := NewHotTable(64)
	ht.SetTick(1)
	w := newWheel(ht)

	ht.Put([]byte("kept"), []byte("v"), TagString)
	w.expire([]byte("kept"), tickMs(10))
	if !w.expire([]byte("kept"), 0) {
		t.Fatal("persist refused")
	}
	ht.Put([]byte("gone"), []byte("v"), TagString)
	w.expire([]byte("gone"), tickMs(10))
	ht.Del([]byte("gone"))
	// The tombstone must not inherit the TTL on revive.
	ht.Put([]byte("gone"), []byte("v2"), TagString)

	advanceTo(ht, w, 10)
	if n := w.reap(1 << 30); n != 0 {
		t.Fatalf("reap killed %d keys, want 0: both entries are stale", n)
	}
	for _, k := range []string{"kept", "gone"} {
		if _, ok := ht.Get([]byte(k)); !ok {
			t.Fatalf("%s died through a stale wheel entry", k)
		}
	}
}

func TestWheelOverflowParksAndRefiles(t *testing.T) {
	ht := NewHotTable(64)
	ht.SetTick(1)
	w := newWheel(ht)
	key := []byte("far")
	at := uint32(1 + wheelHorizon + 10)

	ht.Put(key, []byte("v"), TagString)
	w.expire(key, tickMs(at))
	if len(w.overflow) != 1 {
		t.Fatalf("beyond-horizon expiry filed %d overflow entries, want 1", len(w.overflow))
	}

	// By the first daily rescan the expiry is within the horizon, so
	// the entry must leave the overflow for a level.
	advanceTo(ht, w, wheelRescan)
	if len(w.overflow) != 0 {
		t.Fatal("overflow entry not refiled at the rescan boundary")
	}
	if !wheelHolds(w, slotOf(t, ht, key), at) {
		t.Fatal("refiled entry lost")
	}
	if _, ok := ht.Get(key); !ok {
		t.Fatal("key disturbed by overflow handling")
	}
}

func TestWheelReapBatchBounded(t *testing.T) {
	ht := NewHotTable(256)
	ht.SetTick(1)
	w := newWheel(ht)
	for i := range 200 {
		k := fmt.Appendf(nil, "due-%03d", i)
		ht.Put(k, []byte("v"), TagString)
		w.expire(k, tickMs(5))
	}

	advanceTo(ht, w, 5)
	got := []int{}
	for {
		n := w.reap(wheelReapBatch)
		if n == 0 {
			break
		}
		if n > wheelReapBatch {
			t.Fatalf("pass reaped %d, over the %d bound", n, wheelReapBatch)
		}
		got = append(got, n)
	}
	if len(got) != 4 || got[0] != 64 || got[3] != 8 {
		t.Fatalf("passes %v, want [64 64 64 8]", got)
	}
	if ht.Len() != 0 {
		t.Fatalf("%d keys survived a full reap", ht.Len())
	}
}

func TestWheelMembershipProperty(t *testing.T) {
	ht := NewHotTable(64)
	ht.SetTick(1)
	w := newWheel(ht)
	d := newDrainer(ht, NewMemStore())
	e := newEvictor(ht, 6)
	rng := rand.New(rand.NewPCG(7, 11))

	var maxAt uint32
	for i := range 20000 {
		key := fmt.Appendf(nil, "wk-%02d", rng.IntN(48))
		switch rng.IntN(12) {
		case 0:
			ht.Del(key)
		case 1:
			w.expire(key, 0)
		case 2:
			if _, err := d.drain(context.Background()); err != nil {
				t.Fatal(err)
			}
		case 3:
			e.evict(64)
		case 4:
			advanceTo(ht, w, ht.tick+1+uint32(rng.IntN(400)))
			w.reap(wheelReapBatch)
		default:
			ht.Put(key, []byte("value"), TagString)
			if rng.IntN(2) == 0 {
				at := ht.tick + 1 + uint32(rng.IntN(3000))
				if w.expire(key, tickMs(at)) && at > maxAt {
					maxAt = at
				}
			}
		}

		if i%500 == 0 {
			// E-I3's membership half: every live volatile key holds a
			// matching wheel entry somewhere.
			for s := range ht.hdrs {
				hd := &ht.hdrs[s]
				if hd.state == 0 || hd.valRef == 0 || hd.expireLo == 0 {
					continue
				}
				if !wheelHolds(w, uint32(s), hd.expireLo) {
					t.Fatalf("op %d: volatile slot %d (expiry %d) has no wheel entry", i, s, hd.expireLo)
				}
			}
		}
	}

	// The self-draining half: past every filed expiry the wheel must be
	// empty and every volatile key must be gone.
	advanceTo(ht, w, maxAt+1)
	for w.reap(wheelReapBatch) > 0 {
	}
	for s := range ht.hdrs {
		hd := &ht.hdrs[s]
		if hd.state != 0 && hd.valRef != 0 && hd.expireLo != 0 {
			t.Fatalf("volatile slot %d survived a full advance and reap", s)
		}
	}
	for l := range w.levels {
		for b := range w.levels[l] {
			if len(w.levels[l][b]) != 0 {
				t.Fatalf("level %d bucket %d holds %d entries past the horizon of all filings", l, b, len(w.levels[l][b]))
			}
		}
	}
	if len(w.overflow) != 0 || w.dueHead != len(w.due) {
		t.Fatal("overflow or due queue not drained")
	}
}

func TestWheelCycleZeroAlloc(t *testing.T) {
	ht := NewHotTable(64)
	ht.SetTick(1)
	w := newWheel(ht)
	key := []byte("zw-0")
	val := []byte("value")

	cycle := func() {
		if !ht.Put(key, val, TagString) {
			panic("put refused")
		}
		if !w.expire(key, tickMs(ht.tick+1)) {
			panic("expire refused")
		}
		ht.SetTick(ht.tick + 1)
		w.advance()
		if w.reap(wheelReapBatch) != 1 {
			panic("reap missed")
		}
	}
	// Each cycle files into the next level-0 bucket, so one lap of the
	// wheel warms every bucket's capacity before the measured runs.
	for range 2 * wheelBuckets {
		cycle()
	}
	if allocs := testing.AllocsPerRun(1000, cycle); allocs != 0 {
		t.Errorf("wheel cycle: %.1f allocs/op, want 0", allocs)
	}
}
