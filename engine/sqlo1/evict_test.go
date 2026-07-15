package sqlo1

import (
	"bytes"
	"context"
	"fmt"
	"hash/maphash"
	"testing"
)

// drainAll flushes every dirty record so tests can build resident sets.
func drainAll(t *testing.T, d *drainer) {
	t.Helper()
	for {
		n, err := d.drain(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
}

func TestStampWorthAndWriteWeight(t *testing.T) {
	// One stamp: rate 1 over the age; two stamps: rate 2 over the span.
	if w := stampWorth(0, 0, 10); w != 0 {
		t.Fatalf("unset stamps worth %f", w)
	}
	if w := stampWorth(8, 0, 10); w != 1.0/3 {
		t.Fatalf("single stamp worth %f, want 1/3", w)
	}
	if w := stampWorth(9, 7, 10); w != 0.5 {
		t.Fatalf("double stamp worth %f, want 1/2", w)
	}

	ht := NewHotTable(8)
	ht.SetTick(9)
	e := newEvictor(ht, 1)
	reader := hdr{lastRead: 9, prevRead: 7}
	writer := hdr{lastWrite: 9, prevWrite: 7}
	if rs, ws := e.score(&reader), e.score(&writer); ws != 2*rs {
		t.Fatalf("write score %f, read score %f: writes must weigh 2x", ws, rs)
	}
}

func TestEvictNeverTouchesDirty(t *testing.T) {
	ht := NewHotTable(256)
	ht.SetTick(1)
	d := newDrainer(ht, NewMemStore())
	e := newEvictor(ht, 2)

	for i := range 50 {
		ht.Put(fmt.Appendf(nil, "res-%03d", i), []byte("value"), TagString)
	}
	drainAll(t, d)
	for i := range 50 {
		ht.Put(fmt.Appendf(nil, "dirty-%03d", i), []byte("value"), TagString)
	}

	freed := e.evict(1 << 30) // far past what residents can supply
	if freed <= 0 {
		t.Fatal("evict freed nothing with 50 residents available")
	}
	for i := range ht.hdrs {
		switch ht.hdrs[i].state {
		case 0:
		case stateDirty:
		case stateResident:
			t.Fatalf("slot %d still resident after an unbounded evict", i)
		default:
			t.Fatalf("slot %d in impossible state %d", i, ht.hdrs[i].state)
		}
	}
	if ht.Len() != 50 {
		t.Fatalf("len %d after evicting all residents, want the 50 dirty", ht.Len())
	}
	if ht.dirtyN != 50 {
		t.Fatalf("dirty queue disturbed by eviction: %d entries, want 50", ht.dirtyN)
	}
}

func TestEvictFreedAccountingAndGhost(t *testing.T) {
	ht := NewHotTable(64)
	ht.SetTick(1)
	d := newDrainer(ht, NewMemStore())
	e := newEvictor(ht, 3)

	key := []byte("only-key")
	val := bytes.Repeat([]byte("v"), 100)
	ht.Put(key, val, TagString)
	drainAll(t, d)

	freed := e.evict(1)
	if want := hdrSize + len(key) + len(val); freed != want {
		t.Fatalf("freed %d, want %d", freed, want)
	}
	if _, ok := ht.ghosts.take(maphash.Bytes(ht.seed, key)); !ok {
		t.Fatal("victim left no ghost")
	}
	if e.evict(1) != 0 {
		t.Fatal("evict on an empty resident set freed bytes")
	}
}

func TestEvictPrefersColdKeys(t *testing.T) {
	ht := NewHotTable(512)
	ht.SetTick(1)
	d := newDrainer(ht, NewMemStore())
	e := newEvictor(ht, 4)

	// 200 keys drain at tick 1; the hot half is then read at ticks 40
	// and 50 so it carries two recent read stamps, while the cold half
	// keeps only its ancient insert-write stamp.
	for i := range 200 {
		ht.Put(fmt.Appendf(nil, "key-%03d", i), []byte("value"), TagString)
	}
	drainAll(t, d)
	for _, tick := range []uint32{40, 50} {
		ht.SetTick(tick)
		for i := range 100 {
			if _, ok := ht.Get(fmt.Appendf(nil, "key-%03d", i)); !ok {
				t.Fatalf("hot key %d missing", i)
			}
		}
	}

	// Evict half the tier by count (roughly: each record frees the same
	// bytes) and count survivors per half.
	perKey := hdrSize + len("key-000") + len("value")
	e.evict(100 * perKey)
	hot, cold := 0, 0
	for i := range 200 {
		if _, ok := ht.Get(fmt.Appendf(nil, "key-%03d", i)); !ok {
			continue
		}
		if i < 100 {
			hot++
		} else {
			cold++
		}
	}
	if hot <= 2*cold {
		t.Fatalf("eviction not recency-biased: %d hot vs %d cold survivors", hot, cold)
	}
}

func TestEvictCycleZeroAlloc(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ht.SetTick(1)
	d := newDrainer(ht, nullStore{})
	e := newEvictor(ht, 5)
	keys := [][]byte{[]byte("zk-0"), []byte("zk-1")}
	val := bytes.Repeat([]byte("v"), 16)
	for _, k := range keys {
		if !ht.Put(k, val, TagString) {
			t.Fatal("seed put refused")
		}
	}
	if _, err := d.drain(ctx); err != nil {
		t.Fatal(err)
	}

	cycle := func() {
		if e.evict(1) == 0 {
			panic("evict freed nothing")
		}
		for _, k := range keys {
			if _, ok := ht.Get(k); ok {
				continue
			}
			if !ht.Put(k, val, TagString) {
				panic("reinsert refused")
			}
		}
		if n, err := d.drain(ctx); err != nil || n == 0 {
			panic("drain-back failed")
		}
	}
	cycle()
	if allocs := testing.AllocsPerRun(1000, cycle); allocs != 0 {
		t.Errorf("evict cycle: %.1f allocs/op, want 0", allocs)
	}
}
