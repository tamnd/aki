package sqlo1

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func newTestLadder(capacity, threshold int) (*HotTable, *ladder) {
	ht := NewHotTable(capacity)
	ht.SetTick(1)
	d := newDrainer(ht, NewMemStore())
	d.threshold = threshold
	return ht, newLadder(ht, d, newEvictor(ht, 21))
}

func TestDirtyPressureSignal(t *testing.T) {
	ht, l := newTestLadder(64, 100)
	if p := l.dirtyPressure(); p != 0 {
		t.Fatalf("empty table pressure %f", p)
	}
	ht.Put([]byte("k"), bytes.Repeat([]byte("v"), 149), TagString) // 1 + 149 dirty bytes
	if p := l.dirtyPressure(); p != 1.5 {
		t.Fatalf("pressure %f, want 1.5", p)
	}
	if l.walPressure() != 0 || l.extentPressure() != 0 {
		t.Fatal("stub rungs must read zero until their milestones land")
	}
}

func TestStepQuantaPerRung(t *testing.T) {
	ctx := context.Background()

	// Under the drain line: step spends nothing even with dirty bytes.
	ht, l := newTestLadder(64, 1<<20)
	ht.Put([]byte("calm"), []byte("v"), TagString)
	if n, err := l.step(ctx); err != nil || n != 0 {
		t.Fatalf("step under the drain line spent %d quanta (err %v)", n, err)
	}

	// Between the lines: exactly one voluntary quantum.
	ht, l = newTestLadder(64, 100)
	ht.Put([]byte("warm"), bytes.Repeat([]byte("v"), 146), TagString)
	if n, err := l.step(ctx); err != nil || n != 1 {
		t.Fatalf("step between the lines spent %d quanta (err %v)", n, err)
	}

	// Past the force line: mandatory quanta until back under it. One
	// key per quantum (maxOps 1) makes the count observable.
	ht, l = newTestLadder(256, 100)
	l.d.maxOps = 1
	for i := range 6 {
		ht.Put(fmt.Appendf(nil, "hot-%d", i), bytes.Repeat([]byte("v"), 95), TagString)
	}
	// 6 x 100 dirty bytes = pressure 6; each quantum drains one key.
	n, err := l.step(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := l.dirtyPressure(); got >= dirtyPressureForce {
		t.Fatalf("step left pressure at %f, force line is %f", got, dirtyPressureForce)
	}
	if n != 5 {
		t.Fatalf("step spent %d quanta, want 5 (six keys, stop under 2x)", n)
	}
}

func TestMakeRoomForcesDrainWhenAllDirty(t *testing.T) {
	ctx := context.Background()
	// One value chunk of budget: fills with dirty records, then a put
	// is refused with nothing clean to evict.
	b := Budget{Arenas: 2 * arenaChunkSize, Entries: 128}
	ht := NewBudgetedHotTable(b)
	ht.SetTick(1)
	d := newDrainer(ht, NewMemStore())
	l := newLadder(ht, d, newEvictor(ht, 22))

	val := bytes.Repeat([]byte("v"), 4<<10)
	filled := 0
	for i := range 128 {
		if !ht.Put(fmt.Appendf(nil, "fill-%03d", i), val, TagString) {
			break
		}
		filled++
	}
	if filled == 0 || filled == 128 {
		t.Fatalf("fill count %d: budget never refused", filled)
	}
	key := []byte("blocked")
	if ht.Put(key, val, TagString) {
		t.Fatal("put fit after the budget refused the fill")
	}

	need := hdrSize + len(key) + len(val)
	freed, err := l.makeRoom(ctx, need)
	if err != nil {
		t.Fatal(err)
	}
	if freed < need {
		t.Fatalf("makeRoom freed %d of %d with a full dirty tier to cool", freed, need)
	}
	if !ht.Put(key, val, TagString) {
		t.Fatal("put still refused after makeRoom")
	}

	// Honest failure: an empty tier cannot supply and must say so
	// without spinning.
	ht2, l2 := newTestLadder(8, 100)
	if freed, err := l2.makeRoom(ctx, 1<<30); err != nil || freed != 0 {
		t.Fatalf("empty tier makeRoom returned %d, %v", freed, err)
	}
	_ = ht2
}

func TestLadderStepZeroAlloc(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ht.SetTick(1)
	d := newDrainer(ht, nullStore{})
	d.threshold = 64
	l := newLadder(ht, d, newEvictor(ht, 23))
	keys := [][]byte{[]byte("bp-0"), []byte("bp-1")}
	val := bytes.Repeat([]byte("v"), 64)

	cycle := func() {
		for _, k := range keys {
			if !ht.Put(k, val, TagString) {
				panic("put refused")
			}
		}
		n, err := l.step(ctx)
		if err != nil || n == 0 {
			panic("step spent nothing over the force line")
		}
	}
	cycle()
	if allocs := testing.AllocsPerRun(1000, cycle); allocs != 0 {
		t.Errorf("ladder step cycle: %.1f allocs/op, want 0", allocs)
	}
}
