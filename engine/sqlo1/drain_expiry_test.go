package sqlo1

// The die-in-RAM half of doc 11 section 6: an expired plain put leaves
// the drain as a tombstone (reap-cancel), and a volatile record near
// its deadline sits out up to two queue laps hoping to die first
// (drain reordering). The dieinram lab shaped both: cancel is the
// first-order cliff, deferral only pays inside two laps.

import (
	"context"
	"errors"
	"testing"
)

// A wall-clock base far enough up that the coarse tick is nonzero.
const expT0 = int64(1) << 30

func TestDrainReapCancel(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ms := NewMemStore()
	d := newDrainer(ht, ms)
	ht.SetNow(expT0)

	ht.Put([]byte("dead"), []byte("v"), TagString)
	ht.setExpireMs([]byte("dead"), expT0+500)
	ht.Put([]byte("live"), []byte("v"), TagString)
	// An expired root must still drain as a value: only ReapStep and
	// the command paths mint the genbump a root tombstone needs.
	ht.Put([]byte("root"), []byte("img"), TagRoot|TagHash)
	ht.setExpireMs([]byte("root"), expT0+500)
	ht.SetNow(expT0 + 1000)

	n, err := d.drain(ctx)
	if err != nil || n != 3 {
		t.Fatalf("drain = %d %v, want 3", n, err)
	}
	if d.cancels != 1 {
		t.Fatalf("cancels = %d, want 1", d.cancels)
	}
	if _, err := ms.Get(ctx, []byte("dead")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired put reached the store as a value: %v", err)
	}
	if rec, err := ms.Get(ctx, []byte("root")); err != nil || string(rec.Value) != "img" || !rec.Root {
		t.Fatalf("expired root = %+v %v, want the value image", rec, err)
	}
	if rec, err := ms.Get(ctx, []byte("live")); err != nil || string(rec.Value) != "v" {
		t.Fatalf("live key = %+v %v", rec, err)
	}
	// The cancelled header cools like any drained slot and its value
	// stays invisible: the hot tier answers expiry definitively.
	hd := &ht.hdrs[slotOf(t, ht, []byte("dead"))]
	if hd.state != stateResident {
		t.Fatalf("cancelled header state %d, want resident", hd.state)
	}
	if _, _, hit, definitive := func() ([]byte, uint8, bool, bool) {
		v, tag, _, h, def := ht.probeEntry([]byte("dead"))
		return v, tag, h, def
	}(); hit || !definitive {
		t.Fatalf("cancelled key still readable: hit=%v definitive=%v", hit, definitive)
	}
}

func TestDrainVolatileDefersAndForceCollect(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ms := NewMemStore()
	d := newDrainer(ht, ms)
	ht.SetNow(expT0)

	ht.Put([]byte("vol"), []byte("v"), TagString)
	ht.setExpireMs([]byte("vol"), expT0+1500)
	ht.Put([]byte("plain"), []byte("v"), TagString)
	// A seeded cadence of one lap per second at the current backlog
	// size puts the horizon at two seconds, inside vol's 1.5s of life.
	d.gapEwmaMs, d.bytesEwma = 1000, int64(ht.dirtyBytes)

	n, err := d.drain(ctx)
	if err != nil || n != 1 {
		t.Fatalf("first cycle = %d %v, want the plain key alone", n, err)
	}
	if d.volDefers != 1 {
		t.Fatalf("volDefers = %d, want 1", d.volDefers)
	}
	if _, err := ms.Get(ctx, []byte("vol")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deferred record reached the store: %v", err)
	}
	hd := &ht.hdrs[slotOf(t, ht, []byte("vol"))]
	if hd.state != stateDirty || hd.queued&queuedBit == 0 {
		t.Fatalf("deferred header state %d queued %#x, want dirty and re-filed", hd.state, hd.queued)
	}
	if hd.queued&queuedVolMask != queuedVolStep {
		t.Fatalf("lap count %#x, want one step", hd.queued&queuedVolMask)
	}

	// The queue now holds only the deferrable record: the cycle must
	// still make progress or the ladder's loops would read the table
	// as clean while dirty bytes sit unaccounted.
	d.gapEwmaMs, d.bytesEwma = 1000, int64(ht.dirtyBytes)
	n, err = d.drain(ctx)
	if err != nil || n != 1 {
		t.Fatalf("force-collect cycle = %d %v, want 1", n, err)
	}
	rec, err := ms.Get(ctx, []byte("vol"))
	if err != nil || string(rec.Value) != "v" || rec.ExpireMs != expT0+1500 {
		t.Fatalf("force-collected record = %+v %v, want the value with its stamp", rec, err)
	}
	// A fresh transition into dirty resets the deferral rights: a new
	// write is a new record as far as the lap budget goes.
	ht.Put([]byte("vol"), []byte("v2"), TagString)
	if hd := &ht.hdrs[slotOf(t, ht, []byte("vol"))]; hd.queued&queuedVolMask != 0 {
		t.Fatalf("rewrite kept lap count %#x", hd.queued&queuedVolMask)
	}
}

func TestDrainDeferCapsAtTwoLaps(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ms := NewMemStore()
	d := newDrainer(ht, ms)
	ht.SetNow(expT0)

	ht.Put([]byte("vol"), []byte("v"), TagString)
	ht.setExpireMs([]byte("vol"), expT0+1500)
	for lap, fill := range []string{"p1", "p2"} {
		ht.Put([]byte(fill), []byte("v"), TagString)
		// An effectively infinite horizon isolates the lap cap from
		// the cadence arithmetic.
		d.gapEwmaMs, d.bytesEwma = 1<<20, 1
		if n, err := d.drain(ctx); err != nil || n != 1 {
			t.Fatalf("lap %d = %d %v, want the filler alone", lap+1, n, err)
		}
	}
	if d.volDefers != 2 {
		t.Fatalf("volDefers = %d, want 2", d.volDefers)
	}
	ht.Put([]byte("p3"), []byte("v"), TagString)
	d.gapEwmaMs, d.bytesEwma = 1<<20, 1
	if n, err := d.drain(ctx); err != nil || n != 2 {
		t.Fatalf("capped cycle = %d %v, want vol and the filler", n, err)
	}
	if rec, err := ms.Get(ctx, []byte("vol")); err != nil || string(rec.Value) != "v" {
		t.Fatalf("capped-out record = %+v %v, want the value", rec, err)
	}
}

func TestDrainDeferredRecordCancels(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	ms := NewMemStore()
	d := newDrainer(ht, ms)
	ht.SetNow(expT0)

	ht.Put([]byte("vol"), []byte("v"), TagString)
	ht.setExpireMs([]byte("vol"), expT0+1500)
	ht.Put([]byte("p1"), []byte("v"), TagString)
	d.gapEwmaMs, d.bytesEwma = 1<<20, 1
	if n, err := d.drain(ctx); err != nil || n != 1 || d.volDefers != 1 {
		t.Fatalf("defer cycle = %d %v defers %d", n, err, d.volDefers)
	}

	// The lap worked: the record died in RAM before its next pop, so
	// the deferral converts to a cancel and the store never sees the
	// value bytes at all.
	ht.SetNow(expT0 + 2000)
	if n, err := d.drain(ctx); err != nil || n != 1 {
		t.Fatalf("cancel cycle = %d %v, want 1", n, err)
	}
	if d.cancels != 1 {
		t.Fatalf("cancels = %d, want 1", d.cancels)
	}
	if _, err := ms.Get(ctx, []byte("vol")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deferred-then-dead record reached the store: %v", err)
	}
}

func TestTieredReapCancelStats(t *testing.T) {
	ctx := context.Background()
	now := expT0
	tr := NewTiered(NewMemStore(), TieredConfig{
		Budget:   Budget{Entries: 64, Arenas: 1 << 20},
		PromoteP: -1,
		NowMs:    func() int64 { return now },
	})
	if ok := tr.ht.Put([]byte("k"), []byte("v"), TagString); !ok {
		t.Fatal("put refused")
	}
	tr.ht.setExpireMs([]byte("k"), expT0+500)
	now = expT0 + 1000
	tr.ht.SetNow(now)
	if err := tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	s := tr.Stats()
	if s.ReapCancels != 1 {
		t.Fatalf("stats ReapCancels = %d, want 1", s.ReapCancels)
	}
	if _, err := tr.st.Get(ctx, []byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancelled key in the store: %v", err)
	}
}
