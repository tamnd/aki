package sqlo1

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestRootRedirtyDefersBehindLaterWrites pins the W1 batch-order rule:
// a root re-dirtied while queued gives up its early queue position, so
// its summary image lands with or after every segment a later command
// wrote under it, never before. The control key shows plain records
// keep first-dirtied order under the same churn.
func TestRootRedirtyDefersBehindLaterWrites(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	rs := newRecordingStore()
	d := newDrainer(ht, rs)

	ht.Put([]byte("root"), []byte("r1"), TagHash|TagRoot)
	ht.PutGen([]byte("segA"), []byte("a"), TagHash, 1)
	// The second command rewrites the root and then its segment: the
	// root's old queue position sits ahead of segB, which is exactly
	// the ordering deferral must undo.
	ht.Put([]byte("root"), []byte("r2"), TagHash|TagRoot)
	ht.PutGen([]byte("segB"), []byte("b"), TagHash, 1)
	if n, err := d.drain(ctx); err != nil || n != 3 {
		t.Fatalf("drain = %d, %v, want 3 records", n, err)
	}
	got := make([]string, 0, 3)
	for _, op := range rs.batches[0].Ops {
		got = append(got, string(op.Rec.Key))
	}
	want := []string{"segA", "segB", "root"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("batch order = %v, want %v", got, want)
		}
	}

	ht.Put([]byte("k1"), []byte("v1"), TagString)
	ht.Put([]byte("k2"), []byte("v2"), TagString)
	ht.Put([]byte("k1"), []byte("v1b"), TagString)
	if n, err := d.drain(ctx); err != nil || n != 2 {
		t.Fatalf("control drain = %d, %v, want 2 records", n, err)
	}
	if k := rs.batches[1].Ops[0].Rec.Key; string(k) != "k1" {
		t.Fatalf("non-root re-dirty moved to %q-first order, want k1 keeping its position", k)
	}
}

// TestDeltaWindowCoalescing pins the W2 flag's lifetime across the hot
// tier: Delta survives a dirty window only when every write in it was
// a delta root write, the drain consumes the window, and an expiry
// re-dirty on a resident header never inherits the flag.
func TestDeltaWindowCoalescing(t *testing.T) {
	ctx := context.Background()
	ht := NewHotTable(64)
	rs := newRecordingStore()
	d := newDrainer(ht, rs)
	key := []byte("root")

	tagOf := func(when string) uint8 {
		t.Helper()
		_, tag, hit, _ := ht.probeReadTag(key)
		if !hit {
			t.Fatalf("%s: key not live", when)
		}
		return tag
	}
	lastRootDelta := func(when string) bool {
		t.Helper()
		b := rs.batches[len(rs.batches)-1]
		for _, op := range b.Ops {
			if !op.Del && op.Rec.Root && string(op.Rec.Key) == string(key) {
				return op.Rec.Delta
			}
		}
		t.Fatalf("%s: last batch carries no root op", when)
		return false
	}

	// A fresh window keeps the caller's claim, and repeating it holds.
	ht.Put(key, []byte("r1"), TagHash|TagRoot|TagDelta)
	ht.Put(key, []byte("r2"), TagHash|TagRoot|TagDelta)
	if tagOf("delta+delta")&TagDelta == 0 {
		t.Fatal("all-delta window lost the flag")
	}
	// One structural write poisons the window for good.
	ht.Put(key, []byte("r3"), TagHash|TagRoot)
	ht.Put(key, []byte("r4"), TagHash|TagRoot|TagDelta)
	if tagOf("structural then delta")&TagDelta != 0 {
		t.Fatal("delta write joining a structural window kept the flag")
	}
	if _, err := d.drain(ctx); err != nil {
		t.Fatal(err)
	}
	if lastRootDelta("poisoned window") {
		t.Fatal("poisoned window drained with Delta set")
	}

	// The drain consumed the window: the next delta write starts clean.
	ht.Put(key, []byte("r5"), TagHash|TagRoot|TagDelta)
	if _, err := d.drain(ctx); err != nil {
		t.Fatal(err)
	}
	if !lastRootDelta("fresh delta window") {
		t.Fatal("fresh all-delta window drained without Delta")
	}

	// An expiry edit re-dirties the resident header without a Put, and
	// segment replay cannot reconstruct an expiry, so no Delta.
	if _, changed, ok := ht.setExpireMs(key, 1<<41); !changed || !ok {
		t.Fatal("setExpireMs refused a live resident key")
	}
	if _, err := d.drain(ctx); err != nil {
		t.Fatal(err)
	}
	if lastRootDelta("expiry re-dirty") {
		t.Fatal("expiry-only re-dirty drained with Delta set")
	}

	// A delta write reviving a tombstone replaces a deletion, which no
	// segment frame can reconstruct either.
	ht.Del(key)
	ht.Put(key, []byte("r6"), TagHash|TagRoot|TagDelta)
	if tagOf("tombstone revival")&TagDelta != 0 {
		t.Fatal("delta write over a dirty tombstone kept the flag")
	}
}

// TestBackendsRejectDeltaWithoutRoot: the flag is a claim about a root
// image, so a bare Delta is a type-layer bug every backend refuses
// before applying anything.
func TestBackendsRejectDeltaWithoutRoot(t *testing.T) {
	ctx := context.Background()
	ms := NewMemStore()
	bad := DrainBatch{Seq: 1, Ops: []Op{{Rec: Record{Key: []byte("k"), Value: []byte("v"), Delta: true}}}}
	if err := ms.ApplyBatch(ctx, &bad); err == nil {
		t.Fatal("MemStore applied a delta flag on a non-root record")
	}
	if got := ms.Stats(); got.Keys != 0 || got.HighWater != 0 {
		t.Fatalf("rejected batch left state behind: %+v", got)
	}
	good := DrainBatch{Seq: 1, Ops: []Op{{Rec: Record{Key: []byte("k"), Value: []byte("v"), Root: true, Delta: true}}}}
	if err := ms.ApplyBatch(ctx, &good); err != nil {
		t.Fatalf("MemStore refused a delta root: %v", err)
	}
}

// TestHashSegRootDeltaDiscipline drives a segmented hash one command
// per drain and checks every root image's Delta against what actually
// happened: created and removed fields claim delta, fence-shape edits
// (upgrade, split, merge) do not, and a pure update writes no root at
// all (W1). Fence growth and shrinkage are read back through the
// decode oracle, so the claim is checked against the structure, not
// against the code paths' own opinion of themselves.
func TestHashSegRootDeltaDiscipline(t *testing.T) {
	r := newHashRig(t)
	ctx := context.Background()
	key := []byte("disc")

	rootOps := func(from int) (n int, delta bool) {
		t.Helper()
		for _, b := range r.rs.batches[from:] {
			for _, op := range b.Ops {
				if !op.Del && op.Rec.Root && string(op.Rec.Key) == string(key) {
					n++
					delta = op.Rec.Delta
				}
			}
		}
		return n, delta
	}
	flush := func() {
		t.Helper()
		if err := r.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	segmented := func() bool {
		t.Helper()
		v, root, ok, err := r.tr.Lookup(ctx, key)
		if err != nil || !ok || !root {
			t.Fatalf("Lookup: ok=%v root=%v err=%v", ok, root, err)
		}
		return v[0] == hashSubSeg
	}

	// Grow one field per drain, inline through upgrade through splits.
	// Inline roots are the data, so they frame full (never delta); the
	// upgrade and every split move the fence and frame full; every
	// other created field is a count-only delta root.
	val := func(i int) string { return fmt.Sprintf("v-%s", strings.Repeat("x", 40+i%25)) }
	fences, splits, upgraded := 0, 0, false
	for i := range 420 {
		mark := len(r.rs.batches)
		if !r.hset("disc", fmt.Sprintf("field-%04d", i), val(i)) {
			t.Fatalf("field %d not created", i)
		}
		flush()
		n, delta := rootOps(mark)
		if n != 1 {
			t.Fatalf("step %d wrote %d root images, want exactly 1", i, n)
		}
		if !segmented() {
			if delta {
				t.Fatalf("step %d: inline root claimed delta", i)
			}
			continue
		}
		now := len(r.segRootOf("disc").fence)
		if !upgraded {
			upgraded, fences = true, now
			if delta {
				t.Fatalf("step %d: upgrade root claimed delta", i)
			}
			continue
		}
		if grew := now > fences; grew == delta {
			t.Fatalf("step %d: fence %d -> %d segments, root delta=%v", i, fences, now, delta)
		}
		if now > fences {
			splits++
		}
		fences = now
	}
	if !upgraded || splits == 0 {
		t.Fatalf("420 fields gave upgraded=%v splits=%d, structural arms unexercised", upgraded, splits)
	}

	// A pure update rewrites its segment and nothing else: no root
	// image in the batch at all.
	mark := len(r.rs.batches)
	if r.hset("disc", "field-0200", "rewritten") {
		t.Fatal("update reported created")
	}
	flush()
	if n, _ := rootOps(mark); n != 0 {
		t.Fatalf("pure update wrote %d root images, want none (W1)", n)
	}

	// Shrink one field per drain: plain removals are delta, the step
	// that merges two segments is not.
	merges := 0
	for i := hashInlineMaxCount + 1; i < 420; i++ {
		mark = len(r.rs.batches)
		removed, err := r.h.HDel(ctx, key, fmt.Appendf(nil, "field-%04d", i))
		if err != nil || !removed {
			t.Fatalf("HDEL %d = %v, %v", i, removed, err)
		}
		flush()
		now := len(r.segRootOf("disc").fence)
		n, delta := rootOps(mark)
		if n != 1 {
			t.Fatalf("del step %d wrote %d root images, want exactly 1", i, n)
		}
		if shrank := now < fences; shrank == delta {
			t.Fatalf("del step %d: fence %d -> %d segments, root delta=%v", i, fences, now, delta)
		}
		if now < fences {
			merges++
		}
		fences = now
	}
	if merges == 0 {
		t.Fatal("the shrink walk never merged, the structural arm went unexercised")
	}
}
