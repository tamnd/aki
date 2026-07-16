package sqlo1b

// The R-I6 bound (doc 04 section 13): compaction holds at most 2
// extents of buffers per shard. CompactExtent reads the sealed extent
// group by group and relocates survivors through the normal put path,
// so its transient hold is one group image at a time beside the open
// group; the index probes allocate a stream of 4 KiB images that die
// immediately, so cumulative allocation says nothing about the hold.
// What a unit test can pin is retention: live heap across the whole
// compaction, measured GC to GC, must not grow by anything close to
// an extent. If this trips, someone started keeping extent images.

import (
	"context"
	"runtime"
	"testing"
)

func TestCompactBufferBound(t *testing.T) {
	r := newStoreRig(t)
	ext, keys := r.fillSealed(t, "cb")
	// Kill half the extent by overwrite so the compaction does real
	// relocation work, not a pure skip pass.
	for i, k := range keys {
		if i%2 == 0 {
			r.apply(t, putOp(k, []byte("moved"), 0))
		}
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	if _, err := r.s.CompactExtent(context.Background(), ext); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)

	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	bound := int64(r.s.sb.ExtentSize) + 256<<10
	if retained > bound {
		t.Fatalf("compaction retained %d live bytes, bound %d (one extent + slack)", retained, bound)
	}
	t.Logf("one-extent compaction retained %d live bytes (bound %d), %d cumulative",
		retained, bound, after.TotalAlloc-before.TotalAlloc)
}
