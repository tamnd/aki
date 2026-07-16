package sqlo1

// Doc 04 section 13 runtime invariants proven from the outside, the
// half that lives at the composite: R-I1 (only the owner mutates
// shard state) and R-I2 (ack paths allocate nothing and block on
// nothing). R-I3 and R-I5 are pinned in evict_tiered_test.go, R-I7
// in budget_test.go and backpressure_maint_test.go, R-I4's batching
// half in tiered_test.go and its IO half in the sqlo1b boundary
// scan, R-I6 in the sqlo1b compaction bound test.

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

// TestTieredOwnerOnlyNoGoroutines drives a Tiered through fills,
// overwrites, reads, drains, eviction pressure, and ticks, and pins
// that the runtime composite never grows concurrency of its own: the
// goroutine count after the churn equals the count before it. The
// sanctioned auxiliaries of R-I1 are the store-side IO workers behind
// the mailbox; everything the composite itself does, drain and
// eviction and the maintenance rungs included, runs on the caller.
func TestTieredOwnerOnlyNoGoroutines(t *testing.T) {
	ctx := context.Background()
	r := newTieredRig(t, 64, -1, 41)

	before := runtime.NumGoroutine()
	for i := range 2000 {
		key := fmt.Sprintf("k%04d", i%256)
		if err := r.t.Set(ctx, []byte(key), []byte("value"), TagString); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		if i%3 == 0 {
			if _, _, err := r.t.Get(ctx, []byte(key)); err != nil {
				t.Fatalf("Get %d: %v", i, err)
			}
		}
		if i%500 == 499 {
			if err := r.t.Tick(ctx); err != nil {
				t.Fatalf("Tick %d: %v", i, err)
			}
		}
	}
	if err := r.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if after := runtime.NumGoroutine(); after != before {
		t.Fatalf("goroutines grew %d -> %d across the churn", before, after)
	}
	if st := r.t.Stats(); st.Evictions == 0 || r.cs.applyBatches == 0 {
		t.Fatalf("churn was not representative: %d evictions, %d drains", st.Evictions, r.cs.applyBatches)
	}
}

// TestTieredAckPathQuiet pins R-I2 where the composite can prove it:
// a steady-state overwrite Set into a tier with room touches the
// store zero times (the syscall half; drains belong to the window)
// and allocates zero bytes (the allocation half, skipped under the
// race detector because instrumentation allocates). The full wire ack
// path is the alloczero lab's business once the server carries it.
func TestTieredAckPathQuiet(t *testing.T) {
	ctx := context.Background()
	r := newTieredRig(t, 4096, -1, 43)

	keys := make([][]byte, 64)
	val := []byte("sixteen-byte-val")
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "ack-%03d", i)
		if err := r.t.Set(ctx, keys[i], val, TagString); err != nil {
			t.Fatalf("prefill %d: %v", i, err)
		}
	}
	if err := r.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	gets, applies := r.cs.batchGets, r.cs.applyBatches
	i := 0
	overwrite := func() {
		if err := r.t.Set(ctx, keys[i&63], val, TagString); err != nil {
			t.Fatalf("overwrite: %v", err)
		}
		i++
	}
	if raceEnabled {
		for range 6400 {
			overwrite()
		}
	} else {
		if allocs := testing.AllocsPerRun(6400, overwrite); allocs != 0 {
			t.Fatalf("steady-state overwrite Set allocates %.1f times per op", allocs)
		}
	}
	if r.cs.batchGets != gets || r.cs.applyBatches != applies {
		t.Fatalf("ack path reached the store: %d reads, %d batches",
			r.cs.batchGets-gets, r.cs.applyBatches-applies)
	}
}
