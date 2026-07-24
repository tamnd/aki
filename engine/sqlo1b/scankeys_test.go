package sqlo1b

import (
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// scanAllKeys drives ScanKeys to completion with the given budget,
// failing on duplicates when the store is quiescent: the forward
// bucket cursor pauses between buckets, so a key that is present for
// the whole walk arrives exactly once.
func scanAllKeys(t *testing.T, s *Store, budget int, wantDups bool) map[string]int {
	t.Helper()
	ctx := context.Background()
	seen := map[string]int{}
	cur := uint64(0)
	for step := 0; ; step++ {
		if step > 100_000 {
			t.Fatal("ScanKeys does not terminate")
		}
		next, err := s.ScanKeys(ctx, cur, budget, func(rec sqlo1.Record) {
			seen[string(rec.Key)]++
		})
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		if next == 0 {
			break
		}
		if next <= cur {
			t.Fatalf("cursor went backward: %d then %d", cur, next)
		}
		cur = next
	}
	if !wantDups {
		for k, n := range seen {
			if n != 1 {
				t.Fatalf("key %q delivered %d times on a quiescent store", k, n)
			}
		}
	}
	return seen
}

// TestScanKeysWalk pins the KeyScanner contract on a quiescent store:
// every live key exactly once, expired records and meta records
// filtered by the store clock, and the whole-bucket pause rule, so a
// budget of one still finishes the walk.
func TestScanKeysWalk(t *testing.T) {
	r := newStoreRig(t)
	want := map[string]bool{}
	ops := []sqlo1.Op{}
	for i := range 40 {
		k := fmt.Sprintf("key%02d", i)
		ops = append(ops, putOp(k, []byte("v"), 0))
		want[k] = true
	}
	// Already past its deadline against the rig clock: the walk must
	// hide it the way Get does.
	ops = append(ops, putOp("dead", []byte("v"), r.now-1))
	r.apply(t, ops...)
	// A generation record shares the index but is not a key.
	r.genbump(t, 0xfeed, 3)

	check := func(seen map[string]int, budget int) {
		t.Helper()
		if len(seen) != len(want) {
			t.Fatalf("budget %d: %d keys delivered, want %d: %v", budget, len(seen), len(want), seen)
		}
		for k := range want {
			if seen[k] != 1 {
				t.Fatalf("budget %d: key %q delivered %d times", budget, k, seen[k])
			}
		}
	}
	check(scanAllKeys(t, r.s, 1<<20, false), 1<<20)
	check(scanAllKeys(t, r.s, 1, false), 1)
	check(scanAllKeys(t, r.s, 7, false), 7)
}

// TestScanKeysSplitStability is the milestone's named property test:
// keys inserted between scan steps force linear-hash splits while the
// walk is in flight, and every key that was present before the scan
// started must still be delivered at least once. The guarantee holds
// because splits only move entries from bucket b to b + 2^level,
// never below the cursor, which is the grow-only argument that let
// the forward cursor replace doc 12's reverse-bit sketch.
func TestScanKeysSplitStability(t *testing.T) {
	r := newStoreRig(t)
	ctx := context.Background()
	const origN = 200
	orig := map[string]bool{}
	ops := []sqlo1.Op{}
	for i := range origN {
		k := fmt.Sprintf("orig%03d", i)
		ops = append(ops, putOp(k, []byte("v"), 0))
		orig[k] = true
	}
	r.apply(t, ops...)

	buckets0 := NumBuckets(r.s.level, r.s.split)
	seen := map[string]bool{}
	cur := uint64(0)
	fill := 0
	for step := 0; ; step++ {
		if step > 100_000 {
			t.Fatal("ScanKeys does not terminate under growth")
		}
		next, err := r.s.ScanKeys(ctx, cur, 32, func(rec sqlo1.Record) {
			seen[string(rec.Key)] = true
		})
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		if next == 0 {
			break
		}
		cur = next
		// Grow the keyspace between steps until the fill cap, so the
		// index splits while the cursor is mid-walk.
		if fill < 600 {
			grow := make([]sqlo1.Op, 0, 8)
			for range 8 {
				grow = append(grow, putOp(fmt.Sprintf("fill%05d", fill), []byte("v"), 0))
				fill++
			}
			r.apply(t, grow...)
		}
	}

	if got := NumBuckets(r.s.level, r.s.split); got <= buckets0 {
		t.Fatalf("no split happened during the scan (buckets %d -> %d); the property was not exercised", buckets0, got)
	}
	for k := range orig {
		if !seen[k] {
			t.Fatalf("key %q present for the whole scan was never delivered", k)
		}
	}
}
