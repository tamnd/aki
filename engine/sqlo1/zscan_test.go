package sqlo1

import (
	"context"
	"errors"
	"testing"
)

// TestZScan drives the fh cursor over the member family: an inline
// zset answers any cursor with everything and a zero next cursor, and
// a segmented walk in small steps visits every member exactly once
// with its score decoded intact, the Redis scan guarantee plus the
// sortable-to-float trip.
func TestZScan(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()

	r.zadd("inl", "a", 1.5, ZAddFlags{})
	r.zadd("inl", "b", -2, ZAddFlags{})
	got := map[string]float64{}
	next, err := r.z.ZScan(ctx, []byte("inl"), 12345, 1, func(m []byte, sc float64) {
		got[string(m)] = sc
	})
	if err != nil || next != 0 || len(got) != 2 || got["a"] != 1.5 || got["b"] != -2 {
		t.Fatalf("inline ZScan = (next %d, %v, %v), want the whole zset with scores and cursor 0", next, got, err)
	}

	// Enough members for several segments so the small-count walk must
	// take many steps, every emitted score checked against its member.
	want := seedZ(t, r, "seg", 0, 1200, func(i int) float64 { return float64(i) * 0.25 })
	seen := map[string]bool{}
	steps := 0
	cursor := uint64(0)
	for {
		next, err := r.z.ZScan(ctx, []byte("seg"), cursor, 16, func(m []byte, sc float64) {
			w, ok := want[string(m)]
			if !ok {
				t.Fatalf("ZScan emitted %q, never added", m)
			}
			if sc != w {
				t.Fatalf("ZScan(%q) score = %g, want %g", m, sc, w)
			}
			if seen[string(m)] {
				t.Fatalf("member %q emitted twice", m)
			}
			seen[string(m)] = true
		})
		if err != nil {
			t.Fatalf("ZScan step %d: %v", steps, err)
		}
		steps++
		if next == 0 {
			break
		}
		if next <= cursor {
			t.Fatalf("cursor went backwards: %d after %d", next, cursor)
		}
		cursor = next
	}
	if len(seen) != len(want) {
		t.Fatalf("scan saw %d members, want %d", len(seen), len(want))
	}
	if steps < 2 {
		t.Fatalf("segmented scan finished in %d step(s); count 16 over %d members should take several", steps, len(want))
	}

	// A cold runtime resumes an in-flight cursor: the first step hands
	// back a live cursor and the resume covers the tail.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	zc := r.reopen()
	mid, err := zc.ZScan(ctx, []byte("seg"), 0, 16, func([]byte, float64) {})
	if err != nil || mid == 0 {
		t.Fatalf("cold first step = (next %d, %v), want a live cursor", mid, err)
	}
	coldSeen := 0
	if _, err := zc.ZScan(ctx, []byte("seg"), mid, 1<<20, func([]byte, float64) { coldSeen++ }); err != nil || coldSeen == 0 {
		t.Fatalf("cold resume = (%d emitted, %v), want the tail", coldSeen, err)
	}

	if next, err := r.z.ZScan(ctx, []byte("absent"), 7, 10, func([]byte, float64) {
		t.Fatal("absent key emitted")
	}); err != nil || next != 0 {
		t.Fatalf("ZScan(absent) = (next %d, %v), want (0, nil)", next, err)
	}

	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatalf("Str.Set: %v", err)
	}
	if _, err := r.z.ZScan(ctx, []byte("str"), 0, 10, func([]byte, float64) {}); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ZScan(str) error = %v, want ErrWrongType", err)
	}
}
