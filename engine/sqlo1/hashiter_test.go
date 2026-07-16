package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// iterAll drains HIterate into a map plus the begin count.
func iterAll(t *testing.T, h *Hash, key string) (map[string]string, int) {
	t.Helper()
	got := map[string]string{}
	count := -1
	err := h.HIterate(context.Background(), []byte(key), func(n int) {
		count = n
	}, func(f, v []byte) {
		got[string(f)] = string(v)
	})
	if err != nil {
		t.Fatalf("HIterate(%q): %v", key, err)
	}
	if count != len(got) {
		t.Fatalf("HIterate(%q) announced %d fields, emitted %d", key, count, len(got))
	}
	return got, count
}

func TestHIterateInline(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)

	got, count := iterAll(t, r.h, "missing")
	if count != 0 || len(got) != 0 {
		t.Fatalf("absent key iterated %d fields", count)
	}

	r.hset("h", "b", "2")
	r.hset("h", "a", "1")
	r.hset("h", "c", "3")

	// Inline emits in insertion order; assert it, not just the set.
	var order []string
	err := r.h.HIterate(ctx, []byte("h"), func(n int) {
		if n != 3 {
			t.Fatalf("begin(%d), want 3", n)
		}
	}, func(f, v []byte) {
		order = append(order, string(f)+"="+string(v))
	})
	if err != nil {
		t.Fatalf("HIterate: %v", err)
	}
	want := []string{"b=2", "a=1", "c=3"}
	for i := range want {
		if i >= len(order) || order[i] != want[i] {
			t.Fatalf("inline order = %v, want %v", order, want)
		}
	}

	if err := r.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	err = r.h.HIterate(ctx, []byte("str"), func(int) {}, func(_, _ []byte) {})
	if !errors.Is(err, ErrWrongType) {
		t.Fatalf("HIterate on a string = %v, want ErrWrongType", err)
	}
}

func TestHIterateSegmented(t *testing.T) {
	r := newHashRig(t)
	val := segRigHash(t, r, "h", 200)

	got, count := iterAll(t, r.h, "h")
	if count != 200 {
		t.Fatalf("hot iterate count = %d, want 200", count)
	}
	for i := range 200 {
		f := fmt.Sprintf("f%03d", i)
		if got[f] != val(i) {
			t.Fatalf("hot iterate field %s = %q, want %q", f, got[f], val(i))
		}
	}

	// Cold: a fresh runtime sees only the store. The full iteration
	// must reproduce the hash and spend exactly one IO round on the
	// root plus one per hashIterBatchSegs segments.
	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	h2 := r.reopen()
	before := r.rs.readRounds
	got, count = iterAll(t, h2, "h")
	rounds := r.rs.readRounds - before
	if count != 200 {
		t.Fatalf("cold iterate count = %d, want 200", count)
	}
	for i := range 200 {
		f := fmt.Sprintf("f%03d", i)
		if got[f] != val(i) {
			t.Fatalf("cold iterate field %s = %q, want %q", f, got[f], val(i))
		}
	}
	nsegs := len(h2.segRoot.fence)
	if nsegs < 2 {
		t.Fatalf("rig hash has %d segments, the batch test needs more", nsegs)
	}
	want := 1 + (nsegs+hashIterBatchSegs-1)/hashIterBatchSegs
	if rounds != want {
		t.Fatalf("cold iterate over %d segments took %d IO rounds, want %d", nsegs, rounds, want)
	}
}

func TestHScanInline(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)

	next, err := r.h.HScan(ctx, []byte("missing"), 0, 10, func(_, _ []byte) {
		t.Fatal("absent key emitted a field")
	})
	if err != nil || next != 0 {
		t.Fatalf("HScan absent = %d, %v; want 0, nil", next, err)
	}

	r.hset("h", "a", "1")
	r.hset("h", "b", "2")

	// Inline answers any cursor with the whole set and cursor zero,
	// the listpack behavior.
	for _, cursor := range []uint64{0, 7} {
		got := map[string]string{}
		next, err := r.h.HScan(ctx, []byte("h"), cursor, 1, func(f, v []byte) {
			got[string(f)] = string(v)
		})
		if err != nil || next != 0 {
			t.Fatalf("HScan(cursor=%d) = %d, %v; want 0, nil", cursor, next, err)
		}
		if len(got) != 2 || got["a"] != "1" || got["b"] != "2" {
			t.Fatalf("HScan(cursor=%d) emitted %v", cursor, got)
		}
	}
}

// TestHScanCoverageUnderSplits is the doc 06 contract test: a field
// present for the whole scan is returned at least once, even while
// writes between cursor steps split segments under the scan. The fh
// cursor survives because an entry's fh never changes and splits
// preserve range order.
func TestHScanCoverageUnderSplits(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	fat := segRigHash(t, r, "h", 240)

	seen := map[string]bool{}
	cursor := uint64(0)
	churn := 0
	for step := 0; ; step++ {
		if step > 500 {
			t.Fatal("scan did not complete in 500 steps")
		}
		var err error
		cursor, err = r.h.HScan(ctx, []byte("h"), cursor, 10, func(f, _ []byte) {
			seen[string(f)] = true
		})
		if err != nil {
			t.Fatalf("HScan step %d: %v", step, err)
		}
		if cursor == 0 {
			break
		}
		// Churn for the first stretch: fat inserts force splits under
		// the cursor, and deletes hit only fields born during the
		// scan, so the at-least-once contract stays on the baseline.
		if step < 12 {
			for range 8 {
				r.hset("h", fmt.Sprintf("g%03d", churn), fat(churn))
				churn++
			}
			for d := churn - 3; d < churn; d++ {
				if _, err := r.h.HDel(ctx, []byte("h"), fmt.Appendf(nil, "g%03d", d)); err != nil {
					t.Fatalf("HDel churn %d: %v", d, err)
				}
			}
		}
	}
	for i := range 240 {
		if f := fmt.Sprintf("f%03d", i); !seen[f] {
			t.Fatalf("baseline field %s never returned by the scan", f)
		}
	}
}

// TestHScanResumeCold proves the cursor is stateless: a scan begun on
// one runtime finishes on a fresh one over the same store.
func TestHScanResumeCold(t *testing.T) {
	ctx := context.Background()
	r := newHashRig(t)
	segRigHash(t, r, "h", 200)

	seen := map[string]bool{}
	collect := func(f, _ []byte) { seen[string(f)] = true }

	cursor, err := r.h.HScan(ctx, []byte("h"), 0, 40, collect)
	if err != nil || cursor == 0 {
		t.Fatalf("first step = %d, %v; want a mid-scan cursor", cursor, err)
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	h2 := r.reopen()
	for step := 0; cursor != 0; step++ {
		if step > 100 {
			t.Fatal("cold resume did not complete in 100 steps")
		}
		cursor, err = h2.HScan(ctx, []byte("h"), cursor, 40, collect)
		if err != nil {
			t.Fatalf("cold step: %v", err)
		}
	}
	if len(seen) != 200 {
		t.Fatalf("scan returned %d distinct fields, want 200", len(seen))
	}
	for i := range 200 {
		if f := fmt.Sprintf("f%03d", i); !seen[f] {
			t.Fatalf("field %s never returned", f)
		}
	}
}
