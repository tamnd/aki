package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// index wraps List.Index into (value, found) strings.
func (r *listRig) index(l *List, key string, idx int64) (string, bool) {
	r.t.Helper()
	e, ok, err := l.Index(context.Background(), []byte(key), idx)
	if err != nil {
		r.t.Fatalf("Index(%q, %d): %v", key, idx, err)
	}
	return string(e), ok
}

// rng collects a Range walk and checks the begin count against the
// emitted count, the header contract every caller leans on.
func (r *listRig) rng(l *List, key string, start, stop int64) []string {
	r.t.Helper()
	var out []string
	n := -1
	err := l.Range(context.Background(), []byte(key), start, stop, func(c int) {
		if n != -1 {
			r.t.Fatalf("Range(%q, %d, %d): begin ran twice", key, start, stop)
		}
		n = c
	}, func(e []byte) {
		out = append(out, string(e))
	})
	if err != nil {
		r.t.Fatalf("Range(%q, %d, %d): %v", key, start, stop, err)
	}
	if n != len(out) {
		r.t.Fatalf("Range(%q, %d, %d): begin said %d, emitted %d", key, start, stop, n, len(out))
	}
	return out
}

// refRange is the oracle: LRANGE's clamped inclusive window over a
// reference slice.
func refRange(ref []string, start, stop int64) []string {
	n := int64(len(ref))
	if start < 0 {
		start = max(start+n, 0)
	}
	if stop < 0 {
		stop += n
	}
	stop = min(stop, n-1)
	if start > stop {
		return nil
	}
	return ref[start : stop+1]
}

// TestListPositionalOracle drives Index, Set, and Range against a
// reference slice on both tiers, cold-reopening after the mutations to
// prove the writes are the durable story.
func TestListPositionalOracle(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	for _, tier := range []struct {
		name string
		n    int
	}{
		{"inline", 50},
		{"noded", 3000},
	} {
		key := "k-" + tier.name
		ref := make([]string, tier.n)
		elems := make([]string, tier.n)
		for i := range ref {
			ref[i] = fmt.Sprintf("e%04d", i)
			elems[i] = ref[i]
		}
		rig.push(key, false, elems...)

		// Every index both ways, plus the two out-of-range sides.
		checkIdx := func(l *List) {
			t.Helper()
			for i := range tier.n {
				if v, ok := rig.index(l, key, int64(i)); !ok || v != ref[i] {
					t.Fatalf("%s: Index(%d) = %q, %v, want %q", tier.name, i, v, ok, ref[i])
				}
				if v, ok := rig.index(l, key, int64(i-tier.n)); !ok || v != ref[i] {
					t.Fatalf("%s: Index(%d) = %q, %v, want %q", tier.name, i-tier.n, v, ok, ref[i])
				}
			}
			for _, i := range []int64{int64(tier.n), int64(-tier.n - 1), 1 << 40, -(1 << 40)} {
				if _, ok := rig.index(l, key, i); ok {
					t.Fatalf("%s: Index(%d) found an element", tier.name, i)
				}
			}
		}
		checkIdx(rig.l)

		// Window shapes: full, clamped, negative, inverted, mid-node
		// starts, and windows crossing one prefetch round.
		windows := [][2]int64{
			{0, -1}, {0, 0}, {5, 5}, {1, 3}, {-2, -1}, {-100000, 100000},
			{int64(tier.n) - 1, int64(tier.n) + 10}, {3, 1}, {-1, -2},
			{int64(tier.n), int64(tier.n) + 5}, {130, 400}, {129, 129},
			{int64(tier.n) / 2, -1},
		}
		checkRanges := func(l *List) {
			t.Helper()
			for _, w := range windows {
				got, want := rig.rng(l, key, w[0], w[1]), refRange(ref, w[0], w[1])
				if len(got) != len(want) {
					t.Fatalf("%s: Range(%d, %d) emitted %d, want %d", tier.name, w[0], w[1], len(got), len(want))
				}
				for i := range got {
					if got[i] != want[i] {
						t.Fatalf("%s: Range(%d, %d)[%d] = %q, want %q", tier.name, w[0], w[1], i, got[i], want[i])
					}
				}
			}
		}
		checkRanges(rig.l)

		// Sets across the shape: both edges, both signs, a mid-node
		// element, and a replacement far larger than the original.
		big := strings.Repeat("B", 700)
		for _, s := range []struct {
			idx int64
			val string
		}{
			{0, "head"}, {-1, "tail"}, {int64(tier.n) / 2, big}, {-int64(tier.n) / 3, "mid"},
		} {
			if err := rig.l.Set(ctx, []byte(key), s.idx, []byte(s.val)); err != nil {
				t.Fatalf("%s: Set(%d): %v", tier.name, s.idx, err)
			}
			i := s.idx
			if i < 0 {
				i += int64(tier.n)
			}
			ref[i] = s.val
		}
		checkIdx(rig.l)
		checkRanges(rig.l)
		if n, err := rig.l.Len(ctx, []byte(key)); err != nil || n != int64(tier.n) {
			t.Fatalf("%s: Len after sets = %d, %v", tier.name, n, err)
		}

		// The cold view a restart would see agrees, after the drain a
		// clean shutdown implies.
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		cold := rig.reopen()
		checkIdx(cold)
		checkRanges(cold)

		// The doors: out-of-range set, missing key, wrong type.
		if err := rig.l.Set(ctx, []byte(key), int64(tier.n), []byte("x")); !errors.Is(err, errListIndexRange) {
			t.Fatalf("%s: Set(len) = %v, want index range", tier.name, err)
		}
	}

	if err := rig.l.Set(ctx, []byte("missing"), 0, []byte("x")); !errors.Is(err, errListNoKey) {
		t.Fatalf("Set on a missing key = %v, want no such key", err)
	}
	if got := rig.rng(rig.l, "missing", 0, -1); len(got) != 0 {
		t.Fatalf("Range on a missing key emitted %d", len(got))
	}
	if _, ok := rig.index(rig.l, "missing", 0); ok {
		t.Fatal("Index on a missing key found an element")
	}
	if err := rig.s.Set(context.Background(), []byte("str"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rig.l.Index(ctx, []byte("str"), 0); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Index on a string = %v", err)
	}
	if err := rig.l.Set(ctx, []byte("str"), 0, []byte("x")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Set on a string = %v", err)
	}
	if err := rig.l.Range(ctx, []byte("str"), 0, -1, func(int) {}, func([]byte) {}); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Range on a string = %v", err)
	}
}

// TestListSetInlineUpgrade pins the LSET byte-cap crossing: replacing
// an inline element with one too large for the inline root upgrades to
// the noded layout with the order and count preserved, the same
// one-way ladder a push takes.
func TestListSetInlineUpgrade(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	ref := make([]string, 20)
	elems := make([]string, 20)
	for i := range ref {
		ref[i] = fmt.Sprintf("v%02d-%s", i, strings.Repeat("a", 60))
		elems[i] = ref[i]
	}
	rig.push("k", false, elems...)
	if enc, ok, _ := rig.l.Encoding(ctx, []byte("k")); !ok || enc != "listpack" {
		t.Fatalf("encoding before = %q, %v", enc, ok)
	}

	big := strings.Repeat("B", listInlineMax)
	if err := rig.l.Set(ctx, []byte("k"), 5, []byte(big)); err != nil {
		t.Fatalf("upgrading Set: %v", err)
	}
	ref[5] = big
	if enc, ok, _ := rig.l.Encoding(ctx, []byte("k")); !ok || enc != "quicklist" {
		t.Fatalf("encoding after = %q, %v", enc, ok)
	}
	if n, err := rig.l.Len(ctx, []byte("k")); err != nil || n != 20 {
		t.Fatalf("Len after upgrade = %d, %v", n, err)
	}
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for _, l := range []*List{rig.l, rig.reopen()} {
		got := rig.rng(l, "k", 0, -1)
		if len(got) != len(ref) {
			t.Fatalf("Range after upgrade emitted %d", len(got))
		}
		for i := range got {
			if got[i] != ref[i] {
				t.Fatalf("Range after upgrade [%d] = %q, want %q", i, got[i], ref[i])
			}
		}
	}
}
