package command

import (
	"fmt"
	"strconv"
	"testing"
)

// TestZRankOrderStatMatchesScan checks the order-statistic ZRANK/ZREVRANK and
// ZRANGE-by-index path against an independent oracle: the canonical member order
// produced by a single forward ZRANGE 0 -1 walk. That full walk seeks once and
// then steps the cursor sequentially, so it never consults the per-position
// Rank/SelectAt descents; comparing the descents' answers to the walk's ordering
// cross-checks the augmented tree against the plain cursor on the same data.
//
// The zset is large enough to be in coll form (skiplist encoding), which is where
// a freshly built zset sub-tree opens order-statistic, so this exercises the wired
// fast path rather than the blob path.
func TestZRankOrderStatMatchesScan(t *testing.T) {
	r, c := startData(t)
	const n = 3000

	// Distinct ascending scores, so member i is the unique element at rank i and the
	// ground-truth order is unambiguous. Pad members so the set stays in coll form.
	pad := make([]byte, 200)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) string { return fmt.Sprintf("m:%06d", i) + string(pad) }
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("ZADD z %d %s", i, member(i)))
	}
	if got := sendLine(t, r, c, "OBJECT ENCODING z"); got != "$8" {
		t.Fatalf("zset not in coll form: OBJECT ENCODING header = %q", got)
	}
	if got := sendLine(t, r, c, ""); got != "skiplist" {
		t.Fatalf("zset not in coll form: OBJECT ENCODING = %q", got)
	}

	// Oracle: the canonical ascending order from one forward scan.
	order := readArray(t, r, c, "ZRANGE z 0 -1")
	if len(order) != n {
		t.Fatalf("ZRANGE 0 -1 returned %d members, want %d", len(order), n)
	}
	rankOf := make(map[string]int, n)
	for i, m := range order {
		rankOf[m] = i
	}

	// ZRANK and ZREVRANK against the oracle for a spread of positions, including the
	// two ends where an off-by-one in the card offset would show.
	for _, i := range []int{0, 1, 2, n / 3, n / 2, (2 * n) / 3, n - 2, n - 1} {
		m := member(i)
		want := rankOf[m]
		if got := sendLine(t, r, c, "ZRANK z "+m); got != ":"+strconv.Itoa(want) {
			t.Fatalf("ZRANK %s = %q, want :%d", m, got, want)
		}
		if got := sendLine(t, r, c, "ZREVRANK z "+m); got != ":"+strconv.Itoa(n-1-want) {
			t.Fatalf("ZREVRANK %s = %q, want :%d", m, got, n-1-want)
		}
	}

	// ZRANGE by single index must SelectAt straight to the i-th member; compare to
	// the oracle order so the seek-by-rank descent is checked across the set.
	for _, i := range []int{0, 1, n / 4, n / 2, n - 1} {
		got := readArray(t, r, c, fmt.Sprintf("ZRANGE z %d %d", i, i))
		if len(got) != 1 || got[0] != order[i] {
			t.Fatalf("ZRANGE z %d %d = %v, want [%s]", i, i, got, order[i])
		}
	}

	// A multi-element window and a reverse window, to check the walk after the
	// order-stat seek lands at the right start in both directions.
	win := readArray(t, r, c, "ZRANGE z 10 19")
	for j, m := range win {
		if m != order[10+j] {
			t.Fatalf("ZRANGE z 10 19 element %d = %s, want %s", j, m, order[10+j])
		}
	}
	rev := readArray(t, r, c, "ZREVRANGE z 0 9")
	for j, m := range rev {
		if m != order[n-1-j] {
			t.Fatalf("ZREVRANGE z 0 9 element %d = %s, want %s", j, m, order[n-1-j])
		}
	}

	// An absent member ranks as a nil reply, not a wrong integer from the descent.
	if got := sendLine(t, r, c, "ZRANK z nope"); got != "$-1" && got != "_" {
		t.Fatalf("ZRANK of absent member = %q, want nil", got)
	}
}
