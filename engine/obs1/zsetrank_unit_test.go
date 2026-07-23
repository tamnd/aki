package obs1_test

import (
	"testing"

	"github.com/tamnd/aki/engine/obs1"
)

// The rank arithmetic edges, RAM-only over hand-built plans: the floor
// before every run, on a run boundary, and past the last run; the
// rank-to-run map at zero, on boundaries, and past the cardinality; and
// the empty plan, which a caller reaches when a zset has no cold score
// runs at all.
func TestZsetRankMathEdges(t *testing.T) {
	refs := []obs1.DirRef{
		{FirstDisc: 100, Count: 10},
		{FirstDisc: 200, Count: 20},
		{FirstDisc: 300, Count: 5},
	}
	if got := obs1.ZsetCard(refs); got != 35 {
		t.Fatalf("card = %d, want 35", got)
	}
	floors := []struct {
		key       uint64
		idx, base int
	}{
		{50, 0, 0},   // before every run: floor to the first
		{100, 0, 0},  // exactly the first boundary
		{150, 0, 0},  // inside the first
		{200, 1, 10}, // second boundary
		{250, 1, 10}, // inside the second
		{300, 2, 30}, // last boundary
		{999, 2, 30}, // past everything: the last run
	}
	for _, f := range floors {
		idx, base := obs1.ZsetRankFloor(refs, f.key)
		if idx != f.idx || base != f.base {
			t.Fatalf("floor(%d) = run %d base %d, want run %d base %d", f.key, idx, base, f.idx, f.base)
		}
	}
	ranks := []struct {
		rank, idx, base int
		ok              bool
	}{
		{0, 0, 0, true},
		{9, 0, 0, true},
		{10, 1, 10, true},
		{29, 1, 10, true},
		{30, 2, 30, true},
		{34, 2, 30, true},
		{35, 0, 0, false},
		{-1, 0, 0, false},
	}
	for _, r := range ranks {
		idx, base, ok := obs1.ZsetRunAtRank(refs, r.rank)
		if ok != r.ok || (ok && (idx != r.idx || base != r.base)) {
			t.Fatalf("runAtRank(%d) = %d,%d,%v, want %d,%d,%v", r.rank, idx, base, ok, r.idx, r.base, r.ok)
		}
	}
	if idx, base := obs1.ZsetRankFloor(nil, 5); idx != 0 || base != 0 {
		t.Fatalf("empty floor = %d,%d, want zeros", idx, base)
	}
	if _, _, ok := obs1.ZsetRunAtRank(nil, 0); ok {
		t.Fatal("empty plan resolved a rank")
	}
	if got := obs1.ZsetCard(nil); got != 0 {
		t.Fatalf("empty card = %d, want 0", got)
	}
}
