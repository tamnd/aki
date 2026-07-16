package set

import (
	"fmt"
	"math/bits"
	"sort"
	"testing"
)

// The per-partition merge fan-out suite (fanout.go, doc 11 section 6.5): the
// group cut covers the domain exactly, partitioned operand pairs answer the
// algebra identically to the probe path across shapes and seeds, SINTERCARD's
// LIMIT early-stop survives the group loop, mixed shapes fall to probe, and
// the STORE aliasing rules hold when the sources are partitioned.

// memGen builds n distinct members whose bytes depend on the seed, so every
// seed reshuffles the hash and therefore the partition placement while the
// index range still controls overlap between operands.
func memGen(seed int64, lo, n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("m%d.%d", seed, lo+i)
	}
	return out
}

// partitionedPair builds two operands under a lowered engagement threshold with
// the algebra flag as set, asserting both land in the partitioned band.
func partitionedPair(t *testing.T, a, b []string) (*set, *set) {
	t.Helper()
	sa, sb := setFrom(a), setFrom(b)
	if sa.enc != encPartitioned || sb.enc != encPartitioned {
		t.Fatalf("enc = %s/%s, want partitioned/partitioned", sa.enc, sb.enc)
	}
	return sa, sb
}

// TestPartitionedGroupsCoverDomain proves the group cut is a partition of the
// set: at g == P and at the cross-P slicings g = 2P and g = 4P, every live
// member appears in exactly one group stream, every entry's hash sits inside
// its group's range, and the resolved members match the set exactly.
func TestPartitionedGroupsCoverDomain(t *testing.T) {
	withThreshold(t, 512)
	defer SetAlgebraMaintain(SetAlgebraMaintain(true))
	members := memGen(3, 0, 4096)
	s := setFrom(members)
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned", s.enc)
	}
	if !s.part.indexed() {
		t.Fatal("partitioned set not fully indexed under the flag")
	}
	p := len(s.part.parts)
	for _, g := range []int{p, 2 * p, 4 * p} {
		gv := s.part.groups(g, s.part.streams(nil))
		if len(gv) != g {
			t.Fatalf("groups(%d) returned %d views", g, len(gv))
		}
		shift := uint(64 - (bits.Len(uint(g)) - 1))
		var got []string
		for i := range gv {
			lo := uint64(i) << shift
			hi := uint64(i+1) << shift // wraps to 0 for the last group
			st := gv[i].s
			for !st.empty() {
				e := st.next()
				if e.h < lo || (hi != 0 && e.h >= hi) {
					t.Fatalf("g=%d group %d holds hash %#x outside [%#x, %#x)", g, i, e.h, lo, hi)
				}
				got = append(got, string(gv[i].h.memberByOrd(e.ord)))
			}
		}
		want := append([]string(nil), members...)
		sort.Strings(want)
		sort.Strings(got)
		eqStrings(t, fmt.Sprintf("groups(%d) members", g), got, want)
	}
}

// TestPartitionedAlgebraOracle runs the four algebra drivers over partitioned
// operand pairs, same-P and cross-P, across shapes and seeds, with the flag on
// (the fan-out merge path) and off (the probe path), against the map oracle.
// The merge choice is asserted, so a silently probe-routed pair fails rather
// than vacuously passing.
func TestPartitionedAlgebraOracle(t *testing.T) {
	withThreshold(t, 256)
	shapes := []struct {
		name   string
		a, b   func(seed int64) []string
		crossP bool
	}{
		{"same-P equal", func(s int64) []string { return memGen(s, 0, 2048) }, func(s int64) []string { return memGen(s, 0, 2048) }, false},
		{"same-P partial", func(s int64) []string { return memGen(s, 0, 2048) }, func(s int64) []string { return memGen(s, 1024, 2048) }, false},
		{"same-P disjoint", func(s int64) []string { return memGen(s, 0, 2048) }, func(s int64) []string { return memGen(s, 10000, 2048) }, false},
		{"cross-P subset", func(s int64) []string { return memGen(s, 0, 800) }, func(s int64) []string { return memGen(s, 0, 2048) }, true},
		{"cross-P partial", func(s int64) []string { return memGen(s, 400, 800) }, func(s int64) []string { return memGen(s, 0, 2048) }, true},
	}
	for _, sh := range shapes {
		for _, seed := range []int64{1, 7, 42} {
			for _, flag := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/seed=%d/maintain=%v", sh.name, seed, flag), func(t *testing.T) {
					defer SetAlgebraMaintain(SetAlgebraMaintain(flag))
					ma, mb := sh.a(seed), sh.b(seed)
					sa, sb := partitionedPair(t, ma, mb)
					if sh.crossP && len(sa.part.parts) == len(sb.part.parts) {
						t.Fatalf("cross-P shape landed same P %d", len(sa.part.parts))
					}
					if flag {
						if !chooseMergeIntersect(sa, sb) {
							t.Fatal("flag on: partitioned pair should choose the merge path")
						}
					} else if sa.mergeable() || sb.mergeable() {
						t.Fatal("flag off: no operand should be mergeable")
					}
					ops := [][]string{ma, mb}
					sets := []*set{sa, sb}
					eqStrings(t, "inter", driveInter(sets), oracleInter(ops))
					eqStrings(t, "union", driveUnion(sets), oracleUnion(ops))
					eqStrings(t, "diff", driveDiff(sets), oracleDiff(ops))
					eqStrings(t, "diff rev", driveDiff([]*set{sb, sa}), oracleDiff([][]string{mb, ma}))
					if got, want := sintercard(nil, sets, 0), len(oracleInter(ops)); got != want {
						t.Fatalf("sintercard(nil, limit 0) = %d, want %d", got, want)
					}
				})
			}
		}
	}
}

// TestPartitionedSintercardLimit checks the LIMIT early-stop across the group
// loop: limits landing inside the first group, spanning group boundaries, at
// the exact intersection size, and past it all answer min(limit, card), with
// limit 0 unlimited (Redis).
func TestPartitionedSintercardLimit(t *testing.T) {
	withThreshold(t, 512)
	defer SetAlgebraMaintain(SetAlgebraMaintain(true))
	sa, sb := partitionedPair(t, memGen(9, 0, 4096), memGen(9, 2048, 4096))
	if !chooseMergeIntersect(sa, sb) {
		t.Fatal("pair should choose the merge path")
	}
	const card = 2048 // members 2048..4095 are shared
	for _, limit := range []int{0, 1, 100, 600, card - 1, card, card + 500} {
		want := limit
		if limit == 0 || limit > card {
			want = card
		}
		if got := sintercard(nil, []*set{sa, sb}, limit); got != want {
			t.Fatalf("limit %d: got %d, want %d", limit, got, want)
		}
	}
}

// TestFanoutChooseGuards checks the shape rules around the partitioned merge:
// a mixed flat and partitioned pair falls to probe (the recorded cross-shape
// deferral), a partitioned set built with the flag off is not mergeable (the
// default-off judgment holds for the band), and an eligible partitioned pair
// chooses merge.
func TestFanoutChooseGuards(t *testing.T) {
	withThreshold(t, 512)
	defer SetAlgebraMaintain(SetAlgebraMaintain(true))
	part := setFrom(memGen(5, 0, 4096))
	if part.enc != encPartitioned || !part.mergeable() {
		t.Fatalf("enc = %s, mergeable = %v; want a mergeable partitioned set", part.enc, part.mergeable())
	}
	flat := indexedSet(gen("m", 0, 400, 8))
	if mergeablePair(flat, part) || mergeablePair(part, flat) {
		t.Fatal("mixed flat and partitioned pair must fall to probe")
	}
	if chooseMergeIntersect(flat, part) {
		t.Fatal("mixed pair chose merge")
	}
	other := setFrom(memGen(6, 0, 4096))
	if !chooseMergeIntersect(part, other) {
		t.Fatal("partitioned pair should choose merge")
	}
	SetAlgebraMaintain(false)
	plain := setFrom(memGen(8, 0, 4096))
	if plain.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned", plain.enc)
	}
	if plain.mergeable() {
		t.Fatal("flag-off partitioned set reported mergeable")
	}
}

// TestPartitionedStoreAliasing drives the STORE aliasing shapes with
// partitioned, merge-eligible sources: the fan-out reads the sources in full
// before place moves the destination pointer, so a destination that is also a
// source is replaced correctly with nothing cloned, and a result past the
// threshold is born partitioned.
func TestPartitionedStoreAliasing(t *testing.T) {
	withThreshold(t, 512)
	defer SetAlgebraMaintain(SetAlgebraMaintain(true))
	ma, mb := memGen(11, 0, 4096), memGen(11, 2048, 4096)
	cases := []struct {
		name string
		op   string
		dest string
		srcs []string
		want []string
	}{
		{"inter dest is first source", "inter", "a", []string{"a", "b"}, oracleInter([][]string{ma, mb})},
		{"inter dest is later source", "inter", "b", []string{"a", "b"}, oracleInter([][]string{ma, mb})},
		{"diff dest is first source", "diff", "a", []string{"a", "b"}, oracleDiff([][]string{ma, mb})},
		{"diff dest is later source", "diff", "b", []string{"a", "b"}, oracleDiff([][]string{ma, mb})},
		{"union dest is first source", "union", "a", []string{"a", "b"}, oracleUnion([][]string{ma, mb})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cx, g := newCtx(t)
			sa, sb := partitionedPair(t, ma, mb)
			if !chooseMergeIntersect(sa, sb) {
				t.Fatal("sources should be merge-eligible")
			}
			g.m["a"], g.m["b"] = sa, sb
			n := applyStore(t, cx, g, c.op, c.dest, c.srcs...)
			if n != len(c.want) {
				t.Fatalf("reply %d, want %d", n, len(c.want))
			}
			eqStrings(t, "dest members", storeMembers(g.m[c.dest]), c.want)
			if len(c.want) >= partitionThreshold {
				if got := g.m[c.dest].enc; got != encPartitioned {
					t.Fatalf("dest enc = %s, want partitioned (born past the threshold)", got)
				}
			}
		})
	}
}
