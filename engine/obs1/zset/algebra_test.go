package zset

import (
	"fmt"
	"math"
	"testing"
)

// The zset algebra kernels (spec 2064/f3/12 section 6.12), settled by
// labs/f3/m2/05_algebra_accum. This suite proves the two accumulation structures
// (merge and hash) are interchangeable exactly as the lab requires, that the
// weighted-arithmetic edge cases match Redis, that the STORE build respects the
// size ladder, and that the ZINTERCARD probe loop allocates nothing.

type msPair struct {
	m string
	s float64
}

// makeZ builds an inline-band zset from the pairs.
func makeZ(pairs []msPair) *zset {
	z := newZset()
	for _, p := range pairs {
		z.update([]byte(p.m), p.s, flags{})
	}
	return z
}

// makeNativeZ builds the same zset already promoted to the native band, so the
// kernels are exercised over the counted tree and member hash rather than the
// blob. A 65-byte member breaches the inline value cap and promotes; removing it
// leaves the real pairs in the native band (promotion is one-way, F4).
func makeNativeZ(pairs []msPair) *zset {
	z := makeZ(pairs)
	long := make([]byte, maxListpackValue+1)
	z.update(long, 0, flags{})
	z.rem(long)
	if z.enc != encSkiplist {
		panic("makeNativeZ did not promote")
	}
	return z
}

// modelUnion is the independent union oracle: fold every source member once per
// source with weighted aggregation, then sort by score.
func modelUnion(ops []operand, mode aggMode) []scoredMember {
	acc := map[string]float64{}
	var order []string
	for _, o := range ops {
		if o.z == nil {
			continue
		}
		o.z.forEach(func(m []byte, s float64) bool {
			k := string(m)
			v := weightScore(o.weight, s)
			if _, seen := acc[k]; !seen {
				acc[k] = v
				order = append(order, k)
			} else {
				acc[k] = aggregate(mode, acc[k], v)
			}
			return true
		})
	}
	out := make([]scoredMember, 0, len(order))
	for _, k := range order {
		out = append(out, scoredMember{member: []byte(k), score: acc[k]})
	}
	sortByScore(out)
	return out
}

// eqPairs asserts two result slices carry the same members in the same order
// with equal scores (score bits, so -0.0 and NaN mismatches surface).
func eqScored(t *testing.T, name string, got, want []scoredMember) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d pairs, want %d\n got=%s\nwant=%s", name, len(got), len(want), fmtPairs(got), fmtPairs(want))
	}
	for i := range got {
		if string(got[i].member) != string(want[i].member) || math.Float64bits(got[i].score) != math.Float64bits(want[i].score) {
			t.Fatalf("%s: at %d got %s, want %s", name, i, fmtPairs(got), fmtPairs(want))
		}
	}
}

func fmtPairs(p []scoredMember) string {
	s := "["
	for i, e := range p {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%s:%g", e.member, e.score)
	}
	return s + "]"
}

func sm(pairs ...msPair) []scoredMember {
	out := make([]scoredMember, len(pairs))
	for i, p := range pairs {
		out[i] = scoredMember{member: []byte(p.m), score: p.s}
	}
	return out
}

// bands runs fn once with inline-band operands and once with native-band
// operands built from the same pairs, so every kernel test covers both encodings.
func bands(t *testing.T, srcs [][]msPair, fn func(t *testing.T, mk func([]msPair) *zset)) {
	t.Helper()
	t.Run("inline", func(t *testing.T) { fn(t, makeZ) })
	t.Run("native", func(t *testing.T) { fn(t, makeNativeZ) })
}

var algebraShapes = [][][]msPair{
	// disjoint
	{{{"a", 1}, {"b", 2}}, {{"c", 3}, {"d", 4}}},
	// full overlap, distinct scores
	{{{"a", 1}, {"b", 2}, {"c", 3}}, {{"a", 10}, {"b", 20}, {"c", 30}}},
	// partial overlap
	{{{"a", 1}, {"b", 2}, {"c", 3}}, {{"b", 5}, {"c", 6}, {"d", 7}}},
	// three-way fan-in
	{{{"a", 1}, {"x", 2}}, {{"a", 3}, {"y", 4}}, {{"a", 5}, {"z", 6}}},
	// score ties (member tiebreak decides order)
	{{{"a", 5}, {"b", 5}, {"c", 5}}, {{"a", 0}, {"c", 0}}},
}

// TestUnionAccumulatorEquivalence is the lab-05 interchange proof: merge and hash
// produce the identical aggregated result at every shape, weight, and aggregate
// mode, and union() (the budget-switched driver) agrees with the model oracle.
func TestUnionAccumulatorEquivalence(t *testing.T) {
	weightsets := [][]float64{nil, {1, 1, 1}, {2, 3, 4}, {0, 1, 1}, {-1, 2, 1}}
	modes := []aggMode{aggSum, aggMin, aggMax}
	for si, srcs := range algebraShapes {
		for _, ws := range weightsets {
			for _, mode := range modes {
				bands(t, srcs, func(t *testing.T, mk func([]msPair) *zset) {
					ops := make([]operand, len(srcs))
					for i, s := range srcs {
						w := 1.0
						if ws != nil {
							w = ws[i]
						}
						ops[i] = operand{z: mk(s), weight: w}
					}
					total := totalInput(ops)
					want := modelUnion(ops, mode)
					mg := mergeUnion(ops, mode, total)
					sortByScore(mg)
					hs := hashUnion(ops, mode, total)
					sortByScore(hs)
					name := fmt.Sprintf("shape%d/w%v/m%d", si, ws, mode)
					eqScored(t, name+"/merge", mg, want)
					eqScored(t, name+"/hash", hs, want)
					eqScored(t, name+"/union", union(ops, mode), want)
				})
			}
		}
	}
}

// TestUnionConcrete pins a union result against numbers taken from live Redis 8.8
// (a={1 a,2 b,3 c}, b={10 a,20 b,30 c}) so the oracle itself is anchored.
func TestUnionConcrete(t *testing.T) {
	a := []msPair{{"a", 1}, {"b", 2}, {"c", 3}}
	b := []msPair{{"a", 10}, {"b", 20}, {"c", 30}}
	ops := []operand{{z: makeZ(a), weight: 1}, {z: makeZ(b), weight: 1}}
	eqScored(t, "sum", union(ops, aggSum), sm(msPair{"a", 11}, msPair{"b", 22}, msPair{"c", 33}))
	eqScored(t, "min", union(ops, aggMin), sm(msPair{"a", 1}, msPair{"b", 2}, msPair{"c", 3}))
	eqScored(t, "max", union(ops, aggMax), sm(msPair{"a", 10}, msPair{"b", 20}, msPair{"c", 30}))
	// WEIGHTS 2 3: a=1*2+10*3=32, b=2*2+20*3=64, c=3*2+30*3=96.
	wops := []operand{{z: makeZ(a), weight: 2}, {z: makeZ(b), weight: 3}}
	eqScored(t, "weights", union(wops, aggSum), sm(msPair{"a", 32}, msPair{"b", 64}, msPair{"c", 96}))
}

// TestWeightedEdgeCases pins the two Redis arithmetic quirks (section 6.12): a
// zero weight times an infinite score is 0, not NaN, and a SUM of +inf and -inf
// is 0, not NaN, so no NaN ever reaches a score key.
func TestWeightedEdgeCases(t *testing.T) {
	inf := math.Inf(1)
	if got := weightScore(0, inf); got != 0 {
		t.Fatalf("0 * +inf = %v, want 0", got)
	}
	if got := weightScore(0, math.Inf(-1)); got != 0 {
		t.Fatalf("0 * -inf = %v, want 0", got)
	}
	if got := aggregate(aggSum, inf, math.Inf(-1)); got != 0 {
		t.Fatalf("SUM(+inf,-inf) = %v, want 0", got)
	}
	// 0 * +inf inside a real union: member with an infinite score, weight 0.
	ops := []operand{{z: makeZ([]msPair{{"a", inf}}), weight: 0}, {z: makeZ([]msPair{{"a", 5}}), weight: 1}}
	eqScored(t, "zero-weight-inf", union(ops, aggSum), sm(msPair{"a", 5}))
	// +inf and -inf across sources fold to 0 under SUM.
	ops2 := []operand{{z: makeZ([]msPair{{"a", inf}}), weight: 1}, {z: makeZ([]msPair{{"a", inf}}), weight: -1}}
	eqScored(t, "inf-minus-inf", union(ops2, aggSum), sm(msPair{"a", 0}))
	// MIN and MAX after weighting keep the extreme, never NaN.
	if got := aggregate(aggMin, 3, -2); got != -2 {
		t.Fatalf("MIN = %v, want -2", got)
	}
	if got := aggregate(aggMax, 3, -2); got != 3 {
		t.Fatalf("MAX = %v, want 3", got)
	}
}

// TestSortByScore checks the total order: score ascending on the sortable key so
// the infinities and signed zero land where the destination tree keys them, ties
// broken by raw member bytes.
func TestSortByScore(t *testing.T) {
	in := sm(
		msPair{"z", math.Inf(1)}, msPair{"m", 0}, msPair{"a", math.Inf(-1)},
		msPair{"b", math.Copysign(0, -1)}, msPair{"c", -5}, msPair{"aa", 0},
	)
	sortByScore(in)
	want := sm(
		msPair{"a", math.Inf(-1)}, msPair{"c", -5},
		msPair{"aa", 0}, msPair{"b", math.Copysign(0, -1)}, msPair{"m", 0},
		msPair{"z", math.Inf(1)},
	)
	// -0.0 and +0.0 share a score key, so their relative order is by member bytes;
	// bits differ, so compare via score value here, not bits.
	if len(in) != len(want) {
		t.Fatalf("len %d want %d", len(in), len(want))
	}
	for i := range in {
		if string(in[i].member) != string(want[i].member) {
			t.Fatalf("at %d got %q want %q (%s)", i, in[i].member, want[i].member, fmtPairs(in))
		}
	}
}

// TestIntersect covers the small-side hash join at both bands: positional weighted
// aggregation, a missing source emptying the result, and the aggregate modes.
func TestIntersect(t *testing.T) {
	shapes := [][][]msPair{
		{{{"a", 1}, {"b", 2}, {"c", 3}}, {{"b", 5}, {"c", 6}, {"d", 7}}},
		{{{"a", 1}, {"b", 2}}, {{"a", 3}, {"b", 4}}, {{"a", 5}, {"b", 6}}},
	}
	for si, srcs := range shapes {
		bands(t, srcs, func(t *testing.T, mk func([]msPair) *zset) {
			ops := make([]operand, len(srcs))
			for i, s := range srcs {
				ops[i] = operand{z: mk(s), weight: 1}
			}
			// Intersection membership is those in every source; score is the SUM.
			want := intersectModel(ops, aggSum)
			eqScored(t, fmt.Sprintf("shape%d/sum", si), intersect(ops, aggSum), want)
			eqScored(t, fmt.Sprintf("shape%d/min", si), intersect(ops, aggMin), intersectModel(ops, aggMin))
			eqScored(t, fmt.Sprintf("shape%d/max", si), intersect(ops, aggMax), intersectModel(ops, aggMax))
		})
	}
	// A missing (nil) source empties the intersection.
	ops := []operand{{z: makeZ([]msPair{{"a", 1}}), weight: 1}, {z: nil, weight: 1}}
	if got := intersect(ops, aggSum); got != nil {
		t.Fatalf("intersect with missing source = %s, want empty", fmtPairs(got))
	}
	// WEIGHTS in the intersection: a in both, 1*2 + 3*10 = 32.
	wops := []operand{{z: makeZ([]msPair{{"a", 1}, {"b", 2}}), weight: 2}, {z: makeZ([]msPair{{"a", 3}}), weight: 10}}
	eqScored(t, "weights", intersect(wops, aggSum), sm(msPair{"a", 32}))
}

// intersectModel is the independent intersection oracle: keep members in every
// source, fold their weighted scores positionally, sort by score.
func intersectModel(ops []operand, mode aggMode) []scoredMember {
	var out []scoredMember
	if len(ops) == 0 || ops[0].z == nil {
		return nil
	}
	ops[0].z.forEach(func(m []byte, _ float64) bool {
		acc := 0.0
		for i := range ops {
			if ops[i].z == nil {
				return true
			}
			s, ok := ops[i].z.score(m)
			if !ok {
				return true
			}
			v := weightScore(ops[i].weight, s)
			if i == 0 {
				acc = v
			} else {
				acc = aggregate(mode, acc, v)
			}
		}
		out = append(out, scoredMember{member: m, score: acc})
		return true
	})
	sortByScore(out)
	return out
}

// TestDiff covers the reject-filter at both bands and the missing-first-source
// empty result.
func TestDiff(t *testing.T) {
	srcs := [][]msPair{{{"a", 1}, {"b", 2}, {"c", 3}}, {{"b", 9}}, {{"c", 9}}}
	bands(t, srcs, func(t *testing.T, mk func([]msPair) *zset) {
		ops := []operand{{z: mk(srcs[0])}, {z: mk(srcs[1])}, {z: mk(srcs[2])}}
		eqScored(t, "diff", diff(ops), sm(msPair{"a", 1}))
	})
	// First source missing => empty.
	if got := diff([]operand{{z: nil}, {z: makeZ([]msPair{{"a", 1}})}}); got != nil {
		t.Fatalf("diff missing first = %s, want empty", fmtPairs(got))
	}
	// Later missing sources are skipped, not treated as matching.
	ops := []operand{{z: makeZ([]msPair{{"a", 1}, {"b", 2}})}, {z: nil}, {z: makeZ([]msPair{{"b", 9}})}}
	eqScored(t, "skip-nil", diff(ops), sm(msPair{"a", 1}))
}

// TestIntercard checks the count and the LIMIT early-stop against a full
// intersection count, at both bands.
func TestIntercard(t *testing.T) {
	srcs := [][]msPair{
		{{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}},
		{{"a", 1}, {"b", 2}, {"c", 3}, {"e", 5}},
	}
	bands(t, srcs, func(t *testing.T, mk func([]msPair) *zset) {
		ops := []operand{{z: mk(srcs[0])}, {z: mk(srcs[1])}}
		if got := intercard(ops, 0); got != 3 {
			t.Fatalf("intercard unlimited = %d, want 3", got)
		}
		if got := intercard(ops, 2); got != 2 {
			t.Fatalf("intercard limit 2 = %d, want 2", got)
		}
		if got := intercard(ops, 10); got != 3 {
			t.Fatalf("intercard limit 10 = %d, want 3", got)
		}
	})
	// A missing source yields zero.
	if got := intercard([]operand{{z: makeZ([]msPair{{"a", 1}})}, {z: nil}}, 0); got != 0 {
		t.Fatalf("intercard missing = %d, want 0", got)
	}
}

// TestIntercardZeroAlloc proves the probe loop allocates nothing on the count
// path, the lab's no-materialization claim for ZINTERCARD.
func TestIntercardZeroAlloc(t *testing.T) {
	var a, b []msPair
	for i := 0; i < 500; i++ {
		a = append(a, msPair{fmt.Sprintf("m%04d", i), float64(i)})
		if i%2 == 0 {
			b = append(b, msPair{fmt.Sprintf("m%04d", i), float64(i)})
		}
	}
	ops := []operand{{z: makeNativeZ(a)}, {z: makeNativeZ(b)}}
	if got := intercard(ops, 0); got != 250 {
		t.Fatalf("intercard = %d, want 250", got)
	}
	allocs := testing.AllocsPerRun(200, func() {
		_ = intercard(ops, 0)
	})
	if allocs != 0 {
		t.Fatalf("intercard allocated %v times per run, want 0", allocs)
	}
}

// TestScratchBudgetSwitchIsBytesNotTime documents the lab-05 rule that the union
// driver switches accumulation structure on the scratch byte budget, not on the
// time crossover the lab left to the M2 gate box. The two are different numbers
// on different axes (a pair count versus a result cardinality), and only the byte
// budget drives union(); this test is the sole reader of the recorded crossover.
func TestScratchBudgetSwitchIsBytesNotTime(t *testing.T) {
	budgetPairs := mergeRunBudget / runEntryBytes
	if budgetPairs <= 0 || hashMergeTimeCrossover <= 0 {
		t.Fatal("switch constants must be positive")
	}
	if budgetPairs == hashMergeTimeCrossover {
		t.Fatalf("byte-budget switch (%d pairs) must not be conflated with the time crossover (%d)", budgetPairs, hashMergeTimeCrossover)
	}
}

// TestBuildDestBandLadder checks the destination band is chosen from the result's
// own cardinality and max member length (section 4, line 348), and that an empty
// result builds nil (a destination delete).
func TestBuildDestBandLadder(t *testing.T) {
	if got := buildDest(nil); got != nil {
		t.Fatalf("empty result built %v, want nil", got)
	}
	// Small and short => inline band, in the given score order.
	small := buildDest(sm(msPair{"a", 1}, msPair{"b", 2}))
	if small.enc != encListpack {
		t.Fatalf("small result enc = %v, want inline", small.enc)
	}
	if small.card() != 2 {
		t.Fatalf("small card = %d, want 2", small.card())
	}
	if s, ok := small.score([]byte("b")); !ok || s != 2 {
		t.Fatalf("small member b = %v,%v want 2,true", s, ok)
	}
	// Over the entry cap => native band.
	var many []scoredMember
	for i := 0; i <= maxListpackEntries; i++ {
		many = append(many, scoredMember{member: []byte(fmt.Sprintf("m%04d", i)), score: float64(i)})
	}
	sortByScore(many)
	big := buildDest(many)
	if big.enc != encSkiplist {
		t.Fatalf("big result enc = %v, want native", big.enc)
	}
	if big.card() != len(many) {
		t.Fatalf("big card = %d, want %d", big.card(), len(many))
	}
	// A long member forces the native band even with few entries.
	long := make([]byte, maxListpackValue+1)
	for i := range long {
		long[i] = 'x'
	}
	ln := buildDest([]scoredMember{{member: long, score: 1}})
	if ln.enc != encSkiplist {
		t.Fatalf("long-member result enc = %v, want native", ln.enc)
	}
	if s, ok := ln.score(long); !ok || s != 1 {
		t.Fatalf("long member = %v,%v want 1,true", s, ok)
	}
}
