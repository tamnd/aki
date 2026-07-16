package zset

import (
	"math/rand/v2"
	"testing"
)

// newReg builds a bare registry with a fixed PRNG stream, so the draw tests are
// deterministic without dragging in a shard Ctx.
func newReg(seed uint64) *reg {
	return &reg{m: map[string]*zset{}, rng: *rand.NewPCG(seed, 0x9e3779b97f4a7c15)}
}

// TestNextUniform checks the draw kernel (reg.next, Lemire with rejection) is
// flat across its range: a large sample binned by value must sit under the
// chi-squared bound for its degrees of freedom, or the "exactly uniform" F15
// claim ZRANDMEMBER rests on is false. The seed is fixed, so this is a stable
// property check, not a coin flip.
func TestNextUniform(t *testing.T) {
	const bins = 64
	const draws = bins * 2000
	g := newReg(1)
	counts := make([]int, bins)
	for i := 0; i < draws; i++ {
		counts[g.next(bins)]++
	}
	if chi := chiSquared(counts, float64(draws)/float64(bins)); chi > 116.3 {
		// 116.3 is the chi-squared 0.999 critical value at 63 dof; a uniform
		// kernel clears it with room, a biased one blows past it.
		t.Fatalf("chi-squared = %.1f over %d bins, want < 116.3 (kernel is skewed)", chi, bins)
	}
}

// TestRandWithReplacementUniform draws with repetition over a native zset and
// bins the drawn members: the whole path (rank draw then tree select) must stay
// uniform, not just the kernel in isolation.
func TestRandWithReplacementUniform(t *testing.T) {
	const card = 64
	const draws = card * 2000
	z := buildNative(card)
	g := newReg(2)
	counts := make([]int, card)
	z.randWithReplacement(g, draws, func(m []byte, _ uint64) {
		// buildNative seats member i at rank i, key member:<pad i>.
		counts[memberIndex(m)]++
	})
	if chi := chiSquared(counts, float64(draws)/float64(card)); chi > 116.3 {
		t.Fatalf("with-replacement chi-squared = %.1f, want < 116.3", chi)
	}
}

// TestRandDistinctNoRepeats checks a positive-count draw returns distinct members
// and, at or above the cardinality, the whole set in ascending order. Both the
// small-sample rejection path and the large-sample shuffle path are exercised by
// spanning the crossover.
func TestRandDistinctNoRepeats(t *testing.T) {
	const card = 4000 // above the native cap and above zsetSmallSampleCap
	z := buildNative(card)
	g := newReg(3)
	for _, want := range []int{1, 10, zsetSmallSampleCap, zsetSmallSampleCap + 1, 3000, card, card + 100} {
		seen := map[string]bool{}
		var last []byte
		full := want >= card
		z.randDistinct(g, want, func(m []byte, _ uint64) {
			s := string(m)
			if seen[s] {
				t.Fatalf("want=%d: member %q drawn twice", want, s)
			}
			seen[s] = true
			if full && last != nil && string(last) >= s {
				t.Fatalf("want=%d: full draw not ascending at %q after %q", want, s, last)
			}
			last = append(last[:0], m...)
		})
		exp := want
		if exp > card {
			exp = card
		}
		if len(seen) != exp {
			t.Fatalf("want=%d: drew %d distinct, want %d", want, len(seen), exp)
		}
	}
}

// TestDrawInlineBand runs the same distinct and with-replacement draws over the
// inline blob, so the non-native at-by-rank walk is covered too.
func TestDrawInlineBand(t *testing.T) {
	z := buildInline(40)
	if z.enc != encListpack {
		t.Fatalf("enc = %s, want listpack", z.enc)
	}
	g := newReg(4)
	seen := map[string]bool{}
	z.randDistinct(g, 25, func(m []byte, _ uint64) { seen[string(m)] = true })
	if len(seen) != 25 {
		t.Fatalf("inline distinct drew %d, want 25", len(seen))
	}
	got := 0
	z.randWithReplacement(g, 100, func([]byte, uint64) { got++ })
	if got != 100 {
		t.Fatalf("inline with-replacement drew %d, want 100", got)
	}
}

// chiSquared returns sum((observed-expected)^2/expected) over the bins.
func chiSquared(counts []int, expected float64) float64 {
	var chi float64
	for _, c := range counts {
		d := float64(c) - expected
		chi += d * d / expected
	}
	return chi
}

// memberIndex parses the trailing integer of a "member:<padded i>" key.
func memberIndex(m []byte) int {
	n := 0
	for i := len("member:"); i < len(m); i++ {
		n = n*10 + int(m[i]-'0')
	}
	return n
}
