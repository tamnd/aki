package hash

import (
	"math/rand/v2"
	"strconv"
	"testing"
)

// The HRANDFIELD draw kernel tests (spec 2064/f3/10 section 7.4), the hash twin of
// zset/draw_test.go. They pin the exact-uniform claim (F15) the count forms rest
// on and cover both the native field table and the inline blob's at-by-position
// walk. Seeds are fixed, so these are stable property checks, not coin flips.

// newReg builds a bare hash registry with a fixed PRNG stream, so the draw tests
// are deterministic without dragging in a shard Ctx.
func newReg(seed uint64) *reg {
	return &reg{m: map[string]*hash{}, rng: *rand.NewPCG(seed, 0x9e3779b97f4a7c15)}
}

// pairsN returns n field-value pairs, field "f<i>" to value "v<i>", so a drawn
// field decodes back to its build index through fieldIndex.
func pairsN(n int) [][2]string {
	p := make([][2]string, n)
	for i := range p {
		p[i] = [2]string{"f" + strconv.Itoa(i), "v" + strconv.Itoa(i)}
	}
	return p
}

// fieldIndex parses the trailing integer of an "f<i>" field name.
func fieldIndex(f []byte) int {
	n := 0
	for i := 1; i < len(f); i++ {
		n = n*10 + int(f[i]-'0')
	}
	return n
}

// TestNextUniform checks the draw kernel (reg.next, Lemire with rejection) is flat
// across its range: a large sample binned by value must sit under the chi-squared
// bound for its degrees of freedom, or the exact-uniform F15 claim HRANDFIELD
// rests on is false.
func TestNextUniform(t *testing.T) {
	const bins = 64
	const draws = bins * 2000
	g := newReg(1)
	counts := make([]int, bins)
	for i := 0; i < draws; i++ {
		counts[g.next(bins)]++
	}
	if chi := chiSquared(counts, float64(draws)/float64(bins)); chi > 116.3 {
		// 116.3 is the chi-squared 0.999 critical value at 63 dof.
		t.Fatalf("chi-squared = %.1f over %d bins, want < 116.3 (kernel is skewed)", chi, bins)
	}
}

// TestRandWithReplacementUniform draws with repetition over a native hash and bins
// the drawn fields: the whole path (rank draw then vector index) must stay
// uniform, not just the kernel in isolation.
func TestRandWithReplacementUniform(t *testing.T) {
	const card = 64
	const draws = card * 2000
	h := buildNative(pairsN(card))
	g := newReg(2)
	counts := make([]int, card)
	h.randWithReplacement(g, draws, func(f, _ []byte) { counts[fieldIndex(f)]++ })
	if chi := chiSquared(counts, float64(draws)/float64(card)); chi > 116.3 {
		t.Fatalf("with-replacement chi-squared = %.1f, want < 116.3", chi)
	}
}

// TestRandDistinctNoRepeats checks a positive-count draw returns distinct fields
// paired with their own values, and at or above the cardinality the whole hash.
// Spanning the crossover exercises both the small-sample rejection path and the
// large-sample shuffle path.
func TestRandDistinctNoRepeats(t *testing.T) {
	const card = 4000 // above the inline cap and above smallSampleCap
	h := buildNative(pairsN(card))
	g := newReg(3)
	for _, want := range []int{1, 10, smallSampleCap, smallSampleCap + 1, 3000, card, card + 100} {
		seen := map[string]bool{}
		h.randDistinct(g, want, func(f, v []byte) {
			s := string(f)
			if seen[s] {
				t.Fatalf("want=%d: field %q drawn twice", want, s)
			}
			seen[s] = true
			// Each field must ride back with its own value, not a mismatched one.
			if got, exp := string(v), "v"+strconv.Itoa(fieldIndex(f)); got != exp {
				t.Fatalf("want=%d: field %q paired with %q, want %q", want, s, got, exp)
			}
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
// inline blob, so the non-native at-by-position walk is covered too.
func TestDrawInlineBand(t *testing.T) {
	h := buildInline(pairsN(40))
	if h.enc != encListpack {
		t.Fatalf("enc = %s, want listpack", h.enc)
	}
	g := newReg(4)
	seen := map[string]bool{}
	h.randDistinct(g, 25, func(f, v []byte) {
		if got, exp := string(v), "v"+strconv.Itoa(fieldIndex(f)); got != exp {
			t.Fatalf("inline field %q paired with %q, want %q", f, got, exp)
		}
		seen[string(f)] = true
	})
	if len(seen) != 25 {
		t.Fatalf("inline distinct drew %d, want 25", len(seen))
	}
	got := 0
	h.randWithReplacement(g, 100, func([]byte, []byte) { got++ })
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
