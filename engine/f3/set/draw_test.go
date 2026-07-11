package set

import (
	"math/rand/v2"
	"strconv"
	"testing"
)

// The draw kernel tests (spec 2064/f3/11 sections 4.3, 5.2, 5.6). They prove two
// properties the gate turns on: exact uniformity (F15), checked with chi-squared
// frequency tests over many draws at inline and hashtable cardinalities, and the
// Redis exactness semantics (SPOP empties and returns the whole set at count >=
// card; SRANDMEMBER positive count is distinct, negative count is with
// replacement). The PRNG is seeded to a fixed stream so the statistics are
// deterministic and the tests never flake.

// newTestReg builds a registry on a fixed PCG stream, so a uniformity test is
// reproducible run to run.
func newTestReg() *reg {
	return &reg{m: map[string]*set{}, rng: *rand.NewPCG(0x243f6a8885a308d3, 0x13198a2e03707344)}
}

func setOfInts(n int) *set {
	s := newSet([]byte("0"))
	for i := 0; i < n; i++ {
		s.add([]byte(strconv.Itoa(i)))
	}
	return s
}

func setOfWords(n int) *set {
	s := newSet([]byte("w0"))
	for i := 0; i < n; i++ {
		s.add([]byte("w" + strconv.Itoa(i)))
	}
	return s
}

// chiSquare returns the Pearson statistic for observed counts against a uniform
// expectation of total/len(obs) per category. A generator that draws uniformly
// lands near len(obs)-1 (the degrees of freedom); a stuck or off-by-one draw
// blows it up far past the generous threshold the callers use.
func chiSquare(obs []int) float64 {
	total := 0
	for _, o := range obs {
		total += o
	}
	exp := float64(total) / float64(len(obs))
	var stat float64
	for _, o := range obs {
		d := float64(o) - exp
		stat += d * d / exp
	}
	return stat
}

// bandCases covers the three encodings the draw runs over, each below and above
// the inline caps, so uniformity is checked on the packed blobs and on the dense
// vector alike.
var bandCases = []struct {
	name string
	set  func(n int) *set
}{
	{"intset", setOfInts},
	{"listpack", setOfWords},
}

func TestDrawSingleUniform(t *testing.T) {
	cases := []struct {
		name string
		s    *set
		card int
	}{
		{"intset", setOfInts(50), 50},
		{"listpack", setOfWords(50), 50},
		{"hashtable", setOfWords(2000), 2000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := newTestReg()
			hits := map[string]int{}
			const perCat = 400
			iters := tc.card * perCat
			var sc [64]byte
			for i := 0; i < iters; i++ {
				hits[string(tc.s.drawOne(g, sc[:]))]++
			}
			if len(hits) != tc.card {
				t.Fatalf("drew %d distinct members, want all %d", len(hits), tc.card)
			}
			obs := make([]int, 0, tc.card)
			for _, h := range hits {
				obs = append(obs, h)
			}
			// Threshold at twice the degrees of freedom: a uniform draw sits near
			// df, so 2*df leaves wide slack yet still fails a biased generator.
			df := float64(tc.card - 1)
			if stat := chiSquare(obs); stat > 2*df {
				t.Fatalf("chi-squared %.1f over %d categories, want < %.1f", stat, tc.card, 2*df)
			}
		})
	}
}

// TestSpopSingleUniform pops one member from a fresh set on every trial and
// checks the popped member is uniform: the swap-remove must not bias which
// member the next draw can reach.
func TestSpopSingleUniform(t *testing.T) {
	for _, bc := range bandCases {
		t.Run(bc.name, func(t *testing.T) {
			const card, perCat = 40, 500
			g := newTestReg()
			hits := map[string]int{}
			var sc [64]byte
			for i := 0; i < card*perCat; i++ {
				s := bc.set(card)
				hits[string(s.popOne(g, sc[:]))]++
			}
			obs := make([]int, 0, card)
			for _, h := range hits {
				obs = append(obs, h)
			}
			if len(obs) != card {
				t.Fatalf("popped %d distinct members, want %d", len(obs), card)
			}
			df := float64(card - 1)
			if stat := chiSquare(obs); stat > 2*df {
				t.Fatalf("chi-squared %.1f, want < %.1f", stat, 2*df)
			}
		})
	}
}

// TestSpopCountUniform pops a count-sample and checks every member is equally
// likely to be in it (each swap-remove keeps the remaining draw uniform).
func TestSpopCountUniform(t *testing.T) {
	const card, want, trials = 40, 8, 6000
	g := newTestReg()
	hits := make([]int, card)
	var sc [64]byte
	for tr := 0; tr < trials; tr++ {
		s := setOfInts(card)
		for i := 0; i < want; i++ {
			m := s.popOne(g, sc[:])
			v, _ := strconv.Atoi(string(m))
			hits[v]++
		}
	}
	df := float64(card - 1)
	if stat := chiSquare(hits); stat > 2*df {
		t.Fatalf("chi-squared %.1f, want < %.1f", stat, 2*df)
	}
}

// TestSrandNegativeUniform draws with replacement and checks the draw is uniform
// over the whole population.
func TestSrandNegativeUniform(t *testing.T) {
	const card, draws = 50, 50 * 400
	g := newTestReg()
	s := setOfInts(card)
	hits := make([]int, card)
	s.sampleWithReplacement(g, draws, func(m []byte) {
		v, _ := strconv.Atoi(string(m))
		hits[v]++
	})
	df := float64(card - 1)
	if stat := chiSquare(hits); stat > 2*df {
		t.Fatalf("chi-squared %.1f, want < %.1f", stat, 2*df)
	}
}

// TestSrandPositiveInclusionUniform checks a distinct sample includes every
// member with equal probability, across both the small-sample rejection branch
// and the large-sample shuffle branch (a want above smallSampleCap).
func TestSrandPositiveInclusionUniform(t *testing.T) {
	cases := []struct{ card, want int }{
		{40, 8},                      // small-sample rejection
		{2000, smallSampleCap + 300}, // large-sample shuffle
	}
	for _, tc := range cases {
		t.Run(strconv.Itoa(tc.card)+"x"+strconv.Itoa(tc.want), func(t *testing.T) {
			g := newTestReg()
			s := setOfWords(tc.card)
			index := map[string]int{}
			s.each(func(m []byte) { index[string(m)] = len(index) })
			hits := make([]int, tc.card)
			trials := 60 * tc.card / tc.want
			for tr := 0; tr < trials; tr++ {
				seen := map[string]bool{}
				s.sampleDistinct(g, tc.want, func(m []byte) {
					k := string(m)
					if seen[k] {
						t.Fatalf("distinct sample repeated %q", k)
					}
					seen[k] = true
					hits[index[k]]++
				})
				if len(seen) != tc.want {
					t.Fatalf("distinct sample size %d, want %d", len(seen), tc.want)
				}
			}
			df := float64(tc.card - 1)
			if stat := chiSquare(hits); stat > 2*df {
				t.Fatalf("chi-squared %.1f, want < %.1f", stat, 2*df)
			}
		})
	}
}

// TestSrandPositiveWholeSet checks a distinct count at or above the cardinality
// returns exactly the whole set, once each.
func TestSrandPositiveWholeSet(t *testing.T) {
	for _, bc := range bandCases {
		t.Run(bc.name, func(t *testing.T) {
			g := newTestReg()
			s := bc.set(30)
			got := map[string]int{}
			s.sampleDistinct(g, 100, func(m []byte) { got[string(m)]++ })
			if len(got) != 30 {
				t.Fatalf("count >= card returned %d distinct, want 30", len(got))
			}
			for m, c := range got {
				if c != 1 {
					t.Fatalf("member %q returned %d times, want 1", m, c)
				}
			}
		})
	}
}

// TestSpopWholeSetAndEmpty checks SPOP count >= card returns the whole set and
// empties it (the full-shuffle branch of doc 11 section 5.2 followed by the key
// delete the command surface does).
func TestSpopWholeSetAndEmpty(t *testing.T) {
	g := newTestReg()
	s := setOfInts(30)
	got := map[string]bool{}
	var sc [64]byte
	popped := 0
	for s.card() > 0 && popped < 100 {
		got[string(s.popOne(g, sc[:]))] = true
		popped++
	}
	if popped != 30 {
		t.Fatalf("popped %d members from a 30-member set, want 30", popped)
	}
	if s.card() != 0 {
		t.Fatalf("set not empty after draining, card = %d", s.card())
	}
	if len(got) != 30 {
		t.Fatalf("drain covered %d distinct members, want 30", len(got))
	}
}

// TestSpopDrainCoversAllHashtable drains a hashtable-band set one pop at a time
// and checks the swap-remove keeps every member reachable exactly once.
func TestSpopDrainCoversAllHashtable(t *testing.T) {
	g := newTestReg()
	s := setOfWords(1500)
	if s.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", s.enc)
	}
	seen := map[string]bool{}
	var sc [64]byte
	for s.card() > 0 {
		m := string(s.popOne(g, sc[:]))
		if seen[m] {
			t.Fatalf("member %q popped twice", m)
		}
		seen[m] = true
	}
	if len(seen) != 1500 {
		t.Fatalf("drain covered %d members, want 1500", len(seen))
	}
}

// The single draw must not allocate (F7, doc 11 section 5.2): SRANDMEMBER single
// reads one vector slot and one record, all in place. Measured across the three
// bands; the result is consumed by length so the scratch never escapes.
func TestZeroAllocDrawSingle(t *testing.T) {
	cases := []struct {
		name string
		s    *set
	}{
		{"intset", setOfInts(300)},
		{"listpack", setOfWords(50)},
		{"hashtable", setOfWords(2000)},
	}
	for _, tc := range cases {
		g := newTestReg()
		checkZero(t, "draw "+tc.name, func() {
			var sc [64]byte
			sinkInt = len(tc.s.drawOne(g, sc[:]))
		})
	}
}
