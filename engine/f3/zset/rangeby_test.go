package zset

import (
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
)

// The bound grammars parse exactly what Redis accepts (spec 2064/f3/12 section
// 6.5): every special form of a score bound and a lex bound, and the rejects.

func TestParseScoreBound(t *testing.T) {
	cases := []struct {
		in    string
		val   float64
		ex    bool
		valid bool
	}{
		{"1", 1, false, true},
		{"(1", 1, true, true},
		{"-2.5", -2.5, false, true},
		{"(-2.5", -2.5, true, true},
		{"inf", math.Inf(1), false, true},
		{"+inf", math.Inf(1), false, true},
		{"-inf", math.Inf(-1), false, true},
		{"(+inf", math.Inf(1), true, true},
		{"(-inf", math.Inf(-1), true, true},
		{"(", 0, false, false},
		{"", 0, false, false},
		{"nan", 0, false, false},
		{"(nan", 0, false, false},
		{"abc", 0, false, false},
		{"[1", 0, false, false},
	}
	for _, c := range cases {
		got, ok := parseScoreBound([]byte(c.in))
		if ok != c.valid {
			t.Errorf("parseScoreBound(%q) valid=%v, want %v", c.in, ok, c.valid)
			continue
		}
		if ok && (got.value != c.val || got.exclusive != c.ex) {
			t.Errorf("parseScoreBound(%q) = %v,%v want %v,%v", c.in, got.value, got.exclusive, c.val, c.ex)
		}
	}
}

func TestParseLexBound(t *testing.T) {
	cases := []struct {
		in    string
		inf   int
		val   string
		ex    bool
		valid bool
	}{
		{"-", lexNegInf, "", false, true},
		{"+", lexPosInf, "", false, true},
		{"[a", lexFinite, "a", false, true},
		{"(a", lexFinite, "a", true, true},
		{"[", lexFinite, "", false, true},
		{"(", lexFinite, "", true, true},
		{"[+", lexFinite, "+", false, true},
		{"(-", lexFinite, "-", true, true},
		{"[a b", lexFinite, "a b", false, true},
		{"--", 0, "", false, false},
		{"+a", 0, "", false, false},
		{"a", 0, "", false, false},
		{"", 0, "", false, false},
	}
	for _, c := range cases {
		got, ok := parseLexBound([]byte(c.in))
		if ok != c.valid {
			t.Errorf("parseLexBound(%q) valid=%v, want %v", c.in, ok, c.valid)
			continue
		}
		if ok && (got.inf != c.inf || string(got.value) != c.val || got.exclusive != c.ex) {
			t.Errorf("parseLexBound(%q) = %d/%q/%v want %d/%q/%v",
				c.in, got.inf, got.value, got.exclusive, c.inf, c.val, c.ex)
		}
	}
}

// applyLimit is the LIMIT arithmetic every by-bound range shares: it turns a
// forward-rank window into the inclusive [a,b] to emit, honoring offset, count,
// direction, and the Redis edge cases (negative offset empty, zero count empty,
// negative count unbounded).
func TestApplyLimit(t *testing.T) {
	cases := []struct {
		lo, hiExcl    int
		rev, limit    bool
		offset, count int
		a, b          int
		empty         bool
	}{
		{2, 8, false, false, 0, 0, 2, 7, false},  // no limit, whole window
		{2, 2, false, false, 0, 0, 0, 0, true},   // empty window
		{2, 8, false, true, 0, 3, 2, 4, false},   // offset 0 count 3
		{2, 8, false, true, 2, 3, 4, 6, false},   // offset 2 count 3
		{2, 8, false, true, 2, -1, 4, 7, false},  // negative count, all remaining
		{2, 8, false, true, 0, 100, 2, 7, false}, // count past end clamps
		{2, 8, false, true, 10, 3, 0, 0, true},   // offset past end
		{2, 8, false, true, -1, 3, 0, 0, true},   // negative offset empty
		{2, 8, false, true, 0, 0, 0, 0, true},    // zero count empty
		{2, 8, true, false, 0, 0, 2, 7, false},   // rev whole window
		{2, 8, true, true, 0, 3, 5, 7, false},    // rev offset 0 count 3 = top three
		{2, 8, true, true, 2, 3, 3, 5, false},    // rev offset 2 count 3
		{2, 8, true, true, 2, -1, 2, 5, false},   // rev negative count
		{2, 8, true, true, 10, 3, 0, 0, true},    // rev offset past end
	}
	for _, c := range cases {
		a, b, empty := applyLimit(c.lo, c.hiExcl, c.rev, c.limit, c.offset, c.count)
		if empty != c.empty || (!empty && (a != c.a || b != c.b)) {
			t.Errorf("applyLimit(%d,%d,rev=%v,limit=%v,%d,%d) = %d,%d,%v want %d,%d,%v",
				c.lo, c.hiExcl, c.rev, c.limit, c.offset, c.count, a, b, empty, c.a, c.b, c.empty)
		}
	}
}

// byScoreStrings runs a ZRANGEBYSCORE-shaped query at the band level and decodes
// the streamed elements, the same bytes the wire reply carries after its header.
func byScoreStrings(t *testing.T, z *zset, min, max scoreBound, rev, ws, limit bool, offset, count int) []string {
	t.Helper()
	lo, hiExcl := z.scoreWindow(min, max)
	a, b, empty := applyLimit(lo, hiExcl, rev, limit, offset, count)
	if empty {
		return nil
	}
	return decodeBulks(t, z.rangeByRankWindow(nil, a, b, rev, ws))
}

func byLexStrings(t *testing.T, z *zset, min, max lexBound, rev, limit bool, offset, count int) []string {
	t.Helper()
	lo, hiExcl := z.lexWindow(min, max)
	a, b, empty := applyLimit(lo, hiExcl, rev, limit, offset, count)
	if empty {
		return nil
	}
	return decodeBulks(t, z.rangeByRankWindow(nil, a, b, rev, false))
}

func inScore(s float64, min, max scoreBound) bool {
	if min.exclusive {
		if !(s > min.value) {
			return false
		}
	} else if !(s >= min.value) {
		return false
	}
	if max.exclusive {
		return s < max.value
	}
	return s <= max.value
}

func inLex(m string, min, max lexBound) bool {
	switch min.inf {
	case lexPosInf:
		return false
	case lexFinite:
		c := strings.Compare(m, string(min.value))
		if min.exclusive {
			if c <= 0 {
				return false
			}
		} else if c < 0 {
			return false
		}
	}
	switch max.inf {
	case lexNegInf:
		return false
	case lexFinite:
		c := strings.Compare(m, string(max.value))
		if max.exclusive {
			return c < 0
		}
		return c <= 0
	}
	return true
}

// modelByScore is the reference for a ZRANGEBYSCORE query: filter the sorted
// model by the score band, reverse when asked, then apply LIMIT the Redis way.
func modelByScore(sm []pairMS, min, max scoreBound, rev, ws, limit bool, offset, count int) []string {
	var in []pairMS
	for _, p := range sm {
		if inScore(p.s, min, max) {
			in = append(in, p)
		}
	}
	return flattenModel(in, rev, ws, limit, offset, count)
}

func modelByLex(sm []pairMS, min, max lexBound, rev, limit bool, offset, count int) []string {
	var in []pairMS
	for _, p := range sm {
		if inLex(p.m, min, max) {
			in = append(in, p)
		}
	}
	return flattenModel(in, rev, false, limit, offset, count)
}

func flattenModel(in []pairMS, rev, ws, limit bool, offset, count int) []string {
	if rev {
		for a, b := 0, len(in)-1; a < b; a, b = a+1, b-1 {
			in[a], in[b] = in[b], in[a]
		}
	}
	if limit {
		if offset < 0 {
			in = nil
		} else if offset >= len(in) {
			in = nil
		} else {
			in = in[offset:]
			// count < 0 keeps all remaining; count >= 0 clamps the tail.
			if count >= 0 && count < len(in) {
				in = in[:count]
			}
			if count == 0 {
				in = nil
			}
		}
	}
	var out []string
	for _, p := range in {
		out = append(out, p.m)
		if ws {
			out = append(out, string(resp.FormatScore(nil, p.s)))
		}
	}
	return out
}

// TestRangeByScoreOracleChurn drives churn and compares every ZRANGEBYSCORE and
// ZREVRANGEBYSCORE window, with and without WITHSCORES and LIMIT, against the
// model. Two member spaces keep one run inline and push the other native, and
// the small score space forces tied bands so exclusive bounds and the member
// tie-break are exercised.
func TestRangeByScoreOracleChurn(t *testing.T) {
	for _, space := range []int{16, 400} {
		space := space
		t.Run(map[bool]string{true: "inline", false: "crosses"}[space <= 100], func(t *testing.T) {
			rng := rand.New(rand.NewPCG(21, uint64(space)))
			z := newZset()
			model := map[string]float64{}
			member := func() string { return "m" + itoa(rng.IntN(space)) }

			bounds := func() (scoreBound, scoreBound) {
				pick := func() scoreBound {
					switch rng.IntN(6) {
					case 0:
						return scoreBound{value: math.Inf(-1)}
					case 1:
						return scoreBound{value: math.Inf(1)}
					default:
						return scoreBound{value: float64(rng.IntN(12) - 6), exclusive: rng.IntN(2) == 0}
					}
				}
				return pick(), pick()
			}

			check := func(step int) {
				sm := sortedModel(model)
				for t2 := 0; t2 < 6; t2++ {
					min, max := bounds()
					for _, rev := range []bool{false, true} {
						for _, ws := range []bool{false, true} {
							limit := rng.IntN(2) == 0
							offset := rng.IntN(5)
							count := rng.IntN(7) - 1
							got := byScoreStrings(t, z, min, max, rev, ws, limit, offset, count)
							want := modelByScore(sm, min, max, rev, ws, limit, offset, count)
							if !eqStrings(got, want) {
								t.Fatalf("step %d enc %s min=%v max=%v rev=%v ws=%v limit=%v/%d/%d:\n got %v\nwant %v",
									step, z.enc, min, max, rev, ws, limit, offset, count, got, want)
							}
						}
					}
					// ZCOUNT is the window width.
					lo, hiExcl := z.scoreWindow(min, max)
					wantCount := 0
					for _, p := range sm {
						if inScore(p.s, min, max) {
							wantCount++
						}
					}
					if hiExcl-lo != wantCount {
						t.Fatalf("step %d ZCOUNT min=%v max=%v = %d, want %d", step, min, max, hiExcl-lo, wantCount)
					}
				}
			}

			for step := 0; step < 3000; step++ {
				m := member()
				switch rng.IntN(3) {
				case 0:
					s := float64(rng.IntN(12) - 6)
					z.update([]byte(m), s, flags{})
					model[m] = s
				case 1:
					d := float64(rng.IntN(6) - 3)
					z.update([]byte(m), d, flags{incr: true})
					model[m] = model[m] + d
				case 2:
					z.rem([]byte(m))
					delete(model, m)
				}
				if step%40 == 0 {
					check(step)
				}
			}
			check(9999)
		})
	}
}

// TestRangeByLexOracleChurn drives churn at a single score so the whole zset is
// one tied band, the only shape ZRANGEBYLEX is defined for (section 3.2), and
// compares every lex window and ZLEXCOUNT against the model. One space stays
// inline, the other crosses into the native tree where the band is keyed by
// member bytes alone.
func TestRangeByLexOracleChurn(t *testing.T) {
	for _, space := range []int{16, 400} {
		space := space
		t.Run(map[bool]string{true: "inline", false: "crosses"}[space <= 100], func(t *testing.T) {
			rng := rand.New(rand.NewPCG(22, uint64(space)))
			z := newZset()
			model := map[string]float64{}
			member := func() string { return "k" + itoa(rng.IntN(space)) }

			bound := func() lexBound {
				switch rng.IntN(5) {
				case 0:
					return lexBound{inf: lexNegInf}
				case 1:
					return lexBound{inf: lexPosInf}
				default:
					return lexBound{value: []byte("k" + itoa(rng.IntN(space))), exclusive: rng.IntN(2) == 0}
				}
			}

			check := func(step int) {
				sm := sortedModel(model)
				for t2 := 0; t2 < 8; t2++ {
					min, max := bound(), bound()
					for _, rev := range []bool{false, true} {
						limit := rng.IntN(2) == 0
						offset := rng.IntN(5)
						count := rng.IntN(7) - 1
						got := byLexStrings(t, z, min, max, rev, limit, offset, count)
						want := modelByLex(sm, min, max, rev, limit, offset, count)
						if !eqStrings(got, want) {
							t.Fatalf("step %d enc %s min=%v max=%v rev=%v limit=%v/%d/%d:\n got %v\nwant %v",
								step, z.enc, min, max, rev, limit, offset, count, got, want)
						}
					}
					lo, hiExcl := z.lexWindow(min, max)
					wantCount := 0
					for _, p := range sm {
						if inLex(p.m, min, max) {
							wantCount++
						}
					}
					if hiExcl-lo != wantCount {
						t.Fatalf("step %d ZLEXCOUNT min=%v max=%v = %d, want %d", step, min, max, hiExcl-lo, wantCount)
					}
				}
			}

			for step := 0; step < 3000; step++ {
				m := member()
				switch rng.IntN(2) {
				case 0:
					z.update([]byte(m), 0, flags{}) // one tied band at score 0
					model[m] = 0
				case 1:
					z.rem([]byte(m))
					delete(model, m)
				}
				if step%40 == 0 {
					check(step)
				}
			}
			check(9999)
		})
	}
}

// TestRangeByBoundInlineNativeAgreement builds identical data as an inline band
// and as a forced native band and checks the score and lex windows agree byte
// for byte across the promotion boundary, both directions and with LIMIT, so the
// streamed native reply matches the inline slice for the same data.
func TestRangeByBoundInlineNativeAgreement(t *testing.T) {
	const n = 100
	rng := rand.New(rand.NewPCG(23, 23))
	inline := newZset()
	model := map[string]float64{}
	for i := 0; i < n; i++ {
		m := "member:" + pad(i)
		s := float64(rng.IntN(12) - 6)
		inline.update([]byte(m), s, flags{})
		model[m] = s
	}
	if inline.enc != encListpack {
		t.Fatalf("inline band promoted early, enc = %s", inline.enc)
	}
	sm := sortedModel(model)
	native := newZset()
	native.nat = newNativeStore(n)
	native.enc = encSkiplist
	for _, p := range sm {
		native.nat.appendSorted([]byte(p.m), p.s)
	}
	native.nat.seal()

	scoreBounds := []scoreBound{
		{value: math.Inf(-1)}, {value: math.Inf(1)},
		{value: -3}, {value: -3, exclusive: true},
		{value: 0}, {value: 2, exclusive: true}, {value: 5},
	}
	for _, min := range scoreBounds {
		for _, max := range scoreBounds {
			for _, rev := range []bool{false, true} {
				for _, ws := range []bool{false, true} {
					for _, lim := range [][3]int{{0, 0, -1}, {1, 2, 3}, {1, 5, 2}} {
						limit := lim[0] == 1
						gi := byScoreStrings(t, inline, min, max, rev, ws, limit, lim[1], lim[2])
						gn := byScoreStrings(t, native, min, max, rev, ws, limit, lim[1], lim[2])
						if !eqStrings(gi, gn) {
							t.Fatalf("byscore min=%v max=%v rev=%v ws=%v lim=%v: inline %v vs native %v",
								min, max, rev, ws, lim, gi, gn)
						}
						want := modelByScore(sm, min, max, rev, ws, limit, lim[1], lim[2])
						if !eqStrings(gi, want) {
							t.Fatalf("byscore min=%v max=%v rev=%v ws=%v lim=%v: inline %v vs model %v",
								min, max, rev, ws, lim, gi, want)
						}
					}
				}
			}
		}
	}
}

// TestRangeByScoreFarSeek checks a narrow score band deep in a large native set
// returns exactly its members: buildNative seats member i at score i, so the
// band [lo,hi] is members lo..hi, reached by a counted seek not a front scan.
func TestRangeByScoreFarSeek(t *testing.T) {
	const n = 5000
	z := buildNative(n)
	min := scoreBound{value: 4000}
	max := scoreBound{value: 4009}
	got := byScoreStrings(t, z, min, max, false, false, false, 0, 0)
	var want []string
	for i := 4000; i <= 4009; i++ {
		want = append(want, "member:"+pad(i))
	}
	if !eqStrings(got, want) {
		t.Fatalf("far score band = %v, want %v", got, want)
	}
	// Exclusive both ends drops the two endpoints.
	gotEx := byScoreStrings(t, z, scoreBound{value: 4000, exclusive: true}, scoreBound{value: 4009, exclusive: true}, false, false, false, 0, 0)
	if !eqStrings(gotEx, want[1:len(want)-1]) {
		t.Fatalf("exclusive far score band = %v, want %v", gotEx, want[1:len(want)-1])
	}
	// LIMIT pages the band.
	gotLim := byScoreStrings(t, z, min, max, false, false, true, 2, 3)
	if !eqStrings(gotLim, want[2:5]) {
		t.Fatalf("limited band = %v, want %v", gotLim, want[2:5])
	}
	// Reverse LIMIT pages from the top.
	gotRev := byScoreStrings(t, z, min, max, true, false, true, 0, 3)
	revWant := []string{want[9], want[8], want[7]}
	if !eqStrings(gotRev, revWant) {
		t.Fatalf("reverse limited band = %v, want %v", gotRev, revWant)
	}
}

// buildTiedNative builds a native band of n members all at score 0, one tied
// lex band, members "k0000".."kNNNN" in order.
func buildTiedNative(n int) *zset {
	z := newZset()
	z.nat = newNativeStore(n)
	z.enc = encSkiplist
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "k" + pad(i)
	}
	sort.Strings(keys)
	for _, k := range keys {
		z.nat.appendSorted([]byte(k), 0)
	}
	z.nat.seal()
	return z
}

// TestRangeByLexFarSeek checks a lex band in a large tied native set returns
// exactly the members in the byte range, with the inclusive and exclusive edges.
func TestRangeByLexFarSeek(t *testing.T) {
	const n = 5000
	z := buildTiedNative(n)
	min := lexBound{value: []byte("k" + pad(4000))}
	max := lexBound{value: []byte("k" + pad(4009))}
	got := byLexStrings(t, z, min, max, false, false, 0, 0)
	var want []string
	for i := 4000; i <= 4009; i++ {
		want = append(want, "k"+pad(i))
	}
	if !eqStrings(got, want) {
		t.Fatalf("far lex band = %v, want %v", got, want)
	}
	gotEx := byLexStrings(t, z, lexBound{value: []byte("k" + pad(4000)), exclusive: true}, max, false, false, 0, 0)
	if !eqStrings(gotEx, want[1:]) {
		t.Fatalf("exclusive-low lex band = %v, want %v", gotEx, want[1:])
	}
	// The whole band via the lex infinities.
	full := byLexStrings(t, z, lexBound{inf: lexNegInf}, lexBound{inf: lexPosInf}, false, false, 0, 0)
	if len(full) != n {
		t.Fatalf("full lex band len = %d, want %d", len(full), n)
	}
}
