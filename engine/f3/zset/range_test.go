package zset

import (
	"math/rand/v2"
	"sort"
	"strconv"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
)

// decodeBulks splits a RESP bulk-string run (the body rangeByIndex streams after
// the array header) into its string elements.
func decodeBulks(t *testing.T, b []byte) []string {
	t.Helper()
	var out []string
	for i := 0; i < len(b); {
		if b[i] != '$' {
			t.Fatalf("expected bulk marker at %d, got %q", i, b[i])
		}
		j := i + 1
		for j < len(b) && b[j] != '\r' {
			j++
		}
		n, err := strconv.Atoi(string(b[i+1 : j]))
		if err != nil {
			t.Fatalf("bad bulk length %q: %v", b[i+1:j], err)
		}
		j += 2
		out = append(out, string(b[j:j+n]))
		i = j + n + 2
	}
	return out
}

// rangeStrings runs z.rangeByIndex over the clamped window and returns the
// decoded element strings (member, or member then score when withScores), the
// same shape the wire reply carries after its array header.
func rangeStrings(t *testing.T, z *zset, start, stop int, rev, withScores bool) []string {
	lo, hi, empty := clampRange(start, stop, z.card())
	if empty {
		return nil
	}
	return decodeBulks(t, z.rangeByIndex(nil, lo, hi, rev, withScores, false))
}

// modelRange is the reference: the sorted model sliced by Redis index semantics,
// flattened to the same element strings rangeStrings yields.
func modelRange(model []pairMS, start, stop int, rev, withScores bool) []string {
	seq := make([]pairMS, len(model))
	copy(seq, model)
	sort.Slice(seq, func(i, j int) bool { return lessStr(seq[i].s, seq[i].m, seq[j].s, seq[j].m) })
	if rev {
		for a, b := 0, len(seq)-1; a < b; a, b = a+1, b-1 {
			seq[a], seq[b] = seq[b], seq[a]
		}
	}
	lo, hi, empty := clampRange(start, stop, len(seq))
	if empty {
		return nil
	}
	var out []string
	for j := lo; j <= hi; j++ {
		out = append(out, seq[j].m)
		if withScores {
			out = append(out, string(resp.FormatScore(nil, seq[j].s)))
		}
	}
	return out
}

type pairMS struct {
	m string
	s float64
}

func sortedModel(model map[string]float64) []pairMS {
	out := make([]pairMS, 0, len(model))
	for m, s := range model {
		out = append(out, pairMS{m, s})
	}
	sort.Slice(out, func(i, j int) bool { return lessStr(out[i].s, out[i].m, out[j].s, out[j].m) })
	return out
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRangeOracleChurn drives random churn and, at every step, compares ZRANGE
// by index against a sorted model: the whole set both directions, a random
// window both directions, and the same with WITHSCORES. Two member spaces keep
// one run inline and push the other through the promotion into the native band,
// so the streaming seek-and-walk and the inline slice are both checked, tied
// score bands included (the score space is small enough to force member
// tiebreaks).
func TestRangeOracleChurn(t *testing.T) {
	for _, space := range []int{16, 400} {
		space := space
		t.Run(map[bool]string{true: "inline", false: "crosses"}[space <= 100], func(t *testing.T) {
			rng := rand.New(rand.NewPCG(3, uint64(space)))
			z := newZset()
			model := map[string]float64{}
			member := func() string { return "m" + itoa(rng.IntN(space)) }

			check := func(step int) {
				sm := sortedModel(model)
				card := len(sm)
				windows := [][2]int{{0, -1}, {0, 0}, {-1, -1}}
				if card > 0 {
					a := rng.IntN(card)
					b := rng.IntN(card)
					windows = append(windows, [2]int{a, b}, [2]int{-a - 1, -1}, [2]int{a, card + 5})
				}
				for _, w := range windows {
					for _, rev := range []bool{false, true} {
						for _, ws := range []bool{false, true} {
							got := rangeStrings(t, z, w[0], w[1], rev, ws)
							want := modelRange(sm, w[0], w[1], rev, ws)
							if !eqStrings(got, want) {
								t.Fatalf("step %d enc %s window %v rev=%v ws=%v:\n got %v\nwant %v",
									step, z.enc, w, rev, ws, got, want)
							}
						}
					}
				}
			}

			for step := 0; step < 3000; step++ {
				m := member()
				switch rng.IntN(3) {
				case 0:
					s := float64(rng.IntN(8) - 4)
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
				if step%50 == 0 {
					check(step)
				}
			}
			check(9999)
		})
	}
}

// TestRangeInlineNativeAgreement builds the same 100 entries as an inline band
// and as a forced native band and checks every range window agrees across the
// promotion boundary, both directions and with or without scores. This is the
// agreement the promotion must preserve: the streamed native reply is byte
// identical to the inline slice for the same data.
func TestRangeInlineNativeAgreement(t *testing.T) {
	const n = 100
	rng := rand.New(rand.NewPCG(5, 5))
	inline := newZset()
	model := map[string]float64{}
	for i := 0; i < n; i++ {
		m := "member:" + pad(i)
		s := float64(rng.IntN(20) - 10) // small score space forces tied bands
		inline.update([]byte(m), s, flags{})
		model[m] = s
	}
	if inline.enc != encListpack {
		t.Fatalf("inline band promoted early, enc = %s", inline.enc)
	}

	// Force the same data into a native band via the sorted append path.
	sm := sortedModel(model)
	native := newZset()
	native.nat = newNativeStore(n)
	native.enc = encSkiplist
	for _, p := range sm {
		native.nat.appendSorted([]byte(p.m), p.s)
	}
	native.nat.seal()

	if inline.card() != native.card() {
		t.Fatalf("cards differ: inline %d native %d", inline.card(), native.card())
	}
	windows := [][2]int{{0, -1}, {0, 9}, {40, 60}, {-10, -1}, {-100, 100}, {90, 200}, {50, 40}}
	for _, w := range windows {
		for _, rev := range []bool{false, true} {
			for _, ws := range []bool{false, true} {
				gi := rangeStrings(t, inline, w[0], w[1], rev, ws)
				gn := rangeStrings(t, native, w[0], w[1], rev, ws)
				if !eqStrings(gi, gn) {
					t.Fatalf("window %v rev=%v ws=%v: inline %v vs native %v", w, rev, ws, gi, gn)
				}
				// And both match the model.
				want := modelRange(sm, w[0], w[1], rev, ws)
				if !eqStrings(gi, want) {
					t.Fatalf("window %v rev=%v ws=%v: inline %v vs model %v", w, rev, ws, gi, want)
				}
			}
		}
	}

	// Ranks agree member for member across the boundary.
	for _, p := range sm {
		ri, _, _ := inline.rank([]byte(p.m))
		rn, _, _ := native.rank([]byte(p.m))
		if ri != rn {
			t.Fatalf("rank(%q): inline %d native %d", p.m, ri, rn)
		}
	}
}

// TestRangeNativeFarSeek checks a far forward window on the native band returns
// the exact ranks, so the counted select seeks rather than walking from the
// front, and that a reverse window returns the mirror image.
func TestRangeNativeFarSeek(t *testing.T) {
	const n = 5000
	z := buildNative(n)
	// buildNative uses "member:"+pad(i) at score i, so rank i is member i.
	got := rangeStrings(t, z, 4000, 4004, false, false)
	want := []string{}
	for i := 4000; i <= 4004; i++ {
		want = append(want, "member:"+pad(i))
	}
	if !eqStrings(got, want) {
		t.Fatalf("far forward window = %v, want %v", got, want)
	}
	// ZREVRANGE 0..4 is the top five in descending order.
	gotRev := rangeStrings(t, z, 0, 4, true, false)
	wantRev := []string{}
	for i := n - 1; i >= n-5; i-- {
		wantRev = append(wantRev, "member:"+pad(i))
	}
	if !eqStrings(gotRev, wantRev) {
		t.Fatalf("reverse top window = %v, want %v", gotRev, wantRev)
	}
}
