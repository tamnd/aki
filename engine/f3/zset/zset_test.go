package zset

import (
	"math"
	"strconv"
	"testing"
)

// order drains a zset into parallel member and score slices in the zset total
// order, so a test can check both contents and ordering across bands.
func order(z *zset) ([]string, []float64) {
	ev := z.entries()
	ms := make([]string, len(ev))
	scs := make([]float64, len(ev))
	for i, e := range ev {
		ms[i] = string(e.member)
		scs[i] = e.score
	}
	return ms, scs
}

func mustScore(t *testing.T, z *zset, m string) float64 {
	t.Helper()
	s, ok := z.score([]byte(m))
	if !ok {
		t.Fatalf("score(%q) absent, want present", m)
	}
	return s
}

func TestInlineAddScoreOrder(t *testing.T) {
	z := newZset()
	add := map[string]float64{"c": 3, "a": 1, "b": 2, "d": 2}
	for m, s := range add {
		z.update([]byte(m), s, flags{})
	}
	if z.enc != encListpack {
		t.Fatalf("enc = %s, want listpack", z.enc)
	}
	if z.card() != 4 {
		t.Fatalf("card = %d, want 4", z.card())
	}
	ms, scs := order(z)
	// score ascending, ties (b and d at 2) broken by member bytes ascending.
	wantM := []string{"a", "b", "d", "c"}
	wantS := []float64{1, 2, 2, 3}
	for i := range wantM {
		if ms[i] != wantM[i] || scs[i] != wantS[i] {
			t.Fatalf("order = %v %v, want %v %v", ms, scs, wantM, wantS)
		}
	}
	if mustScore(t, z, "c") != 3 {
		t.Fatal("ZSCORE c should be 3")
	}
	if _, ok := z.score([]byte("zz")); ok {
		t.Fatal("absent member reports present")
	}
}

func TestInlineRescoreMovesPosition(t *testing.T) {
	z := newZset()
	for _, p := range []struct {
		m string
		s float64
	}{{"a", 1}, {"b", 2}, {"c", 3}} {
		z.update([]byte(p.m), p.s, flags{})
	}
	// Push a to the top.
	z.update([]byte("a"), 9, flags{})
	ms, _ := order(z)
	if ms[len(ms)-1] != "a" {
		t.Fatalf("after rescore order = %v, want a last", ms)
	}
	if mustScore(t, z, "a") != 9 {
		t.Fatal("rescore did not update the score")
	}
	if z.card() != 3 {
		t.Fatalf("rescore changed cardinality to %d", z.card())
	}
}

func TestInlineRemAndRank(t *testing.T) {
	z := newZset()
	for i, m := range []string{"a", "b", "c", "d"} {
		z.update([]byte(m), float64(i), flags{})
	}
	if r, _, ok := z.rank([]byte("c")); !ok || r != 2 {
		t.Fatalf("rank(c) = %d,%v, want 2,true", r, ok)
	}
	if !z.rem([]byte("b")) || z.rem([]byte("b")) {
		t.Fatal("rem(b) should report true once then false")
	}
	if r, _, ok := z.rank([]byte("c")); !ok || r != 1 {
		t.Fatalf("rank(c) after rem = %d, want 1", r)
	}
}

func TestConversionAtEntryCap(t *testing.T) {
	z := newZset()
	for i := 0; i < maxListpackEntries; i++ {
		z.update([]byte("m"+strconv.Itoa(i)), float64(i), flags{})
	}
	if z.enc != encListpack {
		t.Fatalf("at cap enc = %s, want listpack", z.enc)
	}
	z.update([]byte("one-more"), 999, flags{})
	if z.enc != encSkiplist {
		t.Fatalf("past cap enc = %s, want skiplist", z.enc)
	}
	if z.card() != maxListpackEntries+1 {
		t.Fatalf("card = %d after conversion", z.card())
	}
	// Order and scores survive the promotion.
	if mustScore(t, z, "one-more") != 999 {
		t.Fatal("promoted member lost its score")
	}
	ms, scs := order(z)
	for i := 1; i < len(ms); i++ {
		if lessStr(scs[i], ms[i], scs[i-1], ms[i-1]) {
			t.Fatalf("native order broken at %d: %v", i, ms)
		}
	}
}

func TestConversionAtValueCap(t *testing.T) {
	z := newZset()
	z.update([]byte("seed"), 1, flags{})
	big := make([]byte, maxListpackValue+1)
	for i := range big {
		big[i] = 'x'
	}
	z.update(big, 2, flags{})
	if z.enc != encSkiplist {
		t.Fatalf("a member over the value cap must promote, enc = %s", z.enc)
	}
	if mustScore(t, z, string(big)) != 2 {
		t.Fatal("promoted long member lost its score")
	}
	// A 64-byte member stays inline (boundary inclusive).
	z2 := newZset()
	atCap := make([]byte, maxListpackValue)
	for i := range atCap {
		atCap[i] = 'y'
	}
	z2.update(atCap, 1, flags{})
	if z2.enc != encListpack {
		t.Fatalf("a 64-byte member must stay inline, enc = %s", z2.enc)
	}
}

// Removal never converts back: a promoted zset drained to a few members stays
// skiplist, matching Redis's one-way latch.
func TestNoDownwardConversion(t *testing.T) {
	z := newZset()
	for i := 0; i < maxListpackEntries+10; i++ {
		z.update([]byte("m"+strconv.Itoa(i)), float64(i), flags{})
	}
	if z.enc != encSkiplist {
		t.Fatalf("enc = %s, want skiplist", z.enc)
	}
	for i := 0; i < maxListpackEntries+8; i++ {
		z.rem([]byte("m" + strconv.Itoa(i)))
	}
	if z.enc != encSkiplist {
		t.Fatalf("shrunk zset enc = %s, want it to stay skiplist", z.enc)
	}
	if z.card() != 2 {
		t.Fatalf("card = %d, want 2", z.card())
	}
}

// The ZADD flag matrix, checked at the zset level so the decision logic is
// covered independent of argument parsing (spec 2064/f3/12 section 6.1).
func TestUpdateFlagMatrix(t *testing.T) {
	newWith := func() *zset {
		z := newZset()
		z.update([]byte("m"), 5, flags{})
		return z
	}

	// NX skips an existing member.
	z := newWith()
	added, changed, _, applied, _ := z.update([]byte("m"), 9, flags{nx: true})
	if added || changed || applied || mustScore(t, z, "m") != 5 {
		t.Fatal("NX must not touch an existing member")
	}

	// XX skips an absent member.
	z = newWith()
	added, _, _, applied, _ = z.update([]byte("new"), 1, flags{xx: true})
	if added || applied {
		t.Fatal("XX must not add an absent member")
	}
	if _, ok := z.score([]byte("new")); ok {
		t.Fatal("XX added a member it should have skipped")
	}

	// GT only raises.
	z = newWith()
	z.update([]byte("m"), 3, flags{gt: true})
	if mustScore(t, z, "m") != 5 {
		t.Fatal("GT applied a lower score")
	}
	z.update([]byte("m"), 8, flags{gt: true})
	if mustScore(t, z, "m") != 8 {
		t.Fatal("GT did not apply a higher score")
	}

	// LT only lowers.
	z = newWith()
	z.update([]byte("m"), 9, flags{lt: true})
	if mustScore(t, z, "m") != 5 {
		t.Fatal("LT applied a higher score")
	}
	z.update([]byte("m"), 2, flags{lt: true})
	if mustScore(t, z, "m") != 2 {
		t.Fatal("LT did not apply a lower score")
	}

	// GT still adds an absent member (no XX).
	z = newWith()
	added, _, _, applied, _ = z.update([]byte("fresh"), 1, flags{gt: true})
	if !added || !applied {
		t.Fatal("GT must add an absent member when XX is not set")
	}
}

// INCR reply forms: it returns the new score, or nil when a flag suppresses the
// change (spec 2064/f3/12 section 6.1).
func TestUpdateIncr(t *testing.T) {
	z := newZset()
	z.update([]byte("m"), 5, flags{})

	_, _, ns, applied, _ := z.update([]byte("m"), 3, flags{incr: true})
	if !applied || ns != 8 {
		t.Fatalf("INCR = %v applied=%v, want 8 true", ns, applied)
	}

	// INCR with GT that fails the comparison returns nil (not applied).
	_, _, _, applied, _ = z.update([]byte("m"), -1, flags{incr: true, gt: true})
	if applied {
		t.Fatal("INCR GT below the current score must not apply")
	}
	if mustScore(t, z, "m") != 8 {
		t.Fatal("suppressed INCR changed the score")
	}

	// INCR with NX on an existing member returns nil.
	_, _, _, applied, _ = z.update([]byte("m"), 1, flags{incr: true, nx: true})
	if applied {
		t.Fatal("INCR NX on an existing member must not apply")
	}

	// INCR producing NaN (+inf plus -inf) is rejected.
	z.update([]byte("inf"), math.Inf(1), flags{})
	_, _, _, _, nan := z.update([]byte("inf"), math.Inf(-1), flags{incr: true})
	if !nan {
		t.Fatal("INCR to NaN must be rejected")
	}
}
