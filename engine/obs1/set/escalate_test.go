package set

import (
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The F13 draw-escalation tests (spec 2064/f3/11 section 5.4). They pin the four
// properties the escalation owes: the two-level draw is identical to the flat
// weighted draw for every position (so uniformity is unchanged), the draw is
// statistically uniform under a random source even with skewed group weights
// (F15, the chi-squared gate the doc names), escalation is one-way and survives a
// split, and the engagement guards refuse the shapes that cannot fan out. The
// threshold seam lowers the engagement so a set reaches P16 at a few hundred
// members, the same pattern the partition tests use.

// escalatedSet builds a partitioned set past P16 and escalates it into k groups,
// returning the set. It fails the test if the band or the escalation did not
// engage, so every caller starts from a known-escalated set.
func escalatedSet(t *testing.T, members [][]byte, k int) *set {
	t.Helper()
	s := buildHT(members)
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned (raise the corpus)", s.enc)
	}
	if len(s.part.parts) < escalateMinP {
		t.Fatalf("P = %d, below escalateMinP %d (raise the corpus)", len(s.part.parts), escalateMinP)
	}
	if !s.escalateDraws(k) {
		t.Fatalf("escalateDraws(%d) did not engage at P=%d", k, len(s.part.parts))
	}
	if !s.part.escalated() {
		t.Fatal("escalated() false after a successful escalateDraws")
	}
	return s
}

// TestEscalatedLocateMatchesFlat is the exactness proof: for every draw position
// r in [0,total) the two-level escalated locate must resolve to the same partition
// and slot the flat locate resolves to, since the scatter layer plus the
// group-local walk is just a hierarchical reading of the same prefix sum. Equality
// over the whole domain is exactly section 5.4's claim that escalation does not
// disturb the section-4.3 uniformity.
func TestEscalatedLocateMatchesFlat(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(1024) // > 8*threshold, so P16
	s := escalatedSet(t, all, 4)
	pt := s.part

	// Skew the weights so the group subtotals differ widely and the scatter is
	// really exercised: drain partition 0 and thin partition 3.
	for _, m := range all {
		switch partOf(store.Hash(m), len(pt.parts)) {
		case 0:
			s.rem(m)
		case 3:
			if pt.counts[3] > 2 {
				s.rem(m)
			}
		}
	}

	total := pt.total
	for r := uint64(0); r < total; r++ {
		fp, fl := pt.locate(r)
		ep, el := pt.locateEscalated(r)
		if fp != ep || fl != el {
			t.Fatalf("r=%d: escalated (%d,%d) != flat (%d,%d)", r, ep, el, fp, fl)
		}
	}
}

// TestEscalatedDrawBijection checks at(i) over the escalated set is a permutation
// of the live members, the structural exactness of the two-level draw over a
// weight-skewed set.
func TestEscalatedDrawBijection(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(900)
	s := escalatedSet(t, all, 8)
	pt := s.part
	for _, m := range all {
		if partOf(store.Hash(m), len(pt.parts)) == 5 {
			s.rem(m) // empty a whole group's partition so a group subtotal shrinks
		}
	}

	live := map[string]bool{}
	s.each(func(m []byte) { live[string(m)] = true })
	hit := map[string]int{}
	for i := 0; i < s.card(); i++ {
		hit[string(pt.at(i))]++
	}
	if len(hit) != len(live) {
		t.Fatalf("at swept %d distinct over %d indices, want %d live", len(hit), s.card(), len(live))
	}
	for m, c := range hit {
		if !live[m] {
			t.Fatalf("at returned %x, not a live member", m)
		}
		if c != 1 {
			t.Fatalf("at returned %x %d times, want once (bijection)", m, c)
		}
	}
}

// TestEscalatedDrawUniform runs the actual draw path (drawOne) over an escalated
// set on a fixed PCG and checks the chi-squared statistic stays low with the group
// weights skewed, the empirical F15 gate section 5.4 carries.
func TestEscalatedDrawUniform(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(768)
	s := escalatedSet(t, all, 4)
	pt := s.part
	for _, m := range all {
		switch partOf(store.Hash(m), len(pt.parts)) {
		case 0, 1, 2, 3:
			if pt.counts[partOf(store.Hash(m), len(pt.parts))] > 4 {
				s.rem(m) // thin the whole first group so its subtotal is tiny
			}
		}
	}

	card := s.card()
	index := map[string]int{}
	s.each(func(m []byte) { index[string(m)] = len(index) })

	g := newTestReg()
	const perCat = 300
	hits := make([]int, card)
	var sc [64]byte
	for i := 0; i < card*perCat; i++ {
		j, ok := index[string(s.drawOne(g, sc[:]))]
		if !ok {
			t.Fatal("draw returned a non-live member")
		}
		hits[j]++
	}
	for j, h := range hits {
		if h == 0 {
			t.Fatalf("member %d never drawn over %d draws", j, card*perCat)
		}
	}
	df := float64(card - 1)
	if stat := chiSquare(hits); stat > 2*df {
		t.Fatalf("chi-squared %.1f over %d skewed-group categories, want < %.1f", stat, card, 2*df)
	}
}

// TestEscalatePopDrain pops the whole escalated set one member at a time and
// checks every member comes out exactly once and the set empties, proving the
// draw+remove path keeps the group weights consistent as the set drains (the SPOP
// hot-key shape).
func TestEscalatePopDrain(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(700)
	s := escalatedSet(t, all, 4)

	want := map[string]bool{}
	for _, m := range all {
		want[string(m)] = true
	}
	g := newTestReg()
	var sc [64]byte
	seen := map[string]int{}
	for s.card() > 0 {
		m := string(s.popOne(g, sc[:]))
		seen[m]++
		if !want[m] {
			t.Fatalf("popped %x, not an original member", m)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("drain saw %d distinct members, want %d", len(seen), len(want))
	}
	for m, c := range seen {
		if c != 1 {
			t.Fatalf("member %x popped %d times, want once", m, c)
		}
	}
	if s.part.total != 0 {
		t.Fatalf("total = %d after full drain, want 0", s.part.total)
	}
}

// TestEscalateOneWay checks a second escalation is a no-op (the split never
// changes once engaged) and that a drain below any threshold keeps the escalated
// layout, the F4 one-way / no-downward-conversion property.
func TestEscalateOneWay(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(1024)
	s := escalatedSet(t, all, 4)
	firstSpan := s.part.esc.span
	firstK := len(s.part.esc.totals)

	if s.escalateDraws(8) {
		t.Fatal("second escalateDraws engaged; escalation must be one-way")
	}
	if s.part.esc.span != firstSpan || len(s.part.esc.totals) != firstK {
		t.Fatal("re-escalation changed the split")
	}

	// Drain most members; the set must stay escalated and partitioned (F4).
	drained := 0
	for _, m := range all {
		if drained >= 900 {
			break
		}
		if s.rem(m) {
			drained++
		}
	}
	if !s.part.escalated() {
		t.Fatal("drain de-escalated the set")
	}
	if s.enc != encPartitioned {
		t.Fatalf("drain converted the band to %s, want partitioned", s.enc)
	}
}

// TestEscalateGuards checks escalation refuses the shapes that cannot fan out: a
// non-partitioned set, a P below the floor, a k that does not divide P, and k < 2.
func TestEscalateGuards(t *testing.T) {
	// Non-partitioned: a small native set never escalates.
	small := setOfWords(50)
	if small.escalateDraws(2) {
		t.Fatal("a non-partitioned set escalated")
	}

	const threshold = 64
	withThreshold(t, threshold)

	// P8 set (>4*threshold, <8*threshold): k must divide 8 and be >= 2.
	p8 := buildHT(members16(300))
	if got := len(p8.part.parts); got != 8 {
		t.Fatalf("P = %d, want 8 for this corpus", got)
	}
	if p8.part.escalate(3) {
		t.Fatal("escalate(3) engaged at P=8; 3 does not divide 8")
	}
	if p8.part.escalate(1) {
		t.Fatal("escalate(1) engaged; k must be at least 2")
	}
	if !p8.part.escalate(4) {
		t.Fatal("escalate(4) did not engage at P=8")
	}
}

// TestEscalateSurvivesGrow escalates a set, then grows it past the next P
// doubling and checks the escalation still holds with the group count preserved
// and the subtotals equal to a fresh recount, so the split is rebuilt correctly
// across the grow.
func TestEscalateSurvivesGrow(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	s := escalatedSet(t, members16(400), 4) // P8 at 400 (>4*64, <8*64), 4 groups
	if len(s.part.parts) != 8 {
		t.Fatalf("P = %d, want 8 before grow", len(s.part.parts))
	}

	// Add until the count crosses 8*threshold and P doubles to 16.
	for i := 100000; s.card() <= 8*threshold; i++ {
		s.add(members16Range(i, 1)[0])
	}
	if len(s.part.parts) != 16 {
		t.Fatalf("P = %d, want 16 after grow", len(s.part.parts))
	}
	if !s.part.escalated() {
		t.Fatal("grow dropped the escalation")
	}
	if got := len(s.part.esc.totals); got != 4 {
		t.Fatalf("group count = %d after grow, want the preserved 4", got)
	}
	if got := s.part.esc.span; got != 16/4 {
		t.Fatalf("span = %d after grow to P16, want 4", got)
	}

	// The rebuilt subtotals must equal a fresh recount from the partition counts.
	want := newEscalation(s.part.counts, s.part.esc.span, 4)
	for g := range want.totals {
		if s.part.esc.totals[g] != want.totals[g] {
			t.Fatalf("group %d subtotal = %d, want %d", g, s.part.esc.totals[g], want.totals[g])
		}
	}
	// And the draw must still cover every live member.
	live := 0
	s.each(func([]byte) { live++ })
	if live != s.card() {
		t.Fatalf("each visited %d, card %d", live, s.card())
	}
}
