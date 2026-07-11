package set

import (
	"math/rand/v2"
	"sort"
	"testing"
)

// The sorted-array maintenance invariants (doc 11 section 6.3): under any mix of
// SADD, SREM, and SPOP the run stays sorted, the tail stays bounded, and the
// live entries of run-plus-tail are exactly the members the table holds, with
// nothing lost or duplicated. The flag-off path is checked to be byte-for-byte
// the pre-algebra path: no index is ever built and membership is unchanged.

// indexLive returns the members the sorted arrays name as live (run without
// tombstones, plus tail), sorted, resolved through their ordinals.
func indexLive(h *htable) []string {
	var out []string
	for _, e := range h.alg.run {
		if !isTomb(e.ord) {
			out = append(out, string(h.memberByOrd(e.ord)))
		}
	}
	for _, e := range h.alg.tail {
		out = append(out, string(h.memberByOrd(e.ord)))
	}
	sort.Strings(out)
	return out
}

// checkInvariants asserts the run is sorted, the tail is within its bound, and
// the index's live members match the table's.
func checkInvariants(t *testing.T, h *htable) {
	t.Helper()
	if h.alg == nil {
		t.Fatal("expected an engaged index")
	}
	for i := 1; i < len(h.alg.run); i++ {
		if h.alg.run[i].h < h.alg.run[i-1].h {
			t.Fatalf("run not sorted at %d: %x < %x", i, h.alg.run[i].h, h.alg.run[i-1].h)
		}
	}
	if bound := tailCapFor(h.card()); len(h.alg.tail) >= bound && bound > 0 {
		// A full tail must have flushed; the only legal steady state is strictly
		// below the cap right after an onAdd.
		t.Fatalf("tail %d not below its bound %d", len(h.alg.tail), bound)
	}
	got := indexLive(h)
	want := live(h)
	eqStrings(t, "index-vs-table", got, want)
}

func TestMaintenanceUnderChurn(t *testing.T) {
	defer SetAlgebraMaintain(SetAlgebraMaintain(true))

	present := map[string]bool{}
	h := newHashtable(0)
	// Seed past the floor so the index engages, then churn.
	for i := 0; i < 200; i++ {
		m := idMember(i)
		if h.add(m) {
			present[string(m)] = true
		}
	}
	if h.alg == nil {
		t.Fatal("index did not engage past the floor")
	}

	rng := rand.NewPCG(1, 2)
	r := rand.New(rng)
	next := 200
	for step := 0; step < 20000; step++ {
		switch r.IntN(3) {
		case 0: // SADD, mostly fresh members
			m := idMember(next)
			next++
			if h.add(m) {
				present[string(m)] = true
			}
		case 1: // SREM a present member, if any
			if len(present) == 0 {
				continue
			}
			var pick string
			for k := range present {
				pick = k
				break
			}
			if h.rem([]byte(pick)) {
				delete(present, pick)
			}
		case 2: // SPOP through the draw path (popOne -> rem)
			if h.card() == 0 {
				continue
			}
			var sc [64]byte
			g := benchReg()
			m := append([]byte(nil), h.popOneVia(g, sc[:])...)
			delete(present, string(m))
		}
		if step%997 == 0 && h.alg != nil {
			checkInvariants(t, h)
		}
	}

	want := make([]string, 0, len(present))
	for k := range present {
		want = append(want, k)
	}
	sort.Strings(want)
	eqStrings(t, "final-table", live(h), want)
	checkInvariants(t, h)
}

// popOneVia draws and removes one member through the htable's own swap-remove,
// the SPOP kernel the maintenance must stay consistent under. It mirrors
// set.popOne but on a bare table so the test needs no set wrapper.
func (h *htable) popOneVia(g *reg, sc []byte) []byte {
	i := g.next(h.card())
	m := append(sc[:0], h.at(i)...)
	h.rem(m)
	return m
}

// TestFlagOffNoIndex checks that with the flag off no set ever engages, so the
// point ops are the pre-algebra paths and membership is identical to the flag-on
// run over the same op sequence.
func TestFlagOffNoIndex(t *testing.T) {
	ops := make([][2]int, 6000) // {kind, id}
	rng := rand.New(rand.NewPCG(7, 9))
	for i := range ops {
		ops[i] = [2]int{rng.IntN(2), rng.IntN(2000)}
	}

	run := func(on bool) []string {
		defer SetAlgebraMaintain(SetAlgebraMaintain(on))
		h := newHashtable(0)
		for _, op := range ops {
			m := idMember(op[1])
			if op[0] == 0 {
				h.add(m)
			} else {
				h.rem(m)
			}
		}
		if !on && h.alg != nil {
			t.Fatal("flag off but an index engaged")
		}
		return live(h)
	}

	off := run(false)
	on := run(true)
	eqStrings(t, "flag-off-vs-on membership", off, on)
}

// TestScaledTailBound checks the frozen scaled tail T = card/16 (lab 05): the
// tail never reaches its scaled cap in steady state, and the cap tracks
// cardinality rather than staying at a fixed 256.
func TestScaledTailBound(t *testing.T) {
	defer SetAlgebraMaintain(SetAlgebraMaintain(true))
	h := newHashtable(0)
	for i := 0; i < 40000; i++ {
		h.add(idMember(i))
		if h.alg != nil && len(h.alg.tail) >= tailCapFor(h.card()) {
			t.Fatalf("tail %d reached cap %d at card %d", len(h.alg.tail), tailCapFor(h.card()), h.card())
		}
	}
	// At 40000 members the scaled cap is 2500, well past the doc's fixed 256, which
	// is the whole point of the scaled policy.
	if got := tailCapFor(h.card()); got != h.card()/16 {
		t.Fatalf("tailCapFor(%d) = %d, want %d", h.card(), got, h.card()/16)
	}
}

// TestEngageFloor checks a set below the floor never maintains arrays even with
// the flag on, and engages exactly when it crosses the floor.
func TestEngageFloor(t *testing.T) {
	defer SetAlgebraMaintain(SetAlgebraMaintain(true))
	h := newHashtable(0)
	for i := 0; i < algebraFloor-1; i++ {
		h.add(idMember(i))
	}
	if h.indexed() {
		t.Fatalf("engaged below the floor at card %d", h.card())
	}
	h.add(idMember(algebraFloor - 1)) // crosses to algebraFloor
	if !h.indexed() {
		t.Fatalf("did not engage at the floor, card %d", h.card())
	}
	eqStrings(t, "engage-build", indexLive(h), live(h))
}
