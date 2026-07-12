package hash

// The exactly-uniform draw kernel for HRANDFIELD (spec 2064/f3/10 section 7.4),
// the hash twin of the set and zset draws (set/draw.go, zset/draw.go). Every draw
// resolves to a flat forward position in [0, card) through the owner-local PCG
// (reg.next, Lemire rejection so it is exactly uniform, F15), then to a pair
// through hash.at: the native band indexes the dense draw vector, the inline band
// walks its blob. HRANDFIELD never removes, so there is no pop here; the
// with-replacement and distinct forms differ only in whether they reject repeats.
// Each drawn pair is emitted before the next draw, so the aliasing storage is
// safe.

// smallSampleCap is the crossover between the two distinct-sample strategies
// (mirrors set/draw.go and zset/draw.go). At or below it a distinct sample rejects
// repeats against a short chosen-position list (O(want^2), no per-field setup);
// above it it partial-shuffles a full position permutation and takes the front
// (O(card) setup). The cap keeps the rejection loop's quadratic term bounded while
// the shuffle branch carries the large samples the doc's n/2 crossover points at.
const smallSampleCap = 512

// randWithReplacement draws count pairs with repetition allowed (HRANDFIELD
// negative count): count independent uniform draws, each exactly uniform (F15).
func (h *hash) randWithReplacement(g *reg, count int, emit func(field, value []byte)) {
	for i := 0; i < count; i++ {
		f, v := h.at(g.next(h.card()))
		emit(f, v)
	}
}

// randDistinct draws up to want distinct pairs (HRANDFIELD positive count), each
// an exact uniform sample without replacement (F15). want at or above the
// cardinality returns the whole hash in position order. Below it, a small want
// rejects repeats against a chosen-position list; a large want partial-shuffles a
// full position permutation and takes the front. Both scratch buffers live on the
// registry and are reused, so a steady-state draw allocates nothing.
func (h *hash) randDistinct(g *reg, want int, emit func(field, value []byte)) {
	card := h.card()
	if want >= card {
		h.each(emit)
		return
	}
	if want <= smallSampleCap {
		picked := g.pickScratch[:0]
		for len(picked) < want {
			j := g.next(card)
			if intIn(picked, j) {
				continue
			}
			picked = append(picked, j)
			f, v := h.at(j)
			emit(f, v)
		}
		g.pickScratch = picked
		return
	}
	idx := g.identityIndex(card)
	for i := 0; i < want; i++ {
		k := i + g.next(card-i)
		idx[i], idx[k] = idx[k], idx[i]
		f, v := h.at(int(idx[i]))
		emit(f, v)
	}
}

// intIn reports whether v is already in xs, the small-sample draw's repeat check.
func intIn(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
