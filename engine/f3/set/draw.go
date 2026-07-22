package set

// The exactly-uniform draw kernel (spec 2064/f3/11 sections 4.3, 5.2, 5.6). It
// is the port of f1's P10 dense-vector draw (K11: 4.8ns at 100k members, 12.2ns
// at 1M), with f1's COW publication and its back map deleted: under single
// ownership (F1) the owner is the sole reader and writer, so the draw reads the
// dense vector directly and the pop mutates it in place with no snapshot and no
// generation check.
//
// Every draw resolves to a flat index in [0, card) through drawIndex, which is
// the seam the partitioned band will weight. Today the native and inline bands
// are one flat population, so drawIndex is a single unbiased bounded draw. When
// the partitioned band lands (doc 11 section 4.3) drawIndex becomes the weighted
// prefix-sum scan over the per-partition counts (draw r in [0, total); find the
// partition by prefix sum; return r - offset_p), and at()/rem() dispatch on the
// chosen partition. Nothing else in this file changes: drawOne, popOne, and both
// count forms key off the flat index, so weighting the index selection is the
// whole of the partition-draw work. The scan is deliberately shaped as one
// bounded loop so the partition slice can drop in the branchless prefix-sum
// version the lab 04 note calls for.

// smallSampleCap is the crossover between the two distinct-sample strategies of
// doc 11 section 5.2. At or below it a distinct sample rejects repeats against a
// short chosen-index list (O(want^2), no per-member setup); above it the sample
// partial-shuffles a full index permutation (O(card) setup, the shuffle-and-take
// branch). The cap keeps the rejection loop's quadratic term bounded while the
// shuffle branch carries the large samples the doc's n/2 crossover points at.
const smallSampleCap = 512

// drawIndex returns a uniform flat member index in [0, card). This is the draw
// seam: the native and inline bands are one flat vector, so it is one unbiased
// bounded draw; the partitioned band weights it by prefix sum (see the file
// header). The caller guarantees a non-empty set.
func (s *set) drawIndex(g *reg) int { return g.next(s.card()) }

// drawOne returns one uniformly drawn member without removing it (SRANDMEMBER
// single, and each with-replacement draw). The result aliases the set's storage
// or is rendered into sc, valid only until the next draw or mutation, so the
// caller emits it before drawing again. Zero allocation for members that fit sc.
func (s *set) drawOne(g *reg, sc []byte) []byte {
	return s.at(s.drawIndex(g), sc)
}

// popOne draws one member uniformly and removes it, the SPOP kernel: draw a flat
// index, copy the member out (the pop must survive the swap-remove that follows,
// which for the native band moves the vector's tail into the drawn slot and may
// compact the slab), then remove it. The returned bytes live in sc. Repeated
// popOne over a set is an exact uniform sample without replacement, because each
// draw is uniform over the members still present.
func (s *set) popOne(g *reg, sc []byte) []byte {
	i := s.drawIndex(g)
	m := append(sc[:0], s.at(i, sc)...)
	s.remAt(i, m)
	return m
}

// sampleWithReplacement draws count members with repetition allowed (SRANDMEMBER
// negative count): count independent uniform draws, each exactly uniform (F15).
// It removes nothing. Each drawn member is emitted before the next draw, so the
// aliasing scratch is safe.
func (s *set) sampleWithReplacement(g *reg, count int, emit func(m []byte)) {
	var sc [64]byte
	for i := 0; i < count; i++ {
		emit(s.drawOne(g, sc[:]))
	}
}

// sampleDistinct draws up to want distinct members (SRANDMEMBER positive count),
// each an exact uniform sample without replacement (F15). want at or above the
// cardinality returns the whole set. Below the cardinality it takes the doc 11
// section 5.2 branch: a small want rejects repeats against a chosen-index list; a
// large want partial-shuffles a full index permutation and takes the front. Both
// scratch buffers live on the registry and are reused, so a steady-state draw
// allocates nothing.
func (s *set) sampleDistinct(g *reg, want int, emit func(m []byte)) {
	card := s.card()
	if want >= card {
		s.each(emit)
		return
	}
	var sc [64]byte
	if want <= smallSampleCap {
		picked := g.pickScratch[:0]
		for len(picked) < want {
			j := g.next(card)
			if intIn(picked, j) {
				continue
			}
			picked = append(picked, j)
			emit(s.at(j, sc[:]))
		}
		g.pickScratch = picked
		return
	}
	idx := g.identityIndex(card)
	for i := 0; i < want; i++ {
		k := i + g.next(card-i)
		idx[i], idx[k] = idx[k], idx[i]
		emit(s.at(int(idx[i]), sc[:]))
	}
}

// identityIndex returns a reused []uint32 of 0..n-1, the permutation the
// large-sample distinct draw shuffles in place.
func (g *reg) identityIndex(n int) []uint32 {
	if cap(g.idxScratch) < n {
		g.idxScratch = make([]uint32, n)
	}
	idx := g.idxScratch[:n]
	for i := range idx {
		idx[i] = uint32(i)
	}
	return idx
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
