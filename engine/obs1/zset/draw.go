package zset

import "math"

// The exactly-uniform draw kernel for ZRANDMEMBER (spec 2064/f3/12 section 6.8),
// the zset twin of the set draw (set/draw.go). Every draw resolves to a flat
// forward rank in [0, card) through the owner-local PCG (reg.next, Lemire
// rejection so it is exactly uniform, F15), then to a member: the native band
// turns the rank into a member with one counted select on the tree (nat.at), the
// inline band walks its ordered blob to the rank. ZRANDMEMBER never removes, so
// there is no pop here; the with-replacement and distinct forms differ only in
// whether they reject repeats. Each drawn member is emitted before the next draw,
// so the aliasing storage is safe.

// zsetSmallSampleCap is the crossover between the two distinct-sample strategies
// (mirrors set/draw.go smallSampleCap). At or below it a distinct sample rejects
// repeats against a short chosen-rank list; above it it partial-shuffles a full
// rank permutation and takes the front.
const zsetSmallSampleCap = 512

// at resolves the member at forward rank idx to its bytes (aliasing live storage,
// valid until the next mutation) and raw score bits. The native band selects on
// the tree; the inline band walks its ordered blob. The caller guarantees idx is
// in [0, card).
func (z *zset) at(idx int) (member []byte, bits uint64) {
	if z.enc == encSkiplist {
		return z.nat.at(idx)
	}
	b := z.blob
	for i := 0; i < len(b); {
		m, s, next := decodeEntry(b, i)
		if idx == 0 {
			return m, math.Float64bits(s)
		}
		idx--
		i = next
	}
	return nil, 0
}

// eachEntry visits every member in ascending order with its raw score bits, the
// ZRANDMEMBER "count at or above cardinality returns them all" path.
func (z *zset) eachEntry(emit func(member []byte, bits uint64)) {
	if z.enc == encSkiplist {
		z.nat.tree.Each(func(_ uint64, ref uint32) bool {
			r := &z.nat.recs[ref]
			emit(z.nat.slab[r.loc:r.loc+r.mlen], r.bits)
			return true
		})
		return
	}
	b := z.blob
	for i := 0; i < len(b); {
		m, s, next := decodeEntry(b, i)
		emit(m, math.Float64bits(s))
		i = next
	}
}

// randWithReplacement draws count members with repetition allowed (ZRANDMEMBER
// negative count): count independent uniform draws, each exactly uniform (F15).
func (z *zset) randWithReplacement(g *reg, count int, emit func(member []byte, bits uint64)) {
	for i := 0; i < count; i++ {
		m, bits := z.at(g.next(z.card()))
		emit(m, bits)
	}
}

// randDistinct draws up to want distinct members (ZRANDMEMBER positive count),
// each an exact uniform sample without replacement (F15). want at or above the
// cardinality returns the whole set in order. Below it, a small want rejects
// repeats against a chosen-rank list; a large want partial-shuffles a full rank
// permutation and takes the front. Both scratch buffers live on the registry and
// are reused, so a steady-state draw allocates nothing.
func (z *zset) randDistinct(g *reg, want int, emit func(member []byte, bits uint64)) {
	card := z.card()
	if want >= card {
		z.eachEntry(emit)
		return
	}
	if want <= zsetSmallSampleCap {
		picked := g.pickScratch[:0]
		for len(picked) < want {
			j := g.next(card)
			if intIn(picked, j) {
				continue
			}
			picked = append(picked, j)
			m, bits := z.at(j)
			emit(m, bits)
		}
		g.pickScratch = picked
		return
	}
	idx := g.identityIndex(card)
	for i := 0; i < want; i++ {
		k := i + g.next(card-i)
		idx[i], idx[k] = idx[k], idx[i]
		m, bits := z.at(int(idx[i]))
		emit(m, bits)
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
