package set

import (
	"cmp"
	"slices"
	"sort"

	"github.com/tamnd/aki/engine/obs1/store"
)

// byHash orders sorted-array entries by hash. It is a typed comparison for
// slices.SortFunc, which avoids the reflection cost sort.Slice pays per compare
// and is what keeps the tail flush and the merge-entry sort near the lab's write
// tax (lab 05).
func byHash(a, b hentry) int { return cmp.Compare(a.h, b.h) }

// Inline write-time sorted-array maintenance (spec 2064/f3/11 section 6.3): an
// algebra-indexed native set keeps its members in a sorted run plus a bounded
// unsorted tail, current at write time on the owner, so the merge kernels
// (merge.go) can stream it as one ascending sequence without sorting per op.
// This is F3 (derived structures maintained inline) made affordable: the naive
// sorted insert per SADD costs the K16 ~205ns, so the doc keeps the cheaper
// tail form, and lab 05 froze its two constants.
//
// Everything here is off by default. algebraMaintain gates engagement; while it
// is false no set ever builds an index, so htable.add and htable.rem carry only
// one never-taken nil-pointer branch and the point-op paths are the pre-algebra
// paths byte for byte. The algebra-driver slice flips it on once the merge win
// is proven on the gate box (doc 11 section 6, the slice-5 debt).

// algebraMaintain is the package knob. It defaults off (doc 11 section 6.3): the
// point ops stay unchanged until the driver slice proves the win and flips it.
var algebraMaintain = false

// SetAlgebraMaintain toggles inline sorted-array maintenance and returns the
// previous setting. It is the seam the algebra-driver slice flips; a set that is
// already indexed keeps its arrays when this goes back off, since band and index
// state only ever engages, never tears down mid-life (doc 11 section 6.7 defers
// teardown to the LTM epoch clock).
func SetAlgebraMaintain(on bool) bool {
	prev := algebraMaintain
	algebraMaintain = on
	return prev
}

const (
	// algebraFloor is the smaller-operand cardinality below which the merge never
	// beats the probe, so a set below it never maintains arrays (lab 05, carried
	// from setmergefloor, gate-confirmed). A set only engages once it can be the
	// smaller operand of a merge that clears the probe.
	algebraFloor = 128

	// minTail floors the scaled tail so a just-engaged set does not flush on
	// nearly every write. At the floor cardinality the scaled tail is 8, which
	// would merge too often; 16 keeps the flush rate sane without letting the run
	// length grow the write tax.
	minTail = 16
)

// tailCapFor is the bounded tail size T for a set of the given cardinality. Lab
// 05 froze the scaled policy T = card/16 over the doc's fixed T = 256: a fixed
// tail forces every flush to merge against an ever-longer run, so the per-write
// tax climbs to ~185ns at 65536 members, while scaling the tail with cardinality
// holds the run-to-tail ratio and flattens the tax at ~55-68ns.
func tailCapFor(card int) int {
	t := card / 16
	if t < minTail {
		t = minTail
	}
	return t
}

// sortedIndex is one set's algebra representation: a sorted run of (hash,
// ordinal) entries plus a bounded unsorted tail. It is owner-local, so nothing
// locks. tombs counts tombstoned run entries so a churny set compacts before the
// run fills with holes. scratch double-buffers the run across flushes so the
// steady state allocates nothing.
type sortedIndex struct {
	run     []hentry
	tail    []hentry
	tombs   int
	scratch []hentry
}

// onAdd records a new member. It appends to the tail (two stores,
// single-digit-ns, doc 11 section 6.3) and flushes when the tail fills. card is
// the set's post-insert cardinality, which sizes the scaled tail.
func (a *sortedIndex) onAdd(h uint64, ord uint32, card int) {
	a.tail = append(a.tail, hentry{h: h, ord: ord})
	if len(a.tail) >= tailCapFor(card) {
		a.flush()
	}
}

// onRemove drops a member. It tries the tail first (recent inserts), where a
// removal is an O(T) scan and swap-out; failing that it tombstones the run entry,
// found by binary search on the hash and a scan of the equal-hash run for the
// ordinal (doc 11 section 6.3). Tombstones compact on the next flush, so a
// removal is O(1) amortized. The caller guarantees ord was live in this set.
func (a *sortedIndex) onRemove(h uint64, ord uint32) {
	for i := range a.tail {
		if a.tail[i].ord == ord {
			a.tail[i] = a.tail[len(a.tail)-1]
			a.tail = a.tail[:len(a.tail)-1]
			return
		}
	}
	lo := sort.Search(len(a.run), func(i int) bool { return a.run[i].h >= h })
	for i := lo; i < len(a.run) && a.run[i].h == h; i++ {
		if a.run[i].ord == ord {
			a.run[i].ord |= tomb
			a.tombs++
			if a.tombs*4 > len(a.run) {
				a.flush() // compaction: the merge below drops every tombstone
			}
			return
		}
	}
}

// flush sorts the tail and merges it into the run, dropping tombstones, in one
// O(len(run)+T) pass (doc 11 section 6.3). The result lands in the double-buffer
// so a steady churn does not allocate: the old run becomes the next scratch.
func (a *sortedIndex) flush() {
	slices.SortFunc(a.tail, byHash)
	need := len(a.run) - a.tombs + len(a.tail)
	dst := a.scratch
	if cap(dst) < need {
		// Grow with headroom, not to the exact need: a monotonically growing set
		// flushes at an ever-larger need, so an exact allocation reallocs on every
		// flush; half again the need lets several flushes reuse the buffer and
		// keeps the build's allocation churn down (the double-buffer then reuses it
		// under steady churn).
		dst = make([]hentry, 0, need+need/2)
	} else {
		dst = dst[:0]
	}
	i, j := 0, 0
	for i < len(a.run) && j < len(a.tail) {
		if isTomb(a.run[i].ord) {
			i++
			continue
		}
		if a.run[i].h <= a.tail[j].h {
			dst = append(dst, a.run[i])
			i++
		} else {
			dst = append(dst, a.tail[j])
			j++
		}
	}
	for ; i < len(a.run); i++ {
		if !isTomb(a.run[i].ord) {
			dst = append(dst, a.run[i])
		}
	}
	dst = append(dst, a.tail[j:]...)
	a.scratch = a.run[:0]
	a.run = dst
	a.tail = a.tail[:0]
	a.tombs = 0
}

// engageAlgebra builds the sorted index over every live member in one sort pass
// (doc 11 section 6.7: engagement is a bulk build, ~5ms per million members).
// It runs once, when a set first crosses the floor under the flag; after this
// onAdd and onRemove keep the arrays current inline.
func (h *htable) engageAlgebra() {
	a := &sortedIndex{run: make([]hentry, 0, len(h.vec))}
	for _, ord := range h.vec {
		r := &h.recs[ord]
		hash := store.Hash(h.slab[r.loc : r.loc+uint32(r.mlen)])
		a.run = append(a.run, hentry{h: hash, ord: ord})
	}
	slices.SortFunc(a.run, byHash)
	h.alg = a
}

// indexed reports whether the set maintains sorted arrays, the gate the driver
// slice checks before it chooses the merge path over the probe.
func (h *htable) indexed() bool { return h.alg != nil }

// mergeStream builds the run-merge-tail view (merge.go) for the algebra kernels,
// sorting a private copy of the tail once at command entry (doc 11 sections 6.3
// and 6.6) so the source set is untouched. scratch is a reusable tail buffer the
// caller owns; the returned slice is scratch grown to fit, to thread back into
// the next call. It reports false when the set is not indexed, so the driver
// falls to the probe path.
func (h *htable) mergeStream(scratch []hentry) (stream, []hentry, bool) {
	if h.alg == nil {
		return stream{}, scratch, false
	}
	tail := append(scratch[:0], h.alg.tail...)
	slices.SortFunc(tail, byHash)
	return stream{run: h.alg.run, tail: tail}, tail, true
}
