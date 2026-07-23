// Set algebra over discriminator-ordered streams (spec 2064/obs1 doc 08
// section 4): SUNION, SINTER, and SDIFF as k-way merges. Every operand is
// an ElemIter in nondecreasing Disc order whose Data is the raw member
// bytes, the same view MergeShadow yields when a set's overlay shadows its
// chunks, so an operand costs one resident snapshot or one paged cold walk
// and the algebra never materializes a whole operand. Equal-Disc ties
// buffer only the colliding run per operand (collision-sized by the doc 05
// arithmetic), which is what keeps the merge window bounded by the
// big-collection lab's measurement rather than by cardinality.
//
// The STORE forms are the same merges with the yield writing into a result
// set builder instead of a reply, so this file carries no store variants.

package obs1

import (
	"bytes"

	"github.com/tamnd/aki/engine/obs1/store"
)

// SetSame is the MergeShadow identity callback for sets: an element is the
// member bytes themselves, so identity is byte equality. It is what lets
// an overlay SREM claim (Dead true) suppress the cold copy of exactly its
// member inside a discriminator collision.
func SetSame(cold, ov []byte) bool { return bytes.Equal(cold, ov) }

// setStream tracks one operand through a k-way merge.
type setStream struct {
	next ElemIter
	cur  Elem
	ok   bool
	last uint64
}

func (s *setStream) advance() error {
	if s.cur, s.ok = s.next(); s.ok {
		if s.cur.Disc < s.last {
			return ErrDiscOrder
		}
		s.last = s.cur.Disc
	}
	return nil
}

// setMerge drives the shared k-way loop: it finds the least discriminator
// among the live streams, gathers each operand's equal-Disc tie run, and
// hands the per-operand runs to combine, which yields whatever the
// operation keeps. ties[i] is empty for an operand with no member at the
// tie's discriminator.
func setMerge(ops []ElemIter, combine func(ties [][]Elem) error) error {
	streams := make([]setStream, len(ops))
	for i, op := range ops {
		streams[i] = setStream{next: op}
		if err := streams[i].advance(); err != nil {
			return err
		}
	}
	ties := make([][]Elem, len(ops))
	for {
		live := false
		var d uint64
		for i := range streams {
			s := &streams[i]
			if !s.ok {
				continue
			}
			if !live || s.cur.Disc < d {
				live, d = true, s.cur.Disc
			}
		}
		if !live {
			return nil
		}
		for i := range streams {
			s := &streams[i]
			ties[i] = ties[i][:0]
			for s.ok && s.cur.Disc == d {
				ties[i] = append(ties[i], s.cur)
				if err := s.advance(); err != nil {
					return err
				}
			}
		}
		if err := combine(ties); err != nil {
			return err
		}
	}
}

// tieHas reports whether the tie run holds member m.
func tieHas(tie []Elem, m []byte) bool {
	for _, e := range tie {
		if bytes.Equal(e.Data, m) {
			return true
		}
	}
	return false
}

// SetUnion yields every distinct member across the operands exactly once,
// in nondecreasing Disc order, operand order inside a tie. The member
// bytes alias the yielding stream's buffer for the call, the lifetime
// every element in this file carries.
func SetUnion(ops []ElemIter, yield func(m []byte) error) error {
	return setMerge(ops, func(ties [][]Elem) error {
		var seen [][]byte
		for _, tie := range ties {
			for _, e := range tie {
				dup := false
				for _, s := range seen {
					if bytes.Equal(s, e.Data) {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				seen = append(seen, e.Data)
				if err := yield(e.Data); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// SetInter yields the members present in every operand, in nondecreasing
// Disc order. A colliding member missing from one operand's tie run drops
// out exactly as a missing discriminator does.
func SetInter(ops []ElemIter, yield func(m []byte) error) error {
	if len(ops) == 0 {
		return nil
	}
	return setMerge(ops, func(ties [][]Elem) error {
		for _, e := range ties[0] {
			all := true
			for _, tie := range ties[1:] {
				if !tieHas(tie, e.Data) {
					all = false
					break
				}
			}
			if all {
				if err := yield(e.Data); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// SetDiff yields the members of the first operand present in none of the
// rest, in nondecreasing Disc order.
func SetDiff(ops []ElemIter, yield func(m []byte) error) error {
	if len(ops) == 0 {
		return nil
	}
	return setMerge(ops, func(ties [][]Elem) error {
		for _, e := range ties[0] {
			claimed := false
			for _, tie := range ties[1:] {
				if tieHas(tie, e.Data) {
					claimed = true
					break
				}
			}
			if !claimed {
				if err := yield(e.Data); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// ColdCollIter adapts a collection's cold chunks to an ElemIter, paging
// one decoded block at a time: refs is the CollChunks plan, fetch turns
// one ref into its decoded block (the caller owns the GET and any block
// cache), and the iterator walks each chunk's live pairs under the lazy
// expiry rule at nowMs. The buffered window is one chunk's elements, which
// is the one-block-per-operand cost the algebra above is planned around.
// Member bytes are copied out of the fetch buffer because the window
// outlives the walk.
//
// The stream enforces the nondecreasing contract itself: a partitioned
// set's per-partition demotes interleave their coordinate ranges, and
// until the doc 06 rewrite re-sorts a collection's chunks globally, a plan
// over such chunks must fail loudly (*errp = ErrDiscOrder) rather than
// merge a backward range. A fetch or decode error lands in *errp the same
// way; the stream just ends, and the caller checks *errp after the merge.
func ColdCollIter(refs []DirRef, fetch func(DirRef) ([]byte, error), nowMs int64, errp *error) ElemIter {
	var window []Elem
	var last uint64
	var lastObj string
	var lastOff uint64
	var data []byte
	i, w := 0, 0
	return func() (Elem, bool) {
		for {
			if w < len(window) {
				e := window[w]
				w++
				if e.Disc < last {
					*errp = ErrDiscOrder
					return Elem{}, false
				}
				last = e.Disc
				return e, true
			}
			if i >= len(refs) {
				return Elem{}, false
			}
			ref := refs[i]
			i++
			if data == nil || ref.ObjKey != lastObj || ref.Block.Offset != lastOff {
				d, err := fetch(ref)
				if err != nil {
					*errp = err
					return Elem{}, false
				}
				data, lastObj, lastOff = d, ref.ObjKey, ref.Block.Offset
			}
			window, w = window[:0], 0
			err := WalkColdFields(data, ref.OffInBlock, nowMs, func(p store.PackedPair) error {
				m := append([]byte(nil), p.Field...)
				window = append(window, Elem{Disc: Disc(m), Data: m})
				return nil
			})
			if err != nil {
				*errp = err
				return Elem{}, false
			}
		}
	}
}
