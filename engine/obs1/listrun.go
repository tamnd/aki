// The list position-run plane (spec 2064/obs1 doc 08 section 6): a
// demoted list chunk folds as a valueless packed-pair run under an
// 8-byte virtual-position disc, so the directory's disc64 lift is the
// run's start position and the per-run counts give LLEN and the
// LINDEX/LRANGE positional math as RAM-only prefix sums. The count and
// rank arithmetic is exactly the zset run math, so this file delegates
// to it; only the stream differs, yielding bare element values in
// position order. Ends stay hot by construction: the demote pass sheds
// interior chunks only, so a fold plan never holds the ring's ends and
// the end ops never reach this planner (lab #1321).
package obs1

import (
	"fmt"

	"github.com/tamnd/aki/engine/obs1/store"
)

// ListRunCard sums the plan's packed counts, the folded LLEN with no
// request. Overlay deltas reconcile above the planner.
func ListRunCard(refs []DirRef) int {
	return ZsetCard(refs)
}

// ListRunAtIndex finds the run containing the zero-based dense index and
// the index of its first element, the LINDEX and LRANGE start plan: the
// stream begins at that run and skips index minus base elements in RAM
// after one block fetch. Reports false past the cardinality.
func ListRunAtIndex(refs []DirRef, index int) (idx, base int, ok bool) {
	return ZsetRunAtRank(refs, index)
}

// ListRunIter streams the plan's element values from refs[start:] in
// position order, paging one decoded block at a time with the same
// window and prefetch discipline as ZsetRunIter. The position guard
// works at run granularity: element positions are not derivable from the
// valueless payload, so the iterator checks that each run's FirstDisc is
// not behind the previous run's, the partition interleave guard, and a
// violation is *errp = ErrDiscOrder. A run pair that carries value bytes
// is a torn or misfiled frame and lands in *errp too. Element bytes copy
// out of the fetch buffer because the window outlives the walk.
func ListRunIter(refs []DirRef, start int, fetch func(DirRef) ([]byte, error), prefetch func(DirRef), nowMs int64, errp *error) func() ([]byte, bool) {
	var window [][]byte
	var lastDisc uint64
	var haveDisc bool
	var lastObj string
	var lastOff uint64
	var data []byte
	i, w := start, 0
	return func() ([]byte, bool) {
		for {
			if w < len(window) {
				v := window[w]
				w++
				return v, true
			}
			if i >= len(refs) {
				return nil, false
			}
			ref := refs[i]
			i++
			if haveDisc && ref.FirstDisc < lastDisc {
				*errp = ErrDiscOrder
				return nil, false
			}
			lastDisc, haveDisc = ref.FirstDisc, true
			if data == nil || ref.ObjKey != lastObj || ref.Block.Offset != lastOff {
				if prefetch != nil {
					for j := i; j < len(refs); j++ {
						if refs[j].ObjKey != ref.ObjKey || refs[j].Block.Offset != ref.Block.Offset {
							prefetch(refs[j])
							break
						}
					}
				}
				d, err := fetch(ref)
				if err != nil {
					*errp = err
					return nil, false
				}
				data, lastObj, lastOff = d, ref.ObjKey, ref.Block.Offset
			}
			window, w = window[:0], 0
			err := WalkColdFields(data, ref.OffInBlock, nowMs, func(p store.PackedPair) error {
				if len(p.Value) != 0 {
					return fmt.Errorf("obs1: position-run pair carries %d value bytes, want a valueless element frame", len(p.Value))
				}
				window = append(window, append([]byte(nil), p.Field...))
				return nil
			})
			if err != nil {
				*errp = err
				return nil, false
			}
		}
	}
}
