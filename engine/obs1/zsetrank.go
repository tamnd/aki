// The zset rank and range plane (spec 2064/obs1 doc 08 section 5): rank
// arithmetic is RAM-only prefix sums over the score runs' directory
// counts, fetching at most the boundary blocks, and every range form
// streams the runs in score order from a floored start. The planner
// works over the kind-restricted CollChunksKind plan, whose refs carry
// each run's count and first discriminator; the discriminator coordinate
// is the leading 8 bytes of the demoter's composite disc, which is
// exactly the IEEE-754 total-order score key, so a chunk floor by score
// key is a floor into the zset's (score, member) order with at worst a
// one-run overshoot on a boundary score tie, which the boundary scan
// absorbs.
package obs1

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/tamnd/aki/engine/obs1/store"
)

// ZsetScoreKey lifts a score into the total-order u64 coordinate the
// score runs are discriminated by, the same lift the demoter applies:
// positive floats set the sign bit, negatives bit-invert, so unsigned
// compare equals float compare with -0 and +0 adjacent.
func ZsetScoreKey(s float64) uint64 {
	b := math.Float64bits(s)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | 1<<63
}

// ZsetPair is one streamed element of a score-run read: the member, the
// raw IEEE-754 score bits as packed, and the score coordinate.
type ZsetPair struct {
	Member []byte
	Bits   uint64
	Key    uint64
}

// ZsetCard sums the plan's packed counts, the ZCARD of the folded state
// with no request at all. Overlay deltas reconcile above the planner.
func ZsetCard(refs []DirRef) int {
	n := 0
	for _, r := range refs {
		n += int(r.Count)
	}
	return n
}

// ZsetRankFloor finds the run owning the score coordinate and the count
// of elements packed before it, the ZRANK and ZCOUNT boundary plan: the
// exact position inside the floor run is a scan of that run's block, so
// the request bill is zero when the caller already holds the block and
// one otherwise. A coordinate before every run floors to run zero with
// base zero.
func ZsetRankFloor(refs []DirRef, key uint64) (idx, base int) {
	for i, r := range refs {
		if i > 0 && r.FirstDisc > key {
			return i - 1, base - int(refs[i-1].Count)
		}
		base += int(r.Count)
	}
	if len(refs) == 0 {
		return 0, 0
	}
	return len(refs) - 1, base - int(refs[len(refs)-1].Count)
}

// ZsetRunAtRank finds the run containing the zero-based rank and the
// rank of its first element, the ZRANGE start plan: the stream begins at
// that run and skips rank minus base elements in RAM after one block
// fetch. Reports false past the cardinality.
func ZsetRunAtRank(refs []DirRef, rank int) (idx, base int, ok bool) {
	if rank < 0 {
		return 0, 0, false
	}
	for i, r := range refs {
		if rank < base+int(r.Count) {
			return i, base, true
		}
		base += int(r.Count)
	}
	return 0, 0, false
}

// ZsetRunIter streams the plan's pairs from refs[start:] in (score,
// member) order, paging one decoded block at a time and walking every
// run that shares it, the same window discipline as ColdCollIter. The
// prefetch hook, when non-nil, hears the next distinct block's first ref
// as soon as the current block starts serving, the doc 05 readahead seam
// (admission wiring lands with the scan slice). Member bytes copy out of
// the fetch buffer because the window outlives the walk. A backward
// score key across the stream is *errp = ErrDiscOrder, the partition
// interleave guard; fetch and decode errors land in *errp the same way.
func ZsetRunIter(refs []DirRef, start int, fetch func(DirRef) ([]byte, error), prefetch func(DirRef), nowMs int64, errp *error) func() (ZsetPair, bool) {
	var window []ZsetPair
	var last uint64
	var lastObj string
	var lastOff uint64
	var data []byte
	i, w := start, 0
	return func() (ZsetPair, bool) {
		for {
			if w < len(window) {
				p := window[w]
				w++
				if p.Key < last {
					*errp = ErrDiscOrder
					return ZsetPair{}, false
				}
				last = p.Key
				return p, true
			}
			if i >= len(refs) {
				return ZsetPair{}, false
			}
			ref := refs[i]
			i++
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
					return ZsetPair{}, false
				}
				data, lastObj, lastOff = d, ref.ObjKey, ref.Block.Offset
			}
			window, w = window[:0], 0
			err := WalkColdFields(data, ref.OffInBlock, nowMs, func(p store.PackedPair) error {
				if len(p.Value) != 8 {
					return fmt.Errorf("obs1: score-run pair %q carries %d value bytes, want the 8 score bits", p.Field, len(p.Value))
				}
				bits := binary.BigEndian.Uint64(p.Value)
				window = append(window, ZsetPair{
					Member: append([]byte(nil), p.Field...),
					Bits:   bits,
					Key:    ZsetScoreKey(math.Float64frombits(bits)),
				})
				return nil
			})
			if err != nil {
				*errp = err
				return ZsetPair{}, false
			}
		}
	}
}
