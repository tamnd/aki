// The stream pending-entries run plane (spec 2064/obs1 doc 08 section
// 7): a group's PEL folds as its own chunk kind under the stream's
// collection key, one chunk per (group, demoted block), so XPENDING and
// XAUTOCLAIM plan PEL chunks like any range without touching the entry
// runs. The 24-byte disc leads with the group's tag, so the directory's
// disc64 lift groups the plan by group and the stable sort keeps each
// group's chunks in the emission's ID order; acks and claims that land
// after a fold are overlay deltas the reader merges on top.
package obs1

import (
	"encoding/binary"
	"fmt"

	"github.com/tamnd/aki/engine/obs1/store"
)

// StreamPelTag is the group's chunk-plane tag, the leading 8 bytes of
// every PEL chunk disc the group emits. Both sides of the seam derive it
// from the group name alone.
func StreamPelTag(group []byte) uint64 { return Disc(group) }

// StreamPelEntry is one folded pending entry: the delivered ID, the
// owning consumer's name (empty during a FORCE claim's ownerless
// moment), the delivery count, and the last delivery's unix ms.
type StreamPelEntry struct {
	ID          StreamRunID
	Consumer    []byte
	Deliveries  uint16
	DeliveredMs uint64
}

// StreamPelRefs filters a kind-restricted plan to one group's PEL
// chunks. The tag rides each chunk's disc64 coordinate, so the filter is
// pure RAM; the surviving refs keep their plan order, which is the
// group's ID order.
func StreamPelRefs(refs []DirRef, tag uint64) []DirRef {
	var out []DirRef
	for _, r := range refs {
		if r.FirstDisc == tag {
			out = append(out, r)
		}
	}
	return out
}

// StreamPelIter streams a group's folded pending entries in ID order,
// paging one decoded block at a time with the shared window and prefetch
// discipline. Each chunk's payload is packed pairs: a 16-byte big-endian
// ID as the field, then the delivery facts as the value (8-byte ms,
// 2-byte count, consumer name). A wrong-shape pair or a short disc is a
// misfile error, and a backward ID across the stream is ErrDiscOrder.
func StreamPelIter(refs []DirRef, fetch func(DirRef) ([]byte, error), prefetch func(DirRef), errp *error) func() (StreamPelEntry, bool) {
	var window []StreamPelEntry
	var lastID StreamRunID
	var haveID bool
	var lastObj string
	var lastOff uint64
	var data []byte
	i, w := 0, 0
	return func() (StreamPelEntry, bool) {
		for {
			if w < len(window) {
				e := window[w]
				w++
				if haveID && e.ID.Less(lastID) {
					*errp = ErrDiscOrder
					return StreamPelEntry{}, false
				}
				lastID, haveID = e.ID, true
				return e, true
			}
			if i >= len(refs) {
				return StreamPelEntry{}, false
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
					return StreamPelEntry{}, false
				}
				data, lastObj, lastOff = d, ref.ObjKey, ref.Block.Offset
			}
			frame, err := ColdChunkFrame(data, ref.OffInBlock)
			if err != nil {
				*errp = err
				return StreamPelEntry{}, false
			}
			if len(frame.Disc) != 24 {
				*errp = fmt.Errorf("obs1: pel run disc is %d bytes, want the 24-byte (tag, ms, seq) form", len(frame.Disc))
				return StreamPelEntry{}, false
			}
			window, w = window[:0], 0
			var perr error
			ok := store.WalkPackedPairs(frame.Payload, frame.Flags, int(frame.Count), func(_ int, p store.PackedPair) bool {
				if len(p.Field) != 16 {
					perr = fmt.Errorf("obs1: pel pair ID is %d bytes, want the 16-byte (ms, seq) pair", len(p.Field))
					return false
				}
				if len(p.Value) < 10 {
					perr = fmt.Errorf("obs1: pel pair value is %d bytes, want the delivery facts", len(p.Value))
					return false
				}
				window = append(window, StreamPelEntry{
					ID: StreamRunID{
						Ms:  binary.BigEndian.Uint64(p.Field[0:8]),
						Seq: binary.BigEndian.Uint64(p.Field[8:16]),
					},
					DeliveredMs: binary.BigEndian.Uint64(p.Value[0:8]),
					Deliveries:  binary.BigEndian.Uint16(p.Value[8:10]),
					Consumer:    append([]byte(nil), p.Value[10:]...),
				})
				return true
			})
			if perr == nil && !ok {
				perr = fmt.Errorf("obs1: pel chunk payload is torn")
			}
			if perr != nil {
				*errp = perr
				return StreamPelEntry{}, false
			}
		}
	}
}
