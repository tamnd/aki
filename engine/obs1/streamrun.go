// The stream ID-range run plane (spec 2064/obs1 doc 08 section 7): a
// demoted stream block folds whole, its blob already the dense immutable
// wire-form run, under a 16-byte (ms, seq) disc whose leading ms word is
// the directory's disc64 coordinate. XRANGE and an XREAD catch-up floor
// the plan by ms and stream from there with the shared window and
// prefetch discipline; the boundary scan absorbs the ms-tie overshoot
// the same way the zset floor absorbs a score tie. The walker decodes
// the block wire form standalone (master frame first, then same-schema
// and general frames against the master's names), because the fold-plane
// reader has no resident block header: the frame count rides the chunk
// frame and the exact first ID rides its 16-byte disc.
package obs1

import (
	"encoding/binary"
	"fmt"
)

// StreamRunID is a stream entry ID on the fold plane, the (ms, seq) pair
// the run discs pack big endian.
type StreamRunID struct {
	Ms  uint64
	Seq uint64
}

// Less orders IDs ms first then seq, the stream ID order.
func (a StreamRunID) Less(b StreamRunID) bool {
	if a.Ms != b.Ms {
		return a.Ms < b.Ms
	}
	return a.Seq < b.Seq
}

// StreamField is one field-value pair of a streamed entry.
type StreamField struct {
	Name  []byte
	Value []byte
}

// StreamEntry is one streamed element of an ID-range run read.
type StreamEntry struct {
	ID     StreamRunID
	Fields []StreamField
}

// StreamRunFloor finds the run a range starting at ms streams from: the
// last run whose coordinate is at or below ms, or run zero when every
// run starts past it. The coordinate is the run's first ms only (the
// disc64 lift of the 16-byte disc), so a seq-level bound resolves inside
// the floored run's scan, and an ms tie across runs can overshoot by
// one, which the scan absorbs.
func StreamRunFloor(refs []DirRef, ms uint64) int {
	idx, _ := ZsetRankFloor(refs, ms)
	return idx
}

// Stream block wire form (mirrors stream/block.go, doc 14 section 3.3):
// each frame is a flags byte, an ID delta against the block's first ID
// (ms delta unsigned varint, seq delta signed varint), then the body. A
// general frame carries a field count and length-prefixed name and value
// pairs; a same-schema frame carries only values against the master's
// names; the first frame is the master and is always general. Bit 0 is
// same-schema, bit 1 is an XDEL tombstone the walk skips.
const (
	streamFrameSameSchema = 1 << 0
	streamFrameDeleted    = 1 << 1
)

// WalkStreamRun decodes count frames of a folded stream block payload,
// yielding the live entries in ID order. first is the block's first ID,
// read off the run's 16-byte disc. The yielded field views alias
// payload. It returns an error on a torn or misfiled payload.
func WalkStreamRun(payload []byte, first StreamRunID, count int, fn func(id StreamRunID, fields []StreamField) error) error {
	type nameRef struct{ off, n int }
	var names []nameRef
	var fields []StreamField
	pos := 0
	for i := 0; i < count; i++ {
		if pos >= len(payload) {
			return fmt.Errorf("obs1: stream run truncated at frame %d of %d", i, count)
		}
		flags := payload[pos]
		pos++
		md, n1 := binary.Uvarint(payload[pos:])
		if n1 <= 0 {
			return fmt.Errorf("obs1: stream run frame %d has a torn ms delta", i)
		}
		sd, n2 := binary.Varint(payload[pos+n1:])
		if n2 <= 0 {
			return fmt.Errorf("obs1: stream run frame %d has a torn seq delta", i)
		}
		pos += n1 + n2
		id := StreamRunID{Ms: first.Ms + md, Seq: uint64(int64(first.Seq) + sd)}
		fields = fields[:0]
		if flags&streamFrameSameSchema != 0 {
			if i == 0 {
				return fmt.Errorf("obs1: stream run master frame claims same-schema")
			}
			for _, nr := range names {
				vl, n := binary.Uvarint(payload[pos:])
				if n <= 0 || pos+n+int(vl) > len(payload) {
					return fmt.Errorf("obs1: stream run frame %d has a torn value", i)
				}
				pos += n
				fields = append(fields, StreamField{Name: payload[nr.off : nr.off+nr.n], Value: payload[pos : pos+int(vl)]})
				pos += int(vl)
			}
		} else {
			nf, n := binary.Uvarint(payload[pos:])
			if n <= 0 {
				return fmt.Errorf("obs1: stream run frame %d has a torn field count", i)
			}
			pos += n
			for j := 0; j < int(nf); j++ {
				nl, n := binary.Uvarint(payload[pos:])
				if n <= 0 || pos+n+int(nl) > len(payload) {
					return fmt.Errorf("obs1: stream run frame %d has a torn name", i)
				}
				pos += n
				nameOff := pos
				pos += int(nl)
				if i == 0 {
					names = append(names, nameRef{off: nameOff, n: int(nl)})
				}
				vl, n := binary.Uvarint(payload[pos:])
				if n <= 0 || pos+n+int(vl) > len(payload) {
					return fmt.Errorf("obs1: stream run frame %d has a torn value", i)
				}
				pos += n
				fields = append(fields, StreamField{Name: payload[nameOff : nameOff+int(nl)], Value: payload[pos : pos+int(vl)]})
				pos += int(vl)
			}
		}
		if flags&streamFrameDeleted != 0 {
			continue
		}
		if err := fn(id, fields); err != nil {
			return err
		}
	}
	return nil
}

// StreamRunIter streams the plan's live entries from refs[start:] in ID
// order, paging one decoded block at a time with the same window and
// prefetch discipline as ZsetRunIter. fetch returns the decoded segment
// block for a ref; the iterator parses each run's own chunk frame inside
// it (ColdChunkFrame), so the exact 16-byte first ID comes off the disc
// rather than the ms-only directory lift. Entry bytes copy out of the
// fetch buffer because the window outlives the walk. A backward entry ID
// across the stream is *errp = ErrDiscOrder, the partition interleave
// guard; walk and fetch errors land in *errp the same way.
func StreamRunIter(refs []DirRef, start int, fetch func(DirRef) ([]byte, error), prefetch func(DirRef), errp *error) func() (StreamEntry, bool) {
	var window []StreamEntry
	var lastID StreamRunID
	var haveID bool
	var lastObj string
	var lastOff uint64
	var data []byte
	i, w := start, 0
	return func() (StreamEntry, bool) {
		for {
			if w < len(window) {
				e := window[w]
				w++
				if haveID && e.ID.Less(lastID) {
					*errp = ErrDiscOrder
					return StreamEntry{}, false
				}
				lastID, haveID = e.ID, true
				return e, true
			}
			if i >= len(refs) {
				return StreamEntry{}, false
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
					return StreamEntry{}, false
				}
				data, lastObj, lastOff = d, ref.ObjKey, ref.Block.Offset
			}
			frame, err := ColdChunkFrame(data, ref.OffInBlock)
			if err != nil {
				*errp = err
				return StreamEntry{}, false
			}
			if len(frame.Disc) != 16 {
				*errp = fmt.Errorf("obs1: stream run disc is %d bytes, want the 16-byte (ms, seq) pair", len(frame.Disc))
				return StreamEntry{}, false
			}
			first := StreamRunID{
				Ms:  binary.BigEndian.Uint64(frame.Disc[0:8]),
				Seq: binary.BigEndian.Uint64(frame.Disc[8:16]),
			}
			window, w = window[:0], 0
			err = WalkStreamRun(frame.Payload, first, int(frame.Count), func(id StreamRunID, fields []StreamField) error {
				fs := make([]StreamField, len(fields))
				for k, f := range fields {
					fs[k] = StreamField{
						Name:  append([]byte(nil), f.Name...),
						Value: append([]byte(nil), f.Value...),
					}
				}
				window = append(window, StreamEntry{ID: id, Fields: fs})
				return nil
			})
			if err != nil {
				*errp = err
				return StreamEntry{}, false
			}
		}
	}
}
