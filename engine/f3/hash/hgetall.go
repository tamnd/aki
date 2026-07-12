package hash

import (
	"github.com/tamnd/aki/f3srv/resp"
)

// HGETALL/HKEYS/HVALS over the native band stream (spec 2064/f3/10 section 7.5),
// the field-table twin of set's SMEMBERS (set/smembers.go): a hash with a million
// fields must not build one giant reply buffer, so the reply is framed on the fly
// and pumped through the shard's bounded streaming ring, the same window a chunked
// GET rides. HGETALL is the headline m=1000 memory-bandwidth row of this milestone;
// its cost on a large hash is reading the field and value slab, so the streaming
// path keeps the reply's peak footprint at the ring window, not the whole hash.
//
// The stream carries a snapshot of the draw-vector ordinals taken at command time
// plus a pin on the table (field.go, the streams field): the field and value bytes
// it reads from the live slab stay put while it drains, because record reuse and
// slab compaction both stand down until the last stream closes. The reply is the
// hash as of the command, which is what these verbs promise: a field removed after
// the snapshot is still returned, a field added after it is not.

// enumMode selects which halves of each field-value pair the enumeration emits.
type enumMode uint8

const (
	enumPairs enumMode = iota // HGETALL: field then value
	enumKeys                  // HKEYS: field only
	enumVals                  // HVALS: value only
)

// enumStream is the StreamRaw source for one HGETALL/HKEYS/HVALS: it yields the
// whole multi-bulk reply, the array header and every element, already framed. The
// encoder is resumable across Next calls: buf holds the element being emitted and
// off is how much of it has gone out, so an element wider than a chunk straddles
// the boundary without materializing more than itself. elem is the flat element
// cursor: for HGETALL element 2k is field k and 2k+1 is value k, for HKEYS/HVALS
// element k is the one half of pair k.
type enumStream struct {
	ft   *ftable
	ords []uint32
	mode enumMode

	total   int    // element count: 2*len(ords) for pairs, len(ords) otherwise
	elem    int    // next element to frame
	buf     []byte // the element currently being emitted (header, then bulks)
	off     int    // bytes of buf already copied to the wire
	started bool   // array header framed into buf yet
}

// locate maps a flat element index to its record ordinal and whether the value
// half (not the field half) is wanted.
func (e *enumStream) locate(elem int) (ord uint32, value bool) {
	switch e.mode {
	case enumKeys:
		return e.ords[elem], false
	case enumVals:
		return e.ords[elem], true
	default: // enumPairs
		return e.ords[elem/2], elem%2 == 1
	}
}

// Next fills dst with the next run of reply bytes and returns the count, zero once
// the reply is exhausted. It frames one element at a time into buf and copies from
// there, so its working set is one field or value plus the chunk, never the whole
// hash.
func (e *enumStream) Next(dst []byte) (int, error) {
	n := 0
	for n < len(dst) {
		if e.off >= len(e.buf) {
			switch {
			case !e.started:
				e.buf = resp.AppendArrayHeader(e.buf[:0], e.total)
				e.started = true
				e.off = 0
			case e.elem < e.total:
				ord, value := e.locate(e.elem)
				b := e.ft.fieldByOrd(ord)
				if value {
					b = e.ft.valueByOrd(ord)
				}
				e.buf = resp.AppendBulk(e.buf[:0], b)
				e.elem++
				e.off = 0
			default:
				return n, nil // every element framed and copied
			}
		}
		c := copy(dst[n:], e.buf[e.off:])
		e.off += c
		n += c
	}
	return n, nil
}

// Release unpins the table when the stream drains, on the owner goroutine off the
// pump (stream.finish). It is the match to the pinStream the handler took, letting
// record reuse and slab compaction resume.
func (e *enumStream) Release() { e.ft.unpinStream() }

// enumTotal is the exact byte width of the enumeration reply over the native band:
// the array header plus every element's bulk frame. The handler needs it to decide
// whether the reply is worth streaming and to commit the wire to a length before
// the first chunk goes out.
func (f *ftable) enumTotal(mode enumMode) int64 {
	n := f.drawLen()
	elems := n
	if mode == enumPairs {
		elems = 2 * n
	}
	tot := int64(arrayHeaderLen(elems))
	for i := 0; i < n; i++ {
		ord := f.ordAt(i)
		if mode != enumVals {
			tot += bulkFrameLen(f.flenByOrd(ord))
		}
		if mode != enumKeys {
			tot += bulkFrameLen(f.vlenByOrd(ord))
		}
	}
	return tot
}

// pinEnumStream snapshots the live draw-vector ordinals, pins the table, and
// returns the stream source. The snapshot is 4 bytes per field, the only copy the
// enumeration makes; the field and value bytes themselves are never duplicated.
func (f *ftable) pinEnumStream(mode enumMode) *enumStream {
	n := f.drawLen()
	ords := make([]uint32, n)
	for i := 0; i < n; i++ {
		ords[i] = f.ordAt(i)
	}
	f.pinStream()
	total := n
	if mode == enumPairs {
		total = 2 * n
	}
	return &enumStream{ft: f, ords: ords, mode: mode, total: total}
}

// decLen is the decimal digit count of a non-negative n, for sizing a RESP frame
// without formatting it.
func decLen(n int) int {
	d := 1
	for n >= 10 {
		n /= 10
		d++
	}
	return d
}

// arrayHeaderLen is the width of a *N\r\n header.
func arrayHeaderLen(n int) int { return 1 + decLen(n) + 2 }

// bulkFrameLen is the width of a $L\r\n<L bytes>\r\n bulk.
func bulkFrameLen(l int) int64 { return int64(1 + decLen(l) + 2 + l + 2) }
