package sqlo1

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// The doc 10 stream root, slice 2 of the stream model. Streams have no
// inline tier: the first XADD mints a plane and cuts the first run, and
// OBJECT ENCODING answers stream at every size the way Redis does. The
// root carries the ID-keyed fence over the entry runs plus every
// summary field XADD validation, XLEN, and XINFO STREAM read, all O(1)
// (X-I2). Unlike a list root, a stream root with count 0 is legal: a
// fully trimmed stream still exists and keeps its last-generated ID.
const (
	// streamSub is the doc 10 stream root layout, taking the type-tag
	// slot of the count-up convention like the set and zset subs. It
	// carries the shared planed prefix (rootgen at 4, rooth at 8) that
	// planedRootInfo reads.
	streamSub = TagStream

	// Root layout:
	//
	//	u8   sub          // streamSub
	//	u8   xflags       // reserved zero; bit0 will mark a paged fence
	//	u16  reserved
	//	u32  rootgen
	//	u64  rooth        // shared planed prefix, offset 8
	//	u64  count        // live entries, tombstones excluded
	//	u64  entries_added
	//	u64  last_ms, last_seq       // last generated ID, survives trims
	//	u64  maxdel_ms, maxdel_seq   // largest XDELed ID
	//	u64  next_segid
	//	u32  group_count
	//	u32  fence_n
	//	fence: fence_n x { u64 ms, u64 seq, u64 segid_lo48|meta_hi16, u32 count }
	//
	// The fence maps ID ranges to runs in ID order: entry i's base is
	// its run's first ID, and every ID in run i is below entry i+1's
	// base. Counts are live counts, so range math and XLEN never decode
	// interior runs, and the decode cross-checks their sum against the
	// root count, the same exactness the list fence states.
	streamRootHdrLen  = 80
	streamFenceEntLen = 28
)

// streamFenceMaxRuns bounds the flat fence to the same root budget the
// other collections use. A var, not a const, so tests reach the refusal
// below without building thousands of entries.
var streamFenceMaxRuns = (listInlineMax - streamRootHdrLen) / streamFenceEntLen

// errStreamFenceFull is this slice's honest edge: fence paging (kind 3
// pages, the doc 10 two-level structure) is a later T6 slice, and until
// it lands an XADD that needs a run past the flat cap refuses
// side-effect free.
var errStreamFenceFull = errors.New("sqlo1: stream fence is full; fence paging has not landed yet")

// streamFenceEnt is one fence slot: the run's base ID, its segid, the
// advisory meta (reserved zero for now), and its live entry count.
type streamFenceEnt struct {
	base  streamID
	segid uint64
	meta  uint16
	count uint32
}

// streamRoot is the decoded stream root. The fence is copied out of the
// read on decode, so it survives the run reads an op does next.
type streamRoot struct {
	rootgen    uint32
	rooth      uint64
	count      uint64
	added      uint64
	last       streamID
	maxDel     streamID
	nextSegid  uint64
	groupCount uint32
	fence      []streamFenceEnt
}

// appendStreamRoot encodes r onto dst.
func appendStreamRoot(dst []byte, r *streamRoot) []byte {
	var h [streamRootHdrLen]byte
	h[0] = streamSub
	binary.LittleEndian.PutUint32(h[4:], r.rootgen)
	binary.LittleEndian.PutUint64(h[8:], r.rooth)
	binary.LittleEndian.PutUint64(h[16:], r.count)
	binary.LittleEndian.PutUint64(h[24:], r.added)
	binary.LittleEndian.PutUint64(h[32:], r.last.ms)
	binary.LittleEndian.PutUint64(h[40:], r.last.seq)
	binary.LittleEndian.PutUint64(h[48:], r.maxDel.ms)
	binary.LittleEndian.PutUint64(h[56:], r.maxDel.seq)
	binary.LittleEndian.PutUint64(h[64:], r.nextSegid)
	binary.LittleEndian.PutUint32(h[72:], r.groupCount)
	binary.LittleEndian.PutUint32(h[76:], uint32(len(r.fence)))
	dst = append(dst, h[:]...)
	for _, e := range r.fence {
		var b [streamFenceEntLen]byte
		binary.LittleEndian.PutUint64(b[0:], e.base.ms)
		binary.LittleEndian.PutUint64(b[8:], e.base.seq)
		binary.LittleEndian.PutUint64(b[16:], e.segid|uint64(e.meta)<<48)
		binary.LittleEndian.PutUint32(b[24:], e.count)
		dst = append(dst, b[:]...)
	}
	return dst
}

// decodeStreamRoot validates everything and copies the fence into the
// caller's scratch, so the returned root does not alias v and stays
// valid across the run reads that follow.
func decodeStreamRoot(v []byte, fence []streamFenceEnt) (streamRoot, error) {
	if len(v) < streamRootHdrLen {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root of %d bytes has no header", len(v))
	}
	if v[0] != streamSub {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root has sub %d", v[0])
	}
	if v[1] != 0 {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root has unknown xflags %#x", v[1])
	}
	if v[2] != 0 || v[3] != 0 {
		return streamRoot{}, errors.New("sqlo1: stream root has nonzero reserved bytes")
	}
	r := streamRoot{
		rootgen:    binary.LittleEndian.Uint32(v[4:]),
		rooth:      binary.LittleEndian.Uint64(v[8:]),
		count:      binary.LittleEndian.Uint64(v[16:]),
		added:      binary.LittleEndian.Uint64(v[24:]),
		last:       streamID{ms: binary.LittleEndian.Uint64(v[32:]), seq: binary.LittleEndian.Uint64(v[40:])},
		maxDel:     streamID{ms: binary.LittleEndian.Uint64(v[48:]), seq: binary.LittleEndian.Uint64(v[56:])},
		nextSegid:  binary.LittleEndian.Uint64(v[64:]),
		groupCount: binary.LittleEndian.Uint32(v[72:]),
	}
	if r.rootgen == 0 {
		return streamRoot{}, errors.New("sqlo1: stream root has rootgen 0")
	}
	if r.added < r.count {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root count %d exceeds entries added %d", r.count, r.added)
	}
	n := int(binary.LittleEndian.Uint32(v[76:]))
	if n > streamFenceMaxRuns {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root fence count %d out of range", n)
	}
	if len(v) != streamRootHdrLen+n*streamFenceEntLen {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root of %d bytes does not fit %d fence entries", len(v), n)
	}
	sum := uint64(0)
	p := v[streamRootHdrLen:]
	prev := streamID{}
	for i := range n {
		x := binary.LittleEndian.Uint64(p[16:])
		e := streamFenceEnt{
			base:  streamID{ms: binary.LittleEndian.Uint64(p[0:]), seq: binary.LittleEndian.Uint64(p[8:])},
			segid: x & (1<<48 - 1),
			meta:  uint16(x >> 48),
			count: binary.LittleEndian.Uint32(p[24:]),
		}
		if e.base == (streamID{}) {
			return streamRoot{}, fmt.Errorf("sqlo1: stream fence entry %d has the zero base ID", i)
		}
		if i > 0 && !prev.less(e.base) {
			return streamRoot{}, fmt.Errorf("sqlo1: stream fence entry %d has base out of ID order", i)
		}
		if e.segid >= r.nextSegid {
			return streamRoot{}, fmt.Errorf("sqlo1: stream fence entry %d has segid %d at or past next_segid %d", i, e.segid, r.nextSegid)
		}
		if e.count == 0 {
			return streamRoot{}, fmt.Errorf("sqlo1: stream fence entry %d has count 0; emptied runs drop whole", i)
		}
		sum += uint64(e.count)
		prev = e.base
		fence = append(fence, e)
		p = p[streamFenceEntLen:]
	}
	if sum != r.count {
		return streamRoot{}, fmt.Errorf("sqlo1: stream fence counts sum to %d, root count says %d", sum, r.count)
	}
	if n > 0 && r.last.less(prev) {
		return streamRoot{}, errors.New("sqlo1: stream root last ID is below the tail run's base")
	}
	r.fence = fence
	return r, nil
}
