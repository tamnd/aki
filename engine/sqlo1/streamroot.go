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
	//	u8   xflags       // bit0 fence-paged
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
	//
	// A paged root holds the page index in the entry area instead of
	// the fence: each entry names a fence page record (subkey kind 3
	// under the same plane, pageids minted from next_segid like runs),
	// its base is the base ID of the page's first run, and its count is
	// the page's live entry total, so an ID seek prefix-checks two
	// levels, root then page, and a range decodes only boundary runs in
	// boundary pages.
	streamRootHdrLen  = 80
	streamFenceEntLen = 28

	// streamPageHdrLen is the fence page payload header: u16 n, u16
	// reserved, then n fence entries in ID order.
	streamPageHdrLen = 4

	// streamXflagFencePaged marks a paged fence, the one-way second
	// rung of the fence ladder; a paged root never goes back to flat.
	streamXflagFencePaged = 1 << 0
)

// The fence fanouts. Vars, not consts, so the paged ladder (the flat
// cap, page growth, the third-level refusal) is reachable in test-sized
// streams; nothing outside tests writes them.
var (
	// streamFenceMaxRuns bounds the flat fence to the same root budget
	// the other collections use; a cut past it pages the fence.
	streamFenceMaxRuns = (listInlineMax - streamRootHdrLen) / streamFenceEntLen

	// streamFencePageMax is the page fanout: 4 + 143*28 = 4008 bytes,
	// so a full page rides one drain frame comfortably, the list page's
	// sizing rule at the stream entry width.
	streamFencePageMax = 143

	// streamFencePageIdxMax bounds the root's page index. The xcatchup
	// marquee needs ~550 pages at 10^7 small entries, so 4096 leaves an
	// order of magnitude of headroom while capping the worst root at
	// ~112 KiB, framed once per drain window by coalescing.
	streamFencePageIdxMax = 4096
)

// errStreamFenceThirdLevel is the ladder's end: a stream whose page
// index cannot take another page, ~75M small entries at production
// fanouts, feed depths trim owns in practice. A refused XADD is
// side-effect free.
var errStreamFenceThirdLevel = errors.New("sqlo1: stream fence page index is full")

// streamFenceEnt is one fence slot: the run's base ID, its segid, the
// advisory meta (reserved zero for now), and its live entry count.
type streamFenceEnt struct {
	base  streamID
	segid uint64
	meta  uint16
	count uint32
}

// streamRoot is the decoded stream root. The fence (flat) or the page
// index (paged) is copied out of the read on decode, so it survives the
// run and page reads an op does next. Exactly one of fence and pidx is
// populated, by the paged bit.
type streamRoot struct {
	paged      bool
	rootgen    uint32
	rooth      uint64
	count      uint64
	added      uint64
	last       streamID
	maxDel     streamID
	nextSegid  uint64
	groupCount uint32
	fence      []streamFenceEnt
	pidx       []streamFenceEnt
}

// appendStreamFenceEnts encodes an entry array, the shared shape of the
// flat fence, the page index, and the page payload body.
func appendStreamFenceEnts(dst []byte, ents []streamFenceEnt) []byte {
	for _, e := range ents {
		var b [streamFenceEntLen]byte
		binary.LittleEndian.PutUint64(b[0:], e.base.ms)
		binary.LittleEndian.PutUint64(b[8:], e.base.seq)
		binary.LittleEndian.PutUint64(b[16:], e.segid|uint64(e.meta)<<48)
		binary.LittleEndian.PutUint32(b[24:], e.count)
		dst = append(dst, b[:]...)
	}
	return dst
}

// appendStreamRoot encodes r onto dst.
func appendStreamRoot(dst []byte, r *streamRoot) []byte {
	ents := r.fence
	xflags := uint8(0)
	if r.paged {
		ents = r.pidx
		xflags = streamXflagFencePaged
	}
	var h [streamRootHdrLen]byte
	h[0] = streamSub
	h[1] = xflags
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
	binary.LittleEndian.PutUint32(h[76:], uint32(len(ents)))
	dst = append(dst, h[:]...)
	return appendStreamFenceEnts(dst, ents)
}

// decodeStreamFenceEnts walks n encoded entries, validating each (a
// nonzero base, strict ID order, segid below nextSegid, a live count),
// and appends them onto ents. what names the array in errors. The
// running count sum comes back for the caller's cross-check.
func decodeStreamFenceEnts(p []byte, n int, nextSegid uint64, what string, ents []streamFenceEnt) ([]streamFenceEnt, uint64, error) {
	sum := uint64(0)
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
			return nil, 0, fmt.Errorf("sqlo1: stream %s entry %d has the zero base ID", what, i)
		}
		if i > 0 && !prev.less(e.base) {
			return nil, 0, fmt.Errorf("sqlo1: stream %s entry %d has base out of ID order", what, i)
		}
		if e.segid >= nextSegid {
			return nil, 0, fmt.Errorf("sqlo1: stream %s entry %d has segid %d at or past next_segid %d", what, i, e.segid, nextSegid)
		}
		if e.count == 0 {
			return nil, 0, fmt.Errorf("sqlo1: stream %s entry %d has count 0; emptied runs and pages drop whole", what, i)
		}
		sum += uint64(e.count)
		prev = e.base
		ents = append(ents, e)
		p = p[streamFenceEntLen:]
	}
	return ents, sum, nil
}

// decodeStreamRoot validates everything and copies the entry array into
// the caller's scratch (fence flat, pidx paged), so the returned root
// does not alias v and stays valid across the run and page reads that
// follow.
func decodeStreamRoot(v []byte, fence, pidx []streamFenceEnt) (streamRoot, error) {
	if len(v) < streamRootHdrLen {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root of %d bytes has no header", len(v))
	}
	if v[0] != streamSub {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root has sub %d", v[0])
	}
	if v[1]&^uint8(streamXflagFencePaged) != 0 {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root has unknown xflags %#x", v[1])
	}
	if v[2] != 0 || v[3] != 0 {
		return streamRoot{}, errors.New("sqlo1: stream root has nonzero reserved bytes")
	}
	r := streamRoot{
		paged:      v[1]&streamXflagFencePaged != 0,
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
	bound, what := streamFenceMaxRuns, "fence"
	if r.paged {
		bound, what = streamFencePageIdxMax, "page index"
	}
	if n > bound {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root %s count %d out of range", what, n)
	}
	if len(v) != streamRootHdrLen+n*streamFenceEntLen {
		return streamRoot{}, fmt.Errorf("sqlo1: stream root of %d bytes does not fit %d %s entries", len(v), n, what)
	}
	dst := fence
	if r.paged {
		dst = pidx
	}
	ents, sum, err := decodeStreamFenceEnts(v[streamRootHdrLen:], n, r.nextSegid, what, dst)
	if err != nil {
		return streamRoot{}, err
	}
	if sum != r.count {
		return streamRoot{}, fmt.Errorf("sqlo1: stream %s counts sum to %d, root count says %d", what, sum, r.count)
	}
	if n > 0 && r.last.less(ents[len(ents)-1].base) {
		return streamRoot{}, errors.New("sqlo1: stream root last ID is below the tail run's base")
	}
	if r.paged {
		r.pidx = ents
	} else {
		r.fence = ents
	}
	return r, nil
}

// appendStreamFencePage encodes a fence page payload: u16 n, u16
// reserved, then the entries.
func appendStreamFencePage(dst []byte, ents []streamFenceEnt) []byte {
	var h [streamPageHdrLen]byte
	binary.LittleEndian.PutUint16(h[:], uint16(len(ents)))
	dst = append(dst, h[:]...)
	return appendStreamFenceEnts(dst, ents)
}

// decodeStreamFencePage validates a page payload and copies its entries
// into the caller's scratch. The caller cross-checks the returned sum
// and the first entry's base against the parent index entry, the
// two-level invariant.
func decodeStreamFencePage(v []byte, nextSegid uint64, ents []streamFenceEnt) ([]streamFenceEnt, uint64, error) {
	if len(v) < streamPageHdrLen {
		return nil, 0, fmt.Errorf("sqlo1: stream fence page of %d bytes has no header", len(v))
	}
	n := int(binary.LittleEndian.Uint16(v))
	if n == 0 || n > streamFencePageMax {
		return nil, 0, fmt.Errorf("sqlo1: stream fence page count %d out of range", n)
	}
	if binary.LittleEndian.Uint16(v[2:]) != 0 {
		return nil, 0, errors.New("sqlo1: stream fence page has nonzero reserved bytes")
	}
	if len(v) != streamPageHdrLen+n*streamFenceEntLen {
		return nil, 0, fmt.Errorf("sqlo1: stream fence page of %d bytes does not fit %d entries", len(v), n)
	}
	return decodeStreamFenceEnts(v[streamPageHdrLen:], n, nextSegid, "fence page", ents)
}
