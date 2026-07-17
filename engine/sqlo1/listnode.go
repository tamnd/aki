package sqlo1

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// The doc 07 noded layout, slice 2 of the list model. A list past the
// inline thresholds keeps its elements in node segments under a minted
// plane, and the root holds the positional fence: the ordered array of
// nodes with per-node element counts. List order is the fence array
// order, not segid order; segids are just allocation names.
//
// The lab verdicts are baked here: node_max 4032 and the 128 element
// cap from lnode (#1072), merge_max 2016 from lmid (#1073).
const (
	// listNodeMax is the node cut threshold: a push that would grow a
	// node's encoded payload past it cuts a fresh node instead. Like
	// hashSegMax this is a writer policy, not a format fact, and the
	// node decode puts no upper bound on payload size because an
	// element larger than the threshold gets a node of its own.
	listNodeMax = 4032

	// listNodeMaxElems caps elements per node, matching the inline
	// boundary so the first cut out of inline is exactly one node.
	listNodeMaxElems = 128

	// listMergeMax is the lazy half-merge threshold from the lmid
	// verdict: two neighbors merge when their combined payload fits
	// it. Edge pops drop emptied nodes whole, so the deque paths never
	// need it; the middle-op slice (LINSERT, LREM) wields it against
	// the decimation adversary.
	listMergeMax = 2016

	// listNodeHdrLen is the node payload header: u16 n, u16 reserved.
	// No min_expire slot: elements have no per-element TTL, and the
	// list does not claim W3 reconciliation (its roots always frame in
	// full), so nothing reads segment counts store-side.
	listNodeHdrLen = 4

	// Noded root layout:
	//
	//	u8   sub        // listSubNoded
	//	u8   lflags     // bit0 fence-paged (the paging slice's)
	//	u16  reserved
	//	u32  rootgen
	//	u64  rooth      // shared planed prefix, offset 8
	//	u64  count
	//	u64  next_segid
	//	u32  node_count
	//	fence: node_count x { u64 segid_lo48|meta_hi16, u32 count }
	listNodeRootHdrLen = 36
	listFenceEntLen    = 12

	// listFenceMaxNodes bounds the inline fence to the same root
	// budget the inline tier used; a fence past it is the paging
	// slice's layout.
	listFenceMaxNodes = (listInlineMax - listNodeRootHdrLen) / listFenceEntLen

	// lflagFencePaged marks a paged fence; nothing writes it before
	// the paging slice lands.
	lflagFencePaged = 1 << 0
)

// errListFencePaged marks a path the fence paging slice owns: a fence
// grown past listFenceMaxNodes, or a root already carrying the paged
// flag. Nothing writes a paged root yet, so only fence growth reaches
// it, and a refused write is side-effect free.
var errListFencePaged = errors.New("sqlo1: list fence is past the inline cap; the fence paging slice owns this path")

// listFenceEnt is one fence slot: the node's segid, its advisory meta
// (reserved zero for now), and its element count. Fence counts are
// exact, so the decode can cross-check their sum against the root
// count, a stronger invariant than the hash fence can state.
type listFenceEnt struct {
	segid uint64
	meta  uint16
	count uint32
}

// listNodeRoot is the decoded noded root. The fence is copied out of
// the read on decode, so it survives the segment reads an op does
// next.
type listNodeRoot struct {
	lflags    uint8
	rootgen   uint32
	rooth     uint64
	count     uint64
	nextSegid uint64
	fence     []listFenceEnt
}

// appendListNodeRoot encodes r onto dst.
func appendListNodeRoot(dst []byte, r *listNodeRoot) []byte {
	var h [listNodeRootHdrLen]byte
	h[0] = listSubNoded
	h[1] = r.lflags
	binary.LittleEndian.PutUint32(h[4:], r.rootgen)
	binary.LittleEndian.PutUint64(h[8:], r.rooth)
	binary.LittleEndian.PutUint64(h[16:], r.count)
	binary.LittleEndian.PutUint64(h[24:], r.nextSegid)
	binary.LittleEndian.PutUint32(h[32:], uint32(len(r.fence)))
	dst = append(dst, h[:]...)
	for _, e := range r.fence {
		var b [listFenceEntLen]byte
		binary.LittleEndian.PutUint64(b[:], e.segid|uint64(e.meta)<<48)
		binary.LittleEndian.PutUint32(b[8:], e.count)
		dst = append(dst, b[:]...)
	}
	return dst
}

// decodeListNodeRoot validates everything and copies the fence into
// the caller's scratch, so the returned root does not alias v and
// stays valid across the node reads that follow.
func decodeListNodeRoot(v []byte, fence []listFenceEnt) (listNodeRoot, error) {
	if len(v) < listNodeRootHdrLen {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root of %d bytes has no header", len(v))
	}
	if v[0] != listSubNoded {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root has sub %d", v[0])
	}
	if v[1] != 0 {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root has lflags %#x; only the flat fence lands before the paging slice", v[1])
	}
	if v[2] != 0 || v[3] != 0 {
		return listNodeRoot{}, errors.New("sqlo1: noded list root has nonzero reserved bytes")
	}
	r := listNodeRoot{
		lflags:    v[1],
		rootgen:   binary.LittleEndian.Uint32(v[4:]),
		rooth:     binary.LittleEndian.Uint64(v[8:]),
		count:     binary.LittleEndian.Uint64(v[16:]),
		nextSegid: binary.LittleEndian.Uint64(v[24:]),
	}
	if r.rootgen == 0 {
		return listNodeRoot{}, errors.New("sqlo1: noded list root has rootgen 0")
	}
	if r.count == 0 {
		return listNodeRoot{}, errors.New("sqlo1: noded list root has count 0; an empty list is a deleted key")
	}
	n := int(binary.LittleEndian.Uint32(v[32:]))
	if n == 0 || n > listFenceMaxNodes {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root has node_count %d out of range", n)
	}
	if len(v) != listNodeRootHdrLen+n*listFenceEntLen {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root of %d bytes does not fit %d fence entries", len(v), n)
	}
	sum := uint64(0)
	p := v[listNodeRootHdrLen:]
	for i := range n {
		x := binary.LittleEndian.Uint64(p)
		e := listFenceEnt{
			segid: x & (1<<48 - 1),
			meta:  uint16(x >> 48),
			count: binary.LittleEndian.Uint32(p[8:]),
		}
		if e.segid >= r.nextSegid {
			return listNodeRoot{}, fmt.Errorf("sqlo1: fence entry %d has segid %d at or past next_segid %d", i, e.segid, r.nextSegid)
		}
		if e.count == 0 {
			return listNodeRoot{}, fmt.Errorf("sqlo1: fence entry %d has count 0; edge pops drop empty nodes whole", i)
		}
		sum += uint64(e.count)
		fence = append(fence, e)
		p = p[listFenceEntLen:]
	}
	if sum != r.count {
		return listNodeRoot{}, fmt.Errorf("sqlo1: fence counts sum to %d, root count says %d", sum, r.count)
	}
	r.fence = fence
	return r, nil
}

// listNode is a decoded node: the element count and the raw element
// region, the same u32-length entries the inline tier uses. The region
// aliases the read and dies on the next Tiered call.
type listNode struct {
	n     int
	elems []byte
}

// putListNodeHdr fills the node header slot; the buffer already holds
// listNodeHdrLen bytes with contents unspecified (grow's contract), so
// every header byte is written.
func putListNodeHdr(b []byte, n int) {
	binary.LittleEndian.PutUint16(b, uint16(n))
	binary.LittleEndian.PutUint16(b[2:], 0)
}

// decodeListNode validates the header and walks the element region.
// n = 0 is corrupt, not empty: a pop that empties a node drops the
// node record instead of writing it empty. There is no upper size
// bound; an oversize element legitimately owns a node bigger than
// listNodeMax.
func decodeListNode(v []byte) (listNode, error) {
	if len(v) < listNodeHdrLen {
		return listNode{}, fmt.Errorf("sqlo1: list node of %d bytes has no header", len(v))
	}
	n := int(binary.LittleEndian.Uint16(v))
	if n == 0 || n > listNodeMaxElems {
		return listNode{}, fmt.Errorf("sqlo1: list node count %d out of range", n)
	}
	if binary.LittleEndian.Uint16(v[2:]) != 0 {
		return listNode{}, errors.New("sqlo1: list node has nonzero reserved bytes")
	}
	walked := 0
	for p := v[listNodeHdrLen:]; len(p) > 0; walked++ {
		if len(p) < listElemHdrLen {
			return listNode{}, fmt.Errorf("sqlo1: list node element %d torn at the header", walked)
		}
		el := int(binary.LittleEndian.Uint32(p))
		if len(p) < listElemHdrLen+el {
			return listNode{}, fmt.Errorf("sqlo1: list node element %d torn at %d of %d bytes", walked, len(p)-listElemHdrLen, el)
		}
		p = p[listElemHdrLen+el:]
	}
	if walked != n {
		return listNode{}, fmt.Errorf("sqlo1: list node walks %d elements, header says %d", walked, n)
	}
	return listNode{n: n, elems: v[listNodeHdrLen:]}, nil
}
