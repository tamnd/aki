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
	//	u8   lflags     // bit0 fence-paged
	//	u16  reserved
	//	u32  rootgen
	//	u64  rooth      // shared planed prefix, offset 8
	//	u64  count
	//	u64  next_segid
	//	u32  node_count // page_count when paged
	//	fence: node_count x { u64 segid_lo48|meta_hi16, u32 count }
	//
	// A paged root holds the page index in the entry area instead of
	// the fence: each entry names a fence page record (subkey kind 3
	// under the same plane, pageids minted from next_segid like nodes)
	// and carries the page's element total, so positional seeks
	// prefix-sum two levels, root then page, and land in 3 records.
	listNodeRootHdrLen = 36
	listFenceEntLen    = 12

	// listPageHdrLen is the fence page payload header: u16 n, u16
	// reserved, then n fence entries in list order.
	listPageHdrLen = 4

	// lflagFencePaged marks a paged fence, the one-way second rung of
	// the fence ladder; a paged root never goes back to flat.
	lflagFencePaged = 1 << 0
)

// The fence fanouts. Vars, not consts, so the paged ladder (the flat
// cap, page splits, the third-level refusal) is reachable in
// test-sized lists; nothing outside tests writes them.
var (
	// listFenceMaxNodes bounds the flat fence to the same root budget
	// the inline tier used; a push past it pages the fence.
	listFenceMaxNodes = (listInlineMax - listNodeRootHdrLen) / listFenceEntLen

	// listFencePageMax is the doc 07 page fanout: entries per fence
	// page, sized so a page rides one drain frame comfortably.
	listFencePageMax = 330

	// listFencePageIdxMax bounds the root's page index. The lqueue
	// marquee needs ~1600 pages at depth 10^7 on 200 B elements, so
	// the bound sits at 4096, a ~49 KiB root at worst, which drain
	// coalescing frames once per window, not per op.
	listFencePageIdxMax = 4096
)

// errListFenceThirdLevel is the ladder's end: a list whose page index
// cannot take another page. At production fanouts that is ~54 billion
// one-element nodes, far past doc 02's per-key ambitions; a refused
// write is side-effect free.
var errListFenceThirdLevel = errors.New("sqlo1: list fence page index is full")

// listFenceEnt is one fence slot: the node's segid, its advisory meta
// (reserved zero for now), and its element count. Fence counts are
// exact, so the decode can cross-check their sum against the root
// count, a stronger invariant than the hash fence can state.
type listFenceEnt struct {
	segid uint64
	meta  uint16
	count uint32
}

// listNodeRoot is the decoded noded root. The fence (flat) or the page
// index (paged) is copied out of the read on decode, so it survives
// the segment reads an op does next. Exactly one of fence and pidx is
// populated, by the paged bit.
type listNodeRoot struct {
	lflags    uint8
	paged     bool
	rootgen   uint32
	rooth     uint64
	count     uint64
	nextSegid uint64
	fence     []listFenceEnt
	pidx      []listFenceEnt
}

// appendListFenceEnts encodes an entry array, the shared shape of the
// flat fence, the page index, and the page payload body.
func appendListFenceEnts(dst []byte, ents []listFenceEnt) []byte {
	for _, e := range ents {
		var b [listFenceEntLen]byte
		binary.LittleEndian.PutUint64(b[:], e.segid|uint64(e.meta)<<48)
		binary.LittleEndian.PutUint32(b[8:], e.count)
		dst = append(dst, b[:]...)
	}
	return dst
}

// appendListNodeRoot encodes r onto dst.
func appendListNodeRoot(dst []byte, r *listNodeRoot) []byte {
	ents := r.fence
	flags := r.lflags &^ uint8(lflagFencePaged)
	if r.paged {
		ents = r.pidx
		flags |= lflagFencePaged
	}
	var h [listNodeRootHdrLen]byte
	h[0] = listSubNoded
	h[1] = flags
	binary.LittleEndian.PutUint32(h[4:], r.rootgen)
	binary.LittleEndian.PutUint64(h[8:], r.rooth)
	binary.LittleEndian.PutUint64(h[16:], r.count)
	binary.LittleEndian.PutUint64(h[24:], r.nextSegid)
	binary.LittleEndian.PutUint32(h[32:], uint32(len(ents)))
	dst = append(dst, h[:]...)
	return appendListFenceEnts(dst, ents)
}

// decodeListFenceEnts walks n encoded entries, validating each against
// nextSegid and the no-empty rule, and appends them onto ents. what
// names the array in errors. The running element sum comes back for
// the caller's cross-check.
func decodeListFenceEnts(p []byte, n int, nextSegid uint64, what string, ents []listFenceEnt) ([]listFenceEnt, uint64, error) {
	sum := uint64(0)
	for i := range n {
		x := binary.LittleEndian.Uint64(p)
		e := listFenceEnt{
			segid: x & (1<<48 - 1),
			meta:  uint16(x >> 48),
			count: binary.LittleEndian.Uint32(p[8:]),
		}
		if e.segid >= nextSegid {
			return nil, 0, fmt.Errorf("sqlo1: %s entry %d has segid %d at or past next_segid %d", what, i, e.segid, nextSegid)
		}
		if e.count == 0 {
			return nil, 0, fmt.Errorf("sqlo1: %s entry %d has count 0; empty nodes and pages drop whole", what, i)
		}
		sum += uint64(e.count)
		ents = append(ents, e)
		p = p[listFenceEntLen:]
	}
	return ents, sum, nil
}

// decodeListNodeRoot validates everything and copies the entry array
// into the caller's scratch (fence flat, pidx paged), so the returned
// root does not alias v and stays valid across the node and page reads
// that follow.
func decodeListNodeRoot(v []byte, fence, pidx []listFenceEnt) (listNodeRoot, error) {
	if len(v) < listNodeRootHdrLen {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root of %d bytes has no header", len(v))
	}
	if v[0] != listSubNoded {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root has sub %d", v[0])
	}
	if v[1]&^uint8(lflagFencePaged) != 0 {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root has unknown lflags %#x", v[1])
	}
	if v[2] != 0 || v[3] != 0 {
		return listNodeRoot{}, errors.New("sqlo1: noded list root has nonzero reserved bytes")
	}
	r := listNodeRoot{
		lflags:    v[1],
		paged:     v[1]&lflagFencePaged != 0,
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
	bound, what := listFenceMaxNodes, "fence"
	if r.paged {
		bound, what = listFencePageIdxMax, "page index"
	}
	if n == 0 || n > bound {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root has %s count %d out of range", what, n)
	}
	if len(v) != listNodeRootHdrLen+n*listFenceEntLen {
		return listNodeRoot{}, fmt.Errorf("sqlo1: noded list root of %d bytes does not fit %d %s entries", len(v), n, what)
	}
	ents, sum, err := decodeListFenceEnts(v[listNodeRootHdrLen:], n, r.nextSegid, what, fence)
	if err != nil {
		return listNodeRoot{}, err
	}
	if sum != r.count {
		return listNodeRoot{}, fmt.Errorf("sqlo1: %s counts sum to %d, root count says %d", what, sum, r.count)
	}
	if r.paged {
		r.pidx = append(pidx, ents...)
	} else {
		r.fence = ents
	}
	return r, nil
}

// appendListFencePage encodes a fence page payload: u16 n, u16
// reserved, then the entries.
func appendListFencePage(dst []byte, ents []listFenceEnt) []byte {
	var h [listPageHdrLen]byte
	binary.LittleEndian.PutUint16(h[:], uint16(len(ents)))
	dst = append(dst, h[:]...)
	return appendListFenceEnts(dst, ents)
}

// decodeListFencePage validates a page payload and copies its entries
// into the caller's scratch. The caller cross-checks the returned sum
// against the parent index entry's total, the two-level invariant.
func decodeListFencePage(v []byte, nextSegid uint64, ents []listFenceEnt) ([]listFenceEnt, uint64, error) {
	if len(v) < listPageHdrLen {
		return nil, 0, fmt.Errorf("sqlo1: list fence page of %d bytes has no header", len(v))
	}
	n := int(binary.LittleEndian.Uint16(v))
	if n == 0 || n > listFencePageMax {
		return nil, 0, fmt.Errorf("sqlo1: list fence page count %d out of range", n)
	}
	if binary.LittleEndian.Uint16(v[2:]) != 0 {
		return nil, 0, errors.New("sqlo1: list fence page has nonzero reserved bytes")
	}
	if len(v) != listPageHdrLen+n*listFenceEntLen {
		return nil, 0, fmt.Errorf("sqlo1: list fence page of %d bytes does not fit %d entries", len(v), n)
	}
	return decodeListFenceEnts(v[listPageHdrLen:], n, nextSegid, "fence page", ents)
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
