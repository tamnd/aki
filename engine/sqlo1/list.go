package sqlo1

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
)

// The doc 07 list model. A short list lives whole in its root value,
// elements in list order, and answers OBJECT ENCODING listpack the way
// Redis does at the same thresholds; past them it upgrades to the
// noded layout (listnode.go) and answers quicklist. Redis downgrades a
// shrunk quicklist back to listpack; this ladder does not, a one-way
// divergence for the compat section to record.
const (
	// listSubNoded is the doc 07 noded list root layout, the next
	// doc-assigned sub after the segmented hash. It carries the shared
	// planed prefix (rootgen at 4, rooth at 8) that planedRootInfo
	// reads.
	listSubNoded = 3

	// listSubInline is the inline list root, planeless like every
	// 0x10-block sub.
	listSubInline = inlineSubBase | TagList

	// The inline rung's thresholds, doc 07 section 1: a list stays
	// inline while the whole encoded root payload fits listInlineMax
	// and the element count fits listInlineMaxCount, the caps the
	// other inline collections use. Redis's boundary differs in kind:
	// its default list-max-listpack-size -2 is a pure 8 KiB byte cap
	// with no entry-count wall (see testdata/compat/README.md).
	listInlineMax      = 2048
	listInlineMaxCount = 128
)

// Inline root payload layout:
//
//	u8   sub     // listSubInline
//	u8   lflags  // reserved, zero inline (bit0 fence-paged is noded-only)
//	u16  count
//	elements, list order
//
// Each element is a u32 length then the bytes, byte-identical to the
// doc 07 section 2 node entry, so the noded upgrade copies the element
// region wholesale instead of re-encoding.
const (
	listInlineHdrLen = 4
	listElemHdrLen   = 4
)

// putListInlineHdr fills the header slot; the buffer already holds
// listInlineHdrLen bytes with contents unspecified (grow's contract),
// so every header byte is written.
func putListInlineHdr(b []byte, count int) {
	b[0] = listSubInline
	b[1] = 0
	binary.LittleEndian.PutUint16(b[2:], uint16(count))
}

// appendListElem encodes one element onto dst. The caller bounds the
// element structurally: an oversized element cannot fit the inline
// payload, and the noded slice owns its own guard.
func appendListElem(dst, e []byte) []byte {
	var h [listElemHdrLen]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(e)))
	return append(append(dst, h[:]...), e...)
}

// listInline is the decoded inline root: the count and the raw element
// region. The region aliases the decoded payload and dies with it.
type listInline struct {
	count int
	elems []byte
}

// decodeListInline validates everything: a corrupt inline root fails
// at the first read that meets it, never at a later write.
func decodeListInline(v []byte) (listInline, error) {
	if len(v) < listInlineHdrLen {
		return listInline{}, fmt.Errorf("sqlo1: inline list root of %d bytes has no header", len(v))
	}
	if v[0] != listSubInline {
		return listInline{}, fmt.Errorf("sqlo1: inline list root has sub %d", v[0])
	}
	if v[1] != 0 {
		return listInline{}, fmt.Errorf("sqlo1: inline list root has reserved lflags %#x", v[1])
	}
	if len(v) > listInlineMax {
		return listInline{}, fmt.Errorf("sqlo1: inline list root of %d bytes is over the cap", len(v))
	}
	count := int(binary.LittleEndian.Uint16(v[2:]))
	if count == 0 || count > listInlineMaxCount {
		return listInline{}, fmt.Errorf("sqlo1: inline list root count %d out of range", count)
	}
	n := 0
	for p := v[listInlineHdrLen:]; len(p) > 0; n++ {
		if len(p) < listElemHdrLen {
			return listInline{}, fmt.Errorf("sqlo1: inline list element %d torn at the header", n)
		}
		el := int(binary.LittleEndian.Uint32(p))
		if len(p) < listElemHdrLen+el {
			return listInline{}, fmt.Errorf("sqlo1: inline list element %d torn at %d of %d bytes", n, len(p)-listElemHdrLen, el)
		}
		p = p[listElemHdrLen+el:]
	}
	if n != count {
		return listInline{}, fmt.Errorf("sqlo1: inline list walks %d elements, header says %d", n, count)
	}
	return listInline{count: count, elems: v[listInlineHdrLen:]}, nil
}

// listElemIter walks a decoded element region. The decode already
// validated the walk, so next never fails.
type listElemIter struct{ p []byte }

func (it *listElemIter) next() ([]byte, bool) {
	if len(it.p) == 0 {
		return nil, false
	}
	el := int(binary.LittleEndian.Uint32(it.p))
	e := it.p[listElemHdrLen : listElemHdrLen+el]
	it.p = it.p[listElemHdrLen+el:]
	return e, true
}

// ListConfig parameterizes the list layer's plane minting.
type ListConfig struct {
	// Shard namespaces the rooth mint, doc 03 section 6.3.
	Shard uint16
	// LeaseN is the mint lease size. Default defaultLeaseN.
	LeaseN uint64
}

// List is the list type layer over the shard runtime. Not safe for
// concurrent use, like the other type layers: the caller serializes
// (R1).
type List struct {
	t    *Tiered
	mint Minter
	cfg  ListConfig

	// The current mint lease: counters [leaseNext, leaseEnd) are ours.
	leaseNext uint64
	leaseEnd  uint64

	// nodeRoot is the decoded noded root at listNodedState; its fence
	// lives in the fence scratch, copied out on decode so it survives
	// the node reads an op does next. fence2 stages fence rebuilds
	// that prepend entries.
	nodeRoot listNodeRoot
	fence    []listFenceEnt
	fence2   []listFenceEnt

	// kbuf holds the subkey of the node being read or written; shared
	// because the seam doors copy key bytes before returning.
	kbuf [SubkeySize]byte

	// rootBuf stages a rebuilt root payload; safe to fill from spans
	// aliasing a read because Tiered copies on Set and nothing else
	// touches it between. nodeBuf stages an amended edge node and
	// segBuf a fresh node, both copied out of a read the same way.
	rootBuf []byte
	nodeBuf []byte
	segBuf  []byte

	// cuts and cutN stage a push's batch boundaries: the simulation
	// that decides node cuts before any write, so a refused fence
	// overflow is side-effect free.
	cuts []int
	cutN []int

	// mgKeyBuf, mgKeys, mgVals, mgRoots, and mgExps carry one range
	// walk's prefetch round, hashiter's shape.
	mgKeyBuf []byte
	mgKeys   [][]byte
	mgVals   [][]byte
	mgRoots  []bool
	mgExps   []int64

	// spans holds one op's element spans into the read payload; valBuf
	// and vals carry a pop's reply, copied out before the write that
	// would recycle the read's arena bytes. valOff records the spans
	// as offsets while valBuf still grows across node reads. Reply
	// values stay valid until the next call on this List.
	spans  [][]byte
	valBuf []byte
	valOff [][2]int
	vals   [][]byte

	// moveBuf carries Move's element across the pop and push that
	// recycle every read view and the pop reply buffers.
	moveBuf []byte

	// The paged-fence scratch: pidxBuf backs the decoded page index,
	// pidxS stages fresh index entries a spill mints, pkbuf and
	// pageBuf carry page reads and writes, pi is the loaded page's
	// index position (-1 none, reset every stateOf), and deadPages
	// collects pageids whose deletes land after the root.
	pidxBuf   []listFenceEnt
	pidxS     []listFenceEnt
	pkbuf     [SubkeySize]byte
	pageBuf   []byte
	pi        int
	deadPages []uint64
}

// NewList builds the list layer over t. The store must carry the
// Minter capability: noded lists cannot exist without minted planes.
func NewList(t *Tiered, cfg ListConfig) (*List, error) {
	mint, ok := t.st.(Minter)
	if !ok {
		return nil, fmt.Errorf("sqlo1: store %T lacks the Minter capability the list ladder needs", t.st)
	}
	if cfg.LeaseN == 0 {
		cfg.LeaseN = defaultLeaseN
	}
	return &List{t: t, mint: mint, cfg: cfg}, nil
}

// nextRooth mints one rooth, taking a fresh durable lease when the
// current one is spent.
func (l *List) nextRooth(ctx context.Context) (uint64, error) {
	if l.leaseNext == l.leaseEnd {
		start, err := l.mint.MintLease(ctx, l.cfg.LeaseN)
		if err != nil {
			return 0, err
		}
		end, err := LeaseEnd(start, l.cfg.LeaseN)
		if err != nil {
			return 0, err
		}
		l.leaseNext, l.leaseEnd = start, end
	}
	c := l.leaseNext
	l.leaseNext++
	return MintRooth(l.cfg.Shard, c)
}

// restamp mirrors Str.restamp: puts a key's expiry back after a write
// that may have gone through a fresh hot header.
func (l *List) restamp(ctx context.Context, key []byte, expMs int64) error {
	if expMs == 0 {
		return nil
	}
	_, err := l.t.ExpireAt(ctx, key, expMs)
	return err
}

// listState classifies a key for the list ops.
type listState int

const (
	listAbsent listState = iota
	listInlineState
	listNodedState
)

// stateOf reads key and classifies it. The decoded inline view aliases
// the read; it dies on the next Tiered call. At listNodedState the
// decoded root lands in l.nodeRoot instead, which does not alias the
// read (the fence is copied out on decode) and stays valid across the
// node reads the op does next.
func (l *List) stateOf(ctx context.Context, key []byte) (listState, listInline, int64, error) {
	v, root, expMs, ok, err := l.t.LookupEntry(ctx, key)
	if err != nil || !ok {
		return listAbsent, listInline{}, 0, err
	}
	if !root {
		return listAbsent, listInline{}, 0, ErrWrongType
	}
	tag, _, err := sniffRoot(v)
	if err != nil {
		return listAbsent, listInline{}, 0, err
	}
	if tag != TagList {
		return listAbsent, listInline{}, 0, ErrWrongType
	}
	if v[0] == listSubNoded {
		l.nodeRoot, err = decodeListNodeRoot(v, l.fence[:0], l.pidxBuf[:0])
		if err != nil {
			return listAbsent, listInline{}, 0, err
		}
		l.pi = -1
		if l.nodeRoot.paged {
			l.pidxBuf = l.nodeRoot.pidx
			l.fence = l.fence[:0]
		} else {
			l.fence = l.nodeRoot.fence
		}
		return listNodedState, listInline{}, expMs, nil
	}
	li, err := decodeListInline(v)
	if err != nil {
		return listAbsent, listInline{}, 0, err
	}
	return listInlineState, li, expMs, nil
}

// readNode reads the node record at segid under the current root's
// plane into a decoded view. The view aliases the read and dies on
// the next Tiered call.
func (l *List) readNode(ctx context.Context, segid uint64) (listNode, error) {
	putHashSegKey(l.kbuf[:], l.nodeRoot.rooth, segid)
	v, ok, err := l.t.Get(ctx, l.kbuf[:])
	if err != nil {
		return listNode{}, err
	}
	if !ok {
		return listNode{}, fmt.Errorf("sqlo1: list node %d of rooth %#x is missing", segid, l.nodeRoot.rooth)
	}
	return decodeListNode(v)
}

// writeNode writes a node image under the current root's plane.
func (l *List) writeNode(ctx context.Context, segid uint64, payload []byte) error {
	putHashSegKey(l.kbuf[:], l.nodeRoot.rooth, segid)
	return l.t.SetGen(ctx, l.kbuf[:], payload, TagList, l.nodeRoot.rootgen)
}

// delNode drops an emptied node record.
func (l *List) delNode(ctx context.Context, segid uint64) error {
	putHashSegKey(l.kbuf[:], l.nodeRoot.rooth, segid)
	_, err := l.t.Del(ctx, l.kbuf[:])
	return err
}

// writeNodeRoot encodes l.nodeRoot and lands it under key. Always a
// full image: the list has not claimed the reconciliation machinery
// behind rule W2's delta elision (ReconcileRef does not know sub 3),
// and the store frames unrecognized roots in full anyway, so the
// claim would be a silent no-op. A later slice that teaches the
// reconcilers the list layouts earns the elision honestly.
func (l *List) writeNodeRoot(ctx context.Context, key []byte) error {
	l.rootBuf = appendListNodeRoot(l.rootBuf[:0], &l.nodeRoot)
	return l.t.Set(ctx, key, l.rootBuf, TagList|TagRoot)
}

// Push appends elems at the left or right end and reports the new
// length. Elements land one at a time in argument order, Redis's rule,
// so a multi-element left push reads back reversed. xOnly is the
// LPUSHX/RPUSHX gate: a missing key stays missing and answers 0. A
// push past the inline thresholds upgrades to the noded layout.
func (l *List) Push(ctx context.Context, key []byte, left, xOnly bool, elems ...[]byte) (int64, error) {
	st, li, expMs, err := l.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	switch st {
	case listNodedState:
		return l.pushNoded(ctx, key, left, elems, expMs)
	case listAbsent:
		if xOnly {
			return 0, nil
		}
	}
	count := li.count + len(elems)
	l.rootBuf = grow(l.rootBuf, listInlineHdrLen)
	if left {
		for i := len(elems) - 1; i >= 0; i-- {
			l.rootBuf = appendListElem(l.rootBuf, elems[i])
		}
		l.rootBuf = append(l.rootBuf, li.elems...)
	} else {
		l.rootBuf = append(l.rootBuf, li.elems...)
		for _, e := range elems {
			l.rootBuf = appendListElem(l.rootBuf, e)
		}
	}
	if count > listInlineMaxCount || len(l.rootBuf) > listInlineMax {
		return l.upgrade(ctx, key, l.rootBuf[listInlineHdrLen:], count, expMs)
	}
	putListInlineHdr(l.rootBuf, count)
	if err := l.t.Set(ctx, key, l.rootBuf, TagList|TagRoot); err != nil {
		return 0, err
	}
	return int64(count), l.restamp(ctx, key, expMs)
}

// upgrade moves a list from the inline tier to nodes. region is the
// finished inline element region already carrying the write that
// crossed a threshold (it sits in l.rootBuf), and count is its element
// count. Node payloads copy region spans wholesale, the move the
// entries' byte-identical encoding was designed for, and the plane
// lands durably before the root that references it: every crash prefix
// reads the old inline root over a plane nothing references yet, the
// setRope rule.
func (l *List) upgrade(ctx context.Context, key []byte, region []byte, count int, expMs int64) (int64, error) {
	// Walk the region once to place the cuts. The region came out of
	// our own build, so the walk trusts it.
	l.cuts, l.cutN = l.cuts[:0], l.cutN[:0]
	size, n := listNodeHdrLen, 0
	off := 0
	for off < len(region) {
		es := listElemHdrLen + int(binary.LittleEndian.Uint32(region[off:]))
		if n > 0 && (n >= listNodeMaxElems || size+es > listNodeMax) {
			l.cuts, l.cutN = append(l.cuts, off), append(l.cutN, n)
			size, n = listNodeHdrLen, 0
		}
		size += es
		n++
		off += es
	}
	l.cuts, l.cutN = append(l.cuts, off), append(l.cutN, n)
	if len(l.cuts) > listFenceMaxNodes && pageChunks(len(l.cuts)) > listFencePageIdxMax {
		return 0, errListFenceThirdLevel
	}

	rooth, err := l.nextRooth(ctx)
	if err != nil {
		return 0, err
	}
	r := &l.nodeRoot
	*r = listNodeRoot{rootgen: 1, rooth: rooth, count: uint64(count)}
	l.pi = -1
	l.fence = l.fence[:0]
	start := 0
	for i, end := range l.cuts {
		l.segBuf = grow(l.segBuf, listNodeHdrLen)
		l.segBuf = append(l.segBuf, region[start:end]...)
		putListNodeHdr(l.segBuf, l.cutN[i])
		if err := l.writeNode(ctx, r.nextSegid, l.segBuf); err != nil {
			return 0, err
		}
		l.fence = append(l.fence, listFenceEnt{segid: r.nextSegid, count: uint32(l.cutN[i])})
		r.nextSegid++
		start = end
	}
	if len(l.fence) > listFenceMaxNodes {
		// An upgrade that overshoots the flat cap pages straight away;
		// the pages join the nodes under the flush below, all of it
		// unreferenced until the root lands.
		if err := l.pageFence(ctx, l.fence, false); err != nil {
			return 0, err
		}
	} else {
		r.fence = l.fence
	}
	if err := l.t.Flush(ctx); err != nil {
		return 0, err
	}
	if err := l.writeNodeRoot(ctx, key); err != nil {
		return 0, err
	}
	return int64(count), l.restamp(ctx, key, expMs)
}

// pushNoded appends elems at an edge of a noded list: the edge node
// amends until full, then fresh nodes cut off it, batch by batch. The
// cuts are simulated before any write, so a push the format cannot
// take is refused side-effect free. A flat fence grown past its cap
// transitions to pages, and a full edge page spills its overflow into
// fresh pages; both follow listpage.go's ordering, staging the edge
// amendment until after the fresh records flush.
func (l *List) pushNoded(ctx context.Context, key []byte, left bool, elems [][]byte, expMs int64) (int64, error) {
	r := &l.nodeRoot
	if r.paged {
		pj := len(r.pidx) - 1
		if left {
			pj = 0
		}
		if err := l.loadPage(ctx, pj); err != nil {
			return 0, err
		}
	}
	ei := len(l.fence) - 1
	if left {
		ei = 0
	}
	edgeSegid := l.fence[ei].segid
	node, err := l.readNode(ctx, edgeSegid)
	if err != nil {
		return 0, err
	}

	// Batch 0 amends the edge node; each later cut starts a fresh one.
	// cuts[i] ends batch i at an element index into elems.
	l.cuts = l.cuts[:0]
	size, n := listNodeHdrLen+len(node.elems), node.n
	for i, e := range elems {
		es := listElemHdrLen + len(e)
		if n > 0 && (n >= listNodeMaxElems || size+es > listNodeMax) {
			l.cuts = append(l.cuts, i)
			size, n = listNodeHdrLen, 0
		}
		size += es
		n++
	}
	l.cuts = append(l.cuts, len(elems))
	newEnts := len(l.cuts) - 1

	// Capacity, before any write: a flat fence past its cap
	// transitions, a full page spills, and a full page index is the
	// ladder's end.
	transition := false
	if !r.paged {
		if len(l.fence)+newEnts > listFenceMaxNodes {
			transition = true
			if pageChunks(len(l.fence)+newEnts) > listFencePageIdxMax {
				return 0, errListFenceThirdLevel
			}
		}
	} else if over := len(l.fence) + newEnts - listFencePageMax; over > 0 {
		if len(r.pidx)+pageChunks(over) > listFencePageIdxMax {
			return 0, errListFenceThirdLevel
		}
	}

	// Stage the amended edge payload out of the read now: the fresh
	// node and page writes below recycle the view, and the amended
	// image must ride the batch that carries the root, never a flush
	// ahead of it. A left push lands its batch reversed ahead of the
	// old region, one-at-a-time semantics.
	b0 := l.cuts[0]
	if b0 > 0 {
		l.nodeBuf = grow(l.nodeBuf, listNodeHdrLen)
		if left {
			for i := b0 - 1; i >= 0; i-- {
				l.nodeBuf = appendListElem(l.nodeBuf, elems[i])
			}
			l.nodeBuf = append(l.nodeBuf, node.elems...)
		} else {
			l.nodeBuf = append(l.nodeBuf, node.elems...)
			for _, e := range elems[:b0] {
				l.nodeBuf = appendListElem(l.nodeBuf, e)
			}
		}
		putListNodeHdr(l.nodeBuf, node.n+b0)
		l.fence[ei].count += uint32(b0)
	}

	// Fresh nodes, safe ahead of any flush because nothing durable
	// references their segids until the root lands. fence2 builds the
	// combined entry order: on a left push each later batch sits
	// closer to the head, so the new entries land ahead of the old
	// ones in reverse creation order.
	l.fence2 = l.fence2[:0]
	if !left {
		l.fence2 = append(l.fence2, l.fence...)
	}
	newBase := len(l.fence2)
	start := b0
	for _, end := range l.cuts[1:] {
		l.segBuf = grow(l.segBuf, listNodeHdrLen)
		if left {
			for i := end - 1; i >= start; i-- {
				l.segBuf = appendListElem(l.segBuf, elems[i])
			}
		} else {
			for _, e := range elems[start:end] {
				l.segBuf = appendListElem(l.segBuf, e)
			}
		}
		putListNodeHdr(l.segBuf, end-start)
		if err := l.writeNode(ctx, r.nextSegid, l.segBuf); err != nil {
			return 0, err
		}
		l.fence2 = append(l.fence2, listFenceEnt{segid: r.nextSegid, count: uint32(end - start)})
		r.nextSegid++
		start = end
	}
	if left {
		for i, j := newBase, len(l.fence2)-1; i < j; i, j = i+1, j-1 {
			l.fence2[i], l.fence2[j] = l.fence2[j], l.fence2[i]
		}
		l.fence2 = append(l.fence2, l.fence...)
	}

	switch {
	case transition:
		// The whole combined fence becomes pages, flushed with the
		// fresh nodes before the root that flips the paged bit.
		if err := l.pageFence(ctx, l.fence2, left); err != nil {
			return 0, err
		}
		if err := l.t.Flush(ctx); err != nil {
			return 0, err
		}
	case r.paged && len(l.fence2) > listFencePageMax:
		// Spill: the edge page keeps the entries nearest the old ones,
		// and the overflow, all freshly cut, chunks into fresh pages
		// on the pushed side, landed and flushed before the root whose
		// index gains them; the partial chunk faces the pushed end so
		// the new edge page has room.
		spill := len(l.fence2) - listFencePageMax
		var kept, over []listFenceEnt
		if left {
			over, kept = l.fence2[:spill], l.fence2[spill:]
		} else {
			kept, over = l.fence2[:listFencePageMax], l.fence2[listFencePageMax:]
		}
		l.pidxS = l.pidxS[:0]
		for i := range pageChunks(spill) {
			pe, err := l.writeFreshPage(ctx, chunkEnts(over, i, left))
			if err != nil {
				return 0, err
			}
			l.pidxS = append(l.pidxS, pe)
		}
		if err := l.t.Flush(ctx); err != nil {
			return 0, err
		}
		at := len(r.pidx)
		if left {
			at = 0
		}
		r.pidx = slices.Insert(r.pidx, at, l.pidxS...)
		l.pidxBuf = r.pidx
		l.fence = append(l.fence[:0], kept...)
		if left {
			l.pi += len(l.pidxS)
		}
		if err := l.writeFencePage(ctx); err != nil {
			return 0, err
		}
	case r.paged:
		l.fence = append(l.fence[:0], l.fence2...)
		if err := l.writeFencePage(ctx); err != nil {
			return 0, err
		}
	default:
		l.fence, l.fence2 = l.fence2, l.fence
		r.fence = l.fence
	}

	if b0 > 0 {
		if err := l.writeNode(ctx, edgeSegid, l.nodeBuf); err != nil {
			return 0, err
		}
	}
	r.count += uint64(len(elems))
	if err := l.writeNodeRoot(ctx, key); err != nil {
		return 0, err
	}
	return int64(r.count), l.restamp(ctx, key, expMs)
}

// Pop removes up to count elements from the left or right end, in pop
// order (a right pop reads back tail first, Redis's rule). ok reports
// whether the key existed; a missing key answers nil, false. Popping
// the last element deletes the key. The returned values stay valid
// until the next call on this List.
func (l *List) Pop(ctx context.Context, key []byte, left bool, count int) ([][]byte, bool, error) {
	st, li, expMs, err := l.stateOf(ctx, key)
	if err != nil {
		return nil, false, err
	}
	switch st {
	case listNodedState:
		return l.popNoded(ctx, key, left, count, expMs)
	case listAbsent:
		return nil, false, nil
	}
	l.vals = l.vals[:0]
	if count <= 0 {
		return l.vals, true, nil
	}
	k := min(count, li.count)

	// The spans alias the read and stay valid until the write below,
	// the first Tiered call after it. The reply copies out first.
	l.spans = l.spans[:0]
	it := listElemIter{p: li.elems}
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		l.spans = append(l.spans, e)
	}
	total := 0
	for i := range k {
		if left {
			total += len(l.spans[i])
		} else {
			total += len(l.spans[len(l.spans)-1-i])
		}
	}
	l.valBuf = grow(l.valBuf, total)
	off := 0
	for i := range k {
		e := l.spans[i]
		if !left {
			e = l.spans[len(l.spans)-1-i]
		}
		copy(l.valBuf[off:], e)
		l.vals = append(l.vals, l.valBuf[off:off+len(e)])
		off += len(e)
	}

	if k == li.count {
		if _, err := l.t.Del(ctx, key); err != nil {
			return nil, false, err
		}
		return l.vals, true, nil
	}
	l.rootBuf = grow(l.rootBuf, listInlineHdrLen)
	rest := l.spans[k:]
	if !left {
		rest = l.spans[:li.count-k]
	}
	for _, e := range rest {
		l.rootBuf = appendListElem(l.rootBuf, e)
	}
	putListInlineHdr(l.rootBuf, li.count-k)
	if err := l.t.Set(ctx, key, l.rootBuf, TagList|TagRoot); err != nil {
		return nil, false, err
	}
	return l.vals, true, l.restamp(ctx, key, expMs)
}

// appendVal copies one popped element into valBuf and records its
// span as offsets: valBuf keeps growing across node reads, so slices
// into it are cut only when the copying is done.
func (l *List) appendVal(e []byte) {
	off := len(l.valBuf)
	l.valBuf = append(l.valBuf, e...)
	l.valOff = append(l.valOff, [2]int{off, off + len(e)})
}

// finishVals cuts l.vals from the finished valBuf.
func (l *List) finishVals() {
	for _, o := range l.valOff {
		l.vals = append(l.vals, l.valBuf[o[0]:o[1]])
	}
}

// copyNodeVals copies k popped elements out of node into valBuf in pop
// order and returns the byte length of the region they occupied. A
// left pop takes the node's front, walked in order; a right pop takes
// the back, replied tail first. The copies land before the caller's
// next Tiered call recycles the node view.
func (l *List) copyNodeVals(node listNode, right bool, k int) int {
	if !right {
		it := listElemIter{p: node.elems}
		for range k {
			e, _ := it.next()
			l.appendVal(e)
		}
		return len(node.elems) - len(it.p)
	}
	l.spans = l.spans[:0]
	it := listElemIter{p: node.elems}
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		l.spans = append(l.spans, e)
	}
	region := 0
	for i := range k {
		e := l.spans[len(l.spans)-1-i]
		l.appendVal(e)
		region += listElemHdrLen + len(e)
	}
	return region
}

// popNoded removes up to count elements from an edge of a noded list:
// whole edge nodes drain and drop, the final node shrinks in place,
// and a pop that empties the list deletes the key and retires the
// plane in O(1), the hdelSeg last-field rule.
func (l *List) popNoded(ctx context.Context, key []byte, left bool, count int, expMs int64) ([][]byte, bool, error) {
	r := &l.nodeRoot
	l.vals = l.vals[:0]
	if count <= 0 {
		return l.vals, true, nil
	}
	l.valBuf = l.valBuf[:0]
	l.valOff = l.valOff[:0]

	if uint64(count) >= r.count {
		// The whole list drains: every node reads and copies out in
		// pop order, page by page when paged, then the key dies and
		// the plane retires whole under a generation bump, nodes and
		// fence pages alike, so the retired records can never be
		// misread. A recreate starts inline under a fresh rootgen.
		npages := 1
		if r.paged {
			npages = len(r.pidx)
		}
		for p := range npages {
			if r.paged {
				pj := p
				if !left {
					pj = npages - 1 - p
				}
				if err := l.loadPage(ctx, pj); err != nil {
					return nil, false, err
				}
			}
			for i := range l.fence {
				fi := i
				if !left {
					fi = len(l.fence) - 1 - i
				}
				node, err := l.readNode(ctx, l.fence[fi].segid)
				if err != nil {
					return nil, false, err
				}
				l.copyNodeVals(node, !left, node.n)
			}
		}
		l.t.Bump(key, r.rooth, r.rootgen+1)
		if _, err := l.t.Del(ctx, key); err != nil {
			return nil, false, err
		}
		l.finishVals()
		return l.vals, true, nil
	}

	remaining := count
	l.deadPages = l.deadPages[:0]
	for remaining > 0 {
		if r.paged {
			pj := 0
			if !left {
				pj = len(r.pidx) - 1
			}
			if err := l.loadPage(ctx, pj); err != nil {
				return nil, false, err
			}
		}
		ei := 0
		if !left {
			ei = len(l.fence) - 1
		}
		ent := l.fence[ei]
		node, err := l.readNode(ctx, ent.segid)
		if err != nil {
			return nil, false, err
		}
		if remaining >= int(ent.count) {
			l.copyNodeVals(node, !left, node.n)
			if err := l.delNode(ctx, ent.segid); err != nil {
				return nil, false, err
			}
			if left {
				l.fence = l.fence[1:]
			} else {
				l.fence = l.fence[:len(l.fence)-1]
			}
			remaining -= int(ent.count)
			if r.paged && len(l.fence) == 0 {
				// The edge page drained whole; its record dies after
				// the root that drops it from the index.
				l.deadPages = append(l.deadPages, r.pidx[l.pi].segid)
				if left {
					r.pidx = r.pidx[1:]
				} else {
					r.pidx = r.pidx[:len(r.pidx)-1]
				}
				l.pi = -1
			}
			continue
		}
		// The final node shrinks in place: the kept elements are one
		// contiguous span of the region, copied into nodeBuf before
		// the write recycles the view.
		cut := l.copyNodeVals(node, !left, remaining)
		l.nodeBuf = grow(l.nodeBuf, listNodeHdrLen)
		if left {
			l.nodeBuf = append(l.nodeBuf, node.elems[cut:]...)
		} else {
			l.nodeBuf = append(l.nodeBuf, node.elems[:len(node.elems)-cut]...)
		}
		putListNodeHdr(l.nodeBuf, node.n-remaining)
		if err := l.writeNode(ctx, ent.segid, l.nodeBuf); err != nil {
			return nil, false, err
		}
		l.fence[ei].count -= uint32(remaining)
		remaining = 0
	}
	if r.paged {
		// A surviving loaded page always took an entry drop or a
		// count shrink; a pop that ended exactly on a page boundary
		// left nothing loaded.
		if l.pi >= 0 {
			if err := l.writeFencePage(ctx); err != nil {
				return nil, false, err
			}
		}
	} else {
		r.fence = l.fence
	}
	r.count -= uint64(count)
	if err := l.writeNodeRoot(ctx, key); err != nil {
		return nil, false, err
	}
	for _, pid := range l.deadPages {
		if err := l.delPage(ctx, pid); err != nil {
			return nil, false, err
		}
	}
	l.finishVals()
	return l.vals, true, l.restamp(ctx, key, expMs)
}

// Len reports the list length; a missing key answers 0, Redis's LLEN.
// Both tiers answer from the root alone, the doc 07 L-I2 exactness
// bet: the noded root's count is exact, never an estimate.
func (l *List) Len(ctx context.Context, key []byte) (int64, error) {
	st, li, _, err := l.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	switch st {
	case listAbsent:
		return 0, nil
	case listNodedState:
		return int64(l.nodeRoot.count), nil
	}
	return int64(li.count), nil
}

// Encoding answers OBJECT ENCODING for a list key: listpack inline,
// quicklist noded, matching Redis's names at the same thresholds.
func (l *List) Encoding(ctx context.Context, key []byte) (string, bool, error) {
	st, _, _, err := l.stateOf(ctx, key)
	if err != nil {
		return "", false, err
	}
	switch st {
	case listAbsent:
		return "", false, nil
	case listNodedState:
		return "quicklist", true, nil
	}
	return "listpack", true, nil
}
