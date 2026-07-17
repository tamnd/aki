package sqlo1

// The list positional surface, doc 07 slice 4. LLEN answers from the
// root alone (list.go's Len); LINDEX and LSET resolve their index
// through the fence prefix-sum to exactly one node; LRANGE does the
// same math to its start node and then streams nodes in fence order,
// cold nodes prefetched in IO batches ahead of the RESP writer, the
// L-I4 bounded-RAM walk.

import (
	"context"
	"errors"
	"fmt"
)

// listRangeBatchNodes is the node prefetch width of a range walk,
// hashIterBatchSegs's reasoning at the list's node size: a full round
// is 16 nodes of up to ~4 KiB, bounding the IO round at about 64 KiB
// while amortizing the cold index path across the batch.
const listRangeBatchNodes = 16

// LSET's two doors, worded the way Redis words them; storeErr's ERR
// prefix completes the wire text.
var (
	errListNoKey      = errors.New("no such key")
	errListIndexRange = errors.New("index out of range")
)

// listResolveIdx resolves a possibly-negative index against count,
// Redis's rule: negative counts back from the tail, and ok is false
// when the resolved position falls outside the list.
func listResolveIdx(idx, count int64) (int64, bool) {
	if idx < 0 {
		idx += count
	}
	return idx, idx >= 0 && idx < count
}

// fenceSeek prefix-sums the fence to the node holding absolute
// position pos and the offset inside it. The caller bounds pos below
// the root count, and the decode cross-checked that the fence counts
// sum to it, so the walk always lands.
func (l *List) fenceSeek(pos int64) (ni int, off int) {
	for i := range l.fence {
		c := int64(l.fence[i].count)
		if pos < c {
			return i, int(pos)
		}
		pos -= c
	}
	return len(l.fence) - 1, int(pos)
}

// listElemSpan walks a validated element region to element k and
// returns its span's byte bounds, header included.
func listElemSpan(region []byte, k int) (a, b int) {
	it := listElemIter{p: region}
	for range k {
		it.next()
	}
	a = len(region) - len(it.p)
	e, _ := it.next()
	return a, a + listElemHdrLen + len(e)
}

// Index answers the element at idx, negative resolved against the
// length; ok false is a missing key or an out-of-range index, LINDEX's
// nil. The returned bytes alias the read and die at the next Tiered
// call.
func (l *List) Index(ctx context.Context, key []byte, idx int64) ([]byte, bool, error) {
	st, li, _, err := l.stateOf(ctx, key)
	if err != nil {
		return nil, false, err
	}
	switch st {
	case listAbsent:
		return nil, false, nil
	case listInlineState:
		i, ok := listResolveIdx(idx, int64(li.count))
		if !ok {
			return nil, false, nil
		}
		it := listElemIter{p: li.elems}
		for range i {
			it.next()
		}
		e, _ := it.next()
		return e, true, nil
	}
	i, ok := listResolveIdx(idx, int64(l.nodeRoot.count))
	if !ok {
		return nil, false, nil
	}
	ni, off := l.fenceSeek(i)
	node, err := l.readNode(ctx, l.fence[ni].segid)
	if err != nil {
		return nil, false, err
	}
	it := listElemIter{p: node.elems}
	for range off {
		it.next()
	}
	e, _ := it.next()
	return e, true, nil
}

// Set replaces the element at idx, LSET: a missing key is its own
// error, an out-of-range index another. On the noded tier the touched
// node rewrites in place and the root does not change, the L-I1 O(1)
// bound; the node cut threshold is push policy, not a format bound, so
// a grown replacement never splits here. An inline root grown past its
// byte cap upgrades to nodes, the same one-way ladder a push takes.
func (l *List) Set(ctx context.Context, key []byte, idx int64, elem []byte) error {
	st, li, expMs, err := l.stateOf(ctx, key)
	if err != nil {
		return err
	}
	switch st {
	case listAbsent:
		return errListNoKey
	case listNodedState:
		i, ok := listResolveIdx(idx, int64(l.nodeRoot.count))
		if !ok {
			return errListIndexRange
		}
		ni, off := l.fenceSeek(i)
		node, err := l.readNode(ctx, l.fence[ni].segid)
		if err != nil {
			return err
		}
		// nodeBuf copies the kept spans out of the read before the
		// write recycles it.
		a, b := listElemSpan(node.elems, off)
		l.nodeBuf = grow(l.nodeBuf, listNodeHdrLen)
		l.nodeBuf = append(l.nodeBuf, node.elems[:a]...)
		l.nodeBuf = appendListElem(l.nodeBuf, elem)
		l.nodeBuf = append(l.nodeBuf, node.elems[b:]...)
		putListNodeHdr(l.nodeBuf, node.n)
		return l.writeNode(ctx, l.fence[ni].segid, l.nodeBuf)
	}
	i, ok := listResolveIdx(idx, int64(li.count))
	if !ok {
		return errListIndexRange
	}
	a, b := listElemSpan(li.elems, int(i))
	l.rootBuf = grow(l.rootBuf, listInlineHdrLen)
	l.rootBuf = append(l.rootBuf, li.elems[:a]...)
	l.rootBuf = appendListElem(l.rootBuf, elem)
	l.rootBuf = append(l.rootBuf, li.elems[b:]...)
	if len(l.rootBuf) > listInlineMax {
		_, err := l.upgrade(ctx, key, l.rootBuf[listInlineHdrLen:], li.count, expMs)
		return err
	}
	putListInlineHdr(l.rootBuf, li.count)
	if err := l.t.Set(ctx, key, l.rootBuf, TagList|TagRoot); err != nil {
		return err
	}
	return l.restamp(ctx, key, expMs)
}

// Range streams the elements from start to stop under LRANGE's
// inclusive clamped grammar: negatives resolve against the length,
// the ends clamp, and an inverted or out-of-range window is empty.
// begin runs exactly once, before any emit, with the exact number of
// elements that will follow, so a RESP writer can put the array header
// down and stream the rest; emitted bytes alias the current IO round
// and die at the next Tiered call. A missing key is begin(0).
func (l *List) Range(ctx context.Context, key []byte, start, stop int64, begin func(n int), emit func(e []byte)) error {
	st, li, _, err := l.stateOf(ctx, key)
	if err != nil {
		return err
	}
	count := int64(0)
	switch st {
	case listInlineState:
		count = int64(li.count)
	case listNodedState:
		count = int64(l.nodeRoot.count)
	}
	if start < 0 {
		start = max(start+count, 0)
	}
	if stop < 0 {
		stop += count
	}
	stop = min(stop, count-1)
	if start > stop {
		begin(0)
		return nil
	}
	n := int(stop - start + 1)
	begin(n)
	if st == listInlineState {
		it := listElemIter{p: li.elems}
		for range start {
			it.next()
		}
		for range n {
			e, _ := it.next()
			emit(e)
		}
		return nil
	}

	// Noded: seek the start node, prefix-sum on to the last node the
	// window touches, then walk that window in prefetched rounds. Each
	// round's emits drain before the next round's IO recycles them,
	// which is what keeps the RAM bound at one round.
	ni, off := l.fenceSeek(start)
	nj := ni
	for acc := int64(l.fence[ni].count) - int64(off); acc < int64(n); nj++ {
		acc += int64(l.fence[nj+1].count)
	}
	remaining := n
	for base := ni; base <= nj && remaining > 0; base += listRangeBatchNodes {
		w := min(listRangeBatchNodes, nj+1-base)
		l.mgKeyBuf = grow(l.mgKeyBuf, w*SubkeySize)
		l.mgKeys = l.mgKeys[:0]
		for j := range w {
			k := l.mgKeyBuf[j*SubkeySize : (j+1)*SubkeySize]
			putHashSegKey(k, l.nodeRoot.rooth, l.fence[base+j].segid)
			l.mgKeys = append(l.mgKeys, k)
		}
		l.mgVals, l.mgRoots, l.mgExps, err = l.t.LookupBatch(ctx, l.mgKeys, l.mgVals, l.mgRoots, l.mgExps)
		if err != nil {
			return err
		}
		for j := 0; j < w && remaining > 0; j++ {
			if l.mgVals[j] == nil {
				return fmt.Errorf("sqlo1: list node %d of rooth %#x is missing", l.fence[base+j].segid, l.nodeRoot.rooth)
			}
			node, err := decodeListNode(l.mgVals[j])
			if err != nil {
				return err
			}
			it := listElemIter{p: node.elems}
			if base+j == ni {
				for range off {
					it.next()
				}
			}
			for {
				e, ok := it.next()
				if !ok || remaining == 0 {
					break
				}
				emit(e)
				remaining--
			}
		}
	}
	return nil
}
