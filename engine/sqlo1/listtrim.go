package sqlo1

// LTRIM, doc 07 slice 5. The window grammar is LRANGE's, and the cost
// shape is the doc 07 operator table's O(edges): nodes wholly outside
// the kept window drop by fence cut and tombstone without being read,
// at most the two edge nodes rewrite, and the root rewrites once. A
// trim to the empty window deletes the key, Redis's rule.

import "context"

// Trim keeps the clamped inclusive window [start, stop] and discards
// the rest. A missing key is a no-op; an empty window deletes the key.
func (l *List) Trim(ctx context.Context, key []byte, start, stop int64) error {
	st, li, expMs, err := l.stateOf(ctx, key)
	if err != nil {
		return err
	}
	count := int64(0)
	switch st {
	case listAbsent:
		return nil
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
	if start == 0 && stop == count-1 {
		return nil
	}
	if start > stop {
		// The empty window: the key dies, and a noded plane retires
		// whole under a generation bump without reading a single node,
		// popNoded's drain rule minus the reads.
		if st == listNodedState {
			l.t.Bump(key, l.nodeRoot.rooth, l.nodeRoot.rootgen+1)
		}
		_, err := l.t.Del(ctx, key)
		return err
	}

	if st == listInlineState {
		// One span of the element region survives; the rebuild copies
		// it behind a fresh header.
		a, _ := listElemSpan(li.elems, int(start))
		b := len(li.elems)
		if int(stop)+1 < li.count {
			b, _ = listElemSpan(li.elems, int(stop)+1)
		}
		l.rootBuf = grow(l.rootBuf, listInlineHdrLen)
		l.rootBuf = append(l.rootBuf, li.elems[a:b]...)
		putListInlineHdr(l.rootBuf, int(stop-start)+1)
		if err := l.t.Set(ctx, key, l.rootBuf, TagList|TagRoot); err != nil {
			return err
		}
		return l.restamp(ctx, key, expMs)
	}

	// Noded: the kept window spans fence[ni] through fence[nj]. Nodes
	// outside it drop unread; the two edge nodes shrink in place only
	// when the window starts or ends inside them.
	r := &l.nodeRoot
	ni, off := l.fenceSeek(start)
	nj, offj := l.fenceSeek(stop)
	for i := range ni {
		if err := l.delNode(ctx, l.fence[i].segid); err != nil {
			return err
		}
	}
	for i := nj + 1; i < len(l.fence); i++ {
		if err := l.delNode(ctx, l.fence[i].segid); err != nil {
			return err
		}
	}

	if ni == nj {
		node, err := l.readNode(ctx, l.fence[ni].segid)
		if err != nil {
			return err
		}
		keep := offj + 1 - off
		if keep < node.n {
			a, _ := listElemSpan(node.elems, off)
			b := len(node.elems)
			if offj+1 < node.n {
				b, _ = listElemSpan(node.elems, offj+1)
			}
			l.nodeBuf = grow(l.nodeBuf, listNodeHdrLen)
			l.nodeBuf = append(l.nodeBuf, node.elems[a:b]...)
			putListNodeHdr(l.nodeBuf, keep)
			if err := l.writeNode(ctx, l.fence[ni].segid, l.nodeBuf); err != nil {
				return err
			}
		}
		l.fence[ni].count = uint32(keep)
	} else {
		if off > 0 {
			node, err := l.readNode(ctx, l.fence[ni].segid)
			if err != nil {
				return err
			}
			a, _ := listElemSpan(node.elems, off)
			l.nodeBuf = grow(l.nodeBuf, listNodeHdrLen)
			l.nodeBuf = append(l.nodeBuf, node.elems[a:]...)
			putListNodeHdr(l.nodeBuf, node.n-off)
			if err := l.writeNode(ctx, l.fence[ni].segid, l.nodeBuf); err != nil {
				return err
			}
			l.fence[ni].count -= uint32(off)
		}
		if offj+1 < int(l.fence[nj].count) {
			node, err := l.readNode(ctx, l.fence[nj].segid)
			if err != nil {
				return err
			}
			b, _ := listElemSpan(node.elems, offj+1)
			l.nodeBuf = grow(l.nodeBuf, listNodeHdrLen)
			l.nodeBuf = append(l.nodeBuf, node.elems[:b]...)
			putListNodeHdr(l.nodeBuf, offj+1)
			if err := l.writeNode(ctx, l.fence[nj].segid, l.nodeBuf); err != nil {
				return err
			}
			l.fence[nj].count = uint32(offj + 1)
		}
	}
	l.fence = l.fence[ni : nj+1]
	r.fence = l.fence
	r.count = uint64(stop - start + 1)
	if err := l.writeNodeRoot(ctx, key); err != nil {
		return err
	}
	return l.restamp(ctx, key, expMs)
}
