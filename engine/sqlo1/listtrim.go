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
	// when the window starts or ends inside them. Paged, the same
	// shape one level up: pages wholly outside the window drop with
	// their nodes, at most the two edge pages rewrite, and the page
	// deletes land after the root that stopped referencing them.
	r := &l.nodeRoot
	if r.paged {
		return l.trimPaged(ctx, key, start, stop, expMs)
	}
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

// dropPage deletes every node of page j unread-except-the-page and
// queues the page record itself for the after-root delete.
func (l *List) dropPage(ctx context.Context, j int) error {
	if err := l.loadPage(ctx, j); err != nil {
		return err
	}
	for i := range l.fence {
		if err := l.delNode(ctx, l.fence[i].segid); err != nil {
			return err
		}
	}
	l.deadPages = append(l.deadPages, l.nodeRoot.pidx[j].segid)
	return nil
}

// trimPaged is Trim's noded body one level up: pages wholly outside
// the kept window drop with their nodes, the two edge pages rewrite
// in place beside the root, and the dead page records delete after
// it. start and stop arrive resolved and clamped.
func (l *List) trimPaged(ctx context.Context, key []byte, start, stop, expMs int64) error {
	r := &l.nodeRoot
	si, ls := l.pageForPos(start)
	sj, lstop := l.pageForPos(stop)
	l.deadPages = l.deadPages[:0]
	for j := range si {
		if err := l.dropPage(ctx, j); err != nil {
			return err
		}
	}
	for j := sj + 1; j < len(r.pidx); j++ {
		if err := l.dropPage(ctx, j); err != nil {
			return err
		}
	}

	if si == sj {
		if err := l.loadPage(ctx, si); err != nil {
			return err
		}
		ni, off := l.fenceSeek(ls)
		nj, offj := l.fenceSeek(lstop)
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
			if err := l.trimPageFront(ctx, ni, off); err != nil {
				return err
			}
			if err := l.trimPageBack(ctx, nj, offj); err != nil {
				return err
			}
		}
		l.fence = l.fence[ni : nj+1]
		if err := l.writeFencePage(ctx); err != nil {
			return err
		}
		r.pidx = r.pidx[si : si+1]
	} else {
		if err := l.loadPage(ctx, si); err != nil {
			return err
		}
		ni, off := l.fenceSeek(ls)
		for i := range ni {
			if err := l.delNode(ctx, l.fence[i].segid); err != nil {
				return err
			}
		}
		if err := l.trimPageFront(ctx, ni, off); err != nil {
			return err
		}
		l.fence = l.fence[ni:]
		if err := l.writeFencePage(ctx); err != nil {
			return err
		}
		if err := l.loadPage(ctx, sj); err != nil {
			return err
		}
		nj, offj := l.fenceSeek(lstop)
		for i := nj + 1; i < len(l.fence); i++ {
			if err := l.delNode(ctx, l.fence[i].segid); err != nil {
				return err
			}
		}
		if err := l.trimPageBack(ctx, nj, offj); err != nil {
			return err
		}
		l.fence = l.fence[:nj+1]
		if err := l.writeFencePage(ctx); err != nil {
			return err
		}
		r.pidx = r.pidx[si : sj+1]
	}
	r.count = uint64(stop - start + 1)
	if err := l.writeNodeRoot(ctx, key); err != nil {
		return err
	}
	for _, pid := range l.deadPages {
		if err := l.delPage(ctx, pid); err != nil {
			return err
		}
	}
	return l.restamp(ctx, key, expMs)
}

// trimPageFront shrinks the window's first node in place when the
// window starts inside it, and trimPageBack mirrors it at the stop
// edge; both edit the loaded fence view's counts.
func (l *List) trimPageFront(ctx context.Context, ni, off int) error {
	if off == 0 {
		return nil
	}
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
	return nil
}

func (l *List) trimPageBack(ctx context.Context, nj, offj int) error {
	if offj+1 >= int(l.fence[nj].count) {
		return nil
	}
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
	return nil
}
