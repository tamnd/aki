package sqlo1

// XTRIM and the XADD trim options, doc 10's trim slice. The cost shape
// is the operator table's: runs wholly below the cut drop by fence cut
// and record delete without being read, an exact trim rewrites at most
// the one boundary run, and the root rewrites once. The approximate
// form cuts only at run boundaries, Redis's node-boundary semantics
// (X-I4), and the last-generated ID, entries-added, and max-deleted-ID
// root fields never move (X-I2). Unlike a list, a stream emptied by
// trim keeps its key: a count 0 root is legal and the IDs must survive.

import "context"

// streamTrim carries one trim's mode and running tallies across the
// fence views it walks.
type streamTrim struct {
	byID   bool
	approx bool
	maxlen int64
	minid  streamID
	// limit bounds removed run-granularly, 0 unlimited. The command
	// layer resolves Redis's default for the approximate form and
	// always passes 0 for the exact one.
	limit int64

	removed   int64 // live entries removed so far
	remaining int64 // live entries still in the stream
}

// Trim discards entries from the head: byID false keeps at most maxlen
// newest live entries, byID true drops every entry below minid. approx
// cuts only at whole runs. It reports the live entries removed; a
// missing key removes none.
func (x *Stream) Trim(ctx context.Context, key []byte, byID bool, maxlen int64, minid streamID, approx bool, limit int64) (int64, error) {
	exists, expMs, err := x.stateOf(ctx, key)
	if err != nil || !exists {
		return 0, err
	}
	r := &x.root
	tr := streamTrim{byID: byID, approx: approx, maxlen: maxlen, minid: minid, limit: limit, remaining: int64(r.count)}
	if r.paged {
		return x.trimPaged(ctx, key, &tr, expMs)
	}
	cut, stopped, err := x.trimFront(ctx, &tr, true, streamID{})
	if err != nil {
		return 0, err
	}
	if !stopped && !approx && cut < len(x.fence) {
		keep, err := x.trimBoundary(ctx, &tr, &x.fence[cut])
		if err != nil {
			return 0, err
		}
		if !keep {
			cut++
		}
	}
	if tr.removed == 0 {
		return 0, nil
	}
	x.fence = x.fence[cut:]
	r.count -= uint64(tr.removed)
	if err := x.writeRoot(ctx, key); err != nil {
		return 0, err
	}
	return tr.removed, x.restamp(ctx, key, expMs)
}

// delRun drops a trimmed run's record, unread.
func (x *Stream) delRun(ctx context.Context, segid uint64) error {
	putHashSegKey(x.kbuf[:], x.root.rooth, segid)
	_, err := x.t.Del(ctx, x.kbuf[:])
	return err
}

// delFencePage drops a fence page record, always after the root that
// stopped referencing it.
func (x *Stream) delFencePage(ctx context.Context, pageid uint64) error {
	putHashFenceKey(x.pkbuf[:], x.root.rooth, pageid)
	_, err := x.t.Del(ctx, x.pkbuf[:])
	return err
}

// trimFront drops the leading whole runs of the fence in scratch (the
// flat fence or one loaded page), deleting each run record unread, and
// reports how many it cut. A run is droppable when every entry in it
// falls to the trim: below the length target with the whole run gone,
// or wholly below minid, judged by the next run's base since the fence
// does not store run last IDs. nextBase is the following page's index
// base; lastView marks the stream's final fence view, where the root's
// last generated ID is the wall. stopped reports a limit stop.
func (x *Stream) trimFront(ctx context.Context, tr *streamTrim, lastView bool, nextBase streamID) (cut int, stopped bool, err error) {
	for cut < len(x.fence) {
		fe := x.fence[cut]
		var droppable bool
		if tr.byID {
			switch {
			case cut+1 < len(x.fence):
				droppable = !tr.minid.less(x.fence[cut+1].base)
			case lastView:
				droppable = x.root.last.less(tr.minid)
			default:
				droppable = !tr.minid.less(nextBase)
			}
		} else {
			droppable = tr.remaining-int64(fe.count) >= tr.maxlen
		}
		if !droppable {
			return cut, false, nil
		}
		if tr.limit > 0 && tr.removed+int64(fe.count) > tr.limit {
			return cut, true, nil
		}
		if err := x.delRun(ctx, fe.segid); err != nil {
			return cut, false, err
		}
		tr.removed += int64(fe.count)
		tr.remaining -= int64(fe.count)
		cut++
	}
	return cut, false, nil
}

// trimBoundary rewrites the boundary run in place for an exact trim,
// dropping its leading entries: below minid byID, or the count over the
// length target otherwise, plus the dead entries interleaved with the
// dropped prefix. keep false means no live entry survived and the run
// was deleted whole, dead survivors included, which is legal because a
// tombstoned entry is invisible everywhere.
func (x *Stream) trimBoundary(ctx context.Context, tr *streamTrim, fe *streamFenceEnt) (keep bool, err error) {
	var need int64
	if tr.byID {
		if !fe.base.less(tr.minid) {
			return true, nil
		}
	} else {
		need = tr.remaining - tr.maxlen
		if need <= 0 {
			return true, nil
		}
	}
	v, err := x.readRun(ctx, fe.segid)
	if err != nil {
		return false, err
	}
	x.ents, x.fvPool, x.fvOffs = x.ents[:0], x.fvPool[:0], x.fvOffs[:0]
	dropped, live := int64(0), uint32(0)
	skipping := true
	_, err = walkStreamRun(v, func(_ int, e streamEntry) error {
		if skipping {
			if tr.byID {
				if e.id.less(tr.minid) {
					if !e.dead {
						dropped++
					}
					return nil
				}
			} else {
				if dropped < need {
					if !e.dead {
						dropped++
					}
					return nil
				}
				if e.dead {
					// Dead entries between the last dropped live entry
					// and the first survivor go with the prefix.
					return nil
				}
			}
			skipping = false
		}
		x.fvOffs = append(x.fvOffs, len(x.fvPool))
		x.fvPool = append(x.fvPool, e.fv...)
		x.ents = append(x.ents, streamEntry{id: e.id, dead: e.dead})
		if !e.dead {
			live++
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	if dropped == 0 {
		return true, nil
	}
	x.fvOffs = append(x.fvOffs, len(x.fvPool))
	for i := range x.ents {
		x.ents[i].fv = x.fvPool[x.fvOffs[i]:x.fvOffs[i+1]]
	}
	tr.removed += dropped
	tr.remaining -= dropped
	if live == 0 {
		if err := x.delRun(ctx, fe.segid); err != nil {
			return false, err
		}
		return false, nil
	}
	x.runBuf = appendStreamRun(x.runBuf[:0], x.ents)
	if err := x.writeRun(ctx, fe.segid, x.runBuf); err != nil {
		return false, err
	}
	fe.base = x.ents[0].id
	fe.count = live
	return true, nil
}

// trimPaged is Trim one level up: pages emptied by the cut queue their
// record deletes for after the root, listtrim's discipline, and at most
// the one boundary page rewrites in place beside it. The walk stops at
// the first page holding a surviving run, since the drop region is a
// prefix on this rung too.
func (x *Stream) trimPaged(ctx context.Context, key []byte, tr *streamTrim, expMs int64) (int64, error) {
	r := &x.root
	x.deadPages = x.deadPages[:0]
	pcut := 0
	for p := 0; p < len(r.pidx); p++ {
		if err := x.loadPage(ctx, p); err != nil {
			return 0, err
		}
		lastView := p+1 == len(r.pidx)
		var nextBase streamID
		if !lastView {
			nextBase = r.pidx[p+1].base
		}
		cut, stopped, err := x.trimFront(ctx, tr, lastView, nextBase)
		if err != nil {
			return 0, err
		}
		if !stopped && cut == len(x.fence) {
			x.deadPages = append(x.deadPages, r.pidx[p].segid)
			pcut++
			continue
		}
		before := tr.removed
		if !stopped && !tr.approx && cut < len(x.fence) {
			keep, err := x.trimBoundary(ctx, tr, &x.fence[cut])
			if err != nil {
				return 0, err
			}
			if !keep {
				cut++
			}
		}
		if cut == len(x.fence) {
			x.deadPages = append(x.deadPages, r.pidx[p].segid)
			pcut++
		} else if cut > 0 || tr.removed > before {
			x.fence = x.fence[cut:]
			if err := x.writeFencePage(ctx); err != nil {
				return 0, err
			}
		}
		break
	}
	if tr.removed == 0 {
		return 0, nil
	}
	r.pidx = r.pidx[pcut:]
	x.pi = -1
	r.count -= uint64(tr.removed)
	if err := x.writeRoot(ctx, key); err != nil {
		return 0, err
	}
	for _, pid := range x.deadPages {
		if err := x.delFencePage(ctx, pid); err != nil {
			return 0, err
		}
	}
	return tr.removed, x.restamp(ctx, key, expMs)
}
