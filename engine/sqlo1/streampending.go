package sqlo1

// The pending surface over the kind 5 PEL: XPENDING's summary and
// extended forms read the fence and segments, XCLAIM moves ownership
// of explicit IDs, and XAUTOCLAIM sweeps a cursor window. Every
// behavior here is pinned against Redis 8.8: a claim of an entry that
// was deleted from the stream drops its pending row no matter the
// idle filter, a plain claim bumps the delivery count while JUSTID
// and an explicit RETRYCOUNT do not, FORCE mints a fresh pending row
// for a live entry at count one and then the normal rules apply, and
// the XAUTOCLAIM cursor resumes inclusively at the next entry the
// walk did not examine, giving up after count times ten attempts.

import (
	"bytes"
	"context"
	"math"
	"sort"
)

// streamPendingCons is one XPENDING summary row: a consumer that owns
// at least one pending entry, and how many.
type streamPendingCons struct {
	name []byte
	n    uint64
}

// streamIDPrev is the predecessor ID, the inclusive-start step; ok
// false at 0-0, which no entry can carry, so 0-0 itself serves as the
// all-inclusive walk floor.
func streamIDPrev(id streamID) (streamID, bool) {
	if id.seq > 0 {
		return streamID{ms: id.ms, seq: id.seq - 1}, true
	}
	if id.ms > 0 {
		return streamID{ms: id.ms - 1, seq: math.MaxUint64}, true
	}
	return streamID{}, false
}

// pendingAfter turns an inclusive start bound into pelCollect's
// exclusive one.
func pendingAfter(start streamID) streamID {
	if prev, ok := streamIDPrev(start); ok {
		return prev
	}
	return streamID{}
}

// PendingSummary is the XPENDING summary form: the total pending
// count, the smallest and largest pending IDs, and the owning
// consumers in name order, only those with at least one entry. A
// missing key or group answers the shared NOGROUP sentinel.
func (x *Stream) PendingSummary(ctx context.Context, key, group []byte) (total uint64, minID, maxID streamID, cons []streamPendingCons, err error) {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return 0, streamID{}, streamID{}, nil, err
	}
	if !exists {
		return 0, streamID{}, streamID{}, nil, errStreamNoGroup
	}
	ord, g0, err := x.findGroup(ctx, group)
	if err != nil {
		return 0, streamID{}, streamID{}, nil, err
	}
	if ord < 0 {
		return 0, streamID{}, streamID{}, nil, errStreamNoGroup
	}
	g := x.copyGroupOwned(&g0)
	for i := range g.cons {
		total += g.cons[i].pel
	}
	if total == 0 {
		return 0, streamID{}, streamID{}, nil, nil
	}
	minID = g.pelf[0].base
	last, err := x.readPelSeg(ctx, g.pelf[len(g.pelf)-1].segid)
	if err != nil {
		return 0, streamID{}, streamID{}, nil, err
	}
	maxID = last[len(last)-1].id
	order := make([]int, 0, len(g.cons))
	for i := range g.cons {
		if g.cons[i].pel > 0 {
			order = append(order, i)
		}
	}
	sort.Slice(order, func(a, b int) bool {
		return bytes.Compare(g.cons[order[a]].name, g.cons[order[b]].name) < 0
	})
	for _, i := range order {
		cons = append(cons, streamPendingCons{name: g.cons[i].name, n: g.cons[i].pel})
	}
	return total, minID, maxID, cons, nil
}

// PendingExt is the XPENDING extended form: pending rows in [start,
// end] in ID order, at most count, optionally only consumer's, and
// only entries idle at least minIdle. The consumer name in each row
// is owned by the emit call.
func (x *Stream) PendingExt(ctx context.Context, key, group []byte, start, end streamID, count int64, consumer []byte, minIdle int64, nowMs int64, emit func(id streamID, cons []byte, idle int64, dcount uint32)) error {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return errStreamNoGroup
	}
	ord, g0, err := x.findGroup(ctx, group)
	if err != nil {
		return err
	}
	if ord < 0 {
		return errStreamNoGroup
	}
	if count <= 0 {
		return nil
	}
	g := x.copyGroupOwned(&g0)
	ci := -1
	if consumer != nil {
		for i := range g.cons {
			if bytes.Equal(g.cons[i].name, consumer) {
				ci = i
				break
			}
		}
		if ci < 0 {
			return nil
		}
	}
	var n int64
	return x.pelCollect(ctx, &g, pendingAfter(start), func(e streamPelEnt) bool {
		if end.less(e.id) {
			return false
		}
		if ci >= 0 && int(e.cidx) != ci {
			return true
		}
		if minIdle > 0 && nowMs-e.dtime < minIdle {
			return true
		}
		emit(e.id, g.cons[e.cidx].name, nowMs-e.dtime, e.dcount)
		n++
		return n < count
	})
}

// streamClaimOpts is XCLAIM's option set after parsing: retry is the
// RETRYCOUNT value or -1 unset, and dtime resolution runs IDLE over
// TIME over now, clamped to [0, now] the way Redis clamps.
type streamClaimOpts struct {
	setIdle bool
	idle    int64
	setTime bool
	time    int64
	retry   int64
	force   bool
	justid  bool
	setLast bool
	last    streamID
}

// pelSegEdit is one segment loaded for claim-side editing, keyed by
// its position in the fence at load time; claims never reorder IDs,
// so fence positions are stable until the finalize pass. fresh marks
// a segment whose segid was minted this op and has never landed, so
// the finalize pass flushes it before the caller's record write.
type pelSegEdit struct {
	ents  []streamPelEnt
	dirty bool
	fresh bool
}

// pelEditLoad reads fence slot fi into the edit cache, an owned copy
// since the decode scratch is shared.
func (x *Stream) pelEditLoad(ctx context.Context, g *streamGroup, cache map[int]*pelSegEdit, fi int) (*pelSegEdit, error) {
	if e, ok := cache[fi]; ok {
		return e, nil
	}
	ents, err := x.readPelSeg(ctx, g.pelf[fi].segid)
	if err != nil {
		return nil, err
	}
	e := &pelSegEdit{ents: append([]streamPelEnt(nil), ents...)}
	cache[fi] = e
	return e, nil
}

// pelSegEncodedLen is the encoded payload size of ents.
func pelSegEncodedLen(ents []streamPelEnt) int {
	size := streamPelSegHdrLen
	prev := streamID{}
	for i := range ents {
		size += streamPelEntLen(prev, ents[i].id)
		prev = ents[i].id
	}
	return size
}

// pelEditFinalize lands every edited segment: an emptied one queues
// for deletion after the record write, an oversized one (a FORCE
// insert past the caps) splits with the second half on a fresh segid,
// and the rest rewrite in place. Fresh segments flush before the
// caller's record write; rootDirty reports minted segids. The fence
// full check runs before any write, so a refusal is side-effect free.
func (x *Stream) pelEditFinalize(ctx context.Context, g *streamGroup, cache map[int]*pelSegEdit) (rootDirty bool, err error) {
	x.pelDrops = x.pelDrops[:0]
	idxs := make([]int, 0, len(cache))
	splits := 0
	for fi, e := range cache {
		if !e.dirty {
			continue
		}
		idxs = append(idxs, fi)
		if len(e.ents) > streamPelSegMaxEnts || pelSegEncodedLen(e.ents) > streamPelSegMaxBytes {
			splits++
		}
	}
	if len(g.pelf)+splits > streamPelFenceMax {
		return false, errStreamPelFenceFull
	}
	sort.Ints(idxs)
	// Splits mint segids and shift fence slots, so they rewrite the
	// fence from the back forward; every fresh segment (a split's
	// second half or a first-ever mint) flushes before the caller
	// writes the record, so a crash prefix leaves only orphans.
	fresh := false
	for k := len(idxs) - 1; k >= 0; k-- {
		fi := idxs[k]
		e := cache[fi]
		if len(e.ents) == 0 {
			g.pelf[fi].count = 0
			x.pelDrops = append(x.pelDrops, g.pelf[fi].segid)
			continue
		}
		if len(e.ents) > streamPelSegMaxEnts || pelSegEncodedLen(e.ents) > streamPelSegMaxBytes {
			cut := len(e.ents) / 2
			head, tail := e.ents[:cut], e.ents[cut:]
			segid := x.root.nextSegid
			x.root.nextSegid++
			rootDirty = true
			fresh = true
			if err := x.writePelSeg(ctx, segid, tail); err != nil {
				return false, err
			}
			g.pelf = append(g.pelf, streamPelFenceEnt{})
			copy(g.pelf[fi+2:], g.pelf[fi+1:])
			g.pelf[fi+1] = streamPelFenceEnt{base: tail[0].id, segid: segid, count: uint32(len(tail))}
			e.ents = head
		}
		if e.fresh {
			rootDirty = true
			fresh = true
		}
		if err := x.writePelSeg(ctx, g.pelf[fi].segid, e.ents); err != nil {
			return false, err
		}
		g.pelf[fi].base = e.ents[0].id
		g.pelf[fi].count = uint32(len(e.ents))
	}
	if fresh {
		if err := x.t.Flush(ctx); err != nil {
			return false, err
		}
	}
	live := g.pelf[:0]
	for i := range g.pelf {
		if g.pelf[i].count > 0 {
			live = append(live, g.pelf[i])
		}
	}
	g.pelf = live
	return rootDirty, nil
}

// entryLive reports whether id still exists in the stream's runs.
func (x *Stream) entryLive(ctx context.Context, key []byte, id streamID) (bool, error) {
	found := false
	err := x.Range(ctx, key, id, id, 1, false, func(int) {}, func(streamID, [][]byte) {
		found = true
	})
	return found, err
}

// Claim is XCLAIM: move ownership of ids to consumer, filtered by
// minIdle, with the pinned side rules. A pending entry whose stream
// entry was deleted drops from the PEL no matter the idle filter and
// never reaches the reply. FORCE mints a pending row for a live
// non-pending entry at delivery count one, and then the count rules
// run as usual: an explicit retry sets it, JUSTID freezes it, and a
// plain claim increments it. The claiming consumer auto-creates even
// when nothing claims, the pinned ghost row.
func (x *Stream) Claim(ctx context.Context, key, group, consumer []byte, minIdle int64, ids []streamID, o *streamClaimOpts, nowMs int64) ([]streamID, error) {
	exists, expMs, err := x.stateOf(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errStreamNoGroup
	}
	ord, g0, err := x.findGroup(ctx, group)
	if err != nil {
		return nil, err
	}
	if ord < 0 {
		return nil, errStreamNoGroup
	}
	g := x.copyGroupOwned(&g0)
	ci, err := groupConsumer(&g, consumer, nowMs)
	if err != nil {
		return nil, err
	}
	if ci > math.MaxUint8 {
		return nil, errStreamPelConsumerCap
	}
	dtime := nowMs
	if o.setIdle {
		dtime = nowMs - o.idle
	} else if o.setTime {
		dtime = o.time
	}
	if dtime < 0 || dtime > nowMs {
		dtime = nowMs
	}
	cache := map[int]*pelSegEdit{}
	var claimed []streamID
	for _, id := range ids {
		fi := pelFenceSeek(g.pelf, id)
		var seg *pelSegEdit
		ei := -1
		if fi >= 0 {
			seg, err = x.pelEditLoad(ctx, &g, cache, fi)
			if err != nil {
				return nil, err
			}
			for i := range seg.ents {
				if seg.ents[i].id == id {
					ei = i
					break
				}
			}
		}
		if ei < 0 {
			if !o.force {
				continue
			}
			live, err := x.entryLive(ctx, key, id)
			if err != nil {
				return nil, err
			}
			if !live {
				continue
			}
			// The minted row starts at count one; the shared claim
			// rules below then bump or overwrite it.
			e := streamPelEnt{id: id, cidx: uint8(ci), dcount: 1, dtime: dtime}
			if len(g.pelf) == 0 {
				// First pending entry ever: mint the segid now and
				// hand the finalize pass a fresh single-entry slot.
				segid := x.root.nextSegid
				x.root.nextSegid++
				g.pelf = append(g.pelf, streamPelFenceEnt{base: id, segid: segid, count: 1})
				seg = &pelSegEdit{ents: []streamPelEnt{e}, dirty: true, fresh: true}
				cache[0] = seg
				g.cons[ci].pel++
				ei, fi = 0, 0
			} else {
				ti := max(fi, 0)
				seg, err = x.pelEditLoad(ctx, &g, cache, ti)
				if err != nil {
					return nil, err
				}
				at := sort.Search(len(seg.ents), func(i int) bool {
					return id.less(seg.ents[i].id)
				})
				seg.ents = append(seg.ents, streamPelEnt{})
				copy(seg.ents[at+1:], seg.ents[at:])
				seg.ents[at] = e
				seg.dirty = true
				g.cons[ci].pel++
				ei = at
				fi = ti
			}
		} else {
			live, err := x.entryLive(ctx, key, id)
			if err != nil {
				return nil, err
			}
			if !live {
				g.cons[seg.ents[ei].cidx].pel--
				seg.ents = append(seg.ents[:ei], seg.ents[ei+1:]...)
				seg.dirty = true
				continue
			}
			if minIdle > 0 && nowMs-seg.ents[ei].dtime < minIdle {
				continue
			}
			g.cons[seg.ents[ei].cidx].pel--
			g.cons[ci].pel++
			seg.ents[ei].cidx = uint8(ci)
			seg.dirty = true
		}
		seg.ents[ei].dtime = dtime
		if o.retry >= 0 {
			// The on-disk count is u32; a wider RETRYCOUNT saturates.
			seg.ents[ei].dcount = uint32(min(o.retry, math.MaxUint32))
		} else if !o.justid {
			seg.ents[ei].dcount++
		}
		claimed = append(claimed, id)
	}
	if o.setLast && g.last.less(o.last) {
		g.last = o.last
		g.read = -1
	}
	g.cons[ci].seenMs = nowMs
	if len(claimed) > 0 {
		g.cons[ci].activeMs = nowMs
	}
	rootDirty, err := x.pelEditFinalize(ctx, &g, cache)
	if err != nil {
		return nil, err
	}
	if err := x.writeGroupRec(ctx, uint32(ord), &g); err != nil {
		return nil, err
	}
	if err := x.pelDropPending(ctx); err != nil {
		return nil, err
	}
	if rootDirty {
		if err := x.writeRoot(ctx, key); err != nil {
			return nil, err
		}
		return claimed, x.restamp(ctx, key, expMs)
	}
	return claimed, nil
}

// AutoClaim is XAUTOCLAIM: walk the PEL inclusively from start,
// claiming entries idle at least minIdle for consumer, dropping the
// rows whose stream entries are gone into the deleted reply, until
// count entries claim or count times ten attempts run out. The cursor
// is the next entry the walk did not examine, 0-0 when it drained.
func (x *Stream) AutoClaim(ctx context.Context, key, group, consumer []byte, minIdle int64, start streamID, count int64, justid bool, nowMs int64) (cursor streamID, claimed, deleted []streamID, err error) {
	zero := streamID{}
	exists, expMs, err := x.stateOf(ctx, key)
	if err != nil {
		return zero, nil, nil, err
	}
	if !exists {
		return zero, nil, nil, errStreamNoGroup
	}
	ord, g0, err := x.findGroup(ctx, group)
	if err != nil {
		return zero, nil, nil, err
	}
	if ord < 0 {
		return zero, nil, nil, errStreamNoGroup
	}
	g := x.copyGroupOwned(&g0)
	ci, err := groupConsumer(&g, consumer, nowMs)
	if err != nil {
		return zero, nil, nil, err
	}
	if ci > math.MaxUint8 {
		return zero, nil, nil, errStreamPelConsumerCap
	}
	after := pendingAfter(start)
	attempts := count * 10
	cache := map[int]*pelSegEdit{}
	cursor = zero
	done := false
	for fi := max(pelFenceSeek(g.pelf, after), 0); fi < len(g.pelf) && !done; fi++ {
		seg, err := x.pelEditLoad(ctx, &g, cache, fi)
		if err != nil {
			return zero, nil, nil, err
		}
		for i := 0; i < len(seg.ents); i++ {
			e := &seg.ents[i]
			if !after.less(e.id) {
				continue
			}
			if int64(len(claimed)) >= count || attempts <= 0 {
				cursor = e.id
				done = true
				break
			}
			attempts--
			live, err := x.entryLive(ctx, key, e.id)
			if err != nil {
				return zero, nil, nil, err
			}
			if !live {
				deleted = append(deleted, e.id)
				g.cons[e.cidx].pel--
				seg.ents = append(seg.ents[:i], seg.ents[i+1:]...)
				seg.dirty = true
				i--
				continue
			}
			if minIdle > 0 && nowMs-e.dtime < minIdle {
				continue
			}
			g.cons[e.cidx].pel--
			g.cons[ci].pel++
			e.cidx = uint8(ci)
			e.dtime = nowMs
			if !justid {
				e.dcount++
			}
			seg.dirty = true
			claimed = append(claimed, e.id)
		}
	}
	g.cons[ci].seenMs = nowMs
	if len(claimed) > 0 {
		g.cons[ci].activeMs = nowMs
	}
	// A pure remove-or-rewrite pass can never grow a segment past the
	// caps (a removed entry's bytes always exceed its successor's delta
	// growth), so rootDirty stays false here; the handling is belt and
	// braces against that arithmetic ever changing.
	rootDirty, err := x.pelEditFinalize(ctx, &g, cache)
	if err != nil {
		return zero, nil, nil, err
	}
	if err := x.writeGroupRec(ctx, uint32(ord), &g); err != nil {
		return zero, nil, nil, err
	}
	if err := x.pelDropPending(ctx); err != nil {
		return zero, nil, nil, err
	}
	if rootDirty {
		if err := x.writeRoot(ctx, key); err != nil {
			return zero, nil, nil, err
		}
		return cursor, claimed, deleted, x.restamp(ctx, key, expMs)
	}
	return cursor, claimed, deleted, nil
}
