package sqlo1

import (
	"context"
	"fmt"
	"math"
)

// Test-only handles. The WAL and the recovery entry are package-internal
// API used by the server loop, but the Track A recovery-order test has to
// live in an external test package because engine/sqlo1a imports this
// package; these hand it the internals.
var (
	OpenWALForTest      = openWAL
	RecoverStoreForTest = recoverStore
	AppendPutForTest    = appendPutPayload
	AppendDelForTest    = appendDelPayload
)

const (
	WalOpPutForTest = walOpPut
	WalOpDelForTest = walOpDel
)

// SetExpireForTest stamps an absolute expire_ms on a hot key; the real
// expiry surface arrives when doc 11 wires the wheel into Tiered, and
// the sqlo1b integration test needs the stamp before that.
func (t *Tiered) SetExpireForTest(key []byte, atMs int64) {
	t.ht.setExpireMs(key, atMs)
}

// EvictAllForTest drops every resident header, forcing the next reads
// through the cold path.
func (t *Tiered) EvictAllForTest() {
	for s := range t.ht.hdrs {
		if t.ht.hdrs[s].state == stateResident {
			t.ht.evict(uint32(s), true)
		}
	}
}

// MemScoreForTest and RunWalkForTest hand the zset's two families to
// the Z-I4 torn-tail matrix, which lives in the external test package
// and cross-checks them against each other at every cut.
func (z *ZSet) MemScoreForTest(ctx context.Context, key, member []byte) (float64, bool, error) {
	return z.memScore(ctx, key, member)
}

func (z *ZSet) RunWalkForTest(ctx context.Context, key []byte, emit func(sortable uint64, member []byte)) error {
	return z.zrunWalk(ctx, key, emit)
}

var ZScoreSortableForTest = zScoreSortable

// FencePagedForTest reports whether key's score fence has crossed to
// the paged representation, so the paged torn-tail matrix can prove
// its scenario really drives the pages before it starts cutting.
func (z *ZSet) FencePagedForTest(ctx context.Context, key []byte) (bool, error) {
	st, _, _, err := z.h.stateOf(ctx, key)
	if err != nil || st != hashSegState {
		return false, err
	}
	if err := z.zloadTail(); err != nil {
		return false, err
	}
	return z.zpaged, nil
}

// SetZFenceCapsForTest shrinks the score fence fanouts so the paged
// ladder (transition, leaf split, upper split, third-level error) is
// reachable in test-sized zsets. The transition builds one leaf from
// the whole flat fence plus the splitting run, so flat+1 must fit
// leaf. Callers restore via the returned func.
func SetZFenceCapsForTest(flat, leaf, upper, root int) (restore func()) {
	of, ol, ou, or := zFenceMaxRuns, zFenceLeafMax, zFenceUpperMax, zFenceRootMax
	zFenceMaxRuns, zFenceLeafMax, zFenceUpperMax, zFenceRootMax = flat, leaf, upper, root
	return func() { zFenceMaxRuns, zFenceLeafMax, zFenceUpperMax, zFenceRootMax = of, ol, ou, or }
}

// ListFencePagedForTest reports whether key's fence has crossed to the
// paged representation, so the paged torn-tail matrix can prove its
// scenario really drives the pages before it starts cutting.
func (l *List) ListFencePagedForTest(ctx context.Context, key []byte) (bool, error) {
	st, _, _, err := l.stateOf(ctx, key)
	if err != nil || st != listNodedState {
		return false, err
	}
	return l.nodeRoot.paged, nil
}

// SetListFenceCapsForTest shrinks the list fence fanouts so the paged
// ladder (transition, page spill, page split, third-level error) is
// reachable in test-sized lists. Callers restore via the returned
// func.
func SetListFenceCapsForTest(flat, page, idx int) (restore func()) {
	of, op, oi := listFenceMaxNodes, listFencePageMax, listFencePageIdxMax
	listFenceMaxNodes, listFencePageMax, listFencePageIdxMax = flat, page, idx
	return func() { listFenceMaxNodes, listFencePageMax, listFencePageIdxMax = of, op, oi }
}

// StreamFencePagedForTest reports whether key's fence has crossed to
// the paged representation, so the paged torn-tail matrix can prove
// its scenario really drives the pages before it starts cutting.
func (x *Stream) StreamFencePagedForTest(ctx context.Context, key []byte) (bool, error) {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil || !exists {
		return false, err
	}
	return x.root.paged, nil
}

// SetStreamFenceCapsForTest shrinks the stream fence fanouts so the
// paged ladder (transition, tail page growth, fresh page, third-level
// error) is reachable in test-sized streams. Callers restore via the
// returned func.
func SetStreamFenceCapsForTest(flat, page, idx int) (restore func()) {
	of, op, oi := streamFenceMaxRuns, streamFencePageMax, streamFencePageIdxMax
	streamFenceMaxRuns, streamFencePageMax, streamFencePageIdxMax = flat, page, idx
	return func() { streamFenceMaxRuns, streamFencePageMax, streamFencePageIdxMax = of, op, oi }
}

// The stream crash matrix lives in the external test package, and
// streamID is package-internal, so these doors drive explicit-ID adds
// and full-range walks with plain integers on the seam.
func (x *Stream) AddExplicitForTest(ctx context.Context, key []byte, ms, seq uint64, fv [][]byte) error {
	_, _, err := x.Add(ctx, key, xidExplicit, streamID{ms: ms, seq: seq}, 0, false, fv)
	return err
}

func (x *Stream) RangeAllForTest(ctx context.Context, key []byte, emit func(ms, seq uint64, fv [][]byte)) error {
	full := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
	return x.Range(ctx, key, streamID{}, full, -1, false, func(int) {}, func(id streamID, fv [][]byte) {
		emit(id.ms, id.seq, fv)
	})
}

// TrimForTest drives Trim from the external crash package, taking the
// MINID threshold as plain integers on the same seam.
func (x *Stream) TrimForTest(ctx context.Context, key []byte, byID bool, maxlen int64, minidMs, minidSeq uint64, approx bool, limit int64) (int64, error) {
	return x.Trim(ctx, key, byID, maxlen, streamID{ms: minidMs, seq: minidSeq}, approx, limit)
}

// SetIDForTest drives SetID from the external crash package, taking
// both IDs as plain integers on the same seam.
func (x *Stream) SetIDForTest(ctx context.Context, key []byte, ms, seq uint64, setAdded bool, added uint64, setMaxDel bool, mdMs, mdSeq uint64) error {
	return x.SetID(ctx, key, streamID{ms: ms, seq: seq}, setAdded, added, setMaxDel, streamID{ms: mdMs, seq: mdSeq})
}

// StreamRootLineForTest renders key's root accounting fields for the
// crash snapshot, with ok false when the key is absent, so the torn
// tail matrix proves the counters land with their commands.
func (x *Stream) StreamRootLineForTest(ctx context.Context, key []byte) (line string, ok bool, err error) {
	info, err := x.Info(ctx, key)
	if err == errStreamNoKey {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return fmt.Sprintf("count=%d added=%d last=%d-%d maxdel=%d-%d",
		info.count, info.added, info.last.ms, info.last.seq, info.maxDel.ms, info.maxDel.seq), true, nil
}

// The group crash phase drives XGROUP writes from the external
// package; the ID-taking calls ride the same plain-integer seam, and
// GroupLinesForTest renders the whole group table for the snapshot so
// the matrix proves last-delivered-ID persistence at every cut.
func (x *Stream) GroupCreateForTest(ctx context.Context, key, group []byte, ms, seq uint64, mkstream bool, read int64) error {
	return x.GroupCreate(ctx, key, group, true, streamID{ms: ms, seq: seq}, false, mkstream, read)
}

func (x *Stream) GroupSetIDForTest(ctx context.Context, key, group []byte, ms, seq uint64, read int64) error {
	return x.GroupSetID(ctx, key, group, true, streamID{ms: ms, seq: seq}, false, read)
}

func (x *Stream) GroupLinesForTest(ctx context.Context, key []byte) ([]string, error) {
	var lines []string
	err := x.GroupsInfo(ctx, key, func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool) {
		line := fmt.Appendf(nil, "group=%s last=%d-%d read=%d pending=%d lag=%d/%v",
			g.name, g.last.ms, g.last.seq, g.read, pending, lag, lagOK)
		for i := range g.cons {
			c := &g.cons[i]
			line = fmt.Appendf(line, " cons=%s seen=%d active=%d pel=%d", c.name, c.seenMs, c.activeMs, c.pel)
		}
		lines = append(lines, string(line))
	})
	if err == errStreamNoKey {
		return nil, nil
	}
	return lines, err
}

var ErrStreamFenceThirdLevelForTest = errStreamFenceThirdLevel

// SetStreamPelCapsForTest shrinks the PEL segment and fence caps so
// segment cuts, multi-segment acks, and the inline fence refusal are
// reachable in test-sized PELs. Callers restore via the returned func.
func SetStreamPelCapsForTest(segBytes, segEnts, fenceMax int) (restore func()) {
	ob, oe, of := streamPelSegMaxBytes, streamPelSegMaxEnts, streamPelFenceMax
	streamPelSegMaxBytes, streamPelSegMaxEnts, streamPelFenceMax = segBytes, segEnts, fenceMax
	return func() { streamPelSegMaxBytes, streamPelSegMaxEnts, streamPelFenceMax = ob, oe, of }
}

// The PEL crash phase drives XREADGROUP delivery and XACK from the
// external package on the plain-integer seam, and PelLinesForTest
// renders every group's pending rows through the real fence-and-
// segment read path, so the matrix proves X-I5 exactness at every cut.
func (x *Stream) ReadGroupNewForTest(ctx context.Context, key, group, consumer []byte, count int64, noack bool, nowMs int64) (int, error) {
	n := 0
	err := x.ReadGroupNew(ctx, key, group, consumer, count, noack, nowMs, func(k int) { n = k }, func(streamID, [][]byte) {})
	return n, err
}

func (x *Stream) AckForTest(ctx context.Context, key, group []byte, ids [][2]uint64) (int64, error) {
	sids := make([]streamID, len(ids))
	for i, p := range ids {
		sids[i] = streamID{ms: p[0], seq: p[1]}
	}
	return x.Ack(ctx, key, group, sids)
}

// DelForTest drives XDEL from the external crash package on the
// plain-integer seam.
func (x *Stream) DelForTest(ctx context.Context, key []byte, ids [][2]uint64) (int64, error) {
	sids := make([]streamID, len(ids))
	for i, p := range ids {
		sids[i] = streamID{ms: p[0], seq: p[1]}
	}
	return x.Del(ctx, key, sids)
}

// ClaimForTest and AutoClaimForTest drive the pending-surface crash
// phase from the external package on the plain-integer seam.
func (x *Stream) ClaimForTest(ctx context.Context, key, group, consumer []byte, minIdle int64, ids [][2]uint64, force, justid bool, nowMs int64) (int, error) {
	sids := make([]streamID, len(ids))
	for i, p := range ids {
		sids[i] = streamID{ms: p[0], seq: p[1]}
	}
	o := streamClaimOpts{retry: -1, force: force, justid: justid}
	claimed, err := x.Claim(ctx, key, group, consumer, minIdle, sids, &o, nowMs)
	return len(claimed), err
}

func (x *Stream) AutoClaimForTest(ctx context.Context, key, group, consumer []byte, minIdle int64, startMs, startSeq uint64, count int64, nowMs int64) (claimed, deleted int, err error) {
	_, c, d, err := x.AutoClaim(ctx, key, group, consumer, minIdle, streamID{ms: startMs, seq: startSeq}, count, false, nowMs)
	return len(c), len(d), err
}

func (x *Stream) PelLinesForTest(ctx context.Context, key []byte) ([]string, error) {
	var lines []string
	err := x.FullGroupsInfo(ctx, key, -1, func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool, rows []streamPelRow, consRows [][]streamPelRow) {
		line := fmt.Appendf(nil, "pel group=%s n=%d", g.name, pending)
		for _, r := range rows {
			line = fmt.Appendf(line, " %d-%d@%s#%d/%d", r.id.ms, r.id.seq, g.cons[r.cidx].name, r.dcount, r.dtime)
		}
		lines = append(lines, string(line))
	})
	if err == errStreamNoKey {
		return nil, nil
	}
	return lines, err
}
