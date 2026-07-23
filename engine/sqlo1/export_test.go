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

var ErrStreamFenceThirdLevelForTest = errStreamFenceThirdLevel
