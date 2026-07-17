package sqlo1

import "context"

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
