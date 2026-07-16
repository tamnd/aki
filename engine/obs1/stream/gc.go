package stream

import structs "github.com/tamnd/aki/engine/obs1/struct"

// The gc rewrite of a partially-tombstoned block (spec 2064/f3/14 section 6.5). XDEL
// only flips a tombstone flag on a sealed block (block.go): the block stays
// append-frozen and keeps its bytes, because a mid-block splice would force a full
// re-encode on the reply path of a point command. So an interior block, neither at
// the front nor empty, accumulates dead bytes that the front whole-block drop (trim.go)
// can never reclaim. This is the deferred rewrite side of that choice: a sealed block
// whose deleted fraction has crossed the gc ratio is rewritten to its live entries on
// the owner's between-batches step (reg.go registers the maintainer, the worker runs
// it at idle), its bytes reclaimed off the command hot path.
//
// The ratio is 0.5, settled by labs/f3/m5/04_tombstone_rewrite: rewriting a block
// whose dead fraction is f reclaims f of its bytes and copies the other 1-f, so the
// reclaim pays for the copy exactly at f=0.5 (the real encoded bytes measure 0.994
// there, re-mastering bending the analytic f/(1-f) a hair). Below half-dead a rewrite
// is a net copy, above it a net reclaim. Under sustained interior churn the pass holds
// the retained dead near half the ratio and the footprint far below the never-collect
// case; tightening the ratio buys little more tightness for much more copy work.

// gcRatioNum and gcRatioDen express stream-block-gc-ratio as an exact fraction so the
// predicate is integer-only on the hot-ish scan: a block qualifies when
// deleted/count >= gcRatioNum/gcRatioDen, i.e. deleted*gcRatioDen >= count*gcRatioNum.
// 1/2 is the 0.5 default from lab 04.
const (
	gcRatioNum = 1
	gcRatioDen = 2
)

// gcStats is what one gc pass reports, for the maintainer's accounting and the tests:
// the sealed blocks rewritten to their live entries, the fully-dead sealed blocks
// dropped whole, and the entry-data bytes reclaimed (the shrink of rewritten blocks;
// a dropped block's whole footprint is freed with the block).
type gcStats struct {
	rewritten int
	dropped   int
	reclaimed int
}

// gc runs one owner between-batches pass over the stream's sealed blocks. A sealed
// block at or past the gc ratio is rewritten in place to its live entries; a sealed
// block that has gone fully dead is dropped whole. The open tail block is never
// touched, it is still filling, and the inline band is left to trim's front splice,
// which already compacts its single block. It returns the pass's work so the
// maintainer and the tests can see what it reclaimed.
func (s *stream) gc() gcStats {
	var st gcStats
	// The gc only applies to the native band's sealed blocks. Inline streams hold one
	// block compacted on trim, and a native stream with only the open tail has nothing
	// sealed to rewrite.
	if s.kind != bandNative || s.dir == nil || len(s.blocks) <= 1 {
		return st
	}
	tail := len(s.blocks) - 1 // physical index of the open tail, left untouched
	dropped := false
	for i := 0; i < tail; i++ {
		b := s.blocks[i]
		// A demoted block holds no resident bytes to reclaim, so gc leaves it cold: its
		// dead entries cost nothing resident, and a write reaching one promotes it first
		// (cold.go, section 7.3), so a cold block never carries a fresh tombstone here.
		if b.cold() {
			continue
		}
		if b.deleted == 0 {
			continue // no tombstones, nothing to reclaim
		}
		if b.live() == 0 {
			dropped = true // fully dead: collected in the drop pass below
			continue
		}
		if b.deleted*gcRatioDen < b.count*gcRatioNum {
			continue // below the ratio: the reclaim would not pay for the copy
		}
		nb := s.rewriteBlock(b)
		s.resBlob -= uint64(b.size() - nb.size())
		st.reclaimed += b.size() - nb.size()
		st.rewritten++
		// A rewrite re-masters against the first surviving entry, so the block's
		// firstID advances whenever its old master was among the tombstoned. The
		// directory keys on firstID, so repoint the leaf; the logical reference is
		// unchanged (same physical slot i), so no survivor renumber is needed. Delete
		// keys off the old first before the swap so the Member callback still resolves
		// the old seq, then insert the new first after.
		if nb.first != b.first {
			s.dir.Delete(b.first.ms, seqKey(b.first), s)
			s.blocks[i] = nb
			s.dir.Insert(nb.first.ms, seqKey(nb.first), uint32(i)+s.base, s)
		} else {
			s.blocks[i] = nb
		}
	}
	if dropped {
		st.dropped = s.dropDeadSealedBlocks()
	}
	return st
}

// rewriteBlock builds a fresh block holding b's live entries in order, re-encoded
// against the first survivor as the new master (section 6.5). The walk yields field
// views into b's blob and appendEntry copies them into the new blob immediately, so
// no clone is needed. The result is strictly no larger than b (fewer or equal entries,
// deltas against a later-or-equal base), so a live entry always lands and the fresh
// block never overflows the budget.
func (s *stream) rewriteBlock(b *block) *block {
	nb := newBlock()
	var scratch []field
	b.walk(scratch, func(id streamID, fields []field) bool {
		nb.appendEntry(id, fields)
		return true
	})
	return nb
}

// dropDeadSealedBlocks removes every fully-dead sealed block (live 0), keeping the
// open tail, and rebuilds the directory over the survivors. A fully-dead block holds
// only tombstoned bytes the front drop cannot reach unless it is at the front, so gc
// collects it here; because an interior removal shifts the physical slots of every
// later block, the base-offset trick (which only tracks front drops) no longer holds,
// so the directory is rebuilt from scratch over the physical survivors with the base
// reset to zero. Removal is rare, only when a block's last live entry is deleted, so
// the O(survivors) rebuild is an acceptable cold path. Returns the count dropped.
func (s *stream) dropDeadSealedBlocks() int {
	tail := len(s.blocks) - 1
	dst := 0
	dropped := 0
	for i, b := range s.blocks {
		// A cold block is skipped: its dead entries hold no resident bytes and dropping
		// it whole would leave a live demote descriptor, so gc keeps it and an approximate
		// XTRIM front drop (trim.go) reclaims it by handle. In practice a block goes fully
		// dead while resident and is dropped the same pass, so this is a defensive guard.
		if i < tail && b.count > 0 && b.live() == 0 && !b.cold() {
			dropped++
			s.resBlob -= uint64(len(b.blob))
			continue
		}
		s.blocks[dst] = b
		dst++
	}
	if dropped == 0 {
		return 0
	}
	for i := dst; i < len(s.blocks); i++ {
		s.blocks[i] = nil // release the dropped blocks
	}
	s.blocks = s.blocks[:dst]
	s.dir = structs.NewTree()
	s.base = 0
	for i := range s.blocks {
		s.dirInsert(i)
	}
	return dropped
}
