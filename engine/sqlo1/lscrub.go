package sqlo1

import (
	"context"
	"fmt"
)

// Scrub cross-check, the closing clause of doc 07's L-I6: the fence is
// the single source of list order, so every fence entry must name a
// live node whose header count matches the entry, no node may be named
// twice, and the counts must sum to the root's exact length. The write
// discipline upholds that per command and the replay contract across
// crashes, so a disagreement here means damage the CRC scrub cannot
// see: records that verify fine byte-wise but describe two different
// lists. The engine owns no keyspace walk, so the caller brings the
// candidate roots; VerifySample draws the deterministic sample doc 07
// asks for from whatever list the suite runner supplies.

// Verify cross-checks one list key, answering nil for a healthy key
// and a descriptive error naming the first disagreement otherwise.
// Absent and inline keys pass trivially: the inline decode already
// walks every element against its header count. Noded, the check
// walks the fence in list order, page by page when paged, and reads
// every node once; the root decode has already held the entry sums to
// the root count and every page load cross-checks its parent's total,
// so what Verify adds is the fence-to-node-header agreement and the
// no-aliasing rule those decodes cannot see. Read-only; the footprint
// is one fence view and one node at a time.
func (l *List) Verify(ctx context.Context, key []byte) error {
	st, _, _, err := l.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if st != listNodedState {
		return nil
	}
	r := &l.nodeRoot
	seen := make(map[uint64]bool, r.count/16+1)
	npages := 1
	if r.paged {
		npages = len(r.pidx)
		for i, pe := range r.pidx {
			if seen[pe.segid] {
				return fmt.Errorf("sqlo1: list scrub: page index entry %d of %q names page %d twice", i, key, pe.segid)
			}
			seen[pe.segid] = true
		}
	}
	total, nodes := uint64(0), 0
	for p := range npages {
		if r.paged {
			if err := l.loadPage(ctx, p); err != nil {
				return err
			}
		}
		for i, ent := range l.fence {
			if seen[ent.segid] {
				return fmt.Errorf("sqlo1: list scrub: fence of %q names node %d twice", key, ent.segid)
			}
			seen[ent.segid] = true
			node, err := l.readNode(ctx, ent.segid)
			if err != nil {
				return err
			}
			if node.n != int(ent.count) {
				return fmt.Errorf("sqlo1: list scrub: fence entry %d of %q holds count %d for node %d, node header says %d", i, key, ent.count, ent.segid, node.n)
			}
			total += uint64(ent.count)
			nodes++
		}
	}
	if total != r.count {
		return fmt.Errorf("sqlo1: list scrub: fence of %q walks %d elements over %d nodes, root count says %d", key, total, nodes, r.count)
	}
	return nil
}

// VerifySample verifies up to n keys drawn without replacement from
// keys, a splitmix-style walk over the indices so the same seed always
// draws the same sample. It stops at the first unhealthy key.
func (l *List) VerifySample(ctx context.Context, keys [][]byte, n int, seed uint64) error {
	if n > len(keys) {
		n = len(keys)
	}
	if n <= 0 {
		return nil
	}
	idx := make([]int, len(keys))
	for i := range idx {
		idx[i] = i
	}
	s := seed
	for i := 0; i < n; i++ {
		s = s*6364136223846793005 + 1442695040888963407
		j := i + int(s>>33)%(len(idx)-i)
		idx[i], idx[j] = idx[j], idx[i]
		if err := l.Verify(ctx, keys[idx[i]]); err != nil {
			return err
		}
	}
	return nil
}
