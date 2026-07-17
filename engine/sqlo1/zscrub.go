package sqlo1

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
)

// Scrub cross-check, the closing clause of doc 09's Z-I4: the member
// side and the score side of a segmented zset must carry the same
// (score, member) set, and both must agree with the exact card. The
// dual-write discipline upholds that per command and the replay
// contract upholds it across crashes, so a disagreement here means
// damage the CRC scrub cannot see: bytes that verify fine but
// describe two different zsets. The engine owns no keyspace walk, so
// the caller brings the candidate roots; ZVerifySample draws the
// deterministic sample doc 09 asks for from whatever list the suite
// runner or a future maintenance walk supplies.

// zvPair is one member-side entry staged for the cross-check, its
// member bytes parked in the shared arena.
type zvPair struct {
	s        uint64
	off, end int
}

// ZVerify cross-checks one zset key both ways and against ZCARD,
// answering nil for a healthy key and a descriptive error naming the
// first disagreement otherwise. Absent and inline keys pass
// trivially: the inline tier stores scores only on the member
// entries, one plane, so there is nothing to disagree. The check is
// read-only and streams the score side run by run against the sorted
// member side, so its footprint is the member-side pairs plus one run
// image, and a fence count pointing past its run's live bytes
// surfaces as the run codec's own error.
func (z *ZSet) ZVerify(ctx context.Context, key []byte) error {
	st, _, _, err := z.h.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if st != hashSegState {
		return nil
	}
	card, err := z.ZCard(ctx, key)
	if err != nil {
		return err
	}
	var arena []byte
	pairs := make([]zvPair, 0, card)
	var perr error
	cursor := uint64(0)
	for {
		next, err := z.h.HScan(ctx, key, cursor, 512, func(f, v []byte) {
			if perr != nil {
				return
			}
			if len(v) != 8 {
				perr = fmt.Errorf("sqlo1: zset scrub: member %q of %q carries a %d-byte score value, want 8", f, key, len(v))
				return
			}
			off := len(arena)
			arena = append(arena, f...)
			pairs = append(pairs, zvPair{s: binary.BigEndian.Uint64(v), off: off, end: len(arena)})
		})
		if err != nil {
			return err
		}
		if perr != nil {
			return perr
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if int64(len(pairs)) != card {
		return fmt.Errorf("sqlo1: zset scrub: member side of %q holds %d entries, ZCARD says %d", key, len(pairs), card)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].s != pairs[j].s {
			return pairs[i].s < pairs[j].s
		}
		return bytes.Compare(arena[pairs[i].off:pairs[i].end], arena[pairs[j].off:pairs[j].end]) < 0
	})
	rank := 0
	var werr error
	err = z.zrunWalk(ctx, key, func(s uint64, m []byte) {
		if werr != nil {
			return
		}
		if rank >= len(pairs) {
			werr = fmt.Errorf("sqlo1: zset scrub: score side of %q holds run entry (%#x, %q) past the member side's %d entries", key, s, m, len(pairs))
			return
		}
		p := pairs[rank]
		if p.s != s || !bytes.Equal(arena[p.off:p.end], m) {
			werr = fmt.Errorf("sqlo1: zset scrub: %q diverges at rank %d: score side holds (%#x, %q), member side (%#x, %q)", key, rank, s, m, p.s, arena[p.off:p.end])
			return
		}
		rank++
	})
	if err != nil {
		return err
	}
	if werr != nil {
		return werr
	}
	if rank != len(pairs) {
		return fmt.Errorf("sqlo1: zset scrub: score side of %q holds %d run entries, member side %d", key, rank, len(pairs))
	}
	return nil
}

// ZVerifySample verifies up to n keys drawn without replacement from
// keys, a splitmix-style walk over the indices so the same seed
// always draws the same sample. It stops at the first unhealthy key.
func (z *ZSet) ZVerifySample(ctx context.Context, keys [][]byte, n int, seed uint64) error {
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
		if err := z.ZVerify(ctx, keys[idx[i]]); err != nil {
			return err
		}
	}
	return nil
}
