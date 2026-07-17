package sqlo1

import (
	"context"
	"encoding/binary"
	"fmt"
)

// ZSet is the sorted-set layer over Tiered: the doc 09 model, whose
// member side is the doc 06 hash machinery with the value fixed at
// the 8-byte sortable score image. It rides the same Hash type with
// the encZMem codec dimension, so the representation ladder (inline
// root, member segments, fence paging), the mh partitioning, and the
// W1-W4 write rules are all the hash's, byte discipline included.
// The stored score bytes are the big-endian #950 sortable transform,
// so the exact bytes a member entry holds are the bytes the score
// runs sort and fence on, with no re-encoding at the seam. The score
// side (runs, root tail section) is the next slice; until it lands,
// the member ops stay unexported and ZADD does not exist.
type ZSet struct {
	h *Hash

	// sbuf holds the encoded score image of the current point write.
	sbuf [zmemScoreLen]byte

	// Score-side scratch, zrun.go: the decoded fence of the root
	// under operation, the tail image a root write lands, the run
	// images an op builds, and the run subkey.
	zfence []zFenceEnt
	ztail  []byte
	zrbuf  []byte
	zrbuf2 []byte
	zkbuf  [SubkeySize]byte
}

// NewZSet builds the zset layer over t.
func NewZSet(t *Tiered, cfg HashConfig) (*ZSet, error) {
	h, err := newSegLadder(t, cfg)
	if err != nil {
		return nil, err
	}
	h.tag, h.subSeg, h.subInline = TagZset, zsetSubSeg, zsetSubInline
	h.enc = encZMem
	return &ZSet{h: h}, nil
}

// memSet writes member's score on the member side, reporting whether
// the member was created. Slice 4's ZADD wraps this with the score
// side of the dual write; alone it upholds Z-I1's member half only.
func (z *ZSet) memSet(ctx context.Context, key, member []byte, score float64) (bool, error) {
	binary.BigEndian.PutUint64(z.sbuf[:], zScoreSortable(score))
	return z.h.hset(ctx, key, member, z.sbuf[:], 0)
}

// memScore answers member's score, the ZSCORE read path: root plus at
// most one segment hot, the doc 09 two-record cold bound.
func (z *ZSet) memScore(ctx context.Context, key, member []byte) (float64, bool, error) {
	v, _, ok, err := z.h.getEntry(ctx, key, member)
	if err != nil || !ok {
		return 0, false, err
	}
	if len(v) != zmemScoreLen {
		return 0, false, fmt.Errorf("sqlo1: zset member score of %d bytes, want %d", len(v), zmemScoreLen)
	}
	return zScoreFromSortable(binary.BigEndian.Uint64(v)), true, nil
}

// memDel removes member from the member side, reporting whether it
// existed. Slice 4's ZREM pairs this with the score-run removal.
func (z *ZSet) memDel(ctx context.Context, key, member []byte) (bool, error) {
	return z.h.HDel(ctx, key, member)
}

// ZCard answers the member count. The member-side root count is the
// Z-I2 authority the score-run counts must sum to.
func (z *ZSet) ZCard(ctx context.Context, key []byte) (int64, error) {
	return z.h.HLen(ctx, key)
}

// zmemStats is the member-side occupancy telemetry memStats answers:
// segment count, member total, summed encoded payload bytes, and the
// smallest and largest per-segment member counts. The doc 09 band
// (80-150 members per segment at leaderboard-shaped keys) is what the
// occupancy test holds the ladder to.
type zmemStats struct {
	segs    int
	members int
	bytes   int
	minSeg  int
	maxSeg  int
}

// memStats walks every member segment of a segmented zset and sums
// the occupancy picture. Inline roots answer the zero value with the
// member count; a telemetry walk, not a serving path, so it reads
// segments one at a time.
func (z *ZSet) memStats(ctx context.Context, key []byte) (zmemStats, error) {
	h := z.h
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil || st == hashAbsent {
		return zmemStats{}, err
	}
	if st == hashInlineState {
		return zmemStats{members: hi.count, bytes: len(hi.entries)}, nil
	}
	r := &h.segRoot
	s := zmemStats{members: int(r.count)}
	pages := 1
	if r.paged {
		pages = len(r.pidx)
	}
	for p := range pages {
		if err := h.loadPage(ctx, p); err != nil {
			return zmemStats{}, err
		}
		for _, e := range r.fence {
			seg, err := h.readSeg(ctx, e.segid)
			if err != nil {
				return zmemStats{}, err
			}
			if s.segs == 0 || seg.n < s.minSeg {
				s.minSeg = seg.n
			}
			s.maxSeg = max(s.maxSeg, seg.n)
			s.segs++
			s.bytes += len(seg.entries)
		}
	}
	return s, nil
}
