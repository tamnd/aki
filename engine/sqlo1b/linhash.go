package sqlo1b

import (
	"errors"
	"fmt"
)

// Linear hashing over chunk buckets (doc 03 section 8.5).
//
// The table state is the pair (level L, split pointer S), packed
// into the superblock's hash_epoch by PackHashEpoch. Buckets
// 0..2^L+S-1 exist; buckets below S have already split at level L
// and address with L+1 placement bits. Splits are local: one bucket
// redistributes into two, the state advances, and nothing else
// moves.
//
// The split policy is lf85 from the chunkindex verdict
// (results/sqlo1/b2-chunkindex.md): split when the table is over 85%
// of buckets*42 entries. The verdict showed the doc's overflow-only
// policy has no steady state (fill sweeps 0.82..0.97 per doubling
// and chain2 crosses the 0.1% red line at cycle top), while lf85
// holds 0.45 B/key at 1e9 with chains priced in.

// ErrWindowExhausted reports that a bucket's split windows do not
// cover the split level and no refresh callback was supplied. The
// caller reads the bucket's records and retries with a RefreshFunc.
var ErrWindowExhausted = errors.New("sqlo1b: split windows do not cover the split level")

// BucketOf maps placement bits to a bucket under (level, split).
// Buckets below the split pointer have already split and address
// with one more bit.
func BucketOf(placement uint64, level uint8, split uint64) uint64 {
	b := placement & (uint64(1)<<level - 1)
	if b < split {
		b = placement & (uint64(1)<<(level+1) - 1)
	}
	return b
}

// NumBuckets returns the bucket count under (level, split).
func NumBuckets(level uint8, split uint64) uint64 {
	return uint64(1)<<level + split
}

// AdvanceSplit moves the state past one completed split, wrapping
// the split pointer into the next level when the doubling finishes.
func AdvanceSplit(level uint8, split uint64) (uint8, uint64) {
	split++
	if split == uint64(1)<<level {
		return level + 1, 0
	}
	return level, split
}

// ShouldSplit is the lf85 policy in integer form: split while
// entries exceed 85% of buckets*42, that is entries*20 >
// buckets*714 (714 = 42*17 and 85/100 = 17/20).
func ShouldSplit(entries, buckets uint64) bool {
	return entries*20 > buckets*714
}

// A RefreshFunc returns the key hash behind each vptr, in the order
// given. The store implements it as one batched read of the
// bucket's records; the machinery calls it only when the bucket's
// windows no longer cover the split level, which section 8.5 prices
// at once per nine levels a chunk survives.
type RefreshFunc func(vptrs []uint64) ([]uint64, error)

type splitEntry struct {
	fp   uint16
	meta uint16
	vptr uint64
}

// SplitBucket redistributes bucket bucketNo (base chunk first, then
// its overflow chain in order) between the bucket it keeps and the
// new bucket at bucketNo + 2^level, routing each entry by hash bit
// level. Both sides come back as fresh chunks, base first and
// overflow chunks after, with chain pointers unset because linking
// needs the positions the store assigns at write time; a side that
// will chain holds 41 entries in every chunk before its last.
//
// When every chunk shares one window base that covers the level,
// routing uses the stored windows and the children inherit the
// base. Otherwise the whole bucket refreshes through the callback
// and the children rebase to the level. Mixed bases force a refresh
// even if each covers the level on its own, because the children
// need every window expressed at one base and only the hash can
// re-derive a window.
func SplitBucket(chunks []*Chunk, bucketNo uint64, level uint8, refresh RefreshFunc) (left, right []*Chunk, err error) {
	if len(chunks) == 0 {
		return nil, nil, errors.New("sqlo1b: split of an empty bucket image")
	}
	if level > maxWindowBase {
		return nil, nil, fmt.Errorf("sqlo1b: split level %d past placement bit %d", level, maxWindowBase)
	}
	if bucketNo>>level != 0 {
		return nil, nil, fmt.Errorf("sqlo1b: bucket %d does not exist at level %d", bucketNo, level)
	}
	for _, c := range chunks {
		if c.ChunkNoLo() != uint32(bucketNo) {
			return nil, nil, fmt.Errorf("sqlo1b: chunk_no_lo %#x in the image of bucket %#x", c.ChunkNoLo(), bucketNo)
		}
	}

	base := chunks[0].WindowBase()
	covered := level >= base && level-base < WindowBits
	for _, c := range chunks[1:] {
		if c.WindowBase() != base {
			covered = false
		}
	}

	var ls, rs []splitEntry
	childBase := base
	if covered {
		for _, c := range chunks {
			for i := range c.Count() {
				fp, meta, vptr := c.EntryAt(i)
				bit, ok := WindowBit(meta, base, level)
				if !ok {
					return nil, nil, fmt.Errorf("sqlo1b: window at base %d lost level %d", base, level)
				}
				e := splitEntry{fp, meta, vptr}
				if bit == 0 {
					ls = append(ls, e)
				} else {
					rs = append(rs, e)
				}
			}
		}
	} else {
		if refresh == nil {
			return nil, nil, ErrWindowExhausted
		}
		childBase = level
		var vptrs []uint64
		var ents []splitEntry
		for _, c := range chunks {
			for i := range c.Count() {
				fp, meta, vptr := c.EntryAt(i)
				ents = append(ents, splitEntry{fp, meta, vptr})
				vptrs = append(vptrs, vptr)
			}
		}
		hashes, rerr := refresh(vptrs)
		if rerr != nil {
			return nil, nil, fmt.Errorf("sqlo1b: bucket %d refresh: %w", bucketNo, rerr)
		}
		if len(hashes) != len(vptrs) {
			return nil, nil, fmt.Errorf("sqlo1b: refresh returned %d hashes for %d records", len(hashes), len(vptrs))
		}
		for i, e := range ents {
			h := hashes[i]
			if Fingerprint(h) != e.fp {
				return nil, nil, fmt.Errorf("sqlo1b: refreshed hash %#x mismatches fingerprint %#x at vptr %#x", h, e.fp, e.vptr)
			}
			if PlacementBits(h)&(uint64(1)<<level-1) != bucketNo {
				return nil, nil, fmt.Errorf("sqlo1b: refreshed hash %#x does not place in bucket %d at level %d", h, bucketNo, level)
			}
			w, werr := SplitWindow(h, level)
			if werr != nil {
				return nil, nil, werr
			}
			meta, werr := MetaWithWindow(e.meta, w)
			if werr != nil {
				return nil, nil, werr
			}
			e.meta = meta
			if PlacementBits(h)>>level&1 == 0 {
				ls = append(ls, e)
			} else {
				rs = append(rs, e)
			}
		}
	}

	left, err = packBucket(ls, bucketNo, childBase)
	if err != nil {
		return nil, nil, err
	}
	right, err = packBucket(rs, bucketNo+uint64(1)<<level, childBase)
	if err != nil {
		return nil, nil, err
	}
	return left, right, nil
}

// packBucket lays entries into fresh chunks for bucket chunkNo. Any
// chunk followed by another keeps its count at 41 so the store's
// SetChain fits afterward; the last chunk holds up to 42. An empty
// side still gets its one empty chunk, because both buckets exist
// after a split.
func packBucket(entries []splitEntry, chunkNo uint64, windowBase uint8) ([]*Chunk, error) {
	var out []*Chunk
	rest := entries
	for {
		c, err := NewChunk(chunkNo, windowBase)
		if err != nil {
			return nil, err
		}
		n := len(rest)
		if n > ChunkCap {
			n = ChunkChainCap
		}
		for _, e := range rest[:n] {
			if err := c.InsertEntry(e.fp, e.meta, e.vptr); err != nil {
				return nil, err
			}
		}
		out = append(out, c)
		rest = rest[n:]
		if len(rest) == 0 {
			return out, nil
		}
	}
}
