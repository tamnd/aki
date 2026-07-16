package sqlo1

import (
	"bytes"
	"context"
)

// Set is the set layer over Tiered: the doc 08 model, which is the
// doc 06 hash machinery with no values. It rides the same Hash type
// with the valless codec dimension on, so the representation ladder
// (inline root, member segments, fence paging), the fh partitioning,
// and the W1-W4 write rules are all the hash's, byte discipline
// included. Members are hash fields; there is no value and no field
// TTL, so a set entry is eflags, mlen, member and nothing else.
type Set struct {
	h *Hash
}

// NewSet builds the set layer over t.
func NewSet(t *Tiered, cfg HashConfig) (*Set, error) {
	h, err := newSegLadder(t, cfg)
	if err != nil {
		return nil, err
	}
	h.tag, h.subSeg, h.subInline = TagSet, setSubSeg, setSubInline
	h.valless = true
	return &Set{h: h}, nil
}

// SAdd adds member to the set at key, reporting whether it was
// created. Adding a member that already exists is a no-op write of
// the same bytes; the hash layer already treats it as not-created.
func (s *Set) SAdd(ctx context.Context, key, member []byte) (bool, error) {
	return s.h.hset(ctx, key, member, nil, 0)
}

// SRem removes member, reporting whether it was there.
func (s *Set) SRem(ctx context.Context, key, member []byte) (bool, error) {
	return s.h.HDel(ctx, key, member)
}

// SIsMember reports membership.
func (s *Set) SIsMember(ctx context.Context, key, member []byte) (bool, error) {
	_, _, ok, err := s.h.getEntry(ctx, key, member)
	return ok, err
}

// SCard returns the member count, 0 for a missing key. Count
// exactness is the hash's SE-I2 story: inline counts sit in the root
// header and segmented counts are patched by W3 reconciliation.
func (s *Set) SCard(ctx context.Context, key []byte) (int64, error) {
	return s.h.HLen(ctx, key)
}

// SMIsMember answers membership for a batch of members through the
// hash's batched read: on a segmented set every needed segment is
// fetched in one LookupBatch round, so a burst of members probing one
// segment costs one record read. emit runs once per member in order.
func (s *Set) SMIsMember(ctx context.Context, key []byte, members [][]byte, emit func(ok bool)) error {
	return s.h.HMGet(ctx, key, members, func(_ []byte, ok bool) {
		emit(ok)
	})
}

// SMove moves member from src to dst, reporting whether it happened
// (false means member was not in src). Both keys type-gate before any
// write, so a wrong-typed dst never leaves src half-moved.
//
// The crash story is doc 08's frame group, built from the write order
// and one guard. The add to dst goes first and the remove from src
// second, so the member is in at least one set at every drain batch
// boundary; a torn tail can leave it in both (the command replays as
// not-yet-finished) but can never lose it. The guard is the one
// setRangeRope uses: a record the remove will coalesce into (src root
// or the member's segment) that is already dirty holds a drain-queue
// position ahead of the add's fresh entries, and a batch cut there
// would commit the remove before the add, so the tier flushes first.
// dst-side dirt only moves the add earlier, the safe direction, but
// it is checked too so the pair's frames stay contiguous in the WAL.
func (s *Set) SMove(ctx context.Context, src, dst, member []byte) (bool, error) {
	h := s.h
	st, _, _, err := h.stateOf(ctx, src)
	if err != nil {
		return false, err
	}
	dirty := h.t.ht.dirtyKey(src)
	if st == hashSegState {
		i, err := h.fenceIdx(ctx, hashFH(member))
		if err != nil {
			return false, err
		}
		putHashSegKey(h.kbuf[:], h.segRoot.rooth, h.segRoot.fence[i].segid)
		dirty = dirty || h.t.ht.dirtyKey(h.kbuf[:])
	}
	dstSt, _, _, err := h.stateOf(ctx, dst)
	if err != nil {
		return false, err
	}
	dirty = dirty || h.t.ht.dirtyKey(dst)
	if dstSt == hashSegState {
		i, err := h.fenceIdx(ctx, hashFH(member))
		if err != nil {
			return false, err
		}
		putHashSegKey(h.kbuf[:], h.segRoot.rooth, h.segRoot.fence[i].segid)
		dirty = dirty || h.t.ht.dirtyKey(h.kbuf[:])
	}
	if _, _, ok, err := h.getEntry(ctx, src, member); err != nil || !ok {
		return false, err
	}
	if bytes.Equal(src, dst) {
		return true, nil
	}
	if dirty {
		if err := h.t.Flush(ctx); err != nil {
			return false, err
		}
	}
	if _, err := h.hset(ctx, dst, member, nil, 0); err != nil {
		return false, err
	}
	if _, err := h.HDel(ctx, src, member); err != nil {
		return false, err
	}
	return true, nil
}

// Encoding answers OBJECT ENCODING for sets: intset for an inline
// all-integer set, listpack for any other inline set, hashtable once
// segmented. intset is compat surface only; there is one inline
// encoding underneath and the all-int flag picks the answer. The flag
// is one-way like Redis's intset conversion: set on create when the
// first member is a canonical integer, cleared for the key's lifetime
// by the first non-integer member, never restored by removals.
func (s *Set) Encoding(ctx context.Context, key []byte) (string, bool, error) {
	st, hi, _, err := s.h.stateOf(ctx, key)
	if err != nil {
		return "", false, err
	}
	switch st {
	case hashAbsent:
		return "", false, nil
	case hashSegState:
		return "hashtable", true, nil
	}
	if hi.allInt {
		return "intset", true, nil
	}
	return "listpack", true, nil
}

// isCanonicalInt reports whether b is a canonical integer member in
// the string2ll sense the INCR family uses: no leading zeros, no
// plus, no minus zero, fits int64.
func isCanonicalInt(b []byte) bool {
	_, ok := parseCanonicalInt(b)
	return ok
}
