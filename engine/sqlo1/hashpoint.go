package sqlo1

// The hash point surface past HSET/HGET/HDEL: doc 06 section 3's
// point table. Everything here rides hash.go's single-owner rules;
// returned values alias internal buffers until the next call, and the
// ops that mutate after reading copy through valBuf first.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
)

// The hash arithmetic sentinels carry Redis's exact wire texts (no
// ERR prefix; the command layer adds it). Overflow and NaN results
// reuse the string layer's sentinels, whose texts match the hash
// commands' replies too.
var (
	ErrHashNotInt   = errors.New("hash value is not an integer")
	ErrHashNotFloat = errors.New("hash value is not a float")
)

// HSetNX writes field only if it is absent and reports whether it
// wrote, Redis's HSETNX. The doc 06 point table words it as HGET then
// HSET, and under the single-owner rule that is exactly what it is.
func (h *Hash) HSetNX(ctx context.Context, key, field, val []byte) (bool, error) {
	_, _, ok, err := h.getEntry(ctx, key, field)
	if err != nil || ok {
		return false, err
	}
	if _, err := h.hset(ctx, key, field, val, 0); err != nil {
		return false, err
	}
	return true, nil
}

// HMGet reads fields in order and calls emit exactly once per field:
// the value and true for a hit, nil and false for a miss. An absent
// key emits all misses; another type is ErrWrongType (HMGET raises it,
// unlike MGET). On a segmented hash every needed segment is read in
// one LookupBatch round, deduped, so a burst of fields in one segment
// costs one record read. Emitted values alias the round's buffers and
// die when HMGet returns.
func (h *Hash) HMGet(ctx context.Context, key []byte, fields [][]byte, emit func(v []byte, ok bool)) error {
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil {
		return err
	}
	switch st {
	case hashAbsent:
		for range fields {
			emit(nil, false)
		}
		return nil
	case hashInlineState:
		// One region walk per field; the inline cap bounds the region
		// at 2 KiB and no Tiered call happens before the emits, so the
		// values alias the root read safely.
		for _, f := range fields {
			it := hashEntryIter{p: hi.entries, enc: h.enc}
			hit := false
			for {
				ef, ev, _, ok, err := it.next()
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				if bytes.Equal(ef, f) {
					emit(ev, true)
					hit = true
					break
				}
			}
			if !hit {
				emit(nil, false)
			}
		}
		return nil
	}

	// Segmented: route every field through the fence, then read each
	// needed segment once. The dedupe keys on segid, not fence index,
	// because a paged root's indexes are page-relative; the linear
	// scan over the unique list is fine at HMGET burst sizes. On a
	// paged root the fenceIdx calls load covering pages before the
	// segment batch, one page cached at a time, so a burst inside one
	// page still costs one page read plus one segment round.
	r := &h.segRoot
	h.mgIdx = h.mgIdx[:0]
	h.mgUniq = h.mgUniq[:0]
	for _, f := range fields {
		i, err := h.fenceIdx(ctx, hashFH(f))
		if err != nil {
			return err
		}
		id := r.fence[i].segid
		slot := -1
		for k, u := range h.mgUniq {
			if u == id {
				slot = k
				break
			}
		}
		if slot < 0 {
			slot = len(h.mgUniq)
			h.mgUniq = append(h.mgUniq, id)
		}
		h.mgIdx = append(h.mgIdx, slot)
	}
	uniq := len(h.mgUniq)
	// The subkeys share one backing buffer, sized up front so appends
	// cannot move it under the aliases already handed out.
	h.mgKeyBuf = grow(h.mgKeyBuf, uniq*SubkeySize)
	h.mgKeys = slices.Grow(h.mgKeys[:0], uniq)[:uniq]
	for slot, id := range h.mgUniq {
		k := h.mgKeyBuf[slot*SubkeySize : (slot+1)*SubkeySize]
		putHashSegKey(k, r.rooth, id)
		h.mgKeys[slot] = k
	}
	h.mgVals, h.mgRoots, h.mgExps, err = h.t.LookupBatch(ctx, h.mgKeys, h.mgVals, h.mgRoots, h.mgExps)
	if err != nil {
		return err
	}
	h.mgSegs = slices.Grow(h.mgSegs[:0], uniq)[:uniq]
	for slot, id := range h.mgUniq {
		if h.mgVals[slot] == nil {
			return fmt.Errorf("sqlo1: hash segment %d of rooth %#x is missing", id, r.rooth)
		}
		h.mgSegs[slot], err = decodeHashSeg(h.mgVals[slot], h.enc)
		if err != nil {
			return err
		}
	}
	for fi, f := range fields {
		v, _, ok, err := hashSegGet(h.mgSegs[h.mgIdx[fi]], hashFH(f), f)
		if err != nil {
			return err
		}
		emit(v, ok)
	}
	return nil
}

// HIncrBy adds delta to the integer at field and returns the result.
// The parse is string2ll-strict like the string INCR family, the
// overflow check happens before any write, and the field's TTL rides
// through unchanged (Redis preserves field TTLs across HINCRBY).
func (h *Hash) HIncrBy(ctx context.Context, key, field []byte, delta int64) (int64, error) {
	v, eExp, ok, err := h.getEntry(ctx, key, field)
	if err != nil {
		return 0, err
	}
	var cur int64
	if ok {
		n, canonical := parseCanonicalInt(v)
		if !canonical {
			return 0, ErrHashNotInt
		}
		cur = n
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	n := cur + delta
	h.valBuf = strconv.AppendInt(h.valBuf[:0], n, 10)
	if _, err := h.hset(ctx, key, field, h.valBuf, eExp); err != nil {
		return 0, err
	}
	return n, nil
}

// HIncrByFloat adds delta to the float at field and returns the exact
// reply bytes, valid until the next call. The current-value and
// result checks mirror Str.IncrByFloat; the caller has already
// rejected a NaN delta.
func (h *Hash) HIncrByFloat(ctx context.Context, key, field []byte, delta float64) ([]byte, error) {
	v, eExp, ok, err := h.getEntry(ctx, key, field)
	if err != nil {
		return nil, err
	}
	var cur float64
	if ok {
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil || math.IsNaN(f) {
			return nil, ErrHashNotFloat
		}
		cur = f
	}
	n := cur + delta
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return nil, ErrNaNInf
	}
	h.valBuf = appendRedisFloat(h.valBuf[:0], n)
	if _, err := h.hset(ctx, key, field, h.valBuf, eExp); err != nil {
		return nil, err
	}
	return h.valBuf, nil
}

// HGetDel reads and removes one field, Redis's HGETDEL. The copy
// outlives the delete, and deleting the last field kills the key
// through HDel's own door. The returned bytes are valid until the
// next call.
func (h *Hash) HGetDel(ctx context.Context, key, field []byte) ([]byte, bool, error) {
	v, _, ok, err := h.getEntry(ctx, key, field)
	if err != nil || !ok {
		return nil, false, err
	}
	h.valBuf = append(h.valBuf[:0], v...)
	if _, err := h.HDel(ctx, key, field); err != nil {
		return nil, false, err
	}
	return h.valBuf, true, nil
}

// HGetEx reads one field and, when edit is set, rewrites its TTL:
// atMs 0 is PERSIST, a past atMs deletes the field (the command layer
// normally catches that case itself; the check here is the backstop
// for the clock moving between its stamp and ours), anything else
// becomes the field's expiry. The returned bytes are valid until the
// next call.
func (h *Hash) HGetEx(ctx context.Context, key, field []byte, edit bool, atMs int64) ([]byte, bool, error) {
	v, eExp, ok, err := h.getEntry(ctx, key, field)
	if err != nil || !ok {
		return nil, false, err
	}
	h.valBuf = append(h.valBuf[:0], v...)
	if !edit || atMs == eExp {
		return h.valBuf, true, nil
	}
	if atMs != 0 && atMs <= h.t.Now() {
		if _, err := h.HDel(ctx, key, field); err != nil {
			return nil, false, err
		}
		return h.valBuf, true, nil
	}
	if _, err := h.hset(ctx, key, field, h.valBuf, atMs); err != nil {
		return nil, false, err
	}
	return h.valBuf, true, nil
}
