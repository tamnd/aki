package sqlo1

// The field-TTL surface, doc 06 section 4: HEXPIRE and friends set an
// entry's expire_ms through the same hset door every point mutator
// uses, reads filter dead entries lazily (getEntry), and ReapDue is
// the active half: one hash's due segments rewritten without the dead
// entries, the walk the doc 11 expiry loop drives per wheel hit. The
// count-bearing walks (HGETALL, HRANDFIELD) reap before they iterate,
// through stateOfLive, so their exact-count contracts hold without
// filtering mid-stream.

import (
	"context"
	"encoding/binary"
)

// HExpireCond is the HEXPIRE condition flag: set only when the field
// has no TTL (NX), has one (XX), or when the new time is later (GT)
// or earlier (LT) than the current one; GT and LT treat no-TTL as
// infinite and never replace it with a later time or fail to replace
// it with an earlier one, Redis's exact table.
type HExpireCond uint8

const (
	HExpireNone HExpireCond = iota
	HExpireNX
	HExpireXX
	HExpireGT
	HExpireLT
)

// HExpire sets the expiry of fields to the absolute atMs under cond
// and appends one code per field to res: 1 set, 0 condition not met,
// -2 no such field, 2 field deleted because atMs is already due. The
// condition is checked before the past-time delete, and a hash whose
// last live field dies this way keeps its key until the reaper or a
// later delete empties it physically, the lazy model's rule.
func (h *Hash) HExpire(ctx context.Context, key []byte, atMs int64, cond HExpireCond, fields [][]byte, res []int64) ([]int64, error) {
	st, _, _, err := h.stateOf(ctx, key)
	if err != nil {
		return nil, err
	}
	if st == hashAbsent {
		for range fields {
			res = append(res, -2)
		}
		return res, nil
	}
	for _, f := range fields {
		v, eExp, ok, err := h.getEntry(ctx, key, f)
		if err != nil {
			return nil, err
		}
		if !ok {
			res = append(res, -2)
			continue
		}
		pass := true
		switch cond {
		case HExpireNX:
			pass = eExp == 0
		case HExpireXX:
			pass = eExp != 0
		case HExpireGT:
			pass = eExp != 0 && atMs > eExp
		case HExpireLT:
			pass = eExp == 0 || atMs < eExp
		}
		if !pass {
			res = append(res, 0)
			continue
		}
		if atMs <= h.t.Now() {
			if _, err := h.HDel(ctx, key, f); err != nil {
				return nil, err
			}
			res = append(res, 2)
			continue
		}
		if atMs == eExp {
			res = append(res, 1)
			continue
		}
		h.valBuf = append(h.valBuf[:0], v...)
		if _, err := h.hset(ctx, key, f, h.valBuf, atMs); err != nil {
			return nil, err
		}
		res = append(res, 1)
	}
	return res, nil
}

// HTtl appends each field's absolute expire_ms to res: -2 for a
// missing (or expired) field, -1 for a field with no TTL. The command
// layer owns the wire conversions (remaining seconds round up).
func (h *Hash) HTtl(ctx context.Context, key []byte, fields [][]byte, res []int64) ([]int64, error) {
	st, _, _, err := h.stateOf(ctx, key)
	if err != nil {
		return nil, err
	}
	if st == hashAbsent {
		for range fields {
			res = append(res, -2)
		}
		return res, nil
	}
	for _, f := range fields {
		_, eExp, ok, err := h.getEntry(ctx, key, f)
		if err != nil {
			return nil, err
		}
		switch {
		case !ok:
			res = append(res, -2)
		case eExp == 0:
			res = append(res, -1)
		default:
			res = append(res, eExp)
		}
	}
	return res, nil
}

// HPersist clears each field's TTL and appends one code per field to
// res: 1 cleared, -1 no TTL to clear, -2 no such field.
func (h *Hash) HPersist(ctx context.Context, key []byte, fields [][]byte, res []int64) ([]int64, error) {
	st, _, _, err := h.stateOf(ctx, key)
	if err != nil {
		return nil, err
	}
	if st == hashAbsent {
		for range fields {
			res = append(res, -2)
		}
		return res, nil
	}
	for _, f := range fields {
		v, eExp, ok, err := h.getEntry(ctx, key, f)
		if err != nil {
			return nil, err
		}
		switch {
		case !ok:
			res = append(res, -2)
			continue
		case eExp == 0:
			res = append(res, -1)
			continue
		}
		h.valBuf = append(h.valBuf[:0], v...)
		if _, err := h.hset(ctx, key, f, h.valBuf, 0); err != nil {
			return nil, err
		}
		res = append(res, 1)
	}
	return res, nil
}

// stateOfLive is stateOf with the active-expiry preamble: a root whose
// min_expire is due gets its dead fields reaped first, so the state
// the caller walks holds only live entries and the root count is
// exact live. The walks whose begin(count) contract streams an array
// header before the entries (HITERATE, HRANDFIELD) read through here;
// point reads filter per entry instead and HSCAN skips dead entries
// in its emit walk, both cheaper than a reap.
func (h *Hash) stateOfLive(ctx context.Context, key []byte) (hashState, hashInline, int64, error) {
	st, hi, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return st, hi, expMs, err
	}
	now := h.t.Now()
	switch {
	case st == hashInlineState && hi.minExpMs != 0 && hi.minExpMs <= now:
		if _, err := h.reapInline(ctx, key, hi, expMs); err != nil {
			return hashAbsent, hashInline{}, 0, err
		}
	case st == hashSegState && h.segRoot.minExpMs != 0 && h.segRoot.minExpMs <= now:
		if _, err := h.reapSeg(ctx, key, expMs); err != nil {
			return hashAbsent, hashInline{}, 0, err
		}
	default:
		return st, hi, expMs, nil
	}
	return h.stateOf(ctx, key)
}

// ReapDue is the active-expiry walk for one hash, the callee of the
// doc 11 expiry loop's wheel hit (registration rides ExpireHook): due
// segments are rewritten without their dead entries, the segment and
// root min_expire chains recompute exactly from what was read, the
// count drops by what died, and a hash whose every field was dead
// dies as a key. Returns the number of entries reaped. A hash whose
// root min_expire is not due does nothing; a due min with nothing
// actually dead (min is stale-early by design) recomputes the min
// forward, the wasted-probe cost H-I6 prices in.
func (h *Hash) ReapDue(ctx context.Context, key []byte) (int, error) {
	st, hi, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	now := h.t.Now()
	switch st {
	case hashInlineState:
		if hi.minExpMs == 0 || hi.minExpMs > now {
			return 0, nil
		}
		return h.reapInline(ctx, key, hi, expMs)
	case hashSegState:
		if h.segRoot.minExpMs == 0 || h.segRoot.minExpMs > now {
			return 0, nil
		}
		return h.reapSeg(ctx, key, expMs)
	}
	return 0, nil
}

// reapInline rewrites an inline root without its dead entries. The
// inline min_expire is exact (the codec validates it against the
// entries), so a due min always names at least one dead entry.
func (h *Hash) reapInline(ctx context.Context, key []byte, hi hashInline, expMs int64) (int, error) {
	h.rootBuf = grow(h.rootBuf, hashInlineHdrLen)
	it := hashEntryIter{p: hi.entries}
	now := h.t.Now()
	live := 0
	minExp := int64(0)
	for {
		before := it.p
		_, _, eExp, ok, err := it.next()
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		if eExp != 0 && eExp <= now {
			continue
		}
		h.rootBuf = append(h.rootBuf, before[:len(before)-len(it.p)]...)
		live++
		if eExp != 0 && (minExp == 0 || eExp < minExp) {
			minExp = eExp
		}
	}
	dropped := hi.count - live
	if dropped == 0 {
		return 0, nil
	}
	if live == 0 {
		if _, err := h.t.Del(ctx, key); err != nil {
			return 0, err
		}
		h.fireMin(key, hi.minExpMs, 0)
		return dropped, nil
	}
	putHashInlineHdr(h.rootBuf, live, minExp)
	if err := h.t.Set(ctx, key, h.rootBuf, TagHash|TagRoot); err != nil {
		return 0, err
	}
	h.fireMin(key, hi.minExpMs, minExp)
	return dropped, h.restamp(ctx, key, expMs)
}

// reapSeg rewrites the due segments of the current segRoot without
// their dead entries. Candidates are the fence entries whose meta
// carries the has-TTL bit; each is read for its header min, so the
// root min recomputes exactly over everything that could hold a TTL.
// Segments are read and rewritten one at a time (the rewrite is a
// Tiered call, which would invalidate a batch round's other reads);
// fence meta edits coalesce into one page write per touched page, and
// one delta root closes the walk, the hdelSeg frame order. A segment
// emptied by the reap keeps its bare header for the lazy merge.
func (h *Hash) reapSeg(ctx context.Context, key []byte, expMs int64) (int, error) {
	r := &h.segRoot
	now := h.t.Now()
	preMin := r.minExpMs
	dropped := 0
	newMin := int64(0)
	pages := 1
	if r.paged {
		pages = len(r.pidx)
	}
	for p := range pages {
		if err := h.loadPage(ctx, p); err != nil {
			return 0, err
		}
		pageDirty := false
		for i := range r.fence {
			if r.fence[i].meta&hashMetaHasTTL == 0 {
				continue
			}
			s, err := h.readSeg(ctx, r.fence[i].segid)
			if err != nil {
				return 0, err
			}
			if s.minExpMs == 0 || s.minExpMs > now {
				if s.minExpMs != 0 && (newMin == 0 || s.minExpMs < newMin) {
					newMin = s.minExpMs
				}
				continue
			}
			h.segBuf = grow(h.segBuf, hashSegHdrLen)
			it := hashEntryIter{p: s.entries}
			live := 0
			segMin := int64(0)
			for {
				before := it.p
				_, _, eExp, ok, err := it.next()
				if err != nil {
					return 0, err
				}
				if !ok {
					break
				}
				if eExp != 0 && eExp <= now {
					continue
				}
				h.segBuf = append(h.segBuf, before[:len(before)-len(it.p)]...)
				live++
				if eExp != 0 && (segMin == 0 || eExp < segMin) {
					segMin = eExp
				}
			}
			putHashSegHdr(h.segBuf, live, segMin)
			if err := h.writeSeg(ctx, r.fence[i].segid, h.segBuf); err != nil {
				return 0, err
			}
			dropped += s.n - live
			if segMin != 0 && (newMin == 0 || segMin < newMin) {
				newMin = segMin
			}
			if meta := hashSegMeta(live, segMin); meta != r.fence[i].meta {
				r.fence[i].meta = meta
				pageDirty = true
			}
		}
		if pageDirty && r.paged {
			if err := h.writeFencePage(ctx); err != nil {
				return 0, err
			}
		}
	}
	if dropped == 0 && newMin == preMin {
		return 0, nil
	}
	if uint64(dropped) == r.count {
		h.t.Bump(key, r.rooth, r.rootgen+1)
		if _, err := h.t.Del(ctx, key); err != nil {
			return 0, err
		}
		h.fireMin(key, preMin, 0)
		return dropped, nil
	}
	r.count -= uint64(dropped)
	r.minExpMs = newMin
	if err := h.writeSegRoot(ctx, key, true); err != nil {
		return 0, err
	}
	h.fireMin(key, preMin, newMin)
	return dropped, h.restamp(ctx, key, expMs)
}

// hashSegMinOf reads a segment header's min_expire; the reap tests
// pin the chain with it.
func hashSegMinOf(p []byte) int64 {
	return int64(binary.LittleEndian.Uint64(p[4:]))
}
