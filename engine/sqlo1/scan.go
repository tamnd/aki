package sqlo1

import (
	"context"
	"errors"
	"hash/maphash"
)

// The SCAN walk, doc 12 section 2.1: a numeric-cursor pass over the
// whole keyspace, hot shadow first, then the cold walk with per-key
// hot suppression.
//
// The doc sketched a reverse-bit cursor, Redis's dict trick, but that
// trick exists to survive table doublings and shrinks. This index is
// linear-hashed and grow-only: a split moves entries from bucket b to
// b + 2^level and nothing ever moves down, so a plain forward bucket
// walk already delivers every key that is present for the whole scan
// at least once. The forward cursor ships; the divergence is on
// record in the T8 notes.
//
// The hot shadow goes first, and that order is what makes the merge
// split-stable across tier moves: a dirty-only key the shadow has not
// reached yet can only leave the shadow by draining, which files it
// in the cold index before the cold walk (which runs strictly after
// the shadow) begins, and a key can only leave the cold index for the
// shadow by dying first, which no scan promises to report.

// KeyScanner is the optional Store capability behind SCAN: a
// numeric-cursor walk over the store's records that only pauses on
// walk-unit boundaries (index buckets for the chunk index), so a
// small budget can never pin the cursor in place. Cursor 0 starts
// the walk; a returned cursor of 0 means it finished. The walk
// examines at least budget records before pausing when that many
// remain, and may deliver more, because a unit is never split across
// calls. fn sees records of every type; the caller filters. The
// cursor space is 63 bits: the top bit belongs to ScanStep's phase
// encoding, so an implementation must never return it set.
type KeyScanner interface {
	ScanKeys(ctx context.Context, cursor uint64, budget int, fn func(Record)) (uint64, error)
}

// ErrScanUnsupported answers SCAN over a store without the
// KeyScanner capability.
var ErrScanUnsupported = errors.New("sqlo1: store does not support keyspace scans")

// scanHotBit marks a cursor still inside the hot shadow; the low bits
// carry the next header slot. A cold-phase cursor stores the backend
// cursor plus one, so a live cursor is never zero and zero keeps both
// of its SCAN meanings: start on the way in, done on the way out.
const scanHotBit = uint64(1) << 63

// scanShadow walks header slots from slot on, delivering the hot
// shadow of a keyspace scan: live key-class entries the store is not
// known to hold yet (vptr 0), which is exactly the keyDelta +1 set.
// Everything else is either the cold walk's job (vptr set), a plane
// record (gen or fence), or invisible (tombstone, expired). budget
// counts slots examined, so free slots cannot stall the pass, and the
// walk never touches read stamps: a scan must not disturb the clocks
// eviction ranks by. It returns the next slot and whether the shadow
// is exhausted; the caller reads slots examined off next minus from.
func (t *HotTable) scanShadow(from uint32, budget int, fn func(key []byte, tag uint8)) (next uint32, done bool) {
	s := from
	for ; s < uint32(len(t.hdrs)) && budget > 0; s++ {
		budget--
		hd := &t.hdrs[s]
		if hd.state == 0 || hd.valRef == 0 || hd.vptr != 0 {
			continue
		}
		if hd.gen != 0 || hd.typeTag&TagFence != 0 || t.expired(hd) {
			continue
		}
		fn(t.keys.data(hd.keyRef), hd.typeTag&0x0F)
	}
	return s, s >= uint32(len(t.hdrs))
}

// peek resolves key to its header without touching read stamps, the
// scan-side sibling of probeEntry.
func (t *HotTable) peek(key []byte) (*hdr, bool) {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	if !ok {
		return nil, false
	}
	return &t.hdrs[s], true
}

// ScanStep advances a full-keyspace scan by about budget entries and
// returns the follow-up cursor, 0 when the walk has covered
// everything. emit receives each visible key with its type tag
// (TagString..TagStream), exactly once over the full walk except in
// the blind-overwrite window below, which can deliver a key twice.
//
// Dedup is structural, mirroring the KeyCount split: the shadow emits
// only vptr-0 entries, the cold walk emits index records, and a
// resident key keeps vptr through re-dirtying, so it qualifies for
// exactly one phase. The cold walk still probes the hot table per
// key, because the hot copy outranks the index record it shadows: a
// pending tombstone or a hot expiry hides the key, and any other live
// hot copy answers with its hot tag, since a type can change between
// the index record and a hot rewrite. A live hot copy with vptr 0
// over a cold record is the blind-overwrite window (the key was
// evicted, its ghost forgot the cold fact, then it was rewritten),
// and both phases emit it: the rewrite may land after the shadow
// pass, so suppressing here would drop a key that was present for
// the whole scan, and SCAN's contract allows a duplicate but never
// a miss.
func (t *Tiered) ScanStep(ctx context.Context, cursor uint64, budget int, emit func(key []byte, tag uint8)) (uint64, error) {
	if budget < 1 {
		budget = 1
	}
	t.ht.SetNow(t.nowMs())
	if cursor == 0 || cursor&scanHotBit != 0 {
		from := uint32(cursor &^ scanHotBit)
		next, done := t.ht.scanShadow(from, budget, emit)
		if !done {
			return scanHotBit | uint64(next), nil
		}
		budget -= int(next - from)
		if budget <= 0 {
			return 1, nil // cold walk starts on the next step
		}
		cursor = 1
	}
	ks, ok := t.st.(KeyScanner)
	if !ok {
		return 0, ErrScanUnsupported
	}
	var sniffErr error
	next, err := ks.ScanKeys(ctx, cursor-1, budget, func(rec Record) {
		if sniffErr != nil || rec.Gen != 0 || rec.Fence || t.expiredRec(rec) {
			return
		}
		if hd, hot := t.ht.peek(rec.Key); hot {
			if hd.valRef == 0 || t.ht.expired(hd) {
				return
			}
			emit(rec.Key, hd.typeTag&0x0F)
			return
		}
		tag := TagString
		if rec.Root {
			rt, _, err := sniffRoot(rec.Value)
			if err != nil {
				sniffErr = err
				return
			}
			tag = rt
		}
		emit(rec.Key, tag)
	})
	if err != nil {
		return 0, err
	}
	if sniffErr != nil {
		return 0, sniffErr
	}
	if next == 0 {
		return 0, nil
	}
	return next + 1, nil
}
