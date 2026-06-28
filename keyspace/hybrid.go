package keyspace

import (
	"github.com/tamnd/aki/hot"
	"github.com/tamnd/aki/store"
)

// hlEngine is the four-method (plus Len/Clear) seam the keyspace string point path
// depends on. The keyspace holds the engine as this interface rather than a
// concrete type, so either substrate plugs in: the durable-spill store/ engine
// (WithHybridLog) or the clean lock-free hot/ engine (WithHotEngine). Both already
// implement every method with the exact signatures below, so the keyspace code
// above this seam is identical on either engine.
type hlEngine interface {
	Set(key, value []byte) error
	SetWithPrev(key, value []byte) (prevValLen int, err error)
	Get(key []byte) (value []byte, found bool, err error)
	DeleteWithPrev(key []byte) (prevValLen int, ok bool, err error)
	Each(fn func(key, value []byte) bool) error
	Clear() error
	Len() int
}

// Compile-time proof that both engines satisfy the seam. If a method signature
// drifts on either side, this fails to build instead of at the call site.
var (
	_ hlEngine = (*store.Store)(nil)
	_ hlEngine = (*hot.Store)(nil)
)

// hlBox wraps the engine interface so it can live in an atomic.Pointer, which
// needs a concrete element type. The keyspace swaps the box pointer atomically on
// SwapDB and lazy build, so a concurrent point read always loads a consistent
// engine either side of the swap.
type hlBox struct{ e hlEngine }

// hybrid.go is the in-place engine swap, slice S1 (spec 2064 rewrite). When a
// database is opened WithHybridLog its string point path (Set/Get/Delete) runs
// on the v2 hybrid-log store instead of the paged B-tree. The store is a resident
// open-addressed index over an append-only log whose cold pages spill to disk,
// the structure the rewrite proved clears 2x both rivals on GET and SET at
// saturation. The command and compat layer above is unchanged: it still calls
// Set/Get/Delete and reads a ValueHeader; only the substrate under those calls
// changed.
//
// The value the store holds is the same cell the B-tree leaf held: the 40-byte
// ValueHeader followed by the inline body. The store treats it as opaque bytes,
// so all the type/encoding/flags/TTL/version metadata compat depends on rides
// through untouched. The header carries FlagInlineBody because this slice keeps
// the whole value in the log record; overflow bodies and btree-backed collections
// are not reachable on this path and are later slices.
//
// This path is non-durable: the log lives in RAM and spills to a scratch file for
// capacity only, not as a recovery journal. The durability tiers (group-commit
// fsync, index checkpoint, tail replay) are slice S2.

// HybridLog reports whether this keyspace runs its string point path on the v2
// hybrid-log store (opened WithHybridLog). The command layer reads it to route
// writes through the synchronous db.Set path instead of the B-tree write-behind
// machinery, which the hybrid engine does not use.
func (ks *Keyspace) HybridLog() bool { return ks.hybrid }

// ensureHL returns the database's hybrid-log store, building it on first use. The
// store is created lazily so an idle database in a 16-database keyspace does not
// allocate 256 resident tail pages up front. hlOnce makes the build race-free;
// the atomic pointer lets readers load it without a lock.
func (db *DB) ensureHL() (hlEngine, error) {
	if b := db.hl.Load(); b != nil {
		return b.e, nil
	}
	var buildErr error
	db.hlOnce.Do(func() {
		e, err := db.newHL()
		if err != nil {
			buildErr = err
			return
		}
		db.hl.Store(&hlBox{e: e})
	})
	if buildErr != nil {
		return nil, buildErr
	}
	return db.hl.Load().e, nil
}

// hlSet stores body under key with its ValueHeader on the hybrid log. It mirrors
// the inline-body branch of the B-tree set: assign a write version, build the
// header, and write header+body as one record. A non-positive immediate TTL is
// handled the same way the B-tree path handles it, as a delete, so a SET ... PX 1
// that is already expired never leaves a live key behind.
func (db *DB) hlSet(key, body []byte, typ, enc uint8, ttlMs int64) error {
	if ttlMs >= 0 && ttlMs <= nowMillis() {
		_, err := db.hlDelete(key)
		return err
	}
	s, err := db.ensureHL()
	if err != nil {
		return err
	}
	h := ValueHeader{
		Type:     typ,
		Encoding: enc,
		Flags:    FlagInlineBody,
		TTLms:    -1,
		Version:  db.ks.version.Add(1),
		BodyLen:  uint32(len(body)),
		RefCount: 1,
	}
	if ttlMs >= 0 {
		h.Flags |= FlagHasTTL
		h.TTLms = ttlMs
	}
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
	cell = append(cell, body...)
	prevValLen, err := s.SetWithPrev(key, cell)
	if err != nil {
		return err
	}
	db.hlAccountStore(key, len(body), prevValLen)
	return nil
}

// hlEntryBytes is the used_memory cost of one hybrid-log entry: the key bytes, the
// body bytes, and the fixed per-entry overhead. It is the same formula the btree
// path uses (keyspace.go set), so used_memory reads consistently whichever engine a
// database runs on.
func hlEntryBytes(keyLen, bodyLen int) int64 {
	return int64(keyLen) + int64(bodyLen) + entryOverhead
}

// hlAccountStore folds a hybrid store into the used_memory total and the LFU
// bookkeeping the command layer reads, mirroring the btree set's accounting.
// prevValLen is the displaced record's value length from SetWithPrev, or negative
// when the key is new. The stored record is HeaderSize+bodyLen bytes, so the
// previous body length is prevValLen-HeaderSize. A new key seeds the LFU counter;
// an overwrite swaps the old entry's byte cost for the new one's and bumps the
// counter, exactly as db.set does on the btree engine.
func (db *DB) hlAccountStore(key []byte, bodyLen, prevValLen int) {
	isNew := prevValLen < 0
	if !isNew {
		db.ks.dataBytes.Add(-hlEntryBytes(len(key), prevValLen-HeaderSize))
	}
	db.ks.dataBytes.Add(hlEntryBytes(len(key), bodyLen))
	db.recordAccess(key, isNew)
}

// hlGet reads a key's header and body back off the hybrid log. An expired key is
// reported absent and lazily deleted, matching the B-tree read path's lazy expiry.
// The body is the cell past the header; the store returns a slice the caller reads
// before issuing the next command, the same lifetime contract the B-tree cell had.
func (db *DB) hlGet(key []byte) (body []byte, hdr ValueHeader, found bool, err error) {
	b := db.hl.Load()
	if b == nil {
		// No write has happened yet, so the store is not built and the key cannot
		// exist. Avoid building it on a pure-read workload.
		return nil, ValueHeader{}, false, nil
	}
	cell, ok, err := b.e.Get(key)
	if err != nil || !ok {
		return nil, ValueHeader{}, false, err
	}
	h, _, ok := parseHeader(cell)
	if !ok {
		return nil, ValueHeader{}, false, nil
	}
	if db.expired(h) {
		// Lazy expiry mirrors the btree read path. Route through hlDelete so the freed
		// bytes leave used_memory and the key's LFU bookkeeping is dropped, rather than
		// hitting the store directly and leaking the accounting.
		_, _ = db.hlDelete(key)
		return nil, ValueHeader{}, false, nil
	}
	return cell[HeaderSize:], h, true, nil
}

// hlForEachLive is the hybrid-engine implementation of forEachLive: it walks the
// store's enumerate surface and calls fn for every unexpired key, handing fn a
// composite key built from the raw key so the callers (Keys, Scan) see the same
// shape they get from the B-tree cursor and rawKey/copyRaw keep working unchanged.
// An expired key is skipped but not deleted here, matching the B-tree forEachLive,
// which leaves lazy expiry to the read path rather than mutating mid-iteration.
func (db *DB) hlForEachLive(fn func(ck []byte, h ValueHeader) error) error {
	b := db.hl.Load()
	if b == nil {
		return nil
	}
	var ck []byte
	var fnErr error
	walkErr := b.e.Each(func(key, cell []byte) bool {
		h, _, ok := parseHeader(cell)
		if !ok || db.expired(h) {
			return true
		}
		ck = appendCompositeKey(ck, key)
		if err := fn(ck, h); err != nil {
			fnErr = err
			return false
		}
		return true
	})
	if fnErr != nil {
		return fnErr
	}
	return walkErr
}

// hlDelete drops a key from the hybrid log, reporting whether it was present. It
// subtracts the removed entry's byte cost from used_memory and forgets its LFU
// bookkeeping, mirroring the btree delete path, using the removed record's value
// length that DeleteWithPrev surfaces at no extra probe.
func (db *DB) hlDelete(key []byte) (bool, error) {
	b := db.hl.Load()
	if b == nil {
		return false, nil
	}
	prevValLen, ok, err := b.e.DeleteWithPrev(key)
	if err != nil || !ok {
		return ok, err
	}
	db.ks.dataBytes.Add(-hlEntryBytes(len(key), prevValLen-HeaderSize))
	db.dropAccess(key)
	return true, nil
}
