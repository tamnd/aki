package keyspace

import "github.com/tamnd/aki/v2/store"

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

// ensureHL returns the database's hybrid-log store, building it on first use. The
// store is created lazily so an idle database in a 16-database keyspace does not
// allocate 256 resident tail pages up front. hlOnce makes the build race-free;
// the atomic pointer lets readers load it without a lock.
func (db *DB) ensureHL() (*store.Store, error) {
	if s := db.hl.Load(); s != nil {
		return s, nil
	}
	var buildErr error
	db.hlOnce.Do(func() {
		s, err := store.New(*db.hlTun)
		if err != nil {
			buildErr = err
			return
		}
		db.hl.Store(s)
	})
	if buildErr != nil {
		return nil, buildErr
	}
	return db.hl.Load(), nil
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
	return s.Set(key, cell)
}

// hlGet reads a key's header and body back off the hybrid log. An expired key is
// reported absent and lazily deleted, matching the B-tree read path's lazy expiry.
// The body is the cell past the header; the store returns a slice the caller reads
// before issuing the next command, the same lifetime contract the B-tree cell had.
func (db *DB) hlGet(key []byte) (body []byte, hdr ValueHeader, found bool, err error) {
	s := db.hl.Load()
	if s == nil {
		// No write has happened yet, so the store is not built and the key cannot
		// exist. Avoid building it on a pure-read workload.
		return nil, ValueHeader{}, false, nil
	}
	cell, ok, err := s.Get(key)
	if err != nil || !ok {
		return nil, ValueHeader{}, false, err
	}
	h, _, ok := parseHeader(cell)
	if !ok {
		return nil, ValueHeader{}, false, nil
	}
	if db.expired(h) {
		s.Delete(key)
		return nil, ValueHeader{}, false, nil
	}
	return cell[HeaderSize:], h, true, nil
}

// hlDelete drops a key from the hybrid log, reporting whether it was present.
func (db *DB) hlDelete(key []byte) (bool, error) {
	s := db.hl.Load()
	if s == nil {
		return false, nil
	}
	return s.Delete(key)
}
