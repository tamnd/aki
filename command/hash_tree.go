package command

import (
	"math/rand/v2"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// A large hash is stored element-per-row in the key's btree-backed collection
// sub-tree (keyspace.CollUpdate / CollRead): one row per field,
//
//	field -> uvarint(ttlMs) + value
//
// ttlMs is the field's absolute expiry in Unix ms, 0 when the field never
// expires. A small hash keeps the single-blob form in hash_codec.go. A hash
// promotes to the sub-tree exactly when its reported encoding becomes hashtable,
// so OBJECT ENCODING flips at the same threshold as Redis and never demotes.
//
// This file holds the form-agnostic helpers the hash commands route through: a
// header probe, a point-field read, a full materialize, and a length. Point
// writes (HSET/HSETNX/HDEL) branch in hash.go because they need to maintain the
// element count inside the CollUpdate callback.

// hashRowEncode packs a field's ttl and value into the sub-tree row value.
func hashRowEncode(ttl int64, value []byte) []byte {
	b := encoding.AppendUvarint(make([]byte, 0, 1+len(value)), uint64(ttl))
	return append(b, value...)
}

// hashRowDecode splits a sub-tree row value back into its ttl and value. The
// returned value aliases row; copy it if it must outlive the cursor position.
func hashRowDecode(row []byte) (int64, []byte, error) {
	t, n, err := encoding.Uvarint(row)
	if err != nil {
		return 0, nil, err
	}
	return int64(t), row[n:], nil
}

// hashRowExpired reports whether a field ttl has passed. A zero ttl never expires.
func hashRowExpired(ttl int64) bool { return ttl != 0 && ttl <= keyspace.NowMillis() }

// hashWantsTree reports whether a hash with these fields should live in the
// btree-backed form. The rule is the encoding rule: a hash is tree-backed exactly
// when it reports hashtable, so promotion happens at the listpack threshold and
// the encoding name stays correct for free.
func hashWantsTree(lim encLimits, fields []hashField, prevEnc uint8) bool {
	return hashEncoding(lim, fields, prevEnc) == keyspace.EncHashtable
}

// hashPromote moves a hash from the blob form to the btree-backed form. It writes
// every field as a sub-tree row through CollUpdate, which creates the fresh
// sub-tree, frees the old blob, and carries over the key's TTL. Callers reach it
// when an applied write pushes the field set past the hashtable threshold.
func hashPromote(db *keyspace.DB, key []byte, fields []hashField) error {
	return db.CollUpdate(key, keyspace.TypeHash, keyspace.EncHashtable, func(w *keyspace.CollWriter) error {
		for _, f := range fields {
			if _, e := w.Put(f.field, hashRowEncode(f.ttl, f.value)); e != nil {
				return e
			}
		}
		w.SetCount(uint64(len(fields)))
		return nil
	})
}

// hashHeader probes the value header at key without decoding the body, so a write
// command can route to the blob path or the sub-tree path. found is false for a
// missing key.
func hashHeader(db *keyspace.DB, key []byte) (keyspace.ValueHeader, bool, error) {
	return db.CollMetaHeader(key)
}

// hashGetField reads one field in whichever form the hash is stored, with a
// single main-tree read. keyFound and hdr let the caller emit WRONGTYPE or a key
// miss; fieldFound reports field presence. For a btree-backed hash the read is a
// point sub-tree lookup, not a full scan.
func hashGetField(db *keyspace.DB, key, field []byte) (value []byte, fieldFound bool, hdr keyspace.ValueHeader, keyFound bool, err error) {
	body, hdr, keyFound, err := db.Get(key)
	if err != nil || !keyFound {
		return nil, false, hdr, keyFound, err
	}
	if hdr.Type != keyspace.TypeHash {
		return nil, false, hdr, true, nil
	}
	if hdr.IsColl() {
		_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
			row, present, e := r.Get(field)
			if e != nil || !present {
				return e
			}
			ttl, val, de := hashRowDecode(row)
			if de != nil {
				return de
			}
			if hashRowExpired(ttl) {
				return nil
			}
			value = append([]byte(nil), val...)
			fieldFound = true
			return nil
		})
		return value, fieldFound, hdr, true, err
	}
	fields, de := hashDecode(body)
	if de != nil {
		return nil, false, hdr, true, de
	}
	fields = dropExpiredFields(fields)
	if idx := hashFind(fields, field); idx >= 0 {
		return fields[idx].value, true, hdr, true, nil
	}
	return nil, false, hdr, true, nil
}

// hashMaterialize returns every live field in insertion order (blob) or tree
// order (btree-backed). It backs HGETALL/HKEYS/HVALS and is O(N) either way,
// which those commands already are.
func hashMaterialize(db *keyspace.DB, key []byte) (fields []hashField, hdr keyspace.ValueHeader, keyFound bool, err error) {
	body, hdr, keyFound, err := db.Get(key)
	if err != nil || !keyFound {
		return nil, hdr, keyFound, err
	}
	if hdr.Type != keyspace.TypeHash {
		return nil, hdr, true, nil
	}
	if hdr.IsColl() {
		_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
			c := r.Cursor()
			if e := c.First(); e != nil {
				return e
			}
			for c.Valid() {
				ttl, val, de := hashRowDecode(c.Value())
				if de != nil {
					return de
				}
				if !hashRowExpired(ttl) {
					fields = append(fields, hashField{
						field: append([]byte(nil), c.Key()...),
						value: append([]byte(nil), val...),
						ttl:   ttl,
					})
				}
				if e := c.Next(); e != nil {
					return e
				}
			}
			return nil
		})
		return fields, hdr, true, err
	}
	fields, err = hashDecode(body)
	if err != nil {
		return nil, hdr, true, err
	}
	return dropExpiredFields(fields), hdr, true, nil
}

// hashRowExpiredAt reports whether a field ttl has passed as of now. A zero ttl
// never expires. It takes the clock explicitly so a two-pass walk (count, then
// emit) uses one instant for both passes and the live set cannot shift between
// them.
func hashRowExpiredAt(ttl, now int64) bool { return ttl != 0 && ttl <= now }

// streamHashFull writes every live field of a btree-backed hash as a full reply,
// straight from a sub-tree cursor into the connection's encoder. emitKeys and
// emitVals pick HKEYS (keys), HVALS (vals), or HGETALL (both); asMap writes a map
// header (HGETALL in RESP3) rather than a flat array. Per-field TTLs mean the
// metadata count can include expired rows, so the live length is not known up
// front: the walk runs twice under one read view, once to count the live fields
// for the reply header and once to emit them, with one captured clock so the two
// passes agree on the live set even if another connection mutates the hash during
// the reply. Retained memory across the whole reply is the cursor pages plus the
// flush buffer, never the field total, so HGETALL on a hash far larger than RAM
// neither clones the hash onto the heap nor holds the whole reply in memory.
func streamHashFull(ctx *Ctx, db *keyspace.DB, key []byte, emitKeys, emitVals, asMap bool) error {
	enc := ctx.enc()
	now := keyspace.NowMillis()
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		live := 0
		cc := r.Cursor()
		cc.UseArena()
		if e := cc.First(); e != nil {
			return e
		}
		for cc.Valid() {
			ttl, _, de := hashRowDecode(cc.Value())
			if de != nil {
				return de
			}
			if !hashRowExpiredAt(ttl, now) {
				live++
			}
			if e := cc.Next(); e != nil {
				return e
			}
		}

		if asMap {
			enc.WriteMapLen(live)
		} else {
			enc.WriteArrayLen(live)
		}

		c := r.Cursor()
		c.UseArena()
		if e := c.First(); e != nil {
			return e
		}
		for i := 0; c.Valid(); {
			ttl, val, de := hashRowDecode(c.Value())
			if de != nil {
				return de
			}
			if !hashRowExpiredAt(ttl, now) {
				if emitKeys {
					enc.WriteBulkStreaming(c.Key())
				}
				if emitVals {
					enc.WriteBulkStreaming(val)
				}
				if i++; i&streamFlushEvery == streamFlushEvery {
					if e := ctx.Conn.StreamFlush(); e != nil {
						return e
					}
				}
			}
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return err
}

// hashCollRandFields picks fields from a btree-backed hash for HRANDFIELD without
// cloning the whole hash. A positive count gives that many distinct fields, capped
// at the live field count; a negative count gives exactly its magnitude, with
// repeats allowed. The pick is a single forward reservoir pass: memory stays
// proportional to the requested count, never the hash size, so a HRANDFIELD with a
// small count on a multi-million-field hash no longer materializes the whole hash
// (the truncated-input crash and the OOM both came from routing through the blob
// getter). The pass skips per-field-expired rows, so the sample matches the live
// set the materialized path would return.
//
// Time is a single walk of the rows, O(n) cursor steps with no per-step allocation;
// a true O(count) sample would need order-statistics counts in the btree, the same
// upgrade the deep rank seeks want, and is held for that later slice. The bound that
// matters here, no whole-hash clone, holds now.
func hashCollRandFields(db *keyspace.DB, key []byte, count int64) (out []hashField, err error) {
	if count == 0 {
		return nil, nil
	}
	clone := func(c *keyspace.CollCursor, val []byte, ttl int64) hashField {
		return hashField{
			field: append([]byte(nil), c.Key()...),
			value: append([]byte(nil), val...),
			ttl:   ttl,
		}
	}
	live := 0
	_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
		c := r.Cursor()
		// The pick is a forward-only reservoir walk that clones each chosen field
		// immediately, so the arena's reuse-until-next-move contract holds and the
		// whole scan allocates a small constant instead of decoding each leaf onto
		// the heap.
		c.UseArena()
		if e := c.First(); e != nil {
			return e
		}
		if count > 0 {
			// Distinct sample: a size-k reservoir over the live fields. Each live
			// field replaces a random slot with probability k/seen, so the result is
			// a uniform sample of min(k, liveCount) distinct fields.
			k := int(count)
			for c.Valid() {
				ttl, val, de := hashRowDecode(c.Value())
				if de != nil {
					return de
				}
				if !hashRowExpired(ttl) {
					live++
					if len(out) < k {
						out = append(out, clone(c, val, ttl))
					} else if j := rand.IntN(live); j < k {
						out[j] = clone(c, val, ttl)
					}
				}
				if e := c.Next(); e != nil {
					return e
				}
			}
			return nil
		}
		// Negative count: -count independent draws with repeats. Each slot keeps its
		// own size-one reservoir over the same stream, so each ends as an independent
		// uniform field and a field can land in more than one slot.
		m := int(-count)
		buf := make([]hashField, m)
		for c.Valid() {
			ttl, val, de := hashRowDecode(c.Value())
			if de != nil {
				return de
			}
			if !hashRowExpired(ttl) {
				live++
				var f hashField
				built := false
				for s := 0; s < m; s++ {
					if rand.IntN(live) == 0 {
						if !built {
							f = clone(c, val, ttl)
							built = true
						}
						buf[s] = f
					}
				}
			}
			if e := c.Next(); e != nil {
				return e
			}
		}
		if live > 0 {
			out = buf
		}
		return nil
	})
	return out, err
}

// hashLen returns the field count in whichever form the hash is stored. For a
// btree-backed hash it reads the metadata count in O(1) rather than walking.
func hashLen(db *keyspace.DB, key []byte) (n int64, hdr keyspace.ValueHeader, keyFound bool, err error) {
	hdr, keyFound, err = hashHeader(db, key)
	if err != nil || !keyFound {
		return 0, hdr, keyFound, err
	}
	if hdr.Type != keyspace.TypeHash {
		return 0, hdr, true, nil
	}
	if hdr.IsColl() {
		_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
			n = int64(r.Count())
			return nil
		})
		return n, hdr, true, err
	}
	body, _, ok, e := db.Get(key)
	if e != nil || !ok {
		return 0, hdr, true, e
	}
	fields, e := hashDecode(body)
	if e != nil {
		return 0, hdr, true, e
	}
	return int64(len(dropExpiredFields(fields))), hdr, true, nil
}
