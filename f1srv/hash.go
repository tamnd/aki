package f1srv

import (
	"encoding/binary"
	"encoding/hex"
)

// Hash is the first collection type on f1raw, and it is element-per-row: every field
// is its own record under a composite key, and a per-hash header row carries the
// maintained field count so HLEN is O(1) without a scan. This is the structural model
// the larger-than-memory design turns on (spec 2064/f1_rewrite_ltm/05): a hash of a
// million fields is a million small records the buffer pool can page independently,
// not one blob that must be resident whole. It rides straight on the lock-free point
// store, so HGET is a single index probe, the same shape as GET.
//
// Namespaces are kept disjoint by the record kind byte (the spec's type_tag) rather
// than by mangling the string keyspace, so the string hot path is byte-for-byte
// unchanged. A field row is kindHashField under the composite key, a header row is
// kindHashMeta under the bare hash key, and a string is kindString (0) under its key;
// the same key bytes in different kinds never collide.
//
// Field sub-key layout: uvarint(len(hashKey)) | hashKey | field. The length prefix
// makes the pair (hashKey, field) injective, so ("a", "b:c") and ("a:b", "c") map to
// different rows instead of both landing on "a:b:c".
//
// Write serialization: HSET/HSETNX/HDEL take the per-key stripe lock (shared with the
// INCR family) so a hash's field rows and its header count stay consistent under
// concurrent writers to the same hash. Reads (HGET/HMGET/HEXISTS/HLEN/HSTRLEN) are
// lock-free.
const (
	kindHashField byte = 0x01 // a single hash field row
	kindHashMeta  byte = 0x08 // the per-hash header row (coll_header)
)

const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

// fieldKey builds the composite element key for (hashKey, field) into the reused
// scratch buffer, so a hash command allocates nothing for its key.
func (c *connState) fieldKey(hkey, field []byte) []byte {
	b := c.kbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(hkey)))
	b = append(b, tmp[:n]...)
	b = append(b, hkey...)
	b = append(b, field...)
	c.kbuf = b
	return b
}

// hashCount reads a hash's maintained field count from its header row, returning 0
// when the hash has no fields (no header row).
func (c *connState) hashCount(hkey []byte) uint64 {
	var cb [8]byte
	v, ok := c.srv.store.GetKind(hkey, cb[:0], kindHashMeta)
	if !ok || len(v) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(v)
}

// setHashCount writes a hash's field count to its header row, or deletes the header
// when the count reaches zero so the hash key stops existing.
func (c *connState) setHashCount(hkey []byte, count uint64) error {
	if count == 0 {
		c.srv.store.DeleteKind(hkey, kindHashMeta)
		return nil
	}
	var ob [8]byte
	binary.LittleEndian.PutUint64(ob[:], count)
	_, err := c.srv.store.PutKind(hkey, ob[:], kindHashMeta)
	return err
}

// stringConflict reports whether a plain string already holds hkey, in which case a
// hash write must fail with WRONGTYPE. It probes the string namespace only, so it
// never sees the hash's own header or field rows.
func (c *connState) stringConflict(hkey []byte) bool {
	_, ok := c.srv.store.Get(hkey, c.vbuf[:0])
	return ok
}

func (c *connState) cmdHSet(argv [][]byte) {
	// HSET key field value [field value ...]
	if len(argv) < 4 || len(argv)%2 != 0 {
		c.writeErr("ERR wrong number of arguments for 'hset' command")
		return
	}
	hkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	if c.stringConflict(hkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	hadTTL := c.hashHasFieldTTL(hkey)
	created := 0
	for i := 2; i+1 < len(argv); i += 2 {
		fk := c.fieldKey(hkey, argv[i])
		c.discardFieldTTLBeforeSet(hkey, fk, hadTTL)
		isNew, err := c.srv.store.PutKind(fk, argv[i+1], kindHashField)
		if err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		if isNew {
			c.srv.store.CollInsert(fk, kindHashField)
			created++
		}
	}
	if created > 0 {
		if err := c.setHashCount(hkey, c.hashCount(hkey)+uint64(created)); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()
	c.writeInt(int64(created))
}

// cmdHMSet is the deprecated HMSET: HSET's write path with a +OK reply.
func (c *connState) cmdHMSet(argv [][]byte) {
	if len(argv) < 4 || len(argv)%2 != 0 {
		c.writeErr("ERR wrong number of arguments for 'hmset' command")
		return
	}
	hkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	if c.stringConflict(hkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	hadTTL := c.hashHasFieldTTL(hkey)
	created := 0
	for i := 2; i+1 < len(argv); i += 2 {
		fk := c.fieldKey(hkey, argv[i])
		c.discardFieldTTLBeforeSet(hkey, fk, hadTTL)
		isNew, err := c.srv.store.PutKind(fk, argv[i+1], kindHashField)
		if err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		if isNew {
			c.srv.store.CollInsert(fk, kindHashField)
			created++
		}
	}
	if created > 0 {
		if err := c.setHashCount(hkey, c.hashCount(hkey)+uint64(created)); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()
	c.writeSimple("OK")
}

func (c *connState) cmdHSetNX(argv [][]byte) {
	// HSETNX key field value
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'hsetnx' command")
		return
	}
	hkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	if c.stringConflict(hkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	fk := c.fieldKey(hkey, argv[2])
	// An already-expired field reads as absent, so reap it first and let the NX set proceed.
	if c.hashHasFieldTTL(hkey) {
		if at, has := c.fieldTTL(fk); has && at <= c.nowMs {
			c.reapFieldLocked(hkey, fk)
			fk = c.fieldKey(hkey, argv[2])
		}
	}
	if c.srv.store.ExistsKind(fk, kindHashField) {
		mu.Unlock()
		c.writeInt(0)
		return
	}
	if _, err := c.srv.store.PutKind(fk, argv[3], kindHashField); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	c.srv.store.CollInsert(fk, kindHashField)
	if err := c.setHashCount(hkey, c.hashCount(hkey)+1); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()
	c.writeInt(1)
}

func (c *connState) cmdHGet(argv [][]byte) {
	// HGET key field
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'hget' command")
		return
	}
	// Lazy field expiry runs before fk is built for the value read: hfieldExpired may reap the
	// field and rebuild the scratch kbuf, so a fk captured earlier would dangle.
	if c.hfieldExpired(argv[1], argv[2]) {
		c.writeNil()
		return
	}
	fk := c.fieldKey(argv[1], argv[2])
	v, ok := c.srv.store.GetKind(fk, c.vbuf, kindHashField)
	c.vbuf = v
	if !ok {
		c.writeNil()
		return
	}
	c.writeBulk(v)
}

func (c *connState) cmdHMGet(argv [][]byte) {
	// HMGET key field [field ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'hmget' command")
		return
	}
	c.writeArrayHeader(len(argv) - 2)
	for _, field := range argv[2:] {
		if c.hfieldExpired(argv[1], field) {
			c.writeNil()
			continue
		}
		fk := c.fieldKey(argv[1], field)
		v, ok := c.srv.store.GetKind(fk, c.vbuf, kindHashField)
		c.vbuf = v
		if !ok {
			c.writeNil()
			continue
		}
		c.writeBulk(v)
	}
}

func (c *connState) cmdHDel(argv [][]byte) {
	// HDEL key field [field ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'hdel' command")
		return
	}
	hkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	if c.stringConflict(hkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	deleted := 0
	// One coarse gate: when no field of this hash carries a TTL (answered by a single atomic
	// load while the keyspace has no field TTL at all) the per-field TTL-row delete below is a
	// guaranteed no-op, so skip it and save a hash probe per deleted field.
	hadTTL := c.hashHasFieldTTL(hkey)
	for _, field := range argv[2:] {
		fk := c.fieldKey(hkey, field)
		if c.srv.store.DeleteKind(fk, kindHashField) {
			c.srv.store.CollRemove(fk)
			// Drop any TTL sibling the field carried so the global hfe gate and the per-hash
			// hint stay exact when a TTL'd field is deleted outright.
			if hadTTL {
				c.clearFieldTTLLocked(hkey, fk)
			}
			deleted++
		}
	}
	if deleted > 0 {
		count := c.hashCount(hkey)
		if uint64(deleted) >= count {
			count = 0
		} else {
			count -= uint64(deleted)
		}
		if err := c.setHashCount(hkey, count); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()
	c.writeInt(int64(deleted))
}

func (c *connState) cmdHExists(argv [][]byte) {
	// HEXISTS key field
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'hexists' command")
		return
	}
	if c.hfieldExpired(argv[1], argv[2]) {
		c.writeInt(0)
		return
	}
	fk := c.fieldKey(argv[1], argv[2])
	if c.srv.store.ExistsKind(fk, kindHashField) {
		c.writeInt(1)
		return
	}
	c.writeInt(0)
}

func (c *connState) cmdHLen(argv [][]byte) {
	// HLEN key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'hlen' command")
		return
	}
	// When the hash carries field TTLs, reap the expired ones under the stripe lock first so the
	// count reflects only live fields, matching Redis. The gate skips this for a TTL-free hash.
	if c.hashHasFieldTTL(argv[1]) {
		mu := &c.srv.incrMu[c.srv.stripe(argv[1])]
		mu.Lock()
		c.reapHashExpiredLocked(argv[1])
		mu.Unlock()
	}
	c.writeInt(int64(c.hashCount(argv[1])))
}

func (c *connState) cmdHStrlen(argv [][]byte) {
	// HSTRLEN key field
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'hstrlen' command")
		return
	}
	if c.hfieldExpired(argv[1], argv[2]) {
		c.writeInt(0)
		return
	}
	fk := c.fieldKey(argv[1], argv[2])
	v, ok := c.srv.store.GetKind(fk, c.vbuf, kindHashField)
	c.vbuf = v
	if !ok {
		c.writeInt(0)
		return
	}
	c.writeInt(int64(len(v)))
}

// hashPrefix builds the bounding prefix uvarint(len(hkey)) | hkey for a hash's field
// rows into the reusable pbuf. Every field row's composite key starts with this exact
// prefix and no other hash's does, so a scan bounded by it enumerates precisely one
// hash. It uses pbuf, distinct from the fieldKey scratch kbuf, so the prefix stays
// stable across an enumeration that reads values through other scratch buffers.
func (c *connState) hashPrefix(hkey []byte) []byte {
	b := c.pbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(hkey)))
	b = append(b, tmp[:n]...)
	b = append(b, hkey...)
	c.pbuf = b
	return b
}

// hashScanBatch is the batch size one CollScan pulls from the ordered index before the
// index read lock is released, so a huge HGETALL streams in bounded windows and never
// holds the index lock across the whole collection.
const hashScanBatch = 256

// streamHash is the shared enumeration body for HGETALL, HKEYS, and HVALS. It takes the
// hash's stripe lock so the header count it frames the RESP array with cannot drift
// against the field rows it then streams, rejects a string of the same key as
// WRONGTYPE, and walks the ordered element index in bounded batches, emitting the field
// name (wantField) and the value re-resolved through the authoritative index
// (wantValue) for each row. The header count and the live field-row count stay exactly
// equal because every create pairs CollInsert with a count bump and every delete pairs
// CollRemove with a decrement, so the framed length always matches what is streamed.
func (c *connState) streamHash(hkey []byte, wantField, wantValue bool) {
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	// A whole-hash read only has to exclude concurrent writers, not other readers, so it takes
	// the shared lock and lets many HGETALL/HKEYS/HVALS of one hot key run on many cores at
	// once. The one exception is reaping expired fields, a mutation that needs the exclusive
	// lock; reaping is only possible when some hash in the keyspace carries a field TTL
	// (hfe > 0), so a TTL-free keyspace, the common case, always takes the shared path.
	reap := c.srv.hfe.Load() != 0
	if reap {
		mu.Lock()
	} else {
		mu.RLock()
	}
	if c.stringConflict(hkey) {
		if reap {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
		c.writeErr(wrongType)
		return
	}
	// Reap expired fields before framing the reply so the header count and the streamed rows
	// both exclude them. Only reached on the exclusive path, so the mutation is safe.
	if reap {
		c.reapHashExpiredLocked(hkey)
	}
	count := c.hashCount(hkey)
	perRow := 0
	if wantField {
		perRow++
	}
	if wantValue {
		perRow++
	}
	c.writeArrayHeader(int(count) * perRow)

	prefix := c.hashPrefix(hkey)
	plen := len(prefix)
	var after []byte
	// The scan key and offset batches are reused across calls off the connection, so a
	// whole-hash read allocates nothing per call. At a high HGETALL rate a fresh
	// make([][]byte) plus make([]uint64) every call was a steady stream of garbage whose
	// collection showed up as a p99 tail spike (p50 held near the mean but p99 ran an order
	// of magnitude past it); reusing the connection's buffers flattens that tail.
	scanK := c.hscanK[:0]

	if !wantValue {
		// HKEYS: field names only. The plain key scan needs no per-element value read, so
		// it stays the one-lookup-per-field path it already was.
		for {
			keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scanK[:0])
			if len(keys) == 0 {
				break
			}
			scanK = keys // retain the grown backing array across windows and calls
			for _, k := range keys {
				c.writeBulk(k[plen:])
			}
			if last == nil {
				break
			}
			after = last
		}
		c.hscanK = scanK
		if reap {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
		return
	}

	// HGETALL and HVALS carry the value alongside the key: CollScanKV returns each field's
	// record offset from the ordered walk it already does, so a field is one lookup, not the
	// walk plus a GetKind re-resolve the split path paid. The offset is authoritative because
	// PutKind refreshes the ordered index on an outgrow-republish, and the stripe lock this
	// holds keeps a concurrent writer from moving a field mid-walk, so the value is never stale.
	//
	// The value read is zero-copy: because this holds the stripe lock no writer can rewrite a
	// field record under us, so ValueAtLocked hands the value bytes straight out of the arena to
	// writeBulk, the same way the field name is already a zero-copy arena subslice. That drops
	// the per-field copy into c.vbuf that ReadValueAt paid on every one of a hundred-thousand
	// fields, so HGETALL walks value-carrying at the same cost SMEMBERS walks key-only. A cold-log
	// separated value is a pointer, not inline bytes, so ValueAtLocked reports inline=false for it
	// and we fall back to the copying ReadValueAt, which resolves the cold log.
	scanO := c.hscanO[:0]
	for {
		keys, offs, last := c.srv.store.CollScanKV(prefix, after, hashScanBatch, scanK[:0], scanO[:0])
		if len(keys) == 0 {
			break
		}
		scanK = keys // retain both grown backing arrays across windows and calls
		scanO = offs
		for i, k := range keys {
			if wantField {
				c.writeBulk(k[plen:])
			}
			if v, inline := c.srv.store.ValueAtLocked(offs[i]); inline {
				c.writeBulk(v)
			} else {
				c.vbuf = c.srv.store.ReadValueAt(offs[i], c.vbuf)
				c.writeBulk(c.vbuf)
			}
		}
		if last == nil {
			break
		}
		after = last
	}
	c.hscanK = scanK
	c.hscanO = scanO
	if reap {
		mu.Unlock()
	} else {
		mu.RUnlock()
	}
}

// cmdHScan is the LTM-safe incremental hash enumeration (spec 2064/f1_rewrite_ltm/05
// section 8.2): each call scans a bounded window of field rows and returns an opaque
// cursor to resume, so a client walks a billion-field hash without the server ever
// materializing it. The cursor encodes the position in field-name order, which is
// exactly the order the ordered element index already walks, so resuming is a
// predecessor/successor seek, not a rescan.
//
// Cursor encoding: "0" starts a fresh iteration and "0" is returned when it completes;
// any live position is the hex of the last composite key returned. A composite key
// always carries the uvarint length prefix, so it is never empty and its hex is never
// the single byte "0", which keeps a live cursor from ever colliding with the done
// sentinel. On resume the hex decodes straight back into the scan's `after` bound.
//
// Cursor stability: a field present for the whole scan and not modified is returned
// exactly once (the ordered index walks each key once and the cursor resumes strictly
// after the last one), a field added or removed mid-scan may or may not appear, and the
// scan never returns a deleted field's stale value (each surviving key re-resolves its
// value through the authoritative index). The scan is lock-free like the other hash
// reads: it takes no stripe lock, and one batch reads the ordered index under its own
// read lock, so a huge iteration never blocks writers across the whole hash.
func (c *connState) cmdHScan(argv [][]byte) {
	// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'hscan' command")
		return
	}
	hkey := argv[1]

	var after []byte
	if len(argv[2]) != 1 || argv[2][0] != '0' {
		dec, err := hex.DecodeString(string(argv[2]))
		if err != nil {
			c.writeErr("ERR invalid cursor")
			return
		}
		after = dec
	}

	count := 10
	var pattern []byte
	noValues := false
	for i := 3; i < len(argv); i++ {
		switch {
		case eqFold(argv[i], "MATCH"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			pattern = argv[i+1]
			i++
		case eqFold(argv[i], "COUNT"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, err := atoi64(argv[i+1])
			if err != nil || n <= 0 {
				c.writeErr("ERR syntax error")
				return
			}
			count = int(n)
			i++
		case eqFold(argv[i], "NOVALUES"):
			noValues = true
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	if c.stringConflict(hkey) {
		c.writeErr(wrongType)
		return
	}

	prefix := c.hashPrefix(hkey)
	// Cap the initial slice header allocation so a client's large COUNT hint cannot make
	// the server preallocate a giant slice; append grows it as the batch actually fills.
	initCap := count
	if initCap > hashScanBatch {
		initCap = hashScanBatch
	}
	scan := make([][]byte, 0, initCap)
	keys, last := c.srv.store.CollScan(prefix, after, count, scan)

	// Filter by MATCH in place: the composite keys are arena subslices stable for the
	// store's life, so collecting the survivors before the reply header costs no copy and
	// lets the array length be exact.
	plen := len(prefix)
	// When the hash carries field TTLs, an already-expired field reads as absent, so it is
	// skipped from this window rather than emitted; a later point read or whole-hash read reaps
	// it. The gate keeps a TTL-free hash on the plain filter path.
	checkTTL := c.hashHasFieldTTL(hkey)
	matched := keys[:0]
	for _, k := range keys {
		if pattern != nil && !globMatch(pattern, k[plen:]) {
			continue
		}
		if checkTTL {
			if at, has := c.fieldTTL(k); has && at <= c.nowMs {
				continue
			}
		}
		matched = append(matched, k)
	}

	// A short batch (fewer than COUNT scanned) means the prefix is exhausted, so the
	// iteration is complete and the cursor is the done sentinel; otherwise resume past
	// the last scanned key.
	var cursor []byte
	if len(keys) < count || last == nil {
		cursor = []byte{'0'}
	} else {
		cursor = []byte(hex.EncodeToString(last))
	}

	perRow := 2
	if noValues {
		perRow = 1
	}
	c.writeArrayHeader(2)
	c.writeBulk(cursor)
	c.writeArrayHeader(len(matched) * perRow)
	for _, k := range matched {
		c.writeBulk(k[plen:])
		if !noValues {
			v, _ := c.srv.store.GetKind(k, c.vbuf, kindHashField)
			c.vbuf = v
			c.writeBulk(v)
		}
	}
}

// globMatch reports whether s matches the Redis glob pattern: '*' matches any run,
// '?' any single byte, '[...]' a class with '^' negation, 'a-z' ranges, and '\'
// escaping inside, and '\' escapes a metacharacter elsewhere. It is the MATCH filter
// for HSCAN and works on the raw field-name bytes so it never allocates a string.
func globMatch(pattern, s []byte) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			for len(pattern) > 1 && pattern[1] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 1 {
				return true // a trailing star matches the rest of s
			}
			for i := 0; i <= len(s); i++ {
				if globMatch(pattern[1:], s[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
			s = s[1:]
		case '[':
			if len(s) == 0 {
				return false
			}
			p := pattern[1:]
			neg := false
			if len(p) > 0 && p[0] == '^' {
				neg = true
				p = p[1:]
			}
			match := false
			for len(p) > 0 && p[0] != ']' {
				switch {
				case p[0] == '\\' && len(p) >= 2:
					if p[1] == s[0] {
						match = true
					}
					p = p[2:]
				case len(p) >= 3 && p[1] == '-' && p[2] != ']':
					lo, hi := p[0], p[2]
					if lo > hi {
						lo, hi = hi, lo
					}
					if s[0] >= lo && s[0] <= hi {
						match = true
					}
					p = p[3:]
				default:
					if p[0] == s[0] {
						match = true
					}
					p = p[1:]
				}
			}
			if len(p) > 0 {
				p = p[1:] // consume the closing ']'
			}
			if neg {
				match = !match
			}
			if !match {
				return false
			}
			s = s[1:]
			pattern = p
			continue
		case '\\':
			if len(pattern) >= 2 {
				pattern = pattern[1:]
			}
			fallthrough
		default:
			if len(s) == 0 || pattern[0] != s[0] {
				return false
			}
			s = s[1:]
		}
		pattern = pattern[1:]
	}
	return len(s) == 0
}

func (c *connState) cmdHGetAll(argv [][]byte) {
	// HGETALL key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'hgetall' command")
		return
	}
	c.streamHash(argv[1], true, true)
}

func (c *connState) cmdHKeys(argv [][]byte) {
	// HKEYS key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'hkeys' command")
		return
	}
	c.streamHash(argv[1], true, false)
}

func (c *connState) cmdHVals(argv [][]byte) {
	// HVALS key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'hvals' command")
		return
	}
	c.streamHash(argv[1], false, true)
}
