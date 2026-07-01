package f1srv

import "encoding/binary"

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
	created := 0
	for i := 2; i+1 < len(argv); i += 2 {
		fk := c.fieldKey(hkey, argv[i])
		isNew, err := c.srv.store.PutKind(fk, argv[i+1], kindHashField)
		if err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		if isNew {
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
	created := 0
	for i := 2; i+1 < len(argv); i += 2 {
		fk := c.fieldKey(hkey, argv[i])
		isNew, err := c.srv.store.PutKind(fk, argv[i+1], kindHashField)
		if err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		if isNew {
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
	for _, field := range argv[2:] {
		fk := c.fieldKey(hkey, field)
		if c.srv.store.DeleteKind(fk, kindHashField) {
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
	c.writeInt(int64(c.hashCount(argv[1])))
}

func (c *connState) cmdHStrlen(argv [][]byte) {
	// HSTRLEN key field
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'hstrlen' command")
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
