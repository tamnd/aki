package f1srv

import (
	"encoding/binary"
	"encoding/hex"
)

// Set is the second collection type on f1raw, and like the hash it is element-per-row:
// every member is its own record under a composite key, and a per-set header row carries
// the maintained cardinality so SCARD is O(1) without a scan. The set is the hash with
// the value stripped out (spec 2064/f1_rewrite_ltm/06 section 1.1): a member row is
// member-plus-nothing, so its value field is zero bytes and membership is a single
// index probe, the same shape as HEXISTS.
//
// Namespaces stay disjoint by the record kind byte, exactly as the hash does. A member
// row is kindSetMember under the composite key, a header row is kindSetMeta under the
// bare set key. The set meta kind is distinct from the hash meta kind so SCARD on a hash
// key and HLEN on a set key never cross-read one another's header count.
//
// Member sub-key layout: uvarint(len(setKey)) | setKey | member, the same length-prefixed
// composite the hash uses. The length prefix makes (setKey, member) injective, so two
// different sets can never share a member row and a prefix scan bounded by
// uvarint(len(setKey))|setKey enumerates precisely one set in member-byte order. That
// member order is what makes the set algebra a k-way merge (section 5), and it is a
// superset of the API's no-order promise: SMEMBERS returns a valid unspecified order that
// happens to be sorted.
//
// Write serialization: SADD/SREM take the per-key stripe lock (shared with the INCR
// family and the hash) so a set's member rows and its header count stay consistent under
// concurrent writers. Reads (SISMEMBER/SMISMEMBER/SCARD) are lock-free.
const (
	kindSetMember byte = 0x02 // a single set member row, empty value
	kindSetMeta   byte = 0x09 // the per-set header row (coll_header)
)

// memberKey builds the composite element key for (setKey, member) into the reused
// scratch buffer, so a set command allocates nothing for its key.
func (c *connState) memberKey(skey, member []byte) []byte {
	b := c.kbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	b = append(b, tmp[:n]...)
	b = append(b, skey...)
	b = append(b, member...)
	c.kbuf = b
	return b
}

// setPrefix builds the bounding prefix uvarint(len(skey)) | skey for a set's member rows
// into the reusable pbuf, distinct from the memberKey scratch kbuf so the prefix stays
// stable across an enumeration. Every member row's composite key starts with this exact
// prefix and no other set's does, so a scan bounded by it enumerates precisely one set.
func (c *connState) setPrefix(skey []byte) []byte {
	b := c.pbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	b = append(b, tmp[:n]...)
	b = append(b, skey...)
	c.pbuf = b
	return b
}

// setCard reads a set's maintained cardinality from its header row, returning 0 when the
// set has no members (no header row).
func (c *connState) setCard(skey []byte) uint64 {
	var cb [8]byte
	v, ok := c.srv.store.GetKind(skey, cb[:0], kindSetMeta)
	if !ok || len(v) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(v)
}

// setSetCard writes a set's cardinality to its header row, or deletes the header when the
// count reaches zero so the set key stops existing (empty set is no set).
func (c *connState) setSetCard(skey []byte, count uint64) error {
	if count == 0 {
		c.srv.store.DeleteKind(skey, kindSetMeta)
		return nil
	}
	var ob [8]byte
	binary.LittleEndian.PutUint64(ob[:], count)
	_, err := c.srv.store.PutKind(skey, ob[:], kindSetMeta)
	return err
}

func (c *connState) cmdSAdd(argv [][]byte) {
	// SADD key member [member ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'sadd' command")
		return
	}
	skey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	added := 0
	for _, member := range argv[2:] {
		mk := c.memberKey(skey, member)
		isNew, err := c.srv.store.PutKind(mk, nil, kindSetMember)
		if err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
		if isNew {
			c.srv.store.CollInsert(mk, kindSetMember)
			added++
		}
	}
	if added > 0 {
		if err := c.setSetCard(skey, c.setCard(skey)+uint64(added)); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()
	c.writeInt(int64(added))
}

func (c *connState) cmdSRem(argv [][]byte) {
	// SREM key member [member ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'srem' command")
		return
	}
	skey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	removed := 0
	for _, member := range argv[2:] {
		mk := c.memberKey(skey, member)
		if c.srv.store.DeleteKind(mk, kindSetMember) {
			c.srv.store.CollRemove(mk)
			removed++
		}
	}
	if removed > 0 {
		count := c.setCard(skey)
		if uint64(removed) >= count {
			count = 0
		} else {
			count -= uint64(removed)
		}
		if err := c.setSetCard(skey, count); err != nil {
			mu.Unlock()
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	mu.Unlock()
	c.writeInt(int64(removed))
}

func (c *connState) cmdSIsMember(argv [][]byte) {
	// SISMEMBER key member
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'sismember' command")
		return
	}
	mk := c.memberKey(argv[1], argv[2])
	if c.srv.store.ExistsKind(mk, kindSetMember) {
		c.writeInt(1)
		return
	}
	c.writeInt(0)
}

func (c *connState) cmdSMIsMember(argv [][]byte) {
	// SMISMEMBER key member [member ...]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'smismember' command")
		return
	}
	c.writeArrayHeader(len(argv) - 2)
	for _, member := range argv[2:] {
		mk := c.memberKey(argv[1], member)
		if c.srv.store.ExistsKind(mk, kindSetMember) {
			c.writeInt(1)
			continue
		}
		c.writeInt(0)
	}
}

func (c *connState) cmdSCard(argv [][]byte) {
	// SCARD key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'scard' command")
		return
	}
	c.writeInt(int64(c.setCard(argv[1])))
}

// streamSet is the enumeration body for SMEMBERS. It takes the set's stripe lock so the
// header count it frames the RESP array with cannot drift against the member rows it then
// streams, rejects a string of the same key as WRONGTYPE, and walks the ordered element
// index in bounded batches, emitting each member (the composite key past the prefix) in
// member-byte order. The header count and the live member-row count stay exactly equal
// because every SADD pairs CollInsert with a count bump and every SREM pairs CollRemove
// with a decrement, so the framed length always matches what is streamed.
func (c *connState) streamSet(skey []byte) {
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	count := c.setCard(skey)
	c.writeArrayHeader(int(count))

	prefix := c.setPrefix(skey)
	plen := len(prefix)
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			c.writeBulk(k[plen:])
		}
		if last == nil {
			break
		}
		after = last
	}
	mu.Unlock()
}

func (c *connState) cmdSMembers(argv [][]byte) {
	// SMEMBERS key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'smembers' command")
		return
	}
	c.streamSet(argv[1])
}

// cmdSScan is the LTM-safe incremental set enumeration (spec 2064/f1_rewrite_ltm/06
// section 8): each call scans a bounded window of member rows and returns an opaque
// cursor to resume, so a client walks a billion-member set without the server ever
// materializing it. The set has no per-member value, so SSCAN returns a flat member array
// with no NOVALUES option, unlike HSCAN.
//
// Cursor encoding mirrors HSCAN: "0" starts a fresh iteration and "0" is returned when it
// completes, and any live position is the hex of the last composite key returned. A
// composite key always carries the uvarint length prefix, so it is never empty and its
// hex is never the single byte "0", which keeps a live cursor from ever colliding with the
// done sentinel.
//
// Cursor stability: a member present for the whole scan and never removed is returned
// exactly once (the ordered index walks each key once and the cursor resumes strictly
// after the last one), and a member added or removed mid-scan may or may not appear. The
// scan is lock-free like the other set reads.
func (c *connState) cmdSScan(argv [][]byte) {
	// SSCAN key cursor [MATCH pattern] [COUNT count]
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'sscan' command")
		return
	}
	skey := argv[1]

	var after []byte
	if !(len(argv[2]) == 1 && argv[2][0] == '0') {
		dec, err := hex.DecodeString(string(argv[2]))
		if err != nil {
			c.writeErr("ERR invalid cursor")
			return
		}
		after = dec
	}

	count := 10
	var pattern []byte
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
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	if c.stringConflict(skey) {
		c.writeErr(wrongType)
		return
	}

	prefix := c.setPrefix(skey)
	initCap := count
	if initCap > hashScanBatch {
		initCap = hashScanBatch
	}
	scan := make([][]byte, 0, initCap)
	keys, last := c.srv.store.CollScan(prefix, after, count, scan)

	plen := len(prefix)
	matched := keys[:0]
	for _, k := range keys {
		if pattern != nil && !globMatch(pattern, k[plen:]) {
			continue
		}
		matched = append(matched, k)
	}

	var cursor []byte
	if len(keys) < count || last == nil {
		cursor = []byte{'0'}
	} else {
		cursor = []byte(hex.EncodeToString(last))
	}

	c.writeArrayHeader(2)
	c.writeBulk(cursor)
	c.writeArrayHeader(len(matched))
	for _, k := range matched {
		c.writeBulk(k[plen:])
	}
}
