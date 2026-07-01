package f1srv

// The string commands that set or drop a value together with its TTL in one step, spec
// 2064/f1_rewrite_ltm/11 section 2.5: SETEX and PSETEX (write a value with a TTL), SETNX
// (write only when the key is absent), and the read-and-mutate pair GETDEL and GETSET.
// They all reuse the same sibling-row expiry the EXPIRE family and SET options use, so a
// SETEX-set TTL reads back through TTL/PTTL and a GETDEL or GETSET clears the row exactly
// the way DEL and a plain SET do. Each takes the key's stripe lock so the read, the write,
// and the TTL change are one atomic step, the same discipline cmdSetOptions follows.

// cmdSetEx implements SETEX (seconds) and PSETEX (milliseconds): SET the value and a
// strictly-positive relative TTL, or reply with the command's own invalid-expire-time
// error without writing anything. It is SET key value EX/PX time with a fixed argument
// shape, so the expiry arithmetic is the shared expiryDeadline.
func (c *connState) cmdSetEx(argv [][]byte, ms bool) {
	name := "setex"
	unit := unitEXsec
	if ms {
		name = "psetex"
		unit = unitPXms
	}
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	n, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	// The time argument must be strictly positive, and folding it to an absolute deadline
	// must not overflow; either failure is the invalid-expire-time error, checked before the
	// write so a bad SETEX leaves the key untouched.
	atMs, ok := c.expiryDeadline(unit, n)
	if !ok {
		c.writeErr("ERR invalid expire time in '" + name + "' command")
		return
	}
	key, val := argv[1], argv[3]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	if err := c.srv.store.Set(key, val); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.setExpiryLocked(key, atMs)
	c.writeSimple("OK")
}

// cmdSetNX implements SETNX: write the value only when the key does not exist, replying 1
// on a write and 0 when the key is already present. It carries no TTL. The dispatch-boundary
// reap has already dropped an expired key, so resolveType under the lock sees it as absent,
// but the reap is re-checked here under the lock to close the window where a TTL lands
// between the boundary reap and acquiring the lock.
func (c *connState) cmdSetNX(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'setnx' command")
		return
	}
	key, val := argv[1], argv[2]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	if c.srv.volatile.Load() != 0 {
		if at, ok := c.getExpiry(key); ok && at <= c.nowMs {
			c.dropKeyLocked(key)
		}
	}
	if c.resolveType(key) != keyMissing {
		c.writeInt(0)
		return
	}
	if err := c.srv.store.Set(key, val); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeInt(1)
}

// cmdGetDel implements GETDEL: return the string value and delete the key (and its TTL row)
// in one atomic step, or reply nil for a missing key and WRONGTYPE for a non-string without
// deleting anything. The value store.Get returns is a copy into vbuf, so it survives the
// drop that follows.
func (c *connState) cmdGetDel(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'getdel' command")
		return
	}
	key := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	kt := c.resolveType(key)
	if kt == keyMissing {
		c.writeNil()
		return
	}
	if kt != keyString {
		c.writeErr(wrongType)
		return
	}
	v, _ := c.srv.store.Get(key, c.vbuf[:0])
	c.vbuf = v
	c.writeBulk(v)
	c.dropKeyLocked(key)
}

// cmdGetSet implements GETSET: SET the new value and return the old one, or nil when the key
// was absent and WRONGTYPE when it held a non-string. Like a plain SET it clears any existing
// TTL. It is the always-set form of SET ... GET, so it has no NX/XX guard.
func (c *connState) cmdGetSet(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'getset' command")
		return
	}
	key, val := argv[1], argv[2]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	kt := c.resolveType(key)
	if kt != keyMissing && kt != keyString {
		c.writeErr(wrongType)
		return
	}
	var oldVal []byte
	var haveOld bool
	if kt == keyString {
		oldVal, haveOld = c.srv.store.Get(key, c.vbuf[:0])
		c.vbuf = oldVal
	}
	if err := c.srv.store.Set(key, val); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	if c.srv.volatile.Load() != 0 {
		c.clearExpiryLocked(key)
	}
	c.replyOldValue(oldVal, haveOld)
}
