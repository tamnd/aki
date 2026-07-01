package f1srv

// The raw string-value operators, spec 2064/f1_rewrite_ltm/11 section 2: STRLEN and GETRANGE
// (with its SUBSTR alias) read a stored string, APPEND and SETRANGE grow one in place. They
// all treat a missing key as the empty string and a non-string key as a WRONGTYPE error, and
// unlike SET they leave any existing TTL alone, since they modify a value rather than replace
// the object. The two writers take the key's stripe lock so the read-modify-write is atomic
// against a concurrent writer on the same key, the discipline INCR and the SET options follow.

// checkStringLength is Redis's 512 MiB proto-max-bulk-len ceiling on the size a string can
// grow to. APPEND and SETRANGE reject a write that would cross it with the same error Redis
// gives, before allocating the new value.
const maxStringLength = 512 * 1024 * 1024

// cmdStrlen implements STRLEN: the byte length of the stored string, 0 for a missing key,
// WRONGTYPE for any other type.
func (c *connState) cmdStrlen(argv [][]byte) {
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'strlen' command")
		return
	}
	key := argv[1]
	switch c.resolveType(key) {
	case keyMissing:
		c.writeInt(0)
	case keyString:
		v, _ := c.srv.store.Get(key, c.vbuf[:0])
		c.vbuf = v
		c.writeInt(int64(len(v)))
	default:
		c.writeErr(wrongType)
	}
}

// cmdGetRange implements GETRANGE and its SUBSTR alias: the substring between two indices,
// each of which may be negative to count from the end. The clamping matches Redis's
// getrangeCommand exactly: negative indices fold once, then start and end are pinned into
// range, and start past end (or an empty string) yields an empty bulk. name is the canonical
// verb for the arity error, so SUBSTR reports 'substr'.
func (c *connState) cmdGetRange(argv [][]byte, name string) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	start, err1 := atoi64(argv[2])
	end, err2 := atoi64(argv[3])
	if err1 != nil || err2 != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	key := argv[1]
	switch c.resolveType(key) {
	case keyMissing:
		c.writeBulk(nil)
		return
	case keyString:
	default:
		c.writeErr(wrongType)
		return
	}
	v, _ := c.srv.store.Get(key, c.vbuf[:0])
	c.vbuf = v
	c.writeBulk(getRangeBytes(v, start, end))
}

// getRangeBytes returns the inclusive [start, end] slice of v after Redis's index clamping:
// a negative index counts from the end, both ends are pinned to the valid range, and an
// empty string or a start past the (clamped) end returns nothing.
func getRangeBytes(v []byte, start, end int64) []byte {
	strlen := int64(len(v))
	// Redis rejects a wholly-negative range whose start is right of its end before folding,
	// so GETRANGE k -100 -200 is empty rather than a clamped single byte.
	if start < 0 && end < 0 && start > end {
		return nil
	}
	if start < 0 {
		start = strlen + start
	}
	if end < 0 {
		end = strlen + end
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= strlen {
		end = strlen - 1
	}
	if start > end || strlen == 0 {
		return nil
	}
	return v[start : end+1]
}

// cmdAppend implements APPEND: append the argument to the string, creating it from the
// argument when the key is absent, and reply with the new length. It keeps any existing TTL,
// since it modifies the value rather than replacing the key.
func (c *connState) cmdAppend(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'append' command")
		return
	}
	key, add := argv[1], argv[2]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	switch c.resolveType(key) {
	case keyMissing:
		if err := c.srv.store.Set(key, add); err != nil {
			c.writeErr("ERR " + err.Error())
			return
		}
		c.writeInt(int64(len(add)))
	case keyString:
		old, _ := c.srv.store.Get(key, c.vbuf[:0])
		if int64(len(old))+int64(len(add)) > maxStringLength {
			c.writeErr("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
			return
		}
		// old is backed by vbuf; build the concatenation in a fresh buffer so the store write
		// does not read from and write to the same bytes.
		buf := make([]byte, 0, len(old)+len(add))
		buf = append(buf, old...)
		buf = append(buf, add...)
		if err := c.srv.store.Set(key, buf); err != nil {
			c.writeErr("ERR " + err.Error())
			return
		}
		c.writeInt(int64(len(buf)))
	default:
		c.writeErr(wrongType)
	}
}

// cmdSetRange implements SETRANGE: overwrite the string starting at offset, zero-padding any
// gap between the old length and offset, creating the key when absent. An empty value is a
// no-op that reports the current length. It keeps any existing TTL.
func (c *connState) cmdSetRange(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'setrange' command")
		return
	}
	key := argv[1]
	offset, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	if offset < 0 {
		c.writeErr("ERR offset is out of range")
		return
	}
	val := argv[3]
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	kt := c.resolveType(key)
	if kt != keyMissing && kt != keyString {
		c.writeErr(wrongType)
		return
	}
	var old []byte
	if kt == keyString {
		old, _ = c.srv.store.Get(key, c.vbuf[:0])
		c.vbuf = old
	}
	// An empty value never creates or grows the key; it just reports the current length.
	if len(val) == 0 {
		c.writeInt(int64(len(old)))
		return
	}
	if offset+int64(len(val)) > maxStringLength {
		c.writeErr("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
		return
	}
	newLen := offset + int64(len(val))
	if int64(len(old)) > newLen {
		newLen = int64(len(old))
	}
	buf := make([]byte, newLen)
	copy(buf, old)
	copy(buf[offset:], val)
	if err := c.srv.store.Set(key, buf); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeInt(newLen)
}
