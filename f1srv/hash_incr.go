package f1srv

import (
	"math"
	"math/rand/v2"
	"strconv"
)

// The two remaining hash commands that are not float arithmetic: HINCRBY, which adds a
// signed integer to a field in place, and HRANDFIELD, the non-destructive random field
// read (spec 2064/f1_rewrite_ltm/05). Both ride the element-per-row model the rest of the
// hash uses: HINCRBY is one point read of the field row, an add, and one point write, and
// HRANDFIELD samples off the ordered element index with the same order-statistic seek
// SRANDMEMBER and ZRANDMEMBER use, so a random field is an O(log n) descent, never an O(n)
// count. HINCRBYFLOAT is deliberately a separate slice, since matching Redis's long-double
// formatting from Go's float64 needs its own careful reply path.

// strictInt64 parses b as a base-10 integer with exactly Redis's string2ll rules: an
// optional single leading '-', no '+', no leading zeros (except "0" itself), no surrounding
// spaces, and it rejects anything that would overflow int64. HINCRBY parses both its
// increment argument and the stored field value this way, so its not-an-integer and
// overflow boundaries match Redis byte for byte. It is deliberately stricter than atoi64,
// which the lenient INCR family already uses on the plain string keyspace.
func strictInt64(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	if len(b) == 1 && b[0] == '0' {
		return 0, true
	}
	i := 0
	neg := false
	if b[0] == '-' {
		neg = true
		i = 1
		if i == len(b) {
			return 0, false
		}
	}
	// The first digit must be 1-9: a leading zero on a multi-digit number is not canonical,
	// so Redis rejects it rather than reading it as octal or trimming it.
	if b[i] < '1' || b[i] > '9' {
		return 0, false
	}
	v := uint64(b[i] - '0')
	i++
	for ; i < len(b); i++ {
		d := b[i]
		if d < '0' || d > '9' {
			return 0, false
		}
		// v*10 + digit overflows uint64 exactly when v > (max - digit)/10.
		if v > (math.MaxUint64-uint64(d-'0'))/10 {
			return 0, false
		}
		v = v*10 + uint64(d-'0')
	}
	if neg {
		// The magnitude of the most negative int64 is 2^63; anything past it is out of range.
		// v == 2^63 converts to math.MinInt64 through the two's-complement wrap, which is the
		// value we want.
		if v > uint64(1)<<63 {
			return 0, false
		}
		return -int64(v), true
	}
	if v > math.MaxInt64 {
		return 0, false
	}
	return int64(v), true
}

// cmdHIncrBy implements HINCRBY: add a signed integer to a hash field, creating the field
// (as the increment) and the hash when either is absent, and reply with the new value. A
// field that holds a non-integer is "ERR hash value is not an integer", and a sum past the
// int64 range is "ERR increment or decrement would overflow", both checked before the
// write so a failed HINCRBY leaves the field untouched. It takes the hash's stripe lock so
// the read-add-write and the header count stay consistent against a concurrent writer, the
// same discipline HSET follows.
func (c *connState) cmdHIncrBy(argv [][]byte) {
	// HINCRBY key field increment
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'hincrby' command")
		return
	}
	incr, ok := strictInt64(argv[3])
	if !ok {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	hkey := argv[1]
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	defer mu.Unlock()
	if c.stringConflict(hkey) {
		c.writeErr(wrongType)
		return
	}
	fk := c.fieldKey(hkey, argv[2])
	// An already-expired field reads as absent, so HINCRBY starts from zero and creates it
	// fresh (with no TTL), matching Redis. A live-TTL field keeps its TTL through the increment.
	if c.hashHasFieldTTL(hkey) {
		if at, has := c.fieldTTL(fk); has && at <= c.nowMs {
			c.reapFieldLocked(hkey, fk)
			fk = c.fieldKey(hkey, argv[2])
		}
	}
	old, exists := c.srv.store.GetKind(fk, c.vbuf[:0], kindHashField)
	c.vbuf = old
	var oldVal int64
	if exists {
		v, ok := strictInt64(old)
		if !ok {
			c.writeErr("ERR hash value is not an integer")
			return
		}
		oldVal = v
	}
	sum, ok := addOverflow(oldVal, incr)
	if !ok {
		c.writeErr("ERR increment or decrement would overflow")
		return
	}
	// Format the new value into a fresh stack buffer: old is backed by vbuf, so building the
	// replacement elsewhere keeps the store write from reading and writing the same bytes.
	var nb [20]byte
	out := strconv.AppendInt(nb[:0], sum, 10)
	isNew, err := c.srv.store.PutKind(fk, out, kindHashField)
	if err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	if isNew {
		c.srv.store.CollInsert(fk, kindHashField)
		if err := c.setHashCount(hkey, c.hashCount(hkey)+1); err != nil {
			c.writeErr("ERR " + err.Error())
			return
		}
	}
	c.writeInt(sum)
}

// hashWalkAll appends every field-row key of a hash, in field order, to dst as arena-stable
// full keys (prefix included, so the caller can slice the field name and read the value row).
// It is the whole-hash walk the large-count HRANDFIELD sampler falls back to.
func (c *connState) hashWalkAll(prefix []byte, dst [][]byte) [][]byte {
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		dst = append(dst, keys...)
		if last == nil {
			break
		}
		after = last
	}
	return dst
}

// hashSampleDistinct returns count distinct field-row keys of a hash of cardinality card
// (count is assumed already clamped to at most card), as arena-stable full keys. It mirrors
// the crossover the set and zset samplers use: below half the cardinality it draws uniform
// random indices into the ordered index and dedups on the index, so each field appears at
// most once and every draw is a descent; at or above half it walks once and partial-shuffles,
// avoiding the retry storm the dedup path hits as count nears card. The caller serializes the
// hash's writers so card and the index agree for the span of the sample.
func (c *connState) hashSampleDistinct(prefix []byte, card, count int) [][]byte {
	if count >= card {
		return c.hashWalkAll(prefix, make([][]byte, 0, card))
	}
	if count*2 >= card {
		all := c.hashWalkAll(prefix, make([][]byte, 0, card))
		for i := 0; i < count; i++ {
			j := i + rand.IntN(len(all)-i)
			all[i], all[j] = all[j], all[i]
		}
		return all[:count]
	}
	seen := make(map[int]struct{}, count)
	out := make([][]byte, 0, count)
	for len(out) < count {
		idx := rand.IntN(card)
		if _, dup := seen[idx]; dup {
			continue
		}
		seen[idx] = struct{}{}
		k, ok := c.srv.store.CollSelectAt(prefix, idx)
		if !ok {
			continue
		}
		out = append(out, k)
	}
	return out
}

// emitRandField writes a drawn field name, and its value after it when withValues is set. k
// is a full field-row key; the field name is its tail past the prefix and the value is its
// row value.
func (c *connState) emitRandField(k []byte, plen int, withValues bool) {
	c.writeBulk(k[plen:])
	if withValues {
		v, _ := c.srv.store.GetKind(k, c.vbuf[:0], kindHashField)
		c.vbuf = v
		c.writeBulk(v)
	}
}

// cmdHRandField implements HRANDFIELD: the non-destructive random field read. The no-count
// form returns one field as a bulk string (nil for a missing or wrong-type key); the count
// form follows Redis's sign convention exactly, the same trap SRANDMEMBER and ZRANDMEMBER
// share: a positive count returns up to that many distinct fields (capped at the cardinality,
// no duplicates), while a negative count returns exactly abs(count) fields with replacement,
// so duplicates are possible and the result is never capped. WITHVALUES pairs each field with
// its value.
func (c *connState) cmdHRandField(argv [][]byte) {
	// HRANDFIELD key [count [WITHVALUES]]
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'hrandfield' command")
		return
	}
	hkey := argv[1]

	if len(argv) == 2 {
		// No-count form: one field as a bulk string, or nil for a missing (or wrong-type) key.
		if c.stringConflict(hkey) {
			c.writeErr(wrongType)
			return
		}
		if c.hashHasFieldTTL(hkey) {
			mu := &c.srv.incrMu[c.srv.stripe(hkey)]
			mu.Lock()
			c.reapHashExpiredLocked(hkey)
			mu.Unlock()
		}
		card := c.hashCount(hkey)
		if card == 0 {
			c.writeNil()
			return
		}
		prefix := c.hashPrefix(hkey)
		k, ok := c.srv.store.CollSelectAt(prefix, rand.IntN(int(card)))
		if !ok {
			c.writeNil()
			return
		}
		c.writeBulk(k[len(prefix):])
		return
	}

	count, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	// Redis validates the count before the option shape, so a bad count with a trailing token
	// reports the count error, and any extra token past WITHVALUES is a syntax error rather
	// than an arity error.
	withValues := false
	if len(argv) == 4 {
		if !eqFold(argv[3], "WITHVALUES") {
			c.writeErr("ERR syntax error")
			return
		}
		withValues = true
	} else if len(argv) > 4 {
		c.writeErr("ERR syntax error")
		return
	}

	// The stripe lock keeps the cardinality and the ordered index consistent across a
	// multi-pick sample, the same serialization the hash's writers take.
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	if c.stringConflict(hkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	// Reap expired fields under the lock so the sample draws only from live fields.
	c.reapHashExpiredLocked(hkey)
	card := int(c.hashCount(hkey))
	if count == 0 || card == 0 {
		mu.Unlock()
		c.writeArrayHeader(0)
		return
	}
	prefix := c.hashPrefix(hkey)
	plen := len(prefix)

	if count < 0 {
		// With replacement: exactly abs(count) fields, duplicates allowed.
		n := int(-count)
		hdr := n
		if withValues {
			hdr = n * 2
		}
		c.writeArrayHeader(hdr)
		for i := 0; i < n; i++ {
			k, ok := c.srv.store.CollSelectAt(prefix, rand.IntN(card))
			if !ok {
				c.writeNil()
				if withValues {
					c.writeNil()
				}
				continue
			}
			c.emitRandField(k, plen, withValues)
		}
		mu.Unlock()
		return
	}

	want := int(count)
	if want > card {
		want = card
	}
	keys := c.hashSampleDistinct(prefix, card, want)
	hdr := len(keys)
	if withValues {
		hdr = len(keys) * 2
	}
	c.writeArrayHeader(hdr)
	for _, k := range keys {
		c.emitRandField(k, plen, withValues)
	}
	mu.Unlock()
}
