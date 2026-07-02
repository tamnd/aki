package f1srv

import "encoding/binary"

// Hash field TTL on f1raw, spec 2064/f1_rewrite_ltm/05 section 5.
//
// A hash field can expire independently of the hash key and of the other fields, the one
// place in the whole data model where a sub-element carries its own deadline (Redis 7.4,
// carried by Redis 8.x and Valkey 9.1). The spec's long-term model inlines the deadline on
// the field record itself (a has-expiry flag plus an expire_at varint in the record
// framing), the same way the string model inlines the key deadline. That inline slot changes
// the fixed record header and touches the arena layout and the hot read path, so like the
// key-TTL slice it is a later optimization; this slice reaches the same semantics with a
// dedicated sibling row.
//
// A field's expiry for (hashKey, field) lives in its own kindHashFieldTTL record under the
// exact composite key the field row uses (fieldKey), an 8-byte little-endian absolute
// deadline in unix milliseconds. The store keys every record by (key, kind), so the TTL row
// and the field row never collide, and because a TTL row is never CollInsert-ed it never
// appears in the ordered element index, so HGETALL/HKEYS/HVALS/HSCAN/HRANDFIELD never
// enumerate it.
//
// Two gates keep this off the hot path. The global srv.hfe counter is the keyspace-wide
// "does any hash field have a TTL" gate: while it is zero every hash read returns after one
// atomic load. When it is non-zero a whole-hash read still consults the per-hash
// kindHashTTLMeta hint (the count of TTL-carrying fields in that one hash) before it will
// scan, so a hash that never used field TTL stays O(1) even when some other hash does. A
// point read (HGET and friends) checks only the one field it touches, so it is O(1)
// regardless.
const (
	kindHashFieldTTL byte = 0x11 // per-field expiry: 8-byte LE absolute unix-ms deadline
	kindHashTTLMeta  byte = 0x12 // per-hash count of fields that carry a TTL; absent means none
)

// fieldTTL reads the absolute unix-ms deadline for the field row at fk and reports whether
// one is set. Lock-free, the same read contract as GetKind.
func (c *connState) fieldTTL(fk []byte) (int64, bool) {
	var s [8]byte
	v, ok := c.srv.store.GetKind(fk, s[:0], kindHashFieldTTL)
	if !ok || len(v) < 8 {
		return 0, false
	}
	return int64(binary.LittleEndian.Uint64(v)), true
}

// hashTTLCount reads how many fields of hkey currently carry a TTL, from the per-hash
// kindHashTTLMeta hint row (absent means zero). It is the per-hash gate that keeps a
// TTL-free hash off the active-reap scan even when the global hfe counter is non-zero.
func (c *connState) hashTTLCount(hkey []byte) uint64 {
	var s [8]byte
	v, ok := c.srv.store.GetKind(hkey, s[:0], kindHashTTLMeta)
	if !ok || len(v) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(v)
}

// setHashTTLCount writes the per-hash TTL-field count, deleting the hint row when it reaches
// zero so a hash that no longer has any field TTL stops paying the active-reap probe. The
// caller must hold hkey's stripe lock.
func (c *connState) setHashTTLCount(hkey []byte, n uint64) {
	if n == 0 {
		c.srv.store.DeleteKind(hkey, kindHashTTLMeta)
		return
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], n)
	_, _ = c.srv.store.PutKind(hkey, b[:], kindHashTTLMeta)
}

// setFieldTTLLocked sets the field at fk to expire at atMs. When it creates a fresh TTL
// (the field had none) it bumps the global hfe counter and this hash's TTL-field count;
// updating an existing TTL leaves both alone. The caller must hold hkey's stripe lock.
func (c *connState) setFieldTTLLocked(hkey, fk []byte, atMs int64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(atMs))
	created, err := c.srv.store.PutKind(fk, b[:], kindHashFieldTTL)
	if err == nil && created {
		c.srv.hfe.Add(1)
		c.setHashTTLCount(hkey, c.hashTTLCount(hkey)+1)
	}
}

// clearFieldTTLLocked removes the field's TTL row and reports whether one was present,
// dropping the global hfe counter and this hash's TTL-field count when it removes a row. The
// caller must hold hkey's stripe lock. It is the shared "this field no longer expires" step
// behind HPERSIST, HGETEX PERSIST, an HSET overwrite, and a field reap.
func (c *connState) clearFieldTTLLocked(hkey, fk []byte) bool {
	if !c.srv.store.DeleteKind(fk, kindHashFieldTTL) {
		return false
	}
	c.srv.hfe.Add(-1)
	if n := c.hashTTLCount(hkey); n > 0 {
		c.setHashTTLCount(hkey, n-1)
	}
	return true
}

// reapFieldLocked deletes an expired (or past-dated) field: its value row, its index entry,
// its TTL row, and one from the header count, dropping the hash key entirely when it was the
// last field. The caller must hold hkey's stripe lock and must have already built fk for
// this field.
func (c *connState) reapFieldLocked(hkey, fk []byte) {
	c.srv.store.DeleteKind(fk, kindHashField)
	c.srv.store.CollRemove(fk)
	c.clearFieldTTLLocked(hkey, fk)
	if count := c.hashCount(hkey); count <= 1 {
		_ = c.setHashCount(hkey, 0)
	} else {
		_ = c.setHashCount(hkey, count-1)
	}
}

// discardFieldTTLBeforeSet is the pre-write step HSET/HMSET run per field so an overwrite
// matches Redis, where writing a field's value discards any TTL it carried. When the field's
// TTL has already fired it reaps the field so the following PutKind recreates it and it counts
// as new; when the TTL is still live it clears just the TTL and leaves the value in place for
// the overwrite. hadTTL is the caller's one-shot gate (c.hashHasFieldTTL) so a TTL-free hash
// skips the probe. The caller holds hkey's stripe lock and has already built fk.
func (c *connState) discardFieldTTLBeforeSet(hkey, fk []byte, hadTTL bool) {
	if !hadTTL {
		return
	}
	at, has := c.fieldTTL(fk)
	if !has {
		return
	}
	if at <= c.nowMs {
		c.reapFieldLocked(hkey, fk)
	} else {
		c.clearFieldTTLLocked(hkey, fk)
	}
}

// hfieldExpired reports whether (hkey, field) carries a field TTL that has already fired,
// reaping the field when it has so the calling point read treats it as absent, matching the
// lazy expiry Redis runs on every field access. Gated on the global hfe counter, so a
// keyspace with no field TTL returns after one atomic load with no key build, no probe, and
// no lock. It must be called before the caller builds its own fieldKey for the value read,
// because the locked reap path rebuilds the scratch key.
func (c *connState) hfieldExpired(hkey, field []byte) bool {
	if c.srv.hfe.Load() == 0 {
		return false
	}
	fk := c.fieldKey(hkey, field)
	at, ok := c.fieldTTL(fk)
	if !ok || at > c.nowMs {
		return false
	}
	mu := &c.srv.incrMu[c.srv.stripe(hkey)]
	mu.Lock()
	defer mu.Unlock()
	// Re-read under the lock: a concurrent HPERSIST, renew, or reap may have landed between the
	// lock-free probe and the lock.
	fk = c.fieldKey(hkey, field)
	at, ok = c.fieldTTL(fk)
	if !ok || at > c.nowMs {
		return false
	}
	c.reapFieldLocked(hkey, fk)
	return true
}

// hashHasFieldTTL reports whether hkey has at least one field carrying a TTL, so a whole-hash
// read knows whether it must run the active reap pass. It is gated on the global hfe counter
// first, so a TTL-free keyspace answers with one atomic load and a hash that never used field
// TTL answers with a single header probe.
func (c *connState) hashHasFieldTTL(hkey []byte) bool {
	if c.srv.hfe.Load() == 0 {
		return false
	}
	return c.hashTTLCount(hkey) > 0
}

// reapHashExpiredLocked deletes every already-expired field of hkey and fixes the header and
// per-hash TTL counts, so a whole-hash read (HGETALL/HKEYS/HVALS/HLEN/HSCAN/HRANDFIELD) frames
// its reply from a count that reflects only live fields, matching Redis. The caller must hold
// hkey's stripe lock. It is gated by hashHasFieldTTL, so it never scans a hash that has no
// field TTL. It collects the expired keys in one pass (copying each, since a reap mutates the
// index it walks) and reaps them after, which keeps the ordered-index walk stable.
func (c *connState) reapHashExpiredLocked(hkey []byte) {
	if !c.hashHasFieldTTL(hkey) {
		return
	}
	// A local prefix, not the shared pbuf, so a caller that borrows pbuf for its own scan after
	// this returns is unaffected.
	var pfx []byte
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(hkey)))
	pfx = append(pfx, tmp[:n]...)
	pfx = append(pfx, hkey...)

	var expired [][]byte
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(pfx, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			var s [8]byte
			v, ok := c.srv.store.GetKind(k, s[:0], kindHashFieldTTL)
			if ok && len(v) >= 8 && int64(binary.LittleEndian.Uint64(v)) <= c.nowMs {
				kc := make([]byte, len(k))
				copy(kc, k)
				expired = append(expired, kc)
			}
		}
		if last == nil {
			break
		}
		after = last
		scan = keys
	}
	for _, fk := range expired {
		c.reapFieldLocked(hkey, fk)
	}
}

// dropHashFieldTTLsLocked clears every field-TTL sibling row and the per-hash hint row of a
// hash that is being deleted whole, so DEL/UNLINK/RENAME-over and the key-level expiry reap
// release the global hfe counter for the fields they drop. Gated by hashHasFieldTTL so a
// TTL-free hash pays nothing. It must run before the field rows themselves are dropped, since
// it finds the TTL rows by scanning the still-present field prefix. The caller holds hkey's
// stripe lock.
func (c *connState) dropHashFieldTTLsLocked(hkey, prefix []byte) {
	if !c.hashHasFieldTTL(hkey) {
		return
	}
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			if c.srv.store.DeleteKind(k, kindHashFieldTTL) {
				c.srv.hfe.Add(-1)
			}
		}
		if last == nil {
			break
		}
		after = last
		scan = keys
	}
	c.srv.store.DeleteKind(hkey, kindHashTTLMeta)
}

// propagateHashFieldTTLs re-keys every field-TTL sibling row and the per-hash TTL hint of a
// hash from src to dst, so RENAME and COPY carry field expiry the way Redis does. It walks the
// src field prefix, which the caller keeps present for the span of the call (RENAME runs this
// before it moves the field rows), and for each field that carries a TTL publishes the same
// deadline under dst's composite key, bumping the global hfe counter per new row. When del is
// set (RENAME) it also drops the src TTL row and the src hint so the source releases its share
// of hfe; COPY leaves the source untouched. Gated on the global hfe counter and the src hint so
// a TTL-free hash pays nothing. The caller holds both stripe locks. It uses local buffers, not
// the shared kbuf/pbuf, so a caller's own scan cursors are unaffected.
func (c *connState) propagateHashFieldTTLs(src, dst []byte, del bool) {
	if c.srv.hfe.Load() == 0 || c.hashTTLCount(src) == 0 {
		return
	}
	var tmp [binary.MaxVarintLen64]byte
	var pfx []byte
	pn := binary.PutUvarint(tmp[:], uint64(len(src)))
	pfx = append(pfx, tmp[:pn]...)
	pfx = append(pfx, src...)
	slen := len(pfx)

	var dhdr []byte
	dn := binary.PutUvarint(tmp[:], uint64(len(dst)))
	dhdr = append(dhdr, tmp[:dn]...)
	dhdr = append(dhdr, dst...)

	var moved uint64
	var dfk []byte
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(pfx, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			var s [8]byte
			v, ok := c.srv.store.GetKind(k, s[:0], kindHashFieldTTL)
			if !ok || len(v) < 8 {
				continue
			}
			dfk = append(dfk[:0], dhdr...)
			dfk = append(dfk, k[slen:]...)
			if created, err := c.srv.store.PutKind(dfk, v, kindHashFieldTTL); err == nil && created {
				c.srv.hfe.Add(1)
			}
			if del {
				if c.srv.store.DeleteKind(k, kindHashFieldTTL) {
					c.srv.hfe.Add(-1)
				}
			}
			moved++
		}
		if last == nil {
			break
		}
		after = last
		scan = keys
	}
	if moved > 0 {
		c.setHashTTLCount(dst, moved)
	}
	if del {
		c.srv.store.DeleteKind(src, kindHashTTLMeta)
	}
}

// hfeCondition is the NX/XX/GT/LT gate a HEXPIRE-family set applies per field.
type hfeCondition int

const (
	hfeNone hfeCondition = iota
	hfeNX                // set only if the field has no current TTL
	hfeXX                // set only if the field already has a TTL
	hfeGT                // set only if the new deadline is later than the current (no TTL = infinity)
	hfeLT                // set only if the new deadline is earlier than the current (no TTL = infinity)
)

// hfeMode is which of the four setter verbs is running, so one parser and one per-field loop
// serve HEXPIRE, HPEXPIRE, HEXPIREAT, and HPEXPIREAT.
type hfeMode int

const (
	hfeRelSec hfeMode = iota // HEXPIRE: argument is a relative seconds count
	hfeRelMs                 // HPEXPIRE: relative milliseconds
	hfeAbsSec                // HEXPIREAT: absolute unix seconds
	hfeAbsMs                 // HPEXPIREAT: absolute unix milliseconds
)

// hfeFamily selects which of Redis 8.8's three FIELDS-clause error dialects a command speaks.
// The setters (HEXPIRE and friends), the read-only group (HTTL/HPERSIST/HGETDEL and the rest),
// and HGETEX each grew a slightly different parser, so a bad numfields, a missing FIELDS keyword,
// and a field-count mismatch each carry different wording per family. parseFieldsClause
// reproduces all three so the error replies match Redis byte for byte.
type hfeFamily int

const (
	hfeFamSetter hfeFamily = iota // HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT
	hfeFamReader                  // HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME/HPERSIST/HGETDEL
	hfeFamGetEx                   // HGETEX
)

// parseFieldsClause parses the shared "[cond] FIELDS numfields field [field ...]" tail that
// every field-TTL command carries after its per-command head. It returns the condition (only
// the setters accept one; the readers pass acceptCond=false) and the field list, or an error
// string in the dialect fam selects. numfields must be positive and match the number of fields
// that follow, the same check Redis and Valkey enforce, and the exact wording of every rejection
// follows the family so the replies match Redis 8.8.
func parseFieldsClause(argv [][]byte, start int, acceptCond bool, fam hfeFamily, cmdName string) (hfeCondition, [][]byte, string) {
	cond := hfeNone
	i := start
	if acceptCond && i < len(argv) {
		switch {
		case eqFold(argv[i], "NX"):
			cond, i = hfeNX, i+1
		case eqFold(argv[i], "XX"):
			cond, i = hfeXX, i+1
		case eqFold(argv[i], "GT"):
			cond, i = hfeGT, i+1
		case eqFold(argv[i], "LT"):
			cond, i = hfeLT, i+1
		}
	}
	if i >= len(argv) {
		return hfeNone, nil, "ERR wrong number of arguments for '" + cmdName + "' command"
	}
	if !eqFold(argv[i], "FIELDS") {
		// A token where FIELDS was expected: the readers name the misplaced keyword, the setters
		// and HGETEX report it as an unknown argument, matching each family's parser.
		if fam == hfeFamReader {
			return hfeNone, nil, "ERR Mandatory argument FIELDS is missing or not at the right position"
		}
		return hfeNone, nil, "ERR unknown argument: " + string(argv[i])
	}
	i++
	if i >= len(argv) {
		return hfeNone, nil, "ERR wrong number of arguments for '" + cmdName + "' command"
	}
	nf, err := atoi64(argv[i])
	if err != nil || nf <= 0 {
		switch fam {
		case hfeFamReader:
			return hfeNone, nil, "ERR Number of fields must be a positive integer"
		case hfeFamGetEx:
			return hfeNone, nil, "ERR invalid number of fields"
		default:
			return hfeNone, nil, "ERR Parameter `numFields` should be greater than 0"
		}
	}
	i++
	fields := argv[i:]
	if int64(len(fields)) == nf {
		return cond, fields, ""
	}
	// A count mismatch: the readers report it uniformly, while the setters and HGETEX split it,
	// treating too few fields as a bad arity and too many as a surplus (unknown) argument.
	if fam == hfeFamReader {
		return hfeNone, nil, "ERR The `numfields` parameter must match the number of arguments"
	}
	if int64(len(fields)) < nf {
		return hfeNone, nil, "ERR wrong number of arguments"
	}
	return hfeNone, nil, "ERR unknown argument: " + string(fields[int(nf)])
}

// cmdHExpireGeneric implements HEXPIRE, HPEXPIRE, HEXPIREAT, and HPEXPIREAT. It parses the
// shared clause, converts the per-command time argument to an absolute unix-ms deadline, then
// per field returns one integer: -2 no such field, 0 the NX/XX/GT/LT condition was not met, 1
// the TTL was set or updated, 2 the field was deleted because the deadline is already in the
// past. A deadline at or before now deletes the field outright, exactly as Redis does.
func (c *connState) cmdHExpireGeneric(argv [][]byte, mode hfeMode, cmdName string) {
	// H(P)EXPIRE(AT) key time [cond] FIELDS numfields field [field ...]
	if len(argv) < 6 {
		c.writeErr("ERR wrong number of arguments for '" + cmdName + "' command")
		return
	}
	t, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	if t < 0 {
		c.writeErr("ERR invalid expire time, must be >= 0")
		return
	}
	cond, fields, perr := parseFieldsClause(argv, 3, true, hfeFamSetter, cmdName)
	if perr != "" {
		c.writeErr(perr)
		return
	}

	var atMs int64
	var ok bool
	switch mode {
	case hfeRelSec:
		ms, o := secToMs(t)
		if !o {
			c.writeErr("ERR invalid expire time in '" + cmdName + "' command")
			return
		}
		atMs, ok = addOverflow(c.nowMs, ms)
	case hfeRelMs:
		atMs, ok = addOverflow(c.nowMs, t)
	case hfeAbsSec:
		atMs, ok = secToMs(t)
	case hfeAbsMs:
		atMs, ok = t, true
	}
	if !ok {
		c.writeErr("ERR invalid expire time in '" + cmdName + "' command")
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

	c.writeArrayHeader(len(fields))
	for _, field := range fields {
		fk := c.fieldKey(hkey, field)
		if !c.srv.store.ExistsKind(fk, kindHashField) {
			c.writeInt(-2)
			continue
		}
		// A field whose current TTL has already fired is gone as far as the client is
		// concerned; reap it and report no such field.
		if cur, has := c.fieldTTL(fk); has && cur <= c.nowMs {
			c.reapFieldLocked(hkey, fk)
			c.writeInt(-2)
			continue
		}
		if atMs <= c.nowMs {
			// A past deadline deletes the field rather than storing an already-expired TTL.
			c.reapFieldLocked(hkey, fk)
			c.writeInt(2)
			continue
		}
		if !c.hfeConditionMet(cond, fk, atMs) {
			c.writeInt(0)
			continue
		}
		c.setFieldTTLLocked(hkey, fk, atMs)
		c.writeInt(1)
	}
}

// hfeConditionMet reports whether the NX/XX/GT/LT condition allows setting the field at fk to
// the new deadline atMs. A field with no current TTL is treated as an infinite deadline, so GT
// never fires against it and LT always does, matching Redis.
func (c *connState) hfeConditionMet(cond hfeCondition, fk []byte, atMs int64) bool {
	cur, has := c.fieldTTL(fk)
	switch cond {
	case hfeNX:
		return !has
	case hfeXX:
		return has
	case hfeGT:
		return has && atMs > cur
	case hfeLT:
		return !has || atMs < cur
	default:
		return true
	}
}

// hfeReadMode selects which of the four query replies cmdHTTLGeneric emits.
type hfeReadMode int

const (
	hfeReadTTLSec hfeReadMode = iota // HTTL: remaining seconds
	hfeReadTTLMs                     // HPTTL: remaining milliseconds
	hfeReadExpSec                    // HEXPIRETIME: absolute unix seconds
	hfeReadExpMs                     // HPEXPIRETIME: absolute unix milliseconds
)

// cmdHTTLGeneric implements HTTL, HPTTL, HEXPIRETIME, and HPEXPIRETIME. Per field it returns
// -2 no such field, -1 the field exists but has no TTL, otherwise the remaining time or the
// absolute deadline in the unit the verb asks for. It reaps a field whose TTL has already
// fired so the reply is never a stale positive.
func (c *connState) cmdHTTLGeneric(argv [][]byte, mode hfeReadMode, cmdName string) {
	// H(P)(TTL|EXPIRETIME) key FIELDS numfields field [field ...]
	if len(argv) < 5 {
		c.writeErr("ERR wrong number of arguments for '" + cmdName + "' command")
		return
	}
	_, fields, perr := parseFieldsClause(argv, 2, false, hfeFamReader, cmdName)
	if perr != "" {
		c.writeErr(perr)
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
	c.writeArrayHeader(len(fields))
	for _, field := range fields {
		fk := c.fieldKey(hkey, field)
		if !c.srv.store.ExistsKind(fk, kindHashField) {
			c.writeInt(-2)
			continue
		}
		cur, has := c.fieldTTL(fk)
		if has && cur <= c.nowMs {
			c.reapFieldLocked(hkey, fk)
			c.writeInt(-2)
			continue
		}
		if !has {
			c.writeInt(-1)
			continue
		}
		switch mode {
		case hfeReadTTLSec:
			c.writeInt((cur - c.nowMs + 500) / 1000)
		case hfeReadTTLMs:
			c.writeInt(cur - c.nowMs)
		case hfeReadExpSec:
			c.writeInt(cur / 1000)
		case hfeReadExpMs:
			c.writeInt(cur)
		}
	}
}

// cmdHPersist implements HPERSIST: per field, -2 no such field, -1 the field exists but has no
// TTL, 1 the TTL was removed.
func (c *connState) cmdHPersist(argv [][]byte) {
	// HPERSIST key FIELDS numfields field [field ...]
	if len(argv) < 5 {
		c.writeErr("ERR wrong number of arguments for 'hpersist' command")
		return
	}
	_, fields, perr := parseFieldsClause(argv, 2, false, hfeFamReader, "hpersist")
	if perr != "" {
		c.writeErr(perr)
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
	c.writeArrayHeader(len(fields))
	for _, field := range fields {
		fk := c.fieldKey(hkey, field)
		if !c.srv.store.ExistsKind(fk, kindHashField) {
			c.writeInt(-2)
			continue
		}
		cur, has := c.fieldTTL(fk)
		if has && cur <= c.nowMs {
			c.reapFieldLocked(hkey, fk)
			c.writeInt(-2)
			continue
		}
		if !has {
			c.writeInt(-1)
			continue
		}
		c.clearFieldTTLLocked(hkey, fk)
		c.writeInt(1)
	}
}

// cmdHGetDel implements HGETDEL: read the named fields and delete them in one step, returning
// the values (nil for a missing field). It is a point read-and-remove, so it touches only the
// named rows.
func (c *connState) cmdHGetDel(argv [][]byte) {
	// HGETDEL key FIELDS numfields field [field ...]
	if len(argv) < 5 {
		c.writeErr("ERR wrong number of arguments for 'hgetdel' command")
		return
	}
	_, fields, perr := parseFieldsClause(argv, 2, false, hfeFamReader, "hgetdel")
	if perr != "" {
		c.writeErr(perr)
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
	// Frame the reply, then per field read the value, emit it, and delete the field. A field
	// whose TTL already fired reads as absent.
	c.writeArrayHeader(len(fields))
	deleted := 0
	for _, field := range fields {
		fk := c.fieldKey(hkey, field)
		if cur, has := c.fieldTTL(fk); has && cur <= c.nowMs {
			c.reapFieldLocked(hkey, fk)
			c.writeNil()
			continue
		}
		v, ok := c.srv.store.GetKind(fk, c.vbuf[:0], kindHashField)
		c.vbuf = v
		if !ok {
			c.writeNil()
			continue
		}
		c.writeBulk(v)
		c.srv.store.DeleteKind(fk, kindHashField)
		c.srv.store.CollRemove(fk)
		c.clearFieldTTLLocked(hkey, fk)
		deleted++
	}
	if deleted > 0 {
		count := c.hashCount(hkey)
		if uint64(deleted) >= count {
			count = 0
		} else {
			count -= uint64(deleted)
		}
		_ = c.setHashCount(hkey, count)
	}
}

// cmdHGetEx implements HGETEX: read the named fields and adjust their TTLs in one step. The
// optional leading EX|PX|EXAT|PXAT sets a new deadline on each read field, PERSIST clears it,
// and no option reads without touching the TTL (like HMGET). Values come back in field order,
// nil for a missing field.
func (c *connState) cmdHGetEx(argv [][]byte) {
	// HGETEX key [EX sec | PX ms | EXAT ts | PXAT ts | PERSIST] FIELDS numfields field [...]
	if len(argv) < 5 {
		c.writeErr("ERR wrong number of arguments for 'hgetex' command")
		return
	}
	i := 2
	setTTL := false
	persist := false
	var mode hfeMode
	var tArg int64
	// Redis parses the leading options in a loop and rejects a second TTL/PERSIST option, so
	// walk tokens until FIELDS: an EX/PX/EXAT/PXAT/PERSIST is consumed once, a repeat is the
	// "only one of" error, and any other token is an unknown argument.
	for i < len(argv) && !eqFold(argv[i], "FIELDS") {
		switch {
		case eqFold(argv[i], "EX"), eqFold(argv[i], "PX"), eqFold(argv[i], "EXAT"), eqFold(argv[i], "PXAT"):
			if setTTL || persist {
				c.writeErr("ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified")
				return
			}
			if i+1 >= len(argv) {
				c.writeErr("ERR wrong number of arguments for 'hgetex' command")
				return
			}
			t, err := atoi64(argv[i+1])
			if err != nil {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			switch {
			case eqFold(argv[i], "EX"):
				mode = hfeRelSec
			case eqFold(argv[i], "PX"):
				mode = hfeRelMs
			case eqFold(argv[i], "EXAT"):
				mode = hfeAbsSec
			case eqFold(argv[i], "PXAT"):
				mode = hfeAbsMs
			}
			tArg = t
			setTTL = true
			i += 2
		case eqFold(argv[i], "PERSIST"):
			if setTTL || persist {
				c.writeErr("ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified")
				return
			}
			persist = true
			i++
		default:
			c.writeErr("ERR unknown argument: " + string(argv[i]))
			return
		}
	}
	if setTTL && tArg < 0 {
		c.writeErr("ERR invalid expire time, must be >= 0")
		return
	}

	var atMs int64
	if setTTL {
		var ok bool
		switch mode {
		case hfeRelSec:
			ms, o := secToMs(tArg)
			if !o {
				c.writeErr("ERR invalid expire time in 'hgetex' command")
				return
			}
			atMs, ok = addOverflow(c.nowMs, ms)
		case hfeRelMs:
			atMs, ok = addOverflow(c.nowMs, tArg)
		case hfeAbsSec:
			atMs, ok = secToMs(tArg)
		case hfeAbsMs:
			atMs, ok = tArg, true
		}
		if !ok {
			c.writeErr("ERR invalid expire time in 'hgetex' command")
			return
		}
	}

	_, fields, perr := parseFieldsClause(argv, i, false, hfeFamGetEx, "hgetex")
	if perr != "" {
		c.writeErr(perr)
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
	c.writeArrayHeader(len(fields))
	for _, field := range fields {
		fk := c.fieldKey(hkey, field)
		if cur, has := c.fieldTTL(fk); has && cur <= c.nowMs {
			c.reapFieldLocked(hkey, fk)
			c.writeNil()
			continue
		}
		v, ok := c.srv.store.GetKind(fk, c.vbuf[:0], kindHashField)
		c.vbuf = v
		if !ok {
			c.writeNil()
			continue
		}
		c.writeBulk(v)
		switch {
		case persist:
			c.clearFieldTTLLocked(hkey, fk)
		case setTTL:
			if atMs <= c.nowMs {
				c.reapFieldLocked(hkey, fk)
			} else {
				c.setFieldTTLLocked(hkey, fk, atMs)
			}
		}
	}
}

// The four setter verbs share cmdHExpireGeneric; these thin wrappers name the mode.
func (c *connState) cmdHExpire(argv [][]byte)    { c.cmdHExpireGeneric(argv, hfeRelSec, "hexpire") }
func (c *connState) cmdHPExpire(argv [][]byte)   { c.cmdHExpireGeneric(argv, hfeRelMs, "hpexpire") }
func (c *connState) cmdHExpireAt(argv [][]byte)  { c.cmdHExpireGeneric(argv, hfeAbsSec, "hexpireat") }
func (c *connState) cmdHPExpireAt(argv [][]byte) { c.cmdHExpireGeneric(argv, hfeAbsMs, "hpexpireat") }

// The four reader verbs share cmdHTTLGeneric.
func (c *connState) cmdHTTL(argv [][]byte)         { c.cmdHTTLGeneric(argv, hfeReadTTLSec, "httl") }
func (c *connState) cmdHPTTL(argv [][]byte)        { c.cmdHTTLGeneric(argv, hfeReadTTLMs, "hpttl") }
func (c *connState) cmdHExpireTime(argv [][]byte)  { c.cmdHTTLGeneric(argv, hfeReadExpSec, "hexpiretime") }
func (c *connState) cmdHPExpireTime(argv [][]byte) { c.cmdHTTLGeneric(argv, hfeReadExpMs, "hpexpiretime") }
