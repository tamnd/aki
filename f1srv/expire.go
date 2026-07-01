package f1srv

import "encoding/binary"

// Key TTL on f1raw, spec 2064/f1_rewrite_ltm/11 sections 1-3.
//
// A key's expiry is an absolute wall-clock deadline in unix milliseconds. The spec's
// long-term model inlines that deadline on the value record itself (a has-expiry flag
// plus an expire_at varint in the record framing), so lazy expiry is one compare on a
// field already loaded and there is no separate expiry dict to keep in sync. That
// inline model changes the fixed 16-byte f1raw record header, which touches the arena
// layout, the seqlock, and the GET hot path that the in-memory 2x rides on, so it is a
// later optimization. This slice reaches the same semantics with a dedicated sibling
// row: the expiry for key K lives in its own kindExpire record under the same key K,
// an 8-byte little-endian absolute-ms value, written and read with the same PutKind /
// GetKind / DeleteKind primitives every collection already uses.
//
// The sibling row keeps the record header untouched and the point path unchanged, and
// it reaps through the existing type-agnostic dropKeyLocked cascade, so an expired
// string, hash, set, zset, list, or stream is cleaned up by the same code DEL uses. It
// costs one extra record per key that carries a TTL, which shows up in DBSIZE exactly
// the way every element row already does (f1srv counts physical records, the
// element-per-row model's existing behavior), and the volatile counter below keeps the
// whole thing off the hot path when no key has a TTL.
const kindExpire byte = 0x10

// getExpiry reads key's absolute expiry in unix milliseconds and reports whether the
// key carries one. It is lock-free, the same read contract as GetKind, so the lazy
// check and the TTL readers call it without serializing against unrelated keys.
func (c *connState) getExpiry(key []byte) (int64, bool) {
	var scratch [8]byte
	v, ok := c.srv.store.GetKind(key, scratch[:0], kindExpire)
	if !ok || len(v) < 8 {
		return 0, false
	}
	return int64(binary.LittleEndian.Uint64(v)), true
}

// setExpiryLocked writes key's absolute expiry, bumping the volatile counter only when
// it creates a fresh expire row (a key that had no TTL now has one). Updating an
// existing expire row leaves the count alone. The caller must hold key's stripe lock so
// the created flag and the counter stay exact against a concurrent EXPIRE on the same
// key.
func (c *connState) setExpiryLocked(key []byte, atMs int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(atMs))
	created, err := c.srv.store.PutKind(key, buf[:], kindExpire)
	if err == nil && created {
		c.srv.volatile.Add(1)
	}
}

// clearExpiryLocked removes key's expire row and reports whether one was present,
// dropping the volatile counter when it removes a row. The caller must hold key's
// stripe lock. It is the shared "drop the TTL" step behind PERSIST, SET (without
// KEEPTTL), and the reap of an expired key.
func (c *connState) clearExpiryLocked(key []byte) bool {
	if c.srv.store.DeleteKind(key, kindExpire) {
		c.srv.volatile.Add(-1)
		return true
	}
	return false
}

// clearExpiry is clearExpiryLocked wrapped in key's stripe lock, for a caller (SET)
// that does not otherwise hold it. It is gated by the volatile counter at the call site,
// so a TTL-free keyspace never takes the lock.
func (c *connState) clearExpiry(key []byte) bool {
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	removed := c.clearExpiryLocked(key)
	mu.Unlock()
	return removed
}

// reapExpiredKeys drops an expired key before its command's handler runs, so a typed read
// such as HGET or ZSCORE sees an expired key as absent, matching Redis, where every key
// lookup runs expireIfNeeded. It is gated on the volatile TTL counter, so a keyspace with
// no TTL at all pays a single atomic load here and the per-command key resolution below
// never runs, which is what keeps lazy expiry off the hot path.
//
// The primary key is argv[1] for the overwhelming majority of commands: every single-key
// typed command, and the first key of the multi-key commands. The exceptions are the verbs
// that name no key and the three subcommand-first commands (OBJECT, XINFO, XGROUP), whose
// real key is argv[2]. Reaping the additional keys of a multi-key command (a second SINTER
// source, a ZUNIONSTORE source, a second XREAD stream) is a later slice; this one closes
// the single-key typed-command gap, which is the common case and the one the point-path
// handlers (hash.go, set.go, zset.go, list.go, stream.go) miss because they read their
// element rows directly without a type probe.
func (c *connState) reapExpiredKeys(cmd []byte, argv [][]byte) {
	if c.srv.volatile.Load() == 0 || len(argv) < 2 {
		return
	}
	switch {
	case eqFold(cmd, "OBJECT") || eqFold(cmd, "XINFO") || eqFold(cmd, "XGROUP"):
		if len(argv) >= 3 {
			c.expireIfNeeded(argv[2])
		}
	case noKeyCommand(cmd):
		// Connection, server, and introspection verbs touch no keyspace, so there is
		// nothing to reap and argv[1] is a message or subcommand, not a key.
	default:
		c.expireIfNeeded(argv[1])
	}
}

// noKeyCommand reports whether a command names no key in argv[1], so the reap step leaves
// its argument alone. These are the connection, server, and introspection verbs f1srv
// answers without touching the keyspace.
func noKeyCommand(cmd []byte) bool {
	switch {
	case eqFold(cmd, "PING"), eqFold(cmd, "ECHO"), eqFold(cmd, "COMMAND"),
		eqFold(cmd, "INFO"), eqFold(cmd, "QUIT"), eqFold(cmd, "SELECT"),
		eqFold(cmd, "CLIENT"), eqFold(cmd, "CONFIG"), eqFold(cmd, "RESET"),
		eqFold(cmd, "DBSIZE"), eqFold(cmd, "FLUSHALL"), eqFold(cmd, "FLUSHDB"),
		eqFold(cmd, "PFSELFTEST"):
		return true
	}
	return false
}

// expireIfNeeded is the lazy-expiry check: if key carries an expiry that is at or before
// the batch's cached now, it reaps the whole key (every row of whatever type it is) and
// reports true, so the calling command treats the key as absent. It is gated on the
// volatile counter, so when no key in the store has a TTL it returns after one atomic
// load with no probe and no lock, which is what keeps the machinery off the hot path.
//
// The gate is a fast reject, not the decision: after it passes, the deadline is read
// lock-free, and only a key that is actually past its deadline takes the stripe lock and
// re-reads under it before reaping, so a concurrent PERSIST or renew that landed between
// the gate and the lock is respected. The reap goes through dropKeyLocked, the same
// type-agnostic cascade DEL uses, and clears the expire row alongside it.
func (c *connState) expireIfNeeded(key []byte) bool {
	if c.srv.volatile.Load() == 0 {
		return false
	}
	at, ok := c.getExpiry(key)
	if !ok || at > c.nowMs {
		return false
	}
	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	defer mu.Unlock()
	// Re-read under the lock: another goroutine may have reaped, persisted, or renewed
	// this key's TTL between the lock-free probe above and acquiring the lock.
	at, ok = c.getExpiry(key)
	if !ok || at > c.nowMs {
		return false
	}
	// dropKeyLocked clears the expire row alongside the value rows, so the whole key
	// (every row of whatever type) and its TTL are gone after this call.
	c.dropKeyLocked(key)
	return true
}
