package zset

import "github.com/tamnd/aki/engine/obs1/shard"

// The key-level TTL accessors (spec 2064/obs1 doc 08 section 8): the
// authoritative deadline lives on the root struct so it dies with the
// root, and the keyspace package reads and writes it through these. The
// shard's rootExp map is only the hint index the expiry guard consults
// first; these are the truth it validates against.

// Deadline reports key's root deadline in absolute unix ms and whether a sorted
// set exists here at all. A live root with no TTL answers (0, true).
// Owner goroutine only.
func Deadline(cx *shard.Ctx, key []byte) (int64, bool) {
	z := registry(cx).m[string(key)]
	if z == nil {
		return 0, false
	}
	return z.expireAt, true
}

// SetDeadline sets or clears key's root deadline (at 0 = PERSIST) and
// reports whether a sorted set exists to carry it. Owner goroutine only.
func SetDeadline(cx *shard.Ctx, key []byte, at int64) bool {
	z := registry(cx).m[string(key)]
	if z == nil {
		return false
	}
	z.expireAt = at
	return true
}
