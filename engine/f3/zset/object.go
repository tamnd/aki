package zset

import "github.com/tamnd/aki/engine/f3/shard"

// Encoding reports the OBJECT ENCODING name for the zset at key on this shard,
// listpack or skiplist per its live band, and whether a zset lives there at all.
// It builds no registry when none exists, the read-only discipline every
// encoding probe keeps (the same non-creating form as Has), so a shard that ran
// no zset command answers ("", false) and leaves no residency state behind.
//
// zset is not the head of the OBJECT chain; the set handler consults this probe
// between its own set band and the string-store fallback, threading zset into
// the single OBJECT verb so it answers for every type (stream, hash, list, set,
// zset, then string).
func Encoding(cx *shard.Ctx, key []byte) (string, bool) {
	if cx.ZColl == nil {
		return "", false
	}
	if z := cx.ZColl.(*reg).m[string(key)]; z != nil {
		return z.enc.String(), true
	}
	return "", false
}
