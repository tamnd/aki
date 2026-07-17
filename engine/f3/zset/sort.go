package zset

import "github.com/tamnd/aki/engine/f3/shard"

// SortElements returns a copy of every member of the sorted set at key, the
// buffer SORT materializes on one owner (spec 2064/f3/17 section 12). SORT sorts
// the members themselves and ignores their scores, so the scores forEach yields
// are dropped here. It returns nil when no zset lives at key; the dispatch SORT
// handler types the key before calling, so a wrong-type or missing key never
// reaches here. Members are copied out of the owner-local band the caller
// outlives. The handler confirmed a live zset via Has, so the registry exists.
func SortElements(cx *shard.Ctx, key []byte) [][]byte {
	if cx.ZColl == nil {
		return nil
	}
	z, _ := cx.ZColl.(*reg).lookup(cx, key)
	if z == nil {
		return nil
	}
	out := make([][]byte, 0, z.card())
	z.forEach(func(m []byte, _ float64) bool {
		out = append(out, append([]byte(nil), m...))
		return true
	})
	return out
}
