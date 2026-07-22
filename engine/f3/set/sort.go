package set

import "github.com/tamnd/aki/engine/f3/shard"

// SortElements returns a copy of every member of the set at key, the element
// buffer SORT materializes on one owner (spec 2064/f3/17 section 12). It returns
// nil when no set lives at key; the dispatch SORT handler decides the key's type
// before calling, so a wrong-type or missing key never reaches here. Each member
// is copied, since the set's backing bytes are owner-local scratch the caller
// outlives. It routes through lookup so a cold or expiring set resolves the same
// way every set read does; the handler has already confirmed a live set via Has,
// so the registry exists and lookup builds nothing new.
func SortElements(cx *shard.Ctx, key []byte) [][]byte {
	if cx.Coll == nil {
		return nil
	}
	s, _ := cx.Coll.(*reg).operand(cx, key)
	if s == nil {
		return nil
	}
	out := make([][]byte, 0, s.card())
	s.each(func(m []byte) { out = append(out, append([]byte(nil), m...)) })
	return out
}
