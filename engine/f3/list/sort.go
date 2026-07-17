package list

import "github.com/tamnd/aki/engine/f3/shard"

// SortElements returns a copy of every element of the list at key in list order,
// the buffer SORT materializes on one owner (spec 2064/f3/17 section 12). It
// returns nil when no list lives at key; the dispatch SORT handler types the key
// before calling, so a wrong-type or missing key never reaches here. Elements are
// copied out of the owner-local bands the caller outlives. It routes through
// lookup, the same read path every list command takes; the handler confirmed a
// live list via Has, so the registry already exists and lookup builds nothing.
func SortElements(cx *shard.Ctx, key []byte) [][]byte {
	v, ok := regs.Load(cx.St)
	if !ok {
		return nil
	}
	l, _ := v.(*reg).lookup(cx, key)
	if l == nil {
		return nil
	}
	out := make([][]byte, 0, l.length())
	l.each(func(e []byte) { out = append(out, append([]byte(nil), e...)) })
	return out
}
