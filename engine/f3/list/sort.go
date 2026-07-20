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

// SortStore replaces key with a fresh list holding elems in order, the
// destination write of SORT ... STORE (spec 2064/f3/17 section 12). The caller
// has already cleared any prior value at key of every type (restoreClear) and
// guarantees elems is non-empty, so this only builds: a new list, each element
// appended to the back in order and logged for durability the way RPUSH logs a
// created key, then the footprint noted into the resident total. It returns the
// stored length. An empty result never reaches here; the SORT handler deletes the
// destination instead, matching Redis, which keeps no empty list around.
func SortStore(cx *shard.Ctx, key []byte, elems [][]byte) int {
	g := registry(cx)
	l := newList()
	g.install(cx, key, l)
	for _, v := range elems {
		l.pushBack(v)
		logPush(cx, key, v, false)
	}
	g.note(l)
	return l.length()
}
