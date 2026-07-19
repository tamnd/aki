package hash

import (
	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/shard"
)

// Object answers OBJECT ENCODING key for a hash (spec 2064/f3/10 section 4.4):
// the inline band reports listpack, listpackex once it has taken a field TTL, and
// the native band reports hashtable, which is what the differential test checks
// against Redis. A key this package does not own falls through to the list
// handler, which reports the list bands and then delegates to the set handler for
// the set bands, the zset band, and the string store, so the one OBJECT verb
// answers for every type in the dispatch chain (hash then list then set then zset
// then string).
// The hash probe reaches the registry through regs.Load, not registry(), so a
// read-only OBJECT for a key this shard holds under another type builds no hash
// registry. hash.Object is the chain's fallthrough for every non-stream key, so
// the creating form would have allocated a registry on nearly every OBJECT.
func Object(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if eqFold(args[0], "ENCODING") && len(args) == 2 {
		if v, ok := regs.Load(cx.St); ok {
			if h := v.(*reg).peek(cx, args[1]); h != nil {
				r.Bulk([]byte(h.encName()))
				return
			}
		}
	}
	list.Object(cx, args, r)
}

// Encoding reports the OBJECT ENCODING name for the hash at key on this shard,
// listpack, listpackex once a field has taken a TTL, or hashtable per its live
// band, and whether a hash lives there at all. It is the value-returning form the
// DEBUG OBJECT line needs, reached through regs.Load so a read-only probe builds no
// hash registry on a shard that never ran a hash command.
func Encoding(cx *shard.Ctx, key []byte) (string, bool) {
	if v, ok := regs.Load(cx.St); ok {
		if h := v.(*reg).peek(cx, key); h != nil {
			return h.encName(), true
		}
	}
	return "", false
}

// MemoryUsage reports the approximate resident bytes the hash at key charges and
// whether a hash lives there, the MEMORY USAGE contribution for a hash key. It is
// the per-collection footprint the demote loop weighs, reached through regs.Load
// so a read-only probe builds no hash registry on a shard that never ran a hash
// command.
func MemoryUsage(cx *shard.Ctx, key []byte) (uint64, bool) {
	if v, ok := regs.Load(cx.St); ok {
		if h := v.(*reg).peek(cx, key); h != nil {
			return h.residentBytes(), true
		}
	}
	return 0, false
}

// eqFold is a case-insensitive ASCII compare of b against the uppercase word s,
// the subcommand token check without allocating a lowercase copy.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		if c != s[i] {
			return false
		}
	}
	return true
}
