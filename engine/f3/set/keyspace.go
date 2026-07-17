package set

import "github.com/tamnd/aki/engine/f3/shard"

// Single-key DEL spans both the set registry and the string store, taking its
// point path from the string package. The multi-key fan form of DEL stays
// string-only until the keyspace-unification slice threads the registry into
// the fan sub-handlers; a set key is invisible to a multi-key DEL for now,
// which is recorded as owed, not designed. TYPE, single-key EXISTS, and the
// read-only expiry queries have all moved to the dispatch package, where they
// span every collection type.

// Del answers single-key DEL and UNLINK: remove the key from whichever
// keyspace holds it and report 1 when something was removed. Reclamation is
// owner-local and immediate, so UNLINK shares the path.
func Del(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	g := registry(cx)
	removed := false
	if g.m[string(key)] != nil {
		g.drop(key)
		removed = true
	}
	if cx.St.Del(key, cx.NowMs) {
		removed = true
	}
	if removed {
		r.Int(1)
		return
	}
	r.Int(0)
}
