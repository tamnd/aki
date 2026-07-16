package set

import "github.com/tamnd/aki/engine/obs1/shard"

// The keyspace commands a set participates in span both the set registry and
// the string store, so this slice takes over their single-key point path from
// the string package. The multi-key fan forms of EXISTS and DEL stay
// string-only until the keyspace-unification slice threads the registry into
// the fan sub-handlers; a set key is invisible to a multi-key EXISTS or DEL
// for now, which is recorded as owed, not designed.

// Type answers TYPE key: "set" for a set, "string" for a string-store value,
// "none" when the key is absent.
func Type(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if registry(cx).m[string(args[0])] != nil {
		r.Status("set")
		return
	}
	if cx.St.Exists(args[0], cx.NowMs) {
		r.Status("string")
		return
	}
	r.Status("none")
}

// Exists answers single-key EXISTS: 1 when the key holds a set or a string
// value, 0 otherwise.
func Exists(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if registry(cx).m[string(args[0])] != nil || cx.St.Exists(args[0], cx.NowMs) {
		r.Int(1)
		return
	}
	r.Int(0)
}

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
