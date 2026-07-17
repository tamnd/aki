package set

import "github.com/tamnd/aki/engine/f3/shard"

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

// keyDeadline resolves a key's absolute unix-ms expiry the way TTL and its
// siblings all need it: (-2, _) for an absent key, (-1, _) for a live key with
// no deadline, and (0, ms) for a live key that expires at ms. A set carries no
// key-level TTL this slice, so a set key reads as live-without-a-deadline; the
// string store answers for a string value. Its reach is set plus string, the
// same as TYPE and EXISTS, and misses the other collection registries until the
// keyspace-unification slice, owed not designed.
func keyDeadline(cx *shard.Ctx, key []byte) (state int, at int64) {
	if registry(cx).m[string(key)] != nil {
		return -1, 0
	}
	at, ok := cx.St.Deadline(key, cx.NowMs)
	if !ok {
		return -2, 0
	}
	if at == 0 {
		return -1, 0
	}
	return 0, at
}

// Ttl answers TTL key: the remaining lifetime in whole seconds, -2 for a
// missing key, -1 for a key with no deadline. Seconds round to nearest, Redis's
// (ttl+500)/1000.
func Ttl(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	state, at := keyDeadline(cx, args[0])
	if state != 0 {
		r.Int(int64(state))
		return
	}
	r.Int((at - cx.NowMs + 500) / 1000)
}

// Pttl answers PTTL key: the remaining lifetime in milliseconds, with the same
// -2 and -1 sentinels as TTL.
func Pttl(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	state, at := keyDeadline(cx, args[0])
	if state != 0 {
		r.Int(int64(state))
		return
	}
	r.Int(at - cx.NowMs)
}

// Expiretime answers EXPIRETIME key: the absolute unix time in seconds at which
// the key expires, with the same -2 and -1 sentinels. Seconds floor the ms
// deadline, Redis's expire/1000.
func Expiretime(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	state, at := keyDeadline(cx, args[0])
	if state != 0 {
		r.Int(int64(state))
		return
	}
	r.Int(at / 1000)
}

// Pexpiretime answers PEXPIRETIME key: the absolute unix time in milliseconds
// at which the key expires, with the same -2 and -1 sentinels.
func Pexpiretime(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	state, at := keyDeadline(cx, args[0])
	if state != 0 {
		r.Int(int64(state))
		return
	}
	r.Int(at)
}
