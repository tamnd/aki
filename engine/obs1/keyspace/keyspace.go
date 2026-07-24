// Package keyspace owns the commands that span every keyspace a shard
// carries: the string store and the five collection registries. It holds
// the key-level TTL surface (the EXPIRE family, spec 2064/obs1 doc 08
// section 8), the lazy-expiry guard the dispatch table runs ahead of
// every keyed point command, and the TYPE, EXISTS, and DEL point paths
// that must see every type. The keyspaces themselves stay separate (the
// sanctioned f3 mirror); this package is the one place that probes them
// all, so the multi-key fan forms of EXISTS and DEL, which run in the
// string package's shard sub-handlers, still miss the collection halves
// and the guard, an owed gap that parks with keyspace unification.
//
// A collection's authoritative deadline lives on its root struct, read
// and written through each type package's Deadline and SetDeadline; the
// shard's rootExp map is only the hint index the guard consults first.
// Strings keep their deadline inline on the record, where the store
// already reaps it lazily on every touch.
package keyspace

import (
	"github.com/tamnd/aki/engine/obs1/hash"
	"github.com/tamnd/aki/engine/obs1/list"
	"github.com/tamnd/aki/engine/obs1/set"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/stream"
	"github.com/tamnd/aki/engine/obs1/zset"
)

// collDeadline probes the five collection registries for key and reports
// its root deadline and which type holds it, "" when none does. Probe
// order is fixed but arbitrary; a key never lives in two registries.
func collDeadline(cx *shard.Ctx, key []byte) (int64, string) {
	if at, ok := set.Deadline(cx, key); ok {
		return at, "set"
	}
	if at, ok := hash.Deadline(cx, key); ok {
		return at, "hash"
	}
	if at, ok := zset.Deadline(cx, key); ok {
		return at, "zset"
	}
	if at, ok := list.Deadline(cx, key); ok {
		return at, "list"
	}
	if at, ok := stream.Deadline(cx, key); ok {
		return at, "stream"
	}
	return 0, ""
}

// setCollDeadline lands at on whichever registry holds key, reporting
// whether one did, and keeps the shard hint index in step.
func setCollDeadline(cx *shard.Ctx, key []byte, at int64) bool {
	if set.SetDeadline(cx, key, at) || hash.SetDeadline(cx, key, at) ||
		zset.SetDeadline(cx, key, at) || list.SetDeadline(cx, key, at) ||
		stream.SetDeadline(cx, key, at) {
		cx.SetRootDeadline(key, at)
		return true
	}
	return false
}

// dropColl removes key's collection root from whichever registry holds
// it, reporting whether one did. The type packages' replay probes are
// exactly the registry drop this needs, so serve time shares them.
func dropColl(cx *shard.Ctx, key []byte) bool {
	return set.ReplayDrop(cx, key) || hash.ReplayDrop(cx, key) ||
		zset.ReplayDrop(cx, key) || list.ReplayDrop(cx, key) ||
		stream.ReplayDrop(cx, key)
}

// Reap is the lazy-expiry guard: dispatch runs it on a keyed command's
// routing key before the handler. The hint map answers the common case
// (no deadline, or one still ahead) in one lookup; a fired hint is
// validated against the root's own deadline before anything is deleted,
// because a hint can outlive its root across a drop and recreate. A
// confirmed fire deletes the key from both keyspaces, the same shape DEL
// leaves, and frames one keydel so replay converges. Strings are not
// this guard's problem: the store reaps a fired string on every touch.
func Reap(cx *shard.Ctx, key []byte) error {
	hint := cx.RootDeadline(key)
	if hint == 0 || hint > cx.NowMs {
		return nil
	}
	at, kind := collDeadline(cx, key)
	if kind == "" || at == 0 {
		// The root is gone or was recreated without a TTL; the hint is
		// stale, not the key.
		cx.DropRootDeadline(key)
		return nil
	}
	if at > cx.NowMs {
		// A recreate or extension moved the real deadline past the stale
		// hint; refresh it.
		cx.SetRootDeadline(key, at)
		return nil
	}
	dropColl(cx, key)
	cx.St.Del(key, cx.NowMs)
	cx.DropRootDeadline(key)
	return cx.LogKeyDel(key)
}

// Type answers TYPE key across every keyspace: the collection type when
// a registry holds the key, "string" for a string-store value, "none"
// when absent.
func Type(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if _, kind := collDeadline(cx, args[0]); kind != "" {
		r.Status(kind)
		return
	}
	if cx.St.Exists(args[0], cx.NowMs) {
		r.Status("string")
		return
	}
	r.Status("none")
}

// Exists answers single-key EXISTS: 1 when any keyspace holds the key.
func Exists(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if _, kind := collDeadline(cx, args[0]); kind != "" || cx.St.Exists(args[0], cx.NowMs) {
		r.Int(1)
		return
	}
	r.Int(0)
}

// Del answers single-key DEL and UNLINK: remove the key from every
// keyspace that holds it and report 1 when something was removed.
// Reclamation is owner-local and immediate, so UNLINK shares the path.
func Del(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	removed := dropColl(cx, key)
	if cx.St.Del(key, cx.NowMs) {
		removed = true
	}
	if !removed {
		r.Int(0)
		return
	}
	cx.DropRootDeadline(key)
	if err := cx.LogKeyDel(key); err != nil {
		r.Err(err.Error())
		return
	}
	r.Int(1)
}
