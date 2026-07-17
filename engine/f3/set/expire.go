package set

import (
	"github.com/tamnd/aki/engine/f3/expire"
	"github.com/tamnd/aki/engine/f3/shard"
)

// Key-level TTL for set keys (spec 2064/f3/16 section 2, rollout plan
// Spec/2064/f3/milestones/M-expiry-generic-key-ttl-plan.md). The deadline lives
// inline in the set header (set.go expireAt); the EXPIRE-family semantics live
// once in package expire, and this file supplies only the set keyspace's
// plumbing: read the current deadline, store a new one, drop the key. Setting a
// set's deadline never allocates arena and never fails, so Store always succeeds.

// setBackend drives an EXPIRE-family command over one set key. It resolves the
// set once on Present through the same live funnel every set command uses, so an
// already-expired set is dropped and reported absent before any deadline is set.
type setBackend struct {
	g   *reg
	cx  *shard.Ctx
	key []byte
	s   *set
}

func (b *setBackend) Present() (curAt int64, present bool) {
	b.s = b.g.live(b.cx, b.key)
	if b.s == nil {
		return 0, false
	}
	return b.s.expireAt, true
}

func (b *setBackend) Delete() { b.g.drop(b.key) }

func (b *setBackend) Store(at int64) bool {
	b.s.expireAt = at
	return true
}

// Expire answers one of EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT on a set key. verb is
// the uppercase command name; args is key, time, then an optional condition flag.
// The dispatch router sends a set key here after confirming the type with Has.
func Expire(cx *shard.Ctx, args [][]byte, r shard.Reply, verb string) {
	expire.Apply(cx, args, r, verb, &setBackend{g: registry(cx), cx: cx, key: args[0]})
}

// Deadline reports key's deadline and whether it holds a live set on this shard.
// at is 0 for a live set with no TTL, which the TTL reader renders as -1. It is
// the set arm of the unified TTL/PTTL/EXPIRETIME resolution; an expired set is
// dropped by the live funnel and reported absent.
func Deadline(cx *shard.Ctx, key []byte) (at int64, ok bool) {
	if cx.Coll == nil {
		return 0, false
	}
	g := cx.Coll.(*reg)
	s := g.live(cx, key)
	if s == nil {
		return 0, false
	}
	return s.expireAt, true
}

// Persist removes a set key's deadline, reporting whether one was removed: the
// set arm of PERSIST. A live set with no deadline, an expired set, or an absent
// key all report false, so PERSIST answers 0 for them. It builds no registry when
// none exists.
func Persist(cx *shard.Ctx, key []byte) bool {
	if cx.Coll == nil {
		return false
	}
	g := cx.Coll.(*reg)
	s := g.live(cx, key)
	if s == nil || s.expireAt == 0 {
		return false
	}
	s.expireAt = 0
	return true
}
