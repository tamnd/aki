package zset

import (
	"github.com/tamnd/aki/engine/f3/expire"
	"github.com/tamnd/aki/engine/f3/shard"
)

// Key-level TTL for zset keys (spec 2064/f3/16 section 2, rollout plan
// Spec/2064/f3/milestones/M-expiry-generic-key-ttl-plan.md). The deadline lives
// inline in the zset header (zset.go expireAt); the EXPIRE-family semantics live
// once in package expire, and this file supplies only the zset keyspace's
// plumbing: read the current deadline, store a new one, drop the key. Setting a
// zset's deadline never allocates arena and never fails, so Store always succeeds.

// zsetBackend drives an EXPIRE-family command over one zset key. It resolves the
// zset once on Present through the same live funnel every zset command uses, so an
// already-expired zset is dropped and reported absent before any deadline is set.
type zsetBackend struct {
	g   *reg
	cx  *shard.Ctx
	key []byte
	z   *zset
}

func (b *zsetBackend) Present() (curAt int64, present bool) {
	b.z = b.g.live(b.cx, b.key)
	if b.z == nil {
		return 0, false
	}
	return b.z.expireAt, true
}

func (b *zsetBackend) Delete() {
	// An EXPIRE to a past instant deletes the key on the spot; log the key-delete so a
	// replay drops it instead of resurrecting the members from an earlier effect.
	logDeleteKey(b.cx, b.key)
	b.g.drop(b.key)
}

func (b *zsetBackend) Store(at int64) bool {
	b.z.expireAt = at
	logExpire(b.cx, b.key, at)
	return true
}

// Expire answers one of EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT on a zset key. verb is
// the uppercase command name; args is key, time, then an optional condition flag.
// The dispatch router sends a zset key here after confirming the type with Has.
func Expire(cx *shard.Ctx, args [][]byte, r shard.Reply, verb string) {
	expire.Apply(cx, args, r, verb, &zsetBackend{g: registry(cx), cx: cx, key: args[0]})
}

// Deadline reports key's deadline and whether it holds a live zset on this shard.
// at is 0 for a live zset with no TTL, which the TTL reader renders as -1. It is
// the zset arm of the unified TTL/PTTL/EXPIRETIME resolution; an expired zset is
// dropped by the live funnel and reported absent.
func Deadline(cx *shard.Ctx, key []byte) (at int64, ok bool) {
	if cx.ZColl == nil {
		return 0, false
	}
	g := cx.ZColl.(*reg)
	z := g.live(cx, key)
	if z == nil {
		return 0, false
	}
	return z.expireAt, true
}

// Persist removes a zset key's deadline, reporting whether one was removed: the
// zset arm of PERSIST. A live zset with no deadline, an expired zset, or an absent
// key all report false, so PERSIST answers 0 for them. It builds no registry when
// none exists.
func Persist(cx *shard.Ctx, key []byte) bool {
	if cx.ZColl == nil {
		return false
	}
	g := cx.ZColl.(*reg)
	z := g.live(cx, key)
	if z == nil || z.expireAt == 0 {
		return false
	}
	z.expireAt = 0
	// Log the cleared deadline so a replay persists the zset instead of restoring the
	// TTL a prior effect or snapshot carried.
	logExpire(cx, key, 0)
	return true
}
