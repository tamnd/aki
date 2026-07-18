package hash

import (
	"github.com/tamnd/aki/engine/f3/expire"
	"github.com/tamnd/aki/engine/f3/shard"
)

// Key-level TTL for hash keys (spec 2064/f3/16 section 2, rollout plan
// Spec/2064/f3/milestones/M-expiry-generic-key-ttl-plan.md). This is the whole
// key's EXPIRE deadline, an inline expireAt in the hash header (hash.go), and is
// distinct from the per-field HEXPIRE TTLs in expire.go: the key-level deadline
// drops the whole hash, fields and all, and is checked first by the live funnel.
// The EXPIRE-family semantics live once in package expire; this file supplies only
// the hash keyspace's plumbing: read the current deadline, store a new one, drop
// the key. Setting a hash's key deadline never allocates and never fails, so Store
// always succeeds.

// hashBackend drives an EXPIRE-family command over one hash key. It resolves the
// hash once on Present through the same live funnel every hash command uses, so an
// already-expired hash (by key deadline or by field reap) is dropped and reported
// absent before any deadline is set.
type hashBackend struct {
	g   *reg
	cx  *shard.Ctx
	key []byte
	h   *hash
}

func (b *hashBackend) Present() (curAt int64, present bool) {
	b.h = b.g.live(b.cx, b.key)
	if b.h == nil {
		return 0, false
	}
	return b.h.expireAt, true
}

func (b *hashBackend) Delete() {
	// An EXPIRE to a past instant deletes the key on the spot; log the key-delete so a
	// replay drops it instead of resurrecting the fields from an earlier effect.
	logDeleteKey(b.cx, b.key)
	b.g.drop(b.key)
}

func (b *hashBackend) Store(at int64) bool {
	b.h.expireAt = at
	logExpire(b.cx, b.key, at)
	return true
}

// Expire answers one of EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT on a hash key. verb is
// the uppercase command name; args is key, time, then an optional condition flag.
// The dispatch router sends a hash key here after confirming the type with Has.
func Expire(cx *shard.Ctx, args [][]byte, r shard.Reply, verb string) {
	expire.Apply(cx, args, r, verb, &hashBackend{g: registry(cx), cx: cx, key: args[0]})
}

// Deadline reports key's key-level deadline and whether it holds a live hash on
// this shard. at is 0 for a live hash with no key TTL, which the TTL reader renders
// as -1. It is the hash arm of the unified TTL/PTTL/EXPIRETIME resolution; an
// expired hash is dropped by the live funnel and reported absent. It builds no
// registry when none exists, so a key of another type reads not present here.
func Deadline(cx *shard.Ctx, key []byte) (at int64, ok bool) {
	v, loaded := regs.Load(cx.St)
	if !loaded {
		return 0, false
	}
	g := v.(*reg)
	h := g.live(cx, key)
	if h == nil {
		return 0, false
	}
	return h.expireAt, true
}

// Persist removes a hash key's key-level deadline, reporting whether one was
// removed: the hash arm of PERSIST (the whole-key PERSIST, not the per-field
// HPERSIST). A live hash with no key deadline, an expired hash, or an absent key
// all report false, so PERSIST answers 0 for them. It builds no registry when none
// exists.
func Persist(cx *shard.Ctx, key []byte) bool {
	v, loaded := regs.Load(cx.St)
	if !loaded {
		return false
	}
	g := v.(*reg)
	h := g.live(cx, key)
	if h == nil || h.expireAt == 0 {
		return false
	}
	h.expireAt = 0
	// Log the cleared key deadline so a replay persists the hash instead of restoring
	// the TTL a prior effect or snapshot carried.
	logExpire(cx, key, 0)
	return true
}
