package set

import (
	"github.com/tamnd/aki/engine/f3/expire"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
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
	g    *reg
	cx   *shard.Ctx
	key  []byte
	s    *set
	home int
}

func (b *setBackend) Present() (curAt int64, present bool) {
	b.s, b.home = b.g.resolveTouch(b.cx, b.key)
	if b.s == nil {
		// homeString cannot reach here: the EXPIRE dispatch confirmed the set type
		// with Has before routing to this backend, so an absent set is the only case.
		return 0, false
	}
	return b.s.expireAt, true
}

func (b *setBackend) Delete() {
	// An EXPIRE to a past instant deletes the key on the spot; log the key-delete so a
	// replay drops it instead of resurrecting the members from an earlier effect.
	logDeleteKey(b.cx, b.key)
	switch b.home {
	case homeReg:
		b.g.drop(b.key)
	case homeArena:
		b.cx.St.DropCollBlob(b.key)
	}
}

func (b *setBackend) Store(at int64) bool {
	b.s.expireAt = at
	// commit rewrites the arena record with the new deadline (or reconciles the
	// g.m set's footprint), so a volatile arena set carries its TTL in the inline
	// record, not a side table.
	b.g.commit(b.cx, b.key, b.s, b.home)
	logExpire(b.cx, b.key, at)
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
	if cx.Coll != nil {
		if s := cx.Coll.(*reg).live(cx, key); s != nil {
			return s.expireAt, true
		}
	}
	if _, _, at, present := peekArenaSet(cx, key); present {
		return at, true
	}
	return 0, false
}

// Persist removes a set key's deadline, reporting whether one was removed: the
// set arm of PERSIST. A live set with no deadline, an expired set, or an absent
// key all report false, so PERSIST answers 0 for them. It builds no registry when
// none exists.
func Persist(cx *shard.Ctx, key []byte) bool {
	if cx.Coll != nil {
		if s := cx.Coll.(*reg).live(cx, key); s != nil {
			if s.expireAt == 0 {
				return false
			}
			s.expireAt = 0
			// Log the cleared deadline so a replay persists the set instead of restoring
			// the TTL a prior effect or snapshot carried.
			logExpire(cx, key, 0)
			return true
		}
	}
	// A tiny arena set carries its deadline in the inline record: clearing it
	// rewrites the record with expireAt 0 through PutCollBlob, the same in-place
	// republish commit uses, so the set stays inline with no TTL.
	blob, bits, at, present := peekArenaSet(cx, key)
	if !present || at == 0 {
		return false
	}
	_ = cx.St.PutCollBlob(key, store.KindSet, bits, blob, 0, cx.NowMs)
	logExpire(cx, key, 0)
	return true
}
