package stream

import (
	"github.com/tamnd/aki/engine/f3/expire"
	"github.com/tamnd/aki/engine/f3/shard"
)

// Key-level TTL for stream keys (spec 2064/f3/16 section 2, rollout plan
// Spec/2064/f3/milestones/M-expiry-generic-key-ttl-plan.md). This is the last of
// the five collection keyspaces to gain a key deadline; with it the interim
// not-yet error in dispatch goes away and EXPIRE routes to a real backend for every
// type. The deadline lives inline in the stream header (stream.go expireAt); the
// EXPIRE-family semantics live once in package expire, and this file supplies only
// the stream keyspace's plumbing: read the current deadline, store a new one, drop
// the key. A stream has no per-field TTL, so like the list there is one deadline and
// nothing else to reap. Setting a stream's deadline never allocates and never fails,
// so Store always succeeds.

// streamBackend drives an EXPIRE-family command over one stream key. It resolves the
// stream once on Present through the same live funnel every stream command uses, so
// an already-expired stream is dropped and reported absent before any deadline is
// set.
type streamBackend struct {
	g   *reg
	cx  *shard.Ctx
	key []byte
	s   *stream
}

func (b *streamBackend) Present() (curAt int64, present bool) {
	b.s = b.g.live(b.cx, b.key)
	if b.s == nil {
		return 0, false
	}
	return b.s.expireAt, true
}

func (b *streamBackend) Delete() {
	// An EXPIRE to a past instant deletes the key on the spot; log the key-delete so a
	// replay drops it instead of resurrecting the entries from an earlier effect.
	logDeleteKey(b.cx, b.key)
	b.g.drop(b.key)
}

func (b *streamBackend) Store(at int64) bool {
	b.s.expireAt = at
	logExpire(b.cx, b.key, at)
	return true
}

// Expire answers one of EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT on a stream key. verb is
// the uppercase command name; args is key, time, then an optional condition flag.
// The dispatch router sends a stream key here after confirming the type with Has.
func Expire(cx *shard.Ctx, args [][]byte, r shard.Reply, verb string) {
	expire.Apply(cx, args, r, verb, &streamBackend{g: registry(cx), cx: cx, key: args[0]})
}

// Deadline reports key's deadline and whether it holds a live stream on this shard.
// at is 0 for a live stream with no TTL, which the TTL reader renders as -1. It is
// the stream arm of the unified TTL/PTTL/EXPIRETIME resolution; an expired stream is
// dropped by the live funnel and reported absent. It builds no registry when none
// exists, so a key of another type reads not present here.
func Deadline(cx *shard.Ctx, key []byte) (at int64, ok bool) {
	v, loaded := regs.Load(cx.St)
	if !loaded {
		return 0, false
	}
	g := v.(*reg)
	s := g.live(cx, key)
	if s == nil {
		return 0, false
	}
	return s.expireAt, true
}

// Persist removes a stream key's deadline, reporting whether one was removed: the
// stream arm of PERSIST. A live stream with no deadline, an expired stream, or an
// absent key all report false, so PERSIST answers 0 for them. It builds no registry
// when none exists.
func Persist(cx *shard.Ctx, key []byte) bool {
	v, loaded := regs.Load(cx.St)
	if !loaded {
		return false
	}
	g := v.(*reg)
	s := g.live(cx, key)
	if s == nil || s.expireAt == 0 {
		return false
	}
	s.expireAt = 0
	// Log the cleared deadline so a replay persists the stream instead of restoring the
	// TTL a prior effect or snapshot carried.
	logExpire(cx, key, 0)
	return true
}
