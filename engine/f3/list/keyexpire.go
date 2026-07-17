package list

import (
	"github.com/tamnd/aki/engine/f3/expire"
	"github.com/tamnd/aki/engine/f3/shard"
)

// Key-level TTL for list keys (spec 2064/f3/16 section 2, rollout plan
// Spec/2064/f3/milestones/M-expiry-generic-key-ttl-plan.md). The deadline lives
// inline in the list header (list.go expireAt); the EXPIRE-family semantics live
// once in package expire, and this file supplies only the list keyspace's
// plumbing: read the current deadline, store a new one, drop the key. A list has
// no per-field TTL, so unlike the hash there is one deadline and nothing else to
// reap. Setting a list's deadline never allocates and never fails, so Store always
// succeeds.

// listBackend drives an EXPIRE-family command over one list key. It resolves the
// list once on Present through the same live funnel every list command uses, so an
// already-expired list is dropped and reported absent before any deadline is set.
type listBackend struct {
	g   *reg
	cx  *shard.Ctx
	key []byte
	l   *list
}

func (b *listBackend) Present() (curAt int64, present bool) {
	b.l = b.g.live(b.cx, b.key)
	if b.l == nil {
		return 0, false
	}
	return b.l.expireAt, true
}

func (b *listBackend) Delete() { b.g.drop(b.key) }

func (b *listBackend) Store(at int64) bool {
	b.l.expireAt = at
	return true
}

// Expire answers one of EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT on a list key. verb is
// the uppercase command name; args is key, time, then an optional condition flag.
// The dispatch router sends a list key here after confirming the type with Has.
func Expire(cx *shard.Ctx, args [][]byte, r shard.Reply, verb string) {
	expire.Apply(cx, args, r, verb, &listBackend{g: registry(cx), cx: cx, key: args[0]})
}

// Deadline reports key's deadline and whether it holds a live list on this shard.
// at is 0 for a live list with no TTL, which the TTL reader renders as -1. It is
// the list arm of the unified TTL/PTTL/EXPIRETIME resolution; an expired list is
// dropped by the live funnel and reported absent. It builds no registry when none
// exists, so a key of another type reads not present here.
func Deadline(cx *shard.Ctx, key []byte) (at int64, ok bool) {
	v, loaded := regs.Load(cx.St)
	if !loaded {
		return 0, false
	}
	g := v.(*reg)
	l := g.live(cx, key)
	if l == nil {
		return 0, false
	}
	return l.expireAt, true
}

// Persist removes a list key's deadline, reporting whether one was removed: the
// list arm of PERSIST. A live list with no deadline, an expired list, or an absent
// key all report false, so PERSIST answers 0 for them. It builds no registry when
// none exists.
func Persist(cx *shard.Ctx, key []byte) bool {
	v, loaded := regs.Load(cx.St)
	if !loaded {
		return false
	}
	g := v.(*reg)
	l := g.live(cx, key)
	if l == nil || l.expireAt == 0 {
		return false
	}
	l.expireAt = 0
	return true
}
