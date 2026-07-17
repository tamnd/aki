package str

import (
	"github.com/tamnd/aki/engine/f3/expire"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The EXPIRE family for string keys (spec 2064/f3/16 section 2, doc 17 rows at
// line 195). The command semantics (unit conversion, NX/XX/GT/LT gate, the
// past-instant-deletes quirk) live once in package expire; this file supplies
// only the string keyspace's deadline plumbing. The deadline lives inline in the
// string record header, so setting it is a plain store through cx.St.SetExpire,
// no second dict.
//
// Non-string keys never reach here: the dispatch router checks the collection
// keyspaces first, routing the types that carry an inline deadline to their own
// backend and answering the rest with an honest not-yet error (rollout plan
// Spec/2064/f3/milestones/M-expiry-generic-key-ttl-plan.md).

// strBackend drives an EXPIRE-family command over the string store. It captures
// the value on Present so Store can rebuild the slotless record with the deadline
// word set (cx.St.SetExpire needs the value; the store keeps no value it can read
// back cheaply for a cold key).
type strBackend struct {
	cx  *shard.Ctx
	r   shard.Reply
	key []byte
	val []byte
}

func (b *strBackend) Present() (curAt int64, present bool) {
	val, ok := b.cx.St.GetString(b.key, b.cx.NowMs, b.cx.Val)
	if !ok {
		return 0, false
	}
	b.cx.Val = val
	b.val = val
	// Deadline reports at==0 for a live key with no deadline, which the core reads
	// as an infinite TTL.
	at, _ := b.cx.St.Deadline(b.key, b.cx.NowMs)
	return at, true
}

func (b *strBackend) Delete() { b.cx.St.Del(b.key, b.cx.NowMs) }

func (b *strBackend) Store(at int64) bool {
	if _, err := b.cx.St.SetExpire(b.key, b.val, at, b.cx.NowMs); err != nil {
		if b.cx.ParkFull(err) {
			return false
		}
		b.r.Err(storeErr(err))
		return false
	}
	return true
}

// Expire answers one of EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT on a string key. verb
// is the uppercase command name; args is key, time, then an optional condition
// flag. The caller has already confirmed the key is not a collection.
func Expire(cx *shard.Ctx, args [][]byte, r shard.Reply, verb string) {
	expire.Apply(cx, args, r, verb, &strBackend{cx: cx, r: r, key: args[0]})
}
