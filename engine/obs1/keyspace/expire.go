package keyspace

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The key-level EXPIRE family (spec 2064/obs1 doc 08 section 8). Every
// verb resolves its argument to one absolute unix-ms deadline, applies
// it to whichever keyspaces hold the key, and frames one expire op, so
// replay restores the same deadline on the same halves. A deadline at or
// behind the clock deletes on the spot and frames a keydel instead, the
// post-decision rule. The class a folding segment earns from the
// deadline is advisory (OB13), so nothing here ever touches the bucket.

// expiry argument units.
const (
	unitSec = iota
	unitMs
)

// condition flags: at most NX alone, or any of XX, GT, LT together
// (Redis's rule; GT with LT is refused).
const (
	condNX = 1 << iota
	condXX
	condGT
	condLT
)

// eqFold reports ASCII-case-insensitive equality against an uppercase
// spelling, the option-parsing helper every type package carries.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

// parseCond folds the option tail into a condition mask, "" on success.
func parseCond(args [][]byte) (int, string) {
	cond := 0
	for _, a := range args {
		switch {
		case eqFold(a, "NX"):
			cond |= condNX
		case eqFold(a, "XX"):
			cond |= condXX
		case eqFold(a, "GT"):
			cond |= condGT
		case eqFold(a, "LT"):
			cond |= condLT
		default:
			return 0, "ERR Unsupported option " + string(a)
		}
	}
	if cond&condNX != 0 && cond&(condXX|condGT|condLT) != 0 {
		return 0, "ERR NX and XX, GT or LT options at the same time are not compatible"
	}
	if cond&condGT != 0 && cond&condLT != 0 {
		return 0, "ERR GT and LT options at the same time are not compatible"
	}
	return cond, ""
}

// deadlineOf reports key's deadline across every keyspace: at is the
// absolute unix ms (0 for a live key with no TTL) and exists whether any
// keyspace holds the key. A key both keyspaces hold answers the
// collection's deadline, the same precedence TYPE gives.
func deadlineOf(cx *shard.Ctx, key []byte) (at int64, exists bool) {
	if at, kind := collDeadline(cx, key); kind != "" {
		return at, true
	}
	if cx.St.Exists(key, cx.NowMs) {
		return cx.St.ExpireAt(key, cx.NowMs), true
	}
	return 0, false
}

// expireGeneric is EXPIRE, PEXPIRE, EXPIREAT, and PEXPIREAT: resolve the
// argument to an absolute ms deadline, gate on the condition flags
// against the current deadline (none counts as infinitely far for GT and
// LT, so GT never sets one on a persistent key), then land it on both
// halves. Unlike SET's expiry options a non-positive resolved deadline
// is legal and deletes the key immediately, answering 1.
func expireGeneric(cx *shard.Ctx, args [][]byte, r shard.Reply, name string, unit int, absolute bool) {
	key := args[0]
	n, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	cond, errMsg := parseCond(args[2:])
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	at := n
	if unit == unitSec {
		at = n * 1000
		if n != 0 && at/1000 != n {
			r.Err("ERR invalid expire time in '" + name + "' command")
			return
		}
	}
	if !absolute {
		s := cx.NowMs + at
		if (at > 0 && s < cx.NowMs) || (at < 0 && s > cx.NowMs) {
			r.Err("ERR invalid expire time in '" + name + "' command")
			return
		}
		at = s
	}
	cur, exists := deadlineOf(cx, key)
	if !exists {
		r.Int(0)
		return
	}
	if cond&condNX != 0 && cur != 0 ||
		cond&condXX != 0 && cur == 0 ||
		cond&condGT != 0 && (cur == 0 || at <= cur) ||
		cond&condLT != 0 && cur != 0 && at >= cur {
		r.Int(0)
		return
	}
	if at <= cx.NowMs {
		// Fired on arrival: the key dies now, framed as the keydel it is.
		dropColl(cx, key)
		cx.St.Del(key, cx.NowMs)
		cx.DropRootDeadline(key)
		if err := cx.LogKeyDel(key); err != nil {
			r.Err(err.Error())
			return
		}
		r.Int(1)
		return
	}
	hit := setCollDeadline(cx, key, at)
	if lived, err := cx.St.SetExpire(key, at, cx.NowMs); err != nil {
		r.Err(err.Error())
		return
	} else if lived {
		hit = true
	}
	if !hit {
		// deadlineOf saw the key, so it left between the two probes only
		// if the string store reaped a fired record; that key is gone.
		r.Int(0)
		return
	}
	if err := cx.LogExpire(key, at); err != nil {
		r.Err(err.Error())
		return
	}
	r.Int(1)
}

// Expire answers EXPIRE key seconds [NX|XX|GT|LT].
func Expire(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	expireGeneric(cx, args, r, "expire", unitSec, false)
}

// PExpire answers PEXPIRE key milliseconds [NX|XX|GT|LT].
func PExpire(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	expireGeneric(cx, args, r, "pexpire", unitMs, false)
}

// ExpireAt answers EXPIREAT key unix-seconds [NX|XX|GT|LT].
func ExpireAt(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	expireGeneric(cx, args, r, "expireat", unitSec, true)
}

// PExpireAt answers PEXPIREAT key unix-ms [NX|XX|GT|LT].
func PExpireAt(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	expireGeneric(cx, args, r, "pexpireat", unitMs, true)
}

// ttlGeneric is TTL, PTTL, EXPIRETIME, and PEXPIRETIME: -2 for an absent
// key, -1 for a live key with no deadline, else the deadline rendered as
// a remaining span or an absolute stamp. TTL rounds to the nearest
// second, Redis's (ttl+500)/1000; EXPIRETIME truncates, Redis's /1000.
func ttlGeneric(cx *shard.Ctx, args [][]byte, r shard.Reply, unit int, absolute bool) {
	at, exists := deadlineOf(cx, args[0])
	if !exists {
		r.Int(-2)
		return
	}
	if at == 0 {
		r.Int(-1)
		return
	}
	if absolute {
		if unit == unitSec {
			r.Int(at / 1000)
			return
		}
		r.Int(at)
		return
	}
	ttl := at - cx.NowMs
	if unit == unitSec {
		r.Int((ttl + 500) / 1000)
		return
	}
	r.Int(ttl)
}

// TTL answers TTL key in seconds.
func TTL(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	ttlGeneric(cx, args, r, unitSec, false)
}

// PTTL answers PTTL key in milliseconds.
func PTTL(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	ttlGeneric(cx, args, r, unitMs, false)
}

// ExpireTime answers EXPIRETIME key as an absolute unix-seconds stamp.
func ExpireTime(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	ttlGeneric(cx, args, r, unitSec, true)
}

// PExpireTime answers PEXPIRETIME key as an absolute unix-ms stamp.
func PExpireTime(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	ttlGeneric(cx, args, r, unitMs, true)
}

// Persist answers PERSIST key: clear the deadline from whichever halves
// carry one, 1 when one was cleared, framed as an expire-to-0 so replay
// clears the same halves.
func Persist(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	at, exists := deadlineOf(cx, key)
	if !exists || at == 0 {
		r.Int(0)
		return
	}
	setCollDeadline(cx, key, 0)
	if _, err := cx.St.SetExpire(key, 0, cx.NowMs); err != nil {
		r.Err(err.Error())
		return
	}
	if err := cx.LogExpire(key, 0); err != nil {
		r.Err(err.Error())
		return
	}
	r.Int(1)
}
