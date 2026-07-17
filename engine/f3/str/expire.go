package str

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The EXPIRE family for string keys (spec 2064/f3/16 section 2, doc 17 rows at
// line 195). PEXPIREAT is the primitive; EXPIRE, PEXPIRE, and EXPIREAT convert
// their argument to the same absolute unix-ms instant and share this core. The
// deadline lives inline in the string record header, so setting it is a plain
// store through cx.St.SetExpire, no second dict.
//
// Non-string keys never reach here: the dispatch router checks the collection
// keyspaces first. A collection cannot carry a key-level TTL yet (the per-type
// header deadline is the next expiry slice, Spec/2064/f3/milestones/
// M-expiry-generic-key-ttl-plan.md), so the router answers those honestly rather
// than letting this path report a present collection key as absent.

// expireDeadline is deadline's EXPIRE-family twin. Unlike the SET path, a past
// or non-positive instant is legal: EXPIRE with it deletes the key and still
// returns 1 (Redis's documented quirk). Only arithmetic overflow fails, which
// Redis reports as "invalid expire time".
func expireDeadline(nowMs int64, unit int, n int64) (int64, bool) {
	switch unit {
	case unitEXsec:
		ms, ok := secToMs(n)
		if !ok {
			return 0, false
		}
		return addOverflow(nowMs, ms)
	case unitPXms:
		return addOverflow(nowMs, n)
	case unitEXat:
		return secToMs(n)
	default: // unitPXat
		return n, true
	}
}

// Expire answers one of EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT on a string key. verb
// (uppercase) selects the time unit and names the command in the error texts.
// args is key, time, then an optional single NX|XX|GT|LT condition flag. The
// caller has already confirmed the key is not a collection.
func Expire(cx *shard.Ctx, args [][]byte, r shard.Reply, verb string) {
	var unit int
	var lname string
	switch verb {
	case "EXPIRE":
		unit, lname = unitEXsec, "expire"
	case "PEXPIRE":
		unit, lname = unitPXms, "pexpire"
	case "EXPIREAT":
		unit, lname = unitEXat, "expireat"
	default: // PEXPIREAT
		unit, lname = unitPXat, "pexpireat"
	}

	n, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	var nx, xx, gt, lt bool
	for _, f := range args[2:] {
		switch {
		case eqFold(f, "NX"):
			nx = true
		case eqFold(f, "XX"):
			xx = true
		case eqFold(f, "GT"):
			gt = true
		case eqFold(f, "LT"):
			lt = true
		default:
			r.Err("ERR Unsupported option " + string(f))
			return
		}
	}
	// NX excludes every other flag; GT and LT exclude each other. XX pairs with
	// GT or LT (only-if-exists plus the comparison), which Redis allows.
	if (nx && (xx || gt || lt)) || (gt && lt) {
		r.Err("ERR NX and XX, GT or LT options at the same time are not compatible")
		return
	}
	at, ok := expireDeadline(cx.NowMs, unit, n)
	if !ok {
		r.Err("ERR invalid expire time in '" + lname + "' command")
		return
	}

	key := args[0]
	val, present := cx.St.GetString(key, cx.NowMs, cx.Val)
	if !present {
		r.Int(0)
		return
	}
	cx.Val = val

	// A key with no deadline is an infinite TTL for the GT/LT comparison.
	curAt, _ := cx.St.Deadline(key, cx.NowMs)
	hasTTL := curAt != 0
	switch {
	case nx && hasTTL:
		r.Int(0)
		return
	case xx && !hasTTL:
		r.Int(0)
		return
	case gt && (!hasTTL || at <= curAt):
		r.Int(0)
		return
	case lt && hasTTL && at >= curAt:
		r.Int(0)
		return
	}

	// A deadline at or before now deletes the key and still reports success.
	if at <= cx.NowMs {
		cx.St.Del(key, cx.NowMs)
		r.Int(1)
		return
	}
	if _, err := cx.St.SetExpire(key, val, at, cx.NowMs); err != nil {
		if cx.ParkFull(err) {
			return
		}
		r.Err(storeErr(err))
		return
	}
	r.Int(1)
}
