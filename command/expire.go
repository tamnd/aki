package command

import (
	"math"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// expireCommands returns the TTL command family: the four EXPIRE variants, the
// two EXPIRETIME readers, TTL/PTTL, and PERSIST. They all read or rewrite the
// value header's absolute expiry, never the body itself.
func expireCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "expire", Group: GroupGeneric, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleExpire(ctx, "expire") }},
		{Name: "pexpire", Group: GroupGeneric, Since: "2.6.0",
			Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleExpire(ctx, "pexpire") }},
		{Name: "expireat", Group: GroupGeneric, Since: "1.2.0",
			Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleExpire(ctx, "expireat") }},
		{Name: "pexpireat", Group: GroupGeneric, Since: "2.6.0",
			Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleExpire(ctx, "pexpireat") }},
		{Name: "expiretime", Group: GroupGeneric, Since: "7.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleExpireTime(ctx, false) }},
		{Name: "pexpiretime", Group: GroupGeneric, Since: "7.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleExpireTime(ctx, true) }},
		{Name: "ttl", Group: GroupGeneric, Since: "1.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleTTL(ctx, false) }},
		{Name: "pttl", Group: GroupGeneric, Since: "2.6.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleTTL(ctx, true) }},
		{Name: "persist", Group: GroupGeneric, Since: "2.2.0",
			Arity: 2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handlePersist},
	}
}

// expireCond holds which one of NX/XX/GT/LT was requested, if any.
type expireCond struct {
	nx, xx, gt, lt bool
}

// parseExpireCond reads the optional condition flags after the numeric argument.
// At most one of NX/XX may appear and GT and LT may not appear together, matching
// Redis 7. The bool reports success; on failure the caller's error string is set.
func parseExpireCond(args [][]byte) (expireCond, string, bool) {
	var c expireCond
	for _, a := range args {
		switch strings.ToUpper(string(a)) {
		case "NX":
			c.nx = true
		case "XX":
			c.xx = true
		case "GT":
			c.gt = true
		case "LT":
			c.lt = true
		default:
			return c, "ERR Unsupported option " + string(a), false
		}
	}
	if c.nx && (c.xx || c.gt || c.lt) {
		return c, "ERR NX and XX, GT or LT options at the same time are not compatible", false
	}
	if c.gt && c.lt {
		return c, "ERR GT and LT options at the same time are not compatible", false
	}
	return c, "", true
}

// whenFor turns the numeric argument into an absolute Unix-ms expiry for the
// given command mode. It reports false on overflow so the caller can answer with
// the invalid-expire-time error.
func whenFor(mode string, now, val int64) (int64, bool) {
	switch mode {
	case "expire":
		ms, ok := secsToMillis(val)
		if !ok {
			return 0, false
		}
		return addNoOverflow(now, ms)
	case "pexpire":
		return addNoOverflow(now, val)
	case "expireat":
		return secsToMillis(val)
	default: // pexpireat
		return val, true
	}
}

// secsToMillis multiplies seconds by 1000, reporting false on int64 overflow.
func secsToMillis(s int64) (int64, bool) {
	if s > math.MaxInt64/1000 || s < math.MinInt64/1000 {
		return 0, false
	}
	return s * 1000, true
}

// addNoOverflow adds two int64 values, reporting false on overflow.
func addNoOverflow(a, b int64) (int64, bool) {
	sum := a + b
	if (b > 0 && sum < a) || (b < 0 && sum > a) {
		return 0, false
	}
	return sum, true
}

// handleExpire implements EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT. It returns 1
// when the expiry is applied (including when a past deadline deletes the key) and
// 0 when the key is missing or a condition flag blocks the change.
func handleExpire(ctx *Ctx, mode string) {
	key := ctx.Argv[1]
	val, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	cond, condErr, condOK := parseExpireCond(ctx.Argv[3:])
	if !condOK {
		ctx.enc().WriteError(condErr)
		return
	}
	now := keyspace.NowMillis()
	when, ok := whenFor(mode, now, val)
	if !ok {
		ctx.enc().WriteError("ERR invalid expire time in '" + mode + "' command")
		return
	}

	var res int64
	var deleted bool
	if ctx.update(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		hasTTL := hdr.HasTTL()
		curr := hdr.TTLms
		switch {
		case cond.nx && hasTTL:
			return nil
		case cond.xx && !hasTTL:
			return nil
		case cond.gt && (!hasTTL || when <= curr):
			return nil
		case cond.lt && hasTTL && when >= curr:
			return nil
		}
		if when <= now {
			if _, err := db.Delete(key); err != nil {
				return err
			}
			res = 1
			deleted = true
			return nil
		}
		if err := db.Set(key, body, hdr.Type, hdr.Encoding, when); err != nil {
			return err
		}
		res = 1
		return nil
	}) {
		if res == 1 {
			if deleted {
				ctx.notify(notifyGeneric, "del", key)
			} else {
				ctx.notify(notifyGeneric, "expire", key)
			}
		}
		ctx.enc().WriteInteger(res)
	}
}

// handleExpireTime implements EXPIRETIME and PEXPIRETIME. It returns the absolute
// expiry (seconds or ms), -1 when the key has no expiry, and -2 when it is gone.
func handleExpireTime(ctx *Ctx, ms bool) {
	key := ctx.Argv[1]
	var res int64
	if ctx.view(func(db *keyspace.DB) error {
		_, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		switch {
		case !found:
			res = -2
		case !hdr.HasTTL():
			res = -1
		case ms:
			res = hdr.TTLms
		default:
			res = hdr.TTLms / 1000
		}
		return nil
	}) {
		ctx.enc().WriteInteger(res)
	}
}

// handleTTL implements TTL and PTTL. It returns the remaining time (seconds or
// ms), -1 when the key has no expiry, and -2 when it is gone or already expired.
func handleTTL(ctx *Ctx, ms bool) {
	key := ctx.Argv[1]
	now := keyspace.NowMillis()
	var res int64
	if ctx.view(func(db *keyspace.DB) error {
		_, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		switch {
		case !found:
			res = -2
		case !hdr.HasTTL():
			res = -1
		default:
			remaining := hdr.TTLms - now
			if ms {
				res = remaining
			} else {
				res = remaining / 1000
			}
		}
		return nil
	}) {
		ctx.enc().WriteInteger(res)
	}
}

// handlePersist removes a key's expiry. It returns 1 when a TTL was cleared and 0
// when the key is missing or already persistent.
func handlePersist(ctx *Ctx) {
	key := ctx.Argv[1]
	var res int64
	if ctx.update(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if !found || !hdr.HasTTL() {
			return nil
		}
		if err := db.Set(key, body, hdr.Type, hdr.Encoding, -1); err != nil {
			return err
		}
		res = 1
		return nil
	}) {
		if res == 1 {
			ctx.notify(notifyGeneric, "persist", key)
		}
		ctx.enc().WriteInteger(res)
	}
}
