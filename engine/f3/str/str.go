// Package str holds the string command handlers: the point surface over the
// store's string model (spec 2064/f3/09). Every handler runs on its shard's
// owner goroutine with the batch's cached clock, so reads and writes are plain
// single-owner calls and lazy expiry happens on the touch.
package str

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// maxStringLength is the Redis proto-max-bulk-len string size ceiling. The
// store's embedded band caps values at 64 KiB until the separated and chunked
// bands land, so today store.ErrTooBig fires long before this figure; the
// error text is already the one clients expect.
const errStringTooLong = "ERR string exceeds maximum allowed size (proto-max-bulk-len)"

// storeErr maps a store error to its wire text.
func storeErr(err error) string {
	if err == store.ErrTooBig {
		return errStringTooLong
	}
	return "ERR " + err.Error()
}

// eqFold reports whether b equals the ASCII option name s case-insensitively,
// without allocating. s is all-uppercase at every call site.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		x := b[i]
		if x >= 'a' && x <= 'z' {
			x -= 32
		}
		if x != s[i] {
			return false
		}
	}
	return true
}

// Get answers GET key.
func Get(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	v, ok := cx.St.GetString(args[0], cx.NowMs, cx.Val)
	cx.Val = v
	if !ok {
		r.Null()
		return
	}
	r.Bulk(v)
}

// SET option flags parsed off the trailing arguments.
const (
	setNX      = 1 << iota // write only if the key does not exist
	setXX                  // write only if the key already exists
	setGet                 // reply with the old value (nil when absent)
	setKeepTTL             // carry the existing deadline instead of clearing it
)

// Expiry units for the EX/PX/EXAT/PXAT family.
const (
	unitNone  = 0
	unitEXsec = iota // relative seconds
	unitPXms         // relative milliseconds
	unitEXat         // absolute unix seconds
	unitPXat         // absolute unix milliseconds
)

// secToMs converts seconds to milliseconds, reporting whether the multiply
// fit, so an absurd EX argument errors instead of wrapping to a bogus
// deadline.
func secToMs(sec int64) (int64, bool) {
	ms := sec * 1000
	if sec != 0 && ms/1000 != sec {
		return 0, false
	}
	return ms, true
}

// addOverflow returns a+b and whether it stayed inside int64.
func addOverflow(a, b int64) (int64, bool) {
	s := a + b
	if (b > 0 && s < a) || (b < 0 && s > a) {
		return 0, false
	}
	return s, true
}

// deadline folds a (unit, value) pair into an absolute unix-ms deadline,
// false for a non-positive value or an overflow: the raw argument must be
// strictly positive in every unit, matching Redis's
// getExpireMillisecondsOrReply.
func deadline(nowMs int64, unit int, n int64) (int64, bool) {
	if n <= 0 {
		return 0, false
	}
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

// Set answers SET key value [NX|XX] [GET] [EX s|PX ms|EXAT s|PXAT ms|KEEPTTL].
// The deadline is computed before the key is touched, so a bad expire time
// errors without having written anything; with GET the old value is captured
// into the shard scratch before the write, and a guard-suppressed write still
// returns it.
func Set(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, val := args[0], args[1]
	var flags, unit int
	var timeArg int64
	for i := 2; i < len(args); i++ {
		opt := args[i]
		switch {
		case eqFold(opt, "NX"):
			if flags&setXX != 0 {
				r.Err("ERR syntax error")
				return
			}
			flags |= setNX
		case eqFold(opt, "XX"):
			if flags&setNX != 0 {
				r.Err("ERR syntax error")
				return
			}
			flags |= setXX
		case eqFold(opt, "GET"):
			flags |= setGet
		case eqFold(opt, "KEEPTTL"):
			if unit != unitNone {
				r.Err("ERR syntax error")
				return
			}
			flags |= setKeepTTL
		case eqFold(opt, "EX"), eqFold(opt, "PX"), eqFold(opt, "EXAT"), eqFold(opt, "PXAT"):
			// One expiry option only, and KEEPTTL is an expiry option too.
			if unit != unitNone || flags&setKeepTTL != 0 || i+1 >= len(args) {
				r.Err("ERR syntax error")
				return
			}
			n, ok := store.ParseInt(args[i+1])
			if !ok {
				r.Err("ERR value is not an integer or out of range")
				return
			}
			i++
			timeArg = n
			switch {
			case eqFold(opt, "EX"):
				unit = unitEXsec
			case eqFold(opt, "PX"):
				unit = unitPXms
			case eqFold(opt, "EXAT"):
				unit = unitEXat
			default:
				unit = unitPXat
			}
		default:
			r.Err("ERR syntax error")
			return
		}
	}

	var atMs int64
	if unit != unitNone {
		var ok bool
		if atMs, ok = deadline(cx.NowMs, unit, timeArg); !ok {
			r.Err("ERR invalid expire time in 'set' command")
			return
		}
	}

	// Capture the old value for the GET reply before the write overwrites it.
	// The lookup also reaps an expired record, so NX and GET both see it as
	// absent.
	var oldVal []byte
	haveOld := false
	if flags&setGet != 0 {
		oldVal, haveOld = cx.St.GetString(key, cx.NowMs, cx.Val)
		cx.Val = oldVal
	}
	exists := haveOld
	if flags&(setNX|setXX) != 0 && flags&setGet == 0 {
		exists = cx.St.Exists(key, cx.NowMs)
	}

	// The NX/XX guard decides whether the write happens; GET still returns
	// the old value.
	if (flags&setNX != 0 && exists) || (flags&setXX != 0 && !exists) {
		if flags&setGet != 0 && haveOld {
			r.Bulk(oldVal)
			return
		}
		r.Null()
		return
	}

	if err := cx.St.SetString(key, val, cx.NowMs, atMs, flags&setKeepTTL != 0); err != nil {
		r.Err(storeErr(err))
		return
	}
	if flags&setGet != 0 {
		if haveOld {
			r.Bulk(oldVal)
			return
		}
		r.Null()
		return
	}
	r.Status("OK")
}

// Strlen answers STRLEN key: the value's byte length, 0 when absent.
func Strlen(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	n, _ := cx.St.StrLen(args[0], cx.NowMs)
	r.Int(n)
}

// Exists answers single-key EXISTS. The multi-key form is a fan-out and lands
// with the batch fan-out slice.
func Exists(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if cx.St.Exists(args[0], cx.NowMs) {
		r.Int(1)
		return
	}
	r.Int(0)
}

// Del answers single-key DEL. Deleting an expired record reports 0, the lazy
// expiry answer any read would give.
func Del(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if cx.St.Del(args[0], cx.NowMs) {
		r.Int(1)
		return
	}
	r.Int(0)
}

// Type answers TYPE key. Only string records exist in this slice, so the
// answer is "string" or "none".
func Type(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if cx.St.Exists(args[0], cx.NowMs) {
		r.Status("string")
		return
	}
	r.Status("none")
}
