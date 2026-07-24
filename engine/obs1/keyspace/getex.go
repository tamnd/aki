package keyspace

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// GETEX expiry option units. Unlike the EXPIRE family the argument must
// be strictly positive in every unit (Redis's getExpireMillisecondsOrReply
// rule, the one SET's options share), though an absolute stamp already
// behind the clock is legal and deletes the key after the read.
const (
	exNone = iota
	exSec
	exMs
	exSecAt
	exMsAt
)

// Getex answers GETEX key [EX s|PX ms|EXAT s|PXAT ms|PERSIST]: the value
// first, then the deadline mutation. It lives here rather than in the
// string package because the mutation frames the same expire op EXPIRE
// does, which replay applies to both keyspace halves; landing it on both
// halves live keeps a dual-half key convergent. A key the string store
// does not hold answers nil untouched, so a collection-only key cannot
// have its deadline moved through GETEX.
func Getex(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	unit := exNone
	var timeArg int64
	persist := false
	for i := 1; i < len(args); i++ {
		opt := args[i]
		switch {
		case eqFold(opt, "PERSIST"):
			if persist || unit != exNone {
				r.Err("ERR syntax error")
				return
			}
			persist = true
		case eqFold(opt, "EX"), eqFold(opt, "PX"), eqFold(opt, "EXAT"), eqFold(opt, "PXAT"):
			if persist || unit != exNone || i+1 >= len(args) {
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
				unit = exSec
			case eqFold(opt, "PX"):
				unit = exMs
			case eqFold(opt, "EXAT"):
				unit = exSecAt
			default:
				unit = exMsAt
			}
		default:
			r.Err("ERR syntax error")
			return
		}
	}
	var at int64
	if unit != exNone {
		if timeArg <= 0 {
			r.Err("ERR invalid expire time in 'getex' command")
			return
		}
		at = timeArg
		if unit == exSec || unit == exSecAt {
			at = timeArg * 1000
			if at/1000 != timeArg {
				r.Err("ERR invalid expire time in 'getex' command")
				return
			}
		}
		if unit == exSec || unit == exMs {
			s := cx.NowMs + at
			if s < cx.NowMs {
				r.Err("ERR invalid expire time in 'getex' command")
				return
			}
			at = s
		}
	}

	// The read copies into the shard scratch: an in-place TTL rewrite below
	// must not mutate the bytes the reply is about to send.
	v, okv := cx.St.GetString(key, cx.NowMs, cx.Val)
	cx.Val = v
	if !okv {
		r.Null()
		return
	}
	switch {
	case persist:
		if cur, _ := deadlineOf(cx, key); cur != 0 {
			setCollDeadline(cx, key, 0)
			if _, err := cx.St.SetExpire(key, 0, cx.NowMs); err != nil {
				r.Err(err.Error())
				return
			}
			if err := cx.LogExpire(key, 0); err != nil {
				r.Err(err.Error())
				return
			}
		}
	case unit != exNone && at <= cx.NowMs:
		// A stamp already due: the key dies after the read, framed as the
		// keydel it is, the expireGeneric fired arm.
		dropColl(cx, key)
		cx.St.Del(key, cx.NowMs)
		cx.DropRootDeadline(key)
		if err := cx.LogKeyDel(key); err != nil {
			r.Err(err.Error())
			return
		}
	case unit != exNone:
		setCollDeadline(cx, key, at)
		if _, err := cx.St.SetExpire(key, at, cx.NowMs); err != nil {
			r.Err(err.Error())
			return
		}
		if err := cx.LogExpire(key, at); err != nil {
			r.Err(err.Error())
			return
		}
	}
	r.Bulk(v)
}
