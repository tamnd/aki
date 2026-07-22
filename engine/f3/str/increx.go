package str

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// INCREX (Redis 8.8, spec 2064/f3/17 command-coverage): increment a key by an
// integer or float, with optional bounds and expiry, in one atomic step.
//
//	INCREX key [BYFLOAT inc | BYINT inc] [LBOUND lb] [UBOUND ub] [SATURATE]
//	       [EX s | PX ms | EXAT unix-s | PXAT unix-ms | PERSIST] [ENX]
//
// Unlike INCR/INCRBY it replies a two-element array: the new value, and the
// increment actually applied. If the key is absent it starts from 0. When the
// result would fall outside LBOUND/UBOUND (or the type limits), the default is to
// skip the write and reply [current, 0], leaving the key and its TTL untouched;
// SATURATE instead caps the result at the bound and reports the saturated delta.
// The expiry options set or clear the TTL like SET; ENX sets the TTL only when
// the key currently has none, so a window counter's TTL is set on creation and
// not reset on later increments.

// The two arithmetic modes: integer (default or BYINT) and float (BYFLOAT).
const (
	increxInt = iota
	increxFloat
)

// increxErr is the invalid-expire-time text under INCREX's own name.
const errIncrexExpire = "ERR invalid expire time in 'increx' command"

// increxOpts is the parsed INCREX tail (args after the key).
type increxOpts struct {
	mode     int
	incInt   int64
	incFloat float64

	hasLB, hasUB bool
	lbRaw, ubRaw []byte
	lbInt, ubInt int64
	lbFloat      float64
	ubFloat      float64

	saturate bool

	unit    int
	timeArg int64
	persist bool
	enx     bool
}

// parseIncrex parses the INCREX tail and resolves the bounds against the chosen
// mode. It returns the wire error text on a malformed tail, empty on success.
func parseIncrex(args [][]byte) (increxOpts, string) {
	var o increxOpts
	o.mode = increxInt
	o.incInt = 1
	hasInc := false

	for i := 1; i < len(args); i++ {
		opt := args[i]
		switch {
		case eqFold(opt, "BYINT"):
			if hasInc || i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			n, ok := store.ParseInt(args[i+1])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			o.mode, o.incInt, hasInc = increxInt, n, true
			i++
		case eqFold(opt, "BYFLOAT"):
			if hasInc || i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			f, ok := store.ParseRedisFloat(args[i+1])
			if !ok {
				return o, "ERR value is not a valid float"
			}
			o.mode, o.incFloat, hasInc = increxFloat, f, true
			i++
		case eqFold(opt, "LBOUND"):
			if o.hasLB || i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			o.lbRaw, o.hasLB = args[i+1], true
			i++
		case eqFold(opt, "UBOUND"):
			if o.hasUB || i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			o.ubRaw, o.hasUB = args[i+1], true
			i++
		case eqFold(opt, "SATURATE"):
			if o.saturate {
				return o, "ERR syntax error"
			}
			o.saturate = true
		case eqFold(opt, "PERSIST"):
			if o.unit != unitNone || o.persist {
				return o, "ERR syntax error"
			}
			o.persist = true
		case eqFold(opt, "EX"), eqFold(opt, "PX"), eqFold(opt, "EXAT"), eqFold(opt, "PXAT"):
			if o.unit != unitNone || o.persist || i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			n, ok := store.ParseInt(args[i+1])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			o.timeArg = n
			switch {
			case eqFold(opt, "EX"):
				o.unit = unitEXsec
			case eqFold(opt, "PX"):
				o.unit = unitPXms
			case eqFold(opt, "EXAT"):
				o.unit = unitEXat
			default:
				o.unit = unitPXat
			}
			i++
		case eqFold(opt, "ENX"):
			if o.enx {
				return o, "ERR syntax error"
			}
			o.enx = true
		default:
			return o, "ERR syntax error"
		}
	}

	// ENX only qualifies an EX/PX/EXAT/PXAT and never pairs with PERSIST (which the
	// unit guard above already excludes).
	if o.enx && o.unit == unitNone {
		return o, "ERR syntax error"
	}

	// Resolve the bounds now that the mode is fixed.
	if o.mode == increxInt {
		o.lbInt, o.ubInt = math.MinInt64, math.MaxInt64
		if o.hasLB {
			n, ok := store.ParseInt(o.lbRaw)
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			o.lbInt = n
		}
		if o.hasUB {
			n, ok := store.ParseInt(o.ubRaw)
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			o.ubInt = n
		}
		if o.lbInt > o.ubInt {
			return o, "ERR LBOUND must be less than or equal to UBOUND"
		}
	} else {
		o.lbFloat, o.ubFloat = -math.MaxFloat64, math.MaxFloat64
		if o.hasLB {
			f, ok := store.ParseRedisFloat(o.lbRaw)
			if !ok {
				return o, "ERR value is not a valid float"
			}
			o.lbFloat = f
		}
		if o.hasUB {
			f, ok := store.ParseRedisFloat(o.ubRaw)
			if !ok {
				return o, "ERR value is not a valid float"
			}
			o.ubFloat = f
		}
		if o.lbFloat > o.ubFloat {
			return o, "ERR LBOUND must be less than or equal to UBOUND"
		}
	}
	return o, ""
}

// subOverflow returns a-b and whether it stayed inside int64.
func subOverflow(a, b int64) (int64, bool) {
	d := a - b
	if (b > 0 && d > a) || (b < 0 && d < a) {
		return 0, false
	}
	return d, true
}

// Increx answers INCREX. It reads the current value, applies the bounded
// increment, writes the result unless the bounds skip it, applies the expiry
// option, and replies with the [new value, actual increment] pair.
func Increx(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	o, msg := parseIncrex(args)
	if msg != "" {
		r.Err(msg)
		return
	}
	// Validate the expiry up front, so a bad expire time errors before any
	// arithmetic, the SET convention.
	var atMs int64
	if o.unit != unitNone {
		at, ok := deadline(cx.NowMs, o.unit, o.timeArg)
		if !ok {
			r.Err(errIncrexExpire)
			return
		}
		atMs = at
	}
	if o.mode == increxInt {
		increxIntApply(cx, args[0], o, atMs, r)
		return
	}
	increxFloatApply(cx, args[0], o, atMs, r)
}

// increxIntApply runs the integer path.
func increxIntApply(cx *shard.Ctx, key []byte, o increxOpts, atMs int64, r shard.Reply) {
	var cur int64
	old, present := cx.St.GetString(key, cx.NowMs, cx.Val)
	cx.Val = old
	if present {
		n, ok := store.ParseInt(old)
		if !ok {
			r.Err("ERR value is not an integer or out of range")
			return
		}
		cur = n
	}

	sum, ok := addOverflow(cur, o.incInt)
	inBounds := ok && sum >= o.lbInt && sum <= o.ubInt

	if !inBounds && !o.saturate {
		// Skip: reply [current, 0], key and TTL untouched.
		r.Raw(increxIntReply(cx.Aux[:0], cur, 0))
		return
	}

	var newVal, actual int64
	if inBounds {
		newVal, actual = sum, o.incInt
	} else {
		// Saturate: clamp to the crossed bound (or the type limit on overflow).
		switch {
		case !ok && o.incInt > 0:
			newVal = o.ubInt
		case !ok && o.incInt < 0:
			newVal = o.lbInt
		case sum < o.lbInt:
			newVal = o.lbInt
		default:
			newVal = o.ubInt
		}
		d, dok := subOverflow(newVal, cur)
		if !dok {
			r.Err("ERR increment or decrement would overflow")
			return
		}
		actual = d
	}

	var nb [24]byte
	valBytes := strconv.AppendInt(nb[:0], newVal, 10)
	if !increxWrite(cx, key, valBytes, o, atMs, r) {
		return
	}
	r.Raw(increxIntReply(cx.Aux[:0], newVal, actual))
}

// increxFloatApply runs the BYFLOAT path.
func increxFloatApply(cx *shard.Ctx, key []byte, o increxOpts, atMs int64, r shard.Reply) {
	var cur float64
	old, present := cx.St.GetString(key, cx.NowMs, cx.Val)
	cx.Val = old
	if present {
		f, ok := store.ParseRedisFloat(old)
		if !ok {
			r.Err("ERR value is not a valid float")
			return
		}
		cur = f
	}

	sum := cur + o.incFloat
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		r.Err("ERR increment would produce NaN or Infinity")
		return
	}
	inBounds := sum >= o.lbFloat && sum <= o.ubFloat

	if !inBounds && !o.saturate {
		r.Raw(increxFloatReply(cx.Aux[:0], cur, 0, r.Resp3()))
		return
	}

	var newVal, actual float64
	if inBounds {
		newVal, actual = sum, o.incFloat
	} else {
		if sum < o.lbFloat {
			newVal = o.lbFloat
		} else {
			newVal = o.ubFloat
		}
		actual = newVal - cur
		if math.IsInf(actual, 0) {
			r.Err("ERR increment would produce NaN or Infinity")
			return
		}
	}

	var nb [40]byte
	valBytes := resp.FormatScore(nb[:0], newVal)
	if !increxWrite(cx, key, valBytes, o, atMs, r) {
		return
	}
	r.Raw(increxFloatReply(cx.Aux[:0], newVal, actual, r.Resp3()))
}

// increxWrite stores the new value under the command's expiry policy and fires
// the increx event. It reports false (after writing the reply error) when the
// store refuses the write. The expiry rules: PERSIST clears the TTL, an
// EX/PX/EXAT/PXAT option sets it (ENX only when the key currently has none), and
// no expiry option preserves the existing TTL, like INCR.
func increxWrite(cx *shard.Ctx, key, val []byte, o increxOpts, atMs int64, r shard.Reply) bool {
	writeAt := atMs
	keepTTL := false
	switch {
	case o.persist:
		writeAt, keepTTL = 0, false
	case o.unit != unitNone:
		if o.enx {
			at, live := cx.St.Deadline(key, cx.NowMs)
			if live && at != 0 {
				// Key already has a TTL: leave it, apply only the value.
				writeAt, keepTTL = 0, true
			}
		}
	default:
		writeAt, keepTTL = 0, true
	}
	if err := cx.St.SetString(key, val, cx.NowMs, writeAt, keepTTL); err != nil {
		if cx.ParkFull(err) {
			return false
		}
		r.Err(storeErr(err))
		return false
	}
	cx.NotifyKeyspaceEvent(shard.NotifyString, "increx", key)
	return true
}

// increxIntReply frames the two-integer reply array.
func increxIntReply(dst []byte, newVal, actual int64) []byte {
	dst = resp.AppendArrayHeader(dst, 2)
	dst = resp.AppendInt(dst, newVal)
	return resp.AppendInt(dst, actual)
}

// increxFloatReply frames the two-element float reply: RESP3 doubles, RESP2
// bulk strings, matching the mixed protocol rendering INCRBYFLOAT uses.
func increxFloatReply(dst []byte, newVal, actual float64, resp3 bool) []byte {
	dst = resp.AppendArrayHeader(dst, 2)
	dst = appendIncrexFloat(dst, newVal, resp3)
	return appendIncrexFloat(dst, actual, resp3)
}

// appendIncrexFloat renders one float element in the connection's protocol.
func appendIncrexFloat(dst []byte, v float64, resp3 bool) []byte {
	var sc [40]byte
	digits := resp.FormatScore(sc[:0], v)
	if resp3 {
		return resp.AppendDoubleBytes(dst, digits)
	}
	return resp.AppendBulk(dst, digits)
}
