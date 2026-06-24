package command

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/keyspace"
)

// handleIncr implements INCR key: add 1 to the integer value, treating a missing
// key as 0.
func handleIncr(ctx *Ctx) { incrBy(ctx, 1) }

// handleDecr implements DECR key: subtract 1 from the integer value.
func handleDecr(ctx *Ctx) { incrBy(ctx, -1) }

// handleIncrBy implements INCRBY key increment.
func handleIncrBy(ctx *Ctx) {
	delta, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	incrBy(ctx, delta)
}

// handleDecrBy implements DECRBY key decrement. The decrement is negated, so a
// decrement of the smallest int64 cannot be represented and is an overflow.
func handleDecrBy(ctx *Ctx) {
	delta, ok := parseInteger(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	if delta == math.MinInt64 {
		ctx.enc().WriteError("ERR increment or decrement would overflow")
		return
	}
	incrBy(ctx, -delta)
}

// incrBy is the shared body of the integer counter commands. It reads the
// current value as an integer (0 when absent), adds delta with an overflow
// check, stores the result with the int encoding, preserves any TTL, and replies
// with the new value.
func incrBy(ctx *Ctx, delta int64) {
	key := ctx.Argv[1]
	var (
		wrongTyp bool
		notInt   bool
		overflow bool
		result   int64
	)
	done := ctx.rmwWriteBehind(key, func(b []byte, hdr keyspace.ValueHeader, found bool) rmwResult {
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return rmwResult{}
		}
		var cur int64
		if found {
			v, ok := parseInteger(b)
			if !ok {
				notInt = true
				return rmwResult{}
			}
			cur = v
		}
		sum, ok := addInt64(cur, delta)
		if !ok {
			overflow = true
			return rmwResult{}
		}
		result = sum
		body := strconv.AppendInt(nil, sum, 10)
		return rmwResult{body: body, typ: keyspace.TypeString, enc: keyspace.EncInt, ttlMs: keepTTL(hdr, found), write: true}
	}, nil)
	if !done {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notInt:
		ctx.enc().WriteError("ERR value is not an integer or out of range")
	case overflow:
		ctx.enc().WriteError("ERR increment or decrement would overflow")
	default:
		ctx.notify(notifyString, "incrby", key)
		ctx.enc().WriteInteger(result)
	}
}

// handleIncrByFloat implements INCRBYFLOAT key increment: add a float to the
// value (0 when absent), store the formatted result back as a string, and reply
// with it. The stored value's encoding is re-evaluated, so a whole-number result
// becomes the int encoding.
func handleIncrByFloat(ctx *Ctx) {
	key := ctx.Argv[1]
	incr, ok := parseFloat(ctx.Argv[2])
	if !ok {
		ctx.enc().WriteError("ERR value is not a float or out of range")
		return
	}
	var (
		wrongTyp bool
		notFloat bool
		nanInf   bool
		result   string
	)
	done := ctx.rmwWriteBehind(key, func(b []byte, hdr keyspace.ValueHeader, found bool) rmwResult {
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return rmwResult{}
		}
		var cur float64
		if found {
			v, ok := parseFloat(b)
			if !ok {
				notFloat = true
				return rmwResult{}
			}
			cur = v
		}
		sum := cur + incr
		if math.IsNaN(sum) || math.IsInf(sum, 0) {
			nanInf = true
			return rmwResult{}
		}
		result = formatFloat(sum)
		body := []byte(result)
		return rmwResult{body: body, typ: keyspace.TypeString, enc: stringEncoding(body), ttlMs: keepTTL(hdr, found), write: true}
	}, nil)
	if !done {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notFloat:
		ctx.enc().WriteError("ERR value is not a float or out of range")
	case nanInf:
		ctx.enc().WriteError("ERR increment would produce NaN or Infinity")
	default:
		ctx.notify(notifyString, "incrbyfloat", key)
		ctx.enc().WriteBulkStringStr(result)
	}
}
