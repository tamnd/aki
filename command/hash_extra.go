package command

import (
	"math"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// hashExtraCommands returns the hash counter and random commands: HINCRBY,
// HINCRBYFLOAT and HRANDFIELD (doc 10 §4.2). HSCAN waits for the generic SCAN
// cursor machinery.
func hashExtraCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "hincrby", Group: GroupHash, Since: "2.0.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHIncrBy},
		{Name: "hincrbyfloat", Group: GroupHash, Since: "2.6.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHIncrByFloat},
		{Name: "hrandfield", Group: GroupHash, Since: "6.2.0",
			Arity: -2, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleHRandField},
	}
}

// handleHIncrBy implements HINCRBY: add an integer to a field, treating a missing
// field as 0, and reply with the new value.
func handleHIncrBy(ctx *Ctx) {
	key, field := ctx.Argv[1], ctx.Argv[2]
	delta, ok := parseInteger(ctx.Argv[3])
	if !ok {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}
	var (
		wrongTyp bool
		notInt   bool
		overflow bool
		result   int64
	)
	done := ctx.update(func(db *keyspace.DB) error {
		fields, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		var cur int64
		idx := hashFind(fields, field)
		if idx >= 0 {
			v, ok := parseInteger(fields[idx].value)
			if !ok {
				notInt = true
				return nil
			}
			cur = v
		}
		sum, ok := addInt64(cur, delta)
		if !ok {
			overflow = true
			return nil
		}
		result = sum
		body := strconv.AppendInt(nil, sum, 10)
		if idx >= 0 {
			fields[idx].value = body
		} else {
			fields = append(fields, hashField{field: field, value: body})
		}
		prev := uint8(keyspace.EncListpack)
		if found {
			prev = hdr.Encoding
		}
		return db.Set(key, hashEncode(fields), keyspace.TypeHash, hashEncoding(ctx.encLimits(), fields, prev), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notInt:
		ctx.enc().WriteError("ERR hash value is not an integer")
	case overflow:
		ctx.enc().WriteError("ERR increment or decrement would overflow")
	default:
		ctx.notify(notifyHash, "hincrby", key)
		ctx.enc().WriteInteger(result)
	}
}

// handleHIncrByFloat implements HINCRBYFLOAT: add a float to a field, treating a
// missing field as 0, and reply with the formatted new value.
func handleHIncrByFloat(ctx *Ctx) {
	key, field := ctx.Argv[1], ctx.Argv[2]
	incr, ok := parseFloat(ctx.Argv[3])
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
	done := ctx.update(func(db *keyspace.DB) error {
		fields, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		var cur float64
		idx := hashFind(fields, field)
		if idx >= 0 {
			v, ok := parseFloat(fields[idx].value)
			if !ok {
				notFloat = true
				return nil
			}
			cur = v
		}
		sum := cur + incr
		if math.IsNaN(sum) || math.IsInf(sum, 0) {
			nanInf = true
			return nil
		}
		result = formatFloat(sum)
		body := []byte(result)
		if idx >= 0 {
			fields[idx].value = body
		} else {
			fields = append(fields, hashField{field: field, value: body})
		}
		prev := uint8(keyspace.EncListpack)
		if found {
			prev = hdr.Encoding
		}
		return db.Set(key, hashEncode(fields), keyspace.TypeHash, hashEncoding(ctx.encLimits(), fields, prev), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notFloat:
		ctx.enc().WriteError("ERR hash value is not a float")
	case nanInf:
		ctx.enc().WriteError("ERR increment would produce NaN or Infinity")
	default:
		ctx.notify(notifyHash, "hincrbyfloat", key)
		ctx.enc().WriteBulkStringStr(result)
	}
}

// handleHRandField implements HRANDFIELD key [count [WITHVALUES]]. Without a
// count it replies one random field or nil. A positive count gives distinct
// fields, a negative count gives that many with duplicates allowed.
func handleHRandField(ctx *Ctx) {
	key := ctx.Argv[1]
	hasCount := false
	withValues := false
	var count int64
	switch len(ctx.Argv) {
	case 2:
	case 3, 4:
		c, ok := parseInteger(ctx.Argv[2])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		count = c
		hasCount = true
		if len(ctx.Argv) == 4 {
			if !strings.EqualFold(string(ctx.Argv[3]), "WITHVALUES") {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			withValues = true
		}
	default:
		ctx.enc().WriteError("ERR syntax error")
		return
	}

	var (
		wrongTyp bool
		fields   []hashField
	)
	if !ctx.view(func(db *keyspace.DB) error {
		fs, hdr, found, err := getHash(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeHash {
			wrongTyp = true
			return nil
		}
		fields = fs
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()

	if !hasCount {
		if len(fields) == 0 {
			enc.WriteNull()
			return
		}
		enc.WriteBulkString(fields[rand.IntN(len(fields))].field)
		return
	}

	picks := hashRandIndices(len(fields), count)
	if withValues {
		enc.WriteMapLen(len(picks))
		for _, i := range picks {
			enc.WriteBulkString(fields[i].field)
			enc.WriteBulkString(fields[i].value)
		}
		return
	}
	enc.WriteArrayLen(len(picks))
	for _, i := range picks {
		enc.WriteBulkString(fields[i].field)
	}
}

// hashRandIndices returns the indices to return for HRANDFIELD. A positive count
// gives distinct indices, capped at n. A negative count gives exactly its
// magnitude, with repeats allowed. An empty hash gives no indices.
func hashRandIndices(n int, count int64) []int {
	if n == 0 {
		return nil
	}
	if count < 0 {
		m := int(-count)
		out := make([]int, m)
		for i := range out {
			out[i] = rand.IntN(n)
		}
		return out
	}
	m := int(min(count, int64(n)))
	perm := rand.Perm(n)
	return perm[:m]
}
