package derived

// BITFIELD (spec 2064/f3/15 section 4.4) treats the string as a packed array of
// u1..u63 and i1..i64 fields and runs GET/SET/INCRBY sub-ops with OVERFLOW
// WRAP/SAT/FAIL semantics, the exact Redis grammar. A field spans at most nine
// bytes, so each sub-op touches one or two chunks through the store's FieldGet
// and FieldSet. Atomicity is free under F1: the owner runs the sub-ops in order
// on a value nobody else can observe mid-command. BITFIELD_RO is the GET-only
// replica-safe subset.

import (
	"math"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/obs1srv/resp"
)

const (
	owWrap = iota
	owSat
	owFail
)

const (
	bfGet = iota
	bfSet
	bfIncrby
)

const (
	errBitfieldType = "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
	errOverflowType = "ERR Invalid OVERFLOW type specified"
	errBitfieldRO   = "ERR BITFIELD_RO only supports the GET subcommand"
)

// upper ASCII-uppercases a short command token to a string for the sub-op switch.
func upper(b []byte) string {
	buf := make([]byte, len(b))
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		buf[i] = c
	}
	return string(buf)
}

// bfOp is one parsed sub-op: its opcode, the field type, the resolved absolute
// bit offset, the SET value or INCRBY increment, and the overflow mode in effect
// when it was parsed (OVERFLOW changes the mode for the sub-ops that follow it).
type bfOp struct {
	kind   int
	signed bool
	bits   uint
	off    int64
	arg    int64
	owtype int
}

// BitField answers BITFIELD, the read-write form.
func BitField(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	bitField(cx, args, r, false)
}

// BitFieldRO answers BITFIELD_RO, which rejects everything but GET so a replica
// can serve it.
func BitFieldRO(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	bitField(cx, args, r, true)
}

// bitField parses every sub-op first (so a syntax error in a later op applies no
// write), then executes them in order, building the array reply: one integer per
// GET/SET/INCRBY, a null for a FAIL-mode overflow, nothing for OVERFLOW.
func bitField(cx *shard.Ctx, args [][]byte, r shard.Reply, ro bool) {
	key := args[0]
	ops := make([]bfOp, 0, 4)
	ow := owWrap
	i := 1
	for i < len(args) {
		switch up := upper(args[i]); up {
		case "GET":
			if i+2 >= len(args) {
				r.Err(errSyntax)
				return
			}
			signed, bits, ok := parseFieldType(args[i+1])
			if !ok {
				r.Err(errBitfieldType)
				return
			}
			off, ok := parseFieldOffset(args[i+2], bits)
			if !ok {
				r.Err(errBitOffset)
				return
			}
			ops = append(ops, bfOp{kind: bfGet, signed: signed, bits: bits, off: off, owtype: ow})
			i += 3
		case "SET", "INCRBY":
			if ro {
				r.Err(errBitfieldRO)
				return
			}
			if i+3 >= len(args) {
				r.Err(errSyntax)
				return
			}
			signed, bits, ok := parseFieldType(args[i+1])
			if !ok {
				r.Err(errBitfieldType)
				return
			}
			off, ok := parseFieldOffset(args[i+2], bits)
			if !ok {
				r.Err(errBitOffset)
				return
			}
			arg, ok := store.ParseInt(args[i+3])
			if !ok {
				r.Err(errNotInt)
				return
			}
			kind := bfSet
			if up == "INCRBY" {
				kind = bfIncrby
			}
			ops = append(ops, bfOp{kind: kind, signed: signed, bits: bits, off: off, arg: arg, owtype: ow})
			i += 4
		case "OVERFLOW":
			if ro {
				r.Err(errBitfieldRO)
				return
			}
			if i+1 >= len(args) {
				r.Err(errSyntax)
				return
			}
			switch upper(args[i+1]) {
			case "WRAP":
				ow = owWrap
			case "SAT":
				ow = owSat
			case "FAIL":
				ow = owFail
			default:
				r.Err(errOverflowType)
				return
			}
			i += 2
		default:
			r.Err(errSyntax)
			return
		}
	}

	out := resp.AppendArrayHeader(cx.Aux[:0], len(ops))
	for _, op := range ops {
		switch op.kind {
		case bfGet:
			out = resp.AppendInt(out, readField(cx, key, op))
		case bfSet:
			old := readField(cx, key, op)
			newVal, overflow := applyOverflow(op, 0, op.arg)
			if op.owtype == owFail && overflow {
				out = resp.AppendNull(out)
				break
			}
			writeField(cx, key, op, newVal)
			out = resp.AppendInt(out, old)
		case bfIncrby:
			cur := readField(cx, key, op)
			newVal, overflow := applyOverflow(op, cur, op.arg)
			if op.owtype == owFail && overflow {
				out = resp.AppendNull(out)
				break
			}
			writeField(cx, key, op, newVal)
			out = resp.AppendInt(out, newVal)
		}
	}
	cx.Aux = out
	r.Raw(out)
}

// readField reads op's field and applies the signed or unsigned reading, so the
// returned int64 is the field's value as the type sees it (u63 fits int64, i64
// is native two's complement).
func readField(cx *shard.Ctx, key []byte, op bfOp) int64 {
	raw := cx.St.FieldGet(key, op.off, op.bits, cx.NowMs)
	if op.signed && op.bits < 64 && raw&(uint64(1)<<(op.bits-1)) != 0 {
		raw |= (^uint64(0)) << op.bits
	}
	return int64(raw)
}

// writeField writes the low op.bits bits of newVal into op's field.
func writeField(cx *shard.Ctx, key []byte, op bfOp, newVal int64) {
	_ = cx.St.FieldSet(key, op.off, op.bits, uint64(newVal), cx.NowMs)
}

// applyOverflow computes cur+incr in op's field type under its overflow mode,
// returning the value to store (already wrapped or saturated) and whether an
// overflow occurred. For SET, cur is 0 and incr is the value to set, so this
// clamps or wraps a too-wide SET value the same way an INCRBY overflow does.
func applyOverflow(op bfOp, cur, incr int64) (int64, bool) {
	if op.signed {
		dir, limit := checkSignedOverflow(cur, incr, op.bits, op.owtype)
		if dir != 0 {
			return limit, true
		}
		return cur + incr, false
	}
	dir, limit := checkUnsignedOverflow(uint64(cur), incr, op.bits, op.owtype)
	if dir != 0 {
		return int64(limit), true
	}
	return int64(uint64(cur) + uint64(incr)), false
}

// checkSignedOverflow reports the overflow direction (0 none, 1 high, -1 low) of
// value+incr in a bits-wide signed field and, for WRAP or SAT, the result to
// store. Ported behavior-for-behavior from Redis checkSignedBitfieldOverflow.
func checkSignedOverflow(value, incr int64, bits uint, owtype int) (int, int64) {
	var max int64
	if bits == 64 {
		max = math.MaxInt64
	} else {
		max = (int64(1) << (bits - 1)) - 1
	}
	min := (-max) - 1
	maxincr := max - value
	minincr := min - value
	if value > max || (bits != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr) {
		switch owtype {
		case owWrap:
			return 1, signedWrap(value, incr, bits)
		case owSat:
			return 1, max
		}
		return 1, 0
	} else if value < min || (bits != 64 && incr < minincr) || (value < 0 && incr < 0 && incr < minincr) {
		switch owtype {
		case owWrap:
			return -1, signedWrap(value, incr, bits)
		case owSat:
			return -1, min
		}
		return -1, 0
	}
	return 0, 0
}

// signedWrap adds value+incr as unsigned and sign-extends the bits-wide result,
// two's-complement wraparound.
func signedWrap(value, incr int64, bits uint) int64 {
	c := uint64(value) + uint64(incr)
	if bits < 64 {
		msb := uint64(1) << (bits - 1)
		mask := (^uint64(0)) << bits
		if c&msb != 0 {
			c |= mask
		} else {
			c &^= mask
		}
	}
	return int64(c)
}

// checkUnsignedOverflow is the unsigned analog, ported from Redis
// checkUnsignedBitfieldOverflow. Unsigned fields cap at u63, so value and the
// increments stay inside int64.
func checkUnsignedOverflow(value uint64, incr int64, bits uint, owtype int) (int, uint64) {
	var max uint64
	if bits == 64 {
		max = math.MaxUint64
	} else {
		max = (uint64(1) << bits) - 1
	}
	maxincr := int64(max - value)
	minincr := -int64(value)
	if value > max || (incr > 0 && incr > maxincr) {
		switch owtype {
		case owWrap:
			return 1, unsignedWrap(value, incr, bits)
		case owSat:
			return 1, max
		}
		return 1, 0
	} else if incr < 0 && incr < minincr {
		if owtype == owWrap {
			return -1, unsignedWrap(value, incr, bits)
		}
		return -1, 0
	}
	return 0, 0
}

// unsignedWrap adds and masks to the low bits, modular wraparound.
func unsignedWrap(value uint64, incr int64, bits uint) uint64 {
	res := value + uint64(incr)
	if bits < 64 {
		res &^= (^uint64(0)) << bits
	}
	return res
}

// parseFieldType parses an iN or uN type token: 'i' fields carry 1..64 bits, 'u'
// fields 1..63 (u64 will not fit a RESP integer reply, exactly the Redis cap).
func parseFieldType(tok []byte) (signed bool, bits uint, ok bool) {
	if len(tok) < 2 {
		return false, 0, false
	}
	switch tok[0] {
	case 'i':
		signed = true
	case 'u':
		signed = false
	default:
		return false, 0, false
	}
	n, ok := store.ParseInt(tok[1:])
	if !ok || n < 1 {
		return false, 0, false
	}
	if signed && n > 64 {
		return false, 0, false
	}
	if !signed && n > 63 {
		return false, 0, false
	}
	return signed, uint(n), true
}

// parseFieldOffset parses an offset token: a plain bit offset, or a '#'-prefixed
// field index that multiplies by the type width. The covering bytes must stay
// under the 512MiB bitmap cap, the same ceiling SETBIT enforces.
func parseFieldOffset(tok []byte, bits uint) (int64, bool) {
	raw := tok
	scaled := false
	if len(tok) > 0 && tok[0] == '#' {
		raw = tok[1:]
		scaled = true
	}
	n, ok := store.ParseInt(raw)
	if !ok || n < 0 {
		return 0, false
	}
	off := n
	if scaled {
		off = n * int64(bits)
	}
	if off < 0 || off+int64(bits)-1 > maxBitOffset {
		return 0, false
	}
	return off, true
}
