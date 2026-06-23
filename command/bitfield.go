package command

import (
	"math"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// bitfieldOp is one resolved GET, SET or INCRBY sub-command of a BITFIELD call.
// The overflow mode is captured at parse time from the OVERFLOW directive in
// effect, so the executor does not need to track it.
type bitfieldOp struct {
	kind   byte // 'g' get, 's' set, 'i' incrby
	signed bool
	bits   int
	offset int64
	value  int64 // SET value or INCRBY increment
	ovMode byte  // 'w' wrap, 's' sat, 'f' fail (INCRBY only)
}

// bitfieldResult is one element of the reply array: an integer, or null for an
// INCRBY that failed under OVERFLOW FAIL.
type bitfieldResult struct {
	val  int64
	null bool
}

// bitfieldCommands returns the table for BITFIELD and its read-only twin.
func bitfieldCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "bitfield", Group: GroupBitmap, Since: "3.2.0",
			Arity: -2, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleBitField},
		{Name: "bitfield_ro", Group: GroupBitmap, Since: "6.0.0",
			Arity: -2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleBitFieldRO},
	}
}

// handleBitField implements the full BITFIELD grammar: any mix of GET, SET and
// INCRBY sub-commands, with OVERFLOW directives that affect the SET and INCRBY
// that follow them.
func handleBitField(ctx *Ctx) {
	ops, errMsg := parseBitfield(ctx.Argv[2:], false, ctx.d.protoMaxBulkLen())
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	execBitField(ctx, ctx.Argv[1], ops, hasWrite(ops))
}

// handleBitFieldRO implements BITFIELD_RO, which only accepts GET sub-commands.
func handleBitFieldRO(ctx *Ctx) {
	ops, errMsg := parseBitfield(ctx.Argv[2:], true, ctx.d.protoMaxBulkLen())
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	execBitField(ctx, ctx.Argv[1], ops, false)
}

// hasWrite reports whether any op modifies the value, so a read-only BITFIELD
// (all GET) skips the write path.
func hasWrite(ops []bitfieldOp) bool {
	for _, op := range ops {
		if op.kind != 'g' {
			return true
		}
	}
	return false
}

// parseBitfield turns the argument vector after the key into a list of ops. When
// readOnly is set, any sub-command other than GET is rejected. It returns a
// non-empty error string on a malformed command.
func parseBitfield(args [][]byte, readOnly bool, maxBulk int64) ([]bitfieldOp, string) {
	var ops []bitfieldOp
	ovMode := byte('w')
	i := 0
	for i < len(args) {
		word := strings.ToUpper(string(args[i]))
		switch word {
		case "GET":
			if i+2 >= len(args) {
				return nil, "ERR syntax error"
			}
			signed, bits, ok := parseBitfieldType(args[i+1])
			if !ok {
				return nil, bitfieldTypeError
			}
			offset, ok := parseBitfieldOffset(args[i+2], bits, maxBulk)
			if !ok {
				return nil, bitOffsetError
			}
			ops = append(ops, bitfieldOp{kind: 'g', signed: signed, bits: bits, offset: offset})
			i += 3
		case "SET", "INCRBY":
			if readOnly {
				return nil, "ERR BITFIELD_RO only supports the GET subcommand"
			}
			if i+3 >= len(args) {
				return nil, "ERR syntax error"
			}
			signed, bits, ok := parseBitfieldType(args[i+1])
			if !ok {
				return nil, bitfieldTypeError
			}
			offset, ok := parseBitfieldOffset(args[i+2], bits, maxBulk)
			if !ok {
				return nil, bitOffsetError
			}
			v, ok := parseInteger(args[i+3])
			if !ok {
				return nil, "ERR value is not an integer or out of range"
			}
			kind := byte('s')
			if word == "INCRBY" {
				kind = 'i'
			}
			ops = append(ops, bitfieldOp{kind: kind, signed: signed, bits: bits, offset: offset, value: v, ovMode: ovMode})
			i += 4
		case "OVERFLOW":
			if readOnly {
				return nil, "ERR BITFIELD_RO only supports the GET subcommand"
			}
			if i+1 >= len(args) {
				return nil, "ERR syntax error"
			}
			switch strings.ToUpper(string(args[i+1])) {
			case "WRAP":
				ovMode = 'w'
			case "SAT":
				ovMode = 's'
			case "FAIL":
				ovMode = 'f'
			default:
				return nil, "ERR Invalid OVERFLOW type specified"
			}
			i += 2
		default:
			return nil, "ERR syntax error"
		}
	}
	return ops, ""
}

const (
	bitfieldTypeError = "ERR Invalid bitfield type. Use something like i8 u16 i32 u63 i64 ..."
	bitOffsetError    = "ERR bit offset is not an integer or out of range"
)

// execBitField runs the ops against the key and writes the array reply. When
// write is false the value is only read, so it goes through the read path.
func execBitField(ctx *Ctx, key []byte, ops []bitfieldOp, write bool) {
	var (
		wrongTyp bool
		results  []bitfieldResult
	)
	run := func(db *keyspace.DB) error {
		cur, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		body := append([]byte(nil), cur...)
		modified := false
		results = make([]bitfieldResult, 0, len(ops))
		for _, op := range ops {
			switch op.kind {
			case 'g':
				results = append(results, bitfieldResult{val: bitfieldGet(body, op.signed, op.bits, op.offset)})
			case 's':
				old := bitfieldGet(body, op.signed, op.bits, op.offset)
				body = bitfieldEnsure(body, op.offset, op.bits)
				bitfieldPut(body, op.bits, op.offset, op.value)
				modified = true
				results = append(results, bitfieldResult{val: old})
			case 'i':
				old := bitfieldGet(body, op.signed, op.bits, op.offset)
				nv, ok := bitfieldAdd(old, op.value, op.signed, op.bits, op.ovMode)
				if !ok {
					results = append(results, bitfieldResult{null: true})
					continue
				}
				body = bitfieldEnsure(body, op.offset, op.bits)
				bitfieldPut(body, op.bits, op.offset, nv)
				modified = true
				results = append(results, bitfieldResult{val: nv})
			}
		}
		if write && modified {
			return db.Set(key, body, keyspace.TypeString, keyspace.EncRaw, keepTTL(hdr, found))
		}
		return nil
	}

	var ok bool
	if write {
		ok = ctx.updateShard(key, run)
	} else {
		ok = ctx.view(run)
	}
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(results))
	for _, r := range results {
		if r.null {
			enc.WriteNull()
		} else {
			enc.WriteInteger(r.val)
		}
	}
}

// parseBitfieldType reads a type specifier like u8 or i16. Unsigned widths run
// 1..63 (u64 cannot fit a signed RESP integer); signed widths run 1..64.
func parseBitfieldType(arg []byte) (signed bool, bits int, ok bool) {
	if len(arg) < 2 {
		return false, 0, false
	}
	switch arg[0] {
	case 'i', 'I':
		signed = true
	case 'u', 'U':
		signed = false
	default:
		return false, 0, false
	}
	n := 0
	for _, ch := range arg[1:] {
		if ch < '0' || ch > '9' {
			return false, 0, false
		}
		n = n*10 + int(ch-'0')
		if n > 64 {
			return false, 0, false
		}
	}
	if n < 1 {
		return false, 0, false
	}
	if signed && n > 64 {
		return false, 0, false
	}
	if !signed && n > 63 {
		return false, 0, false
	}
	return signed, n, true
}

// parseBitfieldOffset reads an offset specifier. A leading # multiplies by the
// type width for array-like access; a bare integer is an exact bit offset. The
// offset must be non-negative and keep the field within the string size limit.
func parseBitfieldOffset(arg []byte, width int, maxBulk int64) (int64, bool) {
	mult := false
	body := arg
	if len(arg) > 0 && arg[0] == '#' {
		mult = true
		body = arg[1:]
	}
	n, ok := parseInteger(body)
	if !ok || n < 0 {
		return 0, false
	}
	if mult {
		n *= int64(width)
	}
	need := (n + int64(width) + 7) / 8
	if need > maxBulk {
		return 0, false
	}
	return n, true
}

// bitfieldGet reads the N-bit field at offset, MSB-first, treating bits past the
// end of buf as 0 and sign-extending signed values to int64.
func bitfieldGet(buf []byte, signed bool, bits int, offset int64) int64 {
	var raw uint64
	for i := range bits {
		bitPos := offset + int64(i)
		byteIdx := bitPos / 8
		var b byte
		if byteIdx < int64(len(buf)) {
			b = buf[byteIdx]
		}
		bit := (b >> uint(7-bitPos%8)) & 1
		raw = (raw << 1) | uint64(bit)
	}
	if signed && bits < 64 && (raw>>(uint(bits)-1))&1 == 1 {
		raw |= ^uint64(0) << uint(bits)
	}
	return int64(raw)
}

// bitfieldEnsure grows buf so the N-bit field at offset fits, zero-padding.
func bitfieldEnsure(buf []byte, offset int64, bits int) []byte {
	need := (offset + int64(bits) + 7) / 8
	for int64(len(buf)) < need {
		buf = append(buf, 0)
	}
	return buf
}

// bitfieldPut writes the low N bits of value into the field at offset, MSB-first.
// buf must already be large enough.
func bitfieldPut(buf []byte, bits int, offset int64, value int64) {
	for i := range bits {
		bitPos := offset + int64(i)
		byteIdx := bitPos / 8
		bitInByte := uint(7 - bitPos%8)
		bit := byte((uint64(value) >> uint(bits-1-i)) & 1)
		if bit == 1 {
			buf[byteIdx] |= 1 << bitInByte
		} else {
			buf[byteIdx] &^= 1 << bitInByte
		}
	}
}

// bitfieldAdd applies an increment with the given overflow mode. It returns the
// new value and true, or false when OVERFLOW FAIL rejects the operation.
func bitfieldAdd(old, incr int64, signed bool, bits int, ovMode byte) (int64, bool) {
	sum := old + incr
	ovf := (incr > 0 && sum < old) || (incr < 0 && sum > old)

	if signed {
		var minV, maxV int64
		if bits == 64 {
			minV, maxV = math.MinInt64, math.MaxInt64
		} else {
			minV = int64(-1) << uint(bits-1)
			maxV = int64(1)<<uint(bits-1) - 1
		}
		switch ovMode {
		case 's':
			if ovf {
				if incr > 0 {
					return maxV, true
				}
				return minV, true
			}
			if sum > maxV {
				return maxV, true
			}
			if sum < minV {
				return minV, true
			}
			return sum, true
		case 'f':
			if ovf || sum > maxV || sum < minV {
				return 0, false
			}
			return sum, true
		default: // wrap
			var r uint64
			if bits == 64 {
				r = uint64(sum)
			} else {
				r = uint64(sum) & ((uint64(1) << uint(bits)) - 1)
				if (r>>(uint(bits)-1))&1 == 1 {
					r |= ^uint64(0) << uint(bits)
				}
			}
			return int64(r), true
		}
	}

	// Unsigned widths are 1..63, so maxU fits in int64.
	maxU := int64(1)<<uint(bits) - 1
	switch ovMode {
	case 's':
		if ovf && incr > 0 {
			return maxU, true
		}
		if sum > maxU {
			return maxU, true
		}
		if sum < 0 {
			return 0, true
		}
		return sum, true
	case 'f':
		if (ovf && incr > 0) || sum > maxU || sum < 0 {
			return 0, false
		}
		return sum, true
	default: // wrap
		return int64((uint64(old) + uint64(incr)) & uint64(maxU)), true
	}
}
