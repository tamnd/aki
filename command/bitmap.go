package command

import (
	"math/bits"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// maxBitOffset is the largest bit offset SETBIT and GETBIT accept, matching
// Redis's 2^32 - 1 cap that keeps a bitmap within the 512 MiB string limit.
const maxBitOffset = (1 << 32) - 1

// bitmapCommands returns the command table for the bit operations over string
// values (doc 08 §5). Bitmaps are not a separate type; these commands read and
// write the same string keys.
func bitmapCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "setbit", Group: GroupBitmap, Since: "2.2.0",
			Arity: 4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleSetBit},
		{Name: "getbit", Group: GroupBitmap, Since: "2.2.0",
			Arity: 3, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGetBit},
		{Name: "bitcount", Group: GroupBitmap, Since: "2.6.0",
			Arity: -2, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleBitCount},
		{Name: "bitpos", Group: GroupBitmap, Since: "2.8.7",
			Arity: -3, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleBitPos},
		{Name: "bitop", Group: GroupBitmap, Since: "2.6.0",
			Arity: -4, Flags: FlagWrite | FlagDenyOOM, FirstKey: 2, LastKey: -1, Step: 1,
			Handler: handleBitOp},
	}
}

// handleSetBit implements SETBIT key offset value: set or clear a single bit,
// zero-extending the string when the offset is past its current end, and return
// the bit's previous value. Bit 0 is the most significant bit of byte 0.
func handleSetBit(ctx *Ctx) {
	key := ctx.Argv[1]
	offset, ok := parseInteger(ctx.Argv[2])
	if !ok || offset < 0 || offset > maxBitOffset {
		ctx.enc().WriteError("ERR bit offset is not an integer or out of range")
		return
	}
	bitVal, ok := parseInteger(ctx.Argv[3])
	if !ok || (bitVal != 0 && bitVal != 1) {
		ctx.enc().WriteError("ERR bit is not an integer or it is out of range")
		return
	}
	byteIdx := offset / 8
	bitPos := uint(7 - offset%8)
	if byteIdx+1 > ctx.d.protoMaxBulkLen() {
		ctx.enc().WriteError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
		return
	}

	var (
		wrongTyp bool
		old      int64
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		cur, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		size := max(int(byteIdx)+1, len(cur))
		body := make([]byte, size)
		copy(body, cur)
		old = int64((body[byteIdx] >> bitPos) & 1)
		if bitVal == 1 {
			body[byteIdx] |= 1 << bitPos
		} else {
			body[byteIdx] &^= 1 << bitPos
		}
		return db.Set(key, body, keyspace.TypeString, keyspace.EncRaw, keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(old)
}

// handleGetBit implements GETBIT key offset: return the bit at offset, or 0 when
// the key is missing or the offset is past the string's end. It never extends
// the string.
func handleGetBit(ctx *Ctx) {
	key := ctx.Argv[1]
	offset, ok := parseInteger(ctx.Argv[2])
	if !ok || offset < 0 || offset > maxBitOffset {
		ctx.enc().WriteError("ERR bit offset is not an integer or out of range")
		return
	}
	var (
		wrongTyp bool
		bit      int64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		cur, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		byteIdx := int(offset / 8)
		if byteIdx < len(cur) {
			bit = int64((cur[byteIdx] >> uint(7-offset%8)) & 1)
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(bit)
}

// handleBitCount implements BITCOUNT key [start end [BYTE | BIT]]: count the set
// bits, optionally within a byte or bit range using Redis negative-index rules.
func handleBitCount(ctx *Ctx) {
	key := ctx.Argv[1]
	args := ctx.Argv[2:]
	if len(args) == 1 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	var (
		start, end int64
		bitUnit    bool
		ranged     bool
	)
	if len(args) >= 2 {
		s, ok1 := parseInteger(args[0])
		e, ok2 := parseInteger(args[1])
		if !ok1 || !ok2 {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		start, end, ranged = s, e, true
		if len(args) == 3 {
			unit, ok := parseBitUnit(args[2])
			if !ok {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			bitUnit = unit
		} else if len(args) > 3 {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	var (
		wrongTyp bool
		count    int64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		cur, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		count = bitCount(cur, start, end, ranged, bitUnit)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(count)
}

// bitCount counts the set bits of buf, over the whole string when ranged is
// false, or within the inclusive [start, end] range in byte or bit units.
func bitCount(buf []byte, start, end int64, ranged, bitUnit bool) int64 {
	if len(buf) == 0 {
		return 0
	}
	if !ranged {
		var c int64
		for _, b := range buf {
			c += int64(bits.OnesCount8(b))
		}
		return c
	}
	if bitUnit {
		s, e, ok := clampRange(start, end, int64(len(buf))*8)
		if !ok {
			return 0
		}
		var c int64
		for i := s; i <= e; i++ {
			c += int64((buf[i/8] >> uint(7-i%8)) & 1)
		}
		return c
	}
	s, e, ok := clampRange(start, end, int64(len(buf)))
	if !ok {
		return 0
	}
	var c int64
	for _, b := range buf[s : e+1] {
		c += int64(bits.OnesCount8(b))
	}
	return c
}

// handleBitPos implements BITPOS key bit [start [end [BYTE | BIT]]]: find the
// first bit set to the target value, honoring Redis's tricky "end given vs not
// given" rule when searching for a 0 bit.
func handleBitPos(ctx *Ctx) {
	key := ctx.Argv[1]
	target, ok := parseInteger(ctx.Argv[2])
	if !ok || (target != 0 && target != 1) {
		ctx.enc().WriteError("ERR bit is not an integer or it is out of range")
		return
	}
	args := ctx.Argv[3:]
	var (
		start, end int64
		endGiven   bool
		bitUnit    bool
	)
	if len(args) >= 1 {
		v, ok := parseInteger(args[0])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		start = v
	}
	if len(args) >= 2 {
		v, ok := parseInteger(args[1])
		if !ok {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		end, endGiven = v, true
	}
	if len(args) == 3 {
		unit, ok := parseBitUnit(args[2])
		if !ok {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		bitUnit = unit
	} else if len(args) > 3 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}

	var (
		wrongTyp bool
		pos      int64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		cur, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		pos = bitPos(cur, target, start, end, endGiven, bitUnit)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(pos)
}

// bitPos finds the first bit equal to target. An absent or empty string returns
// 0 when searching for a 0 bit and -1 when searching for a 1 bit. When no match
// is found in the range, searching for a 1 returns -1, and searching for a 0
// returns the bit just past the string when no explicit end was given, or -1
// when one was.
func bitPos(buf []byte, target, start, end int64, endGiven, bitUnit bool) int64 {
	totalBits := int64(len(buf)) * 8
	if totalBits == 0 {
		if target == 0 {
			return 0
		}
		return -1
	}

	var startBit, endBit int64
	if bitUnit {
		startBit = start
		endBit = totalBits - 1
		if endGiven {
			endBit = end
		}
	} else {
		startBit = start * 8
		endBit = totalBits - 1
		if endGiven {
			endBit = end*8 + 7
		}
	}

	// Normalize negative indices against the search domain.
	size := totalBits
	if !bitUnit {
		size = int64(len(buf))
	}
	if start < 0 {
		if bitUnit {
			startBit = start + totalBits
		} else {
			startBit = (start + size) * 8
		}
	}
	if endGiven && end < 0 {
		if bitUnit {
			endBit = end + totalBits
		} else {
			endBit = (end+size)*8 + 7
		}
	}
	if startBit < 0 {
		startBit = 0
	}
	if endBit >= totalBits {
		endBit = totalBits - 1
	}

	for i := startBit; i <= endBit; i++ {
		bit := int64((buf[i/8] >> uint(7-i%8)) & 1)
		if bit == target {
			return i
		}
	}
	if target == 1 {
		return -1
	}
	if endGiven {
		return -1
	}
	return totalBits
}

// handleBitOp implements BITOP operation destkey key [key ...]: a bitwise AND,
// OR, XOR or NOT across the source strings, stored in destkey, returning the
// destination length. Missing sources are zero-length; the result is raw.
func handleBitOp(ctx *Ctx) {
	op := strings.ToUpper(string(ctx.Argv[1]))
	switch op {
	case "AND", "OR", "XOR", "NOT":
	default:
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	dest := ctx.Argv[2]
	srcKeys := ctx.Argv[3:]
	if op == "NOT" && len(srcKeys) != 1 {
		ctx.enc().WriteError("ERR BITOP NOT must be called with a single source key.")
		return
	}

	var (
		wrongTyp bool
		destLen  int64
	)
	done := ctx.update(func(db *keyspace.DB) error {
		bufs := make([][]byte, len(srcKeys))
		maxLen := 0
		for i, k := range srcKeys {
			b, hdr, found, err := db.Get(k)
			if err != nil {
				return err
			}
			if found && hdr.Type != keyspace.TypeString {
				wrongTyp = true
				return nil
			}
			bufs[i] = b
			if len(b) > maxLen {
				maxLen = len(b)
			}
		}
		if maxLen == 0 {
			_, err := db.Delete(dest)
			return err
		}
		result := bitOp(op, bufs, maxLen)
		destLen = int64(len(result))
		return db.Set(dest, result, keyspace.TypeString, keyspace.EncRaw, -1)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(destLen)
}

// bitOp applies op byte by byte across bufs, treating a shorter source as
// zero-padded on the right.
func bitOp(op string, bufs [][]byte, size int) []byte {
	result := make([]byte, size)
	for i := range result {
		switch op {
		case "AND":
			acc := byte(0xFF)
			for _, b := range bufs {
				if i < len(b) {
					acc &= b[i]
				} else {
					acc = 0
				}
			}
			result[i] = acc
		case "OR":
			var acc byte
			for _, b := range bufs {
				if i < len(b) {
					acc |= b[i]
				}
			}
			result[i] = acc
		case "XOR":
			var acc byte
			for _, b := range bufs {
				if i < len(b) {
					acc ^= b[i]
				}
			}
			result[i] = acc
		case "NOT":
			if i < len(bufs[0]) {
				result[i] = ^bufs[0][i]
			} else {
				result[i] = 0xFF
			}
		}
	}
	return result
}

// parseBitUnit reads the BYTE or BIT unit word, returning true for BIT.
func parseBitUnit(arg []byte) (bitUnit bool, ok bool) {
	switch strings.ToUpper(string(arg)) {
	case "BYTE":
		return false, true
	case "BIT":
		return true, true
	default:
		return false, false
	}
}

// clampRange normalizes an inclusive [start, end] over a domain of size units
// with Redis negative indexing, and reports whether the range is non-empty.
func clampRange(start, end, size int64) (int64, int64, bool) {
	if start < 0 {
		start += size
	}
	if end < 0 {
		end += size
	}
	if start < 0 {
		start = 0
	}
	if end >= size {
		end = size - 1
	}
	if size == 0 || start > end || start >= size {
		return 0, 0, false
	}
	return start, end, true
}
