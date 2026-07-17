package derived

// Bitmaps (spec 2064/f3/15 section 2) are a bit-level view over the string
// store: SETBIT and GETBIT run on the same keyspace SET and GET use, with no
// distinct value type, so a value written by SET is readable bit by bit and a
// bitmap is readable whole by GET.

import (
	"bytes"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// maxBitOffset is the highest legal bit offset: byte index offset>>3 must stay
// under 2^29 (the proto-max-bulk-len value ceiling), so the offset caps at
// 2^32-1, the same wire limit Redis enforces for SETBIT and GETBIT.
const maxBitOffset = (1 << 32) - 1

const (
	errBitOffset = "ERR bit offset is not an integer or out of range"
	errBitValue  = "ERR bit is not an integer or out of range"
	errSyntax    = "ERR syntax error"
	errNotInt    = "ERR value is not an integer or out of range"
	errBitArg    = "ERR The bit argument must be 1 or 0."
)

// SetBit answers SETBIT key offset value: set the addressed bit and reply with
// its previous value. The offset is validated against the 4Gib bit ceiling and
// the value against 0/1 before any write, so a bad argument never grows the
// key.
func SetBit(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	offset, ok := store.ParseInt(args[1])
	if !ok || offset < 0 || offset > maxBitOffset {
		r.Err(errBitOffset)
		return
	}
	bit, ok := store.ParseInt(args[2])
	if !ok || (bit != 0 && bit != 1) {
		r.Err(errBitValue)
		return
	}
	old, err := cx.St.SetBit(args[0], offset, int(bit), cx.NowMs)
	if err != nil {
		r.Err("ERR " + err.Error())
		return
	}
	// The frame carries the whole resulting value (post-decision effects,
	// doc 04 section 2), the same read-back APPEND and SETRANGE pay; the
	// write grew the key to cover the offset, so the key is always live here.
	if err := cx.LogStrReadBack(args[0]); err != nil {
		r.Err(err.Error())
		return
	}
	r.Int(int64(old))
}

// GetBit answers GETBIT key offset: the addressed bit, 0 past the end or on a
// missing key. A read never grows the value, so an offset past the current
// length answers 0 from metadata without touching data, and the offset ceiling
// is the SETBIT ceiling.
func GetBit(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	offset, ok := store.ParseInt(args[1])
	if !ok || offset < 0 || offset > maxBitOffset {
		r.Err(errBitOffset)
		return
	}
	r.Int(int64(cx.St.GetBit(args[0], offset, cx.NowMs)))
}

// resolveIndex resolves a Redis start/end pair (either may be negative,
// counting back from the end) into an inclusive [lo, hi] within [0, size), or
// reports an empty range. It is the string-index clamping BITCOUNT and BITPOS
// share, over bytes for the default unit and over bits for the BIT unit.
func resolveIndex(start, end, size int64) (lo, hi int64, ok bool) {
	if start < 0 {
		start += size
	}
	if end < 0 {
		end += size
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= size {
		end = size - 1
	}
	if start > end || size == 0 {
		return 0, 0, false
	}
	return start, end, true
}

// bitRangeMasks turns an inclusive bit range [bitLo, bitHi] into the covering
// byte range and the two boundary masks the store kernel takes: firstMask keeps
// the bits from position bitLo&7 to the end of its byte, lastMask keeps
// positions 0..bitHi&7, both MSB-first per the wire bit contract.
func bitRangeMasks(bitLo, bitHi int64) (lo, hi int64, firstMask, lastMask byte) {
	lo = bitLo >> 3
	hi = bitHi >> 3
	firstMask = byte(0xFF) >> uint(bitLo&7)
	lastMask = byte(0xFF) << uint(7-(bitHi&7))
	return
}

// BitCount answers BITCOUNT key [start end [BYTE|BIT]]: the set bits in the
// value, or in a start/end range measured in bytes by default or bits with the
// BIT unit. A missing key or an empty range answers 0. The count runs on the
// chunk-streamed word kernel and never copies the value whole.
func BitCount(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	n := len(args)
	if n != 1 && n != 3 && n != 4 {
		r.Err(errSyntax)
		return
	}
	total, _ := cx.St.StrLen(args[0], cx.NowMs)
	if total == 0 {
		r.Int(0)
		return
	}
	if n == 1 {
		r.Int(cx.St.BitCount(args[0], 0, total-1, 0xFF, 0xFF, cx.NowMs))
		return
	}
	start, ok := store.ParseInt(args[1])
	if !ok {
		r.Err(errNotInt)
		return
	}
	end, ok := store.ParseInt(args[2])
	if !ok {
		r.Err(errNotInt)
		return
	}
	bitUnit := false
	if n == 4 {
		switch {
		case bytes.EqualFold(args[3], []byte("BYTE")):
		case bytes.EqualFold(args[3], []byte("BIT")):
			bitUnit = true
		default:
			r.Err(errSyntax)
			return
		}
	}
	if bitUnit {
		lo, hi, ok := resolveIndex(start, end, total*8)
		if !ok {
			r.Int(0)
			return
		}
		bLo, bHi, fm, lm := bitRangeMasks(lo, hi)
		r.Int(cx.St.BitCount(args[0], bLo, bHi, fm, lm, cx.NowMs))
		return
	}
	lo, hi, ok := resolveIndex(start, end, total)
	if !ok {
		r.Int(0)
		return
	}
	r.Int(cx.St.BitCount(args[0], lo, hi, 0xFF, 0xFF, cx.NowMs))
}

// BitPos answers BITPOS key bit [start [end [BYTE|BIT]]]: the offset of the
// first bit set to bit, searching bytes by default or bits with the BIT unit.
// A missing key answers -1 for a set-bit search and 0 for a clear-bit search.
// When the search runs to the natural end of the value for a clear bit and none
// is found, the answer is the first bit past the value, matching Redis; an
// explicit end suppresses that and answers -1.
func BitPos(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	n := len(args)
	if n < 2 || n > 5 {
		r.Err(errSyntax)
		return
	}
	bit, ok := store.ParseInt(args[1])
	if !ok || (bit != 0 && bit != 1) {
		r.Err(errBitArg)
		return
	}
	total, _ := cx.St.StrLen(args[0], cx.NowMs)
	if total == 0 {
		if bit == 0 {
			r.Int(0)
		} else {
			r.Int(-1)
		}
		return
	}
	haveEnd := n >= 4
	bitUnit := false
	if n == 5 {
		switch {
		case bytes.EqualFold(args[4], []byte("BYTE")):
		case bytes.EqualFold(args[4], []byte("BIT")):
			bitUnit = true
		default:
			r.Err(errSyntax)
			return
		}
	}
	size := total
	if bitUnit {
		size = total * 8
	}
	var start, end int64
	if n >= 3 {
		start, ok = store.ParseInt(args[2])
		if !ok {
			r.Err(errNotInt)
			return
		}
	}
	if haveEnd {
		end, ok = store.ParseInt(args[3])
		if !ok {
			r.Err(errNotInt)
			return
		}
	} else {
		end = size - 1
	}
	lo, hi, ok := resolveIndex(start, end, size)
	if !ok {
		r.Int(-1)
		return
	}
	var pos int64
	if bitUnit {
		bLo, bHi, fm, lm := bitRangeMasks(lo, hi)
		pos = cx.St.BitPos(args[0], int(bit), bLo, bHi, fm, lm, cx.NowMs)
	} else {
		pos = cx.St.BitPos(args[0], int(bit), lo, hi, 0xFF, 0xFF, cx.NowMs)
	}
	if pos < 0 && bit == 0 && !haveEnd {
		// The range ran to the end of the value and every bit in it was set:
		// the first clear bit is the one just past the value.
		pos = total * 8
	}
	r.Int(pos)
}
