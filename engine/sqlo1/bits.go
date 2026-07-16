package sqlo1

import "context"

// The bit surface: SETBIT, GETBIT, and BITFIELD over the string
// ladder (doc 05 section 3.1). Every operator here reads and writes
// through a byte span of at most nine bytes (a 64-bit field starting
// mid-byte), and every write lands through SetRange, which already
// owns growth, the rope boundary crossing, lazy zero-fill, and the
// TTL restamp rule. That routing is what keeps a far-offset SETBIT
// O(1) chunks: the gap SetRange opens has no records, and the bit's
// chunk is the only one written.

// bitSpanMax is the widest byte span a bit operator can address: a
// 64-bit field whose offset is not byte-aligned straddles nine bytes.
const bitSpanMax = 9

// readSpan reads bytes [b0, b0+len(dst)) of key's value into dst and
// reports the value's logical length. Bytes past the value, inside a
// lazy gap, or of a missing key read as zeros, which is exactly the
// oracle semantics S-I4 demands of every bit reader.
func (s *Str) readSpan(ctx context.Context, key []byte, b0 uint64, dst []byte) (uint64, error) {
	clear(dst)
	v, root, ok, err := s.t.Lookup(ctx, key)
	if err != nil || !ok {
		return 0, err
	}
	if !root {
		if uint64(len(v)) > b0 {
			copy(dst, v[b0:])
		}
		return uint64(len(v)), nil
	}
	r, err := decodeRopeRoot(v)
	if err != nil {
		return 0, err
	}
	if b0 >= r.totalLen {
		return r.totalLen, nil
	}
	hi := min(b0+uint64(len(dst)), r.totalLen)
	out, _, err := s.readRopeRange(ctx, r, b0, hi)
	if err != nil {
		return 0, err
	}
	copy(dst, out)
	return r.totalLen, nil
}

// GetBit returns bit off of key's value; anywhere the value has no
// byte, the bit is zero. The caller guarantees off is in range.
func (s *Str) GetBit(ctx context.Context, key []byte, off int64) (int, error) {
	var buf [1]byte
	if _, err := s.readSpan(ctx, key, uint64(off)>>3, buf[:]); err != nil {
		return 0, err
	}
	if buf[0]&(1<<(7-uint(off)&7)) != 0 {
		return 1, nil
	}
	return 0, nil
}

// SetBit sets bit off of key's value to bit and returns the previous
// bit. The value grows to cover the bit's byte when needed, even when
// the bit being written is zero, matching Redis. A write that would
// change neither a byte nor the length is skipped, which is invisible
// above this layer. The caller guarantees off is in range and bit is
// zero or one.
func (s *Str) SetBit(ctx context.Context, key []byte, off int64, bit int) (int, error) {
	b0 := uint64(off) >> 3
	mask := byte(1) << (7 - uint(off)&7)
	var buf [1]byte
	curLen, err := s.readSpan(ctx, key, b0, buf[:])
	if err != nil {
		return 0, err
	}
	old := 0
	if buf[0]&mask != 0 {
		old = 1
	}
	nb := buf[0] &^ mask
	if bit != 0 {
		nb |= mask
	}
	if nb == buf[0] && b0 < curLen {
		return old, nil
	}
	buf[0] = nb
	if _, err := s.SetRange(ctx, key, int64(b0), buf[:]); err != nil {
		return 0, err
	}
	return old, nil
}

// BitfieldOp is one parsed BITFIELD subcommand. Kind is 'g' for GET,
// 's' for SET, 'i' for INCRBY; Ovf is 'w', 's', or 'f' for WRAP, SAT,
// FAIL. Off is the absolute bit offset (the command layer resolves
// '#' typed indexing), Arg the SET value or INCRBY delta.
type BitfieldOp struct {
	Kind   byte
	Signed bool
	Bits   uint8
	Ovf    byte
	Off    uint64
	Arg    int64
}

// Bitfield executes ops in order against key and returns one result
// per op, with nulls marking FAIL overflows. Ops run sequentially, so
// a later op reads what an earlier op wrote. GET never creates or
// grows the value; SET and INCRBY grow it to the field's last byte,
// per Redis. The returned slices are scratch, valid until the next
// call.
func (s *Str) Bitfield(ctx context.Context, key []byte, ops []BitfieldOp) ([]int64, []bool, error) {
	s.bfRes = s.bfRes[:0]
	s.bfNull = s.bfNull[:0]
	var buf [bitSpanMax]byte
	for _, op := range ops {
		b0 := op.Off >> 3
		span := buf[:(op.Off+uint64(op.Bits)-1)>>3-b0+1]
		if _, err := s.readSpan(ctx, key, b0, span); err != nil {
			return nil, nil, err
		}
		bo := uint(op.Off & 7)
		cur := spanGetBits(span, bo, op.Bits)
		if op.Kind == 'g' {
			s.bfRes = append(s.bfRes, bfExtend(cur, op.Signed, op.Bits))
			s.bfNull = append(s.bfNull, false)
			continue
		}
		// SET is the overflow rules applied to 0 plus the given value,
		// which is how WRAP truncates and SAT clamps an out-of-range
		// input; INCRBY applies them to the current value plus delta.
		base, delta := uint64(0), op.Arg
		if op.Kind == 'i' {
			base = cur
		}
		var res uint64
		var ok bool
		if op.Signed {
			res, ok = bfApplySigned(bfExtend(base, true, op.Bits), delta, op.Bits, op.Ovf)
		} else {
			res, ok = bfApplyUnsigned(base, delta, op.Bits, op.Ovf)
		}
		if !ok {
			s.bfRes = append(s.bfRes, 0)
			s.bfNull = append(s.bfNull, true)
			continue
		}
		spanSetBits(span, bo, op.Bits, res)
		if _, err := s.SetRange(ctx, key, int64(b0), span); err != nil {
			return nil, nil, err
		}
		out := int64(res)
		if op.Kind == 's' {
			out = bfExtend(cur, op.Signed, op.Bits)
		} else if op.Signed {
			out = bfExtend(res, true, op.Bits)
		}
		s.bfRes = append(s.bfRes, out)
		s.bfNull = append(s.bfNull, false)
	}
	return s.bfRes, s.bfNull, nil
}

// spanGetBits reads the w-bit big-endian field at bit off of buf.
// Bit 0 of the value is the most significant bit of byte 0, Redis's
// bit order everywhere.
func spanGetBits(buf []byte, off uint, w uint8) uint64 {
	var v uint64
	for i := range uint(w) {
		b := off + i
		v = v<<1 | uint64(buf[b>>3]>>(7-b&7)&1)
	}
	return v
}

// spanSetBits writes the low w bits of v as the big-endian field at
// bit off of buf.
func spanSetBits(buf []byte, off uint, w uint8, v uint64) {
	for i := range uint(w) {
		b := off + i
		mask := byte(1) << (7 - b&7)
		if v>>(uint(w)-1-i)&1 != 0 {
			buf[b>>3] |= mask
		} else {
			buf[b>>3] &^= mask
		}
	}
}

// bfExtend turns a raw w-bit field into its int64 reading: sign
// extension for signed types, the value itself for unsigned (which
// always fits, since unsigned widths stop at 63).
func bfExtend(raw uint64, signed bool, w uint8) int64 {
	if signed && w < 64 && raw&(uint64(1)<<(w-1)) != 0 {
		raw |= ^(uint64(1)<<w - 1)
	}
	return int64(raw)
}

// bfApplySigned computes cur plus delta in the w-bit signed type
// under the overflow policy, returning the raw field bits to store
// and false for a FAIL that must write nothing. cur arrives already
// sign-extended and in range for the type, which is what makes the
// boundary arithmetic below safe of int64 overflow.
func bfApplySigned(cur, delta int64, w uint8, ovf byte) (uint64, bool) {
	maxv := int64(^uint64(0) >> (65 - uint(w)))
	minv := -maxv - 1
	if (delta > 0 && cur > maxv-delta) || (delta < 0 && cur < minv-delta) {
		switch ovf {
		case 'f':
			return 0, false
		case 's':
			if delta > 0 {
				return uint64(maxv), true
			}
			return uint64(minv) & (^uint64(0) >> (64 - uint(w))), true
		}
	}
	return (uint64(cur) + uint64(delta)) & (^uint64(0) >> (64 - uint(w))), true
}

// bfApplyUnsigned is bfApplySigned for the w-bit unsigned types,
// w at most 63. A negative delta's magnitude is computed in uint64
// so MinInt64 negates cleanly.
func bfApplyUnsigned(cur uint64, delta int64, w uint8, ovf byte) (uint64, bool) {
	maxv := ^uint64(0) >> (64 - uint(w))
	var over bool
	if delta >= 0 {
		over = uint64(delta) > maxv-cur
	} else {
		over = 0-uint64(delta) > cur
	}
	if over {
		switch ovf {
		case 'f':
			return 0, false
		case 's':
			if delta > 0 {
				return maxv, true
			}
			return 0, true
		}
	}
	return (cur + uint64(delta)) & maxv, true
}
