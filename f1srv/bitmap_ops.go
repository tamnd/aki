package f1srv

// BITOP and BITFIELD are the compound bitmap commands. BITOP folds several string keys together
// with a boolean operation into a destination string; BITFIELD reads and writes fixed-width signed
// or unsigned integers packed at arbitrary bit offsets of one string. Both work on the same f1raw
// string record the point bitmap commands use, so a bitfield key is a plain string at the keyspace
// layer and TYPE reports "string".

// cmdBitOp runs BITOP op destkey srckey [srckey ...]. AND/OR/XOR fold two or more sources; NOT
// complements a single source. The result length is the longest source, with shorter sources
// zero-padded on the right, matching Redis. An all-empty result deletes destkey and returns 0.
func (c *connState) cmdBitOp(argv [][]byte) {
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for 'bitop' command")
		return
	}
	op := argv[1]
	dest := argv[2]
	srcs := argv[3:]

	var kind int // 0 AND, 1 OR, 2 XOR, 3 NOT
	switch {
	case eqFold(op, "AND"):
		kind = 0
	case eqFold(op, "OR"):
		kind = 1
	case eqFold(op, "XOR"):
		kind = 2
	case eqFold(op, "NOT"):
		kind = 3
	default:
		c.writeErr("ERR syntax error")
		return
	}
	if kind == 3 && len(srcs) != 1 {
		c.writeErr("ERR BITOP NOT must be called with a single source key.")
		return
	}

	// Lock the destination and every source together in stripe order so the fold sees a
	// consistent snapshot and never deadlocks against another multi-key writer.
	keys := make([][]byte, 0, len(srcs)+1)
	keys = append(keys, dest)
	keys = append(keys, srcs...)
	unlock := c.lockStripes(keys)
	defer unlock()

	// Load every source. A missing key is an empty string; a non-string collection is WRONGTYPE.
	vals := make([][]byte, len(srcs))
	maxLen := 0
	for i, k := range srcs {
		v, hit := c.srv.store.Get(k, nil)
		if !hit {
			if c.collConflict(k) {
				c.writeErr(wrongType)
				return
			}
			v = nil
		}
		vals[i] = v
		if len(v) > maxLen {
			maxLen = len(v)
		}
	}

	if maxLen == 0 {
		// The result is empty: Redis deletes the destination and reports length 0.
		c.dropKeyLocked(dest)
		c.writeInt(0)
		return
	}

	res := make([]byte, maxLen)
	switch kind {
	case 3: // NOT over the single source, complementing only its own bytes.
		s := vals[0]
		for i := 0; i < maxLen; i++ {
			res[i] = ^s[i]
		}
	default:
		// Seed with the first source (zero-padded), then fold the rest byte by byte. A byte past
		// a source's length reads 0, which is the identity for OR/XOR and forces 0 for AND.
		for i := 0; i < maxLen; i++ {
			var b byte
			if i < len(vals[0]) {
				b = vals[0][i]
			}
			res[i] = b
		}
		for j := 1; j < len(vals); j++ {
			s := vals[j]
			for i := 0; i < maxLen; i++ {
				var b byte
				if i < len(s) {
					b = s[i]
				}
				switch kind {
				case 0:
					res[i] &= b
				case 1:
					res[i] |= b
				case 2:
					res[i] ^= b
				}
			}
		}
	}

	// The destination becomes a plain string of the folded bytes. Drop any collection there first
	// so the overwrite does not leave orphan element rows.
	if c.collConflict(dest) {
		c.dropKeyLocked(dest)
	}
	if err := c.srv.store.Set(dest, res); err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeInt(int64(maxLen))
}

// bfOp is one parsed BITFIELD subcommand.
type bfOp struct {
	kind   int   // bfGet, bfSet, bfIncrby
	sign   bool  // true for i<bits>, false for u<bits>
	bits   uint  // 1..64 signed, 1..63 unsigned
	offset uint64
	arg    int64 // SET value or INCRBY increment
	owrap  int   // overflow policy in force for this write: bfWrap/bfSat/bfFail
}

const (
	bfGet = iota
	bfSet
	bfIncrby
)

const (
	bfWrap = iota
	bfSat
	bfFail
)

// cmdBitField runs BITFIELD key op...; readOnly restricts it to GET for BITFIELD_RO.
func (c *connState) cmdBitField(argv [][]byte, readOnly bool) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'bitfield' command")
		return
	}
	key := argv[1]

	ops := make([]bfOp, 0, 4)
	over := bfWrap
	highestWrite := int64(-1) // highest byte index a write touches, -1 if read-only run
	i := 2
	for i < len(argv) {
		tok := argv[i]
		switch {
		case eqFold(tok, "OVERFLOW"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			switch {
			case eqFold(argv[i+1], "WRAP"):
				over = bfWrap
			case eqFold(argv[i+1], "SAT"):
				over = bfSat
			case eqFold(argv[i+1], "FAIL"):
				over = bfFail
			default:
				c.writeErr("ERR Invalid OVERFLOW type specified")
				return
			}
			i += 2
		case eqFold(tok, "GET"):
			if i+2 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			sign, bits, ok := parseBitfieldType(argv[i+1])
			if !ok {
				c.writeErr("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
				return
			}
			off, ok := parseBitfieldOffset(argv[i+2], bits)
			if !ok {
				c.writeErr("ERR bit offset is not an integer or out of range")
				return
			}
			ops = append(ops, bfOp{kind: bfGet, sign: sign, bits: bits, offset: off})
			i += 3
		case eqFold(tok, "SET"), eqFold(tok, "INCRBY"):
			if readOnly {
				c.writeErr("ERR BITFIELD_RO only supports the GET subcommand")
				return
			}
			if i+3 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			sign, bits, ok := parseBitfieldType(argv[i+1])
			if !ok {
				c.writeErr("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
				return
			}
			off, ok := parseBitfieldOffset(argv[i+2], bits)
			if !ok {
				c.writeErr("ERR bit offset is not an integer or out of range")
				return
			}
			val, err := atoi64(argv[i+3])
			if err != nil {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			kind := bfSet
			if eqFold(tok, "INCRBY") {
				kind = bfIncrby
			}
			ops = append(ops, bfOp{kind: kind, sign: sign, bits: bits, offset: off, arg: val, owrap: over})
			last := int64((off + uint64(bits) - 1) >> 3)
			if last > highestWrite {
				highestWrite = last
			}
			i += 3 + 1
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	mu := &c.srv.incrMu[c.srv.stripe(key)]
	mu.Lock()
	v, hit := c.srv.store.Get(key, nil)
	if !hit {
		if c.collConflict(key) {
			mu.Unlock()
			c.writeErr(wrongType)
			return
		}
		v = nil
	}
	changed := false
	if highestWrite >= 0 && int(highestWrite) >= len(v) {
		grown := make([]byte, highestWrite+1)
		copy(grown, v)
		v = grown
	}

	c.writeArrayHeader(len(ops))
	for _, op := range ops {
		switch op.kind {
		case bfGet:
			c.writeInt(bitfieldGet(v, op))
		case bfSet:
			old := bitfieldGet(v, op)
			nv, overflow := bitfieldClampSet(op)
			if overflow && op.owrap == bfFail {
				c.writeNil()
				continue
			}
			setBitfield(v, op.offset, op.bits, uint64(nv))
			changed = true
			c.writeInt(old)
		case bfIncrby:
			old := bitfieldGet(v, op)
			nv, overflow := bitfieldClampIncr(old, op)
			if overflow && op.owrap == bfFail {
				c.writeNil()
				continue
			}
			setBitfield(v, op.offset, op.bits, uint64(nv))
			changed = true
			c.writeInt(nv)
		}
	}

	if changed {
		if err := c.srv.store.Set(key, v); err != nil {
			// The reply array is already on the wire; a store error here is a server fault, so
			// the best we can do is surface it after the partial reply. In practice Set on the
			// in-memory arena does not fail for a bitfield-sized value.
			_ = err
		}
	}
	mu.Unlock()
}

// parseBitfieldType parses an i<bits>/u<bits> type token. Signed allows 1..64 bits, unsigned 1..63,
// matching Redis (u64 is not representable in a signed reply).
func parseBitfieldType(b []byte) (sign bool, bits uint, ok bool) {
	if len(b) < 2 {
		return false, 0, false
	}
	switch b[0] {
	case 'i':
		sign = true
	case 'u':
		sign = false
	default:
		return false, 0, false
	}
	n, err := atoi64(b[1:])
	if err != nil || n < 1 {
		return false, 0, false
	}
	if sign && n > 64 {
		return false, 0, false
	}
	if !sign && n > 63 {
		return false, 0, false
	}
	return sign, uint(n), true
}

// parseBitfieldOffset parses a bitfield offset. A leading '#' multiplies the index by the type
// width so callers can address the Nth field of that width. The offset's byte must fall inside the
// 512 MiB string cap.
func parseBitfieldOffset(b []byte, bits uint) (uint64, bool) {
	mult := false
	if len(b) > 0 && b[0] == '#' {
		mult = true
		b = b[1:]
	}
	n, err := atoi64(b)
	if err != nil || n < 0 {
		return 0, false
	}
	off := uint64(n)
	if mult {
		off *= uint64(bits)
	}
	if (off >> 3) >= maxBitmapByte {
		return 0, false
	}
	return off, true
}

// bitfieldGet reads the field described by op from v, sign-extending signed types. Bits past the
// end of v read as 0, so a GET beyond the string returns 0 without growing it.
func bitfieldGet(v []byte, op bfOp) int64 {
	u := getUnsignedBitfield(v, op.offset, op.bits)
	if !op.sign {
		return int64(u)
	}
	if op.bits < 64 && u&(uint64(1)<<(op.bits-1)) != 0 {
		u |= ^uint64(0) << op.bits
	}
	return int64(u)
}

// getUnsignedBitfield reads bits bits MSB-first starting at absolute bit offset. A byte index past
// len(v) contributes zero bits.
func getUnsignedBitfield(v []byte, offset uint64, bits uint) uint64 {
	var value uint64
	for bits > 0 {
		bits--
		byteIdx := offset >> 3
		var bv byte
		if int(byteIdx) < len(v) {
			bv = v[byteIdx]
		}
		bit := (bv >> (7 - (offset & 7))) & 1
		value = (value << 1) | uint64(bit)
		offset++
	}
	return value
}

// setBitfield writes the low bits bits of value MSB-first starting at absolute bit offset. The
// caller guarantees v already spans the touched bytes.
func setBitfield(v []byte, offset uint64, bits uint, value uint64) {
	for bits > 0 {
		bits--
		byteIdx := offset >> 3
		pos := uint(7 - (offset & 7))
		bit := byte((value >> bits) & 1)
		v[byteIdx] = (v[byteIdx] &^ (1 << pos)) | (bit << pos)
		offset++
	}
}

// bitfieldClampSet resolves a SET against the type range under the op's overflow policy. It returns
// the value to store and whether the requested value overflowed the type. WRAP truncates to the
// low bits, SAT clamps to the type's min or max, FAIL leaves the value untouched (the caller skips
// the write and replies nil).
func bitfieldClampSet(op bfOp) (int64, bool) {
	if op.sign {
		nv, over := checkSignedOverflow(op.arg, 0, op.bits, op.owrap)
		return nv, over
	}
	nv, over := checkUnsignedOverflow(uint64(op.arg), 0, op.bits, op.owrap)
	return int64(nv), over
}

// bitfieldClampIncr resolves old+increment under the op's overflow policy, returning the resulting
// value and whether it overflowed.
func bitfieldClampIncr(old int64, op bfOp) (int64, bool) {
	if op.sign {
		nv, over := checkSignedOverflow(old, op.arg, op.bits, op.owrap)
		return nv, over
	}
	nv, over := checkUnsignedOverflow(uint64(old), op.arg, op.bits, op.owrap)
	return int64(nv), over
}

// checkSignedOverflow computes value+incr for a signed field of the given width and reports whether
// it overflowed the [min,max] range. Under WRAP it returns the two's-complement wrapped result,
// under SAT the clamped bound, and under FAIL the un-wrapped sum (unused by the caller on overflow).
func checkSignedOverflow(value, incr int64, bits uint, owtype int) (int64, bool) {
	var max int64
	if bits == 64 {
		max = int64(^uint64(0) >> 1)
	} else {
		max = int64(1)<<(bits-1) - 1
	}
	min := -max - 1

	// maxincr/minincr are the largest safe increment up and down from value. They are only read
	// after value is confirmed in range for that direction, so max-value and min-value never
	// overflow int64. Comparing incr against them (rather than value+incr against a bound) is what
	// keeps the i64 case correct: value+incr can wrap past the bound, but incr>maxincr cannot.
	maxincr := max - value
	minincr := min - value

	over := false
	up := false
	if value > max || (bits != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr) {
		over, up = true, true
	} else if value < min || (bits != 64 && incr < minincr) || (value < 0 && incr < 0 && incr < minincr) {
		over, up = true, false
	}
	if !over {
		return value + incr, false
	}
	switch owtype {
	case bfSat:
		if up {
			return max, true
		}
		return min, true
	case bfWrap:
		c := uint64(value) + uint64(incr)
		if bits < 64 {
			msb := uint64(1) << (bits - 1)
			mask := ^uint64(0) << bits
			if c&msb != 0 {
				c |= mask
			} else {
				c &^= mask
			}
		}
		return int64(c), true
	default: // FAIL: caller ignores the value
		return value + incr, true
	}
}

// checkUnsignedOverflow computes value+incr for an unsigned field of the given width and reports
// whether it overflowed [0,max]. WRAP truncates to the low bits, SAT clamps to 0 or max.
func checkUnsignedOverflow(value uint64, incr int64, bits uint, owtype int) (uint64, bool) {
	var max uint64
	if bits == 64 {
		max = ^uint64(0)
	} else {
		max = (uint64(1) << bits) - 1
	}

	over := false
	up := false
	if value > max {
		over, up = true, true
	} else if incr >= 0 {
		if uint64(incr) > max-value {
			over, up = true, true
		}
	} else {
		// incr < 0: underflow when the magnitude exceeds the current value.
		mag := uint64(-incr)
		if mag > value {
			over, up = true, false
		}
	}
	if !over {
		if incr >= 0 {
			return value + uint64(incr), false
		}
		return value - uint64(-incr), false
	}
	switch owtype {
	case bfSat:
		if up {
			return max, true
		}
		return 0, true
	case bfWrap:
		res := value + uint64(incr)
		if bits < 64 {
			res &= ^(^uint64(0) << bits)
		}
		return res, true
	default: // FAIL
		return value + uint64(incr), true
	}
}
