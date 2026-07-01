package f1srv

import "math/bits"

// A bitmap is not a distinct type: it is a string addressed at the bit level, exactly as Redis
// treats it. SETBIT/GETBIT/BITCOUNT/BITPOS impose a bit-array view over the string bytes and go
// through the same f1raw string record every GET/SET uses, so a bitmap key is a string key at the
// keyspace layer (TYPE reports "string", EXPIRE and DEL behave as for any string). The bit-to-byte
// mapping is MSB-first within each byte: bit 0 is the high bit (0x80) of byte 0, bit 7 the low bit
// (0x01), so setting bit 0 makes byte 0 equal 0x80, matching Redis for cross-tool exchange.

// maxBitmapByte is the string's 512 MiB hard cap expressed in bytes. The highest addressable bit
// is offset 2^32-1, which lives in byte 2^29-1, so a byte index at or past 2^29 is out of range.
const maxBitmapByte = 512 * 1024 * 1024

// collConflict reports whether key exists as a non-string collection, in which case a bitmap
// (string) command must fail with WRONGTYPE. It probes only the collection headers, so it is the
// guard used after a string Get already missed: a Get hit means the key is a string, a miss plus
// no collection header means the key is absent and the bitmap command treats it as an empty string.
func (c *connState) collConflict(key []byte) bool {
	return c.srv.store.ExistsKind(key, kindHashMeta) ||
		c.srv.store.ExistsKind(key, kindSetMeta) ||
		c.srv.store.ExistsKind(key, kindZsetMeta) ||
		c.srv.store.ExistsKind(key, kindListMeta) ||
		c.srv.store.ExistsKind(key, kindStreamMeta)
}

// parseBitOffset parses a SETBIT/GETBIT bit offset: a non-negative integer whose byte lands inside
// the 512 MiB string cap. It returns false for a negative offset or one past the cap, which the
// caller reports as the Redis "bit offset is not an integer or out of range" error.
func parseBitOffset(b []byte) (int64, bool) {
	n, err := atoi64(b)
	if err != nil || n < 0 || (n>>3) >= maxBitmapByte {
		return 0, false
	}
	return n, true
}

// bitMask returns a byte mask with the MSB-first bit positions lo..hi (0 is the high bit) set.
// It builds the mask from the two edge masks rather than looping, since a byte spans only 8 bits.
func bitMask(lo, hi int64) byte {
	// high 0xFF>>lo keeps positions lo..7; low 0xFF<<(7-hi) keeps positions 0..hi; the AND is lo..hi.
	return byte(0xFF>>uint(lo)) & byte(0xFF<<uint(7-hi))
}

// popcountRange counts the set bits of v in the inclusive absolute bit range [firstBit, lastBit].
// It masks the partial first and last bytes and word-scans the full-byte interior with POPCNT, so
// a BITCOUNT over a wide range costs one pass over only the bytes the range spans, never the whole
// value beyond it.
func popcountRange(v []byte, firstBit, lastBit int64) int64 {
	if firstBit > lastBit {
		return 0
	}
	fb, lb := firstBit>>3, lastBit>>3
	if fb == lb {
		return int64(bits.OnesCount8(v[fb] & bitMask(firstBit&7, lastBit&7)))
	}
	total := int64(bits.OnesCount8(v[fb] & bitMask(firstBit&7, 7)))
	total += int64(bits.OnesCount8(v[lb] & bitMask(0, lastBit&7)))
	// Full-byte interior (fb, lb): eight bytes at a time through POPCNT, then the tail bytes.
	mid := v[fb+1 : lb]
	i := 0
	for ; i+8 <= len(mid); i += 8 {
		total += int64(bits.OnesCount64(uint64(mid[i]) | uint64(mid[i+1])<<8 | uint64(mid[i+2])<<16 |
			uint64(mid[i+3])<<24 | uint64(mid[i+4])<<32 | uint64(mid[i+5])<<40 |
			uint64(mid[i+6])<<48 | uint64(mid[i+7])<<56))
	}
	for ; i < len(mid); i++ {
		total += int64(bits.OnesCount8(mid[i]))
	}
	return total
}

// scanBit returns the absolute index of the first bit equal to target in the inclusive range
// [firstBit, lastBit], or -1 if none. It skips whole bytes that cannot hold the target (0x00 when
// hunting a 1, 0xFF when hunting a 0) so a sparse scan runs at byte speed, then locates the bit.
func scanBit(v []byte, target byte, firstBit, lastBit int64) int64 {
	if firstBit > lastBit {
		return -1
	}
	skip := byte(0x00) // a byte holding no target bit
	if target == 0 {
		skip = 0xFF
	}
	fb, lb := firstBit>>3, lastBit>>3
	for i := fb; i <= lb; i++ {
		b := v[i]
		lo, hi := int64(0), int64(7)
		if i == fb {
			lo = firstBit & 7
		}
		if i == lb {
			hi = lastBit & 7
		}
		if lo == 0 && hi == 7 && b == skip {
			continue
		}
		for p := lo; p <= hi; p++ {
			if (b>>(7-uint(p)))&1 == target {
				return i*8 + p
			}
		}
	}
	return -1
}

func (c *connState) cmdSetBit(argv [][]byte) {
	// SETBIT key offset value
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'setbit' command")
		return
	}
	key := argv[1]
	off, ok := parseBitOffset(argv[2])
	if !ok {
		c.writeErr("ERR bit offset is not an integer or out of range")
		return
	}
	if len(argv[3]) != 1 || (argv[3][0] != '0' && argv[3][0] != '1') {
		c.writeErr("ERR bit is not an integer or out of range")
		return
	}
	set := argv[3][0] == '1'
	byteIdx := off >> 3
	bitPos := uint(7 - (off & 7))

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
	if int(byteIdx) >= len(v) {
		grown := make([]byte, byteIdx+1)
		copy(grown, v)
		v = grown
	}
	old := (v[byteIdx] >> bitPos) & 1
	if set {
		v[byteIdx] |= 1 << bitPos
	} else {
		v[byteIdx] &^= 1 << bitPos
	}
	err := c.srv.store.Set(key, v)
	mu.Unlock()
	if err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeInt(int64(old))
}

func (c *connState) cmdGetBit(argv [][]byte) {
	// GETBIT key offset
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'getbit' command")
		return
	}
	key := argv[1]
	off, ok := parseBitOffset(argv[2])
	if !ok {
		c.writeErr("ERR bit offset is not an integer or out of range")
		return
	}
	v, hit := c.srv.store.Get(key, c.vbuf[:0])
	c.vbuf = v
	if !hit {
		if c.collConflict(key) {
			c.writeErr(wrongType)
			return
		}
		c.writeInt(0)
		return
	}
	byteIdx := off >> 3
	if int(byteIdx) >= len(v) {
		c.writeInt(0)
		return
	}
	c.writeInt(int64((v[byteIdx] >> uint(7-(off&7))) & 1))
}

func (c *connState) cmdBitCount(argv [][]byte) {
	// BITCOUNT key [start end [BYTE|BIT]]
	if len(argv) != 2 && len(argv) != 4 && len(argv) != 5 {
		c.writeErr("ERR syntax error")
		return
	}
	key := argv[1]
	v, hit := c.srv.store.Get(key, c.vbuf[:0])
	c.vbuf = v
	if !hit {
		if c.collConflict(key) {
			c.writeErr(wrongType)
			return
		}
		c.writeInt(0)
		return
	}
	if len(argv) == 2 {
		c.writeInt(popcountRange(v, 0, int64(len(v))*8-1))
		return
	}
	start, err1 := atoi64(argv[2])
	end, err2 := atoi64(argv[3])
	if err1 != nil || err2 != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	inBit := false
	if len(argv) == 5 {
		switch {
		case eqFold(argv[4], "BYTE"):
		case eqFold(argv[4], "BIT"):
			inBit = true
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}
	first, last, empty := bitRange(start, end, int64(len(v)), inBit)
	if empty {
		c.writeInt(0)
		return
	}
	c.writeInt(popcountRange(v, first, last))
}

func (c *connState) cmdBitPos(argv [][]byte) {
	// BITPOS key bit [start [end [BYTE|BIT]]]
	if len(argv) < 3 || len(argv) > 6 {
		c.writeErr("ERR syntax error")
		return
	}
	key := argv[1]
	if len(argv[2]) != 1 || (argv[2][0] != '0' && argv[2][0] != '1') {
		c.writeErr("ERR The bit argument must be 1 or 0.")
		return
	}
	target := argv[2][0] - '0'

	v, hit := c.srv.store.Get(key, c.vbuf[:0])
	c.vbuf = v
	if !hit {
		if c.collConflict(key) {
			c.writeErr(wrongType)
			return
		}
		// A missing key is treated as an empty string: a 0 is found at position 0, a 1 never.
		if target == 0 {
			c.writeInt(0)
		} else {
			c.writeInt(-1)
		}
		return
	}

	inBit := false
	if len(argv) == 6 {
		switch {
		case eqFold(argv[5], "BYTE"):
		case eqFold(argv[5], "BIT"):
			inBit = true
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}
	start := int64(0)
	if len(argv) >= 4 {
		n, err := atoi64(argv[3])
		if err != nil {
			c.writeErr("ERR value is not an integer or out of range")
			return
		}
		start = n
	}
	totlen := int64(len(v))
	if inBit {
		totlen *= 8
	}
	endGiven := len(argv) >= 5
	end := totlen - 1
	if endGiven {
		n, err := atoi64(argv[4])
		if err != nil {
			c.writeErr("ERR value is not an integer or out of range")
			return
		}
		end = n
	}
	first, last, empty := bitRangeNorm(start, end, totlen, inBit)
	if empty {
		c.writeInt(-1)
		return
	}
	pos := scanBit(v, target, first, last)
	if pos < 0 && target == 0 && !endGiven {
		// Hunting a clear bit with no explicit end and the searched span was all ones: Redis
		// treats the string as zero-padded on the right and returns the first bit past it.
		c.writeInt(last + 1)
		return
	}
	c.writeInt(pos)
}

// bitRange normalizes a BITCOUNT start/end pair (default end = last unit) into an absolute inclusive
// bit range, reporting empty when start clamps past end. lenBytes is the value length in bytes;
// inBit selects the BIT unit (indexes count bits) over the default BYTE unit (indexes count bytes).
func bitRange(start, end, lenBytes int64, inBit bool) (first, last int64, empty bool) {
	totlen := lenBytes
	if inBit {
		totlen = lenBytes * 8
	}
	return bitRangeNorm(start, end, totlen, inBit)
}

// bitRangeNorm applies Redis's negative-index and clamp rules for start/end given in unit indexes
// against totlen (the length in that unit), then converts the clamped index range to an absolute
// inclusive bit range. It reports empty when the range collapses (start past end, or a zero-length
// value), which the caller maps to the command's empty answer.
func bitRangeNorm(start, end, totlen int64, inBit bool) (first, last int64, empty bool) {
	if totlen == 0 {
		return 0, 0, true
	}
	if start < 0 {
		start += totlen
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end += totlen
		if end < 0 {
			end = 0
		}
	}
	if end >= totlen {
		end = totlen - 1
	}
	if start > end {
		return 0, 0, true
	}
	if inBit {
		return start, end, false
	}
	return start * 8, end*8 + 7, false
}
