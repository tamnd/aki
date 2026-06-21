package rdb

import (
	"errors"
	"strconv"

	"github.com/tamnd/aki/encoding"
)

// errTruncated means the payload ended before a field finished. RESTORE turns any
// decode error into the single "version or checksum are wrong" reply, so the exact
// message here is for internal clarity only.
var errTruncated = errors.New("rdb: truncated payload")

// special length sub-encodings carried in the low six bits of an 11xxxxxx byte.
const (
	encInt8  = 0
	encInt16 = 1
	encInt32 = 2
	encLZF   = 3
)

// appendLength writes a length using the variable-width RDB scheme: one byte for
// values up to 63, two for up to 16383, then a five-byte 32-bit form and a
// nine-byte 64-bit form. aki never needs lengths past 32 bits in practice but the
// 64-bit form keeps the encoder total.
func appendLength(dst []byte, n uint64) []byte {
	switch {
	case n <= 63:
		return append(dst, byte(n))
	case n <= 16383:
		return append(dst, 0x40|byte(n>>8), byte(n))
	case n <= 0xFFFFFFFF:
		dst = append(dst, 0x80)
		return encoding.AppendU32BE(dst, uint32(n))
	default:
		dst = append(dst, 0x81)
		return encoding.AppendU64BE(dst, n)
	}
}

// reader walks a byte slice with a cursor and a one-shot error. Once a read runs
// past the end every later read is a no-op and err stays set, so callers can run a
// sequence of reads and check the error once at the end.
type reader struct {
	buf []byte
	pos int
	err error
}

// readByte returns the next byte and advances, or records errTruncated.
func (r *reader) readByte() byte {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.buf) {
		r.err = errTruncated
		return 0
	}
	b := r.buf[r.pos]
	r.pos++
	return b
}

// readBytes returns the next n bytes as a copy and advances.
func (r *reader) readBytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || r.pos+n > len(r.buf) {
		r.err = errTruncated
		return nil
	}
	out := make([]byte, n)
	copy(out, r.buf[r.pos:r.pos+n])
	r.pos += n
	return out
}

// readLength reads a length-encoded integer. special is true when the byte carried
// an 11xxxxxx sub-encoding rather than a plain length, in which case the caller
// inspects subType to read the integer or LZF body.
func (r *reader) readLength() (n uint64, special bool, subType byte) {
	first := r.readByte()
	if r.err != nil {
		return 0, false, 0
	}
	switch (first & 0xC0) >> 6 {
	case 0:
		return uint64(first & 0x3F), false, 0
	case 1:
		second := r.readByte()
		return uint64(first&0x3F)<<8 | uint64(second), false, 0
	case 2:
		if first&0x3F == 0 {
			b := r.readBytes(4)
			if r.err != nil {
				return 0, false, 0
			}
			return uint64(encoding.U32BE(b)), false, 0
		}
		b := r.readBytes(8)
		if r.err != nil {
			return 0, false, 0
		}
		return encoding.U64BE(b), false, 0
	default:
		return 0, true, first & 0x3F
	}
}

// appendString writes a value as an RDB string. An integer that round-trips
// through its decimal form is packed into the smallest INT8/INT16/INT32 special
// encoding; everything else is a length prefix followed by the raw bytes. aki does
// not emit LZF, it only reads it.
func appendString(dst, s []byte) []byte {
	if iv, ok := asInt(s); ok {
		switch {
		case iv >= -128 && iv <= 127:
			return append(dst, 0xC0|encInt8, byte(iv))
		case iv >= -32768 && iv <= 32767:
			dst = append(dst, 0xC0|encInt16)
			return encoding.AppendU16(dst, uint16(iv))
		case iv >= -2147483648 && iv <= 2147483647:
			dst = append(dst, 0xC0|encInt32)
			return encoding.AppendU32(dst, uint32(iv))
		}
	}
	dst = appendLength(dst, uint64(len(s)))
	return append(dst, s...)
}

// readString reads an RDB string, returning the raw bytes. Integer encodings come
// back as their decimal text, and LZF bodies are decompressed.
func (r *reader) readString() []byte {
	n, special, subType := r.readLength()
	if r.err != nil {
		return nil
	}
	if !special {
		return r.readBytes(int(n))
	}
	switch subType {
	case encInt8:
		v := int8(r.readByte())
		return []byte(strconv.FormatInt(int64(v), 10))
	case encInt16:
		b := r.readBytes(2)
		if r.err != nil {
			return nil
		}
		return []byte(strconv.FormatInt(int64(int16(encoding.U16(b))), 10))
	case encInt32:
		b := r.readBytes(4)
		if r.err != nil {
			return nil
		}
		return []byte(strconv.FormatInt(int64(int32(encoding.U32(b))), 10))
	case encLZF:
		clen, _, _ := r.readLength()
		ulen, _, _ := r.readLength()
		comp := r.readBytes(int(clen))
		if r.err != nil {
			return nil
		}
		out, err := lzfDecompress(comp, int(ulen))
		if err != nil {
			r.err = err
			return nil
		}
		return out
	default:
		r.err = errTruncated
		return nil
	}
}

// asInt reports whether s is the canonical decimal form of an int64, so that
// re-encoding the parsed value reproduces s exactly. Only then is it safe to pack
// the string as an integer.
func asInt(s []byte) (int64, bool) {
	if len(s) == 0 || len(s) > 20 {
		return 0, false
	}
	v, err := strconv.ParseInt(string(s), 10, 64)
	if err != nil {
		return 0, false
	}
	if strconv.FormatInt(v, 10) != string(s) {
		return 0, false
	}
	return v, true
}
