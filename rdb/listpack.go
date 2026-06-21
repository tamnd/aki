package rdb

import (
	"strconv"

	"github.com/tamnd/aki/encoding"
)

// Listpack is Redis's compact single-allocation layout for a small list, set,
// hash, or sorted set. The blob is a 6-byte header (total byte length, then the
// element count), a run of entries, and a 0xFF terminator. Each entry is an
// encoding byte that selects an integer or string form, the value bytes, and a
// back length so the structure can be walked in reverse. aki only walks it
// forward.

// listpackEncode packs elements into a listpack blob. An element that is the
// canonical decimal form of an integer is stored as an integer, which is how Redis
// keeps numeric members small and is required for byte-identical output.
func listpackEncode(elems [][]byte) []byte {
	var body []byte
	for _, e := range elems {
		var ent []byte
		if iv, ok := asInt(e); ok {
			ent = lpEncodeInt(iv)
		} else {
			ent = lpEncodeStr(e)
		}
		ent = append(ent, lpEncodeBacklen(len(ent))...)
		body = append(body, ent...)
	}

	total := 6 + len(body) + 1
	out := make([]byte, 0, total)
	out = encoding.AppendU32(out, uint32(total))
	num := min(len(elems), 65535)
	out = encoding.AppendU16(out, uint16(num))
	out = append(out, body...)
	out = append(out, 0xFF)
	return out
}

// lpEncodeInt encodes an integer entry, picking the smallest width: a 7-bit
// unsigned form, a 13-bit signed form, then fixed 16, 24, 32 and 64-bit forms.
func lpEncodeInt(v int64) []byte {
	switch {
	case v >= 0 && v <= 127:
		return []byte{byte(v)}
	case v >= -4096 && v <= 4095:
		u := uint16(v) & 0x1FFF
		return []byte{0xC0 | byte(u>>8), byte(u)}
	case v >= -32768 && v <= 32767:
		return []byte{0xF1, byte(v), byte(v >> 8)}
	case v >= -8388608 && v <= 8388607:
		return []byte{0xF2, byte(v), byte(v >> 8), byte(v >> 16)}
	case v >= -2147483648 && v <= 2147483647:
		return []byte{0xF3, byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
	default:
		b := []byte{0xF4}
		return encoding.AppendU64(b, uint64(v))
	}
}

// lpEncodeStr encodes a string entry: a 6-bit length form up to 63 bytes, a
// 12-bit form up to 4095, then a 32-bit length form.
func lpEncodeStr(s []byte) []byte {
	n := len(s)
	switch {
	case n <= 63:
		return append([]byte{0x80 | byte(n)}, s...)
	case n <= 4095:
		return append([]byte{0xE0 | byte(n>>8), byte(n)}, s...)
	default:
		b := []byte{0xF0}
		b = append(b, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
		return append(b, s...)
	}
}

// lpEncodeBacklen encodes the entry length for backward traversal. The bytes are
// laid out most significant first with a continuation bit on every byte after the
// first, which is what lets a reader scan them in reverse.
func lpEncodeBacklen(l int) []byte {
	switch {
	case l <= 127:
		return []byte{byte(l)}
	case l < 16384:
		return []byte{byte(l >> 7), byte(l&127) | 128}
	case l < 2097152:
		return []byte{byte(l >> 14), byte((l>>7)&127) | 128, byte(l&127) | 128}
	case l < 268435456:
		return []byte{byte(l >> 21), byte((l>>14)&127) | 128, byte((l>>7)&127) | 128, byte(l&127) | 128}
	default:
		return []byte{byte(l >> 28), byte((l>>21)&127) | 128, byte((l>>14)&127) | 128, byte((l>>7)&127) | 128, byte(l&127) | 128}
	}
}

// lpBacklenSize is the number of bytes lpEncodeBacklen used for an entry of the
// given length, needed to skip past an entry while reading forward.
func lpBacklenSize(entryLen int) int {
	switch {
	case entryLen <= 127:
		return 1
	case entryLen < 16384:
		return 2
	case entryLen < 2097152:
		return 3
	case entryLen < 268435456:
		return 4
	default:
		return 5
	}
}

// listpackDecode walks a listpack blob forward and returns its elements, with
// integer entries rendered as decimal text. It tolerates the count saturating at
// 65535 by reading until the terminator rather than trusting the header count.
func listpackDecode(blob []byte) ([][]byte, error) {
	if len(blob) < 7 {
		return nil, errTruncated
	}
	p := 6
	var out [][]byte
	for p < len(blob) {
		b := blob[p]
		if b == 0xFF {
			return out, nil
		}
		val, entryLen, err := lpReadEntry(blob, p)
		if err != nil {
			return nil, err
		}
		out = append(out, val)
		p += entryLen + lpBacklenSize(entryLen)
	}
	return nil, errTruncated
}

// lpReadEntry decodes the entry at p and returns its value and the length of the
// encoding-plus-value part (not counting the back length).
func lpReadEntry(blob []byte, p int) ([]byte, int, error) {
	b := blob[p]
	switch {
	case b&0x80 == 0: // 7-bit unsigned int
		return []byte(strconv.FormatInt(int64(b&0x7F), 10)), 1, nil
	case b&0xC0 == 0x80: // 6-bit string
		n := int(b & 0x3F)
		if p+1+n > len(blob) {
			return nil, 0, errTruncated
		}
		return cloneBytes(blob[p+1 : p+1+n]), 1 + n, nil
	case b&0xE0 == 0xC0: // 13-bit signed int
		if p+2 > len(blob) {
			return nil, 0, errTruncated
		}
		raw := uint16(b&0x1F)<<8 | uint16(blob[p+1])
		v := int64(raw)
		if raw&0x1000 != 0 { // sign-extend bit 12
			v = int64(raw) - 8192
		}
		return []byte(strconv.FormatInt(v, 10)), 2, nil
	case b&0xF0 == 0xE0: // 12-bit string
		if p+2 > len(blob) {
			return nil, 0, errTruncated
		}
		n := int(b&0x0F)<<8 | int(blob[p+1])
		if p+2+n > len(blob) {
			return nil, 0, errTruncated
		}
		return cloneBytes(blob[p+2 : p+2+n]), 2 + n, nil
	case b == 0xF1: // 16-bit int
		if p+3 > len(blob) {
			return nil, 0, errTruncated
		}
		v := int64(int16(uint16(blob[p+1]) | uint16(blob[p+2])<<8))
		return []byte(strconv.FormatInt(v, 10)), 3, nil
	case b == 0xF2: // 24-bit int
		if p+4 > len(blob) {
			return nil, 0, errTruncated
		}
		raw := uint32(blob[p+1]) | uint32(blob[p+2])<<8 | uint32(blob[p+3])<<16
		v := int64(raw)
		if raw&0x800000 != 0 {
			v = int64(raw) - (1 << 24)
		}
		return []byte(strconv.FormatInt(v, 10)), 4, nil
	case b == 0xF3: // 32-bit int
		if p+5 > len(blob) {
			return nil, 0, errTruncated
		}
		v := int64(int32(uint32(blob[p+1]) | uint32(blob[p+2])<<8 | uint32(blob[p+3])<<16 | uint32(blob[p+4])<<24))
		return []byte(strconv.FormatInt(v, 10)), 5, nil
	case b == 0xF4: // 64-bit int
		if p+9 > len(blob) {
			return nil, 0, errTruncated
		}
		v := int64(encoding.U64(blob[p+1 : p+9]))
		return []byte(strconv.FormatInt(v, 10)), 9, nil
	case b == 0xF0: // 32-bit string
		if p+5 > len(blob) {
			return nil, 0, errTruncated
		}
		n := int(uint32(blob[p+1]) | uint32(blob[p+2])<<8 | uint32(blob[p+3])<<16 | uint32(blob[p+4])<<24)
		if p+5+n > len(blob) {
			return nil, 0, errTruncated
		}
		return cloneBytes(blob[p+5 : p+5+n]), 5 + n, nil
	default:
		return nil, 0, errTruncated
	}
}

// cloneBytes returns a copy so decoded elements never alias the payload buffer.
func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
