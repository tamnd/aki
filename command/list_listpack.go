package command

import (
	"strconv"

	"github.com/tamnd/aki/encoding"
)

// A list reports listpack or quicklist for OBJECT ENCODING based on the size its
// elements would take inside a Redis listpack, the same rule t_list.c applies
// through quicklistNodeExceedsLimit. aki keeps its own physical list form, so
// these helpers exist only to compute that reported size accurately: the byte
// budget (list-max-listpack-size negative) and the 8KB safety cap (positive)
// both compare against lpBytes, the listpack's own byte count, so an estimate
// that drifts from lpBytes would flip the encoding name at a different element
// count than Redis.

// lpHeaderBytes is the fixed listpack overhead Redis counts in lpBytes: the
// 6-byte header (4-byte total length plus 2-byte element count) and the 1-byte
// 0xFF terminator.
const lpHeaderBytes = 7

// listpackBytes returns the exact number of bytes Redis's listpack uses to hold
// elems, matching lpBytes: the header and terminator plus each element's
// lpEntrySize.
func listpackBytes(elems [][]byte) int {
	total := lpHeaderBytes
	for _, e := range elems {
		total += lpEntrySize(e)
	}
	return total
}

// lpEntrySize returns the bytes one element occupies inside a listpack: its
// encoding plus the backlen field, matching lpEncodeGetType followed by
// lpEncodeBacklen. A value that parses as an int64 the way lpStringToInt64 does
// takes the compact integer encoding, anything else a string encoding sized by
// its length.
func lpEntrySize(e []byte) int {
	enc := lpEncodingSize(e)
	return enc + lpBacklenSize(enc)
}

// lpEncodingSize returns the size of an element's listpack encoding, the type
// byte or bytes plus the payload, before the backlen.
func lpEncodingSize(e []byte) int {
	if v, ok := lpTryInteger(e); ok {
		switch {
		case v >= 0 && v <= 127:
			return 1 // 7-bit unsigned
		case v >= -4096 && v <= 4095:
			return 2 // 13-bit
		case v >= -32768 && v <= 32767:
			return 3 // 16-bit
		case v >= -8388608 && v <= 8388607:
			return 4 // 24-bit
		case v >= -2147483648 && v <= 2147483647:
			return 5 // 32-bit
		default:
			return 9 // 64-bit
		}
	}
	n := len(e)
	switch {
	case n < 64:
		return 1 + n // 6-bit string length
	case n < 4096:
		return 2 + n // 12-bit string length
	default:
		return 5 + n // 32-bit string length
	}
}

// lpBacklenSize returns the number of bytes lpEncodeBacklen uses to store an
// entry whose encoding is encLen bytes long. The backlen lets a listpack be
// walked backwards and grows one byte per 7 bits of entry length.
func lpBacklenSize(encLen int) int {
	switch {
	case encLen <= 127:
		return 1
	case encLen < 16384:
		return 2
	case encLen < 2097152:
		return 3
	case encLen < 268435456:
		return 4
	default:
		return 5
	}
}

// lpTryInteger reports whether e is the canonical decimal form of an int64, the
// test lpStringToInt64 makes before storing an element as an integer. The
// round-trip check rejects leading zeros, a leading plus, surrounding spaces and
// any other non-canonical spelling, so "10" is an integer but "010", "+10" and
// "10\n" are strings, exactly as Redis decides.
func lpTryInteger(e []byte) (int64, bool) {
	if len(e) == 0 || len(e) > 20 {
		return 0, false
	}
	v, err := strconv.ParseInt(string(e), 10, 64)
	if err != nil {
		return 0, false
	}
	if strconv.FormatInt(v, 10) != string(e) {
		return 0, false
	}
	return v, true
}

// listBodyMetrics walks a stored list blob once without allocating and returns
// its element count and the exact listpack byte size those elements would take.
// The body is uvarint(count) followed by each element as uvarint(len)+bytes, so
// the walk reads each length prefix and adds the element's lpEntrySize, giving
// the same total listpackBytes would compute from a decoded slice. The push fast
// path uses it to pick the reported encoding without decoding the whole list.
func listBodyMetrics(body []byte) (count, lpBytes int, err error) {
	if len(body) == 0 {
		return 0, lpHeaderBytes, nil
	}
	n, off, err := encoding.Uvarint(body)
	if err != nil {
		return 0, 0, err
	}
	lp := lpHeaderBytes
	for range n {
		l, m, err := encoding.Uvarint(body[off:])
		if err != nil {
			return 0, 0, err
		}
		off += m
		if off+int(l) > len(body) {
			return 0, 0, errCorruptList
		}
		lp += lpEntrySize(body[off : off+int(l)])
		off += int(l)
	}
	return int(n), lp, nil
}
