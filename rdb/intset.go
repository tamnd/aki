package rdb

import (
	"slices"
	"strconv"

	"github.com/tamnd/aki/encoding"
)

// intset is Redis's encoding for a set whose every member is an integer. The blob
// is a 4-byte element width (2, 4, or 8 bytes), a 4-byte count, then the sorted
// little-endian integers. aki uses it when a set is all integers and small enough,
// which is what real Redis does and what makes the dump bytes match.

// intsetEncodable reports the integer values of members when every member is the
// canonical decimal form of an integer. The second result is false otherwise.
func intsetEncodable(members [][]byte) ([]int64, bool) {
	vals := make([]int64, len(members))
	for i, m := range members {
		v, ok := asInt(m)
		if !ok {
			return nil, false
		}
		vals[i] = v
	}
	return vals, true
}

// intsetEncode builds the intset blob from integer values. The width is the
// smallest that holds every value, and the values are sorted ascending as the
// format requires.
func intsetEncode(vals []int64) []byte {
	slices.Sort(vals)
	width := 2
	for _, v := range vals {
		switch {
		case v < -2147483648 || v > 2147483647:
			width = 8
		case (v < -32768 || v > 32767) && width < 4:
			width = 4
		}
	}
	out := make([]byte, 0, 8+len(vals)*width)
	out = encoding.AppendU32(out, uint32(width))
	out = encoding.AppendU32(out, uint32(len(vals)))
	for _, v := range vals {
		switch width {
		case 2:
			out = encoding.AppendU16(out, uint16(v))
		case 4:
			out = encoding.AppendU32(out, uint32(v))
		default:
			out = encoding.AppendU64(out, uint64(v))
		}
	}
	return out
}

// intsetDecode reads an intset blob into decimal-text members.
func intsetDecode(blob []byte) ([][]byte, error) {
	if len(blob) < 8 {
		return nil, errTruncated
	}
	width := int(encoding.U32(blob[0:4]))
	count := int(encoding.U32(blob[4:8]))
	if width != 2 && width != 4 && width != 8 {
		return nil, errTruncated
	}
	if 8+count*width > len(blob) {
		return nil, errTruncated
	}
	out := make([][]byte, 0, count)
	p := 8
	for range count {
		var v int64
		switch width {
		case 2:
			v = int64(int16(encoding.U16(blob[p : p+2])))
		case 4:
			v = int64(int32(encoding.U32(blob[p : p+4])))
		default:
			v = int64(encoding.U64(blob[p : p+8]))
		}
		p += width
		out = append(out, []byte(strconv.FormatInt(v, 10)))
	}
	return out, nil
}
