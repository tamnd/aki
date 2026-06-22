package rdb

import "errors"

// errBadLZF means an LZF body referenced bytes outside what it had already
// produced, so the payload is corrupt.
var errBadLZF = errors.New("rdb: corrupt LZF stream")

// lzfDecompress expands an LZF body to outLen bytes. aki never writes LZF, but a
// payload produced by real Redis can carry it, so RESTORE must read it. The format
// is a stream of control bytes: a value under 32 is a literal run of that many
// plus one bytes, anything else is a back reference whose top three bits hold the
// match length and whose low bits plus the next byte hold the distance.
func lzfDecompress(in []byte, outLen int) ([]byte, error) {
	// outLen is read from the payload, so cap the preallocation hint. The final
	// len(out) != outLen check below still enforces the real length.
	out := make([]byte, 0, sliceHint(uint64(outLen)))
	i := 0
	for i < len(in) {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			n := ctrl + 1
			if i+n > len(in) {
				return nil, errBadLZF
			}
			out = append(out, in[i:i+n]...)
			i += n
			continue
		}
		length := ctrl >> 5
		if length == 7 {
			if i >= len(in) {
				return nil, errBadLZF
			}
			length += int(in[i])
			i++
		}
		if i >= len(in) {
			return nil, errBadLZF
		}
		ref := len(out) - ((ctrl & 0x1F) << 8) - int(in[i]) - 1
		i++
		if ref < 0 {
			return nil, errBadLZF
		}
		for range length + 2 {
			out = append(out, out[ref])
			ref++
		}
	}
	if len(out) != outLen {
		return nil, errBadLZF
	}
	return out, nil
}
