package resp

import (
	"slices"
	"strconv"
)

// The F19 reply builders (spec 2064/f3/08 section 3): every reply is built
// once, in RESP2 wire form, appended to the caller's buffer in a single pass.
// Each emitter computes its exact byte count up front and grows the buffer at
// most once, so a reply into a warm buffer allocates nothing and touches every
// byte exactly once. No emitter takes an interface value; there is no reply
// tree to walk and nothing to box.

// AppendStatus appends a simple string reply: +s\r\n.
func AppendStatus(dst []byte, s string) []byte {
	dst = slices.Grow(dst, len(s)+3)
	dst = append(dst, '+')
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

// AppendError appends an error reply: -msg\r\n. The message carries its own
// code prefix ("ERR ...", "WRONGTYPE ...").
func AppendError(dst []byte, msg string) []byte {
	dst = slices.Grow(dst, len(msg)+3)
	dst = append(dst, '-')
	dst = append(dst, msg...)
	return append(dst, '\r', '\n')
}

// AppendErrorBytes is AppendError for a message already in byte form.
func AppendErrorBytes(dst, msg []byte) []byte {
	dst = slices.Grow(dst, len(msg)+3)
	dst = append(dst, '-')
	dst = append(dst, msg...)
	return append(dst, '\r', '\n')
}

// AppendInt appends an integer reply: :n\r\n.
func AppendInt(dst []byte, n int64) []byte {
	dst = slices.Grow(dst, 23)
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, n, 10)
	return append(dst, '\r', '\n')
}

// AppendBulk appends a bulk string reply, presized: the header length is
// computed from the payload length, the buffer grows once to the exact total,
// and header, payload, and terminator land in one pass.
func AppendBulk(dst, v []byte) []byte {
	hl := declen(uint64(len(v)))
	total := 1 + hl + 2 + len(v) + 2
	n := len(dst)
	dst = slices.Grow(dst, total)[:n+total]
	dst[n] = '$'
	putDecimal(dst[n+1:n+1+hl], uint64(len(v)))
	dst[n+1+hl] = '\r'
	dst[n+2+hl] = '\n'
	copy(dst[n+3+hl:], v)
	dst[n+total-2] = '\r'
	dst[n+total-1] = '\n'
	return dst
}

// AppendNull appends the RESP2 null bulk: $-1\r\n.
func AppendNull(dst []byte) []byte {
	return append(dst, '$', '-', '1', '\r', '\n')
}

// AppendNullArray appends the RESP2 null array: *-1\r\n.
func AppendNullArray(dst []byte) []byte {
	return append(dst, '*', '-', '1', '\r', '\n')
}

// AppendArrayHeader appends an array header: *n\r\n. The caller then appends
// exactly n elements; presizing from known cardinality is the caller's move,
// this just writes the header.
func AppendArrayHeader(dst []byte, n int) []byte {
	dst = slices.Grow(dst, 23)
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

// declen is the decimal digit count of u.
func declen(u uint64) int {
	n := 1
	for u >= 10 {
		u /= 10
		n++
	}
	return n
}

// putDecimal writes u right-aligned into dst, whose length is exactly
// declen(u).
func putDecimal(dst []byte, u uint64) {
	i := len(dst)
	for {
		i--
		dst[i] = byte('0' + u%10)
		u /= 10
		if u == 0 {
			return
		}
	}
}
