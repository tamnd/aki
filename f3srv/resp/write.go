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

// The RESP3 reply builders (spec 2064/f3/08 section 3.4, doc 17 section 19). A
// connection that negotiated RESP3 through HELLO 3 gets these frame types where
// its RESP2 sibling gets a flat array or a bulk-string number; the reply writer
// picks the builder from the connection's protocol version, so the shape is built
// once in wire form, never converted after (F19). Each mirrors its RESP2 partner's
// presizing so a RESP3 reply into a warm buffer allocates nothing either.

// AppendNull3 appends the RESP3 null: _\r\n, the one null both a bulk and an array
// collapse to under RESP3 (RESP2 keeps the distinct $-1 and *-1 forms).
func AppendNull3(dst []byte) []byte {
	return append(dst, '_', '\r', '\n')
}

// AppendMapHeader appends a RESP3 map header: %n\r\n, where n is the pair count
// (not the element count). The caller then appends exactly 2n elements, key then
// value, the same pairs a RESP2 flat array of 2n carries; only the header byte and
// the count semantics differ, which is why the RESP2 side passes 2n to
// AppendArrayHeader and the RESP3 side passes n here.
func AppendMapHeader(dst []byte, n int) []byte {
	dst = slices.Grow(dst, 23)
	dst = append(dst, '%')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

// AppendSetHeader appends a RESP3 set header: ~n\r\n. Wire-identical to an array
// but for the leading byte, so SMEMBERS and the set-algebra replies swap the array
// header for this one under RESP3 and append their members unchanged.
func AppendSetHeader(dst []byte, n int) []byte {
	dst = slices.Grow(dst, 23)
	dst = append(dst, '~')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

// AppendPushHeader appends a RESP3 push header: >n\r\n. Out-of-band frames (pubsub
// messages, subscribe confirmations, keyspace notifications, invalidations) carry
// this header under RESP3 so a subscribed connection stays usable for normal
// commands, the RESP2 array header they carry otherwise being indistinguishable
// from a command reply on the same stream.
func AppendPushHeader(dst []byte, n int) []byte {
	dst = slices.Grow(dst, 23)
	dst = append(dst, '>')
	dst = strconv.AppendInt(dst, int64(n), 10)
	return append(dst, '\r', '\n')
}

// AppendDouble appends a RESP3 double: ,<value>\r\n. The value bytes are Redis's
// own score formatting (FormatScore, the d2string port), so ZSCORE and the
// withscores pairs carry the same digits under both protocols, only the framing
// differing: a RESP2 bulk string versus this native double. inf/-inf/nan spell out
// through FormatScore the way Redis's addReplyDouble spells them.
func AppendDouble(dst []byte, value float64) []byte {
	dst = append(dst, ',')
	dst = FormatScore(dst, value)
	return append(dst, '\r', '\n')
}

// AppendDoubleBytes appends a RESP3 double from already-formatted digits:
// ,<digits>\r\n. It is AppendDouble for a caller that computed its own decimal
// text (INCRBYFLOAT's long-double digits), so the RESP3 double carries the exact
// bytes the RESP2 bulk string would.
func AppendDoubleBytes(dst, digits []byte) []byte {
	dst = slices.Grow(dst, len(digits)+3)
	dst = append(dst, ',')
	dst = append(dst, digits...)
	return append(dst, '\r', '\n')
}

// AppendBool appends a RESP3 boolean: #t\r\n or #f\r\n. The predicate commands
// (SISMEMBER, the EXPIRE family, SETNX and its siblings) answer 0/1 as an integer
// under RESP2 and this boolean under RESP3, matching Redis's addReplyBool.
func AppendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, '#', 't', '\r', '\n')
	}
	return append(dst, '#', 'f', '\r', '\n')
}

// AppendBigNumber appends a RESP3 big number: (<digits>\r\n. The classic types
// never produce one; it exists so a command that must echo an integer wider than
// 64 bits can, matching Redis's addReplyBigNum. digits is the caller's already
// validated decimal (optionally sign-prefixed) text.
func AppendBigNumber(dst []byte, digits string) []byte {
	dst = slices.Grow(dst, len(digits)+3)
	dst = append(dst, '(')
	dst = append(dst, digits...)
	return append(dst, '\r', '\n')
}

// AppendVerbatim appends a RESP3 verbatim string: =<len>\r\n<fmt>:<payload>\r\n,
// where fmt is the three-byte format hint (Redis uses "txt" and "mkd"). The
// declared length counts the "fmt:" prefix plus the payload, the four extra bytes
// over a plain bulk of the same payload.
func AppendVerbatim(dst []byte, format string, payload []byte) []byte {
	body := len(format) + 1 + len(payload)
	hl := declen(uint64(body))
	total := 1 + hl + 2 + body + 2
	n := len(dst)
	dst = slices.Grow(dst, total)[:n+total]
	dst[n] = '='
	putDecimal(dst[n+1:n+1+hl], uint64(body))
	dst[n+1+hl] = '\r'
	dst[n+2+hl] = '\n'
	p := n + 3 + hl
	p += copy(dst[p:], format)
	dst[p] = ':'
	p++
	p += copy(dst[p:], payload)
	dst[p] = '\r'
	dst[p+1] = '\n'
	return dst
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
