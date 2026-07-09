package bulkarray

import (
	"bytes"
	"strconv"
	"testing"
)

// members builds n RESP bulk payloads shaped like real set members (a short "member:<i>" key), the
// arena subslices the algebra reply frames. The exact bytes do not matter, only that they are the
// short, variable-length values a set holds so the length header is one to a few digits.
func members(n int) [][]byte {
	out := make([][]byte, n)
	for i := range n {
		out[i] = []byte("member:" + strconv.Itoa(i))
	}
	return out
}

// writeBulkAppend is the current f1srv shape: one bulk string per member, five appends each (the '$',
// the decimal length, the header CRLF, the payload, the trailing CRLF).
func writeBulkAppend(out []byte, ms [][]byte) []byte {
	out = append(out, '*')
	out = strconv.AppendInt(out, int64(len(ms)), 10)
	out = append(out, '\r', '\n')
	for _, b := range ms {
		out = append(out, '$')
		out = strconv.AppendInt(out, int64(len(b)), 10)
		out = append(out, '\r', '\n')
		out = append(out, b...)
		out = append(out, '\r', '\n')
	}
	return out
}

// declen returns the number of decimal digits in a non-negative length, the width of a bulk header's
// count field. Members are never empty in these replies but the zero case is handled for safety.
func declen(n int) int {
	if n == 0 {
		return 1
	}
	d := 0
	for n > 0 {
		d++
		n /= 10
	}
	return d
}

// writeBulkFused frames the whole array reply in one growth: it sums the exact byte length of the
// header and every member's bulk framing, grows out once to fit, then fills with a moving index so
// each member costs a length encode and two copies with no per-append grow check or call.
func writeBulkFused(out []byte, ms [][]byte) []byte {
	total := 1 + declen(len(ms)) + 2 // '*' + count + CRLF
	for _, b := range ms {
		total += 1 + declen(len(b)) + 2 + len(b) + 2 // '$' + len + CRLF + payload + CRLF
	}
	base := len(out)
	if cap(out)-base < total {
		grown := make([]byte, base, base+total)
		copy(grown, out)
		out = grown
	}
	out = out[:base+total]
	p := base
	out[p] = '*'
	p++
	p += putUint(out[p:], len(ms))
	out[p] = '\r'
	out[p+1] = '\n'
	p += 2
	for _, b := range ms {
		out[p] = '$'
		p++
		p += putUint(out[p:], len(b))
		out[p] = '\r'
		out[p+1] = '\n'
		p += 2
		p += copy(out[p:], b)
		out[p] = '\r'
		out[p+1] = '\n'
		p += 2
	}
	return out
}

// putUint writes n as decimal into the front of dst and returns the digit count. dst is guaranteed to
// hold declen(n) bytes by the caller's size pass.
func putUint(dst []byte, n int) int {
	if n == 0 {
		dst[0] = '0'
		return 1
	}
	d := declen(n)
	for i := d - 1; i >= 0; i-- {
		dst[i] = byte('0' + n%10)
		n /= 10
	}
	return d
}

func TestFusedMatchesAppend(t *testing.T) {
	for _, n := range []int{0, 1, 9, 10, 99, 100, 256, 1000} {
		ms := members(n)
		a := writeBulkAppend(nil, ms)
		f := writeBulkFused(nil, ms)
		if !bytes.Equal(a, f) {
			t.Fatalf("n=%d: fused reply differs from append reply\n append=%q\n fused =%q", n, a, f)
		}
	}
	// Fused must also respect a non-empty prefix already in the buffer (a pipelined reply).
	pre := []byte("+OK\r\n")
	ms := members(50)
	a := writeBulkAppend(append([]byte(nil), pre...), ms)
	f := writeBulkFused(append([]byte(nil), pre...), ms)
	if !bytes.Equal(a, f) {
		t.Fatalf("with prefix: fused reply differs from append reply")
	}
}

var sink []byte

func benchWrite(b *testing.B, n int, fn func(out []byte, ms [][]byte) []byte) {
	ms := members(n)
	buf := make([]byte, 0, 1<<16)
	b.ReportAllocs()
	for b.Loop() {
		sink = fn(buf[:0], ms)
	}
}

func BenchmarkAppend8(b *testing.B)    { benchWrite(b, 8, writeBulkAppend) }
func BenchmarkFused8(b *testing.B)     { benchWrite(b, 8, writeBulkFused) }
func BenchmarkAppend64(b *testing.B)   { benchWrite(b, 64, writeBulkAppend) }
func BenchmarkFused64(b *testing.B)    { benchWrite(b, 64, writeBulkFused) }
func BenchmarkAppend256(b *testing.B)  { benchWrite(b, 256, writeBulkAppend) }
func BenchmarkFused256(b *testing.B)   { benchWrite(b, 256, writeBulkFused) }
func BenchmarkAppend1024(b *testing.B) { benchWrite(b, 1024, writeBulkAppend) }
func BenchmarkFused1024(b *testing.B)  { benchWrite(b, 1024, writeBulkFused) }
