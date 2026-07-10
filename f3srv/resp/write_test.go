package resp

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"
)

func TestAppendEmitters(t *testing.T) {
	cases := []struct {
		got  []byte
		want string
	}{
		{AppendStatus(nil, "OK"), "+OK\r\n"},
		{AppendError(nil, "ERR syntax error"), "-ERR syntax error\r\n"},
		{AppendErrorBytes(nil, []byte("ERR nope")), "-ERR nope\r\n"},
		{AppendInt(nil, 0), ":0\r\n"},
		{AppendInt(nil, -42), ":-42\r\n"},
		{AppendInt(nil, math.MaxInt64), ":9223372036854775807\r\n"},
		{AppendInt(nil, math.MinInt64), ":-9223372036854775808\r\n"},
		{AppendBulk(nil, nil), "$0\r\n\r\n"},
		{AppendBulk(nil, []byte("hello")), "$5\r\nhello\r\n"},
		{AppendNull(nil), "$-1\r\n"},
		{AppendNullArray(nil), "*-1\r\n"},
		{AppendArrayHeader(nil, 3), "*3\r\n"},
	}
	for _, c := range cases {
		if string(c.got) != c.want {
			t.Fatalf("emitted %q, want %q", c.got, c.want)
		}
	}
}

func TestAppendBulkLengths(t *testing.T) {
	// Cross lengths that step the header digit count, against the naive form.
	for _, n := range []int{0, 1, 9, 10, 99, 100, 999, 1000, 65535, 100000} {
		v := bytes.Repeat([]byte{'x'}, n)
		want := fmt.Sprintf("$%d\r\n%s\r\n", n, v)
		if got := AppendBulk(nil, v); string(got) != want {
			t.Fatalf("len %d: got %d bytes, want %d", n, len(got), len(want))
		}
	}
}

func TestAppendChains(t *testing.T) {
	// Emitters append; a pipeline of replies lands in one buffer in order.
	b := AppendStatus(nil, "OK")
	b = AppendBulk(b, []byte("v"))
	b = AppendInt(b, 7)
	b = AppendNull(b)
	if string(b) != "+OK\r\n$1\r\nv\r\n:7\r\n$-1\r\n" {
		t.Fatalf("chained buffer = %q", b)
	}
}

// TestAppendZeroAllocWarm pins the single-pass discipline: into a buffer with
// capacity, no emitter allocates.
func TestAppendZeroAllocWarm(t *testing.T) {
	buf := make([]byte, 0, 4096)
	payload := []byte("some payload bytes")
	allocs := testing.AllocsPerRun(200, func() {
		buf = buf[:0]
		buf = AppendStatus(buf, "OK")
		buf = AppendError(buf, "ERR syntax error")
		buf = AppendInt(buf, 123456789)
		buf = AppendBulk(buf, payload)
		buf = AppendNull(buf)
		buf = AppendNullArray(buf)
		buf = AppendArrayHeader(buf, 12)
	})
	if allocs != 0 {
		t.Fatalf("warm emitters allocate %.1f allocs/op, want 0", allocs)
	}
}

func TestDeclen(t *testing.T) {
	for _, u := range []uint64{0, 1, 9, 10, 99, 100, 12345, math.MaxUint64} {
		want := len(strconv.FormatUint(u, 10))
		if got := declen(u); got != want {
			t.Fatalf("declen(%d) = %d, want %d", u, got, want)
		}
	}
}
