package resp

import (
	"bytes"
	"math"
	"math/big"
	"testing"
)

// encode runs fn against a fresh encoder at the given proto version and returns
// the bytes it produced.
func encode(proto int, fn func(e *Encoder)) string {
	var buf bytes.Buffer
	fn(NewEncoder(&buf, proto))
	return buf.String()
}

func TestEncoderScalars(t *testing.T) {
	cases := []struct {
		name  string
		proto int
		fn    func(e *Encoder)
		want  string
	}{
		{"status", 2, func(e *Encoder) { e.WriteStatus("OK") }, "+OK\r\n"},
		{"error", 2, func(e *Encoder) { e.WriteError("ERR nope") }, "-ERR nope\r\n"},
		{"int small pooled", 2, func(e *Encoder) { e.WriteInteger(1) }, ":1\r\n"},
		{"int negative pooled", 2, func(e *Encoder) { e.WriteInteger(-1) }, ":-1\r\n"},
		{"int large", 2, func(e *Encoder) { e.WriteInteger(1 << 40) }, ":1099511627776\r\n"},
		{"bulk", 2, func(e *Encoder) { e.WriteBulkStringStr("hello") }, "$5\r\nhello\r\n"},
		{"empty bulk", 2, func(e *Encoder) { e.WriteBulkStringStr("") }, "$0\r\n\r\n"},
		{"null resp2", 2, func(e *Encoder) { e.WriteNull() }, "$-1\r\n"},
		{"null resp3", 3, func(e *Encoder) { e.WriteNull() }, "_\r\n"},
		{"null array resp2", 2, func(e *Encoder) { e.WriteNullArray() }, "*-1\r\n"},
		{"null array resp3", 3, func(e *Encoder) { e.WriteNullArray() }, "_\r\n"},
		{"bool true resp2", 2, func(e *Encoder) { e.WriteBool(true) }, ":1\r\n"},
		{"bool false resp2", 2, func(e *Encoder) { e.WriteBool(false) }, ":0\r\n"},
		{"bool true resp3", 3, func(e *Encoder) { e.WriteBool(true) }, "#t\r\n"},
		{"bool false resp3", 3, func(e *Encoder) { e.WriteBool(false) }, "#f\r\n"},
		// Doubles use %.17g (FormatFloat 'g',17) for byte-identity with Redis,
		// which is why 3.14 renders with its full 17-significant-digit form.
		{"double resp2", 2, func(e *Encoder) { e.WriteDouble(3.14) }, "$18\r\n3.1400000000000001\r\n"},
		{"double resp3", 3, func(e *Encoder) { e.WriteDouble(3.14) }, ",3.1400000000000001\r\n"},
		{"double whole resp3", 3, func(e *Encoder) { e.WriteDouble(5) }, ",5\r\n"},
		{"double inf resp3", 3, func(e *Encoder) { e.WriteDouble(math.Inf(1)) }, ",inf\r\n"},
		{"double -inf resp3", 3, func(e *Encoder) { e.WriteDouble(math.Inf(-1)) }, ",-inf\r\n"},
		// "txt:" (4) + "Hello World!" (12) = 16 payload bytes.
		{"verbatim resp3", 3, func(e *Encoder) { e.WriteVerbatimString("txt", []byte("Hello World!")) }, "=16\r\ntxt:Hello World!\r\n"},
		{"verbatim resp2", 2, func(e *Encoder) { e.WriteVerbatimString("txt", []byte("Hi")) }, "$2\r\nHi\r\n"},
		{"bignum resp3", 3, func(e *Encoder) { e.WriteBigNumber(big.NewInt(-1)) }, "(-1\r\n"},
		{"bignum resp2", 2, func(e *Encoder) { e.WriteBigNumber(big.NewInt(42)) }, "$2\r\n42\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := encode(tc.proto, tc.fn); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEncoderAggregates(t *testing.T) {
	// HGETALL-shaped reply: a map of two pairs differs only in the header byte.
	pairs := func(e *Encoder) {
		e.WriteMapLen(2)
		e.WriteBulkStringStr("name")
		e.WriteBulkStringStr("Alice")
		e.WriteBulkStringStr("age")
		e.WriteBulkStringStr("30")
	}
	want2 := "*4\r\n$4\r\nname\r\n$5\r\nAlice\r\n$3\r\nage\r\n$2\r\n30\r\n"
	want3 := "%2\r\n$4\r\nname\r\n$5\r\nAlice\r\n$3\r\nage\r\n$2\r\n30\r\n"
	if got := encode(2, pairs); got != want2 {
		t.Fatalf("map resp2: got %q want %q", got, want2)
	}
	if got := encode(3, pairs); got != want3 {
		t.Fatalf("map resp3: got %q want %q", got, want3)
	}

	set := func(e *Encoder) {
		e.WriteSetLen(2)
		e.WriteBulkStringStr("a")
		e.WriteBulkStringStr("b")
	}
	if got := encode(2, set); got != "*2\r\n$1\r\na\r\n$1\r\nb\r\n" {
		t.Fatalf("set resp2: got %q", got)
	}
	if got := encode(3, set); got != "~2\r\n$1\r\na\r\n$1\r\nb\r\n" {
		t.Fatalf("set resp3: got %q", got)
	}

	push := func(e *Encoder) {
		e.WritePushLen(3)
		e.WriteBulkStringStr("subscribe")
		e.WriteBulkStringStr("news")
		e.WriteInteger(1)
	}
	if got := encode(2, push); got != "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n" {
		t.Fatalf("push resp2: got %q", got)
	}
	if got := encode(3, push); got != ">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n" {
		t.Fatalf("push resp3: got %q", got)
	}
}

func TestEncoderBulkError(t *testing.T) {
	// A short single-line error stays a simple error even in RESP3.
	if got := encode(3, func(e *Encoder) { e.WriteBulkError("ERR nope") }); got != "-ERR nope\r\n" {
		t.Fatalf("short bulk error: got %q", got)
	}
	// An error containing a newline must use the ! type in RESP3.
	if got := encode(3, func(e *Encoder) { e.WriteBulkError("a\nb") }); got != "!3\r\na\nb\r\n" {
		t.Fatalf("multiline bulk error: got %q", got)
	}
	// In RESP2 it always downgrades, dropping the newline-safety (Redis does the
	// same: bulk errors do not exist on RESP2).
	if got := encode(2, func(e *Encoder) { e.WriteBulkError("ERR x") }); got != "-ERR x\r\n" {
		t.Fatalf("resp2 bulk error: got %q", got)
	}
}

func TestEncoderStreamedBulk(t *testing.T) {
	got := encode(3, func(e *Encoder) {
		e.BeginStreamedBulkString()
		e.WriteChunk([]byte("hello"))
		e.WriteChunk([]byte(" world"))
		e.EndStreamedBulkString()
	})
	want := "$?\r\n;5\r\nhello\r\n;6\r\n world\r\n;0\r\n"
	if got != want {
		t.Fatalf("streamed bulk: got %q want %q", got, want)
	}
}

func TestEncoderBinarySafeBulk(t *testing.T) {
	// CRLF inside a payload must pass through untouched.
	got := encode(2, func(e *Encoder) { e.WriteBulkString([]byte("a\r\nb")) })
	if got != "$4\r\na\r\nb\r\n" {
		t.Fatalf("binary bulk: got %q", got)
	}
}

func TestFormatDouble(t *testing.T) {
	cases := map[float64]string{
		math.Inf(1):  "inf",
		math.Inf(-1): "-inf",
		math.NaN():   "nan",
		0:            "0",
		1.5:          "1.5",
	}
	for in, want := range cases {
		if got := FormatDouble(in); got != want {
			t.Fatalf("FormatDouble(%v) = %q want %q", in, got, want)
		}
	}
}

// sliceWriter is a Writer that is not a *bytes.Buffer, so WriteBulkArray takes
// its per-element fallback path. It lets the test prove both paths produce the
// same wire bytes.
type sliceWriter struct{ b []byte }

func (w *sliceWriter) WriteString(s string) (int, error) { w.b = append(w.b, s...); return len(s), nil }
func (w *sliceWriter) WriteByte(c byte) error            { w.b = append(w.b, c); return nil }
func (w *sliceWriter) Write(p []byte) (int, error)       { w.b = append(w.b, p...); return len(p), nil }

func TestWriteBulkArray(t *testing.T) {
	cases := [][][]byte{
		nil,
		{},
		{[]byte("")},
		{[]byte("a")},
		{[]byte("alpha"), []byte("beta"), []byte("gamma")},
		{[]byte("x"), nil, []byte("z")},
		{bytes.Repeat([]byte("q"), 1234)},
	}
	for _, items := range cases {
		// The hand-written array header plus per-element WriteBulkString loop is the
		// reference shape every multi-bulk reader uses today.
		var want bytes.Buffer
		ref := NewEncoder(&want, 2)
		ref.WriteArrayLen(len(items))
		for _, it := range items {
			ref.WriteBulkString(it)
		}

		// Fast path: the sink is a *bytes.Buffer.
		var fast bytes.Buffer
		NewEncoder(&fast, 2).WriteBulkArray(items)
		if !bytes.Equal(fast.Bytes(), want.Bytes()) {
			t.Fatalf("fast path mismatch for %d items: got %q want %q", len(items), fast.Bytes(), want.Bytes())
		}

		// Fallback path: a Writer that is not a *bytes.Buffer.
		slow := &sliceWriter{}
		NewEncoder(slow, 2).WriteBulkArray(items)
		if !bytes.Equal(slow.b, want.Bytes()) {
			t.Fatalf("fallback path mismatch for %d items: got %q want %q", len(items), slow.b, want.Bytes())
		}
	}
}

// BenchmarkBulkReply100 compares the batched WriteBulkArray against the manual
// array-header-plus-loop on a 100-element reply of short values, the exact shape
// redis-benchmark LRANGE_100 produces.
func BenchmarkBulkReply100(b *testing.B) {
	items := make([][]byte, 100)
	for i := range items {
		items[i] = []byte("xxx")
	}
	b.Run("loop", func(b *testing.B) {
		var buf bytes.Buffer
		e := NewEncoder(&buf, 2)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			e.WriteArrayLen(len(items))
			for _, it := range items {
				e.WriteBulkString(it)
			}
		}
	})
	b.Run("array", func(b *testing.B) {
		var buf bytes.Buffer
		e := NewEncoder(&buf, 2)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			e.WriteBulkArray(items)
		}
	})
}
