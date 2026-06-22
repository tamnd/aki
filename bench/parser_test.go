package bench_test

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/resp"
)

// multibulk builds a RESP multibulk command from its arguments, the wire form a
// client sends and the parser hot path decodes.
func multibulk(args ...string) []byte {
	var b bytes.Buffer
	b.WriteByte('*')
	b.WriteString(itoaBench(len(args)))
	b.WriteString("\r\n")
	for _, a := range args {
		b.WriteByte('$')
		b.WriteString(itoaBench(len(a)))
		b.WriteString("\r\n")
		b.WriteString(a)
		b.WriteString("\r\n")
	}
	return b.Bytes()
}

func itoaBench(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// BenchmarkParseMultibulkGet decodes a two-argument GET, the most common read.
func BenchmarkParseMultibulkGet(b *testing.B) {
	buf := multibulk("GET", "hello")
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := resp.Decode(buf, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseMultibulkSet64 decodes a SET with a 64-byte value.
func BenchmarkParseMultibulkSet64(b *testing.B) {
	buf := multibulk("SET", "key", string(bytes.Repeat([]byte("x"), 64)))
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := resp.Decode(buf, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseMultibulkSet1k decodes a SET with a 1024-byte value.
func BenchmarkParseMultibulkSet1k(b *testing.B) {
	buf := multibulk("SET", "key", string(bytes.Repeat([]byte("x"), 1024)))
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := resp.Decode(buf, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParsePipeline16 decodes 16 GET commands back to back from one buffer.
func BenchmarkParsePipeline16(b *testing.B) {
	benchPipeline(b, 16)
}

// BenchmarkParsePipeline256 decodes 256 GET commands from one buffer.
func BenchmarkParsePipeline256(b *testing.B) {
	benchPipeline(b, 256)
}

func benchPipeline(b *testing.B, n int) {
	one := multibulk("GET", "hello")
	var buf []byte
	for range n {
		buf = append(buf, one...)
	}
	b.ReportAllocs()
	for b.Loop() {
		pos := 0
		for pos < len(buf) {
			_, next, err := resp.Decode(buf, pos)
			if err != nil {
				b.Fatal(err)
			}
			pos = next
		}
	}
}
