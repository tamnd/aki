package bench_test

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/resp"
)

// BenchmarkEncodeSimpleString encodes a +OK status reply.
func BenchmarkEncodeSimpleString(b *testing.B) {
	var buf bytes.Buffer
	e := resp.NewEncoder(&buf, 2)
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		e.WriteStatus("OK")
	}
}

// BenchmarkEncodeError encodes an error reply.
func BenchmarkEncodeError(b *testing.B) {
	var buf bytes.Buffer
	e := resp.NewEncoder(&buf, 2)
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		e.WriteError("ERR unknown command")
	}
}

// BenchmarkEncodeBulkString64 encodes a 64-byte bulk string.
func BenchmarkEncodeBulkString64(b *testing.B) {
	var buf bytes.Buffer
	e := resp.NewEncoder(&buf, 2)
	data := bytes.Repeat([]byte("x"), 64)
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		e.WriteBulkString(data)
	}
}

// BenchmarkEncodeInteger encodes an integer reply.
func BenchmarkEncodeInteger(b *testing.B) {
	var buf bytes.Buffer
	e := resp.NewEncoder(&buf, 2)
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		e.WriteInteger(12345)
	}
}

// BenchmarkEncodeArray5 encodes a five-element array of bulk strings.
func BenchmarkEncodeArray5(b *testing.B) {
	var buf bytes.Buffer
	e := resp.NewEncoder(&buf, 2)
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		e.WriteArrayLen(len(items))
		for _, it := range items {
			e.WriteBulkString(it)
		}
	}
}

// BenchmarkEncodeNull encodes a null reply (the RESP2 $-1 form here).
func BenchmarkEncodeNull(b *testing.B) {
	var buf bytes.Buffer
	e := resp.NewEncoder(&buf, 2)
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		e.WriteNull()
	}
}
