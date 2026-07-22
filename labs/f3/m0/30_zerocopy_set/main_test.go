package main

import (
	"bytes"
	"testing"
)

// TestSetEquivalence asserts the one-copy path lands the same value bytes in
// the arena the two-copy path does, across the band boundary, so the elision is
// proven safe before its throughput is read.
func TestSetEquivalence(t *testing.T) {
	for _, n := range []int{1, 64, 512, 1024, 1025, 4096, 16384} {
		src, keyLen, valOff := synthCmd(n)
		var a twoCopy
		var b oneCopy
		a.set(src, keyLen, valOff, n)
		b.set(src, keyLen, valOff, n)
		if !bytes.Equal(a.arena[:n], b.arena[:n]) {
			t.Fatalf("n=%d: one-copy arena != two-copy arena", n)
		}
		if !bytes.Equal(a.arena[:n], src[valOff:valOff+n]) {
			t.Fatalf("n=%d: arena != source value", n)
		}
	}
}

func benchSet(b *testing.B, n int, one bool) {
	src, keyLen, valOff := synthCmd(n)
	var tc twoCopy
	var oc oneCopy
	// warm the reused buffers to steady capacity
	tc.set(src, keyLen, valOff, n)
	oc.set(src, keyLen, valOff, n)
	b.SetBytes(int64(n))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if one {
			oc.set(src, keyLen, valOff, n)
		} else {
			tc.set(src, keyLen, valOff, n)
		}
	}
}

func BenchmarkTwoCopy64(b *testing.B)    { benchSet(b, 64, false) }
func BenchmarkOneCopy64(b *testing.B)    { benchSet(b, 64, true) }
func BenchmarkTwoCopy1024(b *testing.B)  { benchSet(b, 1024, false) }
func BenchmarkOneCopy1024(b *testing.B)  { benchSet(b, 1024, true) }
func BenchmarkTwoCopy4096(b *testing.B)  { benchSet(b, 4096, false) }
func BenchmarkOneCopy4096(b *testing.B)  { benchSet(b, 4096, true) }
func BenchmarkTwoCopy16384(b *testing.B) { benchSet(b, 16384, false) }
func BenchmarkOneCopy16384(b *testing.B) { benchSet(b, 16384, true) }
