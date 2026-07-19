package main

import (
	"bytes"
	"testing"
)

// TestScoreKernelsIdentical proves the withScore and skipScore kernels emit
// byte-identical member RESP for every window over the leaf, so skipping the
// discarded score load is a pure performance change.
func TestScoreKernelsIdentical(t *testing.T) {
	const mlen = 16
	l := build(4096, mlen)
	for _, w := range []int{1, 2, 10, 100, 1000} {
		for lo := 0; lo+w <= 4096; lo += w {
			a := l.withScore(nil, lo, w)
			b := l.skipScore(nil, lo, w)
			if !bytes.Equal(a, b) {
				t.Fatalf("window [%d,%d): withScore and skipScore differ", lo, lo+w)
			}
		}
	}
}

// TestSkipReadsNoScore is a guard on the model: the whole point is that skipScore
// never touches the score array, so a leaf with a poisoned score array must still
// produce the same member bytes.
func TestSkipReadsNoScore(t *testing.T) {
	const mlen = 16
	l := build(1024, mlen)
	good := l.skipScore(nil, 0, 1024)
	for i := range l.ent {
		if i%entrySz < 8 { // clobber only the score bytes, not the ref/reserved
			l.ent[i] = 0xff
		}
	}
	poisoned := l.skipScore(nil, 0, 1024)
	if !bytes.Equal(good, poisoned) {
		t.Fatal("skipScore output depends on the score array, model is wrong")
	}
}

func BenchmarkWithScore(b *testing.B) {
	l := build(1_000_000, 16)
	buf := make([]byte, 0, 1<<18)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = l.withScore(buf[:0], (i*100)%(1_000_000-100), 100)
	}
	sink += uint64(len(buf))
}

func BenchmarkSkipScore(b *testing.B) {
	l := build(1_000_000, 16)
	buf := make([]byte, 0, 1<<18)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = l.skipScore(buf[:0], (i*100)%(1_000_000-100), 100)
	}
	sink += uint64(len(buf))
}
