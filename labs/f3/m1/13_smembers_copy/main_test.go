package main

import (
	"bytes"
	"testing"
)

// TestFramingIdentical is the safety property the elision rests on: the direct
// encoder must emit byte-identical replies to the scratch encoder for every
// member size and every chunk size, including the sizes that straddle a chunk
// boundary. A framing change on the wire path is only safe if the bytes are
// unchanged.
func TestFramingIdentical(t *testing.T) {
	sizes := []int{1, 8, 16, 63, 64, 255, 256, 1000}
	chunks := []int{16, 64, 128, 512, 4096, 16384}
	for _, size := range sizes {
		members := makeMembers(1000, size)
		for _, chunk := range chunks {
			a, _ := drainScratch(members, chunk)
			b, _ := drainDirect(members, chunk)
			if !bytes.Equal(a, b) {
				t.Fatalf("size=%d chunk=%d: replies differ (%d vs %d bytes)", size, chunk, len(a), len(b))
			}
		}
	}
}

// TestDirectElidesCopy confirms the direct encoder copies far fewer member bytes
// than the scratch encoder: with a chunk that comfortably holds each member,
// nothing straddles, so the direct path copies essentially none of the member
// payload while the scratch path copies all of it.
func TestDirectElidesCopy(t *testing.T) {
	members := makeMembers(10000, 64)
	_, scratchCopied := drainScratch(members, 4096)
	_, directCopied := drainDirect(members, 4096)
	if directCopied >= scratchCopied/4 {
		t.Fatalf("direct path did not elide the copy: scratch=%d direct=%d", scratchCopied, directCopied)
	}
}

// The benchmarks drain through a single reused wire chunk and discard it, the
// way the real pump does, so they isolate the framing cost the elision changes
// rather than a growing-reassembly allocation that would swamp it.
func BenchmarkScratch(b *testing.B) {
	members := makeMembers(10000, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		drainScratchWire(members, 4096)
	}
}

func BenchmarkDirect(b *testing.B) {
	members := makeMembers(10000, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		drainDirectWire(members, 4096)
	}
}
