package main

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

// TestHopFormulas pins the hop counts the verdict rests on: streaming pays one
// length hop per source shard plus, per chunk, one read hop per source shard and
// one write hop, so it grows with the length; gather pays one hop per source shard
// plus one write, flat in the length.
func TestHopFormulas(t *testing.T) {
	cases := []struct {
		ml, srcShards, want int
	}{
		{0, 2, 2},                   // no chunks, just the length hops.
		{1, 2, 2 + 1*3},             // one chunk, two reads plus one write.
		{chunkSize, 2, 2 + 1*3},     // still one chunk.
		{chunkSize + 1, 2, 2 + 2*3}, // two chunks.
		{4 * chunkSize, 3, 3 + 4*4}, // four chunks over three source shards.
	}
	for _, c := range cases {
		if got := streamingHops(c.ml, c.srcShards); got != c.want {
			t.Fatalf("streamingHops(%d,%d)=%d want %d", c.ml, c.srcShards, got, c.want)
		}
	}
	if got := gatherHops(3); got != 4 {
		t.Fatalf("gatherHops(3)=%d want 4", got)
	}
	// Gather never grows with the length. At zero length streaming skips the write
	// hop, so it is one under gather; once there is any chunk to stream it climbs
	// past gather and keeps climbing with the length.
	if streamingHops(0, 2) >= gatherHops(2) {
		t.Fatalf("at zero length streaming should skip the write hop and stay under gather")
	}
	if streamingHops(chunkSize, 2) <= gatherHops(2) {
		t.Fatalf("with a chunk to stream the streaming form should hop more")
	}
}

// TestPeakFormulas pins the memory side: the streaming peak is flat in the length
// and the gather peak grows with it, so past a threshold gather crosses the bar
// streaming never moves.
func TestPeakFormulas(t *testing.T) {
	if streamPeak(3) != 4*chunkSize {
		t.Fatalf("streamPeak(3)=%d want %d", streamPeak(3), 4*chunkSize)
	}
	small, big := gatherPeak(3, 1<<20), gatherPeak(3, 256<<20)
	if big <= small {
		t.Fatalf("gather peak should climb with length: %d then %d", small, big)
	}
	// A 256MiB, 3-source gather holds far more than the flat streaming peak.
	if big <= streamPeak(3) {
		t.Fatalf("gather peak %d should dwarf the flat streaming peak %d", big, streamPeak(3))
	}
}

// TestStreamingMatchesGather checks the streaming assembly is byte-for-byte the
// whole-buffer answer over sources of unequal length (the zero-pad tail), so the
// chunked form the coordinator runs is not just cheaper on memory but correct.
func TestStreamingMatchesGather(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x04ab, 0x0b17))
	for iter := 0; iter < 40; iter++ {
		nsrc := 2 + rng.IntN(3)
		srcs := make([][]byte, nsrc)
		for i := range srcs {
			b := make([]byte, rng.IntN(3*chunkSize))
			for j := range b {
				b[j] = byte(rng.Uint32())
			}
			srcs[i] = b
		}
		want := runGather(srcs)
		got := make([]byte, len(want))
		runStreaming(srcs, func(off int, b []byte) { copy(got[off:], b) })
		if !bytes.Equal(got, want) {
			t.Fatalf("iter %d: streaming assembly differs from gather", iter)
		}
	}
}
