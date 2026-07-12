package main

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/engine/f3/store"
)

// TestFramersAgree checks the two framers put the exact same bytes on the wire.
// The buffered framer's output is the reference reply; the streamed framer must
// reproduce it byte for byte across chunk boundaries, or the stream would ship a
// different reply than the buffered path it replaces. Driven at a size that
// straddles several chunks so the drain-and-continue seam is exercised.
func TestFramersAgree(t *testing.T) {
	for _, tc := range []struct{ m, w int }{
		{10, 8}, {100, 64}, {1000, 64}, {2000, 512},
	} {
		h := makeHash(tc.m, tc.w)
		want := append([]byte(nil), frameBuffered(nil, h)...)

		var got []byte
		emit := reassemble(&got)
		streamInto(h, emit)
		if !bytes.Equal(got, want) {
			t.Fatalf("m=%d w=%d: streamed reply differs from buffered (len %d vs %d)",
				tc.m, tc.w, len(got), len(want))
		}
		if len(want) != h.replyBytes() {
			t.Fatalf("m=%d w=%d: replyBytes said %d, framed %d", tc.m, tc.w, h.replyBytes(), len(want))
		}
	}
}

// streamInto reruns the streamed framer's element walk, handing each drained run
// to emit instead of folding it, so the test can reassemble the whole reply and
// compare it to the buffered reference.
func streamInto(h *hashModel, emit func([]byte)) {
	chunk := make([]byte, 0, store.ChunkSize)
	chunk = appendArrayHeader(chunk[:0], 2*len(h.ords))
	put := func(b []byte) {
		if len(chunk)+bulkFrameLen(len(b)) > cap(chunk) {
			emit(chunk)
			chunk = chunk[:0]
		}
		chunk = appendBulk(chunk, b)
	}
	for _, o := range h.ords {
		put(h.field(o))
		put(h.value(o))
	}
	emit(chunk)
}

func reassemble(out *[]byte) func([]byte) {
	return func(b []byte) { *out = append(*out, b...) }
}

// TestStreamPeakIsFlat is the memory-bar assertion: the streamed framer's peak
// working set is one chunk window, flat in the field count, while the buffered
// reply grows without bound. Above the chunk cutover the streamed peak must stay
// pinned at store.ChunkSize even as the reply passes it by orders of magnitude.
func TestStreamPeakIsFlat(t *testing.T) {
	small := makeHash(1000, 64)
	big := makeHash(100000, 64)
	if small.replyBytes() < store.ChunkSize {
		t.Fatalf("m=1000 valW=64 reply %d did not clear the %d chunk cutover, pick a bigger gate cell",
			small.replyBytes(), store.ChunkSize)
	}
	// The buffered peak scales with the hash; the streamed peak does not.
	if big.replyBytes() <= small.replyBytes() {
		t.Fatal("buffered peak did not grow with field count")
	}
	// A run over the streamed framer must never let a chunk exceed its window.
	for _, h := range []*hashModel{small, big} {
		max := watchStreamedChunk(h)
		if max > store.ChunkSize {
			t.Fatalf("streamed chunk peaked at %d, over the %d window", max, store.ChunkSize)
		}
	}
}

// watchStreamedChunk runs the streamed walk and returns the largest the chunk
// ever grew, the number that must stay under the window.
func watchStreamedChunk(h *hashModel) int {
	chunk := make([]byte, 0, store.ChunkSize)
	chunk = appendArrayHeader(chunk[:0], 2*len(h.ords))
	max := len(chunk)
	put := func(b []byte) {
		if len(chunk)+bulkFrameLen(len(b)) > cap(chunk) {
			chunk = chunk[:0]
		}
		chunk = appendBulk(chunk, b)
		if len(chunk) > max {
			max = len(chunk)
		}
	}
	for _, o := range h.ords {
		put(h.field(o))
		put(h.value(o))
	}
	return max
}

// TestQuickSweep runs the smoke path: the sweep body completes and every framer
// call returns finite work without panicking, at a tiny op budget so it stays
// under a second on a loaded CI runner. The reported cardinalities run under
// go run .; the framing code they exercise is the same this small cell drives.
func TestQuickSweep(t *testing.T) {
	h := makeHash(1000, 64)
	buf := make([]byte, 0, h.replyBytes())
	chunk := make([]byte, 0, store.ChunkSize)
	const smoke = 200
	if got := timeFrame(smoke, func() { buf = frameBuffered(buf, h) }); got <= 0 {
		t.Fatalf("buffered framer timed non-positive: %v", got)
	}
	if got := timeFrame(smoke, func() { frameStreamed(chunk, h) }); got <= 0 {
		t.Fatalf("streamed framer timed non-positive: %v", got)
	}
	if got := allocPerOp(smoke, func() { fold(frameBuffered(nil, h)) }); got == 0 {
		t.Fatal("cold buffered framer reported zero allocation, the transient should show")
	}
}
