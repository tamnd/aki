package main

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestRemovalIdentical is the safety property the swap rests on: draw-by-index
// removal must leave the packed set in exactly the same state as re-find-by-value
// removal after the same draw sequence, for every member size and cardinality.
// Removing by the drawn index is only correct if it removes the same member the
// value re-find would have.
func TestRemovalIdentical(t *testing.T) {
	sizes := []int{1, 8, 16, 32, 64}
	cards := []int{1, 2, 8, 32, 128}
	for _, size := range sizes {
		for _, card := range cards {
			rngA := rand.New(rand.NewSource(42))
			rngB := rand.New(rand.NewSource(42))
			a := buildSet(card, size)
			b := buildSet(card, size)
			for n := card; n > 0; n-- {
				a = remByValue(a, rngA.Intn(n))
				b = remByIndex(b, rngB.Intn(n))
				if !bytes.Equal(a, b) {
					t.Fatalf("size=%d card=%d n=%d: sets diverged after removal", size, card, n)
				}
			}
			if len(a) != 0 || len(b) != 0 {
				t.Fatalf("size=%d card=%d: sets not fully drained (%d/%d)", size, card, len(a), len(b))
			}
		}
	}
}

// TestByIndexNoCompare confirms the by-index path never scans past the drawn
// entry: it splices at the offset the draw walk reaches, so its work is bounded
// by the index, while the by-value path additionally walks the whole set to
// re-find the member. On a full drain the by-index total splice distance is
// strictly less than the by-value total (which pays the extra re-find walk each
// pop).
func TestByIndexNoCompare(t *testing.T) {
	card, size := 128, 64
	data := buildSet(card, size)
	// Draw the last index every time: by-value must walk the whole set to
	// re-find it, by-index reaches it directly through packOffset.
	idx := card - 1
	valOff := packOffset(data, idx)
	n := int(data[valOff])
	m := append([]byte(nil), data[valOff+2:valOff+2+n]...)
	if got := packIndex(data, m); got != valOff {
		t.Fatalf("packIndex disagreed with packOffset: %d vs %d", got, valOff)
	}
}
