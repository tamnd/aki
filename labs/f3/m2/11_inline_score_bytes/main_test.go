package main

import (
	"math"
	"testing"
)

// The codec must round-trip every score exactly, so a stored zset reads back the
// score a client set, and the class-tagged width must match on the three call
// sites the engine uses (encWidth for sizing, putScore for writing, readScore for
// decoding).
func TestRoundTrip(t *testing.T) {
	cases := []float64{
		0, 1, -1, 127, -128, 128, -129, 32767, -32768, 32768, -32769,
		math.MaxInt32, math.MinInt32, math.MaxInt32 + 1, math.MinInt32 - 1,
		1.5, -0.25, 3.14159, math.MaxFloat64, -math.MaxFloat64,
		math.Inf(1), math.Inf(-1), 9007199254740992,
	}
	for _, s := range cases {
		var b [9]byte
		w := putScore(b[:], s)
		if ew := encWidth(s); ew != w {
			t.Fatalf("score %v: encWidth=%d putScore=%d", s, ew, w)
		}
		got, rw := readScore(b[:])
		if rw != w {
			t.Fatalf("score %v: readScore width=%d putScore=%d", s, rw, w)
		}
		if got != s {
			t.Fatalf("score %v round-trip got %v", s, got)
		}
	}
}

// The small-integer band must pick the tightest class: a rank or small counter
// costs 2 bytes, not the flat 8 the old blob spent.
func TestClassWidths(t *testing.T) {
	widths := map[float64]int{
		0: 2, 7: 2, 127: 2, -128: 2,
		128: 3, 32767: 3, -32768: 3,
		32768: 5, 2147483647: 5, -2147483648: 5,
		2147483648: 9, 1.5: 9, math.Inf(1): 9,
	}
	for s, want := range widths {
		if got := encWidth(s); got != want {
			t.Fatalf("score %v width %d want %d", s, got, want)
		}
	}
}

// A tiny integer-scored zset must shrink versus the flat 8-byte-score blob, the
// memory-row property.
func TestBlobShrinks(t *testing.T) {
	scores := []float64{0, 1, 2, 3, 4, 5, 6, 7}
	oldB := blobBytesOld(len(scores), 2)
	newB := blobBytesNew(scores, 2)
	if newB >= oldB {
		t.Fatalf("new blob %d did not shrink old %d", newB, oldB)
	}
}
