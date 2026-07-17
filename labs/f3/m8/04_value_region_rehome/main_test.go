package main

import (
	"encoding/binary"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// TestPerValueSegmentIsWholeSegment pins the unusable corner the batching writer
// avoids: a tiny value in its own segment costs a whole 4KiB segment.
func TestPerValueSegmentIsWholeSegment(t *testing.T) {
	r := measure(16, 1)
	if r.segBytes != akifile.SegmentAlign {
		t.Fatalf("one tiny value spanned %d, want one segment %d", r.segBytes, akifile.SegmentAlign)
	}
	if r.akiPerVal != float64(akifile.SegmentAlign) {
		t.Fatalf("per-value bytes = %.0f, want %d", r.akiPerVal, akifile.SegmentAlign)
	}
	if r.amplify != float64(akifile.SegmentAlign)/16 {
		t.Fatalf("amplify = %.2f, want %.2f", r.amplify, float64(akifile.SegmentAlign)/16)
	}
}

// TestBatchingAmortizesPadding is the whole point: for a fixed value size, the
// amplification falls monotonically as the batch grows, because the segment
// header and 4KiB padding spread over more values.
func TestBatchingAmortizesPadding(t *testing.T) {
	var prev row
	for i, b := range []int{1, 8, 64, 512, 4096} {
		r := measure(16, b)
		if i > 0 && r.amplify >= prev.amplify {
			t.Fatalf("batch %d amplify %.3f did not fall below %.3f", b, r.amplify, prev.amplify)
		}
		prev = r
	}
}

// TestFrameTaxIsVarintPlusCRC checks the unavoidable per-value overhead equals
// the frame's varint length plus its four CRC bytes, independent of the batch.
func TestFrameTaxIsVarintPlusCRC(t *testing.T) {
	for _, s := range []int{16, 1024, 4096, 65536} {
		var hdr [binary.MaxVarintLen64]byte
		wantTax := float64(binary.PutUvarint(hdr[:], uint64(s)) + 4)
		for _, b := range []int{1, 64, 4096} {
			r := measure(s, b)
			if r.frameTax != wantTax {
				t.Fatalf("size %d batch %d frame tax = %.1f, want %.1f", s, b, r.frameTax, wantTax)
			}
		}
	}
}

// TestLargeValueInsensitive proves the padding only bites small values: a 64KiB
// value amplifies under the default bar even in its own segment, while a 16-byte
// value in its own segment blows far past it.
func TestLargeValueInsensitive(t *testing.T) {
	if a := measure(65536, 1).amplify; a >= 1.10 {
		t.Fatalf("64KiB unbatched amplify = %.3f, want under 1.10", a)
	}
	if a := measure(16, 1).amplify; a < 100 {
		t.Fatalf("16B unbatched amplify = %.1f, want a large penalty", a)
	}
}

// TestModelMatchesCodec confirms the reported per-value bytes are exactly the
// codec's segment span over the batch, so the model is the format's arithmetic.
func TestModelMatchesCodec(t *testing.T) {
	for _, s := range []int{16, 4096, 65536} {
		for _, b := range []int{1, 64, 512} {
			r := measure(s, b)
			if r.akiPerVal*float64(b) != float64(r.segBytes) {
				t.Fatalf("size %d batch %d: per-value %.1f * %d != segment %d", s, b, r.akiPerVal, b, r.segBytes)
			}
		}
	}
}
