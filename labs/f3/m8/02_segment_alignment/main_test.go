package main

import "testing"

// TestAlignedSpanIsSectorWhole is the durability property the pad buys: every
// aligned span is a whole number of 512-byte sectors, so a durable write lands on
// whole sectors with no read-modify-write and a torn tail can only be its own.
func TestAlignedSpanIsSectorWhole(t *testing.T) {
	for _, p := range []int{0, 1, 63, 64, 65, 511, 4096, 65536, 262144} {
		r := measure(p, defaultAlign)
		if r.span%sector != 0 {
			t.Fatalf("payload %d: span %d not a whole number of %d-byte sectors", p, r.span, sector)
		}
		if r.span%defaultAlign != 0 {
			t.Fatalf("payload %d: span %d not a multiple of the alignment %d", p, r.span, defaultAlign)
		}
		if r.span < r.logical {
			t.Fatalf("payload %d: span %d is smaller than the logical size %d", p, r.span, r.logical)
		}
	}
}

// TestPointValueIsUnaffordable pins the shape's low end: a 64-byte value one per
// segment amplifies past 20x, which is why the hot path keeps values in the arena
// and never writes point values one to a segment.
func TestPointValueIsUnaffordable(t *testing.T) {
	r := measure(64, defaultAlign)
	if r.amp <= 20 {
		t.Fatalf("64-byte point value amp %.1fx, want the unaffordable >20x", r.amp)
	}
	if r.span != defaultAlign {
		t.Fatalf("64-byte value span %d, want a single aligned sector-run %d", r.span, defaultAlign)
	}
}

// TestPadIsNearConstant pins the header-tip finding: a round (power-of-two) payload
// tips the header just past a 4KiB boundary, so the pad is a near-constant ~4032
// bytes across the batch sizes, not a fixed fraction. That is why only large
// batches amortize it.
func TestPadIsNearConstant(t *testing.T) {
	for _, p := range []int{4096, 8192, 16384, 32768, 65536, 262144} {
		if w := measure(p, defaultAlign).waste; w != defaultAlign-segHeaderLen {
			t.Fatalf("round payload %d waste %d, want the near-full pad %d", p, w, defaultAlign-segHeaderLen)
		}
	}
}

// TestBatchScaleIsAffordable pins the high end: a 32KiB cold chunk stays under 1.15x
// and a 256KiB run under 1.02x, so tens-of-KiB batches amortize the pad well even
// with the header tip.
func TestBatchScaleIsAffordable(t *testing.T) {
	if r := measure(32768, defaultAlign); r.amp >= 1.15 {
		t.Fatalf("32KiB chunk amp %.3fx, want under 1.15x", r.amp)
	}
	if r := measure(262144, defaultAlign); r.amp >= 1.02 {
		t.Fatalf("256KiB run amp %.3fx, want under 1.02x", r.amp)
	}
}

// TestTightSizingErasesThePad pins the refinement: a payload sized to fill whole
// alignment units (units*A - header) leaves no pad at all, so its span is exactly
// the logical size and the amplification is flat 1.0.
func TestTightSizingErasesThePad(t *testing.T) {
	for _, units := range []int{1, 2, 8, 64} {
		p := tightPayload(units, defaultAlign)
		r := measure(p, defaultAlign)
		if r.waste != 0 || r.amp != 1.0 || r.span != units*defaultAlign {
			t.Fatalf("tight payload %d (units %d): waste %d amp %.4f span %d, want a flush fill", p, units, r.waste, r.amp, r.span)
		}
	}
}

// TestAmpFallsWithPayload is the monotonicity the design leans on: a larger payload
// never amplifies more, so growing the batch always dilutes the pad.
func TestAmpFallsWithPayload(t *testing.T) {
	payloads := []int{64, 128, 512, 1024, 4096, 8192, 16384, 32768, 65536, 262144}
	var prev float64
	for i, p := range payloads {
		r := measure(p, defaultAlign)
		if i > 0 && r.amp > prev {
			t.Fatalf("payload %d amp %.4f rose above the smaller payload's %.4f", p, r.amp, prev)
		}
		prev = r.amp
	}
}

// TestWasteNeverReachesAlignment bounds the pad: a rounded-up span wastes strictly
// less than one alignment unit, so the worst case is a single padded sector-run.
func TestWasteNeverReachesAlignment(t *testing.T) {
	for _, p := range []int{0, 1, 64, 100, 4095, 4096, 4097, 65536, 262143} {
		r := measure(p, defaultAlign)
		if r.waste < 0 || r.waste >= defaultAlign {
			t.Fatalf("payload %d: waste %d out of [0,%d)", p, r.waste, defaultAlign)
		}
	}
}

// TestPackedRivalPaysPartialSectors pins the read-modify-write side of the trade:
// a tightly packed segment whose logical size is not sector-whole shares two
// partial sectors with its neighbors, while a sector-whole one shares none.
func TestPackedRivalPaysPartialSectors(t *testing.T) {
	if got := packedPartialSectors(64); got != 2 { // 64+64 = 128, not a whole sector
		t.Fatalf("packed 64-byte value partial sectors = %d, want 2", got)
	}
	if got := packedPartialSectors(sector - segHeaderLen); got != 0 { // exactly one sector
		t.Fatalf("packed sector-whole segment partial sectors = %d, want 0", got)
	}
}
