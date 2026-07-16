package main

import "testing"

// The lab's claims as invariants over the packing model, so CI catches a drift
// between the mirrored constants and the encoding they stand for: the 20-byte set
// row hits its spec target and beats the listpack estimate because the uvarint
// prefix is leaner, frame overhead stays near 1% across the sweep, resident cold
// metadata sits in the spec's fraction-of-a-byte band and far under element-per-row,
// the freed fraction grows with the member width, and the entry index never
// overflows the locator's ceiling.

const keyLen = 16

// TestSetRowMeetsSpec pins section 6.2's set row: a 20-byte member packs at least
// the spec's ~170 per 4 KiB chunk (the shipped uvarint encoding is leaner than the
// listpack-class estimate, so it packs more, never fewer), and the frame overhead is
// about the spec's 1%.
func TestSetRowMeetsSpec(t *testing.T) {
	r := measure(20, keyLen)
	if r.members < 170 {
		t.Fatalf("20-byte member packs %d per chunk, want at least the spec's ~170", r.members)
	}
	if r.overhead > 0.02 {
		t.Fatalf("frame overhead %.3f above the spec's ~1%%", r.overhead)
	}
}

// TestOverheadStaysLowAcrossWidths pins that the frame overhead stays near 1% for
// every member width the sweep covers: the fixed header, key, and discriminator
// amortize over a full 4 KiB payload regardless of member size, so no band pays a
// materially higher frame tax.
func TestOverheadStaysLowAcrossWidths(t *testing.T) {
	for _, width := range []int{8, 16, 20, 32, 64, 128, 256} {
		r := measure(width, keyLen)
		if r.overhead > 0.02 {
			t.Fatalf("width %d: frame overhead %.3f above 2%%", width, r.overhead)
		}
	}
}

// TestMetadataInSpecBand pins section 6.3's directory sizing: resident cold metadata
// per member sits in the 0.16-to-0.32-byte band at the spec's element widths and
// stays far below element-per-row's ~13 bytes, which is the whole reason the chunk
// form replaces the element-per-row layout.
func TestMetadataInSpecBand(t *testing.T) {
	r := measure(20, keyLen)
	if r.metaPerEl < 0.16 || r.metaPerEl > 0.32 {
		t.Fatalf("20-byte member resident meta %.3f B outside the spec's 0.16-0.32 band", r.metaPerEl)
	}
	if r.amortize < 20 {
		t.Fatalf("resident meta only %.0fx below element-per-row, want a wide amortization", r.amortize)
	}
}

// TestFreedFractionGrowsWithWidth pins the memory pitch: the wider the member, the
// larger the resident fraction a demote moves to disk, because the retained record
// and vector slot are fixed while the freed slab bytes grow with the member. A
// 20-byte member frees about half; a 256-byte member frees the large majority.
func TestFreedFractionGrowsWithWidth(t *testing.T) {
	small := measure(20, keyLen)
	large := measure(256, keyLen)
	if small.freedFrac <= 0 {
		t.Fatalf("20-byte demote freed %.3f of the footprint, want a positive saving", small.freedFrac)
	}
	if large.freedFrac <= small.freedFrac {
		t.Fatalf("256-byte freed %.3f not above 20-byte %.3f", large.freedFrac, small.freedFrac)
	}
	if large.freedFrac < 0.9 {
		t.Fatalf("256-byte member freed only %.3f, want the large majority", large.freedFrac)
	}
}

// TestChunkFitsLocatorCeiling pins that no member width packs more members into one
// chunk than the locator's 12-bit entry index can address (set/cold.go
// maxChunkEntry), so the byte target flushes a chunk well before the entry ceiling
// would. The smallest members pack the most, so the tightest width is the one to
// check.
func TestChunkFitsLocatorCeiling(t *testing.T) {
	const maxChunkEntry = 1 << 12
	for _, width := range []int{1, 8, 16, 20} {
		if r := measure(width, keyLen); r.members > maxChunkEntry {
			t.Fatalf("width %d packs %d members, over the locator's %d ceiling", width, r.members, maxChunkEntry)
		}
	}
}
