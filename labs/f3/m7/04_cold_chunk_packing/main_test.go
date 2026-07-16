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

// TestListWholeChunkFreesNearlyAll pins the list column's headline: a list demotes a
// whole chunk and keeps no per-element resident record, only one descriptor per shed
// chunk, so a demote frees essentially the entire chunk footprint at every member
// width. Unlike the set, whose freed fraction grows with the member, the list's holds
// near-constant because both the freed footprint and the retained descriptor scale
// with the same 1/members.
func TestListWholeChunkFreesNearlyAll(t *testing.T) {
	for _, width := range []int{8, 20, 64, 256} {
		if r := measureList(width, keyLen); r.freedFrac < 0.98 {
			t.Fatalf("width %d: list demote freed %.4f, want ~0.99 of the chunk footprint", width, r.freedFrac)
		}
	}
	small := measureList(20, keyLen)
	large := measureList(256, keyLen)
	if d := large.freedFrac - small.freedFrac; d < -0.01 || d > 0.01 {
		t.Fatalf("list freed fraction drifted %.4f across widths, want near-constant", d)
	}
}

// TestListChunkMetaUnderSet pins the structural saving of the whole-chunk demote: a
// cold list chunk keeps only its directory descriptor resident, where the set keeps
// the descriptor plus an offset-table slot per chunk, because the list rides the ring
// handle's cold offset for a read and needs no directory slot. The saving is per
// chunk; per member it depends on the packing, which the elem-cap ceiling makes
// coarser for a list, so this pins the per-chunk fact the encoding actually fixes.
func TestListChunkMetaUnderSet(t *testing.T) {
	if listDescBytes >= descBytes+offsBytes {
		t.Fatalf("cold list chunk keeps %d resident B, not below the set's %d (descriptor + offset slot)",
			listDescBytes, descBytes+offsBytes)
	}
}

// TestListChunkFitsElemCap pins that the model never packs more than the native
// element ceiling into one list chunk (native.go chunkElemCap): a narrow member hits
// the 128-element cap well before the byte budget, the geometry that lifts the list
// frame overhead above the set's for small members.
func TestListChunkFitsElemCap(t *testing.T) {
	for _, width := range []int{1, 8, 16, 20, 32} {
		if r := measureList(width, keyLen); r.members > listElemCap {
			t.Fatalf("width %d packs %d frames, over the native element cap %d", width, r.members, listElemCap)
		}
	}
}

// TestListFootprintMatchesSource pins the mirrored native chunk footprint against the
// value native.go derives (chunkBlobCap + chunkElemCap*2), so a change to either
// source constant this lab does not track surfaces here rather than in a silently
// wrong freed fraction.
func TestListFootprintMatchesSource(t *testing.T) {
	if listFootprint != 4096+128*2 {
		t.Fatalf("list footprint %d != chunkBlobCap + chunkElemCap*2", listFootprint)
	}
}

// TestHashRowPacksPairs pins the hash row: a chunk packs field-and-value pairs to the
// byte target, so a 20-byte field and value pair packs a healthy count and the frame
// overhead stays near 1% like the set.
func TestHashRowPacksPairs(t *testing.T) {
	r := measureHash(20, keyLen)
	if r.members < 90 {
		t.Fatalf("20-byte pair packs only %d per chunk, want a healthy fill", r.members)
	}
	if r.overhead > 0.02 {
		t.Fatalf("hash frame overhead %.3f above ~1%%", r.overhead)
	}
}

// TestHashKeepsFieldResident pins the design crux: the hash keeps the field bytes
// resident, so a demote frees strictly less than the set frees for the same width,
// because the set sheds the whole member while the hash sheds only the value. This is
// the price of a zero-pread probe, and the model must show it.
func TestHashKeepsFieldResident(t *testing.T) {
	for _, width := range []int{8, 20, 64, 256} {
		h := measureHash(width, keyLen)
		s := measure(width, keyLen)
		if h.freedFrac >= s.freedFrac {
			t.Fatalf("width %d: hash freed %.3f not below set's %.3f (field must stay resident)",
				width, h.freedFrac, s.freedFrac)
		}
		// The resident-after figure must still carry the field bytes.
		if h.coldPerEl < float64(fentryBytes+hashVecBytes+width) {
			t.Fatalf("width %d: hash resident %.1f B dropped the field bytes", width, h.coldPerEl)
		}
	}
}

// TestHashFreedGrowsWithWidth pins that the shed still pays off more as the value
// widens: a wider value is a larger share of the pair's footprint, so the freed
// fraction climbs, even though the resident field caps it below the set's.
func TestHashFreedGrowsWithWidth(t *testing.T) {
	small := measureHash(20, keyLen)
	large := measureHash(256, keyLen)
	if small.freedFrac <= 0 {
		t.Fatalf("20-byte hash demote freed %.3f, want a positive saving", small.freedFrac)
	}
	if large.freedFrac <= small.freedFrac {
		t.Fatalf("256-byte hash freed %.3f not above 20-byte %.3f", large.freedFrac, small.freedFrac)
	}
}

// TestHashChunkFitsLocatorCeiling pins that no field-and-value width packs more pairs
// into one chunk than the locator's 12-bit entry index can address (hash/cold.go
// maxChunkEntry), so the byte target flushes well before the entry ceiling.
func TestHashChunkFitsLocatorCeiling(t *testing.T) {
	const maxChunkEntry = 1 << 12
	for _, width := range []int{1, 8, 16, 20} {
		if r := measureHash(width, keyLen); r.members > maxChunkEntry {
			t.Fatalf("width %d packs %d pairs, over the locator's %d ceiling", width, r.members, maxChunkEntry)
		}
	}
}
