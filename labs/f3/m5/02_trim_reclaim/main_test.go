package main

import "testing"

// The lab's claims as invariants, so CI catches a reclaim-model regression: a
// whole-block drop reclaims essentially all of a removed entry's resident bytes
// while a per-entry tombstone reclaims none, the block-drop path costs O(blocks)
// directory operations against the per-entry O(entries), the approximate overshoot
// is bounded by one block, and the exact boundary tombstone is a bounded add on top
// of the same whole-block drops.

func shape3x8() entryShape { return entryShape{names: []int{8, 8, 8}, vals: []int{8, 8, 8}} }

const rate = 1000

// TestWholeBlockReclaimsFullEntryBytes pins that dropping whole front blocks frees
// the full resident cost of each removed entry (~payload plus overhead), while the
// rejected per-entry tombstone frees nothing immediately.
func TestWholeBlockReclaimsFullEntryBytes(t *testing.T) {
	shape := shape3x8()
	n := 200_000
	keep := n / 10

	sa := buildStream(4096, 128, rate, n, shape)
	ra := sa.trimApprox(keep)
	if ra.removed == 0 {
		t.Fatal("approximate trim removed nothing")
	}
	perEntry := float64(ra.reclaimed) / float64(ra.removed)
	// Payload is 24 bytes; overhead is the 7.36 B/entry lab-01 figure, so a removed
	// entry frees ~31 bytes. Allow a band around it for block-boundary rounding.
	if perEntry < 30 || perEntry > 33 {
		t.Fatalf("whole-block reclaim %.2f B/entry, want ~31 (24 payload + 7.36 overhead)", perEntry)
	}

	sp := buildStream(4096, 128, rate, n, shape)
	rp := sp.trimPerEntry(keep)
	if rp.reclaimed != 0 {
		t.Fatalf("per-entry tombstone reclaimed %d bytes, want 0 (sealed blocks do not shrink)", rp.reclaimed)
	}
}

// TestBlockDropOpsBeatPerEntry pins the O(blocks) versus O(entries) directory cost:
// the ratio is about the entries-per-block, ~128 at the default geometry.
func TestBlockDropOpsBeatPerEntry(t *testing.T) {
	shape := shape3x8()
	n := 200_000
	keep := n / 10

	sa := buildStream(4096, 128, rate, n, shape)
	ra := sa.trimApprox(keep)
	sp := buildStream(4096, 128, rate, n, shape)
	rp := sp.trimPerEntry(keep)

	if ra.dirOps == 0 {
		t.Fatal("approximate trim charged no directory operations")
	}
	ratio := float64(rp.dirOps) / float64(ra.dirOps)
	if ratio < 100 || ratio > 130 {
		t.Fatalf("per-entry/block-drop op ratio %.1f, want ~128 (entries per block)", ratio)
	}
}

// TestApproxOvershootBoundedByOneBlock pins that approximate mode leaves fewer than
// one block of extra entries above the threshold.
func TestApproxOvershootBoundedByOneBlock(t *testing.T) {
	shape := shape3x8()
	n := 200_000
	s := buildStream(4096, 128, rate, n, shape)
	r := s.trimApprox(1000)
	if r.overshoot < 0 || r.overshoot >= 128 {
		t.Fatalf("approximate overshoot %d entries, want < one block (128)", r.overshoot)
	}
}

// TestExactReachesThreshold pins that exact mode lands the live count precisely at
// the threshold, tombstoning the boundary overshoot the whole-block drop left.
func TestExactReachesThreshold(t *testing.T) {
	shape := shape3x8()
	n := 200_000
	keep := 1000
	s := buildStream(4096, 128, rate, n, shape)
	s.trimExact(keep)
	if s.length != keep {
		t.Fatalf("exact trim left %d live entries, want exactly %d", s.length, keep)
	}
}

// TestBaseOffsetKeepsDirectoryRefsValid pins the section 6.6 truncation: after a
// front drop the base advances by the dropped count and a later append keeps its
// logical position above the surviving blocks, so directory references never alias.
func TestBaseOffsetKeepsDirectoryRefsValid(t *testing.T) {
	shape := shape3x8()
	s := buildStream(4096, 128, rate, 200_000, shape)
	blocksBefore := len(s.blocks)
	r := s.trimApprox(20_000)
	if r.blocksDropped == 0 {
		t.Fatal("expected some whole-block drops")
	}
	if s.base != r.blocksDropped {
		t.Fatalf("base = %d after dropping %d blocks, want them equal", s.base, r.blocksDropped)
	}
	if len(s.blocks) != blocksBefore-r.blocksDropped {
		t.Fatalf("blocks = %d, want %d after the drop", len(s.blocks), blocksBefore-r.blocksDropped)
	}
	// A fresh append still lands in the tail without disturbing the offset.
	before := s.length
	s.xadd(shape)
	if s.length != before+1 {
		t.Fatalf("append after trim changed length to %d, want %d", s.length, before+1)
	}
}
