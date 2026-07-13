package main

import (
	"math/rand"
	"testing"
)

// The lab's claims as invariants, so CI catches a gc-model regression: a rewrite
// reclaims the dead fraction's bytes and keeps exactly the live entries, the
// reclaim-equals-copy break-even sits at the 0.5 default (below it a rewrite copies
// more than it frees, above it frees more than it copies), the gc ratio bounds the
// dead bytes sustained interior churn retains while never-rewrite leaks the full
// churn fraction, and a fully tombstoned sealed block is dropped whole.

func shape3x8() entryShape { return entryShape{names: []int{8, 8, 8}, vals: []int{8, 8, 8}} }

const rate = 1000

// TestRewriteReclaimsAndKeepsLive pins that a rewrite of a half-dead block keeps
// exactly the live entries in ID order and frees the tombstoned bytes.
func TestRewriteReclaimsAndKeepsLive(t *testing.T) {
	shape := shape3x8()
	b := fullBlock(4096, 128, rate, shape)
	want := b.count / 2
	// Tombstone every other entry so the survivors are interleaved, not a prefix.
	live := make([]id, 0, want)
	for i := 0; i < b.count; i++ {
		if i%2 == 1 {
			b.tombstone(i)
		} else {
			live = append(live, b.ids[i])
		}
	}
	before := b.used()
	nb := b.rewrite(shape, 4096)
	if nb == nil {
		t.Fatal("rewrite dropped a block with live entries")
	}
	if nb.count != b.live() {
		t.Fatalf("rewrite kept %d entries, want live %d", nb.count, b.live())
	}
	for i, e := range nb.ids {
		if e != live[i] {
			t.Fatalf("survivor %d = %v, want %v (order not preserved)", i, e, live[i])
		}
	}
	if nb.used() >= before {
		t.Fatalf("rewrite did not shrink the block: %d -> %d", before, nb.used())
	}
}

// TestRewriteBreakEvenAtHalf pins the structural knee: reclaimed/copied is below 1
// below half-dead, at ~1 at half-dead, and above 1 above half-dead, which is why the
// default gc-ratio is 0.5.
func TestRewriteBreakEvenAtHalf(t *testing.T) {
	shape := shape3x8()
	ratioAt := func(f float64) float64 {
		b := fullBlock(4096, 128, rate, shape)
		tombstoneFrac(b, f)
		nb := b.rewrite(shape, 4096)
		return float64(b.used()-nb.used()) / float64(nb.used())
	}
	if r := ratioAt(0.25); r >= 1.0 {
		t.Fatalf("recl/copy at f=0.25 = %.3f, want < 1 (copies more than it frees)", r)
	}
	if r := ratioAt(0.5); r < 0.9 || r > 1.1 {
		t.Fatalf("recl/copy at f=0.5 = %.3f, want ~1 (break-even)", r)
	}
	if r := ratioAt(0.75); r <= 1.0 {
		t.Fatalf("recl/copy at f=0.75 = %.3f, want > 1 (frees more than it copies)", r)
	}
}

// TestGcRatioBoundsRetainedDead pins that a finite gc-ratio holds the retained dead
// bytes well below the churn fraction while never-rewrite leaks the full fraction,
// and that a tighter ratio retains less dead but copies more.
func TestGcRatioBoundsRetainedDead(t *testing.T) {
	shape := shape3x8()
	run := func(ratio float64) churnResult {
		s := buildStream(4096, 128, rate, 40_000, shape)
		return churnToDead(s, ratio, 0.6, 20, rand.New(rand.NewSource(1)))
	}
	tight := run(0.25)
	mid := run(0.5)
	never := run(1.0)

	if never.blocksRewritten != 0 || never.deadFracRetained < 0.5 {
		t.Fatalf("never-rewrite should leak: rewrites=%d deadFrac=%.3f, want 0 rewrites and deadFrac>=0.5",
			never.blocksRewritten, never.deadFracRetained)
	}
	if mid.deadFracRetained >= never.deadFracRetained {
		t.Fatalf("gc r=0.5 retained deadFrac %.3f, want below never %.3f",
			mid.deadFracRetained, never.deadFracRetained)
	}
	if tight.deadFracRetained >= mid.deadFracRetained {
		t.Fatalf("gc r=0.25 retained deadFrac %.3f, want below r=0.5 %.3f",
			tight.deadFracRetained, mid.deadFracRetained)
	}
	if tight.entriesCopied <= mid.entriesCopied {
		t.Fatalf("tighter ratio should copy more: r=0.25 copied %d, r=0.5 copied %d",
			tight.entriesCopied, mid.entriesCopied)
	}
	if mid.residentBytes >= never.residentBytes {
		t.Fatalf("gc r=0.5 resident %d, want below never %d", mid.residentBytes, never.residentBytes)
	}
}

// TestGcDropsEmptyBlock pins that a fully tombstoned sealed block is dropped whole,
// reclaiming its full resident bytes with no rewrite.
func TestGcDropsEmptyBlock(t *testing.T) {
	shape := shape3x8()
	s := buildStream(4096, 128, rate, 4000, shape)
	if len(s.blocks) < 3 {
		t.Fatalf("need several blocks, got %d", len(s.blocks))
	}
	victim := s.blocks[0]
	for i := 0; i < victim.count; i++ {
		victim.tombstone(i)
	}
	s.length -= victim.live() // already zero, kept for bookkeeping symmetry
	before := len(s.blocks)
	r := s.gc(1.0) // ratio irrelevant: an empty block drops regardless
	if r.blocksDropped != 1 {
		t.Fatalf("gc dropped %d blocks, want 1", r.blocksDropped)
	}
	if r.blocksRewritten != 0 {
		t.Fatalf("gc rewrote %d blocks, want 0 (empty block drops whole)", r.blocksRewritten)
	}
	if len(s.blocks) != before-1 {
		t.Fatalf("blocks after gc = %d, want %d", len(s.blocks), before-1)
	}
	if r.reclaimed <= 0 {
		t.Fatalf("dropping a full block reclaimed %d bytes, want > 0", r.reclaimed)
	}
}

// TestGcLeavesTailBlock pins that gc never rewrites or drops the open tail block,
// even when it is fully tombstoned, since it is still filling.
func TestGcLeavesTailBlock(t *testing.T) {
	shape := shape3x8()
	s := newStream(4096, 128, rate, shape)
	for i := 0; i < 10; i++ { // one partial block, the tail
		s.xadd()
	}
	tail := s.blocks[len(s.blocks)-1]
	for i := 0; i < tail.count; i++ {
		tail.tombstone(i)
	}
	r := s.gc(0.5)
	if r.blocksDropped != 0 || r.blocksRewritten != 0 {
		t.Fatalf("gc touched the tail: dropped=%d rewritten=%d, want 0/0",
			r.blocksDropped, r.blocksRewritten)
	}
	if len(s.blocks) != 1 {
		t.Fatalf("tail block count = %d, want 1", len(s.blocks))
	}
}
