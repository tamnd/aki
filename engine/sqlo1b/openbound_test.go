package sqlo1b

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// The B1 exit-gate open bound: a synthetic 100 GiB file must open on
// superblock plus root reads alone, inside the doc 03 section 17
// capacity math. At v0 geometry 100 GiB is 102400 extents and a
// 12.5 KiB allocmap, so the whole open budget is the two superblock
// slots, one extent header, and that allocmap: openBoundBytes, about
// 21 KiB, against a hundred-gigabyte file. The proof is a counting
// reader, not a stopwatch, same as TestRecoverReadsAreO1: byte count
// and touched offsets say O(1) by construction on any machine.
const (
	openBoundExtents = 100 << 30 / DefaultExtentSize // 102400
	openBoundMapLen  = (openBoundExtents + 7) / 8    // 12800, the doc's 12.5 KiB
	openBoundBytes   = 2*SuperblockSize + ExtentHeaderSize + openBoundMapLen
)

func TestOpenBound100GiB(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs a filesystem that keeps a 100 GiB truncate sparse")
	}
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "big.aki"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(100 << 30); err != nil {
		t.Fatal(err)
	}

	// The allocmap root lives in the LAST extent, so a targeted read
	// at offset ~100 GiB is the only way to reach it; any scan-shaped
	// open would blow the byte budget five orders of magnitude before
	// getting there.
	rootExt := uint64(openBoundExtents - 1)
	am := make([]byte, openBoundMapLen)
	am[0] |= 1
	am[rootExt/8] |= 1 << (rootExt % 8)
	h := ExtentHeader{Kind: KindAllocmap, EFlags: EFlagSealed, PayloadLen: openBoundMapLen}
	off := int64(rootExt) * DefaultExtentSize
	if _, err := f.WriteAt(h.Encode(), off); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(am, off+ExtentHeaderSize); err != nil {
		t.Fatal(err)
	}
	pos, err := NewBlobPos(rootExt, 0)
	if err != nil {
		t.Fatal(err)
	}

	sb, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	sb.ExtentCount = openBoundExtents
	if err := InitSuperblocks(f, sb); err != nil {
		t.Fatal(err)
	}
	next := *sb
	next.Seq = 2
	next.AllocmapRoot = MakeFullPtr(pos, am)
	if err := CommitSuperblock(f, &next); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(dir, "big.aki-wal")
	const segSize = 256 << 10
	w, err := sqlo1.OpenWAL(walPath, sb.WALDBID(), segSize)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := w.Append(0, sqlo1.WALOpPut, 0, []byte("tail")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// The full open: pick the survivor, replay the tail, then walk
	// the allocmap root arrow and rebuild the grid, all through the
	// counting reader.
	cr := &countReader{r: f}
	rec, err := Recover(cr, walPath, segSize, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.WAL.Close()
	if rec.Super.Seq != 2 || rec.Super.ExtentCount != openBoundExtents {
		t.Fatalf("survivor seq %d extents %d", rec.Super.Seq, rec.Super.ExtentCount)
	}

	hb := make([]byte, ExtentHeaderSize)
	if _, err := cr.ReadAt(hb, off); err != nil {
		t.Fatal(err)
	}
	rh, err := DecodeExtentHeader(hb)
	if err != nil {
		t.Fatal(err)
	}
	if rh.Kind != KindAllocmap || !rh.Sealed() {
		t.Fatalf("root extent header kind %d sealed %v", rh.Kind, rh.Sealed())
	}
	got := make([]byte, rh.PayloadLen)
	if _, err := cr.ReadAt(got, off+ExtentHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := rec.Super.AllocmapRoot.Verify(got); err != nil {
		t.Fatal(err)
	}
	g, err := LoadGrid(got, rec.Super.ExtentCount)
	if err != nil {
		t.Fatal(err)
	}
	if g.State(rootExt) != StateSealed || g.State(1) != StateFree {
		t.Fatalf("grid states root %s ext1 %s", g.State(rootExt), g.State(1))
	}
	if err := rec.RestoreGrid(g); err != nil {
		t.Fatal(err)
	}

	if bytes := cr.bytes.Load(); bytes != openBoundBytes {
		t.Fatalf("open read %d data-file bytes, the capacity-math bound is %d", bytes, openBoundBytes)
	}
	// Sanity on both ends: the reads reached the last extent (the
	// root really is at the far end of 100 GiB), and the budget is
	// still five decimal orders below the file size.
	if maxOff := cr.maxOff.Load(); maxOff <= 100<<30-DefaultExtentSize {
		t.Fatalf("open never touched the last extent, max offset %d", maxOff)
	}
	if openBoundBytes > 32<<10 {
		t.Fatalf("bound %d grew past the recorded 32 KiB envelope", openBoundBytes)
	}
}
