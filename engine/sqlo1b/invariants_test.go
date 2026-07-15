package sqlo1b

import (
	"os"
	"path/filepath"
	"testing"
)

// The doc 03 section 18 invariant list is the B1 test plan; this file
// is the map from each invariant to the named tests that hold it, plus
// the two tests below that close gaps the slice tests left.
//
// F1, checksum-verified path from superblock to record: the
// superblock tail hash and seq echo (TestCorruptionMatrix,
// TestEchoGuard), full pointers verified on every read
// (TestFullPtrVerify), extent checksums held by the referencer
// (TestExtentChecksum, TestScrubSweep), and the allocmap root arrow
// walked under crash by the matrix in cmd/sqlo1crash
// (TestFormatTornMatrix, TestFormatKillMatrix). The directory, chunk,
// and record arrows extend this path as B2 and B3 land those
// structures.
//
// F2, sealed extents immutable: TestGridLifecycle and
// TestIllegalTransitions pin the state machine, TestScrubSweep flags
// sealed-in-the-grid-but-not-on-disk, and the matrix seals only after
// the extent's sync so a referenced extent is durable by
// construction.
//
// F3, no acked write lost outside the fsync window: at the transport
// level TestWalAppendFlushReplay, and end to end the matrix verifier,
// which fails any iteration where an acked frame or seal past the
// trim barrier does not survive recovery. Store-level acked-key
// verification is the A2 kill loop and the B2 record replay.
//
// F4, freed space not reused before a superblock that cannot
// reference it is durable: TestGridLifecycle (quarantine tagged by
// seq, release only at or below the durable seq), TestCheckpointRun
// (release happens at step 6, after the commit point), and the
// matrix free path.
//
// F5, recovery O(WAL tail) plus O(1) root reads: TestRecoverReadsAreO1
// proves it by construction with a counting reader.
//
// F6, every record self-verifies and self-describes: rcrc and the
// full-key envelope are the B2 record envelope slice (milestone doc
// 08-B2), which owns this invariant's direct test. B1 establishes the
// bounds the envelope sits in: slot tables rejecting structural
// damage (TestGroupBuildParseRoundtrip, TestParseGroupRejects,
// TestTornSlots) and referencer checksums over the containing extent
// (TestFullPtrVerify, TestScrubSweep).
//
// F7, record and index agree with the record authoritative: the cold
// index is B2 work and its slices own the agreement test. The B1
// precursor is scrub's referencer-conflict finding in TestScrubSweep,
// which is the reporting half the invariant names.
//
// F8, one command's effects contiguous in the WAL and torn tails cut
// whole trailing frames: TestWalTornTailMatrix and
// TestWalMidFileCorruptionEndsReplay in engine/sqlo1,
// TestRecoverTornTail here, and the matrix WAL flavor tearing live
// tails at sector grain.
//
// F9, no on-disk count, address, or id saturates below 2^40:
// TestInvariantF9AddressHeadroom below, plus TestPosRanges for the
// packed position limits.
//
// F10, geometry read from the superblock, never compiled in:
// TestInvariantF10Geometry below, and the whole crash matrix runs on
// a 16 KiB extent size that no code constant knows.

// TestInvariantF9AddressHeadroom drives every on-disk counter and
// address field past 2^40 and requires exact roundtrips: the format
// must not saturate anywhere below the doc 03 capacity math.
func TestInvariantF9AddressHeadroom(t *testing.T) {
	big := uint64(1)<<40 + 12345

	if _, err := NewPos(maxExtent, 7, 9); err != nil {
		t.Fatalf("extent %d must pack: %v", uint64(maxExtent), err)
	}
	if _, err := NewPos(maxExtent+1, 0, 0); err == nil {
		t.Fatal("extent past 40 bits packed silently")
	}
	p, err := NewPos(maxExtent, maxGroup, 17)
	if err != nil {
		t.Fatal(err)
	}
	if p.Extent() != maxExtent || p.Group() != maxGroup || p.Slot() != 17 {
		t.Fatalf("pos roundtrip lost bits: %v", p)
	}
	bp, err := NewBlobPos(maxExtent, 3)
	if err != nil {
		t.Fatal(err)
	}
	if bp.Extent() != maxExtent || !bp.IsBlob() {
		t.Fatalf("blob pos roundtrip lost bits: %v", bp)
	}

	sb, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	sb.Seq = big
	sb.ExtentCount = big + 1
	sb.WALTrimSeq = big + 2
	sb.HashEpoch = PackHashEpoch(big+3, 7)
	sb.RecordCount = big + 4
	sb.GarbageBytes = big + 5
	sb.DirRoot = FullPtr{Pos: uint64(p), Sum: big + 6}
	got, err := DecodeSuperblock(sb.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if *got != *sb {
		t.Fatalf("superblock roundtrip past 2^40 diverged:\n got %+v\nwant %+v", got, sb)
	}

	h := ExtentHeader{Kind: KindVlog, EFlags: EFlagSealed, SealSeq: big, FirstWALSeq: big + 1}
	hh, err := DecodeExtentHeader(h.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if *hh != h {
		t.Fatalf("extent header roundtrip past 2^40 diverged: %+v", hh)
	}

	s, err := DecodeSealOp(SealOp{Extent: big, Sum: big + 1, Kind: KindVlog}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if s.Extent != big || s.Sum != big+1 {
		t.Fatalf("seal op roundtrip past 2^40 diverged: %+v", s)
	}
	c, err := DecodeCkptOp(CkptOp{SuperSeq: big}.Encode())
	if err != nil || c.SuperSeq != big {
		t.Fatalf("ckpt op roundtrip: %+v %v", c, err)
	}
	tr, err := DecodeTrimOp(TrimOp{WALSeq: big}.Encode())
	if err != nil || tr.WALSeq != big {
		t.Fatalf("trim op roundtrip: %+v %v", tr, err)
	}
}

// TestInvariantF10Geometry writes a file at a geometry no constant in
// this package names and reopens it: readers must take io_unit and
// extent_size from the superblock. The guard half is that decoding
// enforces nothing about the values, only the version.
func TestInvariantF10Geometry(t *testing.T) {
	const oddExtent = 192 * 1024
	const oddUnit = 8192
	if oddExtent == DefaultExtentSize || oddUnit == DefaultIOUnit {
		t.Fatal("test geometry collides with the defaults it must differ from")
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "geom.aki"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sb, err := NewSuperblock()
	if err != nil {
		t.Fatal(err)
	}
	sb.ExtentSize = oddExtent
	sb.IOUnit = oddUnit
	if err := InitSuperblocks(f, sb); err != nil {
		t.Fatal(err)
	}

	got, _, err := ReadSuperblock(f)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExtentSize != oddExtent || got.IOUnit != oddUnit {
		t.Fatalf("reopen geometry %d/%d, wrote %d/%d",
			got.ExtentSize, got.IOUnit, oddExtent, oddUnit)
	}

	// The geometry-consuming paths take the superblock's word: seal an
	// extent at the odd size, hand scrub the odd geometry, and the
	// sweep must verify it (a compiled-in size would read the wrong
	// span and fail the referencer checksum).
	img := make([]byte, oddExtent)
	h := ExtentHeader{Kind: KindVlog, EFlags: EFlagSealed | EFlagBlob, PayloadLen: 11}
	copy(img, h.Encode())
	if _, err := f.WriteAt(img, oddExtent); err != nil {
		t.Fatal(err)
	}
	sum, err := ExtentChecksum(f, got.ExtentSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	g := NewGrid(2)
	if _, err := g.Allocate(KindVlog, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Seal(KindVlog, 0); err != nil {
		t.Fatal(err)
	}
	rep := (&Scrubber{File: f, ExtentSize: got.ExtentSize, Grid: g, Sums: map[uint64]uint64{1: sum}}).Sweep()
	if !rep.Clean() || rep.Scanned != 1 {
		t.Fatalf("odd-geometry sweep scanned %d findings %+v", rep.Scanned, rep.Findings)
	}
}
