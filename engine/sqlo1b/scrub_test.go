package sqlo1b

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const scrubExtSize = 2 * GroupSize

// buildVlogExtent lays out a sealed two-group vlog extent image.
func buildVlogExtent(t *testing.T, sealSeq uint64) []byte {
	t.Helper()
	buf := make([]byte, scrubExtSize)
	g0 := NewGroupBuilder(Group0Payload)
	for range 3 {
		if _, err := g0.Append(bytes.Repeat([]byte{0x33}, 500)); err != nil {
			t.Fatal(err)
		}
	}
	copy(buf[ExtentHeaderSize:], g0.Close())
	g1 := NewGroupBuilder(GroupSize)
	for range 2 {
		if _, err := g1.Append(bytes.Repeat([]byte{0x44}, 700)); err != nil {
			t.Fatal(err)
		}
	}
	copy(buf[GroupSize:], g1.Close())
	h := ExtentHeader{Kind: KindVlog, EFlags: EFlagSealed, SealSeq: sealSeq, PayloadLen: 2900, GroupCount: 2}
	copy(buf, h.Encode())
	return buf
}

func headerOnlyExtent(kind, eflags uint8) []byte {
	buf := bytes.Repeat([]byte{0x77}, scrubExtSize)
	h := ExtentHeader{Kind: kind, EFlags: eflags, GroupCount: 2}
	copy(buf, h.Encode())
	return buf
}

func TestScrubSweep(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "scrub.aki"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Grid: extents 1..4 sealed vlog, 5 an active tail, 6 a sealed
	// blob, 7 sealed stats, 8 and 9 sealed vlog. 5 must be skipped,
	// everything else scanned.
	g := NewGrid(10)
	seal := func(kind uint8, shard uint16) uint64 {
		t.Helper()
		ext, err := g.Allocate(kind, shard)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Seal(kind, shard); err != nil {
			t.Fatal(err)
		}
		return ext
	}
	for range 4 {
		seal(KindVlog, 0)
	}
	if _, err := g.Allocate(KindVlog, 2); err != nil { // extent 5 stays active
		t.Fatal(err)
	}
	seal(KindVlog, 1)  // 6, written as blob
	seal(KindStats, 0) // 7
	seal(KindVlog, 3)  // 8
	seal(KindVlog, 4)  // 9

	writeExt := func(ext uint64, img []byte) {
		t.Helper()
		if _, err := f.WriteAt(img, int64(ext)*scrubExtSize); err != nil {
			t.Fatal(err)
		}
	}
	for ext := uint64(1); ext <= 4; ext++ {
		writeExt(ext, buildVlogExtent(t, ext*10))
	}
	writeExt(5, make([]byte, scrubExtSize))
	writeExt(6, headerOnlyExtent(KindVlog, EFlagSealed|EFlagBlob))
	writeExt(7, headerOnlyExtent(KindStats, EFlagSealed))
	writeExt(8, buildVlogExtent(t, 80))
	writeExt(9, buildVlogExtent(t, 90))

	sums := map[uint64]uint64{}
	for _, ext := range []uint64{1, 2} {
		sum, err := ExtentChecksum(f, scrubExtSize, ext)
		if err != nil {
			t.Fatal(err)
		}
		sums[ext] = sum
	}

	var throttled int
	s := &Scrubber{File: f, ExtentSize: scrubExtSize, Grid: g, Sums: sums,
		Throttle: func(n int) { throttled += n }}

	rep := s.Sweep()
	if !rep.Clean() {
		t.Fatalf("undamaged file has findings: %+v", rep.Findings)
	}
	if rep.Scanned != 8 || rep.Skipped != 1 {
		t.Fatalf("scanned %d skipped %d, want 8 and 1", rep.Scanned, rep.Skipped)
	}
	if throttled != 8*scrubExtSize {
		t.Fatalf("throttle saw %d bytes, want %d", throttled, 8*scrubExtSize)
	}

	// Damage: a flipped payload byte under a registered checksum, a
	// flipped header byte, a smashed slot table, a lost seal flag,
	// and a group count past the extent.
	patch := func(off int64, b []byte) {
		t.Helper()
		if _, err := f.WriteAt(b, off); err != nil {
			t.Fatal(err)
		}
	}
	patch(2*scrubExtSize+int64(ExtentHeaderSize)+10, []byte{0xEE})
	patch(3*scrubExtSize+8, []byte{0xEE})
	patch(4*scrubExtSize+2*GroupSize-2, []byte{0xFF, 0xFF})
	unsealed := ExtentHeader{Kind: KindVlog, SealSeq: 80, PayloadLen: 2900, GroupCount: 2}
	patch(8*scrubExtSize, unsealed.Encode())
	overflow := ExtentHeader{Kind: KindVlog, EFlags: EFlagSealed, SealSeq: 90, PayloadLen: 2900, GroupCount: 3}
	patch(9*scrubExtSize, overflow.Encode())

	rep = s.Sweep()
	want := []struct {
		ext uint64
		msg string
	}{
		{2, "referencer holds"},
		{3, "header_crc"},
		{4, "group 1"},
		{8, "not on disk"},
		{9, "past the extent"},
	}
	if len(rep.Findings) != len(want) {
		t.Fatalf("findings %+v", rep.Findings)
	}
	for i, w := range want {
		got := rep.Findings[i]
		if got.Extent != w.ext || !strings.Contains(got.Err.Error(), w.msg) {
			t.Fatalf("finding %d is extent %d %q, want extent %d containing %q",
				i, got.Extent, got.Err, w.ext, w.msg)
		}
	}
	if rep.Scanned != 8 || rep.Skipped != 1 {
		t.Fatalf("damaged sweep scanned %d skipped %d", rep.Scanned, rep.Skipped)
	}
	// The undamaged extents still verify: blast radius is the extent.
	for _, fd := range rep.Findings {
		if fd.Extent == 1 || fd.Extent == 6 || fd.Extent == 7 {
			t.Fatalf("clean extent %d reported: %v", fd.Extent, fd.Err)
		}
	}
}
