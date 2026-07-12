package main

import "testing"

// The lab's claims as invariants, so CI catches a geometry or encoding
// regression: the 4096/128 default holds the 64B fixed-schema entry inside the
// 6-8 B/entry memory bar with the entry cap binding at 128, the same-schema
// encoding compresses hard against the master, the seq delta stays small across a
// millisecond boundary (the zigzag fix), and the cold-read window costs the
// minimum preads at the default while a smaller block costs more.

func shape3x8() entryShape { return entryShape{names: []int{8, 8, 8}, vals: []int{8, 8, 8}} }

func TestDefaultGeometryMeetsMemoryBar(t *testing.T) {
	s := buildStream(4096, 128, 1000, 200_000, shape3x8())
	if epb := s.entriesPerBlock(); epb < 127.5 || epb > 128.5 {
		t.Fatalf("4096/128 entries per block %.1f, want the 128 cap to bind", epb)
	}
	if ov := s.overheadPerEntry(); ov < 6 || ov > 8 {
		t.Fatalf("4096/128 overhead %.2f B/entry, want the 6-8 memory bar", ov)
	}
	if dpe := s.dirBytesPerEntry(); dpe < 0.24 || dpe > 0.26 {
		t.Fatalf("4096/128 directory %.3f B/entry, want ~0.25 (32/128)", dpe)
	}
}

func TestSameSchemaCompressesAgainstMaster(t *testing.T) {
	e := shape3x8()
	master := e.masterBytes()
	same := e.sameBytes(0, 3) // an early same-ms entry
	// the master carries three 8-byte names, the same-schema entry carries none,
	// so the master must be larger by at least the mastered name bytes: mastering
	// is what recovers the ~100x field-name collapse of section 3.3.
	nameBytes := 0
	for _, n := range e.names {
		nameBytes += n
	}
	if master-same < nameBytes {
		t.Fatalf("same-schema entry %dB vs master %dB, saved %dB, want >= %dB of mastered names",
			same, master, master-same, nameBytes)
	}
}

func TestSeqDeltaSmallAcrossMillisecondBoundary(t *testing.T) {
	// a block firstSeq high, then the millisecond rolls and seq restarts at 0:
	// the delta is negative and must zigzag to a small varint, not blow up to the
	// 10-byte uint64-underflow the plain uvarint produced.
	e := shape3x8()
	firstSeq := uint64(900)
	across := e.sameBytes(1, int64(0)-int64(firstSeq)) // seq 0, delta -900
	within := e.sameBytes(0, 5)                        // a small positive delta
	// -900 zigzags to 1799, a 2-byte varint; the entry stays within one byte of
	// the small-delta entry, nowhere near a 10-byte blowup.
	if across-within > 2 {
		t.Fatalf("cross-boundary entry %dB vs within %dB, seq delta did not stay small", across, within)
	}
}

func TestIDDensityDoesNotBlowOverhead(t *testing.T) {
	// every ID density stays inside the memory bar; the sparse arms must not cost
	// more than the dense one by more than rounding (the zigzag invariant).
	for _, rate := range []uint64{1000, 100, 10, 1} {
		s := buildStream(4096, 128, rate, 100_000, shape3x8())
		if ov := s.overheadPerEntry(); ov < 6 || ov > 8 {
			t.Fatalf("rate %d overhead %.2f B/entry, want inside 6-8", rate, ov)
		}
	}
}

func TestColdWindowPreadsMinimalAtDefault(t *testing.T) {
	// a 100-entry window costs one pread per spanned block; at 4096/128 (128
	// entries/block) a mid-block window spans the minimum two blocks, and a
	// smaller block spans strictly more.
	span := func(capBytes, entryCap int) int {
		s := buildStream(capBytes, entryCap, 1000, 40_000, shape3x8())
		return 100/int(s.entriesPerBlock()) + 2
	}
	def := span(4096, 128)
	small := span(1024, 128)
	if def > 3 {
		t.Fatalf("4096/128 window spans %d blocks, want <=3", def)
	}
	if small <= def {
		t.Fatalf("1024/128 window spans %d, not more than the 4096/128 %d", small, def)
	}
}
