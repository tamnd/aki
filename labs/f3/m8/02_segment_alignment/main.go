// Command 02_segment_alignment models the space-versus-IO trade of the .aki
// segment grid: every segment starts on a 4KiB boundary (akifile SegmentAlign), so
// a segment of h+p bytes (a 64-byte header plus its payload) rounds up to the next
// 4KiB, and the pad is wasted disk and page-cache. The alignment buys sector-whole,
// read-modify-write-free durable writes and a torn write that can never damage a
// neighbor; the pad is what it costs.
//
// The model answers one question: when is that pad affordable? It is a pure
// arithmetic model of doc 07 section 3's layout, no engine import:
//
//   - a segment's on-disk span is span = ceil((h+p)/A) * A for alignment A, so the
//     space amplification is span/(h+p) and the waste is span-(h+p);
//   - an aligned span is always a whole number of 512-byte sectors (4096 and 16384
//     are both multiples of 512), so every write lands on whole sectors: zero
//     read-modify-write, and a torn tail sector can only be this segment's own;
//   - a tightly packed rival (no alignment) wastes no pad but starts and ends mid
//     sector, so it pays a read-modify-write on the head and tail partial sectors
//     it shares with its neighbors, and a torn write there corrupts a neighbor.
//
// The verdict is the design's shape: for a point value (64 bytes) one-per-segment
// the amplification is catastrophic (a 4KiB pad around 128 logical bytes, 32x), so
// the .aki file must never hold point values one per segment. It does not: the hot
// path keeps values in the arena, and only batched, already-large segments reach
// the file (a group-committed log window, a packed cold chunk, a value-log run),
// where the payload dwarfs the pad and the amplification falls to a few percent.
// The lab shows where the crossover sits.
package main

import (
	"flag"
	"fmt"
)

// akifile layout constants (engine/f3/akifile/format.go), copied so the lab stays
// import-free the way 01_group_commit_window does.
const (
	segHeaderLen = 64   // SegHeaderLen, the per-segment framing
	sector       = 512  // device sector, the read-modify-write unit
	defaultAlign = 4096 // SegmentAlign, the shipped boundary
)

// row is one payload point: the on-disk economics of a segment carrying p payload
// bytes at alignment A.
type row struct {
	payload      int     // p, the segment payload
	logical      int     // h+p, the bytes the segment actually carries
	span         int     // ceil((h+p)/A)*A, the on-disk footprint
	amp          float64 // span/logical, the space amplification
	waste        int     // span-logical, the pad bytes
	wholeSectors int     // span/sector, all whole (aligned), zero read-modify-write
}

// measure computes one row: a segment of `payload` bytes aligned to `align`.
func measure(payload, align int) row {
	logical := segHeaderLen + payload
	span := ((logical + align - 1) / align) * align
	return row{
		payload:      payload,
		logical:      logical,
		span:         span,
		amp:          float64(span) / float64(logical),
		waste:        span - logical,
		wholeSectors: span / sector,
	}
}

// packedPartialSectors is the read-modify-write tax of the tightly packed rival: a
// segment whose logical size is not a whole number of sectors shares its head and
// tail sector with a neighbor, so a durable write there is a read-modify-write and
// a torn write can corrupt the neighbor. Whole-sector payloads share nothing.
func packedPartialSectors(payload int) int {
	if (segHeaderLen+payload)%sector == 0 {
		return 0
	}
	return 2
}

// tightPayload is the payload that makes a segment fill whole alignment units with
// no pad at all: units*A minus the header. A writer that sizes its batches this way
// pays zero amplification, which is the refinement a round (power-of-two) payload
// misses by tipping the header just past a boundary.
func tightPayload(units, align int) int {
	return units*align - segHeaderLen
}

func main() {
	align := flag.Int("align", defaultAlign, "segment alignment A in bytes (4096 shipped; try 512 or 16384)")
	quick := flag.Bool("quick", false, "run a short sweep")
	flag.Parse()

	payloads := []int{64, 128, 512, 1024, 4096, 8192, 16384, 32768, 65536, 262144}
	if *quick {
		payloads = []int{64, 8192, 32768, 262144}
	}

	fmt.Printf("segment alignment: A=%d bytes, header=%d, sector=%d\n\n", *align, segHeaderLen, sector)

	const hdr = "%-10s %-10s %-10s %-9s %-10s %-12s %-10s\n"
	fmt.Printf(hdr, "payload", "logical", "span", "amp", "waste", "sectors", "packed RMW")
	fmt.Printf(hdr, "bytes", "h+p", "on-disk", "x", "pad", "whole", "per-seg")
	fmt.Printf(hdr, "-------", "-------", "-------", "---", "-----", "-------", "----------")

	for _, p := range payloads {
		r := measure(p, *align)
		fmt.Printf("%-10d %-10d %-10d %-9.3f %-10d %-12d %-10d\n",
			r.payload, r.logical, r.span, r.amp, r.waste, r.wholeSectors, packedPartialSectors(p))
	}

	point := measure(64, *align)
	window := measure(8192, *align)
	chunk := measure(32768, *align)
	big := measure(262144, *align)
	tight := measure(tightPayload(8, *align), *align)

	fmt.Printf("\nDesign point (why the file holds large batches, not point values):\n")
	fmt.Printf("  a 64-byte value one-per-segment costs %.1fx (a %d-byte pad around %d logical bytes): unaffordable, so the hot path keeps values in the arena\n",
		point.amp, point.waste, point.logical)
	fmt.Printf("  the header tips a round payload just past a boundary, so the pad is a near-constant ~%d bytes: an 8KiB log window still costs %.3fx, a 32KiB cold chunk %.3fx, a 256KiB run %.3fx\n",
		chunk.waste, window.amp, chunk.amp, big.amp)
	fmt.Printf("  so only tens-of-KiB batches amortize the pad; sizing a segment to fill whole units (payload = units*A - header) erases it entirely: %d bytes costs %.3fx flat\n",
		tight.payload, tight.amp)
	fmt.Printf("  every aligned span is %d whole sectors with zero read-modify-write; the packed rival would pay %d partial-sector RMW per segment and risk a torn neighbor\n",
		chunk.wholeSectors, packedPartialSectors(32768))

	// Where the amplification first falls under a 10 percent overhead.
	cross := 0
	for p := segHeaderLen; p <= 1<<20; p += segHeaderLen {
		if measure(p, *align).amp <= 1.10 {
			cross = p
			break
		}
	}
	fmt.Printf("  the amplification first crosses below 1.10x at a payload near %d bytes: a batch this size or larger costs under a tenth\n", cross)
}
