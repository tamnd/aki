// Lab 05: the ID delta base choice (doc 14 section 3.3).
//
// The shipped block codec stores each entry's ID as a delta against the block
// firstID (base-delta), chosen so any entry decodes independently of the ones
// before it. The alternative is a successive delta, against the predecessor
// entry, the form a plain delta stream takes. Base-delta varints grow with an
// entry's distance from the block firstID; successive-delta varints track only
// the gap to the entry before it, which for a monotone ID run stays small. This
// lab measures how many ID bytes each base costs across representative stream ID
// shapes and what a full-block decode costs under each, so the section 3.3
// choice rests on numbers, not just the independent-decode argument.
//
// In-process, no server, no wire, no engine import, the lab-local model labs
// 01, 02, and 04 use. The encoders here mirror engine/f3/stream/id.go exactly:
// the ms delta is an unsigned varint (monotone IDs, so the ms delta against any
// earlier base is non-negative) and the seq delta is a signed zigzag varint (a
// millisecond rollover resets seq to zero, so the delta against an earlier base
// can go negative). Only the ID bytes differ between the two bases; the flags
// byte and the value frames are identical, so the lab prices the ID bytes alone,
// the whole of the difference.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"time"
)

// id is the lab-local (ms, seq) stream ID, the shape of engine/f3/stream.streamID.
type id struct {
	ms  uint64
	seq uint64
}

// uvlen is the encoded length of an unsigned varint, binary.AppendUvarint's.
func uvlen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// zigzag maps a signed int to the unsigned varint binary.AppendVarint writes.
func zigzag(x int64) uint64 {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return ux
}

// vlen is the encoded length of a signed (zigzag) varint.
func vlen(x int64) int { return uvlen(zigzag(x)) }

// idBytes prices id's delta against base, the ms uvarint plus the seq zigzag
// varint, the same two fields putIDDelta writes. base is firstID for base-delta
// and the predecessor for successive-delta; the pricing is identical, only the
// base differs.
func idBytes(base, x id) int {
	return uvlen(x.ms-base.ms) + vlen(int64(x.seq)-int64(base.seq))
}

// baseBytes sums the ID bytes for a block under base-delta: every entry against
// the block firstID.
func baseBytes(ids []id) int {
	if len(ids) == 0 {
		return 0
	}
	first := ids[0]
	total := 1 // the master stores a zero delta against itself, one byte each field
	total += vlen(0) - 1 + uvlen(0) - 1 + 1
	for i := 1; i < len(ids); i++ {
		total += idBytes(first, ids[i])
	}
	return total
}

// succBytes sums the ID bytes under successive-delta: every entry against its
// predecessor. The master is identical to base-delta (a zero self-delta).
func succBytes(ids []id) int {
	if len(ids) == 0 {
		return 0
	}
	total := uvlen(0) + vlen(0) // master, zero self-delta, same as base
	for i := 1; i < len(ids); i++ {
		total += idBytes(ids[i-1], ids[i])
	}
	return total
}

// encodeBase and encodeSucc build the real ID-delta byte stream for a block, so
// the decode benchmark walks bytes, not the id slice. Only the ID fields are
// emitted; the flags and values are constant across the two bases and left out.
func encodeBase(ids []id) []byte {
	var b []byte
	first := ids[0]
	for _, x := range ids {
		b = binary.AppendUvarint(b, x.ms-first.ms)
		b = binary.AppendVarint(b, int64(x.seq)-int64(first.seq))
	}
	return b
}

func encodeSucc(ids []id) []byte {
	var b []byte
	prev := ids[0]
	for _, x := range ids {
		b = binary.AppendUvarint(b, x.ms-prev.ms)
		b = binary.AppendVarint(b, int64(x.seq)-int64(prev.seq))
		prev = x
	}
	return b
}

// decodeBase walks a base-delta stream, reconstructing every ID against firstID,
// and returns the last ID so the compiler cannot drop the loop.
func decodeBase(b []byte, first id) id {
	pos := 0
	var last id
	for pos < len(b) {
		md, n1 := binary.Uvarint(b[pos:])
		sd, n2 := binary.Varint(b[pos+n1:])
		last = id{ms: first.ms + md, seq: uint64(int64(first.seq) + sd)}
		pos += n1 + n2
	}
	return last
}

// decodeSucc walks a successive-delta stream, accumulating each ID from the
// running predecessor.
func decodeSucc(b []byte, first id) id {
	pos := 0
	prev := first
	for pos < len(b) {
		md, n1 := binary.Uvarint(b[pos:])
		sd, n2 := binary.Varint(b[pos+n1:])
		cur := id{ms: prev.ms + md, seq: uint64(int64(prev.seq) + sd)}
		prev = cur
		pos += n1 + n2
	}
	return prev
}

// pattern generates one 128-entry block's IDs for a named workload shape. n is
// the block entry cap. Every generator is deterministic given the seed so the
// numbers reproduce.
type pattern struct {
	name string
	desc string
	gen  func(n int, rng *rand.Rand) []id
}

func patterns() []pattern {
	return []pattern{
		{"dense-1000/ms", "auto-ID burst, 1000 entries per ms: a full block is one ms, seq 0..127", func(n int, _ *rand.Rand) []id {
			out := make([]id, n)
			ms, seq := uint64(1_700_000_000_000), uint64(0)
			for i := range out {
				out[i] = id{ms, seq}
				seq++
				if seq == 1000 {
					seq, ms = 0, ms+1
				}
			}
			return out
		}},
		{"burst-100/ms", "100 entries per ms: a block spans ~1.3 ms, seq rolls at 100", func(n int, _ *rand.Rand) []id {
			out := make([]id, n)
			ms, seq := uint64(1_700_000_000_000), uint64(0)
			for i := range out {
				out[i] = id{ms, seq}
				seq++
				if seq == 100 {
					seq, ms = 0, ms+1
				}
			}
			return out
		}},
		{"one/ms", "one entry per ms, seq always 0: a steady 1 kHz producer", func(n int, _ *rand.Rand) []id {
			out := make([]id, n)
			ms := uint64(1_700_000_000_000)
			for i := range out {
				out[i] = id{ms, 0}
				ms++
			}
			return out
		}},
		{"slow-10ms", "one entry every 10 ms, seq 0: a 100 Hz producer, block spans ~1.3 s", func(n int, _ *rand.Rand) []id {
			out := make([]id, n)
			ms := uint64(1_700_000_000_000)
			for i := range out {
				out[i] = id{ms, 0}
				ms += 10
			}
			return out
		}},
		{"sparse-idle", "bursty with idle gaps: random 1..5000 ms jumps, occasional same-ms pair", func(n int, rng *rand.Rand) []id {
			out := make([]id, n)
			ms, seq := uint64(1_700_000_000_000), uint64(0)
			for i := range out {
				out[i] = id{ms, seq}
				if rng.Intn(4) == 0 { // a quarter of entries share the prior ms
					seq++
				} else {
					seq, ms = 0, ms+1+uint64(rng.Intn(5000))
				}
			}
			return out
		}},
		{"explicit-wide", "explicit user IDs seconds apart: ms jumps 1000..60000, seq 0", func(n int, rng *rand.Rand) []id {
			out := make([]id, n)
			ms := uint64(1_700_000_000_000)
			for i := range out {
				out[i] = id{ms, 0}
				ms += 1000 + uint64(rng.Intn(59000))
			}
			return out
		}},
	}
}

func main() {
	quick := flag.Bool("quick", false, "shrink the decode rep count for the shared runner")
	flag.Parse()

	const n = 128 // blockCap, one full block
	reps := 200_000
	if *quick {
		reps = 2_000
	}

	fmt.Printf("Lab 05: ID delta base, base-delta (vs firstID) vs successive-delta (vs predecessor)\n")
	fmt.Printf("one full %d-entry block per pattern; ID bytes only (flags and values are identical)\n\n", n)
	fmt.Printf("%-14s  %7s  %7s  %8s  %8s  %9s  %9s\n",
		"pattern", "baseB", "succB", "base/e", "succ/e", "save/e", "save%")
	for _, p := range patterns() {
		rng := rand.New(rand.NewSource(0x5a5a5a))
		ids := p.gen(n, rng)
		bb := baseBytes(ids)
		sb := succBytes(ids)
		bpe := float64(bb) / float64(n)
		spe := float64(sb) / float64(n)
		save := bpe - spe
		pct := 0.0
		if bpe > 0 {
			pct = 100 * save / bpe
		}
		fmt.Printf("%-14s  %7d  %7d  %8.3f  %8.3f  %9.3f  %8.1f%%\n",
			p.name, bb, sb, bpe, spe, save, pct)
	}

	fmt.Printf("\nfull-block decode cost, %d reps (single box, read as shape not digits):\n\n", reps)
	fmt.Printf("%-14s  %10s  %10s  %9s\n", "pattern", "baseNs", "succNs", "succ/base")
	for _, p := range patterns() {
		rng := rand.New(rand.NewSource(0x5a5a5a))
		ids := p.gen(n, rng)
		bbuf := encodeBase(ids)
		sbuf := encodeSucc(ids)
		first := ids[0]

		var sink id
		t0 := time.Now()
		for i := 0; i < reps; i++ {
			sink = decodeBase(bbuf, first)
		}
		baseNs := float64(time.Since(t0).Nanoseconds()) / float64(reps)

		t0 = time.Now()
		for i := 0; i < reps; i++ {
			sink = decodeSucc(sbuf, first)
		}
		succNs := float64(time.Since(t0).Nanoseconds()) / float64(reps)
		_ = sink

		ratio := 0.0
		if baseNs > 0 {
			ratio = succNs / baseNs
		}
		fmt.Printf("%-14s  %10.1f  %10.1f  %9.3f\n", p.name, baseNs, succNs, ratio)
	}
}
