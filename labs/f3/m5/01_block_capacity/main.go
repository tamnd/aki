// Lab: block byte budget and entry cap for the stream append log (spec 2064/f3
// doc 14 sections 3.2-3.3 and section 12.5 lab decision 1, M5 lab 01).
//
// The question: doc 14 builds the native stream as an owner-local append log of
// entry blocks, each a contiguous arena allocation with a 48-byte header holding
// a run of consecutive entries, closed when it fills a byte budget or an entry
// cap, whichever binds first (section 3.2). Entries pack with the master-delta
// encoding (section 3.3): the first entry in a block is the master, stored whole
// with its field names; every later same-schema entry stores only a flags byte,
// ID deltas against the block firstID, and its values, the field names implied by
// the master. Section 12.5 fixes the default at 4096 bytes / 128 entries (Redis's
// stream-node knobs) and in the same breath pre-registers the sweep: 1KiB-16KiB
// budgets and 64-256 entry caps, scored on XADD throughput, XRANGE decode, cold
// read size, and directory RSS. This lab runs that sweep and freezes the block
// geometry the numbers ask for.
//
// The memory bar is PRED-F3-M5-STREAMMEM, which doc 14 section 10.1 states as 6-8
// bytes of overhead per entry over payload for the native band (2-4B ID-delta and
// flags varints, 3B value-length varints for a 3-field entry, ~0.2B field names
// mastered once per block, 48B header and 32B directory entry amortized over the
// block), stretch 8, ceiling 10. The compression this reproduces is doc 14's two
// measured wins: dense auto-IDs delta-code to 1-2 varint bytes against the block
// firstID, and fixed-schema field names collapse ~100x by mastering once per block.
//
// Method: in-process, no server, no wire, no engine import. The block here is
// lab-local code that models the doc's structure so the geometry can be priced
// before the entry-chunk slice writes it. A block carries its entries in a byte
// blob after a 48-byte header, encoded exactly as section 3.3 lays out (master
// whole, same-schema entries as flags + ID deltas + value frames), so an encode
// moves the real bytes and the overhead accounting is honest. IDs are dense
// auto-IDs at a settable entries-per-millisecond rate, the benchmark-shaped case
// the memory rows lean on, with a sparse arm to show the delta coding is not
// benchmark-only. Resident cost counts the 48-byte header and the 32-byte
// directory leaf per block plus the entry bytes, so header and directory slack
// are on the ledger the way F14 requires.
//
// Read: XADD ns/entry (tail append encode), XRANGE ns/entry (block decode walk),
// entries per block, directory bytes per entry (32 / entries-per-block), and total
// overhead bytes per entry over payload, at the 64B and 1KiB entry bands. A second
// sweep prices the cold-read tradeoff: a COUNT-100 history window costs one pread
// per spanned block, so a bigger block reads fewer blocks but more slack bytes for
// the same window. See README.md for the sweep tables and the frozen verdict. The
// rival RSS comparison this feeds is PRED-F3-M5-STREAMMEM at the M5 gate run.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"time"
)

const (
	blockHdr = 48 // section 3.2 block header
	dirLeaf  = 32 // section 3.2 directory leaf entry, one per block
)

// uvarintLen returns the number of bytes an unsigned varint of n occupies, the
// cost the ID deltas and length prefixes pay in the encoding.
func uvarintLen(n uint64) int {
	l := 1
	for n >= 0x80 {
		n >>= 7
		l++
	}
	return l
}

// entryShape describes the fixed-schema entry the sweep encodes: a list of field
// name lengths and a matching list of value lengths. The overwhelming common case
// is a fixed schema (same names every entry), which is what mastering compresses.
type entryShape struct {
	names []int // field name byte lengths
	vals  []int // field value byte lengths
}

// payload is the value bytes an entry carries, the denominator the overhead is
// measured against (field names are overhead, collapsed by mastering, per 10.1).
func (e entryShape) payload() int {
	s := 0
	for _, v := range e.vals {
		s += v
	}
	return s
}

// masterBytes is the encoded size of a block's first entry, stored whole:
// flags, the two zero ID deltas, the field count, and every name and value with
// its length prefix.
func (e entryShape) masterBytes() int {
	n := 1 + uvarintLen(0) + uvarintLen(0) + uvarintLen(uint64(len(e.names)))
	for i := range e.names {
		n += uvarintLen(uint64(e.names[i])) + e.names[i]
		n += uvarintLen(uint64(e.vals[i])) + e.vals[i]
	}
	return n
}

// zigzag maps a signed delta to an unsigned varint-friendly value, small in both
// directions, the standard sint encoding.
func zigzag(x int64) uint64 { return uint64((x << 1) ^ (x >> 63)) }

// sameBytes is the encoded size of a same-schema entry: flags, the two ID deltas
// against the block firstID, and the values only, names implied by the master.
// The seq delta is zigzag-coded because a block that spans a millisecond boundary
// carries entries whose seq restarted below the block firstSeq, a negative delta,
// exactly the signed listpack integer Redis stores.
func (e entryShape) sameBytes(msDelta uint64, seqDelta int64) int {
	n := 1 + uvarintLen(msDelta) + uvarintLen(zigzag(seqDelta))
	for _, v := range e.vals {
		n += uvarintLen(uint64(v)) + v
	}
	return n
}

// block models one sealed arena block: the 48-byte header plus the encoded entry
// bytes. blob holds the real bytes so an encode is a real memcpy and a decode is a
// real varint walk. firstMs/firstSeq are the block's firstID; entries are added
// in ID order.
type block struct {
	blob     []byte
	count    int
	firstMs  uint64
	firstSeq uint64
}

func newBlock(capBytes int, firstMs, firstSeq uint64) *block {
	return &block{blob: make([]byte, 0, capBytes), firstMs: firstMs, firstSeq: firstSeq}
}

// used is the entry bytes plus the fixed header, the block's resident footprint.
func (b *block) used() int { return blockHdr + len(b.blob) }

// appendEntry encodes one entry at the tail, master if it is the first, same-schema
// otherwise. ms/seq are the entry's absolute ID; the deltas are taken against the
// block firstID. The bytes are real (varints written, values zero-filled) so the
// encode cost is the real memcpy-class work XADD pays.
func (b *block) appendEntry(e entryShape, ms, seq uint64) {
	var tmp [10]byte
	put := func(x uint64) {
		n := binary.PutUvarint(tmp[:], x)
		b.blob = append(b.blob, tmp[:n]...)
	}
	if b.count == 0 {
		b.blob = append(b.blob, 0) // flags
		put(0)                     // msDelta
		put(0)                     // seqDelta
		put(uint64(len(e.names)))
		for i := range e.names {
			put(uint64(e.names[i]))
			b.blob = append(b.blob, make([]byte, e.names[i])...)
			put(uint64(e.vals[i]))
			b.blob = append(b.blob, make([]byte, e.vals[i])...)
		}
	} else {
		b.blob = append(b.blob, 0) // flags
		put(ms - b.firstMs)
		put(zigzag(int64(seq) - int64(b.firstSeq)))
		for _, v := range e.vals {
			put(uint64(v))
			b.blob = append(b.blob, make([]byte, v)...)
		}
	}
	b.count++
}

// decodeWalk walks the block decoding every entry, returning the entry count, the
// XRANGE decode path: for each entry read the flags, the two ID delta varints, the
// value length varints and skip the value bytes; the master also reads its names.
// Written to touch the same bytes the real decoder walks so the ns/entry is honest.
func (b *block) decodeWalk(nFields int) int {
	p, seen := 0, 0
	for p < len(b.blob) {
		p++ // flags
		_, n := binary.Uvarint(b.blob[p:])
		p += n // msDelta
		_, n = binary.Uvarint(b.blob[p:])
		p += n // seqDelta
		fields := nFields
		if seen == 0 {
			nf, m := binary.Uvarint(b.blob[p:])
			p += m
			fields = int(nf)
			for i := 0; i < fields; i++ {
				nl, m := binary.Uvarint(b.blob[p:])
				p += m + int(nl)
				vl, m2 := binary.Uvarint(b.blob[p:])
				p += m2 + int(vl)
			}
		} else {
			for i := 0; i < fields; i++ {
				vl, m := binary.Uvarint(b.blob[p:])
				p += m + int(vl)
			}
		}
		seen++
	}
	return seen
}

// stream models the append log: a sequence of sealed blocks plus the tail, closing
// a block when the byte budget or the entry cap binds. rate is dense auto-IDs per
// millisecond (the benchmark-shaped ID density); ms/seq advance like the section
// 3.6 allocator.
type stream struct {
	blocks   []*block
	capBytes int
	entryCap int
	rate     uint64 // entries per ms; the ID density
	ms, seq  uint64
	count    int
	payload  int64
}

func newStream(capBytes, entryCap int, rate uint64) *stream {
	return &stream{capBytes: capBytes, entryCap: entryCap, rate: rate, ms: 1}
}

// nextID advances the dense auto-ID: seq increments within a millisecond up to the
// rate, then the millisecond rolls and seq resets, exactly the 3.6 fast path.
func (s *stream) nextID() (uint64, uint64) {
	ms, seq := s.ms, s.seq
	s.seq++
	if s.seq >= s.rate {
		s.seq = 0
		s.ms++
	}
	return ms, seq
}

// xadd appends one entry, sealing and linking a fresh block when the current tail
// cannot hold the entry under the byte budget or the entry cap.
func (s *stream) xadd(e entryShape) {
	ms, seq := s.nextID()
	var tail *block
	if len(s.blocks) > 0 {
		tail = s.blocks[len(s.blocks)-1]
	}
	need := 0
	if tail == nil || tail.count == 0 {
		need = e.masterBytes()
	} else {
		need = e.sameBytes(ms-tail.firstMs, int64(seq)-int64(tail.firstSeq))
	}
	if tail == nil || tail.count >= s.entryCap || tail.used()+need > s.capBytes {
		tail = newBlock(s.capBytes, ms, seq)
		s.blocks = append(s.blocks, tail)
	}
	tail.appendEntry(e, ms, seq)
	s.count++
	s.payload += int64(e.payload())
}

// resident is the log's footprint: the 48-byte header and 32-byte directory leaf
// per block plus the entry bytes. Directory interior nodes are a second-order term
// (one per ~fanout leaves) and priced in the M2 tree labs, not here.
func (s *stream) resident() int64 {
	var b int64
	for _, blk := range s.blocks {
		b += int64(dirLeaf + blk.used())
	}
	return b
}

// overheadPerEntry is resident minus payload per entry, the F14 bar term.
func (s *stream) overheadPerEntry() float64 {
	if s.count == 0 {
		return 0
	}
	return float64(s.resident()-s.payload) / float64(s.count)
}

func (s *stream) entriesPerBlock() float64 {
	if len(s.blocks) == 0 {
		return 0
	}
	return float64(s.count) / float64(len(s.blocks))
}

func (s *stream) dirBytesPerEntry() float64 {
	if s.count == 0 {
		return 0
	}
	return float64(len(s.blocks)*dirLeaf) / float64(s.count)
}

// buildStream fills a stream with n entries of the given shape and ID density.
func buildStream(capBytes, entryCap int, rate uint64, n int, e entryShape) *stream {
	s := newStream(capBytes, entryCap, rate)
	for i := 0; i < n; i++ {
		s.xadd(e)
	}
	return s
}

type geoArm struct {
	name     string
	capBytes int
	entryCap int
}

func main() {
	quick := flag.Bool("quick", false, "smaller op counts for a fast check")
	flag.Parse()

	fmt.Printf("stream block byte-budget and entry-cap sweep, %s\n", time.Now().Format("2006-01-02"))

	arms := []geoArm{
		{"1024/128", 1024, 128},
		{"2048/128", 2048, 128},
		{"4096/128", 4096, 128},
		{"8192/128", 8192, 128},
		{"16384/128", 16384, 128},
		{"4096/64", 4096, 64},
		{"4096/256", 4096, 256},
	}

	// two entry bands: the 3-field 8B fixed schema (the 10.1 measurement shape,
	// ~64B entries) and a 1KiB single-value entry (the S8 payload band).
	bands := []struct {
		name  string
		shape entryShape
	}{
		{"3x8B", entryShape{names: []int{8, 8, 8}, vals: []int{8, 8, 8}}},
		{"1KiB", entryShape{names: []int{8}, vals: []int{1024}}},
	}
	const rate = 1000 // dense auto-IDs, 1000 entries/ms, the benchmark case

	fmt.Println()
	fmt.Println("Sweep A: block geometry, per entry band (dense IDs, rate 1000/ms)")
	fmt.Printf("%-11s %6s %9s %9s %9s %9s %9s\n",
		"arm", "band", "ent/blk", "xaddNs", "xrngNs", "dirB/e", "ovhd/e")
	for _, bd := range bands {
		memN, opN := 400_000, 4_000_000
		if bd.shape.payload() > 256 {
			memN, opN = 40_000, 400_000
		}
		if *quick {
			memN, opN = memN/10, opN/10
		}
		for _, a := range arms {
			// a block that cannot hold two entries of this band under the byte budget
			// is not a stream-geometry point (a fat entry goes to a solo block, 3.7).
			if a.capBytes < blockHdr+2*bd.shape.masterBytes() {
				fmt.Printf("%-11s %6s %9s %9s %9s %9s %9s\n",
					a.name, bd.name, "solo", "-", "-", "-", "-")
				continue
			}

			sm := buildStream(a.capBytes, a.entryCap, rate, memN, bd.shape)
			epb := sm.entriesPerBlock()
			dpe := sm.dirBytesPerEntry()
			ovhd := sm.overheadPerEntry()
			sm = nil

			// XADD timing: fresh stream, time opN appends (the tail-block encode).
			sp := newStream(a.capBytes, a.entryCap, rate)
			s := time.Now()
			for i := 0; i < opN; i++ {
				sp.xadd(bd.shape)
			}
			xaddNs := float64(time.Since(s).Nanoseconds()) / float64(opN)

			// XRANGE timing: decode every block of the stream just built.
			nf := len(bd.shape.vals)
			s = time.Now()
			dec := 0
			for _, blk := range sp.blocks {
				dec += blk.decodeWalk(nf)
			}
			xrngNs := float64(time.Since(s).Nanoseconds()) / float64(dec)
			sp = nil

			fmt.Printf("%-11s %6s %9.1f %9.2f %9.2f %9.3f %9.2f\n",
				a.name, bd.name, epb, xaddNs, xrngNs, dpe, ovhd)
		}
		fmt.Println()
	}

	// Sweep B: the cold-read tradeoff. A COUNT-w history window costs one pread per
	// spanned block (section 9.4). A bigger block reads fewer blocks for the same
	// window but more slack bytes on the boundary block, so this sweep shows the
	// preads-vs-bytes tension the byte budget sets.
	fmt.Println("Sweep B: cold read of a COUNT-100 window, 3x8B entries (dense IDs)")
	fmt.Printf("%-11s %9s %8s %10s %9s\n",
		"arm", "ent/blk", "preads", "bytesRead", "amp")
	const window = 100
	for _, a := range arms {
		sm := buildStream(a.capBytes, a.entryCap, rate, 40_000, bands[0].shape)
		epb := sm.entriesPerBlock()
		// a window of w entries starting at a block boundary spans ceil(w/epb)
		// blocks; a window landing mid-block spans one more. Count the worst-case
		// mid-block start (the honest planner figure).
		spanned := window/int(epb) + 2
		if spanned > len(sm.blocks) {
			spanned = len(sm.blocks)
		}
		// bytes read is a full pread per spanned block (the block is the cold unit).
		avgBlockBytes := float64(sm.resident()-int64(len(sm.blocks)*dirLeaf)) / float64(len(sm.blocks))
		bytesRead := float64(spanned) * avgBlockBytes
		payloadWanted := float64(window * bands[0].shape.payload())
		amp := bytesRead / payloadWanted
		fmt.Printf("%-11s %9.1f %8d %10.0f %9.2f\n",
			a.name, epb, spanned, bytesRead, amp)
		sm = nil
	}
	fmt.Println()

	// Sweep C: ID-density sensitivity of the compression. Dense IDs (rate high)
	// delta-code to 1-2 varint bytes; sparse IDs (rate low, entries spread over ms)
	// grow the msDelta varint. This shows the memory numbers are not benchmark-only.
	fmt.Println("Sweep C: ID density vs overhead at 4096/128, 3x8B entries")
	fmt.Printf("%-14s %9s %9s\n", "rate(ent/ms)", "ent/blk", "ovhd/e")
	for _, r := range []uint64{1000, 100, 10, 1} {
		sm := buildStream(4096, 128, r, 200_000, bands[0].shape)
		fmt.Printf("%-14d %9.1f %9.2f\n", r, sm.entriesPerBlock(), sm.overheadPerEntry())
		sm = nil
	}
}
