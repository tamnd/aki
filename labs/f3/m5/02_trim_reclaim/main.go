// Lab: XTRIM reclaim granularity, whole-block drop versus per-entry tombstone
// (spec 2064/f3/14 section 6.6, M5 lab 02).
//
// The question: doc 14 trims a native stream from the front, the oldest entries,
// and section 6.6 fixes the reclaim unit as the whole block. Approximate (~) drops
// whole front blocks while the result stays at or above the threshold, each drop
// one directory delete plus the freeing of one block, so removing 10k entries that
// live in ~80 blocks is ~80 directory deletes, not 10k entry deletes. Exact (=)
// adds tombstoning the overshoot inside the boundary block, paying the deleted-flag
// writes now and reclaiming the bytes only when that block later empties. The
// design rejects a per-entry front reclaim: sealed blocks are append-frozen, so a
// per-entry front splice would re-encode a block on the command path.
//
// This lab prices those three: it builds a native append log with the frozen
// 4096/128 geometry from lab 01, trims it to a range of keep fractions, and reports
// what each strategy reclaims, how many directory operations it costs, and, for the
// approximate mode, how much it overshoots the threshold. The claim it settles is
// that the block is the right front-reclaim unit: whole-block drop frees essentially
// all of a removed entry's bytes at O(blocks) directory work, while a per-entry
// tombstone frees nothing immediately at O(entries) work, and the approximate
// overshoot is bounded by one block, small against any threshold a trim aims at.
//
// This is the memory-bearing decision the labs-per-perf-change rule attaches to the
// XTRIM slice. The gc-ratio rewrite of a partially-tombstoned block (section 6.5,
// its own lab) is orthogonal: it reclaims the bytes the exact-mode boundary
// tombstones leave, on the owner's background step, not on the trim path.
//
// Method: in-process, no server, no wire, no engine import, the same lab-local
// model lab 01 uses. Blocks carry real byte blobs encoded as section 3.3 lays out
// (master whole, same-schema entries as flags plus ID deltas plus value frames), so
// a drop frees real bytes and a boundary tombstone flips a real flag. IDs are dense
// auto-IDs (1000/ms, the benchmark-shaped case). Resident cost counts the 48-byte
// header and 32-byte directory leaf per block plus the entry bytes, matching lab 01.
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

func uvarintLen(n uint64) int {
	l := 1
	for n >= 0x80 {
		n >>= 7
		l++
	}
	return l
}

func zigzag(x int64) uint64 { return uint64((x << 1) ^ (x >> 63)) }

// entryShape is the fixed-schema entry the sweep encodes: field name and value
// byte lengths. The common case is a fixed schema, which mastering compresses.
type entryShape struct {
	names []int
	vals  []int
}

func (e entryShape) masterBytes() int {
	n := 1 + uvarintLen(0) + uvarintLen(0) + uvarintLen(uint64(len(e.names)))
	for i := range e.names {
		n += uvarintLen(uint64(e.names[i])) + e.names[i]
		n += uvarintLen(uint64(e.vals[i])) + e.vals[i]
	}
	return n
}

func (e entryShape) sameBytes(msDelta uint64, seqDelta int64) int {
	n := 1 + uvarintLen(msDelta) + uvarintLen(zigzag(seqDelta))
	for _, v := range e.vals {
		n += uvarintLen(uint64(v)) + v
	}
	return n
}

// block models one sealed arena block: the 48-byte header plus the encoded entry
// bytes. deleted counts the entries whose deleted flag is set (the tombstone side
// of section 6.5), which does not shrink the blob.
type block struct {
	blob     []byte
	count    int
	deleted  int
	firstMs  uint64
	firstSeq uint64
	lastMs   uint64
	lastSeq  uint64
}

func newBlock(capBytes int, firstMs, firstSeq uint64) *block {
	return &block{blob: make([]byte, 0, capBytes), firstMs: firstMs, firstSeq: firstSeq}
}

func (b *block) used() int  { return blockHdr + len(b.blob) }
func (b *block) live() int  { return b.count - b.deleted }
func (b *block) bytes() int { return dirLeaf + b.used() }

// appendEntry encodes one entry at the tail, master if first, same-schema otherwise.
func (b *block) appendEntry(e entryShape, ms, seq uint64) {
	var tmp [10]byte
	put := func(x uint64) {
		n := binary.PutUvarint(tmp[:], x)
		b.blob = append(b.blob, tmp[:n]...)
	}
	if b.count == 0 {
		b.blob = append(b.blob, 0) // flags
		put(0)
		put(0)
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
	b.lastMs, b.lastSeq = ms, seq
	b.count++
}

// tombstoneOldest flips the deleted flag on the first k live entries by walking the
// blob and rewriting the flags byte, the real exact-mode boundary work: the blob
// keeps its bytes, only the flag and the deleted counter change.
func (b *block) tombstoneOldest(k int, nFields int) int {
	n, p, i := 0, 0, 0
	for p < len(b.blob) && n < k {
		flagsAt := p
		flags := b.blob[p]
		p++
		_, m := binary.Uvarint(b.blob[p:]) // msDelta
		p += m
		_, m = binary.Uvarint(b.blob[p:]) // seqDelta
		p += m
		fields := nFields
		if i == 0 {
			nf, m2 := binary.Uvarint(b.blob[p:])
			p += m2
			fields = int(nf)
			for f := 0; f < fields; f++ {
				nl, m3 := binary.Uvarint(b.blob[p:])
				p += m3 + int(nl)
				vl, m4 := binary.Uvarint(b.blob[p:])
				p += m4 + int(vl)
			}
		} else {
			for f := 0; f < fields; f++ {
				vl, m3 := binary.Uvarint(b.blob[p:])
				p += m3 + int(vl)
			}
		}
		if flags&2 == 0 {
			b.blob[flagsAt] = flags | 2
			b.deleted++
			n++
		}
		i++
	}
	return n
}

// stream models the append log with a base offset, the directory truncation of
// section 6.6: dropping front blocks reslices without renumbering survivors.
type stream struct {
	blocks   []*block
	capBytes int
	entryCap int
	rate     uint64
	ms, seq  uint64
	length   int // live entries
	base     int // logical index of blocks[0], the dropped-block count
	nFields  int
}

func newStream(capBytes, entryCap int, rate uint64, nFields int) *stream {
	return &stream{capBytes: capBytes, entryCap: entryCap, rate: rate, ms: 1, nFields: nFields}
}

func (s *stream) nextID() (uint64, uint64) {
	ms, seq := s.ms, s.seq
	s.seq++
	if s.seq >= s.rate {
		s.seq = 0
		s.ms++
	}
	return ms, seq
}

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
	s.length++
}

// trimResult is what one trim reports: entries removed, front blocks dropped,
// bytes reclaimed immediately (dropped-block resident bytes; boundary tombstones
// reclaim none now), directory operations charged, and the leftover overshoot
// above the threshold (approximate only).
type trimResult struct {
	removed       int
	blocksDropped int
	reclaimed     int64
	dirOps        int
	overshoot     int
}

// dropFront drops whole front blocks while removing one keeps at least keep live
// entries, the shared approximate step. It reslices to a fresh slice and bumps the
// base, freeing the dropped blocks' bytes.
func (s *stream) dropFront(keep int) trimResult {
	drop, removed := 0, 0
	var reclaimed int64
	for drop < len(s.blocks)-1 {
		b := s.blocks[drop]
		if s.length-removed-b.live() < keep {
			break
		}
		reclaimed += int64(b.bytes())
		removed += b.live()
		drop++
	}
	if drop > 0 {
		ns := make([]*block, len(s.blocks)-drop)
		copy(ns, s.blocks[drop:])
		s.blocks = ns
		s.base += drop
		s.length -= removed
	}
	return trimResult{removed: removed, blocksDropped: drop, reclaimed: reclaimed, dirOps: drop}
}

// trimApprox is XTRIM MAXLEN ~ keep: whole-block front drops only, the overshoot
// left in the boundary block.
func (s *stream) trimApprox(keep int) trimResult {
	r := s.dropFront(keep)
	r.overshoot = s.length - keep
	if r.overshoot < 0 {
		r.overshoot = 0
	}
	return r
}

// trimExact is XTRIM MAXLEN = keep: the whole-block drops plus tombstoning the
// boundary overshoot. dirOps stays the block-drop count; the boundary work is flag
// writes on one block, not directory operations.
func (s *stream) trimExact(keep int) trimResult {
	r := s.dropFront(keep)
	if over := s.length - keep; over > 0 {
		t := s.blocks[0].tombstoneOldest(over, s.nFields)
		r.removed += t
		s.length -= t
	}
	r.overshoot = 0
	return r
}

// trimPerEntry models the rejected design: front reclaim one entry at a time. It
// frees nothing immediately (a sealed block cannot shrink without a re-encode) and
// charges one directory-class operation per entry, the O(entries) cost section 6.6
// rejects. It only tombstones so the comparison is apples to apples on reclaim.
func (s *stream) trimPerEntry(keep int) trimResult {
	over := s.length - keep
	if over <= 0 {
		return trimResult{}
	}
	removed, ops, bi := 0, 0, 0
	for removed < over && bi < len(s.blocks)-1 {
		b := s.blocks[bi]
		t := b.tombstoneOldest(over-removed, s.nFields)
		removed += t
		ops += t // one operation per entry, the rejected cost
		if b.live() == 0 {
			bi++
		} else {
			break
		}
	}
	s.length -= removed
	return trimResult{removed: removed, reclaimed: 0, dirOps: ops}
}

func buildStream(capBytes, entryCap int, rate uint64, n int, e entryShape) *stream {
	s := newStream(capBytes, entryCap, rate, len(e.vals))
	for i := 0; i < n; i++ {
		s.xadd(e)
	}
	return s
}

func main() {
	quick := flag.Bool("quick", false, "smaller op counts for a fast check")
	flag.Parse()

	fmt.Printf("stream XTRIM reclaim granularity sweep, %s\n", time.Now().Format("2006-01-02"))

	shape := entryShape{names: []int{8, 8, 8}, vals: []int{8, 8, 8}} // the 10.1 measurement shape
	const rate = 1000
	n := 200_000
	if *quick {
		n = 20_000
	}

	// Sweep A: reclaim and directory cost per strategy, over keep fractions. Each
	// arm rebuilds the same stream and trims it once.
	fmt.Println()
	fmt.Println("Sweep A: reclaim per strategy at 4096/128, 3x8B entries (dense IDs)")
	fmt.Printf("%-8s %8s %8s %10s %10s %10s %8s\n",
		"keep%", "removed", "blocks", "~reclB/e", "=reclB/e", "peReclB/e", "~over")
	for _, kf := range []float64{0.9, 0.5, 0.1, 0.01} {
		keep := int(float64(n) * kf)

		sa := buildStream(4096, 128, rate, n, shape)
		ra := sa.trimApprox(keep)
		se := buildStream(4096, 128, rate, n, shape)
		re := se.trimExact(keep)
		sp := buildStream(4096, 128, rate, n, shape)
		rp := sp.trimPerEntry(keep)

		perE := func(r trimResult) float64 {
			if r.removed == 0 {
				return 0
			}
			return float64(r.reclaimed) / float64(r.removed)
		}
		fmt.Printf("%-8.0f %8d %8d %10.2f %10.2f %10.2f %8d\n",
			kf*100, re.removed, ra.blocksDropped, perE(ra), perE(re), perE(rp), ra.overshoot)
		_ = rp
	}

	// Sweep B: directory operations, whole-block drop versus per-entry, the O(blocks)
	// versus O(entries) claim. Trim to keep 10 percent.
	fmt.Println()
	fmt.Println("Sweep B: trim operations to keep 10%, block-drop vs per-entry")
	fmt.Printf("%-10s %10s %10s %10s %10s\n",
		"entries", "removed", "blkDrops", "peOps", "opsRatio")
	for _, en := range scaleN(n) {
		keep := en / 10
		sa := buildStream(4096, 128, rate, en, shape)
		ra := sa.trimApprox(keep)
		sp := buildStream(4096, 128, rate, en, shape)
		rp := sp.trimPerEntry(keep)
		ratio := 0.0
		if ra.dirOps > 0 {
			ratio = float64(rp.dirOps) / float64(ra.dirOps)
		}
		fmt.Printf("%-10d %10d %10d %10d %10.1f\n", en, ra.removed, ra.dirOps, rp.dirOps, ratio)
	}

	// Sweep C: approximate overshoot as a fraction of the threshold, across block
	// sizes. The overshoot is bounded by one block, so a larger block (more entries)
	// overshoots a small threshold more; this is the ~ memory cost = pays to avoid.
	fmt.Println()
	fmt.Println("Sweep C: approximate overshoot vs threshold, by entry cap (keep target 1000)")
	fmt.Printf("%-10s %9s %10s %10s\n", "cap", "ent/blk", "left", "over%")
	for _, cap := range []int{32, 64, 128, 256} {
		s := buildStream(4096, cap, rate, n, shape)
		epb := float64(s.length) / float64(len(s.blocks))
		r := s.trimApprox(1000)
		overPct := 100 * float64(r.overshoot) / 1000
		fmt.Printf("%-10d %9.1f %10d %10.1f\n", cap, epb, s.length, overPct)
	}

	// Sweep D: trim latency, approximate versus exact, keeping 10 percent. The
	// approximate drop is O(blocks); exact adds O(overshoot) boundary flag writes.
	fmt.Println()
	fmt.Println("Sweep D: trim latency to keep 10%, 3x8B entries")
	fmt.Printf("%-10s %12s %12s\n", "entries", "approxNs", "exactNs")
	for _, en := range scaleN(n) {
		keep := en / 10
		// Bound the prebuilt streams to ~2M entries so a large arm does not thrash
		// GC during timing (which would read as the trim being slower than it is).
		reps := 2_000_000 / en
		if reps < 5 {
			reps = 5
		}
		if reps > 200 {
			reps = 200
		}
		if *quick {
			reps = 5
		}
		aNs := timeTrim(reps, func() *stream { return buildStream(4096, 128, rate, en, shape) },
			func(s *stream) { s.trimApprox(keep) })
		eNs := timeTrim(reps, func() *stream { return buildStream(4096, 128, rate, en, shape) },
			func(s *stream) { s.trimExact(keep) })
		fmt.Printf("%-10d %12.0f %12.0f\n", en, aNs, eNs)
	}
}

// scaleN returns the entry counts Sweep B and D run, capped at the sweep's n.
func scaleN(n int) []int {
	all := []int{10_000, 100_000, 1_000_000}
	out := make([]int, 0, len(all))
	for _, v := range all {
		if v <= n*5 {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		out = []int{n}
	}
	return out
}

// timeTrim times one trim, rebuilding the stream fresh each rep so the trim never
// sees an already-trimmed log, and returns nanoseconds per trim.
func timeTrim(reps int, build func() *stream, trim func(*stream)) float64 {
	streams := make([]*stream, reps)
	for i := range streams {
		streams[i] = build()
	}
	s := time.Now()
	for _, st := range streams {
		trim(st)
	}
	return float64(time.Since(s).Nanoseconds()) / float64(reps)
}
