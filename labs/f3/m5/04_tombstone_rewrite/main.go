// Lab: the gc rewrite of a partially-tombstoned stream block, and the
// stream-block-gc-ratio that triggers it (spec 2064/f3/14 section 6.5, M5 lab 04).
//
// The question: XDEL tombstones an entry in a sealed block, it does not rewrite the
// block (section 6.5, the tombstone side of the tombstone-vs-rewrite choice), so a
// sealed block accumulates dead bytes that the front whole-block drop (lab 02) can
// never reclaim, since an interior block is neither at the front nor empty. Section
// 6.5 reclaims those bytes with a deferred, threshold-gated rewrite: a block whose
// deleted/count crosses stream-block-gc-ratio (default 0.5, lab-swept) is rewritten
// by the owner's between-batches step, its live entries re-encoded into a fresh
// block with a new master, the directory repointed, the old block freed. A block
// whose live count hits zero is dropped whole with no rewrite.
//
// So the questions this lab settles: does a rewrite actually reclaim the dead
// fraction's bytes, what does it cost to re-encode the live entries it keeps, and at
// what dead fraction does the reclaim pay for the copy. From first principles the
// break-even is structural: rewriting a block whose dead fraction is f reclaims f of
// its bytes and copies the other 1-f, so reclaimed/copied is f/(1-f), which crosses
// 1.0 at f=0.5. Below half-dead a rewrite copies more than it frees; above it frees
// more than it copies. That is why the default gc-ratio is 0.5, and this lab checks
// the real encoded bytes bend that analytic knee only slightly (a rewrite re-masters,
// so a nearly-full block reclaims a touch less than f, a nearly-empty one a touch
// more), then shows what the ratio buys under sustained interior churn: r=1.0 (never
// rewrite) leaks the full churn fraction as dead bytes, a finite ratio bounds the
// retained dead to about the ratio while paying copy work that grows as the ratio
// falls.
//
// Method: in-process, no server, no wire, no engine import, the same lab-local model
// labs 01 and 02 use. Blocks carry real byte blobs encoded as section 3.3 lays out
// (master whole, same-schema entries as a flags byte plus the two ID deltas against
// the block firstID plus value frames), and a rewrite builds a real fresh blob by
// re-encoding the surviving entries against a new base, so the reclaim figure is the
// true byte difference, re-mastering included. IDs are dense auto-IDs (1000/ms, the
// benchmark-shaped case). Resident cost counts the 48-byte header and the 32-byte
// directory leaf per block plus the entry bytes, matching labs 01 and 02.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
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

// entryShape is the fixed-schema entry the sweep encodes: field name and value byte
// lengths. The common case is a fixed schema, which mastering compresses.
type entryShape struct {
	names []int
	vals  []int
}

// id is one entry's stream ID, retained so a rewrite can re-encode deltas against a
// fresh base.
type id struct{ ms, seq uint64 }

// block models one arena block: the 48-byte header plus the encoded entry bytes,
// alongside the per-entry IDs and a live/dead bit so a rewrite reconstructs the
// surviving entries exactly. deleted counts tombstoned entries (the section 6.5
// tombstone side), which does not shrink the blob.
type block struct {
	blob     []byte
	ids      []id
	dead     []bool
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
	b.ids = append(b.ids, id{ms, seq})
	b.dead = append(b.dead, false)
	b.lastMs, b.lastSeq = ms, seq
	b.count++
}

// tombstone marks entry i dead if it is not already, the XDEL effect: the blob keeps
// its bytes, only the flag byte and the deleted counter change. Returns whether it
// flipped a live entry.
func (b *block) tombstone(i int) bool {
	if b.dead[i] {
		return false
	}
	b.dead[i] = true
	b.deleted++
	return true
}

// deadFrac is the block's tombstone fraction, the gc-ratio predicate's left side.
func (b *block) deadFrac() float64 {
	if b.count == 0 {
		return 0
	}
	return float64(b.deleted) / float64(b.count)
}

// rewrite re-encodes the block's live entries into a fresh block with a new master
// and deltas against the first survivor, the section 6.5 rewrite. Returns nil when
// nothing survives (the caller drops the block whole instead).
func (b *block) rewrite(e entryShape, capBytes int) *block {
	var nb *block
	for i, eid := range b.ids {
		if b.dead[i] {
			continue
		}
		if nb == nil {
			nb = newBlock(capBytes, eid.ms, eid.seq)
		}
		nb.appendEntry(e, eid.ms, eid.seq)
	}
	return nb
}

// stream models the append log with a base offset (section 6.6): dropping or
// rewriting blocks keeps survivors' directory references without a renumber.
type stream struct {
	blocks   []*block
	shape    entryShape
	capBytes int
	entryCap int
	rate     uint64
	ms, seq  uint64
	length   int // live entries
	base     int // logical index of blocks[0]
}

func newStream(capBytes, entryCap int, rate uint64, e entryShape) *stream {
	return &stream{shape: e, capBytes: capBytes, entryCap: entryCap, rate: rate, ms: 1}
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

func (s *stream) xadd() {
	ms, seq := s.nextID()
	var tail *block
	if len(s.blocks) > 0 {
		tail = s.blocks[len(s.blocks)-1]
	}
	need := 0
	if tail == nil || tail.count == 0 {
		need = masterBytes(s.shape)
	} else {
		need = sameBytes(s.shape, ms-tail.firstMs, int64(seq)-int64(tail.firstSeq))
	}
	if tail == nil || tail.count >= s.entryCap || tail.used()+need > s.capBytes {
		tail = newBlock(s.capBytes, ms, seq)
		s.blocks = append(s.blocks, tail)
	}
	tail.appendEntry(s.shape, ms, seq)
	s.length++
}

// gc runs one owner between-batches pass over the sealed blocks: an empty block is
// dropped whole, a block at or past the gc-ratio is rewritten to its live entries.
// The tail block (still filling) is left alone. Returns the pass's work and reclaim.
func (s *stream) gc(ratio float64) gcResult {
	var r gcResult
	sealed := len(s.blocks)
	if sealed > 0 {
		sealed-- // leave the open tail
	}
	dst := 0
	for i := 0; i < len(s.blocks); i++ {
		b := s.blocks[i]
		if i < sealed && b.live() == 0 {
			// Fully tombstoned sealed block: drop whole, free its bytes, no rewrite.
			r.reclaimed += int64(b.bytes())
			r.blocksDropped++
			s.base++ // one fewer live block ahead; keeps survivor refs stable
			continue
		}
		if i < sealed && b.deadFrac() >= ratio && b.deleted > 0 {
			nb := b.rewrite(s.shape, s.capBytes)
			if nb == nil {
				r.reclaimed += int64(b.bytes())
				r.blocksDropped++
				s.base++
				continue
			}
			r.reclaimed += int64(b.used() - nb.used())
			r.entriesCopied += nb.count
			r.blocksRewritten++
			b = nb
		}
		s.blocks[dst] = b
		dst++
	}
	s.blocks = s.blocks[:dst]
	return r
}

// gcResult is what one gc pass reports.
type gcResult struct {
	blocksRewritten int
	blocksDropped   int
	entriesCopied   int
	reclaimed       int64
}

// residentBytes is the stream's live resident footprint: every block's header,
// directory leaf, and encoded entry bytes.
func (s *stream) residentBytes() int64 {
	var t int64
	for _, b := range s.blocks {
		t += int64(b.bytes())
	}
	return t
}

// deadEntries counts the tombstoned entries still encoded (not yet reclaimed by a
// rewrite or drop), the memory the gc ratio governs.
func (s *stream) deadEntries() int {
	d := 0
	for _, b := range s.blocks {
		d += b.deleted
	}
	return d
}

func (s *stream) totalEncoded() int {
	t := 0
	for _, b := range s.blocks {
		t += b.count
	}
	return t
}

func masterBytes(e entryShape) int {
	n := 1 + uvarintLen(0) + uvarintLen(0) + uvarintLen(uint64(len(e.names)))
	for i := range e.names {
		n += uvarintLen(uint64(e.names[i])) + e.names[i]
		n += uvarintLen(uint64(e.vals[i])) + e.vals[i]
	}
	return n
}

func sameBytes(e entryShape, msDelta uint64, seqDelta int64) int {
	n := 1 + uvarintLen(msDelta) + uvarintLen(zigzag(seqDelta))
	for _, v := range e.vals {
		n += uvarintLen(uint64(v)) + v
	}
	return n
}

func buildStream(capBytes, entryCap int, rate uint64, n int, e entryShape) *stream {
	s := newStream(capBytes, entryCap, rate, e)
	for i := 0; i < n; i++ {
		s.xadd()
	}
	return s
}

// fullBlock builds one full block of exactly entryCap dense entries, the unit Sweep
// A rewrites.
func fullBlock(capBytes, entryCap int, rate uint64, e entryShape) *block {
	s := newStream(capBytes, entryCap, rate, e)
	for s.blocks == nil || s.blocks[0].count < entryCap {
		s.xadd()
	}
	return s.blocks[0]
}

// tombstoneFrac marks the first f fraction of a block's entries dead. Which entries
// carry the same fixed-schema byte cost, so the position does not change the reclaim
// accounting, only the count does.
func tombstoneFrac(b *block, f float64) {
	k := int(f * float64(b.count))
	for i := 0; i < k; i++ {
		b.tombstone(i)
	}
}

// churnToDead deletes random live entries in the sealed blocks, running a gc pass at
// the given ratio after each batch, until the cumulative tombstone fraction reaches
// target. It reports the end state and the total copy work the ratio charged.
func churnToDead(s *stream, ratio, target float64, batches int, rng *rand.Rand) churnResult {
	start := s.length
	toDelete := int(target * float64(start))
	per := toDelete / batches
	if per < 1 {
		per = 1
	}
	var res churnResult
	deleted := 0
	for deleted < toDelete {
		n := per
		if deleted+n > toDelete {
			n = toDelete - deleted
		}
		got := 0
		for tries := 0; got < n && tries < n*40; tries++ {
			sealed := len(s.blocks) - 1
			if sealed <= 0 {
				break
			}
			bi := rng.Intn(sealed)
			b := s.blocks[bi]
			ei := rng.Intn(b.count)
			if b.tombstone(ei) {
				got++
			}
		}
		if got == 0 {
			break
		}
		deleted += got
		s.length -= got
		g := s.gc(ratio)
		res.entriesCopied += g.entriesCopied
		res.blocksRewritten += g.blocksRewritten
		res.blocksDropped += g.blocksDropped
	}
	res.deleted = deleted
	res.residentBytes = s.residentBytes()
	res.liveEntries = s.length
	enc := s.totalEncoded()
	if enc > 0 {
		res.deadFracRetained = float64(s.deadEntries()) / float64(enc)
	}
	return res
}

type churnResult struct {
	deleted          int
	entriesCopied    int
	blocksRewritten  int
	blocksDropped    int
	residentBytes    int64
	liveEntries      int
	deadFracRetained float64
}

func main() {
	quick := flag.Bool("quick", false, "smaller op counts for a fast check")
	flag.Parse()

	fmt.Printf("stream gc rewrite ratio sweep, %s\n", time.Now().Format("2006-01-02"))

	shape := entryShape{names: []int{8, 8, 8}, vals: []int{8, 8, 8}} // the 10.1 measurement shape
	const rate = 1000

	// Sweep A: per-block rewrite economics. Tombstone a dead fraction of one full
	// block, rewrite it, and report the real reclaimed bytes, the bytes copied into
	// the fresh block, and their ratio. The break-even where reclaimed equals copied
	// is the principled gc-ratio; f/(1-f) says 0.5 and the encoded bytes bend it only
	// slightly through re-mastering.
	fmt.Println()
	fmt.Println("Sweep A: one full block, rewrite reclaim vs copy by dead fraction")
	fmt.Printf("%-8s %10s %10s %12s %12s %10s\n",
		"dead f", "live", "reclB", "copiedB", "recl/copy", "rewrNs")
	for _, f := range []float64{0.1, 0.25, 0.4, 0.5, 0.6, 0.75, 0.9} {
		b := fullBlock(4096, 128, rate, shape)
		tombstoneFrac(b, f)
		nb := b.rewrite(shape, 4096)
		copied := nb.used()
		reclaimed := b.used() - nb.used()
		ratio := 0.0
		if copied > 0 {
			ratio = float64(reclaimed) / float64(copied)
		}
		reps := 20000
		if *quick {
			reps = 2000
		}
		rewrNs := timeRewrite(reps, func() *block {
			bb := fullBlock(4096, 128, rate, shape)
			tombstoneFrac(bb, f)
			return bb
		}, shape)
		fmt.Printf("%-8.2f %10d %10d %12d %12.3f %10.0f\n",
			f, nb.count, reclaimed, copied, ratio, rewrNs)
	}

	// Sweep B: sustained interior churn, gc-ratio sweep. Build a stream, delete
	// random live entries up to a target fraction in batches, run gc at each ratio,
	// and report the end-state memory and the copy work the ratio charged. r=1.0
	// never rewrites (the leak), lower ratios bound the retained dead at the price of
	// more copies.
	n := 200_000
	if *quick {
		n = 20_000
	}
	fmt.Println()
	fmt.Printf("Sweep B: interior churn to 60%% deleted, gc-ratio sweep (n=%d)\n", n)
	fmt.Printf("%-8s %10s %10s %10s %12s %12s\n",
		"ratio", "copied", "rewrites", "drops", "deadFrac", "B/live")
	for _, r := range []float64{0.25, 0.5, 0.75, 1.0} {
		s := buildStream(4096, 128, rate, n, shape)
		rng := rand.New(rand.NewSource(1))
		cr := churnToDead(s, r, 0.6, 30, rng)
		bpl := 0.0
		if cr.liveEntries > 0 {
			bpl = float64(cr.residentBytes) / float64(cr.liveEntries)
		}
		label := fmt.Sprintf("%.2f", r)
		if r >= 1.0 {
			label = "never"
		}
		fmt.Printf("%-8s %10d %10d %10d %12.3f %12.2f\n",
			label, cr.entriesCopied, cr.blocksRewritten, cr.blocksDropped, cr.deadFracRetained, bpl)
	}

	// Sweep C: rewrite latency by live-entry count, the between-batches cost the owner
	// pays per block. A rewrite re-encodes only the survivors, so its cost tracks the
	// live count, not the block's original fill.
	fmt.Println()
	fmt.Println("Sweep C: rewrite latency by surviving live entries (dead f=0.5)")
	fmt.Printf("%-10s %12s\n", "liveKept", "rewrNs")
	for _, cap := range []int{16, 32, 64, 128} {
		reps := 20000
		if *quick {
			reps = 2000
		}
		rewrNs := timeRewrite(reps, func() *block {
			bb := fullBlock(4096, cap, rate, shape)
			tombstoneFrac(bb, 0.5)
			return bb
		}, shape)
		fmt.Printf("%-10d %12.0f\n", cap/2, rewrNs)
	}
}

// timeRewrite times one block rewrite, rebuilding a fresh tombstoned block each rep
// so the rewrite never sees an already-compacted block, and returns nanoseconds per
// rewrite.
func timeRewrite(reps int, build func() *block, e entryShape) float64 {
	blocks := make([]*block, reps)
	for i := range blocks {
		blocks[i] = build()
	}
	s := time.Now()
	var sink *block
	for _, b := range blocks {
		sink = b.rewrite(e, 4096)
	}
	_ = sink
	return float64(time.Since(s).Nanoseconds()) / float64(reps)
}
