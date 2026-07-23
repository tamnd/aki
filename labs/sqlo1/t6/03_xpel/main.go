// Lab: PEL segment size and pending-surface latency (spec 2064/sqlo1
// doc 10 sections 1, 3, and 7, milestone T6 lab 03).
//
// The kind 5 slice bakes the PEL segment cut thresholds: seg_max in
// encoded bytes and pcap in entries, whichever binds first. The trade
// is sharper than the run-size trade because the PEL fence lives in
// the group record, which plays the root-discipline role for its
// segments (doc 10 W1-W4 note). Every XREADGROUP frame group carries
// the group record post-image anyway (last_delivered advances), so
// the fence rides free on delivery, but its size does not: at 10^6
// pending a 28 B fence entry per segment puts hundreds of kilobytes
// into a record that is rebilled on every delivery batch and every
// ACK batch. Bigger segments shrink the fence and the group bill but
// fatten the per-touch rewrite: an ACK rewrites its segment's full
// post-image, an XCLAIM the same, so a hot claim loop over a fat
// segment pays the whole segment per touched ID cluster. The lab
// prices both sides across pending populations 10^2..10^6 and picks
// the caps.
//
// The latency arms measure the read surface the fence buys: XPENDING
// extended is a fence seek plus a segment walk in ID order, and
// XAUTOCLAIM is the same walk rewriting touched segments behind a
// cursor. Both are measured as ns per pending entry at each
// population so the T6 pending-surface slice inherits priced
// expectations, not guesses. The ack arms run in delivery order (the
// FIFO consumer, whole head segments empty and drop) and in random
// order (the pathological spread that touches a different segment
// per ID).
//
// The model is the doc 10 shape resident, no store underneath (the
// xadd lab pattern). PEL entries are { varint dms, varint dseq,
// consumer_idx u8, flags u8, delivery_count u32, delivery_time u64 }
// in ID order inside segments behind the group record's fence of
// 28 B entries { ms, seq, pelsegid, count }. The WAL column is
// modeled arithmetic under W2 and W4: a delivery batch bills the
// amended tail segment's post-image plus the group record, an ACK
// batch bills each touched segment's post-image (or a 16 B tombstone
// when it empties) plus the group record, a claim bills the touched
// segments plus the group record. Entry runs are never billed; a
// counter proves the model never touches them, X-I3's lab half. An
// oracle test pins the model against a reference map through random
// deliver, ack, and claim interleavings, and the codec roundtrips.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"
)

// Encoded sizes, doc 10 sections 1 and 3: segment payloads carry an
// 18 B header (base ID, n) behind the 12 B segment envelope; the
// group record costs its 28 B header plus the name, the consumer
// list, and one 28 B fence entry per PEL segment.
const (
	segEnvBytes    = 12
	segHdrBytes    = 18
	fenceEntBytes  = 28
	groupHdrBytes  = 28
	groupNameB     = 12     // 4 B length prefix + a short group name
	consBytes      = 2 * 30 // two consumers: length prefix, name, seen, active, pel_count
	tombBytes      = 16
	fencePageBytes = 4096
	fencePageEnts  = 146
	pageIdxBytes   = 28
	drainThreshold = 8 << 20
)

func varintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// pelEntry is one pending entry: the delivered ID plus ownership and
// delivery bookkeeping.
type pelEntry struct {
	ms, seq uint64
	cidx    byte
	flags   byte
	dcount  uint32
	dtime   uint64
}

func (e *pelEntry) less(o *pelEntry) bool {
	return e.ms < o.ms || (e.ms == o.ms && e.seq < o.seq)
}

// entrySize is the encoded cost of e appended after prev (nil for
// the segment's first entry, whose deltas are zero by construction).
func entrySize(prev, e *pelEntry) int {
	var dms, dseq uint64
	if prev != nil {
		dms = e.ms - prev.ms
		if dms == 0 {
			dseq = e.seq - prev.seq
		} else {
			dseq = e.seq
		}
	}
	return varintLen(dms) + varintLen(dseq) + 1 + 1 + 4 + 8
}

// naiveSize is the same entry with a full 16 B ID and no deltas, the
// baseline the doc 10 16-20 B claim is priced against.
func naiveSize() int { return 16 + 1 + 1 + 4 + 8 }

// seg is one contiguous ID-range slice of the PEL.
type seg struct {
	id    uint64
	ents  []pelEntry
	bytes int // encoded size including envelope and header
}

// recount recomputes the canonical encoded size after a rewrite, the
// same from-scratch re-encode the amendment discipline is pinned to.
func (s *seg) recount() {
	s.bytes = segEnvBytes + segHdrBytes
	var prev *pelEntry
	for i := range s.ents {
		s.bytes += entrySize(prev, &s.ents[i])
		prev = &s.ents[i]
	}
}

// billing is the modeled WAL and drain state.
type billing struct {
	walBytes   int64
	walFrames  int64
	groupBytes int64 // group-record share of walBytes
	cuts       int64
	drops      int64
	segRewr    int64
	runRewr    int64 // must stay zero, X-I3's lab half
	dirty      map[uint64]int
	dirtyBytes int64
	drains     int64
	drainedB   int64
	logicalB   int64
}

func newBilling() *billing { return &billing{dirty: map[uint64]int{}} }

func (b *billing) reset() { *b = billing{dirty: map[uint64]int{}} }

// pel is the doc 10 shape: ID-ordered segments behind the group
// record's fence.
type pel struct {
	segMax  int
	pcap    int
	paged   bool // fence in kind 5 pages instead of inline in the group record
	segs    []*seg
	nextID  uint64
	pending int
	bill    *billing
}

func newPel(segMax, pcap int, bill *billing) *pel {
	return &pel{segMax: segMax, pcap: pcap, bill: bill}
}

// groupBill is the group record post-image: header, name, consumers,
// and the PEL fence, doc 10's root-discipline record for kind 5.
// The paged alternative keeps only a page index in the record, the
// same escape T2 and the stream fence took when their fences
// fattened, and pays one 4 KiB page per touched fence region
// instead.
func (p *pel) groupBill() int {
	base := groupHdrBytes + groupNameB + consBytes
	if p.paged {
		pages := (len(p.segs) + fencePageEnts - 1) / fencePageEnts
		return base + pages*pageIdxBytes
	}
	return base + len(p.segs)*fenceEntBytes
}

func (p *pel) billSeg(s *seg) {
	b := p.bill
	b.walBytes += int64(s.bytes)
	b.walFrames++
	b.dirtyBytes += int64(s.bytes - b.dirty[s.id])
	b.dirty[s.id] = s.bytes
	if b.dirtyBytes >= drainThreshold {
		b.drains++
		b.drainedB += b.dirtyBytes
		b.dirty = map[uint64]int{}
		b.dirtyBytes = 0
	}
}

func (p *pel) billFenceRec(key uint64, n int) {
	b := p.bill
	b.walBytes += int64(n)
	b.groupBytes += int64(n)
	b.walFrames++
	b.dirtyBytes += int64(n - b.dirty[key])
	b.dirty[key] = n
	if b.dirtyBytes >= drainThreshold {
		b.drains++
		b.drainedB += b.dirtyBytes
		b.dirty = map[uint64]int{}
		b.dirtyBytes = 0
	}
}

// billFence bills the fence edit for the segments at the given
// ordinals: inline, the whole group record carries the fence; paged,
// the group record stays small and each touched fence page bills
// 4 KiB. A call with no ordinals bills the group record alone (a
// last_delivered advance or per-consumer count move).
func (p *pel) billFence(segIdxs ...int) {
	p.billFenceRec(0, p.groupBill())
	if !p.paged {
		return
	}
	pages := map[int]bool{}
	for _, si := range segIdxs {
		pages[si/fencePageEnts] = true
	}
	for pg := range pages {
		p.billFenceRec(uint64(1)<<62+uint64(pg), fencePageBytes)
	}
}

func (p *pel) dropSeg(s *seg) {
	b := p.bill
	b.dirtyBytes -= int64(b.dirty[s.id])
	delete(b.dirty, s.id)
	b.drops++
	b.walBytes += tombBytes
	b.walFrames++
}

// deliver appends one batch of freshly delivered entries at the
// tail, the XREADGROUP (>) shape: amend the tail segment, cut on
// either cap, one group record bill for the batch (last_delivered
// advanced and the fence counts moved).
func (p *pel) deliver(batch []pelEntry) {
	touched := map[uint64]*seg{}
	firstIdx := max(len(p.segs)-1, 0)
	for i := range batch {
		e := &batch[i]
		p.bill.logicalB += int64(naiveSize())
		var t *seg
		if len(p.segs) > 0 {
			t = p.segs[len(p.segs)-1]
		}
		if t != nil && len(t.ents) > 0 && !t.ents[len(t.ents)-1].less(e) {
			panic("deliver: non-monotonic ID")
		}
		var prev *pelEntry
		if t != nil && len(t.ents) > 0 {
			prev = &t.ents[len(t.ents)-1]
		}
		if t == nil || t.bytes+entrySize(prev, e) > p.segMax || len(t.ents) >= p.pcap {
			p.nextID++
			t = &seg{id: p.nextID, bytes: segEnvBytes + segHdrBytes + entrySize(nil, e)}
			p.segs = append(p.segs, t)
			p.bill.cuts++
		} else {
			t.bytes += entrySize(prev, e)
		}
		t.ents = append(t.ents, *e)
		p.pending++
		touched[t.id] = t
	}
	for _, s := range touched {
		p.billSeg(s)
	}
	idxs := make([]int, 0, len(p.segs)-firstIdx)
	for i := firstIdx; i < len(p.segs); i++ {
		idxs = append(idxs, i)
	}
	p.billFence(idxs...)
}

// segFor is the fence seek: the last segment whose base is at or
// below the ID.
func (p *pel) segFor(ms, seq uint64) int {
	i := sort.Search(len(p.segs), func(i int) bool {
		b := &p.segs[i].ents[0]
		return b.ms > ms || (b.ms == ms && b.seq > seq)
	})
	return i - 1
}

// ack removes one batch of IDs, the XACK shape: group by segment,
// rewrite each touched post-image or drop the emptied segment, one
// group record bill for the fence counts. Returns acked.
func (p *pel) ack(ids [][2]uint64) int {
	bySeg := map[int][][2]uint64{}
	for _, id := range ids {
		i := p.segFor(id[0], id[1])
		if i < 0 {
			continue
		}
		bySeg[i] = append(bySeg[i], id)
	}
	acked := 0
	var emptied []int
	for i, del := range bySeg {
		s := p.segs[i]
		kept := s.ents[:0]
		for _, e := range s.ents {
			hit := false
			for _, id := range del {
				if e.ms == id[0] && e.seq == id[1] {
					hit = true
					break
				}
			}
			if hit {
				acked++
			} else {
				kept = append(kept, e)
			}
		}
		s.ents = kept
		if len(s.ents) == 0 {
			emptied = append(emptied, i)
			continue
		}
		s.recount()
		p.bill.segRewr++
		p.billSeg(s)
	}
	if len(emptied) > 0 {
		sort.Sort(sort.Reverse(sort.IntSlice(emptied)))
		for _, i := range emptied {
			p.dropSeg(p.segs[i])
			p.segs = append(p.segs[:i], p.segs[i+1:]...)
		}
	}
	p.pending -= acked
	if acked > 0 {
		idxs := make([]int, 0, len(bySeg))
		for i := range bySeg {
			idxs = append(idxs, i)
		}
		p.billFence(idxs...)
	}
	return acked
}

// claim reassigns up to count entries starting at the cursor
// (inclusive, the XAUTOCLAIM cursor contract) to consumer nc: fence
// seek, walk, rewrite each touched segment, one group record bill
// (per-consumer counts moved). Returns the claimed count and the
// next cursor, zero once the walk ran off the tail.
func (p *pel) claim(curMs, curSeq uint64, count int, nc byte, now uint64) (int, [2]uint64) {
	i := max(p.segFor(curMs, curSeq), 0)
	claimed := 0
	next := [2]uint64{0, 0}
	for ; i < len(p.segs) && claimed < count; i++ {
		s := p.segs[i]
		touched := false
		for j := range s.ents {
			e := &s.ents[j]
			if e.ms < curMs || (e.ms == curMs && e.seq < curSeq) {
				continue
			}
			if claimed >= count {
				next = [2]uint64{e.ms, e.seq}
				break
			}
			e.cidx = nc
			e.dcount++
			e.dtime = now
			claimed++
			touched = true
		}
		if touched {
			s.recount()
			p.bill.segRewr++
			p.billSeg(s)
		}
	}
	// The batch ran out exactly at a segment boundary: the next
	// unexamined entry is the following segment's base.
	if next == ([2]uint64{0, 0}) && i < len(p.segs) {
		b := &p.segs[i].ents[0]
		next = [2]uint64{b.ms, b.seq}
	}
	// A claim moves per-consumer counts in the group record but no
	// fence counts, so no page is touched in the paged shape.
	if claimed > 0 {
		p.billFence()
	}
	return claimed, next
}

// walk hands every pending entry to emit in ID order, the XPENDING
// extended view and the oracle's.
func (p *pel) walk(emit func(e *pelEntry)) {
	for _, s := range p.segs {
		for i := range s.ents {
			emit(&s.ents[i])
		}
	}
}

// encodeSeg serializes a segment into the doc 10 payload for the
// encode arm and the roundtrip oracle.
func encodeSeg(buf []byte, s *seg) []byte {
	buf = buf[:0]
	first := &s.ents[0]
	buf = binary.LittleEndian.AppendUint64(buf, first.ms)
	buf = binary.LittleEndian.AppendUint64(buf, first.seq)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s.ents)))
	var prev *pelEntry
	for i := range s.ents {
		e := &s.ents[i]
		var dms, dseq uint64
		if prev != nil {
			dms = e.ms - prev.ms
			if dms == 0 {
				dseq = e.seq - prev.seq
			} else {
				dseq = e.seq
			}
		}
		buf = binary.AppendUvarint(buf, dms)
		buf = binary.AppendUvarint(buf, dseq)
		buf = append(buf, e.cidx, e.flags)
		buf = binary.LittleEndian.AppendUint32(buf, e.dcount)
		buf = binary.LittleEndian.AppendUint64(buf, e.dtime)
		prev = e
	}
	return buf
}

// decodeSeg reverses encodeSeg for the roundtrip oracle.
func decodeSeg(buf []byte) ([]pelEntry, error) {
	if len(buf) < segHdrBytes {
		return nil, fmt.Errorf("short header")
	}
	baseMs := binary.LittleEndian.Uint64(buf)
	baseSeq := binary.LittleEndian.Uint64(buf[8:])
	n := int(binary.LittleEndian.Uint16(buf[16:]))
	p := segHdrBytes
	ents := make([]pelEntry, 0, n)
	prevMs, prevSeq := baseMs, baseSeq
	for i := range n {
		dms, k := binary.Uvarint(buf[p:])
		p += k
		dseq, k := binary.Uvarint(buf[p:])
		p += k
		ms, seq := prevMs+dms, dseq
		if dms == 0 {
			seq = prevSeq + dseq
		}
		if i == 0 {
			ms, seq = baseMs, baseSeq
		}
		e := pelEntry{ms: ms, seq: seq, cidx: buf[p], flags: buf[p+1]}
		p += 2
		e.dcount = binary.LittleEndian.Uint32(buf[p:])
		p += 4
		e.dtime = binary.LittleEndian.Uint64(buf[p:])
		p += 8
		ents = append(ents, e)
		prevMs, prevSeq = ms, seq
	}
	if p != len(buf) {
		return nil, fmt.Errorf("trailing %d bytes", len(buf)-p)
	}
	return ents, nil
}

type config struct {
	mix     string
	segMax  int
	pcap    int
	pending int
	batch   int
	order   string
	fence   string
	seed    int64
}

// gen mints delivery IDs: ten per millisecond, the same same-ms
// burst shape the xadd lab used.
type gen struct {
	ms, seq uint64
	n       int
}

func (g *gen) next(now uint64) pelEntry {
	if g.n%10 == 0 {
		g.ms++
		g.seq = 0
	} else {
		g.seq++
	}
	g.n++
	return pelEntry{ms: g.ms, seq: g.seq, cidx: 0, dcount: 1, dtime: now}
}

func row(cfg config, workload string, ops int, nsOp int64, framesOp, walBOp, x1, x2, x3, x4 float64) {
	fmt.Printf("%s,%d,%d,%d,%d,%s,%s,%s,%d,%d,%.3f,%.1f,%.3f,%.3f,%.3f,%.3f\n",
		cfg.mix, cfg.segMax, cfg.pcap, cfg.pending, cfg.batch, cfg.order, cfg.fence, workload, ops, nsOp, framesOp, walBOp, x1, x2, x3, x4)
}

func shapeRow(cfg config, p *pel) {
	entsPerSeg := 0.0
	if len(p.segs) > 0 {
		entsPerSeg = float64(p.pending) / float64(len(p.segs))
	}
	row(cfg, "shape", p.pending, 0, 0, 0, entsPerSeg, float64(len(p.segs)), float64(p.groupBill()), 0)
}

func drainRow(cfg config, b *billing) {
	wa := 0.0
	if b.logicalB > 0 {
		wa = float64(b.drainedB+b.dirtyBytes) / float64(b.logicalB)
	}
	row(cfg, "drain", int(b.drains), 0, float64(b.walFrames), wa, float64(b.dirtyBytes), 0, 0, 0)
}

func groupShare(b *billing) float64 {
	if b.walBytes == 0 {
		return 0
	}
	return float64(b.groupBytes) / float64(b.walBytes)
}

// prefill delivers cfg.pending entries in cfg.batch batches without
// timing, then resets the bill so the measured arm starts clean.
func prefill(cfg config, p *pel, g *gen) {
	batch := make([]pelEntry, 0, cfg.batch)
	for i := 0; i < cfg.pending; i += len(batch) {
		batch = batch[:0]
		for len(batch) < cfg.batch && i+len(batch) < cfg.pending {
			batch = append(batch, g.next(1000))
		}
		p.deliver(batch)
	}
	p.bill.reset()
}

// runDeliver is the XREADGROUP arm: batched deliveries into a
// growing PEL, the tail-amendment bill plus the group record per
// batch as the fence fattens.
func runDeliver(cfg config) {
	bill := newBilling()
	p := newPel(cfg.segMax, cfg.pcap, bill)
	p.paged = cfg.fence == "paged"
	g := &gen{}
	batch := make([]pelEntry, 0, cfg.batch)
	start := time.Now()
	for i := 0; i < cfg.pending; i += len(batch) {
		batch = batch[:0]
		for len(batch) < cfg.batch && i+len(batch) < cfg.pending {
			batch = append(batch, g.next(1000))
		}
		p.deliver(batch)
	}
	elapsed := time.Since(start)
	row(cfg, "deliver", cfg.pending, elapsed.Nanoseconds()/int64(cfg.pending),
		float64(bill.walFrames)/float64(cfg.pending), float64(bill.walBytes)/float64(cfg.pending),
		groupShare(bill), float64(bill.cuts)*1000/float64(cfg.pending), 0, 0)
	shapeRow(cfg, p)
	drainRow(cfg, bill)
}

// runAck is the ACK-storm arm: prefill, then ack everything in
// batches, in delivery order (head segments empty and drop) or
// random order (every batch sprays across the fence).
func runAck(cfg config) {
	bill := newBilling()
	p := newPel(cfg.segMax, cfg.pcap, bill)
	p.paged = cfg.fence == "paged"
	g := &gen{}
	prefill(cfg, p, g)
	ids := make([][2]uint64, 0, cfg.pending)
	p.walk(func(e *pelEntry) { ids = append(ids, [2]uint64{e.ms, e.seq}) })
	if cfg.order == "random" {
		rng := rand.New(rand.NewSource(cfg.seed))
		rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	}
	acked := 0
	start := time.Now()
	for i := 0; i < len(ids); i += cfg.batch {
		j := min(i+cfg.batch, len(ids))
		acked += p.ack(ids[i:j])
	}
	elapsed := time.Since(start)
	if acked != cfg.pending || p.pending != 0 || len(p.segs) != 0 {
		panic("ack arm did not drain the PEL")
	}
	row(cfg, "ack", acked, elapsed.Nanoseconds()/int64(acked),
		float64(bill.walFrames)/float64(acked), float64(bill.walBytes)/float64(acked),
		groupShare(bill), float64(bill.segRewr)*1000/float64(acked),
		float64(bill.drops)*1000/float64(acked), 0)
	drainRow(cfg, bill)
}

// runClaim is the XAUTOCLAIM arm: prefill to consumer 0, then sweep
// the whole PEL to consumer 1 behind the cursor in count batches.
func runClaim(cfg config) {
	bill := newBilling()
	p := newPel(cfg.segMax, cfg.pcap, bill)
	p.paged = cfg.fence == "paged"
	g := &gen{}
	prefill(cfg, p, g)
	claimed := 0
	cur := [2]uint64{0, 0}
	start := time.Now()
	for claimed < cfg.pending {
		n, next := p.claim(cur[0], cur[1], cfg.batch, 1, 2000)
		if n == 0 {
			break
		}
		claimed += n
		cur = next
		if cur == ([2]uint64{0, 0}) {
			break
		}
	}
	elapsed := time.Since(start)
	if claimed != cfg.pending {
		panic("claim arm did not cover the PEL")
	}
	ok := 0
	p.walk(func(e *pelEntry) {
		if e.cidx == 1 && e.dcount == 2 {
			ok++
		}
	})
	if ok != cfg.pending {
		panic("claim arm left unclaimed entries")
	}
	row(cfg, "claim", claimed, elapsed.Nanoseconds()/int64(claimed),
		float64(bill.walFrames)/float64(claimed), float64(bill.walBytes)/float64(claimed),
		groupShare(bill), float64(bill.segRewr)*1000/float64(claimed), 0, 0)
	drainRow(cfg, bill)
}

// runScan is the XPENDING extended arm: full-range walks with the
// idle filter check per entry, read-only, ns per entry and per call.
func runScan(cfg config) {
	bill := newBilling()
	p := newPel(cfg.segMax, cfg.pcap, bill)
	p.paged = cfg.fence == "paged"
	g := &gen{}
	prefill(cfg, p, g)
	reps := max(1, 2_000_000/cfg.pending)
	now := uint64(5000)
	var matched int
	start := time.Now()
	for range reps {
		p.walk(func(e *pelEntry) {
			if now-e.dtime >= 1000 {
				matched++
			}
		})
	}
	elapsed := time.Since(start)
	total := int64(reps) * int64(cfg.pending)
	if int64(matched) != total {
		panic("scan arm missed entries")
	}
	row(cfg, "scan", int(total), elapsed.Nanoseconds()/total,
		0, 0, float64(elapsed.Nanoseconds()/int64(reps)), float64(len(p.segs)), 0, 0)
}

// runEncode prices the codec: real encoding of the prefilled
// segments, bytes and nanoseconds per entry against the naive
// no-delta baseline, the doc 10 16-20 B claim.
func runEncode(cfg config) {
	bill := newBilling()
	p := newPel(cfg.segMax, cfg.pcap, bill)
	p.paged = cfg.fence == "paged"
	g := &gen{}
	prefill(cfg, p, g)
	var buf []byte
	encoded, entries := 0, 0
	start := time.Now()
	for _, s := range p.segs {
		buf = encodeSeg(buf, s)
		encoded += len(buf)
		entries += len(s.ents)
	}
	elapsed := time.Since(start)
	naive := entries * naiveSize()
	row(cfg, "encode", entries, elapsed.Nanoseconds()/int64(entries),
		0, 0, float64(encoded)/float64(entries), float64(naive)/float64(entries),
		float64(encoded)/float64(naive), float64(len(p.segs)))
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.mix, "mix", "deliver", "op mix: deliver, ack, claim, scan, encode")
	flag.IntVar(&cfg.segMax, "segmax", 4096, "segment cut threshold in encoded bytes")
	flag.IntVar(&cfg.pcap, "pcap", 1024, "segment entry cap")
	flag.IntVar(&cfg.pending, "pending", 100000, "pending entries in the PEL")
	flag.IntVar(&cfg.batch, "batch", 10, "IDs per XREADGROUP/XACK/XAUTOCLAIM call")
	flag.StringVar(&cfg.order, "order", "fifo", "ack order: fifo, random")
	flag.StringVar(&cfg.fence, "fence", "inline", "PEL fence shape: inline, paged")
	flag.Int64Var(&cfg.seed, "seed", 47, "rng seed")
	flag.Parse()
	if *quick {
		cfg.pending = min(cfg.pending, 5000)
	}
	switch cfg.mix {
	case "deliver":
		runDeliver(cfg)
	case "ack":
		runAck(cfg)
	case "claim":
		runClaim(cfg)
	case "scan":
		runScan(cfg)
	case "encode":
		runEncode(cfg)
	default:
		fmt.Fprintf(os.Stderr, "unknown mix %q\n", cfg.mix)
		os.Exit(2)
	}
}
