// Lab: stream run size and entries-per-run (spec 2064/sqlo1 doc 10
// sections 1, 3, and 7, milestone T6 lab 01).
//
// T6 slices 1 and 2 bake the run cut thresholds: run_max in encoded
// bytes and ecap in entries, whichever binds first. The trade is the
// same W4 bandwidth knob the list and hash labs priced, felt on the
// append-native type: every XADD bills the amended tail run's full
// post-image in its frame group, so bigger runs make the steady
// append mix carry more WAL bytes per entry, while smaller ones grow
// the fence, cut runs more often (each a fence-shape bill), and make
// a range read touch more runs for the same output. The entry cap
// binds when entries are small (metric points); the byte cap binds on
// fat payloads. PRED-SQLO1-T6-XADD is priced here: WAL bytes and
// frames per XADD across entry shapes, with and without the steady
// MAXLEN ~ trim, plus the fan-out arm where one producer round-robins
// many streams and the drain model prices how tail-run coalescing
// degrades as the dirty set spreads.
//
// The encode arm prices the format itself with a real codec, not
// arithmetic: the master-entry field name table plus varint ID deltas
// against the naive encoding (full 16 B ID and full names per entry),
// in bytes per entry and encode nanoseconds per entry, since doc 10
// claims byte-competitive with Redis's listpack before compression.
//
// The model is the doc 10 shape resident, no store underneath (the
// lnode pattern; the drain-substrate half of the trade was priced on
// the real backends by earlier labs, and what T6 adds is the
// append-shaped bill). Runs hold consecutive entries in ID order
// behind an ID-sorted fence of 28 B entries; the root carries count,
// entries_added, last_id, and friends, all O(1). The WAL column is
// modeled arithmetic under rules W2 and W4, the lnode convention:
// every XADD bills the tail run's post-image, a dropped run bills a
// tombstone, and a fence-shape change bills the inline root whole or
// one fence page plus the root's page index once paged. Drain
// traffic accumulates dirty post-images against the engine's 8 MiB
// threshold for the WA column, which is where the append shape shines
// or suffers: a single stream keeps only its tail run dirty, a wide
// fan-out keeps one tail run dirty per stream. An oracle test pins
// the model against a reference slice through appends, both trim
// forms, and the codec roundtrip.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

// Encoded sizes, doc 10 section 1: run payloads carry a 21 B header
// (base ID, n, nnames, tomb offset) behind the 12 B segment envelope;
// the root header's O(1) fields cost 64 B and fence entries 28 B;
// fence pages hold 146 entries in a 4 KiB record.
const (
	runEnvBytes    = 12
	runHdrBytes    = 21
	rootHdrBytes   = 64
	fenceEntBytes  = 28
	fenceInlineMax = 2048 - rootHdrBytes
	fencePageBytes = 4096
	fencePageEnts  = 146
	tombBytes      = 16
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

// entry is one stream entry: an ID and one value per schema field.
type entry struct {
	ms, seq uint64
	vals    [][]byte
}

// entrySize is the encoded cost of e appended after prev (nil for the
// run's first entry, whose deltas are zero by construction).
func entrySize(prev *entry, e *entry) int {
	var dms, dseq uint64
	if prev != nil {
		dms = e.ms - prev.ms
		if dms == 0 {
			dseq = e.seq - prev.seq
		} else {
			dseq = e.seq
		}
	}
	n := varintLen(dms) + varintLen(dseq) + varintLen(uint64(len(e.vals)))
	for _, v := range e.vals {
		n += 1 + varintLen(uint64(len(v))) + len(v)
	}
	return n
}

// naiveSize is the same entry without the name table or deltas: full
// 16 B ID and full field names per entry, the baseline the doc 10
// encoding is priced against.
func naiveSize(names [][]byte, e *entry) int {
	n := 16 + varintLen(uint64(len(e.vals)))
	for i, v := range e.vals {
		n += 1 + len(names[i]) + 4 + len(v)
	}
	return n
}

// run is one contiguous slice of the entry log.
type run struct {
	id      uint64
	entries []*entry
	bytes   int // encoded size including envelope, header, and name table
}

// stream is the doc 10 shape: an ID-sorted fence of runs plus the
// root's O(1) fields.
type stream struct {
	sid     uint64 // fan-out stream ordinal, part of the dirty key
	runMax  int
	ecap    int
	names   [][]byte
	tblSize int
	runs    []*run
	count   int
	added   int64
	lastMs  uint64
	lastSeq uint64
	nextID  uint64

	bill *billing
}

// billing is the modeled WAL and drain state, shared across streams
// in the fan-out mix so the dirty set and threshold are global, as
// they are in the engine.
type billing struct {
	walBytes   int64
	walFrames  int64
	structural int64
	cuts       int64
	drops      int64
	edgeRewr   int64
	dirty      map[[2]uint64]int
	dirtyBytes int64
	drains     int64
	drainedB   int64
	logicalB   int64
}

func newBilling() *billing {
	return &billing{dirty: map[[2]uint64]int{}}
}

func (b *billing) reset() {
	*b = billing{dirty: map[[2]uint64]int{}}
}

func newStream(sid uint64, runMax, ecap, nnames int, bill *billing) *stream {
	s := &stream{sid: sid, runMax: runMax, ecap: ecap, bill: bill}
	for i := range nnames {
		nm := fmt.Appendf(nil, "field%d", i)
		s.names = append(s.names, nm)
		s.tblSize += 1 + len(nm)
	}
	return s
}

func (s *stream) paged() bool {
	return len(s.runs)*fenceEntBytes > fenceInlineMax
}

// rootBill is the fence-shape bill under W2: the inline root whole,
// or one fence page plus the root's page index once paged.
func (s *stream) rootBill() int {
	if !s.paged() {
		return rootHdrBytes + len(s.runs)*fenceEntBytes
	}
	pages := (len(s.runs) + fencePageEnts - 1) / fencePageEnts
	return fencePageBytes + rootHdrBytes + pages*fenceEntBytes
}

func (s *stream) billRun(r *run) {
	b := s.bill
	b.walBytes += int64(r.bytes)
	b.walFrames++
	k := [2]uint64{s.sid, r.id}
	b.dirtyBytes += int64(r.bytes - b.dirty[k])
	b.dirty[k] = r.bytes
	if b.dirtyBytes >= drainThreshold {
		b.drains++
		b.drainedB += b.dirtyBytes
		b.dirty = map[[2]uint64]int{}
		b.dirtyBytes = 0
	}
}

func (s *stream) billStructural() {
	s.bill.walBytes += int64(s.rootBill())
	s.bill.walFrames++
	s.bill.structural++
}

func (s *stream) dropRun(r *run) {
	b := s.bill
	k := [2]uint64{s.sid, r.id}
	b.dirtyBytes -= int64(b.dirty[k])
	delete(b.dirty, k)
	b.drops++
	b.walBytes += tombBytes
	b.walFrames++
}

// xadd appends one auto-ID entry at the tail, cutting a fresh run on
// either cap, doc 10's O(1) amortized hot path.
func (s *stream) xadd(e *entry) {
	if e.ms < s.lastMs || (e.ms == s.lastMs && s.added > 0 && e.seq <= s.lastSeq) {
		panic("xadd: non-monotonic ID")
	}
	s.count++
	s.added++
	s.lastMs, s.lastSeq = e.ms, e.seq
	for _, v := range e.vals {
		s.bill.logicalB += int64(len(v))
	}
	var t *run
	if len(s.runs) > 0 {
		t = s.runs[len(s.runs)-1]
	}
	var prev *entry
	if t != nil && len(t.entries) > 0 {
		prev = t.entries[len(t.entries)-1]
	}
	if t == nil || t.bytes+entrySize(prev, e) > s.runMax || len(t.entries) >= s.ecap {
		s.nextID++
		t = &run{id: s.nextID, bytes: runEnvBytes + runHdrBytes + s.tblSize}
		t.bytes += entrySize(nil, e)
		s.runs = append(s.runs, t)
		s.bill.cuts++
		s.billStructural()
	} else {
		t.bytes += entrySize(prev, e)
	}
	t.entries = append(t.entries, e)
	s.billRun(t)
}

// trimApprox is XADD MAXLEN ~ (and XTRIM ~): cut whole head runs off
// the fence while the count past the head run still meets the cap,
// doc 10's X-I4 approximate half. Trimmed runs are dropped unread.
func (s *stream) trimApprox(maxlen int) {
	structural := false
	for len(s.runs) > 1 && s.count-len(s.runs[0].entries) >= maxlen {
		r := s.runs[0]
		s.count -= len(r.entries)
		s.runs = s.runs[1:]
		s.dropRun(r)
		structural = true
	}
	if structural {
		s.billStructural()
	}
}

// trimExact is the exact form: whole-run cuts plus one edge-run
// rewrite, X-I4's other half.
func (s *stream) trimExact(maxlen int) {
	s.trimApprox(maxlen)
	if s.count <= maxlen {
		return
	}
	r := s.runs[0]
	cut := s.count - maxlen
	kept := r.entries[cut:]
	nr := &run{id: r.id, bytes: runEnvBytes + runHdrBytes + s.tblSize}
	var prev *entry
	for _, e := range kept {
		nr.bytes += entrySize(prev, e)
		prev = e
	}
	nr.entries = append(nr.entries, kept...)
	s.runs[0] = nr
	s.count = maxlen
	s.bill.edgeRewr++
	s.billRun(nr)
	s.billStructural()
}

// walk hands every live entry to emit in ID order, the oracle's view.
func (s *stream) walk(emit func(e *entry)) {
	for _, r := range s.runs {
		for _, e := range r.entries {
			emit(e)
		}
	}
}

// encodeRun serializes a run into the doc 10 payload for the encode
// arm and the roundtrip oracle: header, name table, delta entries.
func encodeRun(buf []byte, names [][]byte, r *run) []byte {
	buf = buf[:0]
	first := r.entries[0]
	buf = binary.LittleEndian.AppendUint64(buf, first.ms)
	buf = binary.LittleEndian.AppendUint64(buf, first.seq)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(r.entries)))
	buf = append(buf, byte(len(names)))
	buf = binary.LittleEndian.AppendUint16(buf, 0) // no tombs in this lab
	for _, nm := range names {
		buf = append(buf, byte(len(nm)))
		buf = append(buf, nm...)
	}
	var prev *entry
	for _, e := range r.entries {
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
		buf = binary.AppendUvarint(buf, uint64(len(e.vals)))
		for i, v := range e.vals {
			buf = append(buf, byte(i))
			buf = binary.AppendUvarint(buf, uint64(len(v)))
			buf = append(buf, v...)
		}
		prev = e
	}
	return buf
}

// decodeRun reverses encodeRun for the roundtrip oracle.
func decodeRun(buf []byte) (names [][]byte, entries []*entry, err error) {
	if len(buf) < 21 {
		return nil, nil, fmt.Errorf("short header")
	}
	baseMs := binary.LittleEndian.Uint64(buf)
	baseSeq := binary.LittleEndian.Uint64(buf[8:])
	n := int(binary.LittleEndian.Uint16(buf[16:]))
	nnames := int(buf[18])
	p := 21
	for range nnames {
		l := int(buf[p])
		p++
		names = append(names, buf[p:p+l])
		p += l
	}
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
		nf, k := binary.Uvarint(buf[p:])
		p += k
		e := &entry{ms: ms, seq: seq}
		for range nf {
			p++ // nameref
			vl, k := binary.Uvarint(buf[p:])
			p += k
			e.vals = append(e.vals, buf[p:p+int(vl)])
			p += int(vl)
		}
		entries = append(entries, e)
		prevMs, prevSeq = ms, seq
	}
	if p != len(buf) {
		return nil, nil, fmt.Errorf("trailing %d bytes", len(buf)-p)
	}
	return names, entries, nil
}

type config struct {
	mix      string
	runMax   int
	ecap     int
	nfields  int
	elen     int
	burst    int
	maxlen   int
	nstreams int
	ops      int
	seed     int64
}

// gen mints auto-ID entries: burst entries share a millisecond with
// seq counting up, the Redis same-ms shape the varint deltas exist
// for.
type gen struct {
	rng     *rand.Rand
	nfields int
	elen    int
	burst   int
	ms      uint64
	seq     uint64
	n       int
}

func (g *gen) next() *entry {
	if g.n%g.burst == 0 {
		g.ms++
		g.seq = 0
	} else {
		g.seq++
	}
	g.n++
	e := &entry{ms: g.ms, seq: g.seq}
	per := g.elen / g.nfields
	for i := 0; i < g.nfields; i++ {
		v := make([]byte, per)
		for j := range v {
			v[j] = 'a' + byte(g.rng.Intn(26))
		}
		e.vals = append(e.vals, v)
	}
	return e
}

func row(cfg config, workload string, ops int, nsOp int64, framesOp, walBOp, x1, x2, x3, x4 float64) {
	fmt.Printf("%s,%d,%d,%d,%d,%s,%d,%d,%.3f,%.1f,%.3f,%.3f,%.3f,%.3f\n",
		cfg.mix, cfg.runMax, cfg.ecap, cfg.elen, cfg.nstreams, workload, ops, nsOp, framesOp, walBOp, x1, x2, x3, x4)
}

func shapeRow(cfg config, s *stream) {
	entsPerRun, bytesPerRun := 0.0, 0.0
	if len(s.runs) > 0 {
		total := 0
		for _, r := range s.runs {
			total += r.bytes
		}
		entsPerRun = float64(s.count) / float64(len(s.runs))
		bytesPerRun = float64(total) / float64(len(s.runs))
	}
	paged := 0.0
	if s.paged() {
		paged = 1
	}
	row(cfg, "shape", s.count, 0, 0, 0, entsPerRun, bytesPerRun, float64(len(s.runs)), paged)
}

func drainRow(cfg config, b *billing) {
	wa := 0.0
	if b.logicalB > 0 {
		wa = float64(b.drainedB+b.dirtyBytes) / float64(b.logicalB)
	}
	row(cfg, "drain", int(b.drains), 0, float64(b.walFrames), wa, float64(b.dirtyBytes), 0, 0, 0)
}

// runAppend is the pure XADD arm, PRED-SQLO1-T6-XADD's shape: one
// stream, auto IDs, no trim; the fence grows for the whole run so the
// structural bill prices honest fence growth.
func runAppend(cfg config) {
	bill := newBilling()
	s := newStream(0, cfg.runMax, cfg.ecap, cfg.nfields, bill)
	g := &gen{rng: rand.New(rand.NewSource(cfg.seed)), nfields: cfg.nfields, elen: cfg.elen, burst: cfg.burst}
	start := time.Now()
	for range cfg.ops {
		s.xadd(g.next())
	}
	elapsed := time.Since(start)
	row(cfg, "append", cfg.ops, elapsed.Nanoseconds()/int64(cfg.ops),
		float64(bill.walFrames)/float64(cfg.ops), float64(bill.walBytes)/float64(cfg.ops),
		float64(bill.cuts)*1000/float64(cfg.ops), float64(bill.structural)*1000/float64(cfg.ops), 0, 0)
	shapeRow(cfg, s)
	drainRow(cfg, bill)
}

// runFeed is the trimmed feed: every XADD carries MAXLEN ~ maxlen, so
// the head sheds whole runs as the tail grows, the steady state of
// every capped stream in production.
func runFeed(cfg config) {
	bill := newBilling()
	s := newStream(0, cfg.runMax, cfg.ecap, cfg.nfields, bill)
	g := &gen{rng: rand.New(rand.NewSource(cfg.seed)), nfields: cfg.nfields, elen: cfg.elen, burst: cfg.burst}
	for s.count < cfg.maxlen {
		s.xadd(g.next())
	}
	shapeRow(cfg, s)
	bill.reset()
	start := time.Now()
	for range cfg.ops {
		s.xadd(g.next())
		s.trimApprox(cfg.maxlen)
	}
	elapsed := time.Since(start)
	row(cfg, "feed", cfg.ops, elapsed.Nanoseconds()/int64(cfg.ops),
		float64(bill.walFrames)/float64(cfg.ops), float64(bill.walBytes)/float64(cfg.ops),
		float64(bill.drops)*1000/float64(cfg.ops), float64(bill.structural)*1000/float64(cfg.ops),
		float64(s.count), 0)
	drainRow(cfg, bill)
}

// runFanout round-robins XADD across nstreams capped streams sharing
// one drain threshold, the shape that decides whether tail-run
// coalescing survives a wide producer: every stream keeps its own
// tail run dirty, so the dirty set is nstreams runs and drains carry
// partially filled images the wider the fan.
func runFanout(cfg config) {
	bill := newBilling()
	streams := make([]*stream, cfg.nstreams)
	gens := make([]*gen, cfg.nstreams)
	for i := range streams {
		streams[i] = newStream(uint64(i), cfg.runMax, cfg.ecap, cfg.nfields, bill)
		gens[i] = &gen{rng: rand.New(rand.NewSource(cfg.seed + int64(i))), nfields: cfg.nfields, elen: cfg.elen, burst: cfg.burst}
	}
	start := time.Now()
	for i := range cfg.ops {
		k := i % cfg.nstreams
		streams[k].xadd(gens[k].next())
		streams[k].trimApprox(cfg.maxlen)
	}
	elapsed := time.Since(start)
	row(cfg, "fanout", cfg.ops, elapsed.Nanoseconds()/int64(cfg.ops),
		float64(bill.walFrames)/float64(cfg.ops), float64(bill.walBytes)/float64(cfg.ops),
		float64(bill.drops)*1000/float64(cfg.ops), float64(bill.structural)*1000/float64(cfg.ops), 0, 0)
	drainRow(cfg, bill)
}

// runEncode prices the codec itself: real encoding of sealed runs,
// nanoseconds and bytes per entry, against the naive no-table
// no-delta baseline.
func runEncode(cfg config) {
	bill := newBilling()
	s := newStream(0, cfg.runMax, cfg.ecap, cfg.nfields, bill)
	g := &gen{rng: rand.New(rand.NewSource(cfg.seed)), nfields: cfg.nfields, elen: cfg.elen, burst: cfg.burst}
	for range cfg.ops {
		s.xadd(g.next())
	}
	var buf []byte
	encoded, naive, entries := 0, 0, 0
	start := time.Now()
	for _, r := range s.runs {
		buf = encodeRun(buf, s.names, r)
		encoded += len(buf)
		entries += len(r.entries)
	}
	elapsed := time.Since(start)
	for _, r := range s.runs {
		for _, e := range r.entries {
			naive += naiveSize(s.names, e)
		}
	}
	row(cfg, "encode", entries, elapsed.Nanoseconds()/int64(entries),
		0, 0, float64(encoded)/float64(entries), float64(naive)/float64(entries),
		float64(encoded)/float64(naive), float64(len(s.runs)))
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.mix, "mix", "append", "op mix: append, feed, fanout, encode")
	flag.IntVar(&cfg.runMax, "runmax", 4032, "run cut threshold in encoded bytes")
	flag.IntVar(&cfg.ecap, "ecap", 128, "run entry cap")
	flag.IntVar(&cfg.nfields, "nfields", 4, "fields per entry")
	flag.IntVar(&cfg.elen, "elen", 200, "total value bytes per entry")
	flag.IntVar(&cfg.burst, "burst", 10, "entries per millisecond")
	flag.IntVar(&cfg.maxlen, "maxlen", 100000, "MAXLEN cap for feed and fanout")
	flag.IntVar(&cfg.nstreams, "nstreams", 100, "streams in the fanout mix")
	flag.IntVar(&cfg.ops, "ops", 500000, "ops in the measured mix")
	flag.Int64Var(&cfg.seed, "seed", 47, "rng seed")
	flag.Parse()
	if *quick {
		cfg.ops = 20000
		cfg.maxlen = 5000
	}
	switch cfg.mix {
	case "append":
		runAppend(cfg)
	case "feed":
		runFeed(cfg)
	case "fanout":
		if cfg.maxlen > 2000 && !*quick {
			cfg.maxlen = 2000
		}
		runFanout(cfg)
	case "encode":
		runEncode(cfg)
	default:
		fmt.Fprintf(os.Stderr, "unknown mix %q\n", cfg.mix)
		os.Exit(2)
	}
}
