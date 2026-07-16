// Lab: zset run and member segment sizes (spec 2064/sqlo1 doc 09
// sections 2 and 8, milestone T4 lab 01).
//
// T4 slices 2 and 3 bake two split thresholds: mem_max for the member
// segments (doc 06 machinery with the value fixed at the 8-byte
// sortable score) and run_max for the score runs. The trade is doc
// 06's W4 bandwidth knob doubled: a ZADD that moves a score bills one
// member-segment post-image plus one or two run post-images in its
// frame group, so bigger segments make every move carry more WAL
// bytes, while smaller ones lengthen both fences, split more often,
// and make a rank range touch more runs for the same elements.
// PRED-SQLO1-T4-WALZ is priced here too: WAL bytes and frames per
// score-moving ZADD under zipfian member reuse are the doc 14
// wal-delta tripwire, the same deferral question as doc 06 rule W4.
//
// The model is the doc 09 shape resident, no store underneath (the
// salgebra pattern; the drain-substrate half of the segment-size
// trade was already priced by T2's hseg lab on the real backends, and
// what T4 adds is the dual-family bill, which is arithmetic). The
// member side partitions by mh with entries sorted inside segments
// behind a fence binary search; the score side keeps (score, member)
// sorted runs with exact per-run counts; both split at the median
// entry when the encoded size crosses their threshold. The WAL
// column is modeled arithmetic under rules W2 and W4: every mutating
// command bills the full post-images its frame group carries, and a
// fence-structure change bills the whole root while the fences are
// inline or one fence page plus the root page index once they page.
// Drain traffic accumulates dirty post-images against the engine's 8
// MiB threshold for the WA column. An oracle test pins the model
// against a reference map through scores, ranks, walks, counts, and
// encoded sizes.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"slices"
	"sort"
	"time"
)

// Encoded sizes, doc 09 section 2. Member entries reuse the doc 06
// codec with the value fixed at the 8-byte sortable score; run entries
// are u16 mlen, sortable u64 score, member bytes.
const (
	memEntHdr      = 3
	scoreBytes     = 8
	segHdrBytes    = 12
	rootHdrBytes   = 36
	memFenceEnt    = 16
	runFenceEnt    = 20
	runEntHdr      = 10
	fencePageBytes = 4096
)

func memEntSize(mlen int) int { return memEntHdr + mlen + scoreBytes }
func runEntSize(mlen int) int { return runEntHdr + mlen }

// sortableScore mirrors engine/sqlo1's zScoreSortable (#950): flip all
// bits when the sign is set, flip the sign bit when not, -0 folded to
// +0, so u64 order is Redis's double comparison.
func sortableScore(s float64) uint64 {
	if s == 0 {
		s = 0
	}
	b := math.Float64bits(s)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | 1<<63
}

func scoreFromSortable(u uint64) float64 {
	if u&(1<<63) != 0 {
		return math.Float64frombits(u &^ (1 << 63))
	}
	return math.Float64frombits(^u)
}

// fh is the member-space partitioning hash, the hseg lab's: FNV-1a
// folded through a splitmix64 finalizer.
func fh(member []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, b := range member {
		h = (h ^ uint64(b)) * 1099511628211
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	return h ^ h>>31
}

type memEnt struct {
	mh    uint64
	mi    int32
	score uint64
}

type memSeg struct {
	id    uint64
	lo    uint64
	ents  []memEnt
	size  int // encoded, header included
	dirty bool
}

type fenceEnt struct {
	lo    uint64
	segid uint64
}

type zent struct {
	score uint64
	mi    int32
}

// run is one score-side segment: entries sorted by (score, member),
// the fence lo bound it was cut at, and the exact count the fence
// entry carries (len(ents) here, the model keeps it implicit).
type run struct {
	loScore uint64
	loMi    int32 // -1 on the first run: the -inf sentinel
	ents    []zent
	size    int // encoded, header included
	dirty   bool
}

type zset struct {
	names     [][]byte
	fence     []fenceEnt
	segs      map[uint64]*memSeg
	nextSegid uint64
	runs      []*run
	rootDirty bool
}

func newZset(members int) *zset {
	s := &memSeg{id: 0, lo: 0, size: segHdrBytes}
	r := &run{loMi: -1, size: segHdrBytes}
	return &zset{
		names:     make([][]byte, 0, members),
		fence:     []fenceEnt{{lo: 0, segid: 0}},
		segs:      map[uint64]*memSeg{0: s},
		nextSegid: 1,
		runs:      []*run{r},
		rootDirty: true,
	}
}

func (z *zset) card() int {
	n := 0
	for _, r := range z.runs {
		n += len(r.ents)
	}
	return n
}

// fenceBytes is what the two fences cost in the root, the paging
// input.
func (z *zset) fenceBytes() int {
	return len(z.fence)*memFenceEnt + len(z.runs)*runFenceEnt
}

func (z *zset) rootFull() int { return rootHdrBytes + z.fenceBytes() }

// structuralBill is the WAL bytes a fence-structure change costs one
// command: the whole root post-image while the fences fit inline, one
// fence page plus the root page index once they page (doc 06 fence
// paging, kinds 3 and 4 here).
func (z *zset) structuralBill() int {
	full := z.rootFull()
	if full <= fencePageBytes {
		return full
	}
	pages := (z.fenceBytes() + fencePageBytes - 1) / fencePageBytes
	return fencePageBytes + rootHdrBytes + pages*16
}

// drainRootBytes is what the coalesced per-drain root write costs:
// the full root inline, the page index once paged (dirty fence pages
// ride the same drain but coalesce across the whole window, so the
// per-drain column keeps just the root).
func (z *zset) drainRootBytes() int {
	full := z.rootFull()
	if full <= fencePageBytes {
		return full
	}
	pages := (z.fenceBytes() + fencePageBytes - 1) / fencePageBytes
	return rootHdrBytes + pages*16
}

// memSegFor returns the member segment covering h per the fence.
func (z *zset) memSegFor(h uint64) *memSeg {
	i := sort.Search(len(z.fence), func(i int) bool { return z.fence[i].lo > h })
	return z.segs[z.fence[i-1].segid]
}

// memFind locates mi inside s: binary search on mh, equality scan
// across a collision run.
func (s *memSeg) memFind(h uint64, mi int32) (int, bool) {
	i := sort.Search(len(s.ents), func(i int) bool { return s.ents[i].mh >= h })
	for ; i < len(s.ents) && s.ents[i].mh == h; i++ {
		if s.ents[i].mi == mi {
			return i, true
		}
	}
	return i, false
}

// loAbove reports whether r's lo bound sits above (score, mi); the
// first run's sentinel is below everything.
func (z *zset) loAbove(r *run, score uint64, mi int32) bool {
	if r.loMi < 0 {
		return false
	}
	if r.loScore != score {
		return r.loScore > score
	}
	return bytes.Compare(z.names[r.loMi], z.names[mi]) > 0
}

// runIdxFor returns the index of the run covering (score, mi).
func (z *zset) runIdxFor(score uint64, mi int32) int {
	i := sort.Search(len(z.runs), func(i int) bool { return z.loAbove(z.runs[i], score, mi) })
	return i - 1
}

// runPos returns the insertion position of (score, mi) inside r.
func (z *zset) runPos(r *run, score uint64, mi int32) int {
	return sort.Search(len(r.ents), func(i int) bool {
		e := r.ents[i]
		if e.score != score {
			return e.score > score
		}
		return bytes.Compare(z.names[e.mi], z.names[mi]) >= 0
	})
}

// bill collects one command's frame group: touched post-images and
// whether the fence structure changed.
type bill struct {
	segs []*memSeg
	runs []*run
	root bool
}

func (b *bill) addSeg(s *memSeg) {
	if !slices.Contains(b.segs, s) {
		b.segs = append(b.segs, s)
	}
}

func (b *bill) addRun(r *run) {
	if !slices.Contains(b.runs, r) {
		b.runs = append(b.runs, r)
	}
}

type config struct {
	mix       string
	memMax    int
	runMax    int
	keys      int
	members   int
	ops       int
	mlen      int
	threshold int
	seed      int64
}

type model struct {
	cfg config
	zs  []*zset

	dirtyBytes int
	flushed    int64
	drainRows  int64
	drains     int
	logical    int64

	memSplits  int
	runSplits  int
	runDeaths  int
	structural int
}

// touchSeg accounts a mutated member segment into the dirty pool:
// full size on first dirtying (delta already applied), delta after.
func (m *model) touchSeg(s *memSeg, delta int) {
	if !s.dirty {
		s.dirty = true
		m.dirtyBytes += s.size
	} else {
		m.dirtyBytes += delta
	}
}

func (m *model) touchRun(r *run, delta int) {
	if !r.dirty {
		r.dirty = true
		m.dirtyBytes += r.size
	} else {
		m.dirtyBytes += delta
	}
}

// memInsert adds a new member with its score to the member side.
func (m *model) memInsert(z *zset, mi int32, h, score uint64, b *bill) {
	s := z.memSegFor(h)
	i, found := s.memFind(h, mi)
	if found {
		panic("memInsert on a present member")
	}
	s.ents = slices.Insert(s.ents, i, memEnt{mh: h, mi: mi, score: score})
	sz := memEntSize(len(z.names[mi]))
	s.size += sz
	m.touchSeg(s, sz)
	b.addSeg(s)
	if s.size > m.cfg.memMax {
		if ns := m.memSplit(z, s); ns != nil {
			b.addSeg(ns)
			b.root = true
		}
	}
}

// memSplit cuts s at its entry-median mh, refusing (never in practice
// with 64-bit mh) rather than corrupt the fence on a collision run.
func (m *model) memSplit(z *zset, s *memSeg) *memSeg {
	mid := len(s.ents) / 2
	newLo := s.ents[mid].mh
	for mid > 0 && s.ents[mid-1].mh == newLo {
		mid--
	}
	if mid == 0 || newLo <= s.lo {
		return nil
	}
	ns := &memSeg{id: z.nextSegid, lo: newLo, dirty: true}
	z.nextSegid++
	ns.ents = append(ns.ents, s.ents[mid:]...)
	s.ents = s.ents[:mid]
	moved := 0
	for i := range ns.ents {
		moved += memEntSize(len(z.names[ns.ents[i].mi]))
	}
	ns.size = segHdrBytes + moved
	s.size -= moved
	z.segs[ns.id] = ns
	i := sort.Search(len(z.fence), func(i int) bool { return z.fence[i].lo > newLo })
	z.fence = slices.Insert(z.fence, i, fenceEnt{lo: newLo, segid: ns.id})
	z.rootDirty = true
	m.dirtyBytes += segHdrBytes
	m.memSplits++
	return ns
}

// memSetScore updates a present member's stored score and returns the
// old one.
func (m *model) memSetScore(z *zset, mi int32, h, score uint64, b *bill) uint64 {
	s := z.memSegFor(h)
	i, found := s.memFind(h, mi)
	if !found {
		panic("memSetScore on an absent member")
	}
	old := s.ents[i].score
	s.ents[i].score = score
	m.touchSeg(s, 0)
	b.addSeg(s)
	return old
}

// runInsert adds (score, mi) to its covering run, splitting at the
// median when the encoded size crosses run_max.
func (m *model) runInsert(z *zset, score uint64, mi int32, b *bill) {
	ri := z.runIdxFor(score, mi)
	r := z.runs[ri]
	pos := z.runPos(r, score, mi)
	r.ents = slices.Insert(r.ents, pos, zent{score: score, mi: mi})
	sz := runEntSize(len(z.names[mi]))
	r.size += sz
	m.touchRun(r, sz)
	b.addRun(r)
	if r.size > m.cfg.runMax && len(r.ents) >= 2 {
		mid := len(r.ents) / 2
		nr := &run{loScore: r.ents[mid].score, loMi: r.ents[mid].mi, dirty: true}
		nr.ents = append(nr.ents, r.ents[mid:]...)
		r.ents = r.ents[:mid]
		moved := 0
		for i := range nr.ents {
			moved += runEntSize(len(z.names[nr.ents[i].mi]))
		}
		nr.size = segHdrBytes + moved
		r.size -= moved
		z.runs = slices.Insert(z.runs, ri+1, nr)
		z.rootDirty = true
		m.dirtyBytes += segHdrBytes
		m.runSplits++
		b.addRun(nr)
		b.root = true
	}
}

// runRemove drops (score, mi) from its covering run; an emptied run
// dies whole (its fence entry goes with it), except the sentinel.
func (m *model) runRemove(z *zset, score uint64, mi int32, b *bill) {
	ri := z.runIdxFor(score, mi)
	r := z.runs[ri]
	pos := z.runPos(r, score, mi)
	if pos >= len(r.ents) || r.ents[pos] != (zent{score: score, mi: mi}) {
		panic("runRemove on an absent entry")
	}
	r.ents = slices.Delete(r.ents, pos, pos+1)
	sz := runEntSize(len(z.names[mi]))
	r.size -= sz
	m.touchRun(r, -sz)
	b.addRun(r)
	if len(r.ents) == 0 && ri > 0 {
		z.runs = slices.Delete(z.runs, ri, ri+1)
		z.rootDirty = true
		m.runDeaths++
		b.root = true
	}
}

// zaddNew inserts a fresh member on both sides, one frame group.
func (m *model) zaddNew(z *zset, mi int32, score uint64, b *bill) {
	m.memInsert(z, mi, fh(z.names[mi]), score, b)
	m.runInsert(z, score, mi, b)
	z.rootDirty = true // W1: cardinality change pins the root
	m.logical += int64(len(z.names[mi]) + scoreBytes)
}

// zaddMove re-scores a present member: member entry update, remove
// from the old run, insert into the new, all one frame group. A
// same-score ZADD is a no-op and bills nothing.
func (m *model) zaddMove(z *zset, mi int32, score uint64, b *bill) bool {
	h := fh(z.names[mi])
	s := z.memSegFor(h)
	i, found := s.memFind(h, mi)
	if !found {
		panic("zaddMove on an absent member")
	}
	if s.ents[i].score == score {
		return false
	}
	old := m.memSetScore(z, mi, h, score, b)
	m.runRemove(z, old, mi, b)
	m.runInsert(z, score, mi, b)
	m.logical += int64(len(z.names[mi]) + scoreBytes)
	return true
}

func (z *zset) zscore(mi int32) (uint64, bool) {
	h := fh(z.names[mi])
	s := z.memSegFor(h)
	if i, found := s.memFind(h, mi); found {
		return s.ents[i].score, true
	}
	return 0, false
}

// zrank is the doc 09 rank path: member side for the score, prefix
// sum over the run counts to the covering run, scan inside it.
func (z *zset) zrank(mi int32) int {
	score, ok := z.zscore(mi)
	if !ok {
		return -1
	}
	ri := z.runIdxFor(score, mi)
	rank := 0
	for i := 0; i < ri; i++ {
		rank += len(z.runs[i].ents)
	}
	r := z.runs[ri]
	pos := z.runPos(r, score, mi)
	return rank + pos
}

// rangeWalk streams n elements from rank start: prefix sum to the
// starting run, then sequential runs. It returns the runs touched and
// the encoded bytes a cold read of them would pull, the lab's ZRANGE
// bill.
func (z *zset) rangeWalk(start, n int) (runsTouched, elems, bytesRead int) {
	i := 0
	for ; i < len(z.runs); i++ {
		c := len(z.runs[i].ents)
		if start < c {
			break
		}
		start -= c
	}
	for ; i < len(z.runs) && elems < n; i++ {
		r := z.runs[i]
		runsTouched++
		bytesRead += r.size
		take := min(len(r.ents)-start, n-elems)
		elems += take
		start = 0
	}
	return
}

// flush drains every dirty post-image and the coalesced root writes,
// the W4 side of the ledger.
func (m *model) flush() {
	for _, z := range m.zs {
		if z == nil {
			continue
		}
		for _, s := range z.segs {
			if s.dirty {
				m.flushed += int64(s.size)
				m.drainRows++
				s.dirty = false
			}
		}
		for _, r := range z.runs {
			if r.dirty {
				m.flushed += int64(r.size)
				m.drainRows++
				r.dirty = false
			}
		}
		if z.rootDirty {
			m.flushed += int64(z.drainRootBytes())
			m.drainRows++
			z.rootDirty = false
		}
	}
	m.dirtyBytes = 0
	m.drains++
}

// billWAL prices one committed frame group: post-image bytes for every
// touched segment and run, plus the structural bill when the fence
// changed shape, and returns (frames, bytes).
func (m *model) billWAL(z *zset, b *bill) (int, int) {
	frames, bts := 0, 0
	for _, s := range b.segs {
		frames++
		bts += s.size
	}
	for _, r := range b.runs {
		frames++
		bts += r.size
	}
	if b.root {
		frames++
		bts += z.structuralBill()
		m.structural++
	}
	return frames, bts
}

type mixDef struct {
	movePct  int  // ZADD, fresh uniform score
	incrPct  int  // ZINCRBY-shaped small delta
	scorePct int  // ZSCORE
	rankPct  int  // ZRANK
	topRange bool // ZRANGE from rank 0 (leaderboard head) vs random offset
}

func mixFor(name string) (mixDef, bool) {
	switch name {
	case "zaddheavy":
		return mixDef{movePct: 70, incrPct: 0, scorePct: 20, rankPct: 0}, true
	case "zrangeheavy":
		return mixDef{movePct: 10, incrPct: 0, scorePct: 10, rankPct: 0}, true
	case "board":
		return mixDef{movePct: 10, incrPct: 40, scorePct: 10, rankPct: 10, topRange: true}, true
	}
	return mixDef{}, false
}

const rangeN = 100

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.mix, "mix", "board", "op mix: zaddheavy, zrangeheavy, board")
	flag.IntVar(&cfg.memMax, "memmax", 4032, "member segment split threshold in bytes")
	flag.IntVar(&cfg.runMax, "runmax", 4032, "score run split threshold in bytes")
	flag.IntVar(&cfg.keys, "keys", 4, "zset key count")
	flag.IntVar(&cfg.members, "members", 100000, "members per zset")
	flag.IntVar(&cfg.ops, "ops", 200000, "ops in the measured mix")
	flag.IntVar(&cfg.mlen, "mlen", 16, "member length in bytes")
	flag.IntVar(&cfg.threshold, "threshold", 8<<20, "dirty bytes per drain")
	flag.Int64Var(&cfg.seed, "seed", 47, "rng seed")
	flag.Parse()
	if *quick {
		cfg.keys, cfg.members, cfg.ops, cfg.threshold = 2, 5000, 10000, 1<<20
	}
	if _, ok := mixFor(cfg.mix); !ok {
		fmt.Fprintln(os.Stderr, "hsegz: mix must be zaddheavy, zrangeheavy, or board")
		os.Exit(1)
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "hsegz:", err)
		os.Exit(1)
	}
}

func newModel(cfg config) *model {
	return &model{cfg: cfg, zs: make([]*zset, cfg.keys)}
}

// memberName builds the deterministic member: key and index prefix,
// letter filler to mlen.
func memberName(rng *rand.Rand, ki, mi, mlen int) []byte {
	name := fmt.Appendf(nil, "k%d:m%06d:", ki, mi)
	for len(name) < mlen {
		name = append(name, byte('a'+rng.Intn(26)))
	}
	return name[:mlen]
}

type row struct {
	workload   string
	ops        int
	dur        time.Duration
	p50, p99   time.Duration
	frames     float64
	walB       float64
	runsOp     float64
	readB      float64
	x1, x2, x3 float64
}

func runAll(cfg config, out io.Writer) error {
	mix, _ := mixFor(cfg.mix)
	m := newModel(cfg)
	rng := rand.New(rand.NewSource(cfg.seed))

	// Preload every member through the two-sided insert path so splits
	// happen the way slices 2 and 3 will make them; the measured mix
	// then moves scores over the loaded universe, the steady state the
	// sweep prices.
	start := time.Now()
	for ki := range m.zs {
		z := newZset(cfg.members)
		m.zs[ki] = z
		for mi := range cfg.members {
			z.names = append(z.names, memberName(rng, ki, mi, cfg.mlen))
			var b bill
			m.zaddNew(z, int32(mi), sortableScore(rng.Float64()*1e6), &b)
			if m.dirtyBytes >= cfg.threshold {
				m.flush()
			}
		}
	}
	m.flush()
	emit(cfg, out, row{workload: "load", ops: cfg.keys * cfg.members, dur: time.Since(start)})
	m.logical, m.flushed, m.drainRows, m.drains = 0, 0, 0, 0
	m.memSplits, m.runSplits, m.runDeaths, m.structural = 0, 0, 0, 0

	zipf := rand.NewZipf(rng, 1.1, 1, uint64(cfg.members-1))
	var moveLat, scoreLat, rangeLat, rankLat []time.Duration
	var moveDur, scoreDur, rangeDur, rankDur time.Duration
	var walFrames, walBytes int64
	moves, sameRun, noops := 0, 0, 0
	scores, ranges, ranks := 0, 0, 0
	var runsTouched, rangeBytes int64

	for range cfg.ops {
		ki := rng.Intn(cfg.keys)
		z := m.zs[ki]
		p := rng.Intn(100)
		switch {
		case p < mix.movePct+mix.incrPct:
			mi := int32(zipf.Uint64())
			var score uint64
			if p < mix.movePct {
				score = sortableScore(rng.Float64() * 1e6)
			} else {
				// The leaderboard shape: a bounded increment, so the
				// new score usually lands near the old run.
				old, _ := z.zscore(mi)
				next := scoreFromSortable(old) + (rng.Float64()*1000 - 500)
				score = sortableScore(next)
			}
			var b bill
			t0 := time.Now()
			moved := m.zaddMove(z, mi, score, &b)
			lat := time.Since(t0)
			moveLat = append(moveLat, lat)
			moveDur += lat
			moves++
			if !moved {
				noops++
				continue
			}
			f, bt := m.billWAL(z, &b)
			walFrames += int64(f)
			walBytes += int64(bt)
			if len(b.runs) == 1 {
				sameRun++
			}
		case p < mix.movePct+mix.incrPct+mix.scorePct:
			mi := int32(zipf.Uint64())
			t0 := time.Now()
			if _, ok := z.zscore(mi); !ok {
				return fmt.Errorf("zscore missed a preloaded member %d", mi)
			}
			lat := time.Since(t0)
			scoreLat = append(scoreLat, lat)
			scoreDur += lat
			scores++
		case p < mix.movePct+mix.incrPct+mix.scorePct+mix.rankPct:
			mi := int32(zipf.Uint64())
			t0 := time.Now()
			if z.zrank(mi) < 0 {
				return fmt.Errorf("zrank missed a preloaded member %d", mi)
			}
			lat := time.Since(t0)
			rankLat = append(rankLat, lat)
			rankDur += lat
			ranks++
		default:
			startRank := 0
			if !mix.topRange {
				startRank = rng.Intn(max(cfg.members-rangeN, 1))
			}
			t0 := time.Now()
			rt, elems, bts := z.rangeWalk(startRank, rangeN)
			lat := time.Since(t0)
			if elems != rangeN {
				return fmt.Errorf("rangeWalk returned %d of %d elements", elems, rangeN)
			}
			rangeLat = append(rangeLat, lat)
			rangeDur += lat
			ranges++
			runsTouched += int64(rt)
			rangeBytes += int64(bts)
		}
		if m.dirtyBytes >= cfg.threshold {
			m.flush()
		}
	}
	m.flush()

	billed := moves - noops
	p50, p99, _ := percentiles(moveLat)
	emit(cfg, out, row{workload: "zadd", ops: moves, dur: moveDur, p50: p50, p99: p99,
		frames: float64(walFrames) / float64(max(billed, 1)),
		walB:   float64(walBytes) / float64(max(billed, 1)),
		x1:     float64(sameRun) / float64(max(billed, 1)),
		x2:     float64(m.runSplits+m.memSplits) * 1000 / float64(max(billed, 1)),
		x3:     float64(m.structural) * 1000 / float64(max(billed, 1))})
	p50, p99, _ = percentiles(scoreLat)
	emit(cfg, out, row{workload: "zscore", ops: scores, dur: scoreDur, p50: p50, p99: p99})
	if ranks > 0 {
		p50, p99, _ = percentiles(rankLat)
		emit(cfg, out, row{workload: "zrank", ops: ranks, dur: rankDur, p50: p50, p99: p99})
	}
	p50, p99, _ = percentiles(rangeLat)
	emit(cfg, out, row{workload: "zrange", ops: ranges, dur: rangeDur, p50: p50, p99: p99,
		runsOp: float64(runsTouched) / float64(max(ranges, 1)),
		readB:  float64(rangeBytes) / float64(max(ranges, 1))})

	segsTotal, runsTotal, card, fenceB := 0, 0, 0, 0
	for _, z := range m.zs {
		segsTotal += len(z.segs)
		runsTotal += len(z.runs)
		card += z.card()
		fenceB += z.fenceBytes()
	}
	wa := 0.0
	if m.logical > 0 {
		wa = float64(m.flushed) / float64(m.logical)
	}
	emit(cfg, out, row{workload: "shape", ops: card,
		runsOp: float64(card) / float64(max(runsTotal, 1)),
		readB:  float64(card) / float64(max(segsTotal, 1)),
		x1:     float64(segsTotal), x2: float64(runsTotal), x3: float64(fenceB)})
	emit(cfg, out, row{workload: "drain", ops: m.drains,
		frames: float64(m.drainRows) / float64(max(m.drains, 1)),
		walB:   wa})
	return nil
}

func percentiles(all []time.Duration) (p50, p99, max time.Duration) {
	if len(all) == 0 {
		return 0, 0, 0
	}
	slices.Sort(all)
	return all[len(all)/2], all[len(all)*99/100], all[len(all)-1]
}

func emit(cfg config, out io.Writer, r row) {
	nsPerOp := float64(r.dur.Nanoseconds()) / float64(max(r.ops, 1))
	fmt.Fprintf(out, "%s,%d,%d,%s,%d,%.0f,%d,%d,%.2f,%.0f,%.2f,%.0f,%.3f,%.2f,%.2f\n",
		cfg.mix, cfg.memMax, cfg.runMax,
		r.workload, r.ops, nsPerOp,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(),
		r.frames, r.walB, r.runsOp, r.readB, r.x1, r.x2, r.x3)
}
