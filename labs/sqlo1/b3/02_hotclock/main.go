// Lab: hot-tier promotion and sampling constants against real cold-read
// costs (spec 2064/sqlo1 doc 04 sections 4 and 8, milestone B3 lab 02).
//
// The S1 hotclock lab set the promotion probability D (0.125), the
// eviction sample size K (64), and the two-stamps-per-class header on
// hit ratios alone, against no store at all. Those verdicts assumed a
// miss costs whatever a miss costs; now that sqlo1b exists, a miss is
// three group preads through the real cold index, so this lab re-prices
// the same sweeps in nanoseconds: every cold miss performs an actual
// store Get against a preloaded, checkpointed sqlo1b file, and the
// verdict metric is amortized cold time per point read, which is what
// the runtime actually pays. The hit ratio rides along for continuity
// with the S1 tables; a constant that moves the ratio but not the
// nanoseconds does not get to move the default.
//
// The tier model is byte-for-byte the S1 simulator (dense slot array,
// key-to-slot map, WATT-lite two-stamp scoring, ghost ring at 1/16 of
// capacity, coarse 1024-op ticks), so ratio deltas against the S1 README
// are model-free. Scan-burst touches update tier state through the same
// read path but skip the store read: the page cache is fully warm either
// way and the burst's job is flushing the hot set, not spending IO.
// Writes stay tier-only, because the write path never reads the store.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// tickEvery models the coarse 1-second clock: timestamps advance once per
// this many ops, so entries touched inside one tick are indistinguishable,
// exactly the cheapness the real header buys by not calling time per op.
const tickEvery = 1024

// stamps is one class's access history: up to nMax coarse ticks, newest
// first. worth is the WATT access-rate estimate n/(now-oldest+1).
type stamps struct {
	t [3]uint32
	n uint8
}

func (s *stamps) touch(now uint32, nMax int) {
	if s.n > 0 && s.t[0] == now {
		return
	}
	copy(s.t[1:], s.t[:nMax-1])
	s.t[0] = now
	if int(s.n) < nMax {
		s.n++
	}
}

func (s *stamps) worth(now uint32) float64 {
	if s.n == 0 {
		return 0
	}
	oldest := s.t[s.n-1]
	return float64(s.n) / float64(now-oldest+1)
}

// entry is one hot slot: the key, the two timestamp classes, and the clock
// baseline's ref bit.
type entry struct {
	key   int32
	read  stamps
	write stamps
	ref   bool
}

// policy selects the replacement scoring.
type policy int

const (
	policyClock policy = iota // one ref bit, second-chance hand
	policyWatt2               // two stamps per class, writes 2x
	policyWatt3               // three stamps per class, writes 2x
)

func (p policy) String() string {
	switch p {
	case policyClock:
		return "clock"
	case policyWatt2:
		return "watt2"
	case policyWatt3:
		return "watt3"
	}
	return "?"
}

func (p policy) nStamps() int {
	if p == policyWatt3 {
		return 3
	}
	return 2
}

// tier is the lab hot tier: capacity slots in a dense array behind a
// key-to-slot map (uniform sampling is an index draw, removal a
// swap-delete), plus the ghost ring of evicted keys' timestamps.
type tier struct {
	pol       policy
	cap       int
	sampleK   int
	slots     []entry
	byKey     map[int32]int32
	hand      int
	ghost     map[int32]ghostEntry
	ghostCap  int
	ghostFifo []int32
	rng       *rand.Rand
	now       uint32
	ops       int
}

type ghostEntry struct {
	read  stamps
	write stamps
}

func newTier(pol policy, capacity, sampleK int, seed int64) *tier {
	return &tier{
		pol:      pol,
		cap:      capacity,
		sampleK:  sampleK,
		slots:    make([]entry, 0, capacity),
		byKey:    make(map[int32]int32, capacity),
		ghost:    make(map[int32]ghostEntry, capacity/16),
		ghostCap: capacity / 16,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (t *tier) tick() {
	t.ops++
	if t.ops%tickEvery == 0 {
		t.now++
	}
}

func (t *tier) score(e *entry) float64 {
	return e.read.worth(t.now) + 2*e.write.worth(t.now)
}

// evictOne frees exactly one slot.
func (t *tier) evictOne() {
	if t.pol == policyClock {
		for {
			if t.hand >= len(t.slots) {
				t.hand = 0
			}
			e := &t.slots[t.hand]
			if e.ref {
				e.ref = false
				t.hand++
				continue
			}
			t.remove(t.hand)
			return
		}
	}
	// WATT-lite: sample K uniformly, drop the lowest-worth 10 percent of
	// the sample (at least one). Victims are tracked by key, not slot
	// index, because each removal swap-deletes and would stale the
	// indices of the rest of the batch.
	k := min(t.sampleK, len(t.slots))
	type cand struct {
		key   int32
		score float64
	}
	cands := make([]cand, 0, k)
	for range k {
		e := &t.slots[t.rng.Intn(len(t.slots))]
		cands = append(cands, cand{e.key, t.score(e)})
	}
	drop := max(k/10, 1)
	for range drop {
		best := 0
		for i := 1; i < len(cands); i++ {
			if cands[i].score < cands[best].score {
				best = i
			}
		}
		victim := cands[best].key
		cands[best].score = 1e18 // spent
		if idx, ok := t.byKey[victim]; ok {
			// The batch may sample one key twice; the lookup skips the
			// already-removed duplicate. The extra drops beyond the one
			// slot the caller needs are headroom the real evictor also
			// banks.
			t.remove(int(idx))
		}
	}
}

func (t *tier) remove(idx int) {
	e := t.slots[idx]
	// Evicted keys keep their timestamps in the ghost ring so a re-read
	// shortly after eviction promotes with its history intact.
	if len(t.ghostFifo) >= t.ghostCap && t.ghostCap > 0 {
		old := t.ghostFifo[0]
		t.ghostFifo = t.ghostFifo[1:]
		delete(t.ghost, old)
	}
	if t.ghostCap > 0 {
		if _, ok := t.ghost[e.key]; !ok {
			t.ghostFifo = append(t.ghostFifo, e.key)
		}
		t.ghost[e.key] = ghostEntry{read: e.read, write: e.write}
	}
	delete(t.byKey, e.key)
	last := len(t.slots) - 1
	if idx != last {
		t.slots[idx] = t.slots[last]
		t.byKey[t.slots[idx].key] = int32(idx)
	}
	t.slots = t.slots[:last]
}

func (t *tier) insert(key int32, read, write stamps) {
	for len(t.slots) >= t.cap {
		t.evictOne()
	}
	t.slots = append(t.slots, entry{key: key, read: read, write: write, ref: true})
	t.byKey[key] = int32(len(t.slots) - 1)
}

// get is the read path: hit updates the read stamps, miss consults the
// ghost ring and then the promotion coin. Returns whether the read was a
// hot hit.
func (t *tier) get(key int32, promoteP float64) bool {
	t.tick()
	if idx, ok := t.byKey[key]; ok {
		e := &t.slots[idx]
		e.read.touch(t.now, t.pol.nStamps())
		e.ref = true
		return true
	}
	if g, ok := t.ghost[key]; ok {
		delete(t.ghost, key)
		r := g.read
		r.touch(t.now, t.pol.nStamps())
		t.insert(key, r, g.write)
		return false
	}
	if t.rng.Float64() < promoteP {
		var r stamps
		r.touch(t.now, t.pol.nStamps())
		t.insert(key, r, stamps{})
	}
	return false
}

// set is the write path: any state to dirty means a write always lands in
// the hot tier.
func (t *tier) set(key int32) {
	t.tick()
	if idx, ok := t.byKey[key]; ok {
		e := &t.slots[idx]
		e.write.touch(t.now, t.pol.nStamps())
		e.ref = true
		return
	}
	var w stamps
	w.touch(t.now, t.pol.nStamps())
	if g, ok := t.ghost[key]; ok {
		delete(t.ghost, key)
		gw := g.write
		gw.touch(t.now, t.pol.nStamps())
		t.insert(key, g.read, gw)
		return
	}
	t.insert(key, stamps{}, w)
}

// traceConfig is one workload shape.
type traceConfig struct {
	name      string
	keys      int
	ops       int
	warm      int
	writeFrac float64
	scanEvery int // point ops between scan bursts, 0 for none
	scanLen   int // keys touched per scan burst
}

// result is one run's verdict pair: the S1-comparable hit ratio and the
// metric that now decides, cold store time amortized over every measured
// point read.
type result struct {
	hitRatio float64
	coldNS   float64
}

// run replays the trace against a fresh tier over a live store: every
// point-read miss performs a real Get, timed, and the accumulated cold
// time divides over all measured point reads. The trace rng is separate
// from the tier rng and reseeded per run, so every policy and constant
// sees the identical op sequence.
func run(cfg traceConfig, pol policy, capacity, sampleK int, promoteP float64, db *sqlo1b.Store, keys [][]byte) (result, error) {
	ctx := context.Background()
	t := newTier(pol, capacity, sampleK, 7)
	tr := rand.New(rand.NewSource(11))
	zipf := rand.NewZipf(tr, 1.01, 16, uint64(cfg.keys-1))

	hits, reads := 0, 0
	var coldTime time.Duration
	scanCursor := 0
	pointOps := 0
	for i := 0; i < cfg.ops; i++ {
		if cfg.scanEvery > 0 && pointOps > 0 && pointOps%cfg.scanEvery == 0 {
			// A scan burst: sequential one-touch reads across the
			// keyspace, cursor persists across bursts. Scan touches go
			// through the same read path (and the same promotion coin)
			// but never count toward the metrics and skip the store.
			for range cfg.scanLen {
				t.get(int32(scanCursor%cfg.keys), promoteP)
				scanCursor++
			}
			pointOps++ // leave the burst trigger
			continue
		}
		key := int32(zipf.Uint64())
		if tr.Float64() < cfg.writeFrac {
			t.set(key)
		} else {
			hit := t.get(key, promoteP)
			if !hit {
				t0 := time.Now()
				if _, err := db.Get(ctx, keys[key]); err != nil {
					return result{}, fmt.Errorf("cold read key %d: %w", key, err)
				}
				if i >= cfg.warm {
					coldTime += time.Since(t0)
				}
			}
			if i >= cfg.warm {
				reads++
				if hit {
					hits++
				}
			}
		}
		pointOps++
	}
	if reads == 0 {
		return result{}, nil
	}
	return result{
		hitRatio: float64(hits) / float64(reads),
		coldNS:   float64(coldTime.Nanoseconds()) / float64(reads),
	}, nil
}

func traces(quick bool) []traceConfig {
	keys, ops, warm := 1_000_000, 6_000_000, 2_000_000
	scanEvery, scanLen := 400_000, 131_072
	if quick {
		keys, ops, warm = 200_000, 1_200_000, 400_000
		scanEvery, scanLen = 100_000, 32_768
	}
	// The read-only arms exist because the write path populates the tier
	// unconditionally (any state to dirty), so on a write-bearing mix the
	// promotion coin is not the only door in; a D verdict taken on one
	// mix alone would overfit it.
	return []traceConfig{
		{name: "zipfian", keys: keys, ops: ops, warm: warm, writeFrac: 0.10},
		{name: "zipfian-ro", keys: keys, ops: ops, warm: warm},
		{name: "scan-mix", keys: keys, ops: ops, warm: warm, writeFrac: 0.10,
			scanEvery: scanEvery, scanLen: scanLen},
		{name: "scan-mix-ro", keys: keys, ops: ops, warm: warm,
			scanEvery: scanEvery, scanLen: scanLen},
	}
}

func capacityFor(quick bool) int {
	if quick {
		return 16_384
	}
	return 65_536
}

// openStore preloads the whole keyspace into a fresh sqlo1b file and
// checkpoints it, so every miss in the sweeps is a genuine cold-index
// read: directory page, chunk group, record group.
func openStore(dir string, keys [][]byte, val []byte) (*sqlo1b.Store, error) {
	path := filepath.Join(dir, "hotclock.aki")
	os.Remove(path)
	os.Remove(sqlo1.WALPath(path))
	db, err := sqlo1b.CreateStore(path, 64<<20)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var seq int64
	ops := make([]sqlo1.Op, 0, 1024)
	for off := 0; off < len(keys); off += 1024 {
		n := min(1024, len(keys)-off)
		ops = ops[:0]
		for i := range n {
			ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: keys[off+i], Value: val}})
		}
		seq++
		if err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: seq, Ops: ops}); err != nil {
			db.Close()
			return nil, err
		}
	}
	if err := db.Checkpoint(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func makeKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "k:%013d", i)
	}
	return keys
}

func makeVal(n int) []byte {
	val := make([]byte, n)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	return val
}

func cell(r result) string {
	return fmt.Sprintf(" %.4f/%.0f |", r.hitRatio, r.coldNS)
}

func main() {
	quick := flag.Bool("quick", false, "shrink the sweep for the shared runner")
	dir := flag.String("dir", "", "working directory (default: a temp dir)")
	flag.Parse()

	if *dir == "" {
		d, err := os.MkdirTemp("", "hotclock")
		if err != nil {
			fmt.Fprintln(os.Stderr, "hotclock:", err)
			os.Exit(1)
		}
		defer os.RemoveAll(d)
		*dir = d
	}

	ts := traces(*quick)
	capacity := capacityFor(*quick)
	keys := makeKeys(ts[0].keys)
	db, err := openStore(*dir, keys, makeVal(128))
	if err != nil {
		fmt.Fprintln(os.Stderr, "hotclock:", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Printf("hotclock-b lab: capacity %d, ghost %d, tick every %d ops, %d keys preloaded and checkpointed\n",
		capacity, capacity/16, tickEvery, ts[0].keys)
	fmt.Println("cells are hit-ratio/amortized-cold-ns-per-point-read; the nanoseconds decide")
	fmt.Println()

	sweep := func(title, header string, cols int, cellFor func(tc traceConfig, col int) (result, error)) {
		fmt.Println(title)
		fmt.Println(header)
		fmt.Print("|---|")
		for range cols {
			fmt.Print("---|")
		}
		fmt.Println()
		for _, tc := range ts {
			fmt.Printf("| %s |", tc.name)
			for c := range cols {
				r, err := cellFor(tc, c)
				if err != nil {
					fmt.Fprintln(os.Stderr, "\nhotclock:", err)
					os.Exit(1)
				}
				fmt.Print(cell(r))
			}
			fmt.Println()
		}
	}

	ds := []float64{0, 0.125, 0.25, 0.5, 0.75, 1.0}
	sweep("sweep A: promotion probability D (watt2, K=64)",
		"| trace | D=0 | D=0.125 | D=0.25 | D=0.5 | D=0.75 | D=1.0 |", len(ds),
		func(tc traceConfig, c int) (result, error) {
			return run(tc, policyWatt2, capacity, 64, ds[c], db, keys)
		})

	ks := []int{16, 32, 64, 128, 256}
	fmt.Println()
	sweep("sweep B: sample size K (watt2, D=0.125)",
		"| trace | K=16 | K=32 | K=64 | K=128 | K=256 |", len(ks),
		func(tc traceConfig, c int) (result, error) {
			return run(tc, policyWatt2, capacity, ks[c], 0.125, db, keys)
		})

	pols := []policy{policyClock, policyWatt2, policyWatt3}
	fmt.Println()
	sweep("sweep C: policy (K=64, D=0.125)",
		"| trace | clock | watt2 | watt3 |", len(pols),
		func(tc traceConfig, c int) (result, error) {
			return run(tc, pols[c], capacity, 64, 0.125, db, keys)
		})
}
