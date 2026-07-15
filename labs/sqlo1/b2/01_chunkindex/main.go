// Command chunkindex prices the doc 03 section 8 cold index before
// the B2 slices bake its constants in. Three questions. First,
// PRED-SQLO1-B2-INDEXRAM: what does the resident directory really
// cost per cold key in Go heap bytes, at chunk counts implied by 10^6
// to 10^9 keys? The ledger says 16 B per 42-key chunk, about 0.38 B
// per key at full chunks, but steady-state occupancy under
// overflow-driven linear hashing is the divisor that decides whether
// the measured number stays under the 1 B target or drifts toward
// the 2 B kill line. Second, the occupancy itself: the emergent fill
// distribution, the overflow chain rate (every link is an extra
// group read on probe, chains past 2 links must be rare), and the
// split cadence (a storm of splits in a narrow insert window is a
// latency cliff the doc's local-split argument does not price).
// Third, the fingerprint false-hit rate against the predicted 0.06%
// order: 16 fingerprint bits against a 42-entry scan.
//
// The simulator runs the doc 8.5 protocol at count level: an insert
// that would push a bucket past capacity chains it and advances one
// split at the split pointer, which is linear hashing's contract.
// Redistribution in counts mode moves each entry with a fair coin,
// which is exactly the distribution of the real bit-L partition for
// uniform hashes, and the exact mode (stored hashes, real xxhash
// placement) exists to check that claim against itself; the oracle
// test holds the two within tolerance. The lf policies split ahead
// of overflow at a target load factor, the arm the verdict compares
// the doc's overflow-driven default against.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"time"

	"github.com/cespare/xxhash/v2"
)

// Chunk geometry under test (doc 03 section 8.2): 8 header bytes plus
// 42 entries of 12 bytes is exactly 512, four chunks to a group. A
// chained chunk gives its last entry slot to the overflow pointer.
const (
	chunkBytes  = 512
	chunkHdr    = 8
	entryBytes  = 12
	chunkCap    = (chunkBytes - chunkHdr) / entryBytes // 42
	chainCap    = chunkCap - 1
	dirEntryLen = 16
	pageEntries = 256
	windowBits  = 16 // split-storm window: 65536 inserts
)

// links reports how many overflow chunks a bucket of c entries
// chains: the base holds 41 once chained, intermediate links 41, the
// last link 42.
func links(c uint32) int {
	if c <= chunkCap {
		return 0
	}
	return int((c - chunkCap + chainCap - 1) / chainCap)
}

type table struct {
	level  uint
	split  uint64
	counts []uint32
	hashes [][]uint64 // exact mode only: full key hash per entry
	rng    *rand.Rand
	exact  bool

	inserted     uint64
	splits       uint64
	windowSplits uint64
	maxWindow    uint64
}

func newTable(seed uint64, exact bool) *table {
	t := &table{
		level:  0,
		counts: make([]uint32, 1),
		rng:    rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15)),
		exact:  exact,
	}
	if exact {
		t.hashes = make([][]uint64, 1)
	}
	return t
}

func (t *table) buckets() uint64 { return uint64(len(t.counts)) }

func (t *table) bucket(h uint64) uint64 {
	b := h & (1<<t.level - 1)
	if b < t.split {
		b = h & (1<<(t.level+1) - 1)
	}
	return b
}

// insert adds one key by hash and returns true when the insert
// overflowed its bucket into a new chain link.
func (t *table) insert(h uint64) bool {
	b := t.bucket(h)
	t.counts[b]++
	if t.exact {
		t.hashes[b] = append(t.hashes[b], h)
	}
	t.inserted++
	if links(t.counts[b]) > links(t.counts[b]-1) {
		t.splitStep()
		return true
	}
	return false
}

// splitStep splits the bucket at the split pointer, never the
// overflowing one; that is linear hashing's contract and the reason
// splits stay local.
func (t *table) splitStep() {
	s := t.split
	c := t.counts[s]
	var moved uint32
	if t.exact {
		bit := uint64(1) << t.level
		var stay, move []uint64
		for _, h := range t.hashes[s] {
			if h&bit != 0 {
				move = append(move, h)
			} else {
				stay = append(stay, h)
			}
		}
		t.hashes[s] = stay
		t.hashes = append(t.hashes, move)
		moved = uint32(len(move))
	} else {
		// Fair-coin redistribution: identical in distribution to the
		// real bit-L partition for uniform hashes.
		for rem := c; rem > 0; {
			k := min(rem, 64)
			moved += uint32(bits.OnesCount64(t.rng.Uint64() & (1<<k - 1)))
			rem -= k
		}
		t.counts = append(t.counts, 0)
	}
	if t.exact {
		t.counts = append(t.counts, 0)
	}
	t.counts[s] = c - moved
	t.counts[len(t.counts)-1] = moved
	t.split++
	if t.split == 1<<t.level {
		t.level++
		t.split = 0
	}
	t.splits++
	t.windowSplits++
}

// loadFactor is live entries over base-chunk capacity.
func (t *table) loadFactor() float64 {
	return float64(t.inserted) / (chunkCap * float64(t.buckets()))
}

type runResult struct {
	n           uint64
	buckets     uint64
	chainLinks  uint64
	fillMean    float64
	fillP50     uint32
	fillP95     uint32
	fillMax     uint32
	chainedPct  float64
	chain2Pct   float64
	splits      uint64
	maxWindow   uint64
	dirBPerKey  float64
	diskBPerKey float64
	heapBPerKey float64
	elapsed     time.Duration
}

// runSim builds the index at n keys under a policy: "doc" splits only
// on overflow, "lfNN" also splits whenever the load factor exceeds
// 0.NN.
func runSim(n uint64, policy string, seed uint64, exact bool) runResult {
	var tau float64
	switch policy {
	case "doc":
	case "lf75":
		tau = 0.75
	case "lf85":
		tau = 0.85
	default:
		fmt.Fprintf(os.Stderr, "unknown policy %q\n", policy)
		os.Exit(2)
	}
	start := time.Now()
	t := newTable(seed, exact)
	var key [8]byte
	for i := range n {
		var h uint64
		if exact {
			binary.LittleEndian.PutUint64(key[:], i)
			h = xxhash.Sum64(key[:])
		} else {
			h = t.rng.Uint64()
		}
		t.insert(h)
		if tau != 0 {
			for t.loadFactor() > tau {
				t.splitStep()
			}
		}
		if i&(1<<windowBits-1) == 1<<windowBits-1 {
			t.maxWindow = max(t.maxWindow, t.windowSplits)
			t.windowSplits = 0
		}
	}
	t.maxWindow = max(t.maxWindow, t.windowSplits)

	r := runResult{n: n, buckets: t.buckets(), splits: t.splits, maxWindow: t.maxWindow}
	var chained, chain2, totalLinks uint64
	for _, c := range t.counts {
		l := links(c)
		totalLinks += uint64(l)
		if l >= 1 {
			chained++
		}
		if l >= 2 {
			chain2++
		}
	}
	r.chainLinks = totalLinks
	r.fillMean = float64(n) / (chunkCap * float64(r.buckets))
	sorted := slices.Clone(t.counts)
	slices.Sort(sorted)
	r.fillP50 = sorted[len(sorted)/2]
	r.fillP95 = sorted[len(sorted)*95/100]
	r.fillMax = sorted[len(sorted)-1]
	r.chainedPct = 100 * float64(chained) / float64(r.buckets)
	r.chain2Pct = 100 * float64(chain2) / float64(r.buckets)
	r.dirBPerKey = dirEntryLen * float64(r.buckets) / float64(n)
	r.diskBPerKey = chunkBytes * float64(r.buckets+totalLinks) / float64(n)
	r.heapBPerKey = float64(measureDirHeap(r.buckets)) / float64(n)
	r.elapsed = time.Since(start)
	return r
}

// The resident directory as doc 04 keeps it: 4 KiB pages of 256 full
// pointers plus a radix root of one full pointer per page. The heap
// delta over building it is the honest Go price of the ledger's 16
// bytes per chunk.
type fullPtr struct{ pos, sum uint64 }
type dirPage [pageEntries]fullPtr

func measureDirHeap(chunks uint64) uint64 {
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	pages := make([]*dirPage, (chunks+pageEntries-1)/pageEntries)
	for i := range pages {
		pages[i] = new(dirPage)
		pages[i][i%pageEntries] = fullPtr{uint64(i), ^uint64(i)}
	}
	root := make([]fullPtr, len(pages))
	for i := range root {
		root[i] = fullPtr{uint64(i), uint64(i)}
	}
	runtime.GC()
	runtime.ReadMemStats(&m1)
	var delta uint64
	if m1.HeapAlloc > m0.HeapAlloc {
		delta = m1.HeapAlloc - m0.HeapAlloc
	}
	runtime.KeepAlive(pages)
	runtime.KeepAlive(root)
	return delta
}

// runFalseHit builds the exact-mode index and probes it: every
// fingerprint match that is not the probed key itself is a false hit
// the read path resolves with a record read and a key compare. The
// prediction is meanBucketEntries / 2^16 per probe.
func runFalseHit(n, probes, seed uint64) {
	t := newTable(seed, true)
	var key [8]byte
	for i := range n {
		binary.LittleEndian.PutUint64(key[:], i)
		t.insert(xxhash.Sum64(key[:]))
	}
	var presentFalse, absentFalse uint64
	rng := rand.New(rand.NewPCG(seed^0xABCD, seed))
	for range probes {
		i := rng.Uint64N(n)
		binary.LittleEndian.PutUint64(key[:], i)
		h := xxhash.Sum64(key[:])
		fp := uint16(h >> 48)
		matches := 0
		for _, eh := range t.hashes[t.bucket(h)] {
			if uint16(eh>>48) == fp {
				matches++
			}
		}
		presentFalse += uint64(matches - 1) // one match is the key itself

		binary.LittleEndian.PutUint64(key[:], n+rng.Uint64N(1<<40))
		h = xxhash.Sum64(key[:])
		fp = uint16(h >> 48)
		for _, eh := range t.hashes[t.bucket(h)] {
			if uint16(eh>>48) == fp {
				absentFalse++
			}
		}
	}
	mean := float64(t.inserted) / float64(t.buckets())
	predicted := 100 * mean / 65536
	fmt.Printf("arm,n,probes,present_false,absent_false,present_pct,absent_pct,predicted_pct\n")
	fmt.Printf("falsehit,%d,%d,%d,%d,%.4f,%.4f,%.4f\n",
		n, probes, presentFalse, absentFalse,
		100*float64(presentFalse)/float64(probes),
		100*float64(absentFalse)/float64(probes), predicted)
	fmt.Fprintf(os.Stderr, "falsehit: present %.4f%% absent %.4f%% predicted %.4f%% (mean bucket %.1f)\n",
		100*float64(presentFalse)/float64(probes),
		100*float64(absentFalse)/float64(probes), predicted, mean)
}

func main() {
	arm := flag.String("arm", "occupancy", "occupancy | falsehit")
	n := flag.Uint64("n", 1_000_000, "keys to insert")
	policy := flag.String("policy", "doc", "doc | lf75 | lf85")
	mode := flag.String("mode", "counts", "counts | exact (exact stores every hash, capped at 1e8)")
	probes := flag.Uint64("probes", 1_000_000, "falsehit probes")
	seed := flag.Uint64("seed", 1, "rng seed")
	quick := flag.Bool("quick", false, "smoke run at 1e5 keys")
	header := flag.Bool("header", false, "print the occupancy csv header and exit")
	flag.Parse()
	if *header {
		fmt.Printf("arm,n,policy,mode,buckets,chain_links,fill_mean,fill_p50,fill_p95,fill_max,chained_pct,chain2_pct,splits,max_split_window,dir_b_per_key,disk_b_per_key,heap_dir_b_per_key,elapsed_s\n")
		return
	}
	if *quick {
		*n = 100_000
	}
	exact := *mode == "exact"
	if exact && *n > 100_000_000 {
		fmt.Fprintln(os.Stderr, "exact mode stores 8 bytes per key; use counts mode past 1e8")
		os.Exit(2)
	}
	switch *arm {
	case "falsehit":
		runFalseHit(*n, *probes, *seed)
	case "occupancy":
		r := runSim(*n, *policy, *seed, exact)
		fmt.Printf("occupancy,%d,%s,%s,%d,%d,%.4f,%d,%d,%d,%.4f,%.4f,%d,%d,%.4f,%.1f,%.4f,%.1f\n",
			r.n, *policy, *mode, r.buckets, r.chainLinks, r.fillMean, r.fillP50, r.fillP95, r.fillMax,
			r.chainedPct, r.chain2Pct, r.splits, r.maxWindow,
			r.dirBPerKey, r.diskBPerKey, r.heapBPerKey, r.elapsed.Seconds())
		fmt.Fprintf(os.Stderr, "n=%d policy=%s: %.4f heap B/key (dir %.4f), fill %.3f, chained %.3f%%, chain2 %.4f%%, max window %d\n",
			r.n, *policy, r.heapBPerKey, r.dirBPerKey, r.fillMean, r.chainedPct, r.chain2Pct, r.maxWindow)
	default:
		fmt.Fprintf(os.Stderr, "unknown arm %q\n", *arm)
		os.Exit(2)
	}
}
