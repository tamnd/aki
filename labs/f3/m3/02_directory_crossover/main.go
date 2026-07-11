// Lab: flat-versus-Fenwick chunk-directory crossover (spec 2064/f3 doc 13
// section 2.4, M3 lab 02).
//
// The question: f3's native list is a chunked resident byte deque, a ring of
// fixed-capacity chunks where each chunk holds a contiguous run of consecutive
// list positions. Positional access (LINDEX, LSET, LRANGE) resolves a dense
// index k to a (chunk, in-chunk ordinal) pair through the chunk directory,
// which holds the per-chunk live counts and their prefix sums. Doc 13 gives the
// directory two representations and says "the flat-versus-Fenwick crossover is a
// lab decision" (section 2.4). This lab prices that crossover and freezes
// FLAT_MAX, the chunk count at or below which the flat linear scan resolves a
// position faster than the Fenwick rank descent, and above which Fenwick wins.
//
// FLAT is a flat array of per-chunk live counts resolved by a linear scan:
// walk the counts, subtract each until the running index falls inside a chunk.
// It is branch-predictable and cache-resident, one cache line per eight uint64
// counts, and its update is a single add on one chunk's count. Its select cost
// grows with the chunk count because the scan is linear.
//
// FENWICK is a Fenwick/BIT tree over the same per-chunk counts. select(k) is a
// power-of-two rank descent, O(log chunks) uint64 loads over a dense contiguous
// array indexed by bit arithmetic, no pointer chase and no allocation. Its
// update is an O(log chunks) mirror walk up the tree. For a 17K-chunk list the
// descent is about fourteen loads with prefetch-friendly strides.
//
// Method: in-process, no server, no wire, no engine import. Both directories are
// lab-local and self-contained. The lab sweeps the chunk count across a
// realistic range and, at each point, times select (selectFlat against
// fenwick.rank over uniformly-random k in [0,total)) and update (a single flat
// add against a Fenwick mirror walk). It runs two value bands: a 64B band where
// a chunk holds about sixty elements, and a small-value band where a chunk holds
// about three or four, so the total scales differently for the same chunk count.
// All randomness comes from a fixed-seed xorshift so the table reproduces.
//
// Read: for each chunk count the flat and Fenwick select ns/op, the select
// winner, and both update ns/op. The crossover is where the select winner flips.
// The directory maintenance amortizes over element ops, because a chunk-count
// change happens about once per sixty pushes at 64B values, so select dominates
// the verdict, but the update columns keep the maintenance cost honest. See
// README.md for the sweep table and the frozen FLAT_MAX.
package main

import (
	"flag"
	"fmt"
	"time"
)

// selectFlat resolves a dense index k to a (chunk, remainder) pair by a linear
// scan over the per-chunk live counts. This is the flat directory's whole
// select path: branch-predictable, cache-resident, no state beyond the slice.
// k must be in [0, sum(counts)).
func selectFlat(counts []uint64, k int) (chunk, rem int) {
	for i, c := range counts {
		if uint64(k) < c {
			return i, k
		}
		k -= int(c)
	}
	// Only reached if k is out of range; the callers keep k in [0,total).
	return len(counts) - 1, k
}

// fenwick is a Fenwick/BIT tree over the per-chunk live counts. tree is
// 1-indexed with a dummy slot 0, so tree[i] holds the sum of a block of counts
// ending at position i. pw is the largest power of two not exceeding n, the
// start stride for the rank descent.
type fenwick struct {
	tree []uint64
	n    int
	pw   int
}

// newFenwick builds the tree from per-chunk counts in O(n) by seeding each slot
// with its own count and folding each into its parent.
func newFenwick(counts []uint64) *fenwick {
	n := len(counts)
	tree := make([]uint64, n+1)
	for i, c := range counts {
		tree[i+1] = c
	}
	for i := 1; i <= n; i++ {
		j := i + (i & -i)
		if j <= n {
			tree[j] += tree[i]
		}
	}
	pw := 1
	for pw<<1 <= n {
		pw <<= 1
	}
	return &fenwick{tree: tree, n: n, pw: pw}
}

// rank finds the chunk bracketing dense index k and the remainder within it,
// returning the same (chunk, rem) pair selectFlat does. It is a power-of-two
// descent: at each stride it steps right if the block sum there still fits under
// the remaining index, consuming that block. k must be in [0, total).
func (f *fenwick) rank(k int) (chunk, rem int) {
	pos := 0
	rem = k
	for pw := f.pw; pw > 0; pw >>= 1 {
		next := pos + pw
		if next <= f.n && f.tree[next] <= uint64(rem) {
			pos = next
			rem -= int(f.tree[next])
		}
	}
	return pos, rem
}

// add bumps chunk i's count by delta, walking the mirror path up the tree so
// every block sum that covers i stays correct. This is the Fenwick update, the
// O(log chunks) counterpart to the flat directory's single add.
func (f *fenwick) add(i int, delta int64) {
	for p := i + 1; p <= f.n; p += p & (-p) {
		f.tree[p] = uint64(int64(f.tree[p]) + delta)
	}
}

// total sums every chunk's live count, the number of resolvable positions.
func total(counts []uint64) int {
	var s int
	for _, c := range counts {
		s += int(c)
	}
	return s
}

// xorshift is a deterministic 64-bit PRNG; a fixed seed makes the table
// reproducible from run to run.
type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

// band is one value band: a mean live-count per chunk and a spread, so the lab
// can hold chunks near-full (64B values, about sixty each) or nearly empty
// (small values, about three or four each) and watch the total scale
// differently for the same chunk count.
type band struct {
	name string
	lo   int // smallest per-chunk live count
	hi   int // largest per-chunk live count (inclusive)
}

// makeCounts fills a chunk-count-long slice with per-chunk live counts drawn
// uniformly from the band, seeded deterministically from the chunk count and
// band so every run sees the same directory.
func makeCounts(chunks int, b band, seed uint64) []uint64 {
	counts := make([]uint64, chunks)
	rng := xorshift(seed ^ (uint64(chunks) << 20) ^ uint64(len(b.name)))
	span := uint64(b.hi - b.lo + 1)
	for i := range counts {
		counts[i] = uint64(b.lo) + rng.next()%span
	}
	return counts
}

// selOps and updOps set the sample budget per chunk count. Select is timed with
// more samples at small chunk counts (the op is cheap and needs a stable mean)
// and fewer at large counts (the flat scan is long), keeping total work bounded
// and the run quick. Update is cheap on both sides, so it keeps a flat budget.
func selOps(chunks int, quick bool) int {
	n := 200_000_000 / (chunks + 16)
	if n < 100_000 {
		n = 100_000
	}
	if n > 5_000_000 {
		n = 5_000_000
	}
	if quick {
		n /= 10
		if n < 20_000 {
			n = 20_000
		}
	}
	return n
}

func updOps(quick bool) int {
	if quick {
		return 300_000
	}
	return 3_000_000
}

// timeSelectFlat times the flat linear scan over uniformly-random k.
func timeSelectFlat(counts []uint64, tot, ops int, seed uint64) (float64, int) {
	rng := xorshift(seed)
	var sink int
	s := time.Now()
	for i := 0; i < ops; i++ {
		k := int(rng.next() % uint64(tot))
		c, _ := selectFlat(counts, k)
		sink += c
	}
	el := time.Since(s).Nanoseconds()
	return float64(el) / float64(ops), sink
}

// timeSelectFenwick times the Fenwick rank descent over the same k stream.
func timeSelectFenwick(f *fenwick, tot, ops int, seed uint64) (float64, int) {
	rng := xorshift(seed)
	var sink int
	s := time.Now()
	for i := 0; i < ops; i++ {
		k := int(rng.next() % uint64(tot))
		c, _ := f.rank(k)
		sink += c
	}
	el := time.Since(s).Nanoseconds()
	return float64(el) / float64(ops), sink
}

// timeUpdateFlat times the flat directory's single-add update: pick a chunk,
// bump its live count. This is the O(1) push/pop/surgery count change.
func timeUpdateFlat(counts []uint64, ops int, seed uint64) (float64, uint64) {
	rng := xorshift(seed)
	n := len(counts)
	s := time.Now()
	for i := 0; i < ops; i++ {
		idx := int(rng.next() % uint64(n))
		counts[idx]++
	}
	el := time.Since(s).Nanoseconds()
	var sink uint64
	for _, c := range counts {
		sink += c
	}
	return float64(el) / float64(ops), sink
}

// timeUpdateFenwick times the Fenwick mirror walk over the same chunk stream.
func timeUpdateFenwick(f *fenwick, ops int, seed uint64) (float64, uint64) {
	rng := xorshift(seed)
	n := f.n
	s := time.Now()
	for i := 0; i < ops; i++ {
		idx := int(rng.next() % uint64(n))
		f.add(idx, 1)
	}
	el := time.Since(s).Nanoseconds()
	var sink uint64
	for i := 1; i <= f.n; i++ {
		sink += f.tree[i]
	}
	return float64(el) / float64(ops), sink
}

func main() {
	quick := flag.Bool("quick", false, "smaller sample budgets for a fast run")
	flag.Parse()

	chunkCounts := []int{4, 8, 16, 32, 48, 64, 96, 128, 256, 512, 1024, 4096, 17408}
	bands := []band{
		{"64B", 56, 64}, // chunk near full, about sixty live per chunk
		{"small", 3, 4}, // chunk nearly empty, three or four live per chunk
	}

	const kSeed = 0x9e3779b97f4a7c15
	const updSeed = 0xd1b54a32d192ed03

	fmt.Printf("flat-versus-Fenwick chunk-directory crossover, Apple M4, darwin/arm64, %s\n",
		time.Now().Format("2006-01-02"))

	var guard int
	for _, b := range bands {
		fmt.Printf("\nband %s (%d-%d live per chunk)\n", b.name, b.lo, b.hi)
		fmt.Printf("%8s %10s %8s %11s %8s %11s %11s\n",
			"chunks", "total", "flatSel", "fenwSel", "winner", "flatUpd", "fenwUpd")
		for _, ch := range chunkCounts {
			counts := makeCounts(ch, b, 0x51ab)
			tot := total(counts)
			f := newFenwick(counts)

			so := selOps(ch, *quick)
			flatSel, s1 := timeSelectFlat(counts, tot, so, kSeed)
			fenwSel, s2 := timeSelectFenwick(f, tot, so, kSeed)
			guard += s1 + s2

			uo := updOps(*quick)
			// Fresh copies so the two update loops start from the same directory
			// and neither inherits the other's bumped counts.
			flatCopy := append([]uint64(nil), counts...)
			flatUpd, u1 := timeUpdateFlat(flatCopy, uo, updSeed)
			fenwUpd, u2 := timeUpdateFenwick(newFenwick(counts), uo, updSeed)
			guard += int(u1 + u2)

			winner := "fenwick"
			if flatSel <= fenwSel {
				winner = "flat"
			}
			fmt.Printf("%8d %10d %8.2f %11.2f %8s %11.2f %11.2f\n",
				ch, tot, flatSel, fenwSel, winner, flatUpd, fenwUpd)
		}
	}
	if guard == -1 {
		fmt.Println(guard)
	}
}
