// Lab: zset rank math at scale (spec 2064/sqlo1 doc 09 sections 2 and
// 10, milestone T4 lab 02).
//
// T4 slice 5 bakes the score fence's paged shape, and the flat fence
// shipped in slice 3 caps at 100 runs, roughly 10^4 members, against
// a headline that says ZRANK stays flat to 10^9. Doc 09 sketches one
// root page index with per-page totals and calls rank math two
// bounded scans, but the arithmetic does not close at scale: at the
// hsegz occupancy of ~104 entries per run, 10^9 members are ~10^7
// runs, and a 250-entry root index over 250-entry pages addresses
// 62500. Something gives, and each candidate has a price with a
// different unit. A longer root index pays on every command, because
// the root is the plane's commit point and bills its full frame each
// time. Bigger or deeper fence pages pay per score move, because a
// move edits a run count and the edit propagates one node per index
// level. More levels also lengthen the cold path, one record per
// level. This lab builds the fence shapes for real, measures the hot
// prefix-sum walk, and bills the strict per-move frame group beside a
// drain-coalesced arm, so the slice can pick fanouts on numbers.
//
// The model is fence arithmetic only, resident (the salgebra
// pattern). Run contents never matter for the walk above the leaf,
// so runs are synthetic: a per-run entry count array is the leaf
// level, index levels group it by a fanout per level, and one encoded
// run image stands in for the final in-run scan every rank pays. The
// data-record side of a move's bill (member segment plus two run
// post-images) is carried as constants from the hsegz lab's occupancy
// row, since this lab only decides the fence shape on top of them.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"sort"
	"time"
)

// Encoded sizes. Leaf fence entries are doc 09's 20 bytes (u64
// score_lo, 48-bit segid under 16 bits of meta, u32 count). Upper
// index entries carry a u64 subtree total instead of the u32 count,
// because a top-level subtree at 10^9 members overflows u32; 24 bytes
// covers lo, pageid, and the total.
const (
	leafEntBytes  = 20
	upperEntBytes = 24
	pageHdrBytes  = 4
	rootHdrBytes  = 44

	// The hsegz occupancy row at the baked 4032 thresholds: ~104
	// entries per run and ~2.7 KB per data record image. A score move
	// bills one member segment plus a remove-run plus an insert-run
	// post-image before any fence bytes.
	runEntries   = 104
	dataImgBytes = 2700
	moveDataImgs = 3

	// The in-run scan the rank pays after the walk: u16 mlen, u64
	// sortable, member bytes, the doc 09 run entry.
	runEntHdr = 10
	memberLen = 12
)

// shape is one fence configuration: fanouts of the index levels below
// the root, leaf-most first. Empty means flat, every run entry in the
// root. The root always holds whatever the top level leaves over, and
// its length is a result, not a parameter: the verdict applies the
// root-size cap after seeing the bill.
type shape struct {
	name string
	fan  []int
}

// fence is the built structure: counts is the per-run leaf, levels[k]
// the per-node subtree totals of index level k (leaf-most first),
// the last level being the root index.
type fence struct {
	fan    []int
	counts []uint32
	levels [][]uint64
	total  uint64
}

func buildFence(n uint64, sh shape, rng *rand.Rand) *fence {
	f := &fence{fan: sh.fan}
	// Jittered occupancy around the hsegz row, normalized to sum n:
	// churned runs drift between half and full, and the walk cost only
	// depends on node counts, not exact spread.
	runs := int(n / runEntries)
	if runs == 0 {
		runs = 1
	}
	f.counts = make([]uint32, runs)
	var sum uint64
	for i := range f.counts {
		c := runEntries/2 + rng.Intn(runEntries)
		f.counts[i] = uint32(c)
		sum += uint64(c)
	}
	// Scale to n approximately; exactness is irrelevant to the shape.
	for i := range f.counts {
		f.counts[i] = uint32(uint64(f.counts[i]) * n / sum)
		if f.counts[i] == 0 {
			f.counts[i] = 1
		}
		f.total += uint64(f.counts[i])
	}
	prev := make([]uint64, runs)
	for i, c := range f.counts {
		prev[i] = uint64(c)
	}
	for _, fan := range f.fan {
		nodes := (len(prev) + fan - 1) / fan
		lvl := make([]uint64, nodes)
		for i, v := range prev {
			lvl[i/fan] += v
		}
		f.levels = append(f.levels, lvl)
		prev = lvl
	}
	return f
}

// rootEnts is the root index length: the node count of the top index
// level, or every run when flat.
func (f *fence) rootEnts() int {
	if len(f.levels) == 0 {
		return len(f.counts)
	}
	return len(f.levels[len(f.levels)-1])
}

func (f *fence) rootBytes() int {
	eb := upperEntBytes
	if len(f.levels) == 0 {
		eb = leafEntBytes
	}
	return rootHdrBytes + f.rootEnts()*eb
}

// pageBytes is one index page's encoded size at level k (leaf-most
// first): level 0 pages hold leaf fence entries, upper pages hold
// index entries.
func (f *fence) pageBytes(k int) int {
	eb := upperEntBytes
	if k == 0 {
		eb = leafEntBytes
	}
	return pageHdrBytes + f.fan[k]*eb
}

// rank walks the fence to the run covering absolute rank r, the hot
// arithmetic ZRANK and by-index ZRANGE pay: a linear prefix sum over
// the root index, one node per level down, then the covering run's
// index and the residual offset inside it. The scan is the real loop
// over the built arrays, so the measured time is the walk the engine
// would run over decoded pages. Level k's node i covers child nodes
// [i*fan[k], (i+1)*fan[k]) one level down, runs when k is 0, exactly
// how buildFence grouped them.
func (f *fence) rank(r uint64) (run int, off uint64) {
	lo, hi := 0, f.rootEnts()
	for k := len(f.levels) - 1; k >= 0; k-- {
		lvl := f.levels[k]
		i := lo
		for ; i < hi && i < len(lvl); i++ {
			if r < lvl[i] {
				break
			}
			r -= lvl[i]
		}
		if i >= len(lvl) {
			i = len(lvl) - 1
		}
		lo, hi = i*f.fan[k], (i+1)*f.fan[k]
	}
	for i := lo; i < hi && i < len(f.counts); i++ {
		if r < uint64(f.counts[i]) {
			return i, r
		}
		r -= uint64(f.counts[i])
	}
	last := min(hi, len(f.counts)) - 1
	return last, uint64(f.counts[last]) - 1
}

// scanRun burns the in-run tail of a rank op on a real encoded image:
// decode entries to the offset, the doc 09 run layout.
func scanRun(img []byte, off uint64) uint64 {
	p := 0
	var acc uint64
	for i := uint64(0); i <= off && p+runEntHdr <= len(img); i++ {
		ml := int(binary.LittleEndian.Uint16(img[p:]))
		acc += binary.LittleEndian.Uint64(img[p+2:])
		p += runEntHdr + ml
	}
	return acc
}

func buildRunImg() []byte {
	img := make([]byte, 0, runEntries*(runEntHdr+memberLen))
	for i := range runEntries {
		var e [runEntHdr]byte
		binary.LittleEndian.PutUint16(e[:], memberLen)
		binary.LittleEndian.PutUint64(e[2:], uint64(i)<<32)
		img = append(img, e[:]...)
		img = append(img, make([]byte, memberLen)...)
	}
	return img
}

// move edits the leaf counts for a score move from run a to run b,
// propagates the subtree totals, and returns the distinct index nodes
// touched per level, the strict frame-group bill's fence half. The
// caller keeps a out of run-death (count 1), the model stays clear of
// merge paths.
func (f *fence) move(a, b int) (nodes [][]int) {
	f.counts[a]--
	f.counts[b]++
	nodes = make([][]int, len(f.levels))
	ia, ib := a, b
	for k := range f.levels {
		ia, ib = ia/f.fan[k], ib/f.fan[k]
		if ia != ib {
			f.levels[k][ia]--
			f.levels[k][ib]++
			nodes[k] = []int{ia, ib}
		} else {
			nodes[k] = []int{ia}
		}
	}
	return nodes
}

type quantile struct{ ns []float64 }

func (q *quantile) add(v float64) { q.ns = append(q.ns, v) }
func (q *quantile) at(p float64) float64 {
	if len(q.ns) == 0 {
		return 0
	}
	s := slices.Clone(q.ns)
	sort.Float64s(s)
	i := int(p * float64(len(s)-1))
	return s[i]
}

func main() {
	var (
		n       = flag.Uint64("n", 1_000_000, "members")
		shName  = flag.String("shape", "p250", "flat, p250, p1000, p250x250, p128x128, p512x512")
		pattern = flag.String("pattern", "uniform", "uniform or board move locality")
		window  = flag.Int("window", 64, "drain-coalescing window in commands")
		ranks   = flag.Int("ranks", 50_000, "rank ops to time")
		moves   = flag.Int("moves", 200_000, "moves to bill")
		quick   = flag.Bool("quick", false, "smoke run")
	)
	flag.Parse()
	if *quick {
		*n, *ranks, *moves = 100_000, 5_000, 20_000
	}

	shapes := map[string]shape{
		"flat":     {"flat", nil},
		"p250":     {"p250", []int{250}},
		"p1000":    {"p1000", []int{1000}},
		"p250x250": {"p250x250", []int{250, 250}},
		"p128x128": {"p128x128", []int{128, 128}},
		"p512x512": {"p512x512", []int{512, 512}},
	}
	sh, ok := shapes[*shName]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown shape %q\n", *shName)
		os.Exit(2)
	}

	rng := rand.New(rand.NewSource(1))
	f := buildFence(*n, sh, rng)
	runImg := buildRunImg()

	// Hot rank walk: random absolute ranks, the full arithmetic a
	// ZRANK pays after the member-segment score lookup.
	var q quantile
	var sink uint64
	for i := 0; i < *ranks; i++ {
		r := rng.Uint64() % f.total
		t0 := time.Now()
		run, off := f.rank(r)
		sink += scanRun(runImg, off%runEntries)
		q.add(float64(time.Since(t0).Nanoseconds()))
		_ = run
	}

	// The cold path: one record per fence level below the root, plus
	// the root, the member segment (score lookup), and the run.
	coldRecs := 3 + len(f.fan) // root + member seg + run + one page per level
	coldBytes := f.rootBytes() + dataImgBytes + len(runImg)
	for k := range f.fan {
		coldBytes += f.pageBytes(k)
	}

	// Move bills. Strict: every touched index node is a post-image in
	// the command's frame group, beside the root and the data images.
	// Deferred: dirty index pages coalesce across the window and bill
	// once per drain, the doc 06 W4 lever applied to fence pages; the
	// root still bills every command, it is the commit point.
	var strictB, deferB float64
	dirty := make([]map[int]bool, len(f.levels))
	for k := range dirty {
		dirty[k] = map[int]bool{}
	}
	winB := 0.0
	cmds := 0
	flushWin := func() {
		for k := range dirty {
			winB += float64(len(dirty[k]) * f.pageBytes(k))
			dirty[k] = map[int]bool{}
		}
	}
	for i := 0; i < *moves; i++ {
		a := rng.Intn(len(f.counts))
		for f.counts[a] <= 1 {
			a = rng.Intn(len(f.counts))
		}
		var b int
		if *pattern == "board" {
			b = a + rng.Intn(7) - 3
			if b < 0 || b >= len(f.counts) {
				b = a
			}
		} else {
			b = rng.Intn(len(f.counts))
		}
		nodes := f.move(a, b)
		bill := moveDataImgs*dataImgBytes + f.rootBytes()
		for k, ns := range nodes {
			bill += len(ns) * f.pageBytes(k)
			for _, nd := range ns {
				dirty[k][nd] = true
			}
		}
		strictB += float64(bill)
		winB += float64(moveDataImgs*dataImgBytes + f.rootBytes())
		cmds++
		if cmds%*window == 0 {
			flushWin()
		}
	}
	flushWin()
	deferB = winB

	fmt.Printf("%s,%d,%d,%d,%d,%.1f,%.0f,%.0f,%d,%.1f,%.2f,%.2f,%s,%d,%d\n",
		sh.name, *n, len(f.counts), len(f.fan)+1, f.rootEnts(),
		float64(f.rootBytes())/1024,
		q.at(0.50), q.at(0.99),
		coldRecs, float64(coldBytes)/1024,
		strictB/float64(*moves)/1024, deferB/float64(*moves)/1024,
		*pattern, *window, int(sink%2))
}
