// Lab: partition-parallel merge fan-out (spec 2064/f3 doc 11 section 6.5, M1
// lab 08).
//
// The question: L12 says the single-threaded sorted-hash merge beats the probe
// baseline by only ~1.6x, and the 2x symmetric-algebra gate rides on fanning
// the P partition-pair merges out across workers (doc 11 section 6.5). Same-P
// operands intersect partition by partition because both sides were split by
// the same member hash bits, so the P pair merges are independent read-only
// tasks under the command's barrier. Two things are asked. First, does cutting
// the merge into P pairs cost anything sequentially (K16 measured 5.78ms
// partitioned against 5.80ms flat at 1M, so the expected answer is no)?
// Second, where is the work threshold below which the fan-out barrier costs
// more than it saves, against the doc's pre-registered 64k merge elements?
//
// Method: in-process, no server, no wire, no engine import. A lab-local
// operand is P sorted runs of (hash, ordinal) entries split by the top
// log2(P) hash bits, members resolved from a flat slab of fixed 16-byte
// members, hash ties byte-confirmed before a match counts, galloping advance
// on skewed runs: the section-6.6 kernel shape over the section-6.1 arrays,
// pure sorted runs with no tail (lab 05 priced the bounded tail; it is a
// small constant term either way). Both operands are built same-P; the
// cross-P slicing is engine-tested (engine/f3/set/fanout_test.go), not a
// timing question. Three executors run the same P pair merges:
//
//   - seq: one loop on the calling goroutine, the coordinating-owner form.
//   - spawn: k goroutines started per command, groups claimed off an atomic
//     counter, joined on a WaitGroup. This overstates the donation cost (the
//     engine donates to already-running idle workers), so its crossover is
//     the conservative bound.
//   - pool: k persistent workers parked on channels, kicked per command with
//     static group striding, joined on a channel. This understates dispatch
//     (no queue contention), so the true engine barrier sits between the two.
//
// Axes: per-side cardinality 1M at overlap 0.5 with P in {4, 16, 64, 256} and
// k in {2, 4, 8} for the flatness and scaling read; per-side cardinality
// {2k..512k} at P=16, k=4 for the crossover read, sequential against both
// executors. Read: ns per command, ns per merge element (2n per pair), the
// speedup over seq, and the smallest sweep size where each executor beats
// seq. See README.md for the frozen verdict.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const memW = 16 // fixed member width: "s%015d" renders exactly 16 bytes

// entry is one member's sorted-array cell, the doc 11 section 6.1 shape.
type entry struct {
	h   uint64
	ord uint32
}

// op is one operand: P sorted runs split by the top hash bits over a flat
// member slab.
type op struct {
	runs [][]entry
	slab []byte
}

func (o *op) member(ord uint32) []byte {
	return o.slab[int(ord)*memW : int(ord)*memW+memW]
}

// hash64 is FNV-1a over the member bytes, the lab's stand-in for the engine
// store hash: any 64-bit hash of the bytes works, both operands just have to
// agree on it.
func hash64(m []byte) uint64 {
	h := uint64(0xcbf29ce484222325)
	for _, c := range m {
		h ^= uint64(c)
		h *= 0x100000001b3
	}
	return h
}

// build assembles an operand of n members split into P runs: shared members
// s0..shared-1 (present in both operands of a pair) and distinct members
// prefixed by tag. Runs sort ascending by hash.
func build(p, n, shared int, tag byte) *op {
	o := &op{runs: make([][]entry, p), slab: make([]byte, 0, n*memW)}
	lg := bits.Len(uint(p)) - 1
	add := func(m []byte) {
		h := hash64(m)
		idx := int(h >> (64 - lg))
		o.runs[idx] = append(o.runs[idx], entry{h: h, ord: uint32(len(o.slab) / memW)})
		o.slab = append(o.slab, m...)
	}
	for i := 0; i < shared; i++ {
		add([]byte(fmt.Sprintf("s%015d", i)))
	}
	for i := shared; i < n; i++ {
		add([]byte(fmt.Sprintf("%c%015d", tag, i)))
	}
	for i := range o.runs {
		r := o.runs[i]
		sort.Slice(r, func(a, b int) bool { return r[a].h < r[b].h })
	}
	return o
}

// buildPair builds a same-P operand pair of n members each sharing
// overlap*n members.
func buildPair(p, n int, overlap float64) (*op, *op) {
	shared := int(float64(n) * overlap)
	return build(p, n, shared, 'a'), build(p, n, shared, 'b')
}

// gallop advances i to the first r entry with hash >= target: doubling step,
// then binary search, the section-6.6 advance.
func gallop(r []entry, i int, target uint64) int {
	step := 1
	for i+step < len(r) && r[i+step].h < target {
		i += step
		step <<= 1
	}
	lo, hi := i, i+step
	if hi > len(r) {
		hi = len(r)
	}
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if r[mid].h < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// mergeCount is the partition-pair intersection kernel: two-pointer over the
// sorted runs, galloping past misses, equal-hash spans cross-confirmed by
// member bytes so a bare collision never counts (section 6.6).
func mergeCount(a, b *op, ra, rb []entry) int {
	count := 0
	i, j := 0, 0
	for i < len(ra) && j < len(rb) {
		switch {
		case ra[i].h < rb[j].h:
			i = gallop(ra, i, rb[j].h)
		case ra[i].h > rb[j].h:
			j = gallop(rb, j, ra[i].h)
		default:
			hv := ra[i].h
			ie, je := i, j
			for ie < len(ra) && ra[ie].h == hv {
				ie++
			}
			for je < len(rb) && rb[je].h == hv {
				je++
			}
			for x := i; x < ie; x++ {
				for y := j; y < je; y++ {
					if bytes.Equal(a.member(ra[x].ord), b.member(rb[y].ord)) {
						count++
						break
					}
				}
			}
			i, j = ie, je
		}
	}
	return count
}

// seq runs the P pair merges in one loop on the calling goroutine, the
// coordinating-owner executor.
func seq(a, b *op) int {
	count := 0
	for i := range a.runs {
		count += mergeCount(a, b, a.runs[i], b.runs[i])
	}
	return count
}

// spawn runs the pair merges on k goroutines started per command, groups
// claimed off an atomic counter, joined on a WaitGroup: the conservative
// donation model, paying goroutine start per command.
func spawn(a, b *op, k int) int {
	var next, total atomic.Int64
	var wg sync.WaitGroup
	wg.Add(k)
	for w := 0; w < k; w++ {
		go func() {
			defer wg.Done()
			local := 0
			for {
				i := int(next.Add(1)) - 1
				if i >= len(a.runs) {
					break
				}
				local += mergeCount(a, b, a.runs[i], b.runs[i])
			}
			total.Add(int64(local))
		}()
	}
	wg.Wait()
	return int(total.Load())
}

// pool is k persistent workers parked on channels: per command the
// coordinator publishes the operands, kicks every worker, and drains k
// partial counts, with worker w taking groups w, w+k, w+2k in a static
// stride. This is the donated-task shape with no start cost, the optimistic
// bound on the engine barrier.
type pool struct {
	k     int
	work  []chan [2]*op
	part  chan int
	close sync.Once
}

func newPool(k int) *pool {
	p := &pool{k: k, work: make([]chan [2]*op, k), part: make(chan int, k)}
	for w := 0; w < k; w++ {
		ch := make(chan [2]*op, 1)
		p.work[w] = ch
		go func(w int) {
			for job := range ch {
				a, b := job[0], job[1]
				local := 0
				for i := w; i < len(a.runs); i += p.k {
					local += mergeCount(a, b, a.runs[i], b.runs[i])
				}
				p.part <- local
			}
		}(w)
	}
	return p
}

func (p *pool) run(a, b *op) int {
	for _, ch := range p.work {
		ch <- [2]*op{a, b}
	}
	count := 0
	for w := 0; w < p.k; w++ {
		count += <-p.part
	}
	return count
}

func (p *pool) stop() {
	p.close.Do(func() {
		for _, ch := range p.work {
			close(ch)
		}
	})
}

// timeOp runs fn until minDur and returns ns per command. sink defeats
// dead-code elimination; the count is also the correctness cross-check
// against want.
var sink int

func timeOp(fn func() int, want int) float64 {
	const minDur = 200 * time.Millisecond
	reps := 0
	start := time.Now()
	for time.Since(start) < minDur {
		sink = fn()
		if sink != want {
			panic(fmt.Sprintf("count %d, want %d", sink, want))
		}
		reps++
	}
	return float64(time.Since(start).Nanoseconds()) / float64(reps)
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinalities for a fast check")
	flag.Parse()

	n := 1_000_000
	crossNs := []int{2_048, 4_096, 8_192, 16_384, 32_768, 65_536, 131_072, 262_144, 524_288}
	if *quick {
		n = 200_000
		crossNs = []int{2_048, 8_192, 32_768, 131_072}
	}

	fmt.Printf("scale sweep: %d-by-%d members, overlap 0.5, ns per command\n", n, n)
	fmt.Println("| P | seq ms | seq ns/elem | spawn k=2 | k=4 | k=8 | pool k=2 | k=4 | k=8 |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|")
	for _, p := range []int{4, 16, 64, 256} {
		a, b := buildPair(p, n, 0.5)
		want := seq(a, b)
		seqNs := timeOp(func() int { return seq(a, b) }, want)
		row := fmt.Sprintf("| %d | %.2f | %.2f |", p, seqNs/1e6, seqNs/float64(2*n))
		for _, k := range []int{2, 4, 8} {
			row += fmt.Sprintf(" %.2f |", timeOp(func() int { return spawn(a, b, k) }, want)/1e6)
		}
		for _, k := range []int{2, 4, 8} {
			pl := newPool(k)
			row += fmt.Sprintf(" %.2f |", timeOp(func() int { return pl.run(a, b) }, want)/1e6)
			pl.stop()
		}
		fmt.Println(row)
	}

	fmt.Println()
	fmt.Println("crossover sweep: P=16, k=4, overlap 0.5, us per command")
	fmt.Println("| per-side n | merge elems | seq us | spawn us | pool us | spawn/seq | pool/seq |")
	fmt.Println("|---|---|---|---|---|---|---|")
	pl := newPool(4)
	for _, cn := range crossNs {
		a, b := buildPair(16, cn, 0.5)
		want := seq(a, b)
		seqNs := timeOp(func() int { return seq(a, b) }, want)
		spNs := timeOp(func() int { return spawn(a, b, 4) }, want)
		plNs := timeOp(func() int { return pl.run(a, b) }, want)
		fmt.Printf("| %d | %d | %.1f | %.1f | %.1f | %.2f | %.2f |\n",
			cn, 2*cn, seqNs/1e3, spNs/1e3, plNs/1e3, spNs/seqNs, plNs/seqNs)
	}
	pl.stop()
}
