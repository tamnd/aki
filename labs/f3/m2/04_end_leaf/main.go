// Lab: end-leaf caching for the zset pop paths (spec 2064/f3 doc 12 sections
// 6.7 and 6.10, M2 lab 04).
//
// The question: ZPOPMIN and ZPOPMAX hammer the extreme leaves of the counted
// B+ tree, and slice 6 must decide what cached state (end-leaf pointers, a
// hot-path descent bypass) pays for that hammering and what it costs to keep
// valid under interleaved inserts and deletes. Through the tree's public API a
// pop is two descents: a counted select to rank 0 (or n-1) to learn the extreme
// entry, then a routed delete of that exact key. A cached end run deletes the
// first descent; the delete descent stays, because the tree exports no leaf
// handles and this lab adds no hook, so every cached-arm number here is a lower
// bound on what an engine-internal fused pop can win (see README).
//
// Method: in-process, no server, no wire, benched against the real
// engine/f3/struct tree (#610) through its exported API only. Scores are
// distinct 8-byte sortable keys (mix of a counter, a bijection, so no dedup
// set), members nil, the same convention as the tree's own benchmarks, so the
// Members callback never runs on a descent. Four sweeps:
//
//   - Pure pops across cardinality {1k, 100k, 1M, 4M}: the bare select leg, the
//     uncached ZPOPMIN and ZPOPMAX (select plus delete), the cached-run pop at
//     the 31-entry leaf capacity, and a uniform-random ZREM for contrast
//     between the edge spine and a random descent.
//   - Batch drain at 1M: ZMPOP COUNT=c as one primed run of c plus c deletes,
//     c in {1..124}, split into the prime share and the delete share so the
//     saturation point of the batch win is readable.
//   - Interleave at 1M: pops mixed with uniform writes at pop fractions
//     {90, 50, 10} percent plus an adversarial descending-insert shape, each
//     under three policies: naive (re-descend per pop), invalidate (any edge
//     write drops the run, lazily re-primed), absorb (edge writes edit the
//     cached run in place). The run cache is kept exact: it always mirrors the
//     tree's first entries, so the policy costs are real, not modeled.
//   - Latency quantiles: per-op timed pops and removes, p50/p99/max, because
//     PRED-F3-M2-ZREMTAIL gates the p99 shoulder, not the mean, and sustained
//     end pops make the edge leaf borrow from its right sibling almost every
//     op, which is exactly the kind of hidden constant a mean hides.
//
// Read: ns/op per arm and cardinality, the batch saturation point, the
// interleave crossover if any, and the quantile table. See README.md for the
// tables and the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"sort"
	"time"

	structs "github.com/tamnd/aki/engine/f3/struct"
)

// noMembers is the Members callback for distinct-score keys: the tree compares
// member bytes only on a score tie, every arm here keeps scores distinct, so
// the callback never runs on a descent.
type noMembers struct{}

// Member satisfies structs.Members; it never runs in this lab's arms.
func (noMembers) Member(uint32) []byte { return nil }

// mix is the splitmix64 finalizer, a bijection on uint64, so mixing a counter
// yields distinct uniform keys with no dedup set.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// keygen hands out distinct uniform keys across the whole run: one shared
// counter through the mix bijection, so a churn insert can never collide with a
// build key.
type keygen struct{ next uint64 }

func (k *keygen) key() uint64 {
	k.next++
	return mix(k.next)
}

type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

// sortedEntries turns distinct scores into the sorted Entry slice BulkLoad
// consumes, ref numbered by rank.
func sortedEntries(scores []uint64) []structs.Entry {
	s := make([]uint64, len(scores))
	copy(s, scores)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	ents := make([]structs.Entry, len(s))
	for i, sc := range s {
		ents[i] = structs.Entry{Score: sc, Ref: uint32(i)}
	}
	return ents
}

// popMin is the uncached ZPOPMIN through the public API: one counted select to
// rank 0 for the extreme entry, one routed delete of that key. Two descents.
func popMin(t *structs.Tree) uint64 {
	s, _, ok := t.SelectAt(0)
	if !ok {
		panic("pop on empty tree")
	}
	if _, ok := t.Delete(s, nil, noMembers{}); !ok {
		panic("selected min not deletable")
	}
	return s
}

// popMax is the uncached ZPOPMAX, symmetric on the right spine.
func popMax(t *structs.Tree) uint64 {
	s, _, ok := t.SelectAt(uint64(t.Len() - 1))
	if !ok {
		panic("pop on empty tree")
	}
	if _, ok := t.Delete(s, nil, noMembers{}); !ok {
		panic("selected max not deletable")
	}
	return s
}

// runCache emulates the end-leaf cache through the public API: a primed run of
// the tree's first k entries served head-first. The invariant is exactness:
// ents[idx:] always equals the tree's first len(ents)-idx entries, so any
// write at or below bound is an edge write the cache must observe, and the
// maintenance costs measured here are real, not modeled.
type runCache struct {
	ents     []structs.Entry
	idx      int
	k        int
	reprimes int
	invals   int
}

func (c *runCache) valid() bool   { return c.idx < len(c.ents) }
func (c *runCache) bound() uint64 { return c.ents[len(c.ents)-1].Score }
func (c *runCache) drop(count bool) {
	c.ents = c.ents[:0]
	c.idx = 0
	if count {
		c.invals++
	}
}

// prime refills the run with one seek plus a leaf walk, the WalkFromRank shape
// a ZRANGE low bound rides.
func (c *runCache) prime(t *structs.Tree) {
	c.ents = c.ents[:0]
	c.idx = 0
	c.reprimes++
	t.WalkFromRank(0, func(score uint64, ref uint32) bool {
		c.ents = append(c.ents, structs.Entry{Score: score, Ref: ref})
		return len(c.ents) < c.k
	})
}

// pop serves the next minimum from the run, priming when empty, and pays the
// one descent the public API cannot waive: the routed delete.
func (c *runCache) pop(t *structs.Tree) uint64 {
	if !c.valid() {
		c.prime(t)
		if !c.valid() {
			panic("pop on empty tree")
		}
	}
	e := c.ents[c.idx]
	c.idx++
	if _, ok := t.Delete(e.Score, nil, noMembers{}); !ok {
		panic("cached min not deletable")
	}
	return e.Score
}

// observeInsert keeps the run exact across an insert the caller just applied to
// the tree. absorb edits the run in place (an insert below the current head
// reuses the free slot the last pop left, so the hot adversarial case is O(1));
// invalidate drops the run and lets the next pop re-descend.
func (c *runCache) observeInsert(score uint64, ref uint32, absorb bool) {
	if !c.valid() || score > c.bound() {
		return
	}
	if !absorb {
		c.drop(true)
		return
	}
	if c.idx > 0 && score < c.ents[c.idx].Score {
		c.idx--
		c.ents[c.idx] = structs.Entry{Score: score, Ref: ref}
		return
	}
	live := c.ents[c.idx:]
	pos := c.idx + sort.Search(len(live), func(i int) bool { return live[i].Score > score })
	c.ents = append(c.ents, structs.Entry{})
	copy(c.ents[pos+1:], c.ents[pos:])
	c.ents[pos] = structs.Entry{Score: score, Ref: ref}
}

// observeDelete keeps the run exact across a point delete the caller just
// applied to the tree. The run mirrors the tree prefix exactly, so a deleted
// key at or below bound must be present in it.
func (c *runCache) observeDelete(score uint64, absorb bool) {
	if !c.valid() || score > c.bound() {
		return
	}
	if !absorb {
		c.drop(true)
		return
	}
	live := c.ents[c.idx:]
	pos := c.idx + sort.Search(len(live), func(i int) bool { return live[i].Score >= score })
	if pos >= len(c.ents) || c.ents[pos].Score != score {
		panic("edge delete missing from exact run cache")
	}
	copy(c.ents[pos:], c.ents[pos+1:])
	c.ents = c.ents[:len(c.ents)-1]
}

// ascending panics if a pure-pop sequence ever goes backward, the cheap
// correctness tripwire every pop arm carries equally inside its timed loop.
func ascending(prev *uint64, s uint64) {
	if s <= *prev && *prev != 0 {
		panic("pop sequence went backward")
	}
	*prev = s
}

// pureCell measures one pure-pop arm at cardinality n: rounds of at most n/2
// pops on a fresh bulk-loaded tree, rebuilds outside the timer, ns/op over ops.
// reset, when non-nil, clears any arm-side cached state after each rebuild so a
// cached run never survives into a tree it no longer mirrors.
func pureCell(ents []structs.Entry, ops int, reset func(), pop func(t *structs.Tree) uint64) float64 {
	n := len(ents)
	perBuild := n / 2
	if perBuild < 1 {
		perBuild = 1
	}
	done := 0
	var dur time.Duration
	for done < ops {
		t := structs.BulkLoad(ents)
		if reset != nil {
			reset()
		}
		round := ops - done
		if round > perBuild {
			round = perBuild
		}
		var prev uint64
		start := time.Now()
		for i := 0; i < round; i++ {
			ascending(&prev, pop(t))
		}
		dur += time.Since(start)
		done += round
	}
	return float64(dur.Nanoseconds()) / float64(ops)
}

// pureCellDescending is pureCell for the max side, where the tripwire flips.
func pureCellDescending(ents []structs.Entry, ops int) float64 {
	n := len(ents)
	perBuild := n / 2
	if perBuild < 1 {
		perBuild = 1
	}
	done := 0
	var dur time.Duration
	for done < ops {
		t := structs.BulkLoad(ents)
		round := ops - done
		if round > perBuild {
			round = perBuild
		}
		prev := ^uint64(0)
		start := time.Now()
		for i := 0; i < round; i++ {
			s := popMax(t)
			if s >= prev {
				panic("popmax sequence went forward")
			}
			prev = s
		}
		dur += time.Since(start)
		done += round
	}
	return float64(dur.Nanoseconds()) / float64(ops)
}

// selectCell measures the bare find leg: SelectAt(0) on a static tree, the
// descent a cached end pointer deletes.
func selectCell(ents []structs.Entry, ops int) float64 {
	t := structs.BulkLoad(ents)
	var sink uint64
	start := time.Now()
	for i := 0; i < ops; i++ {
		s, _, _ := t.SelectAt(0)
		sink += s
	}
	d := time.Since(start)
	if sink == 42 {
		fmt.Println(sink)
	}
	return float64(d.Nanoseconds()) / float64(ops)
}

// remCell measures ZREM of uniformly random present members, the contrast row:
// the same routed delete the pops pay, on a cold random path instead of the hot
// edge spine.
func remCell(ents []structs.Entry, scores []uint64, ops int) float64 {
	n := len(ents)
	perBuild := n / 2
	if perBuild < 1 {
		perBuild = 1
	}
	done := 0
	var dur time.Duration
	for done < ops {
		t := structs.BulkLoad(ents)
		round := ops - done
		if round > perBuild {
			round = perBuild
		}
		start := time.Now()
		for i := 0; i < round; i++ {
			if _, ok := t.Delete(scores[i], nil, noMembers{}); !ok {
				panic("random member not deletable")
			}
		}
		dur += time.Since(start)
		done += round
	}
	return float64(dur.Nanoseconds()) / float64(ops)
}

// batchCell measures ZMPOP COUNT=c as groups of one primed run plus c deletes,
// returning the prime and delete shares per popped entry.
func batchCell(ents []structs.Entry, ops, c int) (primeNs, delNs float64) {
	n := len(ents)
	perBuild := n / 2
	done := 0
	var primeDur, delDur time.Duration
	run := make([]structs.Entry, 0, c)
	for done < ops {
		t := structs.BulkLoad(ents)
		inBuild := 0
		var prev uint64
		for done < ops && inBuild+c <= perBuild {
			run = run[:0]
			start := time.Now()
			t.WalkFromRank(0, func(score uint64, ref uint32) bool {
				run = append(run, structs.Entry{Score: score, Ref: ref})
				return len(run) < c
			})
			primeDur += time.Since(start)
			start = time.Now()
			for _, e := range run {
				ascending(&prev, e.Score)
				if _, ok := t.Delete(e.Score, nil, noMembers{}); !ok {
					panic("batched min not deletable")
				}
			}
			delDur += time.Since(start)
			done += len(run)
			inBuild += len(run)
		}
	}
	return float64(primeDur.Nanoseconds()) / float64(ops), float64(delDur.Nanoseconds()) / float64(ops)
}

type policy int

const (
	naive policy = iota
	invalidate
	absorb
)

type interleaveResult struct {
	nsOp     float64
	reprimes int
	invals   int
	misses   int
}

// runInterleave drives ops mixed operations against a fresh tree at cardinality
// n: popPct percent ZPOPMIN, the rest writes. Uniform writes alternate a fresh
// uniform-key insert with a delete of a previously churn-inserted key; the
// adversarial shape makes every write an insert strictly below the current
// minimum, the worst case for any cached end state. The op schedule and the
// tree evolution are identical across policies (every policy pops the true
// minimum), so the three columns are exactly comparable.
func runInterleave(ents []structs.Entry, ops, popPct int, adversarial bool, pol policy, kg *keygen, k int) interleaveResult {
	n := len(ents)
	t := structs.BulkLoad(ents)
	cache := runCache{k: k}
	rng := xorshift(0x2545f4914f6cdd1d ^ uint64(popPct))
	var stack []uint64
	insFlip := false
	advNext := ents[0].Score // adversarial inserts descend from below the build minimum
	var churnRef uint32 = 1 << 30
	var res interleaveResult

	chunk := 50_000
	if chunk > ops {
		chunk = ops
	}
	done := 0
	var dur time.Duration
	for done < ops {
		round := ops - done
		if round > chunk {
			round = chunk
		}
		start := time.Now()
		for i := 0; i < round; i++ {
			if int(rng.next()%100) < popPct {
				if pol == naive {
					popMin(t)
				} else {
					cache.pop(t)
				}
				continue
			}
			doInsert := adversarial || insFlip || len(stack) == 0
			insFlip = !insFlip
			if doInsert {
				var key uint64
				if adversarial {
					advNext--
					key = advNext
				} else {
					key = kg.key()
				}
				churnRef++
				t.Insert(key, nil, churnRef, noMembers{})
				stack = append(stack, key)
				if pol != naive {
					cache.observeInsert(key, churnRef, pol == absorb)
				}
				continue
			}
			j := int(rng.next() % uint64(len(stack)))
			key := stack[j]
			stack[j] = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if _, ok := t.Delete(key, nil, noMembers{}); ok {
				if pol != naive {
					cache.observeDelete(key, pol == absorb)
				}
			} else {
				res.misses++ // the churn key was already popped as a minimum
			}
		}
		dur += time.Since(start)
		done += round
		// Refill to n outside the timer so cardinality holds across the run;
		// the refill is identical across policies and the cache just drops.
		if t.Len() < n*9/10 {
			for t.Len() < n {
				key := kg.key()
				churnRef++
				t.Insert(key, nil, churnRef, noMembers{})
				stack = append(stack, key)
			}
			if pol != naive {
				cache.drop(false)
			}
		}
	}
	res.nsOp = float64(dur.Nanoseconds()) / float64(ops)
	res.reprimes = cache.reprimes
	res.invals = cache.invals
	return res
}

// quantiles sorts per-op samples and reads p50, p99 and the max, in ns.
func quantiles(samples []int64) (p50, p99, maxNs int64) {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	n := len(samples)
	return samples[n/2], samples[n*99/100], samples[n-1]
}

// sampleOps times each op of a pop-shaped kernel individually over a fresh
// bulk-loaded tree, rounds capped at half the cardinality. reset clears any
// arm-side cached state after each rebuild, nil when the arm carries none.
func sampleOps(ents []structs.Entry, ops int, reset func(), op func(t *structs.Tree, i int)) []int64 {
	n := len(ents)
	perBuild := n / 2
	if perBuild < 1 {
		perBuild = 1
	}
	samples := make([]int64, 0, ops)
	for len(samples) < ops {
		t := structs.BulkLoad(ents)
		if reset != nil {
			reset()
		}
		round := ops - len(samples)
		if round > perBuild {
			round = perBuild
		}
		for i := 0; i < round; i++ {
			start := time.Now()
			op(t, i)
			samples = append(samples, time.Since(start).Nanoseconds())
		}
	}
	return samples
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinalities and op counts")
	flag.Parse()

	cards := []int{1_000, 100_000, 1_000_000, 4_000_000}
	ops := 200_000
	sampleN := 100_000
	interOps := 400_000
	if *quick {
		cards = []int{1_000, 100_000}
		ops = 20_000
		sampleN = 20_000
		interOps = 40_000
	}
	kg := &keygen{}

	fmt.Printf("end-leaf caching sweep, %s\n", time.Now().Format("2006-01-02"))
	fmt.Println("tree: engine/f3/struct at the frozen 256B/512B/u32 geometry, distinct scores, nil members")

	// Table 1: pure end pops across cardinality.
	fmt.Printf("\npure pops, ns/op (ops=%d per cell)\n", ops)
	fmt.Printf("%9s %8s %8s %8s %8s %8s\n", "card", "sel0", "popmin", "popmax", "run31", "remRand")
	entsBy := map[int][]structs.Entry{}
	scoresBy := map[int][]uint64{}
	for _, n := range cards {
		scores := make([]uint64, n)
		for i := range scores {
			scores[i] = kg.key()
		}
		ents := sortedEntries(scores)
		entsBy[n] = ents
		scoresBy[n] = scores

		sel := selectCell(ents, ops)
		minNs := pureCell(ents, ops, nil, popMin)
		maxNs := pureCellDescending(ents, ops)
		run := runCache{k: 31}
		runNs := pureCell(ents, ops, func() { run.drop(false) }, run.pop)
		rem := remCell(ents, scores, ops)
		fmt.Printf("%9d %8.1f %8.1f %8.1f %8.1f %8.1f\n", n, sel, minNs, maxNs, runNs, rem)
	}

	// Table 2: batch drain at 1M.
	bn := 1_000_000
	if *quick {
		bn = 100_000
	}
	fmt.Printf("\nbatch drain at card=%d, ns per popped entry (ops=%d per cell)\n", bn, ops)
	fmt.Printf("%6s %8s %8s %8s\n", "count", "prime", "delete", "total")
	for _, c := range []int{1, 2, 4, 8, 16, 31, 62, 124} {
		p, d := batchCell(entsBy[bn], ops, c)
		fmt.Printf("%6d %8.1f %8.1f %8.1f\n", c, p, d, p+d)
	}

	// Table 3: interleaved pops and writes at 1M, cache k=31.
	fmt.Printf("\ninterleave at card=%d, ns/op (ops=%d per cell, cache k=31)\n", bn, interOps)
	fmt.Printf("%-14s %8s %8s %8s %12s %12s\n", "workload", "naive", "inval", "absorb", "invals/1k", "reprimes/1k")
	type workload struct {
		name   string
		popPct int
		adv    bool
	}
	for wi, w := range []workload{
		{"pop90/wr10", 90, false},
		{"pop50/wr50", 50, false},
		{"pop10/wr90", 10, false},
		{"adv pop50/ins50", 50, true},
	} {
		ents := entsBy[bn]
		if w.adv {
			// A sequential build keeps headroom below the minimum for the
			// descending adversarial inserts.
			seq := make([]structs.Entry, bn)
			base := uint64(1) << 40
			for i := range seq {
				seq[i] = structs.Entry{Score: base + uint64(i), Ref: uint32(i)}
			}
			ents = seq
		}
		var rows [3]interleaveResult
		for i, pol := range []policy{naive, invalidate, absorb} {
			// Each policy gets the same key stream: a per-workload counter
			// range disjoint from the build keys, restarted per arm, so the
			// tree evolves identically and the columns are exactly comparable.
			kgw := &keygen{next: uint64(1)<<32 + uint64(wi)<<28}
			rows[i] = runInterleave(ents, interOps, w.popPct, w.adv, pol, kgw, 31)
		}
		// The op schedule is deterministic and every policy pops the true
		// minimum, so the tree evolves identically across the three arms; a
		// diverging churn-delete miss count would mean they were not comparable.
		if rows[0].misses != rows[1].misses || rows[1].misses != rows[2].misses {
			panic("policies diverged on the same schedule")
		}
		perK := float64(interOps) / 1000
		fmt.Printf("%-14s %8.1f %8.1f %8.1f %7.2f/%4.2f %7.2f/%4.2f\n",
			w.name, rows[0].nsOp, rows[1].nsOp, rows[2].nsOp,
			float64(rows[1].invals)/perK, float64(rows[2].invals)/perK,
			float64(rows[1].reprimes)/perK, float64(rows[2].reprimes)/perK)
	}

	// Table 4: per-op latency quantiles, the p99 shoulder read.
	fmt.Printf("\nper-op latency quantiles, ns (samples=%d, per-op timer costs ~20-30ns)\n", sampleN)
	fmt.Printf("%-16s %8s %8s %8s\n", "arm", "p50", "p99", "max")
	qCards := []int{bn, 4_000_000}
	if *quick {
		qCards = []int{bn}
	}
	for _, n := range qCards {
		ents := entsBy[n]
		s := sampleOps(ents, sampleN, nil, func(t *structs.Tree, _ int) { popMin(t) })
		p50, p99, mx := quantiles(s)
		fmt.Printf("popmin@%-9d %8d %8d %8d\n", n, p50, p99, mx)
		run := runCache{k: 31}
		s = sampleOps(ents, sampleN, func() { run.drop(false) }, func(t *structs.Tree, _ int) { run.pop(t) })
		p50, p99, mx = quantiles(s)
		fmt.Printf("run31@%-10d %8d %8d %8d\n", n, p50, p99, mx)
	}
	scores := scoresBy[bn]
	s := sampleOps(entsBy[bn], sampleN, nil, func(t *structs.Tree, i int) {
		if _, ok := t.Delete(scores[i], nil, noMembers{}); !ok {
			panic("random member not deletable")
		}
	})
	p50, p99, mx := quantiles(s)
	fmt.Printf("remRand@%-8d %8d %8d %8d\n", bn, p50, p99, mx)
}
