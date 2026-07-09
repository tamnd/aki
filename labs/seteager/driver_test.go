package seteager

import (
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// This file is the slice-4 driver sweep (spec 2064/f1_rewrite_ltm/24 section 7). order_test.go
// settled the container (per-partition sorted []uint64); this settles the two driver knobs the
// adaptive merge-vs-probe path selects on, each as a measured crossover rather than a guess:
//
//   - the merge-vs-probe crossover k. The driver merges two same-P sorted arrays when the sources
//     are large and comparable and probes off the smaller source when one is far smaller. k is the
//     size ratio |large|/|small| at which the probe (O(|small|) random lookups into the larger set)
//     overtakes the merge (O(|large|+|small|) sequential streaming). Section 7 pre-registers k ~ 7
//     from the two per-member costs (~40 ns probe, ~5.8 ns merge); the sweep refines it.
//   - the parallel fan-out floor. Fanning the P partition merges across workers has a fixed
//     dispatch cost per merge; below some per-partition element count the single-goroutine sweep
//     over all P partitions beats the fan-out. The floor is the per-partition element count where
//     the two cross.
//
// The third section-7 knob, the fold catch-up threshold, is a write-then-read property of the live
// async folder rather than a container model, so it is settled against the engine's folder, not in
// this plain-Go lab. This file models only the two read-path crossovers the driver selects on from
// the operands' sizes and P.
//
// # What the numbers say (Apple M4, GOMAXPROCS=10)
//
// Merge vs probe as the large source outgrows the small one, |large| = 1<<20 fixed, |small| shrunk
// by the ratio (ns/op for the whole intersection; the merge two-pointers the two sorted arrays, the
// probe walks the small source into the large source's already-built hash set, since the real engine
// keeps that index resident and does not rebuild it per call):
//
//	ratio    merge     probe    winner
//	   1    5.73 ms   9.61 ms   merge   (comparable: streaming dominates)
//	   2    4.21 ms   5.00 ms   merge
//	   4    2.33 ms   2.54 ms   merge
//	   7    1.63 ms   1.56 ms   probe   (crossover near here)
//	   8    1.53 ms   1.40 ms   probe
//	  16    1.10 ms   0.67 ms   probe
//	  32    0.89 ms   0.28 ms   probe
//	  64    0.81 ms   0.10 ms   probe   (asymmetric: probe bounded by tiny small)
//
// The crossover sits between ratio 4 (merge ahead by ~8%) and ratio 7 (probe ahead by ~4%), near 6,
// which validates section 7's pre-registered k ~ 7: probe wins once the large source is ~7x the
// small one. The settled default is k = 7. It is conservative on top of the single-thread number
// because the real merge fans across P workers while the doc-20 probe runs single-threaded off the
// small source, so the parallel merge's true crossover sits at or past the single-thread 6-7; k = 7
// keeps the merge only where even its single-thread form is within noise of the probe and its
// parallel form is a clear win. The existing setMergeMaxRatio = 8 is within noise of this; the
// driver sub-PR sets it to the lab number 7.
//
// Parallel fan-out vs single-goroutine sweep of P=64 partition merges, by per-partition element
// count c (both operands c members per partition; ns/op for all 64 partition merges):
//
//	c        single      parallel   winner
//	   16    2.87 us      10.1 us    single    (dispatch dwarfs 64 tiny merges)
//	   64    11.4 us      11.1 us    even      (break-even near here)
//	  256    75.1 us      23.2 us    parallel  (3.2x)
//	 1024    352 us       77.2 us    parallel  (4.6x)
//	 4096    1.22 ms      234 us     parallel  (5.2x)
//	16384    5.41 ms      790 us     parallel  (6.8x)
//
// The fan-out breaks even near c = 64 members per partition: below it the goroutine dispatch for the
// 64 merges outweighs the merges themselves, above it the parallelism pays and by c = 16384 it is
// ~6.8x on this 10-core box. The settled floor default is 128 members per partition, one doubling
// above break-even so the driver fans only where the parallelism is a clear win and not a coin-flip
// that adds goroutine variance for no gain. Below the floor the merge runs on the calling goroutine
// (fanPartitions already collapses to inline for a single worker, so the floor gates whether the
// driver asks for more than one worker at all).
//
// The real code these inform is aki/f1srv/set_algebra.go: setMergeMaxRatio (the k crossover, to be
// set to 7) and a new per-partition fan-out floor (128) the merge driver checks before it fans
// across shard workers.
//
// Numbers observed on an Apple M4 (GOMAXPROCS=10); re-run to reproduce on yours.

// asymHashes returns a large set of `large` member hashes and a small set of `small` that overlaps
// the large one in its tail by half the small set, the asymmetric SINTER shape the crossover sweep
// intersects. Overlapping only the tail keeps real intersection work in the merge without making the
// small set a subset the probe short-circuits.
func asymHashes(large, small int) (big, sm []uint64) {
	all := memberHashes(large + small)
	big = append([]uint64(nil), all[:large]...)
	start := max(large-small/2, 0)
	sm = append([]uint64(nil), all[start:start+small]...)
	return big, sm
}

// probeSmallIntoLarge counts the intersection by probing each of the small source's members into the
// large source's already-built hash set, the doc-20 probe-off-the-smaller-source path the driver
// keeps for the asymmetric case. The table is passed in, not built here: the real engine keeps a
// persistent composite hash index the probe reuses, so charging the table build per call would be as
// unfair as charging the merge a per-call sort. Its cost tracks |small|, so it wins once the large
// source is many times the small one.
func probeSmallIntoLarge(t *probeSet, sm []uint64) int {
	n := 0
	for _, h := range sm {
		if t.has(h) {
			n++
		}
	}
	return n
}

// ratios is the |large|/|small| sweep for the merge-vs-probe crossover. It brackets the
// pre-registered k ~ 7 on both sides so the crossover falls inside the measured band.
var ratios = []int{1, 2, 4, 7, 8, 16, 32, 64}

// BenchmarkCrossoverMerge merges two same-P-ordered sorted arrays at each size ratio, the read the
// driver runs when it judges the sources comparable. The large source is fixed at labN and the small
// one shrinks by the ratio, so the reported ns/op is the merge cost the crossover compares against
// the probe at the same ratio.
func BenchmarkCrossoverMerge(b *testing.B) {
	for _, r := range ratios {
		b.Run("ratio="+strconv.Itoa(r), func(b *testing.B) {
			big, sm := asymHashes(labN, labN/r)
			slices.Sort(big)
			slices.Sort(sm)
			for b.Loop() {
				sink = mergeSorted(big, sm)
			}
		})
	}
}

// BenchmarkCrossoverProbe probes the small source into the large one at each size ratio, the read the
// driver runs when it judges the sources asymmetric. The crossover k is the ratio where this
// overtakes BenchmarkCrossoverMerge.
func BenchmarkCrossoverProbe(b *testing.B) {
	for _, r := range ratios {
		b.Run("ratio="+strconv.Itoa(r), func(b *testing.B) {
			big, sm := asymHashes(labN, labN/r)
			t := newProbeSet(big)
			for b.Loop() {
				sink = probeSmallIntoLarge(t, sm)
			}
		})
	}
}

// buildPartPair builds two P-partition sets whose partitions each hold about c overlapping members,
// the same-P shape the fan-out sweep merges. Each partition's two arrays are sorted so the merge is
// a clean two-pointer pass, isolating the dispatch-vs-work question the floor answers.
func buildPartPair(p, c int) (a, bset *partedSet) {
	a, bset = newPartedSet(p), newPartedSet(p)
	total := p * c
	big, sm := asymHashes(total, total)
	for _, h := range big {
		i := int(h & uint64(p-1))
		a.parts[i] = append(a.parts[i], h)
	}
	for _, h := range sm {
		i := int(h & uint64(p-1))
		bset.parts[i] = append(bset.parts[i], h)
	}
	for i := range p {
		slices.Sort(a.parts[i])
		slices.Sort(bset.parts[i])
	}
	return a, bset
}

// mergeSequential runs the P partition-pair merges on the calling goroutine, the single-worker path
// the driver collapses to below the fan-out floor.
func mergeSequential(a, bset *partedSet) int {
	n := 0
	for i := range a.p {
		n += mergeSorted(a.parts[i], bset.parts[i])
	}
	return n
}

// mergeParallel runs the P partition-pair merges across `workers` goroutines pulling partitions off
// a shared counter, mirroring f1srv's fanPartitions. It is the path whose per-merge dispatch cost the
// fan-out floor weighs against the sequential sweep.
func mergeParallel(a, bset *partedSet, workers int) int {
	var total atomic.Int64
	var next atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= a.p {
					return
				}
				total.Add(int64(mergeSorted(a.parts[i], bset.parts[i])))
			}
		}()
	}
	wg.Wait()
	return int(total.Load())
}

// perPartCounts is the per-partition element sweep for the fan-out floor, spanning the tiny
// partitions where dispatch dominates to the large ones where the parallelism pays.
var perPartCounts = []int{16, 64, 256, 1024, 4096, 16384}

// BenchmarkFanoutSingle merges P=64 partition pairs on one goroutine at each per-partition count, the
// baseline the fan-out has to beat.
func BenchmarkFanoutSingle(b *testing.B) {
	const p = 64
	for _, c := range perPartCounts {
		b.Run("c="+strconv.Itoa(c), func(b *testing.B) {
			a, bset := buildPartPair(p, c)
			for b.Loop() {
				sink = mergeSequential(a, bset)
			}
		})
	}
}

// BenchmarkFanoutParallel merges the same P=64 partition pairs across P workers at each per-partition
// count. The floor is the c where this overtakes BenchmarkFanoutSingle.
func BenchmarkFanoutParallel(b *testing.B) {
	const p = 64
	for _, c := range perPartCounts {
		b.Run("c="+strconv.Itoa(c), func(b *testing.B) {
			a, bset := buildPartPair(p, c)
			for b.Loop() {
				sink = mergeParallel(a, bset, p)
			}
		})
	}
}

// TestDriverModelsAgree checks the probe and both merge sweeps report the same intersection count on
// a small asymmetric fixture, so a benchmark that is fast because it is wrong gets caught. It guards
// the crossover models the way TestContainersAgree guards the container models.
func TestDriverModelsAgree(t *testing.T) {
	const large, small = 8192, 512
	big, sm := asymHashes(large, small)

	fbig := append([]uint64(nil), big...)
	fsm := append([]uint64(nil), sm...)
	slices.Sort(fbig)
	slices.Sort(fsm)
	want := mergeSorted(fbig, fsm)

	if got := probeSmallIntoLarge(newProbeSet(big), sm); got != want {
		t.Fatalf("probe = %d, want %d", got, want)
	}

	const p = 64
	pa, pb := newPartedSet(p), newPartedSet(p)
	for _, h := range big {
		pa.insert(h)
	}
	for _, h := range sm {
		pb.insert(h)
	}
	if got := mergeSequential(pa, pb); got != want {
		t.Fatalf("sequential fan = %d, want %d", got, want)
	}
	if got := mergeParallel(pa, pb, p); got != want {
		t.Fatalf("parallel fan = %d, want %d", got, want)
	}
}
