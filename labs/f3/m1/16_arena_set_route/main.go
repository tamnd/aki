// Lab m1/16: the tiny-set arena-route throughput cost (spec 2064/f3,
// keyspace-unification arc, slice 2, the routing flip).
//
// Slice 1 (lab m1/15) proved the storage footprint: a tiny set inline in one
// arena record clears the memory bar by construction. This slice wires the set
// command funnel onto that record. The routing changes the per-command shape a
// tiny set pays:
//
//   - the old home keeps the set live in the Go-heap registry map g.m, so a
//     command is a map lookup and an in-place mutation of a struct that stays
//     put between commands;
//   - the arena home keeps the set inline in a store record, so a command must
//     resolve it out of the record (PeekCollBlob, then copy the blob into the
//     reusable scratch the way resolveTouch/loadInline do), mutate the copy, and
//     write it back (PutCollBlob, the commit republish).
//
// The record round-trip is the price the memory win charges on the write path:
// one peek, one small-blob copy, one republish per mutating command, versus the
// map's in-place edit. This lab prices that overhead against the real
// engine/f3/store code so the routing is landed only if the per-command cost is
// a bounded small absolute figure a tiny set can afford, not an open-ended
// regression.
//
// The route arm drives the real store: PeekCollBlob to resolve, a blob copy to
// stand in for the loadInline into scratch, the member add, and PutCollBlob to
// commit. The wall arm drives a real Go map of the same set shape with an
// in-place append. Both run the same SADD-of-one-member cycle over a population
// of distinct tiny sets so the working set does not fit a single cache line and
// the numbers reflect the gate's 2M-set spread, not a hot single key.
//
// Read: ns per mutating command for the wall (map, in place) and the route
// (arena, record round-trip), and the route's absolute overhead per command.
// The memory bar (lab 15) is the reason to pay it; this lab bounds the bill.
// See engine/f3/set/reg.go commit and resolveTouch, engine/f3/store/collblob.go.
package main

import (
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

// kindSet is the store's set collection-kind byte, store.KindSet, spelled out so
// the lab reads the same discriminant the engine writes.
const kindSet = 0x02

// oneMemberBlob packs a single member as the engine's listpack does:
// [len][tag][bytes], the tiny-set shape the gate cell builds.
func oneMemberBlob(m []byte) []byte {
	var tag byte
	if len(m) > 0 {
		tag = m[0]
	}
	b := make([]byte, 0, len(m)+2)
	b = append(b, byte(len(m)), tag)
	b = append(b, m...)
	return b
}

// key builds a fixed-width per-set key so every record charges the same aligned
// key bytes and the population spreads across the index the way the gate's 2M
// distinct sets do.
func key(i int) []byte { return fmt.Appendf(nil, "set:%08d", i) }

// wallSet mirrors the tiny-set live shape the g.m home keeps: an encoding tag and
// the packed blob, mutated in place. Only the fields a one-member SADD cycle
// touches are modeled; the point is the in-place edit the map home allows.
type wallSet struct {
	enc  uint8
	data []byte
}

// wallRoute times the g.m home's per-command cost: a map lookup and an in-place
// append to the resolved set's blob, the shape a SADD over a registry-homed set
// runs. It cycles over count distinct sets so the map and the blobs span the same
// spread the gate builds. It returns nanoseconds per command.
func wallRoute(count, iters int, member []byte) float64 {
	m := make(map[string]*wallSet, count)
	blob := oneMemberBlob(member)
	for i := range count {
		b := make([]byte, len(blob))
		copy(b, blob)
		m[string(key(i))] = &wallSet{enc: 1, data: b}
	}

	start := time.Now()
	for it := 0; it < iters; it++ {
		k := key(it % count)
		s := m[string(k)]
		// The in-place mutation a map-homed set allows: append the member's bytes to
		// the resolved blob with no write-back, since the struct stays live in g.m.
		s.data = append(s.data, member...)
		// Keep the blob from growing unbounded across iters so the working-set size
		// stays the gate's tiny-set shape; trim back to the one-member length.
		s.data = s.data[:len(blob)]
	}
	return float64(time.Since(start).Nanoseconds()) / float64(iters)
}

// arenaRoute times the arena home's per-command cost against the real store: the
// PeekCollBlob resolve, a copy of the record blob into a scratch buffer (the
// loadInline resolveTouch does), the member add, and the PutCollBlob commit
// republish. It cycles over count distinct records so the peeks and republishes
// span the index the gate builds. It returns nanoseconds per command.
func arenaRoute(count, iters int, member []byte) float64 {
	blob := oneMemberBlob(member)
	arenaBytes := count*96 + (32 << 20)
	s := store.New(arenaBytes, 1<<20)
	for i := range count {
		if err := s.PutCollBlob(key(i), kindSet, 1, blob, 0, 0); err != nil {
			panic(err)
		}
	}

	scratch := make([]byte, 0, 64)
	start := time.Now()
	for it := 0; it < iters; it++ {
		k := key(it % count)
		// resolveTouch: peek the record and materialize the blob into scratch.
		rec, _, bits, _, ok := s.PeekCollBlob(k)
		if !ok {
			panic("arena route: record missing mid-run")
		}
		scratch = append(scratch[:0], rec...)
		// The member add on the resolved copy, then trim back to the one-member shape
		// so the republished record stays the gate's tiny-set size across iters.
		scratch = append(scratch, member...)
		scratch = scratch[:len(blob)]
		// commit: republish the record in place with the mutated blob.
		if err := s.PutCollBlob(k, kindSet, bits, scratch, 0, 0); err != nil {
			panic(err)
		}
	}
	return float64(time.Since(start).Nanoseconds()) / float64(iters)
}

func median(xs []float64) float64 {
	sort.Float64s(xs)
	return xs[len(xs)/2]
}

func main() {
	reps := flag.Int("reps", 5, "reps per cell, median reported")
	iters := flag.Int("iters", 2_000_000, "mutating commands per rep")
	flag.Parse()

	member := []byte("hello")
	counts := []int{100_000, 500_000, 1_000_000, 2_000_000}

	fmt.Printf("tiny-set arena-route per-command cost: SADD-of-one cycle, member %q\n", member)
	fmt.Printf("%d mutating commands per rep, %d reps, median reported\n\n", *iters, *reps)
	fmt.Printf("%10s  %14s  %14s  %14s\n",
		"count", "wall ns/cmd", "arena ns/cmd", "arena overhead")
	for _, c := range counts {
		wallReps := make([]float64, *reps)
		arenaReps := make([]float64, *reps)
		for r := range *reps {
			wallReps[r] = wallRoute(c, *iters, member)
			arenaReps[r] = arenaRoute(c, *iters, member)
		}
		w := median(wallReps)
		a := median(arenaReps)
		fmt.Printf("%10d  %14.1f  %14.1f  %+13.1f\n", c, w, a, a-w)
	}

	fmt.Printf("\nThe arena route pays one record peek, one tiny-blob copy, and one republish\n")
	fmt.Printf("per mutating command over the map's in-place edit. Lab 15 is why the trade is\n")
	fmt.Printf("worth it: the memory bar clears at <=0.5x the wall. See reg.go commit.\n")
}
