// Lab: the set struct union footprint (spec 2064/f3 doc 11 section 3, M1 lab 12).
//
// The question, from the per-collection memory-bar slim: a set holds exactly one
// of two mutually exclusive inline representations at a time, the intset-class
// sorted []int64 or the listpack-class packed []byte. The original struct carried
// a separate field for each, so every set paid a dead 24-byte slice header for the
// shape it was not, whatever its shape. Unioning the two into a single []byte
// (the intset packs its members as little-endian int64 lanes, the listpack keeps
// its byte format) removes one slice header. The slice tests confirm the struct
// drops from 104 to 80 bytes, but the number that moves the memory bar is not the
// struct size, it is the heap size class the Go allocator rounds that struct up
// to: 104 bytes rounds to the 112 class, 80 bytes lands exactly on the 80 class.
// This lab prices the real per-set heap footprint of the two shapes across the
// count sweep, so the class crossover is a measured win and not an inferred one.
//
// The single-member set is the exact workload that fails the memory bar worst
// (the collection-point-read gate: 1M single-member sets at about 4x the rivals'
// peak), because there is no member data to amortize the fixed per-set cost
// against, so the struct's own size class is nearly the whole footprint. That is
// the cell this lab weighs.
//
// Method: in-process, no server, no wire. The lab defines both struct shapes,
// the pre-union oldSet with ints []int64 plus blob []byte and the unioned newSet
// with a single data []byte, allocates count of each as a single-member listpack
// set (the failing-workload shape), and reads runtime.MemStats.HeapAlloc before
// and after to get the true bytes the allocator charged per set, size class and
// backing blob included. GC is forced and pinned so the delta is the live set of
// the structs alone. Reps are run back to back and the median reported.
//
// Read: bytes/set for each shape across the sweep, and the delta. The delta is
// the memory the union returns to every set on the box, which for 1M sets is the
// figure that walks the single-member peak toward the bar. See README.md.
package main

import (
	"flag"
	"fmt"
	"runtime"
	"sort"
	"unsafe"
)

// oldSet is the pre-union set struct: the two inline representations each carry
// their own slice header, so a listpack set pays for the nil ints header and an
// intset set pays for the nil blob header. Only the representation fields matter
// to the footprint question, so the cold-tier and table fields are reproduced at
// their true widths to keep the struct size and its size class honest.
type oldSet struct {
	enc      uint8
	expireAt int64
	ints     []int64
	blob     []byte
	n        int
	ht       unsafe.Pointer
	part     unsafe.Pointer
	acct     uint64
	cold     unsafe.Pointer
}

// newSet is the unioned struct: one data []byte holds whichever inline shape is
// live, so a set carries a single representation header, not one per shape.
type newSet struct {
	enc      uint8
	expireAt int64
	data     []byte
	n        int
	ht       unsafe.Pointer
	part     unsafe.Pointer
	acct     uint64
	cold     unsafe.Pointer
}

// oneMemberBlob is the packed listpack for a single member: [len][tag][bytes].
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

// heapPerOld allocates count single-member oldSets and returns bytes/set charged
// by the allocator, the struct plus its backing blob at their real size classes.
func heapPerOld(count int, member []byte) float64 {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	sets := make([]*oldSet, count)
	for i := range sets {
		sets[i] = &oldSet{enc: 1, blob: oneMemberBlob(member), n: 1}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	got := float64(after.HeapAlloc-before.HeapAlloc) / float64(count)
	runtime.KeepAlive(sets)
	return got
}

// heapPerNew is the same measurement for the unioned struct.
func heapPerNew(count int, member []byte) float64 {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	sets := make([]*newSet, count)
	for i := range sets {
		sets[i] = &newSet{enc: 1, data: oneMemberBlob(member), n: 1}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	got := float64(after.HeapAlloc-before.HeapAlloc) / float64(count)
	runtime.KeepAlive(sets)
	return got
}

func median(xs []float64) float64 {
	sort.Float64s(xs)
	return xs[len(xs)/2]
}

func main() {
	reps := flag.Int("reps", 5, "reps per cell, median reported")
	flag.Parse()

	member := []byte("hello") // the collection-point-read gate's single member
	counts := []int{100_000, 500_000, 1_000_000, 2_000_000}

	fmt.Printf("set struct union footprint: single-member listpack sets, member %q\n", member)
	fmt.Printf("sizeof oldSet = %d bytes, sizeof newSet = %d bytes\n\n",
		unsafe.Sizeof(oldSet{}), unsafe.Sizeof(newSet{}))
	fmt.Printf("%10s  %12s  %12s  %10s  %14s\n", "count", "old B/set", "new B/set", "saved", "saved (count)")
	for _, c := range counts {
		oldReps := make([]float64, *reps)
		newReps := make([]float64, *reps)
		for r := 0; r < *reps; r++ {
			oldReps[r] = heapPerOld(c, member)
			newReps[r] = heapPerNew(c, member)
		}
		o := median(oldReps)
		n := median(newReps)
		saved := o - n
		fmt.Printf("%10d  %12.1f  %12.1f  %10.1f  %12.1f MB\n",
			c, o, n, saved, saved*float64(c)/(1024*1024))
	}

	fmt.Printf("\nstruct 104 -> 80 bytes; Go size class 112 -> 80; expected ~32 B/set returned per set\n")
}
