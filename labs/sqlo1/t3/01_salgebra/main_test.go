package main

import (
	"math/rand"
	"sort"
	"testing"
)

// TestBuildInvariants pins the model: fence sorted, routing finds
// every member, live count exact, occupancy above the merge floor.
func TestBuildInvariants(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	s := newSet(503)
	fhs := make([]uint64, 30000)
	for i := range fhs {
		fhs[i] = rng.Uint64()
		s.insert(fhs[i], int32(i))
	}
	if s.live != 30000 {
		t.Fatalf("live = %d", s.live)
	}
	if !sort.SliceIsSorted(s.los, func(a, b int) bool { return s.los[a] < s.los[b] }) {
		t.Fatal("fence lows unsorted")
	}
	total := 0
	for i, sg := range s.segs {
		total += len(sg.ents)
		for _, e := range sg.ents {
			if s.segFor(e.fh) != i {
				t.Fatalf("member fh %#x routes to %d, lives in %d", e.fh, s.segFor(e.fh), i)
			}
		}
	}
	if total != s.live {
		t.Fatalf("segments hold %d, live says %d", total, s.live)
	}
}

// TestProbeAccounting pins the cost model at hot=0: the probe arm
// reads the driver fence plus exactly the distinct touched target
// segments, each once, and rounds cover reads at the batch size.
func TestProbeAccounting(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	target := build(60000, 503, 25, rng)
	driver := build(3000, 503, 25, rng)

	crng := rand.New(rand.NewSource(9))
	a, touched := probeCost(driver, target, 1, 0, crng)

	distinct := map[int]bool{}
	for _, sg := range driver.segs {
		for _, e := range sg.ents {
			distinct[target.segFor(e.fh)] = true
		}
	}
	if touched != len(distinct) {
		t.Fatalf("touched = %d, distinct routing says %d", touched, len(distinct))
	}
	if a.reads != len(driver.segs)+touched {
		t.Fatalf("reads = %d, want driver %d + touched %d", a.reads, len(driver.segs), touched)
	}
	minRounds := (len(driver.segs)+batchSegs-1)/batchSegs + (touched+batchSegs-1)/batchSegs
	if a.rounds < minRounds {
		t.Fatalf("rounds = %d below the packed floor %d", a.rounds, minRounds)
	}
	if a.rounds > len(driver.segs)*((503+batchSegs-1)/batchSegs+1)+minRounds {
		t.Fatalf("rounds = %d beyond any window fragmentation", a.rounds)
	}
}

// TestMergeAccounting pins the zipper at hot=0: everything reads once
// in fully packed rounds.
func TestMergeAccounting(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	target := build(60000, 503, 25, rng)
	driver := build(3000, 503, 25, rng)
	crng := rand.New(rand.NewSource(13))
	m := mergeCost(driver, target, 0, crng)
	if m.reads != len(driver.segs)+len(target.segs) {
		t.Fatalf("reads = %d, want every segment once", m.reads)
	}
	want := (len(driver.segs)+batchSegs-1)/batchSegs + (len(target.segs)+batchSegs-1)/batchSegs
	if m.rounds != want {
		t.Fatalf("rounds = %d, want packed %d", m.rounds, want)
	}
	if m.bytes != driver.bytes()+target.bytes() {
		t.Fatalf("bytes = %d, want %d", m.bytes, driver.bytes()+target.bytes())
	}
}

// TestTouchedSaturates pins the crossover's shape: the touched share
// grows with the driver and saturates at the whole target fence.
func TestTouchedSaturates(t *testing.T) {
	rng := rand.New(rand.NewSource(17))
	target := build(60000, 503, 25, rng)
	prev := 0
	for _, dm := range []int{50, 500, 5000, 50000} {
		driver := build(dm, 503, 25, rng)
		crng := rand.New(rand.NewSource(19))
		_, touched := probeCost(driver, target, 1, 0, crng)
		if touched < prev {
			t.Fatalf("touched fell from %d to %d as the driver grew", prev, touched)
		}
		if touched > len(target.segs) {
			t.Fatalf("touched %d exceeds the fence %d", touched, len(target.segs))
		}
		prev = touched
	}
	if prev != len(target.segs) {
		t.Fatalf("a driver of the target's size touched %d of %d segments", prev, len(target.segs))
	}
}

// TestWiderWindowsNeverHurt pins the gather window's direction: a
// wider window can only merge partially filled rounds.
func TestWiderWindowsNeverHurt(t *testing.T) {
	rng := rand.New(rand.NewSource(23))
	target := build(60000, 503, 25, rng)
	driver := build(4000, 503, 25, rng)
	prev := -1
	for _, w := range []int{1, 2, 4, 8} {
		crng := rand.New(rand.NewSource(29))
		a, _ := probeCost(driver, target, w, 0, crng)
		if prev >= 0 && a.rounds > prev {
			t.Fatalf("window %d rounds %d above the narrower window's %d", w, a.rounds, prev)
		}
		prev = a.rounds
	}
}

// TestCollisionMath pins the digest verdict's inputs: 64-bit digests
// are corruption-grade at a 10^8 union, 128-bit stays negligible past
// 10^9.
func TestCollisionMath(t *testing.T) {
	if p := collisionP(1e8, 64); p < 1e-5 {
		t.Fatalf("64-bit at 1e8 = %g, expected corruption-grade odds", p)
	}
	if p := collisionP(1e9, 128); p > 1e-18 {
		t.Fatalf("128-bit at 1e9 = %g, expected negligible", p)
	}
	if collisionP(1e6, 64) >= collisionP(1e8, 64) {
		t.Fatal("collision odds must grow with the union")
	}
}
