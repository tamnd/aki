package main

import (
	"math/rand"
	"sort"
	"testing"
)

// TestArityByWidth pins the interior arity the count width buys at the frozen
// 256-byte branch: a child costs 8 (separator) + 4 (ordinal) + w (count) bytes,
// so a wider count fits fewer children.
func TestArityByWidth(t *testing.T) {
	cases := []struct {
		countW int
		arity  int
	}{
		{2, 18},
		{4, 16},
		{8, 12},
	}
	for _, c := range cases {
		tr := newTree(fixedBranchSz, fixedLeafSz, c.countW)
		if tr.arity != c.arity {
			t.Fatalf("countW %d: arity %d, want %d", c.countW, tr.arity, c.arity)
		}
	}
}

// TestWidthCeilings checks the structural limit each width imposes: a count at
// the width's ceiling round-trips, and a count one past it truncates, which is
// the silent corruption that disqualifies a width once a subtree grows past it.
func TestWidthCeilings(t *testing.T) {
	cases := []struct {
		countW int
		ceil   uint64
	}{
		{2, 1<<16 - 1},
		{4, 1<<32 - 1},
	}
	for _, c := range cases {
		tr := newTree(fixedBranchSz, fixedLeafSz, c.countW)
		o := tr.allocBranch()
		tr.bSetCount(o, 0, c.ceil)
		if got := tr.bCount(o, 0); got != c.ceil {
			t.Fatalf("countW %d: ceiling %d round-tripped as %d", c.countW, c.ceil, got)
		}
		tr.bSetCount(o, 0, c.ceil+1)
		if got := tr.bCount(o, 0); got == c.ceil+1 {
			t.Fatalf("countW %d: value past ceiling stored intact, expected truncation", c.countW)
		}
	}
	// u64 holds any count the collection cap can produce.
	tr := newTree(fixedBranchSz, fixedLeafSz, 8)
	o := tr.allocBranch()
	big := uint64(1) << 40
	tr.bSetCount(o, 0, big)
	if got := tr.bCount(o, 0); got != big {
		t.Fatalf("u64: %d round-tripped as %d", big, got)
	}
}

// TestOverflowAtScale is the disqualifier: a u16 tree large enough that a root
// child subtree passes 65535 stores truncated counts, so interiorStats reports
// the overflow and the inconsistency, while a u32 tree of the same size stays
// exact and ranks correctly.
func TestOverflowAtScale(t *testing.T) {
	const n = 1_000_000
	keys := make([]uint64, n)
	rng := xorshift(0x51ab)
	for i := range keys {
		keys[i] = mix(rng.next())
	}

	u16 := build(2, keys)
	maxCount, consistent := u16.interiorStats()
	if maxCount <= 1<<16-1 {
		t.Fatalf("test too small to overflow u16: maxCount %d", maxCount)
	}
	if consistent {
		t.Fatal("u16 counts consistent past the ceiling, truncation not detected")
	}

	u32 := build(4, keys)
	maxCount, consistent = u32.interiorStats()
	if !consistent {
		t.Fatalf("u32 counts inconsistent at %d entries (maxCount %d)", n, maxCount)
	}
	if maxCount > 1<<32-1 {
		t.Fatalf("u32 maxCount %d over its ceiling", maxCount)
	}
	// u32 ranks correctly where u16 would not.
	sorted := make([]uint64, n)
	copy(sorted, keys)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, i := range []int{0, 1, n / 3, n / 2, n - 1} {
		got, present := u32.rank(sorted[i])
		if !present || got != uint64(i) {
			t.Fatalf("u32 rank(sorted[%d])=%d present=%v, want %d", i, got, present, i)
		}
		if sk := u32.selectAt(uint64(i)); sk != sorted[i] {
			t.Fatalf("u32 select(%d)=%d, want %d", i, sk, sorted[i])
		}
	}
}

// TestRankSelectPerWidth checks that on a tree small enough that no width
// overflows, rank and select agree with a sorted-slice model for every width,
// so the width choice does not change the order-statistic answers, only the
// layout that stores them.
func TestRankSelectPerWidth(t *testing.T) {
	for _, w := range []int{2, 4, 8} {
		tr := newTree(fixedBranchSz, fixedLeafSz, w)
		model := map[uint64]struct{}{}
		rng := rand.New(rand.NewSource(int64(w) * 1009))
		for i := 0; i < 5000; i++ {
			k := rng.Uint64()
			if _, ok := model[k]; ok {
				continue
			}
			model[k] = struct{}{}
			tr.insert(k)
		}
		if _, consistent := tr.interiorStats(); !consistent {
			t.Fatalf("countW %d: counts inconsistent below any ceiling", w)
		}
		keys := make([]uint64, 0, len(model))
		for k := range model {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for probe := 0; probe < 300; probe++ {
			i := rng.Intn(len(keys))
			got, present := tr.rank(keys[i])
			if !present || got != uint64(i) {
				t.Fatalf("countW %d: rank(%d)=%d present=%v, want %d", w, keys[i], got, present, i)
			}
			if sk := tr.selectAt(uint64(i)); sk != keys[i] {
				t.Fatalf("countW %d: select(%d)=%d, want %d", w, i, sk, keys[i])
			}
		}
	}
}
