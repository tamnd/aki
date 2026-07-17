package main

import (
	"math"
	"math/rand/v2"
	"testing"
)

// scramble must be a bijection inside the masked keyspace or the fold
// layout would alias keys and the cache model would lie.
func TestScrambleBijective(t *testing.T) {
	const bits = 16
	mask := uint64(1)<<bits - 1
	seen := make([]bool, 1<<bits)
	for x := uint64(0); x <= mask; x++ {
		y := scramble(x, mask)
		if y > mask {
			t.Fatalf("scramble(%d) = %d, out of domain", x, y)
		}
		if seen[y] {
			t.Fatalf("scramble collides at %d", y)
		}
		seen[y] = true
	}
}

// The SIEVE second chance: with capacity 2, touching the older entry
// protects it and the hand evicts the untouched newer one.
func TestSieveSecondChance(t *testing.T) {
	s := newSieve(2)
	s.access(1)
	s.access(2)
	if !s.access(1) {
		t.Fatal("1 should be resident")
	}
	s.access(3) // hand clears 1's visited bit, evicts unvisited 2
	if !s.access(1) {
		t.Fatal("1 was evicted despite its visited bit")
	}
	if s.access(2) {
		t.Fatal("2 survived, want it evicted as the first unvisited entry")
	}
}

// Under uniform reads the hit rate is the cached byte fraction, at any
// block size: the closed form the uniform rows must land on.
func TestUniformHitTracksBudget(t *testing.T) {
	const n, vsize = 1 << 18, 200
	cold := int64(n) * vsize
	for _, bk := range []int64{32, 256} {
		r := rand.New(rand.NewPCG(7, uint64(bk)))
		p := runPoint(bk<<10, vsize, cold, 0.10, &uniformDraw{n: n, r: r}, 200_000, 800_000)
		if math.Abs(p.hitPct-10) > 1.5 {
			t.Fatalf("uniform hit at %d KiB = %.2f%%, want ~10%%", bk, p.hitPct)
		}
	}
}

// Fragment roundup is exact block arithmetic.
func TestFetchedForFrag(t *testing.T) {
	if got := fetchedForFrag(0, 64<<10, 32<<10); got != 64<<10 {
		t.Fatalf("aligned 64 KiB frag fetched %d", got)
	}
	if got := fetchedForFrag(1, 64<<10, 32<<10); got != 96<<10 {
		t.Fatalf("one-byte-shifted 64 KiB frag fetched %d, want 96 KiB", got)
	}
	if got := fetchedForFrag(100, 10, 32<<10); got != 32<<10 {
		t.Fatalf("tiny frag fetched %d, want one block", got)
	}
}

// The YCSB draw's head probability matches the zeta normalization.
func TestZipfHead(t *testing.T) {
	const n, theta = 1024, 0.99
	zetan := zetaSum(n, theta)
	z := newZipf(rand.New(rand.NewPCG(11, 12)), n, theta, zetan)
	head := 0
	const draws = 200_000
	for range draws {
		if z.rank() == 0 {
			head++
		}
	}
	want := 1 / zetan
	got := float64(head) / draws
	if got < want*0.85 || got > want*1.15 {
		t.Fatalf("p(rank 0) = %.4f, want %.4f within 15%%", got, want)
	}
}

// The tail draw never returns a hot-tier position: every drawn position
// must unscramble to a rank at or above hotN. Checked via the forward
// map on a small keyspace where the full rank set is enumerable.
func TestTailDrawSkipsHotRanks(t *testing.T) {
	const bits, theta = 10, 0.99
	n := uint64(1) << bits
	mask := n - 1
	hot := make(map[uint64]bool)
	for rk := uint64(0); rk < n/10; rk++ {
		hot[scramble(rk, mask)] = true
	}
	d := &zipfDraw{z: newZipf(rand.New(rand.NewPCG(3, 4)), n, theta, zetaSum(n, theta)), mask: mask, hotN: n / 10}
	for range 50_000 {
		if hot[d.next()] {
			t.Fatal("tail draw returned a hot-tier key")
		}
	}
}
