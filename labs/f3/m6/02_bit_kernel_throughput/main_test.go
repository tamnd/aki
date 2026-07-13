package main

import (
	"math/rand/v2"
	"testing"
)

// The lab's correctness floor: the word kernel and the naive form must agree bit for
// bit over every length class and fill, or the throughput numbers compare two different
// answers. These tests run in CI; the benchmarks below are for a hand run.

// randomBytes fills n bytes from a seeded source so a failure reproduces.
func randomBytes(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func TestPopcountFormsAgree(t *testing.T) {
	// Lengths that exercise the 64-byte block, the 8-byte word tail, and the byte tail.
	for _, n := range []int{0, 1, 7, 8, 63, 64, 65, 200, 4096, 70000} {
		for seed := uint64(0); seed < 4; seed++ {
			b := randomBytes(n, seed+uint64(n))
			if got, want := popcountWord8(b), popcountNaive(b); got != want {
				t.Fatalf("popcount n=%d seed=%d: word8 %d != naive %d", n, seed, got, want)
			}
		}
	}
}

func TestFirstSetFormsAgree(t *testing.T) {
	for _, n := range []int{0, 1, 7, 8, 63, 64, 65, 200, 4096, 70000} {
		// All-zero: both report -1.
		zero := make([]byte, n)
		if got, want := firstSetWord(zero), firstSetNaive(zero); got != want {
			t.Fatalf("first-set all-zero n=%d: word %d != naive %d", n, got, want)
		}
		// One set bit at each byte position: both find the same offset.
		for pos := 0; pos < n; pos++ {
			b := make([]byte, n)
			b[pos] = 0x01
			if got, want := firstSetWord(b), firstSetNaive(b); got != want {
				t.Fatalf("first-set n=%d pos=%d: word %d != naive %d", n, pos, got, want)
			}
		}
		// Random fills.
		for seed := uint64(0); seed < 4; seed++ {
			b := randomBytes(n, seed+uint64(n)*7)
			if got, want := firstSetWord(b), firstSetNaive(b); got != want {
				t.Fatalf("first-set n=%d seed=%d: word %d != naive %d", n, seed, got, want)
			}
		}
	}
}

func benchPopcount(b *testing.B, fn func([]byte) int, size int) {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = 0x55
	}
	b.SetBytes(int64(size))
	sink := 0
	for i := 0; i < b.N; i++ {
		sink += fn(buf)
	}
	_ = sink
}

func BenchmarkPopcountNaiveSmall(b *testing.B) { benchPopcount(b, popcountNaive, 64) }
func BenchmarkPopcountWord8Small(b *testing.B) { benchPopcount(b, popcountWord8, 64) }
func BenchmarkPopcountNaiveLLC(b *testing.B)   { benchPopcount(b, popcountNaive, 256<<10) }
func BenchmarkPopcountWord8LLC(b *testing.B)   { benchPopcount(b, popcountWord8, 256<<10) }
func BenchmarkPopcountNaiveDRAM(b *testing.B)  { benchPopcount(b, popcountNaive, 64<<20) }
func BenchmarkPopcountWord8DRAM(b *testing.B)  { benchPopcount(b, popcountWord8, 64<<20) }
