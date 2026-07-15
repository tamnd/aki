package main

import (
	"encoding/binary"
	"testing"

	"github.com/cespare/xxhash/v2"
)

func TestChunkArithmetic(t *testing.T) {
	if chunkHdr+chunkCap*entryBytes != chunkBytes {
		t.Fatalf("chunk layout %d+%d*%d != %d", chunkHdr, chunkCap, entryBytes, chunkBytes)
	}
	if chunkCap != 42 {
		t.Fatalf("chunk capacity %d, doc says 42", chunkCap)
	}
	cases := []struct {
		c    uint32
		want int
	}{
		{0, 0}, {42, 0}, {43, 1}, {83, 1}, {84, 2}, {124, 2}, {125, 3},
	}
	for _, c := range cases {
		if got := links(c.c); got != c.want {
			t.Errorf("links(%d) = %d, want %d", c.c, got, c.want)
		}
	}
}

// TestTableInvariants pins linear hashing's shape after every insert:
// bucket count equals 2^L + S, every placement lands inside it, and
// no entry is lost across splits.
func TestTableInvariants(t *testing.T) {
	tab := newTable(7, true)
	var key [8]byte
	const n = 200_000
	for i := range uint64(n) {
		binary.LittleEndian.PutUint64(key[:], i)
		tab.insert(xxhash.Sum64(key[:]))
		if got, want := tab.buckets(), uint64(1)<<tab.level+tab.split; got != want {
			t.Fatalf("after %d inserts: %d buckets, 2^%d+%d = %d", i+1, got, tab.level, tab.split, want)
		}
	}
	var total uint64
	for b, hs := range tab.hashes {
		total += uint64(len(hs))
		if uint64(len(hs)) != uint64(tab.counts[b]) {
			t.Fatalf("bucket %d count %d, holds %d hashes", b, tab.counts[b], len(hs))
		}
		for _, h := range hs {
			if got := tab.bucket(h); got != uint64(b) {
				t.Fatalf("hash %#x placed in bucket %d, bucket() says %d", h, b, got)
			}
		}
	}
	if total != n {
		t.Fatalf("%d entries survive of %d inserted", total, n)
	}
}

// TestCountsMatchesExact holds the fair-coin redistribution to the
// real bit-partition within tolerance at the same scale: the claim
// that counts mode is distribution-exact for uniform hashes is what
// lets the 1e9 arm run in 32 MiB.
func TestCountsMatchesExact(t *testing.T) {
	const n = 300_000
	exact := runSim(n, "doc", 3, true)
	counts := runSim(n, "doc", 3, false)
	relDiff := func(a, b float64) float64 {
		d := a - b
		if d < 0 {
			d = -d
		}
		return d / b
	}
	if relDiff(float64(counts.buckets), float64(exact.buckets)) > 0.03 {
		t.Fatalf("bucket count diverged: counts %d, exact %d", counts.buckets, exact.buckets)
	}
	if relDiff(counts.fillMean, exact.fillMean) > 0.03 {
		t.Fatalf("fill diverged: counts %.4f, exact %.4f", counts.fillMean, exact.fillMean)
	}
	if relDiff(counts.chainedPct+0.01, exact.chainedPct+0.01) > 0.5 {
		t.Fatalf("chain rate diverged: counts %.3f%%, exact %.3f%%", counts.chainedPct, exact.chainedPct)
	}
}

// TestDirHeapFloor sanity-checks the heap measurement against the
// arithmetic it wraps: a directory over k chunks can never cost less
// than 16 bytes per chunk, and resident pages should keep the
// overhead within a few percent of that floor.
func TestDirHeapFloor(t *testing.T) {
	const chunks = 100_000
	got := measureDirHeap(chunks)
	floor := uint64(chunks) * dirEntryLen
	if got < floor {
		t.Fatalf("measured %d bytes under the %d arithmetic floor", got, floor)
	}
	if got > floor*12/10 {
		t.Fatalf("measured %d bytes, more than 1.2x the %d floor", got, floor)
	}
}
