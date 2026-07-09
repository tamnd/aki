package f1raw

import (
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"
)

// wantSnap builds the sorted (hash, off) arrays a partition's fold should converge to from the live
// member set, so a test can compare the folder's published snapshot against the ground truth.
func wantSnap(live map[uint64]uint64) (h, off []uint64) {
	type pair struct{ h, off uint64 }
	ps := make([]pair, 0, len(live))
	for o, hv := range live {
		ps = append(ps, pair{h: hv, off: o})
	}
	slices.SortFunc(ps, func(a, b pair) int {
		if a.h != b.h {
			if a.h < b.h {
				return -1
			}
			return 1
		}
		if a.off < b.off {
			return -1
		}
		if a.off > b.off {
			return 1
		}
		return 0
	})
	h = make([]uint64, len(ps))
	off = make([]uint64, len(ps))
	for i, p := range ps {
		h[i] = p.h
		off[i] = p.off
	}
	return h, off
}

// TestSortedHashFoldConverges drives a mixed add/remove stream across several partition prefixes,
// forces the fold with SyncSortedHashes, and asserts each prefix's published array is exactly the
// live member set in hash order. This is the facility's definition of done: the sorted array is a
// correct maintained mirror of the members a partition holds.
func TestSortedHashFoldConverges(t *testing.T) {
	s := New(1024, 1<<20)
	s.EnableSortedHashFold()
	defer s.Close()

	prefixes := [][]byte{[]byte("setA"), []byte("setB"), []byte("setC")}
	live := map[string]map[uint64]uint64{}
	for _, p := range prefixes {
		live[string(p)] = map[uint64]uint64{}
	}
	rng := rand.New(rand.NewPCG(7, 11))
	var nextOff uint64 = 1

	for round := 0; round < 60; round++ {
		pfx := prefixes[rng.IntN(len(prefixes))]
		lset := live[string(pfx)]
		// A handful of adds.
		for i := 0; i < 1+rng.IntN(8); i++ {
			off := nextOff
			nextOff++
			hv := rng.Uint64()
			lset[off] = hv
			s.shAppend(pfx, hv, off, true)
		}
		// A few removes of live members.
		if len(lset) > 0 {
			offs := make([]uint64, 0, len(lset))
			for o := range lset {
				offs = append(offs, o)
			}
			for i := 0; i < rng.IntN(4) && len(offs) > 0; i++ {
				k := rng.IntN(len(offs))
				o := offs[k]
				offs[k] = offs[len(offs)-1]
				offs = offs[:len(offs)-1]
				s.shAppend(pfx, lset[o], o, false)
				delete(lset, o)
			}
		}
	}

	s.SyncSortedHashes()
	for _, p := range prefixes {
		if !s.SortedHashCurrent(p) {
			t.Fatalf("%s not current after sync", p)
		}
		snap := s.SortedHashSnapshot(p)
		wh, wo := wantSnap(live[string(p)])
		if !slices.Equal(snap.h, wh) || !slices.Equal(snap.off, wo) {
			t.Fatalf("%s diverged: got %d entries, want %d", p, len(snap.h), len(wh))
		}
	}
}

// TestSortedHashFoldBackgroundDrains checks the folder applies journals on its own without a
// foreground sync: after appending, the test polls SortedHashCurrent until the background folder
// catches up, then asserts convergence.
func TestSortedHashFoldBackgroundDrains(t *testing.T) {
	s := New(1024, 1<<20)
	s.EnableSortedHashFold()
	defer s.Close()

	pfx := []byte("bg")
	live := map[uint64]uint64{}
	for off := uint64(1); off <= 500; off++ {
		hv := off * 2654435761
		live[off] = hv
		s.shAppend(pfx, hv, off, true)
	}

	deadline := time.Now().Add(5 * time.Second)
	for !s.SortedHashCurrent(pfx) {
		if time.Now().After(deadline) {
			t.Fatal("background folder did not catch up")
		}
		time.Sleep(time.Millisecond)
	}
	snap := s.SortedHashSnapshot(pfx)
	wh, wo := wantSnap(live)
	if !slices.Equal(snap.h, wh) || !slices.Equal(snap.off, wo) {
		t.Fatalf("background fold diverged: %d vs %d", len(snap.h), len(wh))
	}
}

// TestSortedHashFoldCloseDrains checks Close applies every pending journal, so the sorted arrays are
// reconciled at shutdown rather than dropped. It appends without syncing, closes, then reads the
// snapshot the closed store still holds.
func TestSortedHashFoldCloseDrains(t *testing.T) {
	s := New(1024, 1<<20)
	s.EnableSortedHashFold()

	pfx := []byte("close")
	live := map[uint64]uint64{}
	for off := uint64(1); off <= 300; off++ {
		hv := off*0x9E3779B1 + 5
		live[off] = hv
		s.shAppend(pfx, hv, off, true)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if !s.SortedHashCurrent(pfx) {
		t.Fatal("not current after close-drain")
	}
	snap := s.SortedHashSnapshot(pfx)
	wh, wo := wantSnap(live)
	if !slices.Equal(snap.h, wh) || !slices.Equal(snap.off, wo) {
		t.Fatalf("close fold diverged: %d vs %d", len(snap.h), len(wh))
	}
}

// TestSortedHashFoldConcurrentAppend hammers one partition from many goroutines while the folder
// runs, then syncs and asserts convergence. It is the race-detector's target: the producer's
// journal append and dirty-stack push run concurrently with the folder's swap and fold.
func TestSortedHashFoldConcurrentAppend(t *testing.T) {
	s := New(1024, 1<<20)
	s.EnableSortedHashFold()
	defer s.Close()

	pfx := []byte("race")
	const workers = 8
	const perWorker = 400

	var mu sync.Mutex
	live := map[uint64]uint64{}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < perWorker; i++ {
				off := base*perWorker + i + 1
				hv := off * 1099511628211
				mu.Lock()
				live[off] = hv
				mu.Unlock()
				s.shAppend(pfx, hv, off, true)
			}
		}(uint64(w))
	}
	wg.Wait()

	s.SyncSortedHashes()
	snap := s.SortedHashSnapshot(pfx)
	wh, wo := wantSnap(live)
	if !slices.Equal(snap.h, wh) || !slices.Equal(snap.off, wo) {
		t.Fatalf("concurrent fold diverged: got %d, want %d", len(snap.h), len(wh))
	}
}

// TestSortedHashFoldDisabledNoSnapshot checks a store that never enables the folder journals nothing
// and its snapshot accessors report absent rather than panicking.
func TestSortedHashFoldDisabledNoSnapshot(t *testing.T) {
	s := New(64, 1<<16)
	defer s.Close()
	if snap := s.SortedHashSnapshot([]byte("x")); snap != nil {
		t.Fatalf("disabled store returned a snapshot: %v", snap)
	}
	if s.SortedHashCurrent([]byte("x")) {
		t.Fatal("disabled store reports current")
	}
	s.SyncSortedHashes() // must be a no-op, not a nil-registry panic
}
