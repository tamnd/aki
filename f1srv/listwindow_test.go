package f1srv

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// TestListWindowTailConcurrent hammers appendTail from many goroutines and asserts the resident ring
// holds the exact bytes for every committed tail position with no gap, loss, or duplication. Fused
// appends claim their positions and fill their ring slots in one mu-guarded step, so the ring is filled
// in commit order by construction; this proves the whole overlay's tail invariant under contention,
// which is what lets a pop and read take bytes straight from the ring instead of probing f1raw. Each
// element carries a token unique to its (goroutine, run, offset), so the multiset of ring bytes over
// the committed span must equal the multiset of everything pushed. The race detector guards the ring
// grow and slot fills that run under the per-list mutex while other goroutines append in parallel.
func TestListWindowTailConcurrent(t *testing.T) {
	const goroutines = 48
	const runsPer = 300
	const maxRun = 9

	w := newListWindow(0, 0)
	pushed := make([][][]byte, goroutines)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			// A cheap per-goroutine PRNG for run sizes, so no shared Rand: the sequence only needs to
			// vary the run lengths, not be uniform.
			x := uint32(seed*2654435761 + 1)
			next := func() uint32 { x ^= x << 13; x ^= x >> 17; x ^= x << 5; return x }
			var mine [][]byte
			for r := 0; r < runsPer; r++ {
				n := int64(next()%maxRun) + 1
				posElems := make([][]byte, n)
				for j := int64(0); j < n; j++ {
					b := []byte(fmt.Sprintf("g%d-r%d-j%d", seed, r, j))
					posElems[j] = b
					mine = append(mine, b)
				}
				w.appendTail(n, posElems)
			}
			pushed[seed] = mine
		}(g)
	}
	wg.Wait()

	ch := w.committedHead.Load()
	ct := w.committedTail.Load()
	if ch != 0 {
		t.Fatalf("tail-only appends moved the head to %d, want 0", ch)
	}
	if rt := w.reservedTail.Load(); rt != ct {
		t.Fatalf("reserved tail %d != committed tail %d", rt, ct)
	}
	assertRingMultiset(t, w, ch, ct, pushed)
}

// TestListWindowHeadConcurrent mirrors the tail proof for appendHead: runs claim positions below the
// head, and after the concurrent barrier the ring must hold the exact bytes at every negative committed
// position with no gap, loss, or duplication. LPUSH lands the run in position order (posElems[j] at
// start+j), the same order pushThroughWindow builds, so the ring fill indexes it by offset.
func TestListWindowHeadConcurrent(t *testing.T) {
	const goroutines = 48
	const runsPer = 300
	const maxRun = 9

	w := newListWindow(0, 0)
	pushed := make([][][]byte, goroutines)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			x := uint32(seed*2654435761 + 1)
			next := func() uint32 { x ^= x << 13; x ^= x >> 17; x ^= x << 5; return x }
			var mine [][]byte
			for r := 0; r < runsPer; r++ {
				n := int64(next()%maxRun) + 1
				posElems := make([][]byte, n)
				for j := int64(0); j < n; j++ {
					b := []byte(fmt.Sprintf("g%d-r%d-j%d", seed, r, j))
					posElems[j] = b
					mine = append(mine, b)
				}
				w.appendHead(n, posElems)
			}
			pushed[seed] = mine
		}(g)
	}
	wg.Wait()

	ch := w.committedHead.Load()
	ct := w.committedTail.Load()
	if ct != 0 {
		t.Fatalf("head-only appends moved the tail to %d, want 0", ct)
	}
	if rh := w.reservedHead.Load(); rh != ch {
		t.Fatalf("reserved head %d != committed head %d", rh, ch)
	}
	assertRingMultiset(t, w, ch, ct, pushed)
}

// assertRingMultiset checks that the ring holds a non-nil slot for every position in [lo, hi) and that
// the multiset of those bytes equals the multiset of everything pushed. Equal counts plus a full span
// (hi-lo positions, one value each) means no position was left unfilled, dropped, or written twice.
func assertRingMultiset(t *testing.T, w *listWindow, lo, hi int64, pushed [][][]byte) {
	t.Helper()
	var total int64
	for _, mine := range pushed {
		total += int64(len(mine))
	}
	if hi-lo != total {
		t.Fatalf("committed span %d != pushed count %d", hi-lo, total)
	}
	got := make(map[string]int, total)
	for p := lo; p < hi; p++ {
		v := w.ring.get(p)
		if v == nil {
			t.Fatalf("ring position %d is unfilled inside the committed span", p)
		}
		got[string(v)]++
	}
	want := make(map[string]int, total)
	for _, mine := range pushed {
		for _, b := range mine {
			want[string(b)]++
		}
	}
	if len(got) != len(want) {
		t.Fatalf("ring holds %d distinct values, pushed %d distinct", len(got), len(want))
	}
	for k, c := range want {
		if got[k] != c {
			t.Fatalf("value %q: ring count %d != pushed count %d", k, got[k], c)
		}
	}
}

// TestListWindowAppendInOrder pins the single-threaded ordering appendTail and appendHead guarantee: a
// sequence of appends lands each run at contiguous positions extending the committed span, and the
// reply base length is the visible length just before the run. This is the property RPUSH/LPUSH replies
// rest on, checked without concurrency so the exact positions and lengths are deterministic.
func TestListWindowAppendInOrder(t *testing.T) {
	w := newListWindow(0, 0)

	if base := w.appendTail(2, [][]byte{[]byte("a"), []byte("b")}); base != 0 {
		t.Fatalf("first RPUSH base len %d, want 0", base)
	}
	if base := w.appendTail(1, [][]byte{[]byte("c")}); base != 2 {
		t.Fatalf("second RPUSH base len %d, want 2", base)
	}
	if base := w.appendHead(2, [][]byte{[]byte("y"), []byte("z")}); base != 3 {
		t.Fatalf("LPUSH base len %d, want 3", base)
	}
	ch, ct := w.bounds()
	if ct-ch != 5 {
		t.Fatalf("committed span %d, want 5", ct-ch)
	}
	// Tail positions 0,1,2 hold a,b,c; head positions -2,-1 hold the LPUSH run in position order.
	for pos, want := range map[int64]string{0: "a", 1: "b", 2: "c", -2: "y", -1: "z"} {
		if got := w.ring.get(pos); !bytes.Equal(got, []byte(want)) {
			t.Fatalf("ring position %d: got %q want %q", pos, got, want)
		}
	}
}
