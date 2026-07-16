package main

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
)

// TestZModelOracle pins the resident model against a reference map at
// a small split threshold, so preload and churn cross both families'
// split paths many times: every score matches, the run walk replays
// the reference (score, member) sort exactly, counts and encoded
// sizes are exact, and both fences partition.
func TestZModelOracle(t *testing.T) {
	cfg := config{mix: "board", memMax: 512, runMax: 512, keys: 1,
		members: 3000, mlen: 12, threshold: 1 << 20, seed: 7}
	m := newModel(cfg)
	rng := rand.New(rand.NewSource(cfg.seed))
	z := newZset(cfg.members)
	m.zs[0] = z
	ref := make(map[int32]uint64, cfg.members)
	for mi := range cfg.members {
		z.names = append(z.names, memberName(rng, 0, mi, cfg.mlen))
		sc := sortableScore(rng.Float64() * 1e6)
		var b bill
		m.zaddNew(z, int32(mi), sc, &b)
		ref[int32(mi)] = sc
		if m.dirtyBytes >= cfg.threshold {
			m.flush()
		}
	}
	for range 20000 {
		mi := int32(rng.Intn(cfg.members))
		var sc uint64
		if rng.Intn(2) == 0 {
			sc = sortableScore(rng.Float64() * 1e6)
		} else {
			sc = sortableScore(scoreFromSortable(ref[mi]) + rng.Float64()*1000 - 500)
		}
		var b bill
		if m.zaddMove(z, mi, sc, &b) {
			ref[mi] = sc
		}
		if m.dirtyBytes >= cfg.threshold {
			m.flush()
		}
	}

	// Scores through the member side.
	for mi, want := range ref {
		got, ok := z.zscore(mi)
		if !ok || got != want {
			t.Fatalf("zscore(%d) = %#x, %v; want %#x", mi, got, ok, want)
		}
	}
	if z.card() != len(ref) {
		t.Fatalf("card %d, want %d", z.card(), len(ref))
	}

	// The run walk replays the reference sort by (score, member).
	type key struct {
		score uint64
		mi    int32
	}
	sorted := make([]key, 0, len(ref))
	for mi, sc := range ref {
		sorted = append(sorted, key{sc, mi})
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].score != sorted[j].score {
			return sorted[i].score < sorted[j].score
		}
		return bytes.Compare(z.names[sorted[i].mi], z.names[sorted[j].mi]) < 0
	})
	i := 0
	for ri, r := range z.runs {
		if ri > 0 && len(r.ents) == 0 {
			t.Fatalf("run %d is empty but alive", ri)
		}
		size := segHdrBytes
		for _, e := range r.ents {
			if e.score != sorted[i].score || e.mi != sorted[i].mi {
				t.Fatalf("walk position %d: run %d holds (%#x, %d), want (%#x, %d)",
					i, ri, e.score, e.mi, sorted[i].score, sorted[i].mi)
			}
			if z.loAbove(r, e.score, e.mi) {
				t.Fatalf("run %d entry (%#x, %d) sits below its run's lo", ri, e.score, e.mi)
			}
			size += runEntSize(len(z.names[e.mi]))
			i++
		}
		if r.size != size {
			t.Fatalf("run %d size %d, recomputed %d", ri, r.size, size)
		}
		if ri > 0 && i-len(r.ents) > 0 {
			prev := sorted[i-len(r.ents)-1]
			if !z.loAbove(r, prev.score, prev.mi) {
				t.Fatalf("run %d lo does not sit above the previous run's tail", ri)
			}
		}
	}
	if i != len(sorted) {
		t.Fatalf("walk covered %d entries, want %d", i, len(sorted))
	}

	// Member fence partitions and segment sizes are exact.
	total := 0
	for fi, fe := range z.fence {
		s := z.segs[fe.segid]
		hi := uint64(1<<64 - 1)
		if fi+1 < len(z.fence) {
			hi = z.fence[fi+1].lo
		}
		size := segHdrBytes
		var prev uint64
		for ei, e := range s.ents {
			if e.mh < fe.lo || (fi+1 < len(z.fence) && e.mh >= hi) {
				t.Fatalf("segment %d entry mh %#x outside [%#x, %#x)", fe.segid, e.mh, fe.lo, hi)
			}
			if ei > 0 && e.mh < prev {
				t.Fatalf("segment %d unsorted at %d", fe.segid, ei)
			}
			prev = e.mh
			size += memEntSize(len(z.names[e.mi]))
		}
		if s.size != size {
			t.Fatalf("segment %d size %d, recomputed %d", fe.segid, s.size, size)
		}
		total += len(s.ents)
	}
	if total != len(ref) {
		t.Fatalf("member side holds %d entries, want %d", total, len(ref))
	}

	// Rank agrees with the reference index at sampled members, and the
	// range walk streams the right window.
	for _, probe := range []int{0, 1, len(sorted) / 2, len(sorted) - 1} {
		if got := z.zrank(sorted[probe].mi); got != probe {
			t.Fatalf("zrank(%d) = %d, want %d", sorted[probe].mi, got, probe)
		}
	}
	if m.memSplits == 0 || m.runSplits == 0 {
		t.Fatalf("splits never fired (mem %d, run %d); the oracle needs the split path",
			m.memSplits, m.runSplits)
	}
}

// TestRangeWalkWindow checks the streamed window against the sorted
// reference at a few offsets, including one spanning a run boundary.
func TestRangeWalkWindow(t *testing.T) {
	cfg := config{memMax: 512, runMax: 512, keys: 1, members: 2000, mlen: 12,
		threshold: 1 << 20, seed: 11}
	m := newModel(cfg)
	rng := rand.New(rand.NewSource(cfg.seed))
	z := newZset(cfg.members)
	m.zs[0] = z
	for mi := range cfg.members {
		z.names = append(z.names, memberName(rng, 0, mi, cfg.mlen))
		var b bill
		m.zaddNew(z, int32(mi), sortableScore(rng.Float64()*1e6), &b)
	}
	if len(z.runs) < 4 {
		t.Fatalf("need several runs, got %d", len(z.runs))
	}
	boundary := len(z.runs[0].ents) + len(z.runs[1].ents) - 3
	for _, start := range []int{0, 7, boundary, cfg.members - rangeN} {
		runs, elems, bts := z.rangeWalk(start, rangeN)
		if elems != rangeN {
			t.Fatalf("start %d: %d elements, want %d", start, elems, rangeN)
		}
		if runs < 1 || bts < runs*segHdrBytes {
			t.Fatalf("start %d: implausible accounting runs=%d bytes=%d", start, runs, bts)
		}
	}
	if runs, _, _ := z.rangeWalk(boundary, rangeN); runs < 2 {
		t.Fatalf("boundary-spanning walk touched %d runs, want at least 2", runs)
	}
}

// TestSortableMirror keeps the lab's copy of the transform honest
// against the engine's semantics: order matches double comparison and
// the zero pair folds to one image.
func TestSortableMirror(t *testing.T) {
	vals := []float64{-1e18, -2.5, -1, 0, 1, 2.5, 1e18}
	for i, a := range vals {
		for _, b := range vals[i+1:] {
			if sortableScore(a) >= sortableScore(b) {
				t.Fatalf("order broke at %v, %v", a, b)
			}
		}
	}
	if sortableScore(0) != sortableScore(mustNegZero()) {
		t.Fatal("the zero pair encodes to two images")
	}
	for _, v := range vals {
		if scoreFromSortable(sortableScore(v)) != v {
			t.Fatalf("round trip broke at %v", v)
		}
	}
}

func mustNegZero() float64 {
	z := 0.0
	return -z
}
