package command

import (
	"testing"

	"github.com/tamnd/aki/keyspace"
)

// setReq builds a fire-and-forget SET request for the coalescing tests.
func setReq(key string, ver uint64) *writeReq {
	return &writeReq{setKey: []byte(key), setVer: ver}
}

// fnReq builds a closure request, which coalesceSets must treat as a barrier.
func fnReq() *writeReq {
	return &writeReq{fn: func(*keyspace.DB) error { return nil }}
}

func runCoalesce(reqs []*writeReq) []bool {
	e := &Engine{}
	sc := newDrainScratch()
	sc.reqs = reqs
	e.coalesceSets(sc)
	out := make([]bool, len(sc.skip))
	copy(out, sc.skip)
	return out
}

func TestCoalesceDistinctKeysKeepsAll(t *testing.T) {
	skip := runCoalesce([]*writeReq{setReq("a", 1), setReq("b", 2), setReq("c", 3)})
	for i, s := range skip {
		if s {
			t.Fatalf("req %d wrongly skipped", i)
		}
	}
}

func TestCoalesceSameKeyKeepsHighestVersion(t *testing.T) {
	// Ascending: only the last (highest) survives.
	skip := runCoalesce([]*writeReq{setReq("a", 1), setReq("a", 2), setReq("a", 3)})
	want := []bool{true, true, false}
	for i := range want {
		if skip[i] != want[i] {
			t.Fatalf("ascending skip = %v want %v", skip, want)
		}
	}
}

func TestCoalesceScrambledVersionsKeepsMax(t *testing.T) {
	// Channel order need not match version order; the highest version wins
	// regardless of position.
	skip := runCoalesce([]*writeReq{setReq("a", 3), setReq("a", 1), setReq("a", 2)})
	want := []bool{false, true, true}
	for i := range want {
		if skip[i] != want[i] {
			t.Fatalf("scrambled skip = %v want %v", skip, want)
		}
	}
}

func TestCoalesceFnIsBarrier(t *testing.T) {
	// A closure between two SETs to the same key blocks coalescing: a
	// read-modify-write may depend on the first SET's B-tree state.
	skip := runCoalesce([]*writeReq{setReq("a", 1), fnReq(), setReq("a", 2)})
	want := []bool{false, false, false}
	for i := range want {
		if skip[i] != want[i] {
			t.Fatalf("barrier skip = %v want %v", skip, want)
		}
	}
}

func TestCoalesceInterleavedKeys(t *testing.T) {
	// a:1, b:1, a:2, b:2 -> the first a and first b are superseded.
	skip := runCoalesce([]*writeReq{setReq("a", 1), setReq("b", 1), setReq("a", 2), setReq("b", 2)})
	want := []bool{true, true, false, false}
	for i := range want {
		if skip[i] != want[i] {
			t.Fatalf("interleaved skip = %v want %v", skip, want)
		}
	}
}
