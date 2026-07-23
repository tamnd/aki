package main

import (
	"context"
	"testing"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestBigCollectionSmoke pins the lab's own invariants at tiny sizes:
// point reads cost one GET and find their element, the intersection is
// exact, its requests follow the coalesced ceil identity, and the
// merge window stays within two blocks plus their decoded discs.
func TestBigCollectionSmoke(t *testing.T) {
	const n = 1 << 13
	ctx := context.Background()
	s := sim.New(sim.Config{})
	a := buildCorpus(n, nil, false, 16<<10, 0x0B)
	b := buildCorpus(2*n, nil, false, 16<<10, 0x0B)
	if _, err := s.Put(ctx, "seg/a", a.obj); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "seg/b", b.obj); err != nil {
		t.Fatal(err)
	}

	before := s.Usage()
	for i := range 200 {
		ok, err := pointGet(ctx, s, "seg/a", a, a.elem((i*7919)%n), false)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("member %d not found in its planned block", i)
		}
	}
	after := s.Usage()
	if gets := after.GetRequests - before.GetRequests; gets != 200 {
		t.Fatalf("200 point ops cost %d GETs, ledger says 200", gets)
	}

	matches, reqs, winPeak, _, err := intersect(ctx, s,
		[2]string{"seg/a", "seg/b"}, [2]int64{int64(len(a.obj)), int64(len(b.obj))})
	if err != nil {
		t.Fatal(err)
	}
	if matches != n {
		t.Fatalf("intersection found %d of %d members", matches, n)
	}
	want := int((int64(len(a.obj))+coalesceBytes-1)/coalesceBytes + (int64(len(b.obj))+coalesceBytes-1)/coalesceBytes)
	if reqs != want {
		t.Fatalf("intersection cost %d requests, ceil identity says %d", reqs, want)
	}
	if winPeak > 3*blockBytes {
		t.Fatalf("merge window peaked at %d bytes, more than two blocks plus discs", winPeak)
	}
}
