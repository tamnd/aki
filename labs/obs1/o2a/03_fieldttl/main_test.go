package main

import (
	"context"
	"testing"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestFieldTTLSmoke pins the lab's own invariants at a small size: the
// column and bitmap encodings at zero TTL use are byte-identical to
// the plain packing (the pay-only-if-used identity), the flag encoding
// pays its byte on every element, live probes hit and expired probes
// answer absent at exactly one GET each under every encoding, and the
// rewrite drops the dead share while keeping every live field.
func TestFieldTTLSmoke(t *testing.T) {
	const n = 1 << 12
	ctx := context.Background()
	value := []byte("0123456789abcdef")
	base := buildTTL(n, value, 16<<10, encBitmap, 0, 0, nil)

	for _, enc := range []encoding{encColumn, encBitmap} {
		c := buildTTL(n, value, 16<<10, enc, 0, 0, nil)
		if string(c.obj) != string(base.obj) {
			t.Fatalf("%s encoding at zero TTL use is not byte-identical to the plain packing", encName[enc])
		}
		if c.contam != 0 {
			t.Fatalf("%s encoding contaminated %d chunks with no bearers", encName[enc], c.contam)
		}
	}
	fc := buildTTL(n, value, 16<<10, encFlag, 0, 0, nil)
	if len(fc.obj) <= len(base.obj) {
		t.Fatal("the flag encoding did not pay its per-element byte at zero TTL use")
	}

	const e = 500
	for _, enc := range []encoding{encColumn, encFlag, encBitmap} {
		s := sim.New(sim.Config{})
		c := buildTTL(n, value, 16<<10, enc, 1000, e, nil)
		if _, err := s.Put(ctx, "seg/h", c.obj); err != nil {
			t.Fatal(err)
		}
		var live, dead []int
		for i := range n {
			if salted(c.elem(i), 0xE2) < e {
				dead = append(dead, i)
			} else {
				live = append(live, i)
			}
		}
		lg, lf, err := probe(ctx, s, "seg/h", c, live[:200])
		if err != nil {
			t.Fatal(err)
		}
		eg, ef, err := probe(ctx, s, "seg/h", c, dead[:200])
		if err != nil {
			t.Fatal(err)
		}
		if lg != 1 || lf != 100 {
			t.Fatalf("%s live probes: %.4f GETs per op, %.2f%% found", encName[enc], lg, lf)
		}
		if eg != 1 || ef != 0 {
			t.Fatalf("%s expired probes: %.4f GETs per op, %.2f%% found", encName[enc], eg, ef)
		}
	}

	s := sim.New(sim.Config{})
	c := buildTTL(n, value, 16<<10, encBitmap, 1000, e, nil)
	keep := func(i int) bool { return salted(c.elem(i), 0xE2) >= e }
	rw := buildTTL(n, value, 16<<10, encBitmap, 1000, e, keep)
	if len(rw.obj) >= len(c.obj) {
		t.Fatal("the rewrite reclaimed nothing")
	}
	if _, err := s.Put(ctx, "seg/h2", rw.obj); err != nil {
		t.Fatal(err)
	}
	var live []int
	for i := range n {
		if keep(i) && len(live) < 200 {
			live = append(live, i)
		}
	}
	lg, lf, err := probe(ctx, s, "seg/h2", rw, live)
	if err != nil {
		t.Fatal(err)
	}
	if lg != 1 || lf != 100 {
		t.Fatalf("post-rewrite live probes: %.4f GETs per op, %.2f%% found", lg, lf)
	}
}
