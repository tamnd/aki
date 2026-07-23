package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestTypePointSmoke builds a small hash corpus with the real chunk
// codec and asserts the ledger invariants the lab scores: every point
// op is exactly one GET of one block and always finds its element.
func TestTypePointSmoke(t *testing.T) {
	const n = 1 << 12
	ctx := context.Background()
	s := sim.New(sim.Config{})
	value := make([]byte, 64)
	fields := make([][]byte, n)
	elems := make([]elem, n)
	for i := range n {
		f := fmt.Appendf(nil, "field:%09d", i)
		fields[i] = f
		elems[i] = elem{disc: fp(f), blob: packHash(f, value)}
	}
	obj, dir := build([]byte("h"), 0x0B, elems, 16<<10)
	if _, err := s.Put(ctx, "seg/h", obj); err != nil {
		t.Fatal(err)
	}
	for _, ce := range dir {
		if ce.ln > blockBytes {
			t.Fatalf("chunk of %d bytes exceeds a block", ce.ln)
		}
		if ce.off/blockBytes != (ce.off+int64(ce.ln)-1)/blockBytes {
			t.Fatalf("chunk at %d len %d spans a block boundary", ce.off, ce.ln)
		}
	}
	c, err := measure("hget", s, 500, func(i int) (bool, error) {
		f := fields[(i*7919)%n]
		blk, off, err := fetchBlock(ctx, s, "seg/h", int64(len(obj)), findChunk(dir, fp(f)))
		if err != nil {
			return false, err
		}
		p, err := chunkPayload(blk, off)
		if err != nil {
			return false, err
		}
		return hashLookup(p, fp(f), f), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.getsPer != 1.0 {
		t.Fatalf("HGET cost %.4f GETs per op, ledger says 1", c.getsPer)
	}
	if c.foundPct != 100 {
		t.Fatalf("HGET found %.2f%% of its fields", c.foundPct)
	}
	if c.kibPer > 128 {
		t.Fatalf("HGET pulled %.1f KiB per op, more than a block", c.kibPer)
	}
}
