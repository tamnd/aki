package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// fastSim leaves the latency model zero so the harness checks run in
// milliseconds; the measured sweeps use S3Standard through main.
func fastSim() *sim.Sim {
	return sim.New(sim.Config{Seed: 7})
}

// TestDenseChain runs contending nodes through the real loop and then
// walks the chain: every seq from 1 to the last must hold a parseable
// batch, the one past it must be absent, and the records the nodes think
// they committed must all be found on the chain exactly once.
func TestDenseChain(t *testing.T) {
	s := fastSim()
	ctx := context.Background()
	const contenders = 4
	prefix := "t/"
	per := make([]metrics, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Go(func() { node(ctx, s, prefix, i, 200, 300*time.Millisecond, "spec", &per[i]) })
	}
	wg.Wait()

	var wantAppends, wantRecords int64
	for i := range per {
		wantAppends += per[i].appends
		wantRecords += per[i].records
	}
	if wantAppends == 0 || wantRecords == 0 {
		t.Fatalf("nodes committed nothing: appends %d records %d", wantAppends, wantRecords)
	}

	var gotRecords int64
	seq := uint64(1)
	for ; ; seq++ {
		b, _, err := s.Get(ctx, chainSeqKey(prefix, seq))
		if errors.Is(err, obs1.ErrNotFound) {
			break
		}
		if err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
		batch, _, err := obs1.ParseChainBatch(b)
		if err != nil {
			t.Fatalf("seq %d does not parse: %v", seq, err)
		}
		gotRecords += int64(len(batch.Records))
	}
	if got := int64(seq - 1); got != wantAppends {
		t.Fatalf("chain holds %d objects, nodes committed %d", got, wantAppends)
	}
	if gotRecords != wantRecords {
		t.Fatalf("chain holds %d records, nodes generated %d", gotRecords, wantRecords)
	}
}

// TestCSVShape keeps the row and the header in step.
func TestCSVShape(t *testing.T) {
	row := sweep(fastSim(), "sim", "none", 2, 100, 200*time.Millisecond)
	if got, want := strings.Count(row, ","), strings.Count(csvHeader, ","); got != want {
		t.Fatalf("row has %d commas, header has %d: %s", got, want, row)
	}
}

// TestBuildBatch pins the coalescing shape: one commit plus n-1
// heartbeats, and the bytes parse back to that count.
func TestBuildBatch(t *testing.T) {
	b := buildBatch(3, 42, 5)
	batch, h, err := obs1.ParseChainBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.Writer != 3 || batch.BatchID != 42 || len(batch.Records) != 5 {
		t.Fatalf("writer %d batch %d records %d, want 3/42/5", h.Writer, batch.BatchID, len(batch.Records))
	}
}
