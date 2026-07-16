package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// bench builds the sim the arms expect: the model on, the object in place.
func bench(t *testing.T) *sim.Sim {
	t.Helper()
	s := sim.New(sim.Config{Seed: 1, Latency: sim.S3Standard})
	if _, err := s.Put(context.Background(), key, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOpenLoop(t *testing.T) {
	r := openLoop(bench(t), 200, 32, 500*time.Millisecond)
	if r.ops != 100 {
		t.Fatalf("open arm ran %d ops, want 100", r.ops)
	}
	if r.peak < 1 || r.peak > 32 {
		t.Fatalf("inflight peak %d outside 1..32", r.peak)
	}
	r.arm = "open"
	if n := strings.Count(r.String(), ","); n != 12 {
		t.Fatalf("row has %d commas, header has 12: %s", n, r)
	}
}

func TestClosedLoop(t *testing.T) {
	r := closedLoop(bench(t), 8, 300*time.Millisecond)
	if r.ops < 8 {
		t.Fatalf("closed arm ran %d ops, want at least one per worker", r.ops)
	}
	if r.peak != 8 {
		t.Fatalf("closed arm peak %d, want the pool size", r.peak)
	}
}
