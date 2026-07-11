package main

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/f3/set"
)

// The harness checks: the fill really loads what the sweep assumes, the
// co-location really co-locates, and the one-shard and eight-shard arms
// return the same SINTERCARD count. The byte-level donation oracles live with
// the engine (engine/f3/set/donate_oracle_test.go, engine/f3/shard/
// donate_test.go); this file only proves the lab measures what it claims.

func TestHarnessCountsAgree(t *testing.T) {
	set.SetAlgebraMaintain(true)
	const n, shared = 30_000, 15_000
	var got [2]string
	for i, s := range []int{1, 8} {
		b := newBench(s)
		keys := b.colocated(2)
		b.fill(keys[0], 'a', shared, n)
		b.fill(keys[1], 'b', shared, n)
		rep := b.do(opSintercard, 1, "2", keys[0], keys[1])
		got[i] = string(rep)
		b.stop()
	}
	want := fmt.Sprintf(":%d\r\n", shared)
	if got[0] != want || got[1] != want {
		t.Fatalf("counts = %q and %q, want %q", got[0], got[1], want)
	}
}

func TestColocated(t *testing.T) {
	b := newBench(8)
	defer b.stop()
	keys := b.colocated(3)
	want := b.rt.ShardOf([]byte(keys[0]))
	for _, k := range keys[1:] {
		if b.rt.ShardOf([]byte(k)) != want {
			t.Fatalf("key %q routed off shard %d", k, want)
		}
	}
}
