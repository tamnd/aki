package main

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The harness checks: the operand helpers really co-locate and really split,
// and the two arms compute the same intersection over the same operands. The
// semantic differential and the atomicity oracle live with the engine
// (engine/f3/set/gathercross_test.go); this file only proves the lab measures
// what it claims.

func TestOperandPlacement(t *testing.T) {
	b := newBench(8)
	defer b.stop()
	co := b.operands(4, false)
	sh := b.rt.ShardOf([]byte(co[0]))
	for _, k := range co {
		if b.rt.ShardOf([]byte(k)) != sh {
			t.Fatal("co-located operands split across shards")
		}
	}
	cross := b.operands(4, true)
	seen := map[int]bool{}
	for _, k := range cross {
		seen[b.rt.ShardOf([]byte(k))] = true
	}
	if len(seen) != len(cross) {
		t.Fatalf("cross operands share shards: %d distinct for %d keys", len(seen), len(cross))
	}
}

func TestArmsAgree(t *testing.T) {
	b := newBench(8)
	defer b.stop()
	co := b.operands(4, false)
	cross := b.operands(4, true)
	for _, key := range append(append([]string{}, co...), cross...) {
		b.fill(key, 64)
	}
	coRep := string(b.do(opSinter, 0, co...))
	crossRep := string(b.txn(cross, func(t *shard.Txn) []byte { return set.SinterCross(t, bytesOf(cross)) }))
	if coRep != crossRep {
		t.Fatalf("SINTER drift: co-located %q, cross-shard %q", coRep, crossRep)
	}
}
