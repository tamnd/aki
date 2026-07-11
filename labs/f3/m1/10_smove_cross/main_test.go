package main

import (
	"testing"
)

// The harness checks: the pair helpers really co-locate and really split, and
// the two arms move the same ball with the same replies over a full
// ping-pong. The semantic differential and the atomicity oracles live with
// the engine (engine/f3/set/smovecross_test.go); this file only proves the
// lab measures what it claims.

func TestPairPlacement(t *testing.T) {
	b := newBench(4)
	defer b.stop()
	coSrc, coDst := b.pair(false)
	if b.rt.ShardOf([]byte(coSrc)) != b.rt.ShardOf([]byte(coDst)) {
		t.Fatal("co-located pair split")
	}
	xSrc, xDst := b.pair(true)
	if b.rt.ShardOf([]byte(xSrc)) == b.rt.ShardOf([]byte(xDst)) {
		t.Fatal("cross pair co-located")
	}
}

func TestArmsAgree(t *testing.T) {
	b := newBench(4)
	defer b.stop()
	coSrc, coDst := b.pair(false)
	xSrc, xDst := b.pair(true)
	b.fill(coSrc, 'a', 8, true)
	b.fill(coDst, 'b', 8, false)
	b.fill(xSrc, 'a', 8, true)
	b.fill(xDst, 'b', 8, false)
	for i := 0; i < 10; i++ {
		co := string(b.do(opSmove, 0, coSrc, coDst, "ball")) + string(b.do(opSmove, 0, coDst, coSrc, "ball"))
		cross := string(b.smoveCross(xSrc, xDst)) + string(b.smoveCross(xDst, xSrc))
		if co != cross {
			t.Fatalf("round %d: co-located %q, cross-shard %q", i, co, cross)
		}
		if co != ":1\r\n:1\r\n" {
			t.Fatalf("round %d: ping-pong lost the ball: %q", i, co)
		}
	}
}
