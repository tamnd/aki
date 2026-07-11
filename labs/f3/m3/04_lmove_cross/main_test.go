package main

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/shard"
)

// The harness checks: the two keys really land on different shards for the cross
// arm and one shard for the co-located arm, and the two arms produce the same
// move. The semantic differential and the atomicity oracle live with the engine
// (engine/f3/list/lmovecross_test.go); this file only proves the lab measures
// what it claims.

func TestKeyPlacement(t *testing.T) {
	b := newBench(8)
	defer b.stop()
	coSrc := b.keyOn(0, "cosrc_")
	coDst := b.keyOn(0, "codst_")
	if b.rt.ShardOf([]byte(coSrc)) != b.rt.ShardOf([]byte(coDst)) {
		t.Fatal("co-located pair split across shards")
	}
	xSrc := b.keyOn(0, "xsrc_")
	xDst := b.keyOn(1, "xdst_")
	if b.rt.ShardOf([]byte(xSrc)) == b.rt.ShardOf([]byte(xDst)) {
		t.Fatal("cross pair landed on one shard")
	}
}

func TestArmsAgree(t *testing.T) {
	b := newBench(8)
	defer b.stop()
	coSrc := b.keyOn(0, "cosrc_")
	coDst := b.keyOn(0, "codst_")
	xSrc := b.keyOn(0, "xsrc_")
	xDst := b.keyOn(1, "xdst_")
	for _, k := range []string{coSrc, coDst, xSrc, xDst} {
		b.fill(k, 16, 8)
	}
	coRep := string(b.do(opLmove, 0, coSrc, coDst, "RIGHT", "LEFT"))
	crossRep := string(b.txn([]string{xSrc, xDst}, func(t *shard.Txn) []byte {
		return list.LmoveCross(t, []byte(xSrc), []byte(xDst), false, true)
	}))
	if coRep != crossRep {
		t.Fatalf("LMOVE drift: co-located %q, cross-shard %q", coRep, crossRep)
	}
}
