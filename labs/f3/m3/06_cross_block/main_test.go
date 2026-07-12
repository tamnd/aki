package main

import (
	"testing"
	"time"
)

// The harness checks: the cross arm really parks and serves across shards while
// the co-located arm stays on one shard, and a cross serve delivers the same
// outcome the co-located serve does. The semantic differential and the
// exactly-once claim oracle live with the engine (engine/f3/list/blockcross_test.go
// and blockmovecross_test.go); this file only proves the lab measures what it
// claims.

func TestKeyPlacement(t *testing.T) {
	b := newBench(8)
	defer b.stop()
	k0 := b.keyOn(0, "pk2_0_")
	k1 := b.keyOn(1, "pk2_1_")
	if b.rt.ShardOf([]byte(k0)) == b.rt.ShardOf([]byte(k1)) {
		t.Fatal("cross pop keys landed on one shard")
	}
	src := b.keyOn(0, "src_")
	dst := b.keyOn(1, "dst0_")
	if b.rt.ShardOf([]byte(src)) == b.rt.ShardOf([]byte(dst)) {
		t.Fatal("cross move source and destination landed on one shard")
	}
}

func TestCrossPopServes(t *testing.T) {
	b := newBench(8)
	defer b.stop()
	k0 := b.keyOn(0, "tpk_0_")
	k1 := b.keyOn(1, "tpk_1_")
	c := b.rt.NewConn()
	b.parkPopCross(c, []string{k0, k1})
	time.Sleep(settle)

	pusher := b.rt.NewConn()
	b.push(pusher, k1, "v") // serve off the second key, a different owner
	var rep []byte
	for rep == nil {
		c.DrainReplies(func(r []byte) { rep = append([]byte(nil), r...) })
	}
	if got := string(rep); got == "" || got[0] != '*' {
		t.Fatalf("cross BLPOP reply = %q, want a served array", got)
	}
}

func TestCrossMoveServes(t *testing.T) {
	b := newBench(8)
	defer b.stop()
	src := b.keyOn(0, "tsrc_")
	dst := b.keyOn(1, "tdst_")
	c := b.rt.NewConn()
	b.parkMoveCross(c, src, dst, false)
	time.Sleep(settle)

	pusher := b.rt.NewConn()
	b.push(pusher, src, "moved")
	var rep []byte
	for rep == nil {
		c.DrainReplies(func(r []byte) { rep = append([]byte(nil), r...) })
	}
	if want := "$5\r\nmoved\r\n"; string(rep) != want {
		t.Fatalf("cross BLMOVE reply = %q, want %q", rep, want)
	}
	// The element landed in the destination on the other shard.
	reader := b.rt.NewConn()
	if err := reader.DoAt(opLrange, 0, bytesOf([]string{dst, "0", "-1"})); err != nil {
		t.Fatal(err)
	}
	reader.Flush()
	var lr []byte
	for lr == nil {
		reader.DrainReplies(func(r []byte) { lr = append([]byte(nil), r...) })
	}
	if want := "*1\r\n$5\r\nmoved\r\n"; string(lr) != want {
		t.Fatalf("destination LRANGE = %q, want %q", lr, want)
	}
}
