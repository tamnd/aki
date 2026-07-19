package drivers

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The zset-type keyspace data events (spec 2064/f3/11:602, class 'z'). Every sorted-set
// write fires its redis event: ZADD/GEOADD fire zadd, ZINCRBY fires zincr, ZREM and the
// ZREMRANGEBY* family fire their command-named event, the pops fire zpopmin/zpopmax from
// the popped end, and the *STORE writers fire their command name on a non-empty result.
// A write that empties the key trails a generic del; an empty STORE result over an
// existing destination fires del instead of a store event.
//
// The notify mask is a process global, so the test resets it on cleanup.

// TestKeyspaceNotifyZsetEvents walks a sorted set through the single-key write, pop, and
// remove-range families, asserting the ordered event stream.
func TestKeyspaceNotifyZsetEvents(t *testing.T) {
	srv := startPubsubServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	send(t, pubNc, "CONFIG", "SET", "notify-keyspace-events", "KEA")
	expect(t, pubBr, "+OK\r\n")

	send(t, subNc, "PSUBSCRIBE", "__keyevent@0__:*")
	if k, ch, n := readSubConfirm(t, subBr); k != "psubscribe" || ch != "__keyevent@0__:*" || n != 1 {
		t.Fatalf("psubscribe confirm = %q %q %d", k, ch, n)
	}

	// ZADD fires one zadd regardless of how many members it wrote.
	send(t, pubNc, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zadd", "z")

	// An all-idempotent ZADD writes nothing and fires nothing; the next real write is
	// the ZINCRBY below, so a stray zadd here would misalign that assertion.
	send(t, pubNc, "ZADD", "z", "1", "a")
	readIntFrom(t, pubBr)

	// ZINCRBY always writes, so it always fires zincr.
	send(t, pubNc, "ZINCRBY", "z", "5", "a")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "zincr", "z")

	// After the idempotent ZADD and the ZINCRBY the key holds a=6, b=2, c=3.
	// ZPOPMIN pops the lowest (b), ZPOPMAX the highest (a): zpopmin, then zpopmax.
	send(t, pubNc, "ZPOPMIN", "z")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "zpopmin", "z")
	send(t, pubNc, "ZPOPMAX", "z")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "zpopmax", "z")

	// ZREM on a fresh two-member key: one zrem while a member remains, then zrem plus
	// the generic del when the last one leaves.
	send(t, pubNc, "ZADD", "zd", "1", "a", "2", "b")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zadd", "zd")
	send(t, pubNc, "ZREM", "zd", "a")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zrem", "zd")
	send(t, pubNc, "ZREM", "zd", "b")
	readIntFrom(t, pubBr)
	wantEvents(t, subBr, kevent{"zrem", "zd"}, kevent{"del", "zd"})

	// ZREMRANGEBYRANK fires its command-named event, and the generic del when the
	// window is the whole set.
	send(t, pubNc, "ZADD", "zr", "1", "a", "2", "b", "3", "c")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zadd", "zr")
	send(t, pubNc, "ZREMRANGEBYRANK", "zr", "0", "0")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zremrangebyrank", "zr")
	send(t, pubNc, "ZREMRANGEBYSCORE", "zr", "-inf", "+inf")
	readIntFrom(t, pubBr)
	wantEvents(t, subBr, kevent{"zremrangebyscore", "zr"}, kevent{"del", "zr"})
}

// TestKeyspaceNotifyZsetStoreEvents pins the *STORE writers: a non-empty result fires the
// command-named zset event on the destination, an empty result over an existing
// destination fires the generic del instead.
func TestKeyspaceNotifyZsetStoreEvents(t *testing.T) {
	srv := startPubsubServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	send(t, pubNc, "CONFIG", "SET", "notify-keyspace-events", "KEA")
	expect(t, pubBr, "+OK\r\n")
	send(t, subNc, "PSUBSCRIBE", "__keyevent@0__:*")
	if k, ch, n := readSubConfirm(t, subBr); k != "psubscribe" || ch != "__keyevent@0__:*" || n != 1 {
		t.Fatalf("psubscribe confirm = %q %q %d", k, ch, n)
	}

	// Two co-located sources under one hash tag so the STORE stays on one shard.
	send(t, pubNc, "ZADD", "{t}s1", "1", "a", "2", "b")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zadd", "{t}s1")
	send(t, pubNc, "ZADD", "{t}s2", "3", "b", "4", "c")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zadd", "{t}s2")

	// ZUNIONSTORE writes a non-empty result: zunionstore.
	send(t, pubNc, "ZUNIONSTORE", "{t}d", "2", "{t}s1", "{t}s2")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zunionstore", "{t}d")

	// ZRANGESTORE fires zrangestore on a non-empty selection.
	send(t, pubNc, "ZRANGESTORE", "{t}d2", "{t}s1", "0", "-1")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zrangestore", "{t}d2")

	// An empty ZINTERSTORE result over an existing destination deletes it: the generic
	// del, not a store event. {t}s1 and {t}d2 hold {a,b}; intersect with a disjoint
	// source to force the empty result.
	send(t, pubNc, "ZADD", "{t}s3", "9", "z")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "zadd", "{t}s3")
	send(t, pubNc, "ZINTERSTORE", "{t}d", "2", "{t}s1", "{t}s3")
	if n := readIntFrom(t, pubBr); n != 0 {
		t.Fatalf("ZINTERSTORE empty = %d, want 0", n)
	}
	wantEvent(t, subBr, "del", "{t}d")
}
