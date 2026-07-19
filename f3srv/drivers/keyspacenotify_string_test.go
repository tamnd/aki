package drivers

import (
	"bufio"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The string-type keyspace data events (spec 2064/f3/11:602, class '$'). Every
// string write fires its redis event: SET and its expiry-bearing siblings fire
// set (the expiry rides inside the set, no separate event), the arithmetic
// family fires incrby/decrby/incrbyfloat, APPEND fires append, SETRANGE fires
// setrange, and the removal forms fire the generic del/persist/expire rather
// than a string event.
//
// The notify mask is a process global, so each test resets it on cleanup.

// TestKeyspaceNotifyStringEvents subscribes to every event channel by pattern and
// walks a string key through the write family, asserting the ordered stream.
func TestKeyspaceNotifyStringEvents(t *testing.T) {
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

	// Plain SET fires set.
	send(t, pubNc, "SET", "s", "hello")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "set", "s")

	// SETEX fires set, the expiry rides inside.
	send(t, pubNc, "SETEX", "s", "100", "world")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "set", "s")

	// SETNX on an existing key writes nothing and fires nothing; on a fresh key
	// it fires set.
	send(t, pubNc, "SETNX", "s", "nope")
	if n := readIntFrom(t, pubBr); n != 0 {
		t.Fatalf("SETNX existing = %d, want 0", n)
	}
	send(t, pubNc, "SETNX", "n", "yes")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("SETNX fresh = %d, want 1", n)
	}
	wantEvent(t, subBr, "set", "n")

	// GETSET is a plain set for notifications.
	send(t, pubNc, "GETSET", "s", "next")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "set", "s")

	// APPEND fires append.
	send(t, pubNc, "APPEND", "s", "tail")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "append", "s")

	// SETRANGE fires setrange.
	send(t, pubNc, "SETRANGE", "s", "0", "X")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "setrange", "s")

	// The arithmetic family names the event by the command, not the sign.
	send(t, pubNc, "SET", "c", "10")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "set", "c")
	send(t, pubNc, "INCR", "c")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "incrby", "c")
	send(t, pubNc, "INCRBY", "c", "5")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "incrby", "c")
	send(t, pubNc, "DECR", "c")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "decrby", "c")
	send(t, pubNc, "DECRBY", "c", "3")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "decrby", "c")
	send(t, pubNc, "INCRBYFLOAT", "c", "1.5")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "incrbyfloat", "c")

	// GETDEL removes the key: the generic del, not a string event.
	send(t, pubNc, "GETDEL", "s")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "del", "s")

	// MSET fires set per pair. The pairs may hash to different shards, so the
	// events arrive in commit order across owners, not argument order; assert the
	// set of keys, not their sequence.
	send(t, pubNc, "MSET", "m1", "a", "m2", "b")
	expect(t, pubBr, "+OK\r\n")
	wantEventSet(t, subBr, "set", "m1", "m2")

	// MSETNX on all-fresh keys fires set per pair; when any key exists it writes
	// nothing and fires nothing.
	send(t, pubNc, "MSETNX", "x1", "a", "x2", "b")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("MSETNX fresh = %d, want 1", n)
	}
	wantEventSet(t, subBr, "set", "x1", "x2")
	send(t, pubNc, "MSETNX", "x1", "c", "x3", "d")
	if n := readIntFrom(t, pubBr); n != 0 {
		t.Fatalf("MSETNX with existing = %d, want 0", n)
	}
}

// TestKeyspaceNotifyGetexEvents pins GETEX's TTL-only events: persist fires only
// when a deadline was actually removed, and an expiry option fires the generic
// expire, matching redis rather than a string event.
func TestKeyspaceNotifyGetexEvents(t *testing.T) {
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

	send(t, pubNc, "SET", "g", "v")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "set", "g")

	// GETEX with an expiry option installs a deadline: the generic expire.
	send(t, pubNc, "GETEX", "g", "EX", "100")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "expire", "g")

	// GETEX PERSIST with a deadline present fires persist.
	send(t, pubNc, "GETEX", "g", "PERSIST")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "persist", "g")

	// GETEX PERSIST with no deadline fires nothing: the next asserted event must
	// be the set from the following write, not a spurious persist.
	send(t, pubNc, "GETEX", "g", "PERSIST")
	readBulkFrom(t, pubBr)
	send(t, pubNc, "SET", "g", "w")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "set", "g")
}

// wantEventSet reads len(keys) keyevent pushes and fails unless every one carries
// the given event and their keys are exactly the expected set, in any order. It
// exists for the fan commands (MSET, MSETNX) whose per-key events arrive in
// cross-shard commit order rather than argument order.
func wantEventSet(t *testing.T, br *bufio.Reader, event string, keys ...string) {
	t.Helper()
	want := make(map[string]bool, len(keys))
	for _, k := range keys {
		want[k] = true
	}
	for range keys {
		gotEvent, gotKey := readEvent(t, br)
		if gotEvent != event {
			t.Fatalf("event = %q %q, want event %q", gotEvent, gotKey, event)
		}
		if !want[gotKey] {
			t.Fatalf("event %q on unexpected key %q, want one of %v", event, gotKey, keys)
		}
		delete(want, gotKey)
	}
}
