package drivers

import (
	"bufio"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The set-type keyspace data events (spec 2064/f3/11:602, class 's'). Every set
// write fires its redis event from the effect layer: sadd, srem, spop, the STORE
// forms, and SMOVE (which fires srem on the source and sadd on the destination,
// not a dedicated event). An op that empties a set fires the generic del after its
// own event.
//
// The notify mask is a process global, so each test resets it on cleanup.

// readEvent reads one keyevent push and returns its channel event name and the key
// it carried. It exists so a test can assert an ordered stream of events off one
// subscriber without repeating the readMessage shape.
func readEvent(t *testing.T, br *bufio.Reader) (event, key string) {
	t.Helper()
	k, _, ch, msg := readPMessage(t, br)
	if k != "pmessage" {
		t.Fatalf("expected pmessage push, got %q on %q", k, ch)
	}
	const pfx = "__keyevent@0__:"
	if len(ch) <= len(pfx) || ch[:len(pfx)] != pfx {
		t.Fatalf("push channel %q is not a keyevent channel", ch)
	}
	return ch[len(pfx):], msg
}

// TestKeyspaceNotifySetEvents subscribes to every set event channel by pattern and
// walks a set through sadd, srem (down to empty, so del follows), spop, and the
// three STORE forms, asserting the ordered event stream. Pattern subscription lets
// one connection see every event in commit order.
func TestKeyspaceNotifySetEvents(t *testing.T) {
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

	// sadd on a fresh key.
	send(t, pubNc, "SADD", "s", "a", "b", "c")
	if n := readIntFrom(t, pubBr); n != 3 {
		t.Fatalf("SADD = %d, want 3", n)
	}
	wantEvent(t, subBr, "sadd", "s")

	// srem one member: srem, no del (set still has two).
	send(t, pubNc, "SREM", "s", "a")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("SREM = %d, want 1", n)
	}
	wantEvent(t, subBr, "srem", "s")

	// srem the rest: srem, then del as the set empties.
	send(t, pubNc, "SREM", "s", "b", "c")
	if n := readIntFrom(t, pubBr); n != 2 {
		t.Fatalf("SREM = %d, want 2", n)
	}
	wantEvent(t, subBr, "srem", "s")
	wantEvent(t, subBr, "del", "s")

	// spop the only member of a one-element set: spop, then del.
	send(t, pubNc, "SADD", "p", "x")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("SADD p = %d, want 1", n)
	}
	wantEvent(t, subBr, "sadd", "p")
	send(t, pubNc, "SPOP", "p")
	expect(t, pubBr, "$1\r\nx\r\n")
	wantEvent(t, subBr, "spop", "p")
	wantEvent(t, subBr, "del", "p")

	// STORE forms fire their own event on the destination.
	send(t, pubNc, "SADD", "a1", "1", "2", "3")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "sadd", "a1")
	send(t, pubNc, "SADD", "a2", "2", "3", "4")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "sadd", "a2")

	send(t, pubNc, "SINTERSTORE", "d", "a1", "a2")
	if n := readIntFrom(t, pubBr); n != 2 {
		t.Fatalf("SINTERSTORE = %d, want 2", n)
	}
	wantEvent(t, subBr, "sinterstore", "d")

	send(t, pubNc, "SUNIONSTORE", "d", "a1", "a2")
	if n := readIntFrom(t, pubBr); n != 4 {
		t.Fatalf("SUNIONSTORE = %d, want 4", n)
	}
	wantEvent(t, subBr, "sunionstore", "d")

	send(t, pubNc, "SDIFFSTORE", "d", "a1", "a2")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("SDIFFSTORE = %d, want 1", n)
	}
	wantEvent(t, subBr, "sdiffstore", "d")
}

// TestKeyspaceNotifySmoveEvents pins SMOVE's event shape: srem on the source (plus
// del when the move empties it) and sadd on the destination, matching redis rather
// than a made-up smove event.
func TestKeyspaceNotifySmoveEvents(t *testing.T) {
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

	// One member in src, so the move empties it: expect srem, del on src, sadd on dst.
	send(t, pubNc, "SADD", "src", "m")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "sadd", "src")

	send(t, pubNc, "SMOVE", "src", "dst", "m")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("SMOVE = %d, want 1", n)
	}
	wantEvent(t, subBr, "srem", "src")
	wantEvent(t, subBr, "del", "src")
	wantEvent(t, subBr, "sadd", "dst")
}

// wantEvent reads the next keyevent push and fails unless it is exactly the event
// and key expected. It centralizes the ordered-stream assertion.
func wantEvent(t *testing.T, br *bufio.Reader, event, key string) {
	t.Helper()
	gotEvent, gotKey := readEvent(t, br)
	if gotEvent != event || gotKey != key {
		t.Fatalf("event = %q %q, want %q %q", gotEvent, gotKey, event, key)
	}
}
