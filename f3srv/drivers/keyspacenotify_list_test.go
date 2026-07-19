package drivers

import (
	"bufio"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The list-type keyspace data events (spec 2064/f3/11:602, class 'l'). Every list
// write fires its redis event named by the touched end: LPUSH/LPUSHX fire lpush,
// RPUSH/RPUSHX fire rpush, LPOP/BLPOP fire lpop, RPOP/BRPOP fire rpop, LSET fires
// lset, LTRIM fires ltrim, LREM fires lrem, LINSERT fires linsert, and the pop
// forms with a count fire one event per command, not per element. A command that
// empties the key fires the generic del after its type event. LMOVE and its
// spellings (RPOPLPUSH, BLMOVE, BRPOPLPUSH, LMPOP/BLMPOP move-nothing aside) fire
// the pop event on the source end and the push event on the destination end.
//
// The notify mask is a process global, so the test resets it on cleanup.

// event is one keyevent push, an (event, key) pair. The list stream is asserted
// as an unordered multiset per step because a cross-shard move fires its pop and
// push on two owners whose commit order is not the argument order.
type kevent struct{ event, key string }

// wantEvents reads len(want) keyevent pushes and fails unless the multiset of
// (event, key) pairs read equals want, in any order. It covers the multi-event
// steps (a move fires a pop and a push, a draining pop fires its type event then
// del) whose fire order is not guaranteed across shards.
func wantEvents(t *testing.T, br *bufio.Reader, want ...kevent) {
	t.Helper()
	remaining := make(map[kevent]int, len(want))
	for _, w := range want {
		remaining[w]++
	}
	for range want {
		e, k := readEvent(t, br)
		got := kevent{e, k}
		if remaining[got] == 0 {
			t.Fatalf("event %q on %q not in wanted set %v", e, k, want)
		}
		remaining[got]--
	}
}

// TestKeyspaceNotifyListEvents walks a list key through the single-key write
// family and asserts the ordered event per command, including the generic del
// fired when a pop drains the key.
func TestKeyspaceNotifyListEvents(t *testing.T) {
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

	// A single co-located key gives a deterministic, ordered stream.
	send(t, pubNc, "RPUSH", "l", "a", "b", "c") // [a b c]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "rpush", "l")

	send(t, pubNc, "LPUSH", "l", "x") // [x a b c]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "lpush", "l")

	send(t, pubNc, "LSET", "l", "0", "y") // [y a b c]
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "lset", "l")

	send(t, pubNc, "LINSERT", "l", "BEFORE", "a", "z") // [y z a b c]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "linsert", "l")

	send(t, pubNc, "LREM", "l", "1", "z") // [y a b c]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "lrem", "l")

	send(t, pubNc, "LTRIM", "l", "0", "2") // [y a b]
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "ltrim", "l")

	send(t, pubNc, "LPOP", "l") // [a b]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "lpop", "l")

	send(t, pubNc, "RPOP", "l") // [a]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "rpop", "l")

	// The last element leaves: lpop then the generic del.
	send(t, pubNc, "LPOP", "l") // []
	readRESP(t, pubBr)
	wantEvents(t, subBr, kevent{"lpop", "l"}, kevent{"del", "l"})

	// The count form fires one event per command, not per element.
	send(t, pubNc, "RPUSH", "c", "a", "b", "c", "d") // [a b c d]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "rpush", "c")

	send(t, pubNc, "LPOP", "c", "2") // [c d]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "lpop", "c")

	// A count pop that drains the key fires the one type event then del.
	send(t, pubNc, "RPOP", "c", "2") // []
	readRESP(t, pubBr)
	wantEvents(t, subBr, kevent{"rpop", "c"}, kevent{"del", "c"})

	// LMPOP fires one pop event on the served key, named by its requested end.
	send(t, pubNc, "RPUSH", "mp", "1", "2") // [1 2]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "rpush", "mp")

	send(t, pubNc, "LMPOP", "1", "mp", "LEFT", "COUNT", "1") // [2]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "lpop", "mp")

	send(t, pubNc, "LMPOP", "1", "mp", "RIGHT") // [] drains
	readRESP(t, pubBr)
	wantEvents(t, subBr, kevent{"rpop", "mp"}, kevent{"del", "mp"})
}

// TestKeyspaceNotifyListMoveEvents pins the two-key move family: LMOVE, RPOPLPUSH,
// and their blocking spellings fire the pop event on the source end and the push
// event on the destination end, plus a generic del when the source drains. The
// pair is asserted unordered because a cross-shard move commits its hops in owner
// order, not argument order.
func TestKeyspaceNotifyListMoveEvents(t *testing.T) {
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

	send(t, pubNc, "RPUSH", "src", "a", "b") // src [a b]
	readRESP(t, pubBr)
	wantEvent(t, subBr, "rpush", "src")

	// LMOVE src dst LEFT RIGHT: pop the source head, push the destination tail. The
	// source keeps an element, so no del.
	send(t, pubNc, "LMOVE", "src", "dst", "LEFT", "RIGHT") // src [b] dst [a]
	readRESP(t, pubBr)
	wantEvents(t, subBr, kevent{"lpop", "src"}, kevent{"rpush", "dst"})

	// RPOPLPUSH src dst: pop the source tail (drains src) and push the destination
	// head. The drained source also fires the generic del.
	send(t, pubNc, "RPOPLPUSH", "src", "dst") // src [] dst [b a]
	readRESP(t, pubBr)
	wantEvents(t, subBr, kevent{"rpop", "src"}, kevent{"del", "src"}, kevent{"lpush", "dst"})
}
