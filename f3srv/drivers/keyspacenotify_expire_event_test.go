package drivers

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The expire generic event (spec 2064/f3/11; redis's "expire", class 'g'). The
// EXPIRE family fires it whenever a command actually installs a key-level deadline,
// across every keyspace, since all six types route the deadline through one shared
// core. The past-instant quirk (a deadline at or before now) deletes the key and
// fires del instead, which these tests also pin.
//
// The notify mask is a process global, so each test resets it on cleanup.

// TestKeyspaceNotifyExpireEvent enables the generic class and checks EXPIRE on a
// string key and PEXPIRE on a zset each publish expire on the keyevent channel. Two
// types, one shared core, so this proves the seam fires regardless of keyspace.
func TestKeyspaceNotifyExpireEvent(t *testing.T) {
	srv := startPubsubServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	send(t, pubNc, "CONFIG", "SET", "notify-keyspace-events", "KEg")
	expect(t, pubBr, "+OK\r\n")

	send(t, subNc, "SUBSCRIBE", "__keyevent@0__:expire")
	if k, ch, n := readSubConfirm(t, subBr); k != "subscribe" || ch != "__keyevent@0__:expire" || n != 1 {
		t.Fatalf("subscribe confirm = %q %q %d", k, ch, n)
	}

	send(t, pubNc, "SET", "sk", "v")
	expect(t, pubBr, "+OK\r\n")
	send(t, pubNc, "EXPIRE", "sk", "100")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("EXPIRE sk = %d, want 1", n)
	}
	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "__keyevent@0__:expire" || msg != "sk" {
		t.Fatalf("string expire push = %q %q %q, want message __keyevent@0__:expire sk", k, ch, msg)
	}

	send(t, pubNc, "ZADD", "zk", "1", "m")
	expect(t, pubBr, ":1\r\n")
	send(t, pubNc, "PEXPIRE", "zk", "100000")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("PEXPIRE zk = %d, want 1", n)
	}
	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "__keyevent@0__:expire" || msg != "zk" {
		t.Fatalf("zset expire push = %q %q %q, want message __keyevent@0__:expire zk", k, ch, msg)
	}
}

// TestKeyspaceNotifyExpirePastInstantDeletes checks the redis quirk: EXPIRE with a
// deadline at or before now deletes the key and returns 1, and the event is del,
// not expire. The subscriber watches both channels so a wrong choice would be
// caught: only the del push should arrive.
func TestKeyspaceNotifyExpirePastInstantDeletes(t *testing.T) {
	srv := startPubsubServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	send(t, pubNc, "CONFIG", "SET", "notify-keyspace-events", "KEg")
	expect(t, pubBr, "+OK\r\n")

	send(t, subNc, "SUBSCRIBE", "__keyevent@0__:del", "__keyevent@0__:expire")
	for i, want := range []string{"__keyevent@0__:del", "__keyevent@0__:expire"} {
		if k, ch, n := readSubConfirm(t, subBr); k != "subscribe" || ch != want || int(n) != i+1 {
			t.Fatalf("subscribe confirm %d = %q %q %d", i, k, ch, n)
		}
	}

	send(t, pubNc, "SET", "gk", "v")
	expect(t, pubBr, "+OK\r\n")
	send(t, pubNc, "EXPIRE", "gk", "-1")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("EXPIRE gk -1 = %d, want 1 (the past-instant delete quirk)", n)
	}

	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "__keyevent@0__:del" || msg != "gk" {
		t.Fatalf("push = %q %q %q, want message __keyevent@0__:del gk (past-instant EXPIRE deletes)", k, ch, msg)
	}
}
