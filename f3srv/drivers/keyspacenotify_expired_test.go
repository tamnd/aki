package drivers

import (
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The expired keyspace event (spec 2064/f3/11; redis's 'x' class). When a key's
// deadline passes, the two paths that actually reap it both publish expired: the
// lazy peek on the next touch, and the background active cycle on an untouched key.
// These tests enable the class (KEA covers 'x'), subscribe to the keyevent family,
// and drive each path.
//
// The notify mask is a process global, so each test resets it on cleanup to keep
// the package's other tests, which assume notifications off, order-independent.

// TestKeyspaceNotifyExpiredActiveString drives the active-cycle path on the string
// store: a key with a short TTL is never read again, so the DBSIZE polls that wake
// the shard let the active cycle reap it, and that reap publishes expired on the
// keyevent channel. This is the store-sink route (the store has no cx of its own),
// so it also proves the sink is wired to the worker's publisher.
func TestKeyspaceNotifyExpiredActiveString(t *testing.T) {
	srv := startPubsubServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })
	// This test drives the active cycle, so it must run with the cycle on regardless
	// of what an earlier test left the process-global toggle at.
	shard.SetActiveExpire(true)
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	send(t, pubNc, "CONFIG", "SET", "notify-keyspace-events", "KEA")
	expect(t, pubBr, "+OK\r\n")

	send(t, subNc, "SUBSCRIBE", "__keyevent@0__:expired")
	if k, ch, n := readSubConfirm(t, subBr); k != "subscribe" || ch != "__keyevent@0__:expired" || n != 1 {
		t.Fatalf("subscribe confirm = %q %q %d", k, ch, n)
	}

	send(t, pubNc, "SET", "vk", "v", "PX", "250")
	expect(t, pubBr, "+OK\r\n")

	if got := waitDBSize(t, pubNc, pubBr, 0, 3*time.Second); got != 0 {
		t.Fatalf("DBSIZE never fell to 0, the active cycle did not reap vk: got %d", got)
	}

	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "__keyevent@0__:expired" || msg != "vk" {
		t.Fatalf("expired push = %q %q %q, want message __keyevent@0__:expired vk", k, ch, msg)
	}
}

// TestKeyspaceNotifyExpiredLazyCollection drives the lazy peek path on a collection
// registry with the active cycle turned off, so the key lingers until the touch
// reaps it and only the lazy drop can publish. The touch is a read on the same key
// (EXISTS), which peek reaps on the way through.
func TestKeyspaceNotifyExpiredLazyCollection(t *testing.T) {
	srv := startPubsubServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })
	defer shard.SetActiveExpire(true)
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	send(t, pubNc, "CONFIG", "SET", "notify-keyspace-events", "KEA")
	expect(t, pubBr, "+OK\r\n")
	send(t, pubNc, "DEBUG", "SET-ACTIVE-EXPIRE", "0")
	expect(t, pubBr, "+OK\r\n")

	send(t, subNc, "SUBSCRIBE", "__keyevent@0__:expired")
	if k, ch, n := readSubConfirm(t, subBr); k != "subscribe" || ch != "__keyevent@0__:expired" || n != 1 {
		t.Fatalf("subscribe confirm = %q %q %d", k, ch, n)
	}

	send(t, pubNc, "ZADD", "zk", "1", "m")
	expect(t, pubBr, ":1\r\n")
	send(t, pubNc, "PEXPIRE", "zk", "40")
	expect(t, pubBr, ":1\r\n")

	// Well past the 40ms deadline; with the active cycle off the key is still counted
	// until the touch below reaps it.
	time.Sleep(120 * time.Millisecond)

	send(t, pubNc, "EXISTS", "zk")
	if n := readIntFrom(t, pubBr); n != 0 {
		t.Fatalf("EXISTS zk after TTL = %d, want 0 (the touch should reap it)", n)
	}

	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "__keyevent@0__:expired" || msg != "zk" {
		t.Fatalf("expired push = %q %q %q, want message __keyevent@0__:expired zk", k, ch, msg)
	}
}
