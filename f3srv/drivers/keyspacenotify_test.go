package drivers

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// Keyspace notifications at the driver seam (spec 2064/f3/11; redis's
// notify-keyspace-events). With the class enabled, a write publishes the event
// name on __keyspace@0__:<key> and the key on __keyevent@0__:<event>, the two
// channel families a client subscribes to for cache invalidation. These tests
// enable the classes over CONFIG SET, subscribe a second connection to both
// families, run a write on a third, and read the pushes back.
//
// The notify mask is a process global, so each test resets it on cleanup to keep
// the package's other tests, which assume notifications off, order-independent.

// TestKeyspaceNotifyDelBothChannels enables the generic class plus both channel
// families (KEA), then deletes a key and checks both the keyspace push (event name
// on the key's channel) and the keyevent push (key on the event's channel) arrive.
// The SET that creates the key fires no event in this slice (string data events
// are a later slice), so the only pushes are the two from DEL.
func TestKeyspaceNotifyDelBothChannels(t *testing.T) {
	srv := startPubsubServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	send(t, pubNc, "CONFIG", "SET", "notify-keyspace-events", "KEA")
	expect(t, pubBr, "+OK\r\n")

	// One connection watches every keyspace channel by pattern and the del event
	// channel exactly, so a single DEL lands on both.
	send(t, subNc, "PSUBSCRIBE", "__keyspace@0__:*")
	if k, ch, n := readSubConfirm(t, subBr); k != "psubscribe" || ch != "__keyspace@0__:*" || n != 1 {
		t.Fatalf("psubscribe confirm = %q %q %d", k, ch, n)
	}
	send(t, subNc, "SUBSCRIBE", "__keyevent@0__:del")
	if k, ch, n := readSubConfirm(t, subBr); k != "subscribe" || ch != "__keyevent@0__:del" || n != 2 {
		t.Fatalf("subscribe confirm = %q %q %d", k, ch, n)
	}

	send(t, pubNc, "SET", "foo", "bar")
	expect(t, pubBr, "+OK\r\n")
	send(t, pubNc, "DEL", "foo")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("DEL foo = %d, want 1", n)
	}

	// The keyspace publish fires first, then the keyevent publish, so the pattern
	// push arrives before the exact-channel push.
	if k, pat, ch, msg := readPMessage(t, subBr); k != "pmessage" || pat != "__keyspace@0__:*" || ch != "__keyspace@0__:foo" || msg != "del" {
		t.Fatalf("keyspace push = %q %q %q %q, want pmessage __keyspace@0__:* __keyspace@0__:foo del", k, pat, ch, msg)
	}
	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "__keyevent@0__:del" || msg != "foo" {
		t.Fatalf("keyevent push = %q %q %q, want message __keyevent@0__:del foo", k, ch, msg)
	}
}

// TestKeyspaceNotifyGenericEvents checks the other centralized generic events this
// slice emits: RENAME fires rename_from on the source and rename_to on the
// destination, and PERSIST fires persist. The subscriber watches the keyevent
// family for each event name.
func TestKeyspaceNotifyGenericEvents(t *testing.T) {
	srv := startPubsubServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	send(t, pubNc, "CONFIG", "SET", "notify-keyspace-events", "KEg")
	expect(t, pubBr, "+OK\r\n")

	send(t, subNc, "SUBSCRIBE", "__keyevent@0__:rename_from", "__keyevent@0__:rename_to", "__keyevent@0__:persist")
	for i, want := range []string{"__keyevent@0__:rename_from", "__keyevent@0__:rename_to", "__keyevent@0__:persist"} {
		if k, ch, n := readSubConfirm(t, subBr); k != "subscribe" || ch != want || int(n) != i+1 {
			t.Fatalf("subscribe confirm %d = %q %q %d, want subscribe %q %d", i, k, ch, n, want, i+1)
		}
	}

	send(t, pubNc, "SET", "src", "v", "EX", "100")
	expect(t, pubBr, "+OK\r\n")
	send(t, pubNc, "PERSIST", "src")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("PERSIST src = %d, want 1", n)
	}
	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "__keyevent@0__:persist" || msg != "src" {
		t.Fatalf("persist push = %q %q %q", k, ch, msg)
	}

	send(t, pubNc, "RENAME", "src", "dst")
	expect(t, pubBr, "+OK\r\n")
	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "__keyevent@0__:rename_from" || msg != "src" {
		t.Fatalf("rename_from push = %q %q %q", k, ch, msg)
	}
	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "__keyevent@0__:rename_to" || msg != "dst" {
		t.Fatalf("rename_to push = %q %q %q", k, ch, msg)
	}
}

// TestNotifyKeyspaceEventsConfigRoundTrip checks the config validates and
// normalizes the flag string the way redis does: KEA folds to AKE (the 'A' alias
// plus the channel-family letters), the empty string clears it, and a bad class
// letter is rejected. The config value is a process-global other tests in this
// package also mutate, so this test sets its own empty baseline rather than
// assuming the process-start default.
func TestNotifyKeyspaceEventsConfigRoundTrip(t *testing.T) {
	_, nc, br := startServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })

	send(t, nc, "CONFIG", "SET", "notify-keyspace-events", "")
	expect(t, br, "+OK\r\n")
	send(t, nc, "CONFIG", "GET", "notify-keyspace-events")
	expect(t, br, "*2\r\n$22\r\nnotify-keyspace-events\r\n$0\r\n\r\n")

	send(t, nc, "CONFIG", "SET", "notify-keyspace-events", "KEA")
	expect(t, br, "+OK\r\n")
	send(t, nc, "CONFIG", "GET", "notify-keyspace-events")
	expect(t, br, "*2\r\n$22\r\nnotify-keyspace-events\r\n$3\r\nAKE\r\n")

	send(t, nc, "CONFIG", "SET", "notify-keyspace-events", "Kq")
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading error reply: %v", err)
	}
	if len(line) < 4 || line[:4] != "-ERR" {
		t.Fatalf("CONFIG SET with a bad class letter = %q, want -ERR", line)
	}
}
