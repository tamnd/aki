package drivers

import (
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The hash-type keyspace data events (spec 2064/f3/11:602, class 'h'). Every hash
// write fires its redis event: HSET/HMSET/HSETNX fire hset, HDEL/HGETDEL fire hdel
// (once per command, not per field) plus the generic del when the last field
// leaves, HINCRBY fires hincrby and HINCRBYFLOAT fires hincrbyfloat. The field-TTL
// family fires hexpire on a set and hpersist on a clear, and a field whose TTL
// fires lazily on the next access publishes hexpired.
//
// The notify mask is a process global, so the test resets it on cleanup.

// TestKeyspaceNotifyHashEvents walks a hash key through the write and field-TTL
// families, asserting the ordered event stream on a single co-located key.
func TestKeyspaceNotifyHashEvents(t *testing.T) {
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

	// HSET fires one hset regardless of how many pairs it wrote.
	send(t, pubNc, "HSET", "h", "f1", "a", "f2", "b")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "hset", "h")

	// HMSET fires hset too.
	send(t, pubNc, "HMSET", "h", "f3", "c")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "hset", "h")

	// HSETNX on a fresh field fires hset; on an existing field it writes nothing and
	// fires nothing.
	send(t, pubNc, "HSETNX", "h", "f4", "d")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("HSETNX fresh = %d, want 1", n)
	}
	wantEvent(t, subBr, "hset", "h")
	send(t, pubNc, "HSETNX", "h", "f4", "z")
	if n := readIntFrom(t, pubBr); n != 0 {
		t.Fatalf("HSETNX existing = %d, want 0", n)
	}

	// HINCRBY fires hincrby, HINCRBYFLOAT fires hincrbyfloat.
	send(t, pubNc, "HINCRBY", "h", "n", "5")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "hincrby", "h")
	send(t, pubNc, "HINCRBYFLOAT", "h", "n", "1.5")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "hincrbyfloat", "h")

	// HDEL fires one hdel for the command; the key still has fields, so no del.
	send(t, pubNc, "HDEL", "h", "f1", "f2")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "hdel", "h")

	// HGETDEL fires hdel for its removals.
	send(t, pubNc, "HGETDEL", "h", "FIELDS", "1", "f3")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "hdel", "h")

	// Draining the last fields fires hdel then the generic del.
	send(t, pubNc, "HDEL", "h", "f4", "n")
	readIntFrom(t, pubBr)
	wantEvents(t, subBr, kevent{"hdel", "h"}, kevent{"del", "h"})
}

// TestKeyspaceNotifyHashFieldTTLEvents pins the field-TTL events: HEXPIRE fires
// hexpire, HPERSIST fires hpersist, and a field whose TTL fires lazily on the next
// touch publishes hexpired (with a generic del when it was the last field).
func TestKeyspaceNotifyHashFieldTTLEvents(t *testing.T) {
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

	send(t, pubNc, "HSET", "ht", "keep", "1", "gone", "2")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "hset", "ht")

	// HEXPIRE sets a field TTL: hexpire.
	send(t, pubNc, "HEXPIRE", "ht", "100", "FIELDS", "1", "keep")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "hexpire", "ht")

	// HPERSIST clears it: hpersist.
	send(t, pubNc, "HPERSIST", "ht", "FIELDS", "1", "keep")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "hpersist", "ht")

	// A field TTL in the past deletes the field synchronously: hdel, not hexpire.
	send(t, pubNc, "HPEXPIRE", "ht", "0", "FIELDS", "1", "keep")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "hdel", "ht")

	// Give the surviving field a short TTL, let it lapse, and touch the key: the
	// lazy reap fires hexpired, and since it was the last field, the generic del.
	send(t, pubNc, "HPEXPIRE", "ht", "20", "FIELDS", "1", "gone")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "hexpire", "ht")
	// Well past the 20ms field deadline; the lazy funnel reaps it on the next touch.
	time.Sleep(80 * time.Millisecond)
	// HGET touches the key through the lazy funnel, reaping the fired field.
	send(t, pubNc, "HGET", "ht", "gone")
	readRESP(t, pubBr)
	wantEvents(t, subBr, kevent{"hexpired", "ht"}, kevent{"del", "ht"})
}
