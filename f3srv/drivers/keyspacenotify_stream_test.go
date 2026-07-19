package drivers

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The stream-type keyspace data events (spec 2064/f3/11:602, class 't'). Every
// stream write fires its redis event: XADD fires xadd (and a trailing xtrim when a
// trim clause drops entries), XTRIM fires xtrim, XDEL fires xdel, XSETID fires
// xsetid, the XGROUP subcommands fire xgroup-create/setid/destroy/createconsumer/
// delconsumer, and the claim family fires xclaim and xautoclaim when they transfer
// at least one pending entry. XACK fires nothing (it changes only the PEL, not the
// stream). Unlike every other collection, a stream keeps its key when it empties:
// XDEL and XTRIM never move lastID back and never fire a generic del.
//
// The notify mask is a process global, so the test resets it on cleanup.

// TestKeyspaceNotifyStreamEvents walks a stream through the single-key write family
// and asserts the ordered event per command, including that draining every entry
// fires no generic del.
func TestKeyspaceNotifyStreamEvents(t *testing.T) {
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

	// XADD fires one xadd per append.
	for _, id := range []string{"1-1", "2-1", "3-1"} {
		send(t, pubNc, "XADD", "s", id, "f", "v")
		readBulkFrom(t, pubBr)
		wantEvent(t, subBr, "xadd", "s")
	}

	// An XADD whose trim clause drops entries fires xadd then a trailing xtrim, in
	// order on the one owning shard. After this the stream holds [2-1 3-1 4-1].
	send(t, pubNc, "XADD", "s", "MAXLEN", "3", "4-1", "f", "v")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "xadd", "s")
	wantEvent(t, subBr, "xtrim", "s")

	// A standalone XTRIM that drops entries fires xtrim. Leaves [3-1 4-1].
	send(t, pubNc, "XTRIM", "s", "MAXLEN", "2")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "xtrim", "s")

	// XDEL fires xdel while an entry remains.
	send(t, pubNc, "XDEL", "s", "3-1")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "xdel", "s")

	// Deleting the last live entry still fires only xdel: a stream keeps its key when
	// it empties, so there is NO generic del here. Were one wrongly fired, the xsetid
	// assertion below would read it and mismatch.
	send(t, pubNc, "XDEL", "s", "4-1")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "xdel", "s")

	// XSETID on the still-present, now-empty key fires xsetid.
	send(t, pubNc, "XSETID", "s", "5-0")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "xsetid", "s")
}

// TestKeyspaceNotifyStreamGroupEvents pins the XGROUP subcommand events: each
// lifecycle subcommand fires its own xgroup-* event on the stream key.
func TestKeyspaceNotifyStreamGroupEvents(t *testing.T) {
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

	send(t, pubNc, "XADD", "g", "1-1", "f", "v")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "xadd", "g")

	send(t, pubNc, "XGROUP", "CREATE", "g", "grp", "0")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "xgroup-create", "g")

	send(t, pubNc, "XGROUP", "CREATECONSUMER", "g", "grp", "c1")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "xgroup-createconsumer", "g")

	send(t, pubNc, "XGROUP", "SETID", "g", "grp", "0")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "xgroup-setid", "g")

	send(t, pubNc, "XGROUP", "DELCONSUMER", "g", "grp", "c1")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "xgroup-delconsumer", "g")

	send(t, pubNc, "XGROUP", "DESTROY", "g", "grp")
	readIntFrom(t, pubBr)
	wantEvent(t, subBr, "xgroup-destroy", "g")
}

// TestKeyspaceNotifyStreamClaimEvents pins the ownership-transfer events: XCLAIM
// fires xclaim and XAUTOCLAIM fires xautoclaim once each when they actually move a
// pending entry to the target consumer. XREADGROUP delivery fires nothing.
func TestKeyspaceNotifyStreamClaimEvents(t *testing.T) {
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

	send(t, pubNc, "XADD", "cl", "1-1", "f", "v")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "xadd", "cl")
	send(t, pubNc, "XADD", "cl", "2-1", "f", "v")
	readBulkFrom(t, pubBr)
	wantEvent(t, subBr, "xadd", "cl")

	send(t, pubNc, "XGROUP", "CREATE", "cl", "grp", "0")
	expect(t, pubBr, "+OK\r\n")
	wantEvent(t, subBr, "xgroup-create", "cl")

	// Deliver both entries to c1, making them pending. XREADGROUP fires no event, so
	// the next assertion reads the xclaim below with nothing stale between them.
	send(t, pubNc, "XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "cl", ">")
	readRESP(t, pubBr)

	// XCLAIM transfers 1-1 to c2, firing one xclaim.
	send(t, pubNc, "XCLAIM", "cl", "grp", "c2", "0", "1-1")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "xclaim", "cl")

	// XAUTOCLAIM sweeps the remaining pending entries to c3, firing one xautoclaim.
	send(t, pubNc, "XAUTOCLAIM", "cl", "grp", "c3", "0", "0")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "xautoclaim", "cl")
}
