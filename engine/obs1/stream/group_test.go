package stream

import (
	"testing"
)

// Unit tests for the group records below the command harness (spec 2064/f3/14
// section 4.4): attaching a group forces the native band, since the ledger has no
// packed-blob form, and an empty stream can still hold a group.

func TestGroupForcesNativeBand(t *testing.T) {
	s := newStream()
	fields := []field{{name: []byte("f"), value: []byte("v")}}
	// A handful of entries keeps the stream inline.
	for i := 0; i < 3; i++ {
		s.appendEntry(streamID{1, uint64(i)}, fields)
	}
	if s.kind != bandInline {
		t.Fatal("stream should still be inline before a group")
	}
	s.addGroup([]byte("g"), newGroup(streamID{}, 0, true))
	if s.kind != bandNative {
		t.Fatal("attaching a group must upgrade the stream to native")
	}
	// The inline entries survive the upgrade unchanged.
	if int(s.length) != 3 {
		t.Fatalf("length = %d after upgrade, want 3", s.length)
	}
	var seen int
	var scratch []field
	for _, b := range s.blocks {
		b.walk(scratch, func(streamID, []field) bool { seen++; return true })
	}
	if seen != 3 {
		t.Fatalf("walked %d entries after upgrade, want 3", seen)
	}
	if s.group([]byte("g")) == nil {
		t.Fatal("group missing after addGroup")
	}
}

func TestGroupOnEmptyStream(t *testing.T) {
	s := newStream()
	// A zero-entry stream can still take a group: it upgrades to native with an
	// empty directory and no blocks (the XGROUP CREATE MKSTREAM shape).
	s.addGroup([]byte("g"), newGroup(streamID{}, 0, true))
	if s.kind != bandNative {
		t.Fatal("empty stream did not upgrade on group create")
	}
	if len(s.blocks) != 0 {
		t.Fatalf("empty native stream holds %d blocks, want 0", len(s.blocks))
	}
	if s.dir == nil {
		t.Fatal("native stream must have a directory")
	}
	if s.group([]byte("missing")) != nil {
		t.Fatal("lookup of an absent group returned non-nil")
	}
}

func TestGroupConsumerLifecycle(t *testing.T) {
	grp := newGroup(streamID{}, 0, true)
	if !grp.createConsumer([]byte("c"), 0) {
		t.Fatal("first createConsumer should report created")
	}
	if grp.createConsumer([]byte("c"), 0) {
		t.Fatal("second createConsumer should report already present")
	}
	if grp.consumer([]byte("c")) == nil {
		t.Fatal("consumer missing after create")
	}
	// No delivery has run, so the consumer owns no pending entries.
	if n := grp.delConsumer([]byte("c")); n != 0 {
		t.Fatalf("delConsumer returned %d pending, want 0", n)
	}
	if grp.consumer([]byte("c")) != nil {
		t.Fatal("consumer still present after delConsumer")
	}
	if n := grp.delConsumer([]byte("absent")); n != 0 {
		t.Fatalf("delConsumer of an absent consumer returned %d, want 0", n)
	}
}
