package stream

import (
	"strconv"
	"testing"
)

// Unit tests for the ID allocator and the band machinery, below the command
// harness so the coarse shard clock can be driven synthetically. The
// backward-clock monotonicity guarantee (section 3.6) is the headline here: a
// clock that steps back must still yield strictly increasing IDs.

func TestAllocAutoBackwardClock(t *testing.T) {
	s := newStream()
	id := mustAlloc(t, s, "*", 1000)
	if id != (streamID{1000, 0}) {
		t.Fatalf("first auto id = %v, want 1000-0", id)
	}
	s.lastID = id
	// The clock steps back to 500. max(500, lastID.ms=1000) keeps ms at 1000, so
	// the seq advances instead of the id going backward.
	id = mustAlloc(t, s, "*", 500)
	if id != (streamID{1000, 1}) {
		t.Fatalf("auto id under backward clock = %v, want 1000-1", id)
	}
	s.lastID = id
	// The clock recovers past lastID: a fresh ms resets the seq to 0.
	id = mustAlloc(t, s, "*", 2000)
	if id != (streamID{2000, 0}) {
		t.Fatalf("auto id after clock recovery = %v, want 2000-0", id)
	}
}

func TestAllocAutoSameMs(t *testing.T) {
	s := newStream()
	prev := streamID{}
	// A stuck clock (same ms every call) still advances via the seq.
	for i := 0; i < 5; i++ {
		id := mustAlloc(t, s, "*", 42)
		if !prev.less(id) {
			t.Fatalf("auto id %v not greater than previous %v", id, prev)
		}
		s.lastID = id
		prev = id
	}
	if prev != (streamID{42, 4}) {
		t.Fatalf("fifth same-ms auto id = %v, want 42-4", prev)
	}
}

func TestAllocPartial(t *testing.T) {
	s := newStream()
	s.lastID = streamID{5, 3}
	// Partial ms below lastID.ms is refused.
	if _, ok, _ := s.allocID([]byte("4-*"), 0); ok {
		t.Fatal("partial id below lastID.ms should fail")
	}
	// Partial ms equal to lastID.ms advances the seq.
	if id := mustAlloc(t, s, "5-*", 0); id != (streamID{5, 4}) {
		t.Fatalf("partial id = %v, want 5-4", id)
	}
	// Partial ms above lastID.ms resets the seq to 0.
	if id := mustAlloc(t, s, "9-*", 0); id != (streamID{9, 0}) {
		t.Fatalf("partial id = %v, want 9-0", id)
	}
}

func TestAllocExplicitErrors(t *testing.T) {
	s := newStream()
	if _, ok, msg := s.allocID([]byte("0-0"), 0); ok || msg != errZeroID {
		t.Fatalf("0-0 alloc = ok %v msg %q, want rejected with errZeroID", ok, msg)
	}
	s.lastID = streamID{5, 5}
	if _, ok, msg := s.allocID([]byte("5-5"), 0); ok || msg != errSmallerID {
		t.Fatalf("equal-to-top alloc = ok %v msg %q, want rejected with errSmallerID", ok, msg)
	}
	if _, ok, msg := s.allocID([]byte("garbage"), 0); ok || msg != errInvalidID {
		t.Fatalf("garbage alloc = ok %v msg %q, want rejected with errInvalidID", ok, msg)
	}
}

// TestBandUpgradeOnEntryCount pins the inline-to-native transition on the entry
// cap: the stream stays inline until the cap would break, then upgrades one-way.
func TestBandUpgradeOnEntryCount(t *testing.T) {
	s := newStream()
	fields := []field{{name: []byte("f"), value: []byte("v")}}
	for i := 0; i < inlineMaxEntries; i++ {
		s.appendEntry(streamID{1, uint64(i)}, fields)
	}
	if s.kind != bandInline {
		t.Fatalf("stream upgraded early at %d entries", inlineMaxEntries)
	}
	if got := len(s.blocks); got != 1 {
		t.Fatalf("inline stream holds %d blocks, want 1", got)
	}
	// The entry that breaks the cap flips the band.
	s.appendEntry(streamID{1, uint64(inlineMaxEntries)}, fields)
	if s.kind != bandNative {
		t.Fatal("stream did not upgrade past the inline entry cap")
	}
	if int(s.length) != inlineMaxEntries+1 {
		t.Fatalf("length = %d, want %d", s.length, inlineMaxEntries+1)
	}
}

// TestBandUpgradeOnBytes pins the transition on the byte cap: a few fat entries
// cross inlineMaxBytes before the entry cap.
func TestBandUpgradeOnBytes(t *testing.T) {
	s := newStream()
	big := make([]byte, 200)
	fields := []field{{name: []byte("f"), value: big}}
	s.appendEntry(streamID{1, 0}, fields)
	s.appendEntry(streamID{1, 1}, fields)
	if s.kind != bandInline {
		t.Fatal("stream upgraded before the byte cap")
	}
	// The third 200-plus-byte entry pushes the block past 512 bytes.
	s.appendEntry(streamID{1, 2}, fields)
	if s.kind != bandNative {
		t.Fatal("stream did not upgrade past the inline byte cap")
	}
}

// TestNativeSpansBlocks writes enough entries to close several native blocks and
// checks every one round-trips and the counters stay exact.
func TestNativeSpansBlocks(t *testing.T) {
	s := newStream()
	const n = 400 // > 128, so at least four blocks
	for i := 0; i < n; i++ {
		s.appendEntry(streamID{uint64(i), 0}, []field{{name: []byte("f"), value: []byte(strconv.Itoa(i))}})
	}
	if s.kind != bandNative {
		t.Fatal("stream should be native after 400 entries")
	}
	if len(s.blocks) < 4 {
		t.Fatalf("400 entries closed only %d blocks, want >= 4", len(s.blocks))
	}
	if int(s.length) != n || int(s.entriesAdded) != n {
		t.Fatalf("length %d entriesAdded %d, want %d each", s.length, s.entriesAdded, n)
	}
	// Every entry decodes back in order across the block boundaries.
	var got int
	var scratch []field
	for _, b := range s.blocks {
		b.walk(scratch, func(id streamID, f []field) bool {
			if id.ms != uint64(got) {
				t.Fatalf("entry %d has id %v", got, id)
			}
			if string(f[0].value) != strconv.Itoa(got) {
				t.Fatalf("entry %d value = %q", got, f[0].value)
			}
			got++
			return true
		})
	}
	if got != n {
		t.Fatalf("walked %d entries, want %d", got, n)
	}
}

// TestDeleteAcrossBlocks tombstones entries in different native blocks and checks
// length, maxDeletedID, and that a walk skips exactly the tombstoned entries.
func TestDeleteAcrossBlocks(t *testing.T) {
	s := newStream()
	const n = 300
	for i := 0; i < n; i++ {
		s.appendEntry(streamID{uint64(i), 0}, []field{{name: []byte("f"), value: []byte("v")}})
	}
	del := []streamID{{0, 0}, {150, 0}, {299, 0}}
	for _, id := range del {
		if !s.delete(id) {
			t.Fatalf("delete %v reported no removal", id)
		}
	}
	// A repeat delete and an absent id both remove nothing.
	if s.delete(streamID{150, 0}) {
		t.Fatal("re-delete of a tombstone removed an entry")
	}
	if s.delete(streamID{9999, 0}) {
		t.Fatal("delete of an absent id removed an entry")
	}
	if int(s.length) != n-len(del) {
		t.Fatalf("length = %d, want %d", s.length, n-len(del))
	}
	if s.maxDeletedID != (streamID{299, 0}) {
		t.Fatalf("maxDeletedID = %v, want 299-0", s.maxDeletedID)
	}
	// The tombstoned ids never appear in a walk; every other id does.
	deleted := map[streamID]bool{}
	for _, id := range del {
		deleted[id] = true
	}
	var scratch []field
	seen := map[uint64]bool{}
	for _, b := range s.blocks {
		b.walk(scratch, func(id streamID, _ []field) bool {
			if deleted[id] {
				t.Fatalf("walk yielded tombstoned id %v", id)
			}
			seen[id.ms] = true
			return true
		})
	}
	if len(seen) != n-len(del) {
		t.Fatalf("walk saw %d live entries, want %d", len(seen), n-len(del))
	}
}

func mustAlloc(t *testing.T, s *stream, arg string, nowMs uint64) streamID {
	t.Helper()
	id, ok, msg := s.allocID([]byte(arg), nowMs)
	if !ok {
		t.Fatalf("allocID(%q, %d) failed: %s", arg, nowMs, msg)
	}
	return id
}
