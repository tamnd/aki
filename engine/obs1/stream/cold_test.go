package stream

import (
	"fmt"
	"testing"
)

// The stream cold chunk demotion (spec 2064/f3/06 sections 6 and 7, plan
// M7-slice-cold-chunk-stream, slice D). A demote pass sheds the log's oldest sealed
// blocks to the cold region, dropping their resident blobs while the block handles,
// headers, and master schemas stay resident, so a range read preads a shed block and
// stitches it to the resident tail transparently. These tests hold three contracts:
// a demote sheds front blocks and keeps the tail margin resident without moving the
// live count, a full read across the cold front reconstructs every entry
// byte-for-byte against the pre-demote read, and a promote brings a shed block back
// resident with its bytes intact.

// flatEntry is a range entry flattened to owned strings, independent of the block
// blobs the gather aliased, so a snapshot taken before a demote survives the demote
// dropping those blobs.
type flatEntry struct {
	id     streamID
	fields []string // name, value, name, value, ...
}

// flattenRange copies a gathered range into owned strings so the comparison does not alias
// any block blob (resident or the shared pread scratch).
func flattenRange(entries []rangeEntry) []flatEntry {
	out := make([]flatEntry, len(entries))
	for i, e := range entries {
		fs := make([]string, 0, 2*len(e.fields))
		for _, f := range e.fields {
			fs = append(fs, string(f.name), string(f.value))
		}
		out[i] = flatEntry{id: e.id, fields: fs}
	}
	return out
}

// snapshotAll reads every live entry as a forward XRANGE over the whole id space, the
// read a demote must leave unchanged.
func snapshotAll(s *stream) []flatEntry {
	lo := bound{id: streamID{ms: 0, seq: 0}}
	hi := bound{id: streamID{ms: ^uint64(0), seq: ^uint64(0)}}
	return flattenRange(s.collectRange(lo, hi, false, -1))
}

// sameFlat fails unless two flattened reads are identical, id and every field byte.
func sameFlat(t *testing.T, want, got []flatEntry) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("entry count %d != %d", len(got), len(want))
	}
	for i := range want {
		if want[i].id != got[i].id {
			t.Fatalf("entry %d id %v != %v", i, got[i].id, want[i].id)
		}
		if len(want[i].fields) != len(got[i].fields) {
			t.Fatalf("entry %d field count %d != %d", i, len(got[i].fields), len(want[i].fields))
		}
		for j := range want[i].fields {
			if want[i].fields[j] != got[i].fields[j] {
				t.Fatalf("entry %d field %d %q != %q", i, j, got[i].fields[j], want[i].fields[j])
			}
		}
	}
}

// buildLog fills a native stream past the tail margin so the demote pass has front
// blocks to shed. It returns the stream and a pre-demote snapshot of the whole log.
func buildLog(t *testing.T, g *reg, n uint64) (*stream, []flatEntry) {
	t.Helper()
	for i := uint64(1); i <= n; i++ {
		addEntry(g, "k", i, "f", fmt.Sprintf("v%d", i))
	}
	s := g.m["k"]
	if s.kind != bandNative {
		t.Fatalf("stream band %v, want native", s.kind)
	}
	if len(s.blocks) <= demoteTailMargin {
		t.Fatalf("stream holds %d blocks, want more than the %d-block tail margin", len(s.blocks), demoteTailMargin)
	}
	want := snapshotAll(s)
	if uint64(len(want)) != n {
		t.Fatalf("pre-demote read returned %d entries, want %d", len(want), n)
	}
	return s, want
}

// TestDemoteShedsFrontBlocksAndReadsBack is the D contract: a demote sheds the oldest
// sealed blocks, keeps the tail margin resident, leaves the live count untouched, and
// a full forward, reverse, and point read reconstructs every entry byte-for-byte
// across the cold front.
func TestDemoteShedsFrontBlocksAndReadsBack(t *testing.T) {
	cx, g := coldCtx(t)
	const n = 700 // 128 fixed-schema entries per block, so this seals five blocks under an open tail
	s, want := buildLog(t, g, n)
	beforeLen := s.length
	beforeResident := g.resident

	shed := s.demote(cx.St, []byte("k"))
	g.note(s)
	if shed == 0 {
		t.Fatal("demote shed no blocks from a multi-block stream")
	}

	// The shed blocks are the front ones and the tail margin stays resident.
	coldCount := 0
	for i, b := range s.blocks {
		if b.cold() {
			coldCount++
			if b.blob != nil {
				t.Fatalf("cold block %d kept its blob", i)
			}
			if b.coldOff == 0 {
				t.Fatalf("cold block %d has no cold offset", i)
			}
		}
	}
	if coldCount != shed {
		t.Fatalf("%d blocks cold, demote reported %d shed", coldCount, shed)
	}
	for i := len(s.blocks) - demoteTailMargin; i < len(s.blocks); i++ {
		if s.blocks[i].cold() {
			t.Fatalf("tail-margin block %d is cold", i)
		}
	}

	// A demote moves bytes between tiers; it drops no entries.
	if s.length != beforeLen {
		t.Fatalf("length %d after demote, want %d", s.length, beforeLen)
	}

	// The running total fell by the shed blob bytes net of the demote directory the
	// cold state now keeps resident, and it still equals the walked sum.
	if g.resident >= beforeResident {
		t.Fatalf("resident %d did not fall after demote, was %d", g.resident, beforeResident)
	}
	wantExact(t, g)

	// The whole log reads back byte-for-byte: a forward XRANGE preads each cold block and
	// stitches it to the resident tail.
	sameFlat(t, want, snapshotAll(s))

	// A reverse read reconstructs the same entries in descending order.
	rev := flattenRange(s.collectRange(
		bound{id: streamID{ms: 0, seq: 0}},
		bound{id: streamID{ms: ^uint64(0), seq: ^uint64(0)}},
		true, -1))
	if len(rev) != n {
		t.Fatalf("reverse read %d entries, want %d", len(rev), n)
	}
	for i := range rev {
		if rev[i].id != want[n-1-i].id {
			t.Fatalf("reverse entry %d id %v, want %v", i, rev[i].id, want[n-1-i].id)
		}
	}

	// A point read of an id in a cold block returns its fields (entryAt on the cold path).
	fields, ok := s.entryAt(want[0].id)
	if !ok {
		t.Fatalf("entryAt(%v) in a cold block missed", want[0].id)
	}
	if len(fields) != 1 || string(fields[0].name) != want[0].fields[0] || string(fields[0].value) != want[0].fields[1] {
		t.Fatalf("entryAt cold entry %v, want %v", fields, want[0].fields)
	}
}

// TestDemoteRangeCrossesTierBoundary reads a window that opens in a cold front block
// and closes in the resident tail, the one-continuous-walk-with-a-pread-per-cold-block
// path of section 6.4, and holds it to the same window read before the demote.
func TestDemoteRangeCrossesTierBoundary(t *testing.T) {
	cx, g := coldCtx(t)
	const n = 700
	s, want := buildLog(t, g, n)

	// A window from an early (soon-cold) id to a late (resident) id, read before demote.
	lo := bound{id: streamID{ms: 50, seq: 0}}
	hi := bound{id: streamID{ms: 600, seq: 0}}
	before := flattenRange(s.collectRange(lo, hi, false, -1))
	if len(before) == 0 {
		t.Fatal("pre-demote window read nothing")
	}
	_ = want

	if s.demote(cx.St, []byte("k")) == 0 {
		t.Fatal("demote shed nothing")
	}
	g.note(s)

	after := flattenRange(s.collectRange(lo, hi, false, -1))
	sameFlat(t, before, after)

	// A COUNT-bounded read across the boundary stops at the limit and still matches the
	// leading slice of the full window.
	const limit = 200
	capped := flattenRange(s.collectRange(lo, hi, false, limit))
	if len(capped) != limit {
		t.Fatalf("count-bounded read returned %d, want %d", len(capped), limit)
	}
	sameFlat(t, before[:limit], capped)
}

// TestPromoteBringsColdBlockResident is the E-facing half the D machinery already
// carries: a promote preads a shed block, copies its blob back resident, clears the
// cold marker, drops the demote descriptor, and grows the running total, all without
// disturbing the log's read-back.
func TestPromoteBringsColdBlockResident(t *testing.T) {
	cx, g := coldCtx(t)
	const n = 700
	s, want := buildLog(t, g, n)

	if s.demote(cx.St, []byte("k")) == 0 {
		t.Fatal("demote shed nothing")
	}
	g.note(s)

	ci := -1
	for i, b := range s.blocks {
		if b.cold() {
			ci = i
			break
		}
	}
	if ci < 0 {
		t.Fatal("no cold block after demote")
	}

	beforeResident := g.resident
	s.promote(ci)
	g.note(s)

	if s.blocks[ci].cold() {
		t.Fatalf("block %d still cold after promote", ci)
	}
	if s.blocks[ci].blob == nil {
		t.Fatalf("promoted block %d has no resident blob", ci)
	}
	if s.blocks[ci].coldOff != 0 {
		t.Fatalf("promoted block %d kept a cold offset", ci)
	}
	if g.resident <= beforeResident {
		t.Fatalf("resident %d did not grow after promote, was %d", g.resident, beforeResident)
	}
	wantExact(t, g)

	// The whole log still reads back byte-for-byte with the block back resident.
	sameFlat(t, want, snapshotAll(s))

	// A second promote of the now-resident block is a no-op.
	steady := g.resident
	s.promote(ci)
	g.note(s)
	if g.resident != steady {
		t.Fatalf("promote of a resident block moved resident %d -> %d", steady, g.resident)
	}
}

// TestDemoteSecondQuantumSkipsColdFront runs two demote passes and asserts the second
// skips the blocks the first already shed, so a repeated pass on a stream whose front
// is already cold sheds only newly-eligible blocks and never re-appends a cold one.
func TestDemoteSecondQuantumSkipsColdFront(t *testing.T) {
	cx, g := coldCtx(t)
	// Enough blocks that the first quantum cannot shed the whole demotable front, so a
	// second pass has fresh front blocks to shed.
	const n = 128 * (demoteQuantum + demoteTailMargin + 3)
	s, want := buildLog(t, g, n)

	first := s.demote(cx.St, []byte("k"))
	g.note(s)
	if first != demoteQuantum {
		t.Fatalf("first demote shed %d, want the quantum %d", first, demoteQuantum)
	}

	second := s.demote(cx.St, []byte("k"))
	g.note(s)
	if second == 0 {
		t.Fatal("second demote shed nothing though demotable front blocks remained")
	}

	// Every cold block is contiguous from the front, so the two passes shed adjacent
	// runs with no gap and no re-demote.
	firstResident := -1
	for i, b := range s.blocks {
		if !b.cold() {
			firstResident = i
			break
		}
	}
	if firstResident != first+second {
		t.Fatalf("first resident block at %d, want the %d shed blocks contiguous from the front", firstResident, first+second)
	}
	wantExact(t, g)
	sameFlat(t, want, snapshotAll(s))
}
