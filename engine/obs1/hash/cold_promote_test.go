package hash

import (
	"testing"

	"github.com/tamnd/aki/engine/obs1/tier"
)

// The hash E slice: because a demote keeps every record on its field table probe
// and only sheds value bytes, reads were already transparent after D1/D2. What E
// adds is the write side. A confirming write to a cold field promotes its whole
// chunk back into the slab (one pread, values re-seated), and the cold-aware
// mutators never dereference a cold record's locator as a slab offset: overwrite
// promotes first, a cold delete charges only the resident field bytes and flags the
// chunk dirty, and a compaction moves field bytes but leaves the locator in place.

// TestHashPromoteOnOverwrite pins the confirming-write bring-up: an HSET onto a cold
// field re-seats every value in that field's chunk into the slab and clears their
// cold band, while records in other chunks stay cold, and the chunk's directory
// descriptor is dropped.
func TestHashPromoteOnOverwrite(t *testing.T) {
	cx, _ := coldCtx(t)
	h := coldNative(200, 40)
	want := map[string]string{}
	h.each(func(f, v []byte) { want[string(f)] = string(v) })

	if chunks := handDemote(t, cx.St, "k", h.ft); chunks < 2 {
		t.Fatalf("demote wrote %d chunks, want >= 2 to prove per-chunk promotion", chunks)
	}
	f := h.ft
	dirBefore := f.cold.dir.Len()

	// Pick a cold field and snapshot the ordinals whose value lives in its chunk.
	var field string
	var slot uint32
	for _, ord := range f.vec {
		field = string(f.fieldByOrd(ord))
		slot = locSlot(f.ents[ord].voff)
		break
	}
	inChunk := map[uint32]bool{}
	for _, ord := range f.vec {
		if locSlot(f.ents[ord].voff) == slot {
			inChunk[ord] = true
		}
	}

	h.set([]byte(field), []byte("promoted-value"))

	for _, ord := range f.vec {
		cold := f.ents[ord].band&tierCold != 0
		if inChunk[ord] && cold {
			t.Fatalf("record in the promoted chunk stayed cold")
		}
		if !inChunk[ord] && !cold {
			t.Fatalf("record outside the promoted chunk lost its cold band")
		}
	}
	if f.cold.dir.Len() != dirBefore-1 {
		t.Fatalf("dir len %d after promote, want %d", f.cold.dir.Len(), dirBefore-1)
	}

	// The overwritten field carries the new value; its promoted neighbours read their
	// originals back from the slab; cold neighbours still pread theirs.
	if got, _ := h.get([]byte(field)); string(got) != "promoted-value" {
		t.Fatalf("HGET %q = %q, want promoted-value", field, got)
	}
	for fld, v := range want {
		if fld == field {
			continue
		}
		if got, ok := h.get([]byte(fld)); !ok || string(got) != v {
			t.Fatalf("HGET %q = %q,%v want %q", fld, got, ok, v)
		}
	}
}

// TestHashPromoteReconcilesResident pins that the registry accounting stays exact
// across a promote: the write path brings a chunk resident (slab grows, directory
// shrinks) and note reconciles the running total to the recomputed footprint.
func TestHashPromoteReconcilesResident(t *testing.T) {
	cx, g := coldCtx(t)
	h := nativeReg(g, "k", 200, 100)
	g.demote(cx, []byte("k"))
	wantExact(t, g)
	dirBefore := h.ft.cold.dir.Len()

	var field string
	h.each(func(f, v []byte) {
		if field == "" {
			field = string(f)
		}
	})
	h.set([]byte(field), []byte("hot"))
	g.note(h)

	wantExact(t, g)
	if h.ft.cold.dir.Len() >= dirBefore {
		t.Fatalf("dir len %d after promote, want below %d", h.ft.cold.dir.Len(), dirBefore)
	}
	if got, _ := h.get([]byte(field)); string(got) != "hot" {
		t.Fatalf("HGET %q = %q, want hot", field, got)
	}
}

// TestHashColdDeleteMarksDirtyNotPromote pins that deleting a cold field leaves its
// chunk on disk: no descriptor is dropped, the chunk is flagged dirty for a later
// repack, and the field's cold neighbours stay cold and readable.
func TestHashColdDeleteMarksDirtyNotPromote(t *testing.T) {
	cx, _ := coldCtx(t)
	h := coldNative(200, 40)
	if chunks := handDemote(t, cx.St, "k", h.ft); chunks < 2 {
		t.Fatalf("demote wrote %d chunks, want >= 2", chunks)
	}
	f := h.ft
	dirBefore := f.cold.dir.Len()

	var field string
	var hh uint64
	var slot uint32
	for _, ord := range f.vec {
		field = string(f.fieldByOrd(ord))
		hh = fieldDisc(f.fieldByOrd(ord))
		slot = locSlot(f.ents[ord].voff)
		break
	}

	if !h.del([]byte(field)) {
		t.Fatalf("del %q missed", field)
	}
	if f.has([]byte(field)) {
		t.Fatal("deleted field still present")
	}

	stillCold := false
	for _, ord := range f.vec {
		if locSlot(f.ents[ord].voff) == slot && f.ents[ord].band&tierCold != 0 {
			stillCold = true
		}
	}
	if !stillCold {
		t.Fatal("a cold delete promoted the chunk instead of leaving it cold")
	}
	if f.cold.dir.Len() != dirBefore {
		t.Fatalf("dir len %d after cold delete, want %d unchanged", f.cold.dir.Len(), dirBefore)
	}
	idx, ok := f.cold.dir.Floor(discOf(hh))
	if !ok {
		t.Fatal("floor missed the deleted field's chunk")
	}
	if _, _, status := f.cold.dir.At(idx); status&tier.DescDirty == 0 {
		t.Fatal("a cold delete did not flag the chunk dirty")
	}
}

// TestHashCompactSkipsColdValues pins that a slab compaction triggered while records
// are cold repacks only the resident field bytes and never dereferences a cold
// record's locator as a slab offset. After a demote leaves the slab holding fields
// only, a resident field with a wide value is added and deleted so its dead bytes
// cross the compaction threshold; a cold-unsafe compaction would panic here.
func TestHashCompactSkipsColdValues(t *testing.T) {
	cx, _ := coldCtx(t)
	h := coldNative(200, 40)
	want := map[string]string{}
	h.each(func(f, v []byte) { want[string(f)] = string(v) })
	handDemote(t, cx.St, "k", h.ft)
	f := h.ft

	wide := make([]byte, 5000)
	for i := range wide {
		wide[i] = 'x'
	}
	h.set([]byte("resident"), wide)
	if !h.del([]byte("resident")) {
		t.Fatal("resident field vanished before delete")
	}

	for fld, v := range want {
		got, ok := h.get([]byte(fld))
		if !ok || string(got) != v {
			t.Fatalf("HGET %q = %q,%v want %q after compaction", fld, got, ok, v)
		}
		if !f.has([]byte(fld)) {
			t.Fatalf("HEXISTS %q false after compaction", fld)
		}
	}
	if f.has([]byte("resident")) {
		t.Fatal("deleted resident field survived")
	}
}
