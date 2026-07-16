package hash

import (
	"fmt"
	"sort"
	"testing"
	"unsafe"

	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/engine/f3/tier"
)

// The hash cold read plumbing (spec 2064/f3/06 sections 6 and 7). A demote sheds a
// field's value to a cold chunk and leaves its field bytes and lengths resident, so
// the probe, HEXISTS, HKEYS, and HSTRLEN stay zero preads and only the value read
// preads. These tests hand-demote a native table, the D2 pass in miniature, and
// hold every read path to the pre-demote answer across the cold boundary, plus the
// resident-byte drop the shed values earn. The demote pass itself and the write
// paths' promote-first cold safety land in the following slices; here every write
// happens before the demote, so no resident mutator meets a cold record.

// fentrySizeStable pins the record cell width the accounting term counts: adding the
// band byte must not have grown the twenty-byte cell (it lands in the padding after
// flen), or ftable.residentBytes would drift.
func TestFentrySizeStable(t *testing.T) {
	if got := unsafe.Sizeof(fentry{}); got != fentryBytes {
		t.Fatalf("sizeof fentry %d, want fentryBytes %d", got, fentryBytes)
	}
}

// handDemote sheds every resident value in f to the cold region by hand, the D2
// demote pass in miniature: it packs each field-value pair in field-hash order into
// chunks, retiers each record's voff to a chunk locator with the cold band, and
// rebuilds the slab to hold only the still-resident field bytes. It installs the
// shared cold state and returns the number of chunks it wrote.
func handDemote(t *testing.T, st *store.Store, key string, f *ftable) int {
	t.Helper()
	f.cold = &coldChunks{st: st}

	type ent struct {
		hash uint64
		ord  uint32
	}
	var ents []ent
	for _, ord := range f.vec {
		e := &f.ents[ord]
		if e.band&tierCold != 0 {
			continue
		}
		ents = append(ents, ent{store.Hash(f.slab[e.foff : e.foff+uint32(e.flen)]), ord})
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].hash < ents[j].hash })

	var payload []byte
	var ords []uint32
	var firstHash uint64
	chunks := 0
	flush := func() {
		off, ok := st.AppendChunk(kindHash, 0, uint16(len(ords)), []byte(key), discOf(firstHash), payload)
		if !ok {
			t.Fatal("AppendChunk refused")
		}
		slot := uint32(len(f.cold.offs))
		f.cold.offs = append(f.cold.offs, off)
		f.cold.dir.Insert(discOf(firstHash), uint32(len(ords)), off)
		for j, ord := range ords {
			f.ents[ord].band |= tierCold
			f.ents[ord].voff = packLoc(slot, uint32(j))
		}
		payload = payload[:0]
		ords = ords[:0]
		chunks++
	}
	for i, e := range ents {
		if len(ords) == 0 {
			firstHash = e.hash
		}
		r := &f.ents[e.ord]
		field := f.slab[r.foff : r.foff+uint32(r.flen)]
		value := f.slab[r.voff : r.voff+r.vlen]
		payload = appendEntry(payload, field, value)
		ords = append(ords, e.ord)
		if len(payload) >= chunkByteTarget || len(ords) >= maxChunkEntry || i == len(ents)-1 {
			flush()
		}
	}

	// Rebuild the slab keeping only the resident field bytes; every value is now cold,
	// so its slab bytes are gone and voff carries a locator, not an offset.
	newslab := make([]byte, 0)
	for _, ord := range f.vec {
		e := &f.ents[ord]
		field := f.slab[e.foff : e.foff+uint32(e.flen)]
		e.foff = uint32(len(newslab))
		newslab = append(newslab, field...)
	}
	f.slab = newslab
	f.dead = 0
	return chunks
}

// coldNative builds a native hash of n fields each with a value w bytes wide,
// distinct so a read can target one without disturbing its neighbours. The values
// are wide enough that a demote sheds real bytes.
func coldNative(n, w int) *hash {
	h := forceNative(newHash())
	for i := 0; i < n; i++ {
		h.set([]byte(fmt.Sprintf("f%05d", i)), []byte(fmt.Sprintf("%0*d", w, i)))
	}
	return h
}

// TestColdValueReadsAreTransparent hand-demotes a native hash and holds every read
// to its pre-demote answer: HGET returns each value across the cold boundary, the
// resident field probe (HEXISTS) and length (HSTRLEN) answer without a chunk, an
// enumeration walks every pair, and a stranger still misses.
func TestColdValueReadsAreTransparent(t *testing.T) {
	cx, _ := coldCtx(t)
	h := coldNative(200, 40)

	// Snapshot the pre-demote answers.
	want := map[string]string{}
	h.each(func(f, v []byte) { want[string(f)] = string(v) })
	if len(want) != 200 {
		t.Fatalf("built %d fields, want 200", len(want))
	}

	if n := handDemote(t, cx.St, "k", h.ft); n < 2 {
		t.Fatalf("demote wrote %d chunks, want at least 2 (packing factor)", n)
	}

	// Every record is cold now: its value is shed, its field stays resident.
	for _, ord := range h.ft.vec {
		if h.ft.ents[ord].band&tierCold == 0 {
			t.Fatal("a record kept a resident value after a full demote")
		}
	}

	for f, v := range want {
		got, ok := h.get([]byte(f))
		if !ok {
			t.Fatalf("HGET %q missed after demote", f)
		}
		if string(got) != v {
			t.Fatalf("HGET %q = %q, want %q", f, got, v)
		}
		if !h.has([]byte(f)) {
			t.Fatalf("HEXISTS %q false after demote", f)
		}
		if h.strlen([]byte(f)) != len(v) {
			t.Fatalf("HSTRLEN %q = %d, want %d", f, h.strlen([]byte(f)), len(v))
		}
	}

	if _, ok := h.get([]byte("stranger")); ok {
		t.Fatal("HGET of an absent field hit after demote")
	}
	if h.has([]byte("stranger")) {
		t.Fatal("HEXISTS of an absent field hit after demote")
	}

	// The enumeration reads every pair back through the cold boundary.
	got := map[string]string{}
	h.each(func(f, v []byte) { got[string(f)] = string(v) })
	if len(got) != len(want) {
		t.Fatalf("enumeration returned %d pairs, want %d", len(got), len(want))
	}
	for f, v := range want {
		if got[f] != v {
			t.Fatalf("enumeration %q = %q, want %q", f, got[f], v)
		}
	}
}

// TestColdResidentBytesDrops holds the memory claim: shedding the values to the cold
// region drops the table's resident footprint below its pre-demote figure, the
// directory and offset table being the only resident cost left of the shed values.
func TestColdResidentBytesDrops(t *testing.T) {
	cx, _ := coldCtx(t)
	h := coldNative(300, 64)

	before := h.residentBytes()
	handDemote(t, cx.St, "k", h.ft)
	after := h.residentBytes()

	if after >= before {
		t.Fatalf("resident bytes %d not below the pre-demote %d", after, before)
	}
	// The cold directory and offset table are the resident cost of the shed values.
	if h.ft.cold.residentBytes() == 0 {
		t.Fatal("cold state reports zero resident bytes after a demote")
	}
}

// TestColdEntryTornReportsMiss covers the torn-frame guard: a value read whose
// packed payload is truncated reports a miss rather than returning garbage.
func TestColdEntryTornReportsMiss(t *testing.T) {
	full := appendEntry(nil, []byte("field"), []byte("value"))
	if _, _, ok := chunkEntry(full, 0); !ok {
		t.Fatal("a well-formed entry did not decode")
	}
	if _, _, ok := chunkEntry(full[:len(full)-1], 0); ok {
		t.Fatal("a truncated value decoded")
	}
	if _, _, ok := chunkEntry(full, 1); ok {
		t.Fatal("an out-of-range entry index decoded")
	}
}

// TestColdPairAndMarkDirty covers the two primitives the promote and cold-delete
// paths (PR E) lean on: pair reads a cold record's field and value back together,
// the promote re-seat read and the M8 recovery unit, and markDirty flags the owning
// chunk's descriptor for a later repack without touching the frame.
func TestColdPairAndMarkDirty(t *testing.T) {
	cx, _ := coldCtx(t)
	h := coldNative(80, 40)
	want := map[string]string{}
	h.each(func(f, v []byte) { want[string(f)] = string(v) })
	handDemote(t, cx.St, "k", h.ft)

	f := h.ft
	for _, ord := range f.vec {
		e := &f.ents[ord]
		field, value, ok := f.cold.pair(e.voff)
		if !ok {
			t.Fatalf("pair miss for cold record %d", ord)
		}
		if want[string(field)] != string(value) {
			t.Fatalf("pair %q = %q, want %q", field, value, want[string(field)])
		}
		// The re-seat read must agree with the resident field the probe still holds.
		if string(field) != string(f.fieldByOrd(ord)) {
			t.Fatalf("pair field %q disagrees with the resident field %q", field, f.fieldByOrd(ord))
		}
	}

	// markDirty on a field's hash flags exactly the owning chunk's descriptor.
	hh := store.Hash(f.fieldByOrd(f.vec[0]))
	f.cold.markDirty(hh)
	idx, ok := f.cold.dir.Floor(discOf(hh))
	if !ok {
		t.Fatal("directory floor missed a demoted field hash")
	}
	if _, _, status := f.cold.dir.At(idx); status&tier.DescDirty == 0 {
		t.Fatal("markDirty did not set the dirty status bit")
	}
}

// TestColdReadPathHelpersRoute exercises the by-ordinal value read and the scan page
// the enumeration commands lean on, so both routes cross the cold boundary.
func TestColdReadPathHelpersRoute(t *testing.T) {
	cx, _ := coldCtx(t)
	h := coldNative(40, 40)
	want := map[string]string{}
	h.each(func(f, v []byte) { want[string(f)] = string(v) })
	handDemote(t, cx.St, "k", h.ft)

	f := h.ft
	for i := 0; i < f.drawLen(); i++ {
		ord := f.ordAt(i)
		field := string(f.fieldByOrd(ord))
		value := string(f.valueByOrd(ord))
		if want[field] != value {
			t.Fatalf("by-ord %q = %q, want %q", field, value, want[field])
		}
		if f.vlenByOrd(ord) != len(want[field]) {
			t.Fatalf("resident vlen %q = %d, want %d", field, f.vlenByOrd(ord), len(want[field]))
		}
	}

	got := map[string]string{}
	f.scanPage(0, 1000, nil, func(field, value []byte) { got[string(field)] = string(value) })
	if len(got) != len(want) {
		t.Fatalf("scan returned %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("scan %q = %q, want %q", k, got[k], v)
		}
	}
}
