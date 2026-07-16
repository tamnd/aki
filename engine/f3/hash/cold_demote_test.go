package hash

import (
	"testing"
)

// The hash demote pass (spec 2064/f3/06 sections 6 and 7). D1 proved the cold value
// reads transparent given a hand-built cold layout; this drives the production pass
// through the registry wrapper the worker's demote loop calls, and holds the shed to
// its contract: the values leave the slab while the fields and the table stay
// resident, every read still answers across the tier boundary, and the registry's
// running total reconciles to the smaller footprint.

// nativeReg registers a native hash of n fields, each field and value w bytes wide,
// and reconciles it into the running total, the shape a run of HSETs leaves. The
// values are wide enough that the pass sheds real bytes and packs more than one
// chunk.
func nativeReg(g *reg, key string, n, w int) *hash {
	h := forceNative(newHash())
	g.m[key] = h
	pairs := fvPairs("", n, w)
	for i := 0; i < len(pairs); i += 2 {
		h.set([]byte(pairs[i]), []byte(pairs[i+1]))
	}
	g.note(h)
	return h
}

// TestDemotePassShedsValues drives the registry demote wrapper end to end: it sheds
// every field's value into the cold region, holds the field table resident, and
// reconciles the running total. HLEN is unchanged, HGET reads every value back across
// the boundary, HEXISTS and HSTRLEN answer without a chunk, and a second pass finds
// nothing left to shed.
func TestDemotePassShedsValues(t *testing.T) {
	cx, g := coldCtx(t)
	h := nativeReg(g, "k", 200, 100)

	want := map[string]string{}
	h.each(func(f, v []byte) { want[string(f)] = string(v) })
	if len(want) != 200 {
		t.Fatalf("built %d fields, want 200", len(want))
	}
	before := h.residentBytes()

	n := g.demote(cx, []byte("k"))
	if n != 200 {
		t.Fatalf("demote shed %d values, want 200", n)
	}

	// The running total reconciled to the post-shed footprint, and that footprint fell
	// (the values left the slab, the directory is the only new resident cost).
	wantExact(t, g)
	if after := h.residentBytes(); after >= before {
		t.Fatalf("resident bytes %d not below the pre-demote %d", after, before)
	}

	// Every record is cold, its value shed and its field kept, and the directory packed
	// more than one chunk.
	ft := h.ft
	if ft.cold.dir.Len() < 2 {
		t.Fatalf("directory holds %d chunks, want at least 2", ft.cold.dir.Len())
	}
	if int(ft.cold.dir.Total()) != 200 {
		t.Fatalf("directory total %d, want 200 packed", ft.cold.dir.Total())
	}
	for _, ord := range ft.vec {
		if ft.ents[ord].band&tierCold == 0 {
			t.Fatal("a record kept a resident value after the pass")
		}
	}

	// HLEN is unchanged, and every read answers across the boundary.
	if h.card() != 200 {
		t.Fatalf("HLEN %d after demote, want 200", h.card())
	}
	for f, v := range want {
		got, ok := h.get([]byte(f))
		if !ok || string(got) != v {
			t.Fatalf("HGET %q = %q,%v, want %q", f, got, ok, v)
		}
		if !h.has([]byte(f)) {
			t.Fatalf("HEXISTS %q false after demote", f)
		}
		if h.strlen([]byte(f)) != len(v) {
			t.Fatalf("HSTRLEN %q = %d, want %d", f, h.strlen([]byte(f)), len(v))
		}
	}

	// A full enumeration reads every pair back.
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

	// Nothing left to shed: a second pass is a no-op and disturbs neither the records
	// nor the total.
	if again := g.demote(cx, []byte("k")); again != 0 {
		t.Fatalf("second demote shed %d, want 0", again)
	}
	wantExact(t, g)
}

// TestDemoteReclaimsSlab holds the memory claim under a real slab rebuild: after the
// shed the slab holds the field bytes alone, so a wide-value hash frees the value
// bytes to disk and keeps only the fields resident.
func TestDemoteReclaimsSlab(t *testing.T) {
	cx, g := coldCtx(t)
	h := nativeReg(g, "k", 300, 120)

	// Snapshot a field to confirm it still reads resident after the rebuild.
	var probe string
	h.each(func(f, v []byte) {
		if probe == "" {
			probe = string(f)
		}
	})

	slabBefore := cap(h.ft.slab)
	g.demote(cx, []byte("k"))
	slabAfter := cap(h.ft.slab)

	if slabAfter >= slabBefore {
		t.Fatalf("slab cap %d not below the pre-demote %d", slabAfter, slabBefore)
	}
	// The rebuilt slab holds only field bytes, roughly the field half of the pairs, so
	// it sits well under the pre-demote slab that held both.
	if slabAfter > slabBefore*3/4 {
		t.Fatalf("rebuilt slab %d did not shed the value bytes (was %d)", slabAfter, slabBefore)
	}
	// The field stays readable straight from the slab: HEXISTS is zero preads through a
	// demote, the point of keeping fields resident.
	if !h.has([]byte(probe)) {
		t.Fatalf("HEXISTS %q false after the slab rebuild", probe)
	}
}

// TestDemoteInlineNoOp holds the inline band below the demote: a listpack hash is
// smaller than one chunk, so the pass never touches it and the total stays put.
func TestDemoteInlineNoOp(t *testing.T) {
	cx, g := coldCtx(t)
	setKey(g, "k", "f", "v")

	if n := g.demote(cx, []byte("k")); n != 0 {
		t.Fatalf("inline demote shed %d, want 0", n)
	}
	if h := g.m["k"]; h.enc != encListpack {
		t.Fatal("inline hash crossed to native under a demote")
	}
	wantExact(t, g)
}

// TestDemoteMissingKeyNoOp holds a demote of an absent key to a clean zero.
func TestDemoteMissingKeyNoOp(t *testing.T) {
	cx, g := coldCtx(t)
	if n := g.demote(cx, []byte("nope")); n != 0 {
		t.Fatalf("demote of an absent key shed %d, want 0", n)
	}
}
