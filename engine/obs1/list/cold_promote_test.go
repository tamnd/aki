package list

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// The list cold-tier scan and write plumbing (plan M7-slice-cold-chunk-list, PR E).
// A demoted interior chunk carries only a cold offset; the read scans (each, LPOS,
// the LINSERT pivot walk) must cross it transparently and leave it cold, while the
// mutators that land in it (LSET, LINSERT, LREM, and a pop or trim that drains the
// margin into it) must promote the whole chunk resident first. These tests run the
// real demote pass to shed a true interior, then exercise each path against the
// pre-demote values.

// coldInterior builds a registry-backed list, runs the real demote pass to shed its
// whole interior, and returns the context, the registry, the native band, the
// pre-demote values, and the list. Head and tail stay resident by the demote margin;
// every chunk between them is cold, so an interior index lands on a demoted chunk.
func coldInterior(t *testing.T) (*shard.Ctx, *reg, *native, [][]byte, *list) {
	t.Helper()
	cx, g := coldCtx(t)
	nt, want := coldTestNative(400, 40)
	if nt.ring.n < 4 {
		t.Fatalf("need several chunks for an interior demote, got %d", nt.ring.n)
	}
	l := &list{nat: nt, everLarge: true}
	g.m["k"] = l
	g.note(l)
	interior := nt.ring.n - 2*demoteMargin
	if n := g.demote(cx, []byte("k")); n != interior {
		t.Fatalf("demoted %d chunks, want the %d interior", n, interior)
	}
	for i := demoteMargin; i < nt.ring.n-demoteMargin; i++ {
		if !nt.ring.at(i).cold() {
			t.Fatalf("interior chunk %d stayed resident after demote", i)
		}
	}
	return cx, g, nt, want, l
}

// TestColdScanReadsStayResidentFree drives the pure reads over a demoted interior:
// each walks the whole list in order, LPOS forward and backward name a hit that
// lives inside the cold chunk, and the LINSERT pivot walk locates it. None of them
// promotes: a read pays a pread, not a re-materialization, so the interior stays
// cold.
func TestColdScanReadsStayResidentFree(t *testing.T) {
	_, _, nt, want, _ := coldInterior(t)
	ci := nt.ring.n / 2
	if !nt.ring.at(ci).cold() {
		t.Fatal("expected an interior cold chunk to test against")
	}
	beforeCold := nt.coldN

	// each visits every element in order across the cold gap.
	var seen [][]byte
	nt.each(func(v []byte) { seen = append(seen, append([]byte(nil), v...)) })
	if len(seen) != len(want) {
		t.Fatalf("each visited %d elements, want %d", len(seen), len(want))
	}
	for i := range want {
		if !bytes.Equal(seen[i], want[i]) {
			t.Fatalf("each[%d] = %q, want %q", i, seen[i], want[i])
		}
	}

	// LPOS forward and backward on a value inside the cold chunk name the same
	// absolute position (the values are distinct, so there is exactly one hit).
	cstart, _ := coldSpan(nt, ci)
	idx := cstart + 1
	target := want[idx]
	if pos := nt.lpos(target, 1, 0, 0); len(pos) != 1 || pos[0] != idx {
		t.Fatalf("LPOS forward = %v, want [%d]", pos, idx)
	}
	if pos := nt.lpos(target, -1, 0, 0); len(pos) != 1 || pos[0] != idx {
		t.Fatalf("LPOS backward = %v, want [%d]", pos, idx)
	}

	// The LINSERT pivot walk locates the same value in the cold chunk.
	fci, ford, found := nt.findPivot(target)
	if !found || fci != ci || ford != 1 {
		t.Fatalf("findPivot = (%d, %d, %v), want (%d, 1, true)", fci, ford, found, ci)
	}

	if nt.coldN != beforeCold {
		t.Fatalf("a read promoted a chunk: coldN %d, want %d", nt.coldN, beforeCold)
	}
	if !nt.ring.at(ci).cold() {
		t.Fatal("a read promoted the cold chunk")
	}
}

// TestColdChunkPromotesOnSet holds LSET against a cold chunk: the write brings the
// whole chunk resident, drops its demote descriptor, records the new value, and
// leaves both neighbors readable. The registry running total reconciles the freed
// then re-materialized bytes on the next note.
func TestColdChunkPromotesOnSet(t *testing.T) {
	_, g, nt, want, l := coldInterior(t)
	ci := nt.ring.n / 2
	cstart, _ := coldSpan(nt, ci)
	idx := cstart + 1
	beforeLen := nt.cold.dir.Len()
	beforeCold := nt.coldN

	nv := []byte("a-brand-new-longer-value-here")
	nt.setAt(idx, nv)

	if nt.ring.at(ci).cold() {
		t.Fatal("LSET did not promote the cold chunk")
	}
	if nt.cold.dir.Len() != beforeLen-1 {
		t.Fatalf("demote directory holds %d descriptors, want %d", nt.cold.dir.Len(), beforeLen-1)
	}
	if nt.coldN != beforeCold-1 {
		t.Fatalf("coldN %d, want %d after promote", nt.coldN, beforeCold-1)
	}
	if got := nt.at(idx); !bytes.Equal(got, nv) {
		t.Fatalf("at(%d) = %q, want the set value %q", idx, got, nv)
	}
	for _, j := range []int{idx - 1, idx + 1} {
		if got := nt.at(j); !bytes.Equal(got, want[j]) {
			t.Fatalf("neighbor at(%d) = %q, want %q", j, got, want[j])
		}
	}
	// The ops-path note reconciles the running total to the post-promote footprint.
	g.note(l)
	if g.resident != nt.residentBytes() {
		t.Fatalf("running total %d != post-promote footprint %d", g.resident, nt.residentBytes())
	}
}

// TestColdChunkPromotesOnInsert holds LINSERT against a pivot that lives in a cold
// chunk: the pivot walk finds it cold, the splice promotes the chunk, and the new
// element lands just before the pivot with the descriptor dropped.
func TestColdChunkPromotesOnInsert(t *testing.T) {
	_, _, nt, want, _ := coldInterior(t)
	ci := nt.ring.n / 2
	cstart, _ := coldSpan(nt, ci)
	idx := cstart + 1
	pivot := want[idx]
	beforeLen := nt.cold.dir.Len()

	if !nt.insert(true, pivot, []byte("inserted-before")) {
		t.Fatal("LINSERT did not find the pivot in the cold chunk")
	}
	if nt.ring.at(ci).cold() {
		t.Fatal("LINSERT did not promote the cold chunk")
	}
	if nt.cold.dir.Len() != beforeLen-1 {
		t.Fatalf("demote directory holds %d descriptors, want %d", nt.cold.dir.Len(), beforeLen-1)
	}
	if got := nt.at(idx); !bytes.Equal(got, []byte("inserted-before")) {
		t.Fatalf("at(%d) = %q, want the inserted value", idx, got)
	}
	if got := nt.at(idx + 1); !bytes.Equal(got, pivot) {
		t.Fatalf("at(%d) = %q, want the pivot %q shifted right", idx+1, got, pivot)
	}
}

// TestColdChunkPromotesOnRemoveMatch holds LREM against a match in a cold chunk: the
// forward scan streams over the earlier cold chunks without touching them and
// promotes only the chunk the removal lands in, dropping its descriptor.
func TestColdChunkPromotesOnRemoveMatch(t *testing.T) {
	_, _, nt, want, _ := coldInterior(t)
	ci := nt.ring.n / 2
	cstart, _ := coldSpan(nt, ci)
	idx := cstart + 1
	victim := want[idx]
	beforeLen := nt.cold.dir.Len()

	if n := nt.remove(0, victim); n != 1 {
		t.Fatalf("LREM removed %d, want 1", n)
	}
	if nt.ring.at(ci).cold() {
		t.Fatal("LREM did not promote the chunk it removed from")
	}
	if nt.cold.dir.Len() != beforeLen-1 {
		t.Fatalf("demote directory holds %d descriptors, want %d (only the hit chunk promotes)", nt.cold.dir.Len(), beforeLen-1)
	}
	// The value is gone and the element that followed it now reads at its index.
	if got := nt.at(idx); !bytes.Equal(got, want[idx+1]) {
		t.Fatalf("at(%d) = %q, want the follower %q after the removal", idx, got, want[idx+1])
	}
}

// TestColdRemoveNoMatchStaysCold holds the streaming contract: an LREM whose value
// is nowhere in the list scans across every cold chunk without promoting one, so a
// scan-through leaves the interior on disk.
func TestColdRemoveNoMatchStaysCold(t *testing.T) {
	_, _, nt, _, _ := coldInterior(t)
	beforeLen := nt.cold.dir.Len()
	beforeCold := nt.coldN

	if n := nt.remove(0, []byte("value-that-is-nowhere")); n != 0 {
		t.Fatalf("LREM removed %d, want 0", n)
	}
	if nt.cold.dir.Len() != beforeLen {
		t.Fatalf("a no-match LREM changed the demote directory: %d, want %d", nt.cold.dir.Len(), beforeLen)
	}
	if nt.coldN != beforeCold {
		t.Fatalf("a no-match LREM promoted a chunk: coldN %d, want %d", nt.coldN, beforeCold)
	}
}

// TestPopFrontPromotesExposedColdChunk drains the resident head chunk so the next
// front is a formerly-interior cold chunk, then pops it: the drain exposes the cold
// chunk and the pop that reaches it promotes it and returns the right element.
func TestPopFrontPromotesExposedColdChunk(t *testing.T) {
	_, _, nt, want, _ := coldInterior(t)
	head0 := nt.ring.front().count()
	beforeLen := nt.cold.dir.Len()

	popped := 0
	for k := 0; k < head0; k++ {
		if v := nt.popFront(); !bytes.Equal(v, want[popped]) {
			t.Fatalf("popFront #%d = %q, want %q", popped, v, want[popped])
		}
		popped++
	}
	if !nt.ring.front().cold() {
		t.Fatal("draining the head chunk did not expose a cold front chunk")
	}
	if v := nt.popFront(); !bytes.Equal(v, want[popped]) {
		t.Fatalf("popFront off the exposed cold chunk = %q, want %q", v, want[popped])
	}
	if nt.ring.front().cold() {
		t.Fatal("popFront did not promote the exposed cold chunk")
	}
	if nt.cold.dir.Len() != beforeLen-1 {
		t.Fatalf("demote directory holds %d descriptors, want %d after the promote", nt.cold.dir.Len(), beforeLen-1)
	}
}

// TestPopBackPromotesExposedColdChunk is the tail mirror: draining the resident tail
// chunk exposes a cold chunk, and the next RPOP promotes it and returns the last
// element.
func TestPopBackPromotesExposedColdChunk(t *testing.T) {
	_, _, nt, want, _ := coldInterior(t)
	tail0 := nt.ring.tail().count()
	beforeLen := nt.cold.dir.Len()

	popped := 0
	for k := 0; k < tail0; k++ {
		want1 := want[len(want)-1-popped]
		if v := nt.popBack(); !bytes.Equal(v, want1) {
			t.Fatalf("popBack #%d = %q, want %q", popped, v, want1)
		}
		popped++
	}
	if !nt.ring.tail().cold() {
		t.Fatal("draining the tail chunk did not expose a cold tail chunk")
	}
	want1 := want[len(want)-1-popped]
	if v := nt.popBack(); !bytes.Equal(v, want1) {
		t.Fatalf("popBack off the exposed cold chunk = %q, want %q", v, want1)
	}
	if nt.ring.tail().cold() {
		t.Fatal("popBack did not promote the exposed cold chunk")
	}
	if nt.cold.dir.Len() != beforeLen-1 {
		t.Fatalf("demote directory holds %d descriptors, want %d after the promote", nt.cold.dir.Len(), beforeLen-1)
	}
}

// TestTrimCrossesColdInterior keeps a window that starts inside the first cold chunk
// and ends inside the last one: LTRIM drops the resident head and tail, promotes the
// two boundary chunks it trims in place, and every kept element reads back in order.
func TestTrimCrossesColdInterior(t *testing.T) {
	_, _, nt, want, _ := coldInterior(t)
	firstCold := demoteMargin
	lastCold := nt.ring.n - 1 - demoteMargin
	if firstCold >= lastCold {
		t.Fatalf("need at least two cold chunks, got firstCold=%d lastCold=%d", firstCold, lastCold)
	}
	fcs, _ := coldSpan(nt, firstCold)
	lcs, lcc := coldSpan(nt, lastCold)
	lo := fcs + 1
	hi := lcs + lcc - 2

	nt.trim(lo, hi)

	if nt.count != hi-lo+1 {
		t.Fatalf("count %d after trim, want %d", nt.count, hi-lo+1)
	}
	got := nt.rangeInto(nil, 0, nt.count-1)
	var full []byte
	for i := lo; i <= hi; i++ {
		full = resp.AppendBulk(full, want[i])
	}
	if !bytes.Equal(got, full) {
		t.Fatal("LTRIM crossing a cold interior != RESP of the kept range")
	}
}
