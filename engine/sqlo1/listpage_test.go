package sqlo1

// Fence paging oracle, T5 slice 8. The fanouts dial down so the paged
// ladder (transition, edge spills, page splits, page death, the third
// level refusal) is reachable in test-sized lists. Fat elements own a
// node each, which makes fence shapes deterministic, and a []string
// model checks every wire-visible answer, hot and cold.

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// fatElem fills a node whole, so element i is node i and fence shapes
// follow from counts alone.
func fatElem(i int) string {
	tag := fmt.Sprintf("e%04d-", i)
	return tag + strings.Repeat("x", listNodeMax-listNodeHdrLen-listElemHdrLen-len(tag))
}

func TestListFencePageCodec(t *testing.T) {
	ents := []listFenceEnt{
		{segid: 3, count: 4},
		{segid: 7, meta: 0x11, count: 1},
		{segid: 5, count: 9},
	}
	enc := appendListFencePage(nil, ents)
	if len(enc) != listPageHdrLen+3*listFenceEntLen {
		t.Fatalf("encoded page is %d bytes", len(enc))
	}
	dec, sum, err := decodeListFencePage(enc, 8, nil)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if !slices.Equal(dec, ents) || sum != 14 {
		t.Fatalf("round trip = %+v sum %d", dec, sum)
	}

	mut := func(f func(b []byte)) []byte {
		b := slices.Clone(enc)
		f(b)
		return b
	}
	corrupt := map[string][]byte{
		"empty":          {},
		"n zero":         mut(func(b []byte) { binary.LittleEndian.PutUint16(b, 0) }),
		"n over the cap": mut(func(b []byte) { binary.LittleEndian.PutUint16(b, uint16(listFencePageMax+1)) }),
		"reserved bytes": mut(func(b []byte) { b[2] = 1 }),
		"size mismatch":  enc[:len(enc)-1],
		"entry count zero": mut(func(b []byte) {
			binary.LittleEndian.PutUint32(b[listPageHdrLen+8:], 0)
		}),
	}
	for name, p := range corrupt {
		if _, _, err := decodeListFencePage(p, 8, nil); err == nil {
			t.Errorf("%s: corrupt page decoded cleanly", name)
		}
	}
	// segid validation runs against the caller's next_segid.
	if _, _, err := decodeListFencePage(enc, 7, nil); err == nil {
		t.Error("segid at next_segid decoded cleanly")
	}
}

// TestListPagedLadder climbs the whole paged surface with fat elements
// at dialed caps: transition, in-place edge growth, spills both
// directions, positional ops, a page split under LINSERT, page death
// under pops, trims, LREM, LMOVE between paged lists, and death with
// rebirth. Every step model-checks the full readback; the cold checks
// reopen over the flushed store.
func TestListPagedLadder(t *testing.T) {
	defer SetListFenceCapsForTest(4, 3, 4)()
	rig := newListRig(t)
	ctx := context.Background()
	var m listModel

	counts := func() []int {
		t.Helper()
		nr := rig.nodedRoot("q")
		if !nr.paged {
			t.Fatal("root is not paged")
		}
		out := make([]int, len(nr.pidx))
		for i, e := range nr.pidx {
			out[i] = int(e.count)
		}
		return out
	}
	check := func(stage string, cold bool) {
		t.Helper()
		if n, err := rig.l.Len(ctx, []byte("q")); err != nil || n != int64(len(m)) {
			t.Fatalf("%s: Len = %d, %v, want %d", stage, n, err, len(m))
		}
		if got := rig.rng(rig.l, "q", 0, -1); !slices.Equal(got, []string(m)) {
			t.Fatalf("%s: readback diverged from the model\n got %d elems\nwant %d elems", stage, len(got), len(m))
		}
		if cold {
			if err := rig.tr.Flush(ctx); err != nil {
				t.Fatalf("%s: Flush: %v", stage, err)
			}
			if got := rig.rng(rig.reopen(), "q", 0, -1); !slices.Equal(got, []string(m)) {
				t.Fatalf("%s: cold readback diverged from the model", stage)
			}
		}
	}

	// Four right pushes fill the flat fence; the fifth transitions, and
	// a right transition keeps the partial page last so the pushed end
	// has room.
	for i := range 5 {
		rig.push("q", false, fatElem(i))
		m.push(false, fatElem(i))
	}
	if got := counts(); !slices.Equal(got, []int{3, 2}) {
		t.Fatalf("pages after the transition = %v, want [3 2]", got)
	}
	check("transition", true)
	if paged, err := rig.l.ListFencePagedForTest(ctx, []byte("q")); err != nil || !paged {
		t.Fatalf("paged probe = %v, %v", paged, err)
	}

	// The edge page grows in place to its cap, then the next right push
	// spills a fresh page.
	rig.push("q", false, fatElem(5))
	m.push(false, fatElem(5))
	if got := counts(); !slices.Equal(got, []int{3, 3}) {
		t.Fatalf("pages after the in-place growth = %v, want [3 3]", got)
	}
	rig.push("q", false, fatElem(6))
	m.push(false, fatElem(6))
	if got := counts(); !slices.Equal(got, []int{3, 3, 1}) {
		t.Fatalf("pages after the right spill = %v, want [3 3 1]", got)
	}
	check("right spill", false)

	// A batched left push over a full head page spills a fresh page at
	// the front.
	rig.push("q", true, fatElem(7), fatElem(8))
	m.push(true, fatElem(7), fatElem(8))
	if got := counts(); !slices.Equal(got, []int{2, 3, 3, 1}) {
		t.Fatalf("pages after the left spill = %v, want [2 3 3 1]", got)
	}
	check("left spill", false)

	// Positional ops land across pages: LINDEX at every page, LSET in a
	// middle page, LPOS both directions.
	for _, i := range []int64{0, 1, 3, 6, 8} {
		if got, ok := rig.index(rig.l, "q", i); !ok || got != m[i] {
			t.Fatalf("Index(%d) = %q ok=%v", i, got[:16], ok)
		}
	}
	if err := rig.l.Set(ctx, []byte("q"), 5, []byte(fatElem(9))); err != nil {
		t.Fatalf("Set(5): %v", err)
	}
	m[5] = fatElem(9)
	check("cross-page set", false)
	if got := rig.pos(rig.l, "q", fatElem(9), 1, 1, 0); !slices.Equal(got, []int64{5}) {
		t.Fatalf("Pos forward = %v", got)
	}
	if got := rig.pos(rig.l, "q", fatElem(8), -1, 1, 0); !slices.Equal(got, []int64{0}) {
		t.Fatalf("Pos reverse = %v", got)
	}

	// Pops drain whole pages away: three from the left kill the head
	// page and shrink its successor, one from the right kills the tail
	// page.
	if got := rig.pop("q", true, 3); !slices.Equal(got, m.pop(true, 3)) {
		t.Fatal("left pop diverged")
	}
	if got := counts(); !slices.Equal(got, []int{2, 3, 1}) {
		t.Fatalf("pages after the left pops = %v, want [2 3 1]", got)
	}
	if got := rig.pop("q", false, 1); !slices.Equal(got, m.pop(false, 1)) {
		t.Fatal("right pop diverged")
	}
	if got := counts(); !slices.Equal(got, []int{2, 3}) {
		t.Fatalf("pages after the right pop = %v, want [2 3]", got)
	}
	check("page death", true)

	// LINSERT into a full page: the fat pivot's node splits, the page
	// cannot take the second entry, and the page splits in half.
	if n := rig.insert("q", true, m[3], fatElem(10)); n != int64(len(m)+1) {
		t.Fatalf("Insert = %d", n)
	}
	m = slices.Insert(m, 3, fatElem(10))
	if got := counts(); !slices.Equal(got, []int{2, 2, 2}) {
		t.Fatalf("pages after the split = %v, want [2 2 2]", got)
	}
	check("page split", true)

	// LREM forward empties a node inside the head page; reverse removes
	// from the tail page.
	if n := rig.rem("q", 1, m[0]); n != 1 {
		t.Fatalf("Rem head = %d", n)
	}
	m = slices.Delete(m, 0, 1)
	if n := rig.rem("q", -1, m[len(m)-1]); n != 1 {
		t.Fatalf("Rem tail = %d", n)
	}
	m = slices.Delete(m, len(m)-1, len(m))
	if got := counts(); !slices.Equal(got, []int{1, 2, 1}) {
		t.Fatalf("pages after the rems = %v, want [1 2 1]", got)
	}
	check("rem", false)

	// LTRIM drops the single-node edge pages whole and keeps the middle
	// page untouched.
	rig.trim("q", 1, 2)
	m = append(listModel{}, m[1:3]...)
	if got := counts(); !slices.Equal(got, []int{2}) {
		t.Fatalf("pages after the trim = %v, want [2]", got)
	}
	check("trim", true)

	// LMOVE between two paged lists exercises the edge pages on both
	// roots plus the flush guard keys.
	var md listModel
	for i := 20; i < 25; i++ {
		rig.push("d", false, fatElem(i))
		md.push(false, fatElem(i))
	}
	if !rig.nodedRoot("d").paged {
		t.Fatal("d did not page")
	}
	if got, ok := rig.move("q", "d", false, true); !ok || got != m[len(m)-1] {
		t.Fatalf("Move right-to-left = %q ok=%v", got[:16], ok)
	}
	md.push(true, m[len(m)-1])
	m.pop(false, 1)
	if got, ok := rig.move("d", "q", true, false); !ok || got != md[0] {
		t.Fatalf("Move left-to-right = %q ok=%v", got[:16], ok)
	}
	m.push(false, md[0])
	md.pop(true, 1)
	check("move", false)
	if got := rig.rng(rig.l, "d", 0, -1); !slices.Equal(got, []string(md)) {
		t.Fatal("move dst diverged from the model")
	}

	// The whole-list pop drains every page and node and deletes the
	// key; a rebirth starts back at the inline tier.
	if got := rig.pop("q", true, len(m)); !slices.Equal(got, []string(m)) {
		t.Fatal("drain pop diverged")
	}
	if exists, _, err := rig.s.Entry(ctx, []byte("q")); err != nil || exists {
		t.Fatalf("drained paged list still exists: %v, %v", exists, err)
	}
	rig.push("q", false, "tiny")
	if got := rig.rng(rig.l, "q", 0, -1); !slices.Equal(got, []string{"tiny"}) {
		t.Fatal("rebirth diverged")
	}
}

// TestListPagedSmallElems runs the positional math across pages with
// many-element nodes, where within-page seeks and the Range page walk
// carry real offsets instead of one element per node.
func TestListPagedSmallElems(t *testing.T) {
	defer SetListFenceCapsForTest(2, 3, 8)()
	rig := newListRig(t)
	ctx := context.Background()
	var m listModel

	// Batched right pushes cut full nodes, blowing past the two-node
	// flat cap into pages.
	for i := 0; i < 2000; i += 250 {
		batch := make([]string, 250)
		for j := range batch {
			batch[j] = fmt.Sprintf("v%04d", i+j)
		}
		rig.push("q", false, batch...)
		m.push(false, batch...)
	}
	nr := rig.nodedRoot("q")
	if !nr.paged || len(nr.pidx) < 2 {
		t.Fatalf("root paged=%v pages=%d, want paged multi-page", nr.paged, len(nr.pidx))
	}

	if got := rig.rng(rig.l, "q", 0, -1); !slices.Equal(got, []string(m)) {
		t.Fatal("full range diverged")
	}
	// Windows that start and end mid-page, cross one boundary, and pin
	// the negative grammar.
	for _, w := range [][2]int64{{0, 99}, {100, 1499}, {1990, 1999}, {-300, -1}, {500, 500}} {
		start, stop := w[0], w[1]
		ms, me := start, stop
		if ms < 0 {
			ms += int64(len(m))
		}
		if me < 0 {
			me += int64(len(m))
		}
		got := rig.rng(rig.l, "q", start, stop)
		if !slices.Equal(got, []string(m)[ms:me+1]) {
			t.Fatalf("Range(%d, %d) diverged", start, stop)
		}
	}
	for _, i := range []int64{0, 127, 128, 1000, 1999, -1, -2000} {
		mi := i
		if mi < 0 {
			mi += int64(len(m))
		}
		if got, ok := rig.index(rig.l, "q", i); !ok || got != m[mi] {
			t.Fatalf("Index(%d) = %q ok=%v, want %q", i, got, ok, m[mi])
		}
	}
	if err := rig.l.Set(ctx, []byte("q"), 1500, []byte("swapped")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	m[1500] = "swapped"
	if got, ok := rig.index(rig.l, "q", 1500); !ok || got != "swapped" {
		t.Fatalf("Index after Set = %q ok=%v", got, ok)
	}

	// LPOS ranks and LREM budgets across pages, on planted duplicates.
	for _, i := range []int64{40, 900, 1750} {
		if err := rig.l.Set(ctx, []byte("q"), i, []byte("dup")); err != nil {
			t.Fatalf("Set dup: %v", err)
		}
		m[i] = "dup"
	}
	if got := rig.pos(rig.l, "q", "dup", 1, 3, 0); !slices.Equal(got, []int64{40, 900, 1750}) {
		t.Fatalf("Pos dup forward = %v", got)
	}
	if got := rig.pos(rig.l, "q", "dup", -1, 2, 0); !slices.Equal(got, []int64{1750, 900}) {
		t.Fatalf("Pos dup reverse = %v", got)
	}
	if n := rig.rem("q", 2, "dup"); n != 2 {
		t.Fatalf("Rem dup = %d", n)
	}
	m = slices.Delete(m, 900, 901)
	m = slices.Delete(m, 40, 41)
	if got := rig.rng(rig.l, "q", 0, -1); !slices.Equal(got, []string(m)) {
		t.Fatal("readback after Rem diverged")
	}

	// A trim to a mid-window drops pages at both ends and shrinks the
	// edges, then the cold reopen agrees.
	rig.trim("q", 300, 1600)
	m = append(listModel{}, m[300:1601]...)
	if n, err := rig.l.Len(ctx, []byte("q")); err != nil || n != int64(len(m)) {
		t.Fatalf("Len after the trim = %d, %v, want %d", n, err, len(m))
	}
	if got := rig.rng(rig.l, "q", 0, -1); !slices.Equal(got, []string(m)) {
		t.Fatal("readback after the trim diverged")
	}
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := rig.rng(rig.reopen(), "q", 0, -1); !slices.Equal(got, []string(m)) {
		t.Fatal("cold readback diverged")
	}
}

// TestListPagedThirdLevel drives the page index to its cap and holds
// every refusal side-effect free: the deque push, the upgrade, and the
// LINSERT page split all answer errListFenceThirdLevel without
// touching the stored list.
func TestListPagedThirdLevel(t *testing.T) {
	defer SetListFenceCapsForTest(4, 3, 4)()
	rig := newListRig(t)
	ctx := context.Background()

	// Fill to the format edge: four pages of three single-element
	// nodes.
	max := 3 * 4
	var m listModel
	for i := range max {
		rig.push("q", false, fatElem(i))
		m.push(false, fatElem(i))
	}
	nr := rig.nodedRoot("q")
	if !nr.paged || len(nr.pidx) != 4 {
		t.Fatalf("root paged=%v pages=%d, want 4 full pages", nr.paged, len(nr.pidx))
	}

	refused := func(stage string, err error) {
		t.Helper()
		if err != errListFenceThirdLevel {
			t.Fatalf("%s = %v, want errListFenceThirdLevel", stage, err)
		}
		if n, lerr := rig.l.Len(ctx, []byte("q")); lerr != nil || n != int64(max) {
			t.Fatalf("%s moved Len to %d, %v", stage, n, lerr)
		}
		if got := rig.rng(rig.l, "q", 0, -1); !slices.Equal(got, []string(m)) {
			t.Fatalf("%s touched the stored list", stage)
		}
	}

	_, err := rig.l.Push(ctx, []byte("q"), false, false, []byte(fatElem(99)))
	refused("right push past the cap", err)
	_, err = rig.l.Push(ctx, []byte("q"), true, false, []byte(fatElem(99)))
	refused("left push past the cap", err)
	_, err = rig.l.Insert(ctx, []byte("q"), true, []byte(m[4]), []byte(fatElem(99)))
	refused("insert past the cap", err)

	// The upgrade overshoot refuses the same way and leaves the key
	// unmade.
	huge := make([][]byte, max+1)
	for i := range huge {
		huge[i] = []byte(fatElem(i))
	}
	if _, err := rig.l.Push(ctx, []byte("fresh"), false, false, huge...); err != errListFenceThirdLevel {
		t.Fatalf("overflowing fresh push = %v, want errListFenceThirdLevel", err)
	}
	if exists, _, err := rig.s.Entry(ctx, []byte("fresh")); err != nil || exists {
		t.Fatalf("refused fresh push created the key: %v, %v", exists, err)
	}

	// Pops still work at the edge, and room freed by them is usable
	// again.
	if got := rig.pop("q", true, 3); !slices.Equal(got, m.pop(true, 3)) {
		t.Fatal("pop at the edge diverged")
	}
	rig.push("q", true, fatElem(50))
	m.push(true, fatElem(50))
	if got := rig.rng(rig.l, "q", 0, -1); !slices.Equal(got, []string(m)) {
		t.Fatal("push after the pop diverged")
	}
}
