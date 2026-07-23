package list

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The list demote-to-fold seam (spec 2064/obs1 doc 08 section 6): the demote
// pass keeps the local cold form exactly as it was (bare uvarint frames under
// the demote-sequence disc) and emits a separate position-run projection
// through EmitFoldChunk, valueless packed pairs under an 8-byte virtual
// position disc, so the fold plane can order and count the runs while the
// local tier keeps its single copy. These tests register a tap, run the real
// demote, and hold the frames to that contract: one fold frame per shed
// chunk, discs that are the biased virtual position of each run's first
// element and advance by exactly the run counts, elements identical to the
// interior in order, and coordinates that keep advancing across queue churn
// because headVirt tracks the head's virtual motion.

type listTapped struct {
	kind    byte
	flags   byte
	count   uint16
	key     []byte
	disc    []byte
	payload []byte
}

// tapListChunks registers a fold tap that copies every chunk frame out of the
// drain buffer, which is only valid during the callback.
func tapListChunks(t *testing.T, st *store.Store) *[]listTapped {
	t.Helper()
	var out []listTapped
	st.SetFoldTap(func(buf []byte) {
		err := store.WalkStagedFrames(buf, func(f store.FoldFrame) error {
			if !f.Chunk {
				return fmt.Errorf("a non-chunk frame crossed the demote tap (kind 0x%02x)", f.Kind)
			}
			out = append(out, listTapped{
				kind:    f.Kind,
				flags:   f.Flags,
				count:   f.Count,
				key:     append([]byte(nil), f.Key...),
				disc:    append([]byte(nil), f.Disc...),
				payload: append([]byte(nil), f.Payload...),
			})
			return nil
		})
		if err != nil {
			t.Errorf("tap walk: %v", err)
		}
	})
	return &out
}

// foldElems decodes a tapped frame's valueless pairs into element values.
func foldElems(t *testing.T, c listTapped) [][]byte {
	t.Helper()
	var out [][]byte
	ok := store.WalkPackedPairs(c.payload, c.flags, int(c.count), func(_ int, p store.PackedPair) bool {
		if len(p.Value) != 0 {
			t.Fatalf("position-run pair carries %d value bytes, want valueless", len(p.Value))
		}
		out = append(out, append([]byte(nil), p.Field...))
		return true
	})
	if !ok {
		t.Fatal("torn packed-pair walk on a tapped frame")
	}
	return out
}

// denseStart is the dense index of ring chunk ci's first element, the prefix
// the fold coordinate adds to headVirt.
func denseStart(nt *native, ci int) int {
	s := 0
	for i := 0; i < ci; i++ {
		s += nt.ring.at(i).count()
	}
	return s
}

func TestDemoteEmitsPositionRuns(t *testing.T) {
	cx, g := coldCtx(t)
	nt, want := coldTestNative(400, 40)
	if nt.ring.n < 4 {
		t.Fatalf("need several chunks for an interior demote, got %d", nt.ring.n)
	}
	l := &list{nat: nt, everLarge: true}
	g.m["k"] = l
	g.note(l)

	tapped := tapListChunks(t, cx.St)
	interior := nt.ring.n - 2*demoteMargin
	n := g.demote(cx, []byte("k"))
	if n != interior {
		t.Fatalf("demoted %d chunks, want the %d interior", n, interior)
	}

	// One fold frame per shed chunk, and the local tier is untouched by the
	// projection: still one demote descriptor per chunk.
	if len(*tapped) != n {
		t.Fatalf("tap heard %d frames, want one per shed chunk (%d)", len(*tapped), n)
	}
	if nt.cold.dir.Len() != n {
		t.Fatalf("local directory holds %d descriptors, want %d", nt.cold.dir.Len(), n)
	}

	// Each frame: the list kind, the table key, an 8-byte disc that is the
	// biased virtual position of the run's first element (headVirt is zero for
	// a pushBack-only build, so that is listPosBias plus the dense prefix),
	// and elements equal to the interior span in order. Consecutive discs
	// advance by exactly the run counts.
	next := listPosBias + uint64(nt.ring.at(0).count())
	var all [][]byte
	for i, c := range *tapped {
		if c.kind != kindList|store.ChunkKindBit {
			t.Fatalf("frame %d kind 0x%02x, want the list position-run kind", i, c.kind)
		}
		if string(c.key) != "k" {
			t.Fatalf("frame %d key %q, want the table key", i, c.key)
		}
		if len(c.disc) != 8 {
			t.Fatalf("frame %d disc is %d bytes, want the 8 position bytes", i, len(c.disc))
		}
		if got := binary.BigEndian.Uint64(c.disc); got != next {
			t.Fatalf("frame %d disc %d, want %d (positions advance by run counts)", i, got, next)
		}
		next += uint64(c.count)
		all = append(all, foldElems(t, c)...)
	}
	margin := nt.ring.at(0).count()
	if len(all) != nt.count-margin-nt.ring.tail().count() {
		t.Fatalf("fold frames carry %d elements, want the whole interior", len(all))
	}
	for i, v := range all {
		if !bytes.Equal(v, want[margin+i]) {
			t.Fatalf("fold element %d = %q, want %q", i, v, want[margin+i])
		}
	}
}

// TestQueueChurnFoldCoordinatesAdvance drives the steady queue shape across
// two demote generations: drain the head, extend the tail, demote again, and
// hold the second generation's coordinates to the headVirt arithmetic. The
// drained positions never come back, so the new runs land strictly after the
// first generation's span and the fold plane sees one monotone position axis.
func TestQueueChurnFoldCoordinatesAdvance(t *testing.T) {
	cx, g := coldCtx(t)
	nt, want := coldTestNative(400, 40)
	if nt.ring.n < 4 {
		t.Fatalf("need several chunks, got %d", nt.ring.n)
	}
	l := &list{nat: nt, everLarge: true}
	g.m["k"] = l
	g.note(l)

	tapped := tapListChunks(t, cx.St)
	if g.demote(cx, []byte("k")) == 0 {
		t.Fatal("first demote shed nothing")
	}
	gen1 := len(*tapped)
	last := (*tapped)[gen1-1]
	gen1End := binary.BigEndian.Uint64(last.disc) + uint64(last.count)

	// Drain 50 off the head (FIFO holds), then extend the tail far enough
	// that the first generation's cold interior gains resident successors.
	logical := append([][]byte(nil), want...)
	for i := 0; i < 50; i++ {
		if got := nt.popFront(); !bytes.Equal(got, logical[0]) {
			t.Fatalf("pop %d = %q, want %q", i, got, logical[0])
		}
		logical = logical[1:]
	}
	if nt.headVirt != 50 {
		t.Fatalf("headVirt %d after 50 pops, want 50", nt.headVirt)
	}
	for i := 0; i < 200; i++ {
		v := []byte(fmt.Sprintf("q%038d", i))
		nt.pushBack(v)
		logical = append(logical, v)
	}

	coldBefore := map[int]bool{}
	for i := 0; i < nt.ring.n; i++ {
		coldBefore[i] = nt.ring.at(i).cold()
	}
	if g.demote(cx, []byte("k")) == 0 {
		t.Fatal("second demote shed nothing")
	}

	// The newly shed chunks in ring order are the second generation's frames
	// in tap order; each disc is listPosBias + headVirt + the chunk's dense
	// start, and all land at or past the first generation's end.
	var newCold []int
	for i := 0; i < nt.ring.n; i++ {
		if nt.ring.at(i).cold() && !coldBefore[i] {
			newCold = append(newCold, i)
		}
	}
	gen2 := (*tapped)[gen1:]
	if len(gen2) != len(newCold) {
		t.Fatalf("tap heard %d second-generation frames, %d chunks flipped cold", len(gen2), len(newCold))
	}
	for j, c := range gen2 {
		ci := newCold[j]
		start := denseStart(nt, ci)
		wantDisc := uint64(int64(listPosBias) + nt.headVirt + int64(start))
		got := binary.BigEndian.Uint64(c.disc)
		if got != wantDisc {
			t.Fatalf("gen2 frame %d disc %d, want headVirt+densePrefix (%d)", j, got, wantDisc)
		}
		if got < gen1End {
			t.Fatalf("gen2 frame %d disc %d fell inside generation one (end %d)", j, got, gen1End)
		}
		for k, v := range foldElems(t, c) {
			if !bytes.Equal(v, logical[start+k]) {
				t.Fatalf("gen2 frame %d element %d = %q, want %q", j, k, v, logical[start+k])
			}
		}
	}
}

// TestPushFrontFoldCoordinates pins the negative headVirt branch: a
// pushFront-only build drives headVirt to minus the length, so every fold
// coordinate sits below the bias and the formula still orders the runs.
func TestPushFrontFoldCoordinates(t *testing.T) {
	cx, g := coldCtx(t)
	nt := &native{}
	n := 400
	logical := make([][]byte, n)
	for i := n - 1; i >= 0; i-- {
		v := []byte(fmt.Sprintf("f%038d", i))
		nt.pushFront(v)
		logical[i] = v
	}
	if nt.headVirt != int64(-n) {
		t.Fatalf("headVirt %d after %d front pushes, want %d", nt.headVirt, n, -n)
	}
	if nt.ring.n < 4 {
		t.Fatalf("need several chunks, got %d", nt.ring.n)
	}
	l := &list{nat: nt, everLarge: true}
	g.m["k"] = l
	g.note(l)

	tapped := tapListChunks(t, cx.St)
	if g.demote(cx, []byte("k")) == 0 {
		t.Fatal("demote shed nothing")
	}
	coldSeen := 0
	for i := 0; i < nt.ring.n; i++ {
		c := nt.ring.at(i)
		if !c.cold() {
			continue
		}
		f := (*tapped)[coldSeen]
		coldSeen++
		start := denseStart(nt, i)
		wantDisc := uint64(int64(listPosBias) + nt.headVirt + int64(start))
		if wantDisc >= listPosBias {
			t.Fatal("fixture failed to exercise the below-bias coordinate range")
		}
		if got := binary.BigEndian.Uint64(f.disc); got != wantDisc {
			t.Fatalf("frame %d disc %d, want %d", coldSeen-1, got, wantDisc)
		}
		for k, v := range foldElems(t, f) {
			if !bytes.Equal(v, logical[start+k]) {
				t.Fatalf("frame %d element %d = %q, want %q", coldSeen-1, k, v, logical[start+k])
			}
		}
	}
	if coldSeen != len(*tapped) {
		t.Fatalf("matched %d frames to cold chunks, tap heard %d", coldSeen, len(*tapped))
	}
}
