package stream

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The stream demote-to-fold seam (spec 2064/obs1 doc 08 section 7): the
// demote pass appends through AppendChunkFold, so each shed block crosses
// the fold tap as one ID-range run, byte-identical to the resident blob,
// under the 16-byte (ms, seq) disc of its first entry. These tests
// register a tap, run the real demote, and hold the frames to that
// contract; the tail margin never folds, which pins the hot-tail policy
// on the fold side by construction.

type streamTapped struct {
	kind    byte
	flags   byte
	count   uint16
	key     []byte
	disc    []byte
	payload []byte
}

// tapStreamChunks registers a fold tap that copies every chunk frame out
// of the drain buffer, which is only valid during the callback.
func tapStreamChunks(t *testing.T, st *store.Store) *[]streamTapped {
	t.Helper()
	var out []streamTapped
	st.SetFoldTap(func(buf []byte) {
		err := store.WalkStagedFrames(buf, func(f store.FoldFrame) error {
			if !f.Chunk {
				return fmt.Errorf("a non-chunk frame crossed the demote tap (kind 0x%02x)", f.Kind)
			}
			out = append(out, streamTapped{
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

func TestDemoteEmitsIDRangeRuns(t *testing.T) {
	cx, g := coldCtx(t)
	s, _ := buildLog(t, g, 700)

	// Snapshot every sealed front block before the demote: the fold frame
	// must carry these exact bytes under these exact coordinates.
	type snap struct {
		first streamID
		count int
		blob  []byte
	}
	var front []snap
	limit := len(s.blocks) - demoteTailMargin
	for i := 0; i < limit; i++ {
		b := s.blocks[i]
		front = append(front, snap{first: b.first, count: b.count, blob: append([]byte(nil), b.blob...)})
	}

	tapped := tapStreamChunks(t, cx.St)
	shed := s.demote(cx.St, []byte("k"))
	if shed == 0 {
		t.Fatal("demote shed no blocks")
	}
	if shed > len(front) {
		t.Fatalf("demote shed %d blocks, only %d were ahead of the tail margin", shed, len(front))
	}
	if len(*tapped) != shed {
		t.Fatalf("tap heard %d frames, want one per shed block (%d)", len(*tapped), shed)
	}

	for i, c := range *tapped {
		want := front[i]
		if c.kind != kindStream|store.ChunkKindBit {
			t.Fatalf("frame %d kind 0x%02x, want the stream run kind", i, c.kind)
		}
		if string(c.key) != "k" {
			t.Fatalf("frame %d key %q, want the stream key", i, c.key)
		}
		wantDisc := discID(want.first)
		if !bytes.Equal(c.disc, wantDisc[:]) {
			t.Fatalf("frame %d disc %x, want the block firstID %x", i, c.disc, wantDisc)
		}
		if int(c.count) != want.count {
			t.Fatalf("frame %d count %d, want the block's %d frames", i, c.count, want.count)
		}
		if !bytes.Equal(c.payload, want.blob) {
			t.Fatalf("frame %d payload diverged from the resident blob (%d vs %d bytes)", i, len(c.payload), len(want.blob))
		}
	}

	// The tail margin stayed resident and silent: no frame carries a disc at
	// or past the first margin block's ID.
	marginFirst := discID(s.blocks[len(s.blocks)-demoteTailMargin].first)
	for i, c := range *tapped {
		if bytes.Compare(c.disc, marginFirst[:]) >= 0 {
			t.Fatalf("frame %d folded a tail-margin block", i)
		}
	}
}

// pelWant is one expected folded pending entry, matched against a decoded
// packed pair.
type pelWant struct {
	id         streamID
	delivered  int64
	deliveries uint16
	consumer   string
}

// decodePelFrame unpacks a tapped kindStreamPel chunk back into entries.
func decodePelFrame(t *testing.T, c streamTapped) []pelWant {
	t.Helper()
	var got []pelWant
	ok := store.WalkPackedPairs(c.payload, c.flags, int(c.count), func(_ int, p store.PackedPair) bool {
		if len(p.Field) != 16 {
			t.Fatalf("pel pair ID is %d bytes, want 16", len(p.Field))
		}
		if len(p.Value) < 10 {
			t.Fatalf("pel pair value is %d bytes, want the delivery facts", len(p.Value))
		}
		got = append(got, pelWant{
			id:         streamID{ms: binary.BigEndian.Uint64(p.Field[0:8]), seq: binary.BigEndian.Uint64(p.Field[8:16])},
			delivered:  int64(binary.BigEndian.Uint64(p.Value[0:8])),
			deliveries: binary.BigEndian.Uint16(p.Value[8:10]),
			consumer:   string(p.Value[10:]),
		})
		return true
	})
	if !ok {
		t.Fatal("pel chunk payload is torn")
	}
	return got
}

// pend inserts one pending entry through the same bookkeeping the delivery
// path runs: PEL insert plus the group and consumer counters.
func pend(grp *streamGroup, id streamID, now int64, c *streamConsumer) {
	if grp.pel == nil {
		grp.pel = newPEL()
	}
	grp.pel.insert(id, now, c.ord)
	grp.pelCount++
	c.pelCount++
}

func TestDemoteFoldsPelProjection(t *testing.T) {
	cx, g := coldCtx(t)
	s, _ := buildLog(t, g, 700)

	// Two groups so the sorted-name emission order and the per-group disc tag
	// are both observable; group g holds two consumers plus one ownerless
	// claimed entry, group h holds one consumer.
	s.addGroup([]byte("g"), newGroup(streamID{}, 0, true))
	s.addGroup([]byte("h"), newGroup(streamID{}, 0, true))
	gg, gh := s.group([]byte("g")), s.group([]byte("h"))
	c1 := gg.ensureConsumer([]byte("c1"), 1)
	c2 := gg.ensureConsumer([]byte("c2"), 1)
	hc := gh.ensureConsumer([]byte("hc"), 1)

	// Blocks pack 128 fixed-schema entries, so the shed front covers IDs
	// 1..512: block 0 gets one pending, block 1 two, block 2 none (its frame
	// must not exist), block 3 an ownerless claimed entry, and 600 sits in
	// the resident tail so it must never fold.
	pend(gg, streamID{ms: 2}, 111, c1)
	pend(gg, streamID{ms: 130}, 222, c2)
	pend(gg, streamID{ms: 200}, 333, c1)
	if gg.pel == nil {
		gg.pel = newPEL()
	}
	gg.pel.insertClaimed(streamID{ms: 400})
	gg.pelCount++
	pend(gg, streamID{ms: 600}, 444, c1)
	pend(gh, streamID{ms: 3}, 555, hc)

	type rng struct{ first, last streamID }
	var front []rng
	for i := 0; i < len(s.blocks)-demoteTailMargin; i++ {
		front = append(front, rng{first: s.blocks[i].first, last: s.blocks[i].last})
	}

	tapped := tapStreamChunks(t, cx.St)
	shed := s.demote(cx.St, []byte("k"))
	if shed != len(front) {
		t.Fatalf("demote shed %d blocks, want the whole %d-block front", shed, len(front))
	}

	var pels []streamTapped
	for _, c := range *tapped {
		if c.kind == kindStreamPel|store.ChunkKindBit {
			pels = append(pels, c)
		}
	}
	want := []struct {
		group   string
		block   int
		entries []pelWant
	}{
		{group: "g", block: 0, entries: []pelWant{{id: streamID{ms: 2}, delivered: 111, deliveries: 1, consumer: "c1"}}},
		{group: "g", block: 1, entries: []pelWant{
			{id: streamID{ms: 130}, delivered: 222, deliveries: 1, consumer: "c2"},
			{id: streamID{ms: 200}, delivered: 333, deliveries: 1, consumer: "c1"},
		}},
		{group: "g", block: 3, entries: []pelWant{{id: streamID{ms: 400}}}},
		{group: "h", block: 0, entries: []pelWant{{id: streamID{ms: 3}, delivered: 555, deliveries: 1, consumer: "hc"}}},
	}
	if len(pels) != len(want) {
		t.Fatalf("tap heard %d pel frames, want %d (empty ranges emit nothing)", len(pels), len(want))
	}
	for i, w := range want {
		c := pels[i]
		if string(c.key) != "k" {
			t.Fatalf("pel frame %d key %q, want the stream key", i, c.key)
		}
		if len(c.disc) != 24 {
			t.Fatalf("pel frame %d disc is %d bytes, want 24", i, len(c.disc))
		}
		if got := binary.BigEndian.Uint64(c.disc[0:8]); got != obs1.Disc([]byte(w.group)) {
			t.Fatalf("pel frame %d tag %x, want group %q's", i, got, w.group)
		}
		first := front[w.block].first
		if binary.BigEndian.Uint64(c.disc[8:16]) != first.ms || binary.BigEndian.Uint64(c.disc[16:24]) != first.seq {
			t.Fatalf("pel frame %d disc ID %x, want block %d's first %d-%d", i, c.disc[8:], w.block, first.ms, first.seq)
		}
		got := decodePelFrame(t, c)
		if len(got) != len(w.entries) {
			t.Fatalf("pel frame %d holds %d entries, want %d", i, len(got), len(w.entries))
		}
		for j, e := range w.entries {
			if got[j] != e {
				t.Fatalf("pel frame %d entry %d = %+v, want %+v", i, j, got[j], e)
			}
		}
	}
}

func TestTrimDropEmitsZeroCountChunks(t *testing.T) {
	cx, g := coldCtx(t)
	s, _ := buildLog(t, g, 700)
	if shed := s.demote(cx.St, []byte("k")); shed == 0 {
		t.Fatal("demote shed no blocks")
	}
	g.note(s)

	// The blocks an approximate MAXLEN 200 will drop whole: front blocks while
	// the remaining length stays at or above the threshold.
	var wantDiscs [][16]byte
	remaining := s.length
	for _, b := range s.blocks[:len(s.blocks)-1] {
		if remaining-uint64(b.live()) < 200 {
			break
		}
		remaining -= uint64(b.live())
		if !b.cold() {
			t.Fatal("a to-be-dropped front block is still resident after the demote")
		}
		wantDiscs = append(wantDiscs, discID(b.first))
	}
	if len(wantDiscs) == 0 {
		t.Fatal("the trim would drop no whole blocks, the fixture is too small")
	}

	tapped := tapStreamChunks(t, cx.St)
	removed := s.trim([]byte("k"), trimSpec{kind: trimMaxlen, approx: true, maxlen: 200})
	if removed != 700-int(remaining) {
		t.Fatalf("trim removed %d entries, want %d", removed, 700-int(remaining))
	}

	if len(*tapped) != len(wantDiscs) {
		t.Fatalf("tap heard %d frames, want one manifest drop per dropped block (%d)", len(*tapped), len(wantDiscs))
	}
	for i, c := range *tapped {
		if c.kind != kindStream|store.ChunkKindBit {
			t.Fatalf("drop frame %d kind 0x%02x, want the stream run kind", i, c.kind)
		}
		if string(c.key) != "k" {
			t.Fatalf("drop frame %d key %q, want the stream key", i, c.key)
		}
		if c.count != 0 {
			t.Fatalf("drop frame %d count %d, want the zero-count replacement", i, c.count)
		}
		if len(c.payload) != 0 {
			t.Fatalf("drop frame %d carries %d payload bytes, want none", i, len(c.payload))
		}
		if !bytes.Equal(c.disc, wantDiscs[i][:]) {
			t.Fatalf("drop frame %d disc %x, want the dropped block's first ID %x", i, c.disc, wantDiscs[i])
		}
	}
}
