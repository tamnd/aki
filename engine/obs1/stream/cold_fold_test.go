package stream

import (
	"bytes"
	"fmt"
	"testing"

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
