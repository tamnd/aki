package hash

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The demote-to-fold seam (spec 2064/obs1 doc 08 section 3): a hash demote now
// appends through AppendChunkFold, so the same chunk frames the local cold
// directory keys also cross the store's fold tap and reach the segment folder.
// These tests register a tap, run the real demote, and hold the frames to the
// contract the fold plane plans against: hash-kind collection chunks under the
// table's key, discriminators in nondecreasing big-endian Disc order matching
// each chunk's first packed field, the TTL bitmap flag exactly on chunks with
// an expiry bearer, and payloads that decode back to the table's live pairs
// with their expiries intact.

type tappedChunk struct {
	kind    byte
	flags   byte
	count   uint16
	key     []byte
	disc    []byte
	payload []byte
}

// tapChunks registers a fold tap that copies every chunk frame out of the
// drain buffer, which is only valid during the callback.
func tapChunks(t *testing.T, st *store.Store) *[]tappedChunk {
	t.Helper()
	var out []tappedChunk
	st.SetFoldTap(func(buf []byte) {
		err := store.WalkStagedFrames(buf, func(f store.FoldFrame) error {
			if !f.Chunk {
				return fmt.Errorf("a non-chunk frame crossed the demote tap (kind 0x%02x)", f.Kind)
			}
			out = append(out, tappedChunk{
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

func TestDemoteFramesCrossFoldTap(t *testing.T) {
	cx, _ := coldCtx(t)
	h := coldNative(200, 40)
	want := map[string]string{}
	h.each(func(f, v []byte) { want[string(f)] = string(v) })

	tapped := tapChunks(t, cx.St)
	chunks := handDemote(t, cx.St, "k", h.ft)
	if chunks < 2 {
		t.Fatalf("demote wrote %d chunks, want >= 2", chunks)
	}
	if len(*tapped) != chunks {
		t.Fatalf("tap heard %d chunk frames, demote wrote %d", len(*tapped), chunks)
	}

	got := map[string]string{}
	var prev uint64
	for i, c := range *tapped {
		if c.kind != kindHash|store.ChunkKindBit {
			t.Fatalf("chunk %d kind 0x%02x, want the hash collection kind", i, c.kind)
		}
		if c.flags&store.ChunkFlagRun != 0 {
			t.Fatalf("chunk %d carries ChunkFlagRun; demoters must leave it clear", i)
		}
		if string(c.key) != "k" {
			t.Fatalf("chunk %d key %q, want the table key", i, c.key)
		}
		if len(c.disc) != 8 {
			t.Fatalf("chunk %d disc is %d bytes, want 8", i, len(c.disc))
		}
		disc := binary.BigEndian.Uint64(c.disc)
		if disc < prev {
			t.Fatalf("chunk %d disc %d below its predecessor %d; the fold plane needs nondecreasing discs", i, disc, prev)
		}
		prev = disc
		first := true
		ok := store.WalkPackedPairs(c.payload, c.flags, int(c.count), func(_ int, p store.PackedPair) bool {
			if first {
				// The frame disc is the first packed field's fold coordinate.
				if fd := fieldDisc(p.Field); fd != disc {
					t.Fatalf("chunk %d disc %d, want its first field's Disc %d", i, disc, fd)
				}
				first = false
			}
			if p.Exp != 0 {
				t.Fatalf("field %q carries expiry %d in a TTL-free table", p.Field, p.Exp)
			}
			got[string(p.Field)] = string(p.Value)
			return true
		})
		if !ok {
			t.Fatalf("chunk %d payload is torn", i)
		}
		if c.flags&store.ChunkFlagTTLBitmap != 0 {
			t.Fatalf("chunk %d carries the TTL bitmap flag with no bearer", i)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("tapped frames decode %d pairs, table holds %d", len(got), len(want))
	}
	for f, v := range want {
		if got[f] != v {
			t.Fatalf("tapped %q = %q, want %q", f, got[f], v)
		}
	}
}

// TestDemoteTTLBitmapOnBearerChunks gives a scattered subset of fields an
// absolute expiry before the demote and holds the flag rule: exactly the
// chunks holding a bearer leave under ChunkFlagTTLBitmap, and every bearer's
// expiry rides the frame byte-exact while its neighbours stay at zero.
func TestDemoteTTLBitmapOnBearerChunks(t *testing.T) {
	cx, _ := coldCtx(t)
	h := coldNative(200, 40)
	wantExp := map[string]uint64{}
	i := 0
	h.each(func(f, v []byte) {
		if i%7 == 0 {
			at := uint64(1_700_000_000_000 + i)
			if !h.setFieldExp(append([]byte(nil), f...), at) {
				t.Fatalf("setFieldExp %q refused", f)
			}
			wantExp[string(f)] = at
		}
		i++
	})
	if len(wantExp) == 0 {
		t.Fatal("no field drew an expiry")
	}

	tapped := tapChunks(t, cx.St)
	if handDemote(t, cx.St, "k", h.ft) < 2 {
		t.Fatal("demote wrote fewer than 2 chunks")
	}

	bearers := 0
	for ci, c := range *tapped {
		hasBearer := false
		ok := store.WalkPackedPairs(c.payload, c.flags, int(c.count), func(_ int, p store.PackedPair) bool {
			if want := wantExp[string(p.Field)]; p.Exp != want {
				t.Fatalf("field %q expiry %d, want %d", p.Field, p.Exp, want)
			}
			if p.Exp != 0 {
				hasBearer = true
				bearers++
			}
			return true
		})
		if !ok {
			t.Fatalf("chunk %d payload is torn", ci)
		}
		if hasBearer != (c.flags&store.ChunkFlagTTLBitmap != 0) {
			t.Fatalf("chunk %d bearer=%v but TTL bitmap flag=%v", ci, hasBearer, c.flags&store.ChunkFlagTTLBitmap != 0)
		}
	}
	if bearers != len(wantExp) {
		t.Fatalf("frames carry %d bearers, want %d", bearers, len(wantExp))
	}

	// The local cold read still applies the inline expiry lazily: a bearer
	// field reads back until its deadline and as absent after.
	var bearer string
	for f := range wantExp {
		bearer = f
		break
	}
	cx.NowMs = int64(wantExp[bearer]) - 1
	if _, ok := h.get([]byte(bearer)); !ok {
		t.Fatalf("bearer %q missing before its deadline", bearer)
	}
}
