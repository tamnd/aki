package set

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The set demote-to-fold seam (spec 2064/obs1 doc 08 section 4): a set demote
// appends through AppendChunkFold, so the chunks the local cold directory keys
// also cross the store's fold tap and reach the segment folder. These tests
// register a tap, run the real demote, and hold the frames to the fold plane's
// contract: set-kind collection chunks under the table's key, valueless packed
// pairs (doc 08's valueless hash reuse, so no chunk ever carries the TTL
// bitmap), and discriminators in nondecreasing big-endian Disc order matching
// each chunk's first packed member.

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

// checkSetFrames holds one demote quantum's frames to the fold contract and
// folds their members into got. Frames are nondecreasing within a quantum;
// separate quanta (a partitioned set's sweeps) may interleave their ranges,
// which is why the caller resets prev between calls.
func checkSetFrames(t *testing.T, frames []tappedChunk, got map[string]bool) {
	t.Helper()
	var prev uint64
	for i, c := range frames {
		if c.kind != kindSet|store.ChunkKindBit {
			t.Fatalf("chunk %d kind 0x%02x, want the set collection kind", i, c.kind)
		}
		if c.flags != 0 {
			t.Fatalf("chunk %d flags 0x%02x; valueless packing never sets a flag", i, c.flags)
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
				if md := memberDisc(p.Field); md != disc {
					t.Fatalf("chunk %d disc %d, want its first member's Disc %d", i, disc, md)
				}
				first = false
			}
			if len(p.Value) != 0 || p.Exp != 0 {
				t.Fatalf("member %q packed with value %q exp %d, want valueless", p.Field, p.Value, p.Exp)
			}
			got[string(p.Field)] = true
			return true
		})
		if !ok {
			t.Fatalf("chunk %d payload is torn", i)
		}
	}
}

func TestSetDemoteFramesCrossFoldTap(t *testing.T) {
	cx, g := coldCtx(t)
	members := gen("m", 0, 1000, 40)
	addKey(g, "k", members...)

	tapped := tapChunks(t, cx.St)
	if n := g.demote(cx, []byte("k")); n != len(members) {
		t.Fatalf("demoted %d members, want %d", n, len(members))
	}
	if len(*tapped) < 2 {
		t.Fatalf("tap heard %d chunk frames, want the pack to split into several", len(*tapped))
	}
	if got := g.m["k"].cold.dir.Len(); got != len(*tapped) {
		t.Fatalf("tap heard %d frames, directory holds %d chunks; the seam must carry every chunk", len(*tapped), got)
	}

	got := map[string]bool{}
	checkSetFrames(t, *tapped, got)
	if len(got) != len(members) {
		t.Fatalf("tapped frames decode %d members, table holds %d", len(got), len(members))
	}
	for _, m := range members {
		if !got[m] {
			t.Fatalf("member %q missing from the tapped frames", m)
		}
	}
}

// TestSetPartitionDemoteQuantaOrdered sweeps a partitioned set one quantum at a
// time and holds the per-quantum contract: every sweep's frames are internally
// nondecreasing, while separate quanta interleave their coordinate ranges in
// the shared directory, the state the doc 06 rewrite later re-sorts and the
// planners until then must refuse to merge silently (ErrDiscOrder).
func TestSetPartitionDemoteQuantaOrdered(t *testing.T) {
	withThreshold(t, 512)
	cx, g := coldCtx(t)
	members := gen("p", 0, 2000, 10)
	addKey(g, "k", members...)
	if enc := g.m["k"].enc; enc != encPartitioned {
		t.Fatalf("2000-member set enc %v, want partitioned", enc)
	}

	tapped := tapChunks(t, cx.St)
	got := map[string]bool{}
	for {
		*tapped = (*tapped)[:0]
		if g.demote(cx, []byte("k")) == 0 {
			break
		}
		if len(*tapped) == 0 {
			t.Fatal("a demote quantum crossed no frames")
		}
		checkSetFrames(t, *tapped, got)
	}
	if len(got) != len(members) {
		t.Fatalf("sweeps decoded %d members, want %d", len(got), len(members))
	}
}
