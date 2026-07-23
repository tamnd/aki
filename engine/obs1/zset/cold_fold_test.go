package zset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The zset demote-to-fold seam (spec 2064/obs1 doc 08 section 5): one demote
// pass emits both projections. The score runs cross the tap through
// AppendChunkFold as they land locally, and after every append succeeds the
// member-hash chunks cross through EmitFoldChunk with no local copy. These
// tests register a tap, run the real demote, and hold the frames to the dual
// contract: the two projections carry distinct kinds, each is ordered by its
// own coordinate (score-key-first discs for the runs, big-endian member Disc
// for the hash chunks), the packed pair multisets are identical (the T-I3
// cross-check seed), and the local tier keeps only the score copy.

type zsetTapped struct {
	kind    byte
	flags   byte
	count   uint16
	key     []byte
	disc    []byte
	payload []byte
}

// tapZsetChunks registers a fold tap that copies every chunk frame out of the
// drain buffer, which is only valid during the callback.
func tapZsetChunks(t *testing.T, st *store.Store) *[]zsetTapped {
	t.Helper()
	var out []zsetTapped
	st.SetFoldTap(func(buf []byte) {
		err := store.WalkStagedFrames(buf, func(f store.FoldFrame) error {
			if !f.Chunk {
				return fmt.Errorf("a non-chunk frame crossed the demote tap (kind 0x%02x)", f.Kind)
			}
			out = append(out, zsetTapped{
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

func TestDemoteEmitsDualProjection(t *testing.T) {
	st := coldStore(t)
	// Wide members so both packs split into several chunks.
	n := newNativeStore(400)
	raw := gen(0, 400, 96)
	members := make([][]byte, len(raw))
	for i, m := range raw {
		members[i] = []byte(m)
		n.insert(members[i], float64(i))
	}

	tapped := tapZsetChunks(t, st)
	if got := n.demote(st, []byte("z"), len(members)); got != len(members) {
		t.Fatalf("demote %d, want %d", got, len(members))
	}

	// Split the tap by kind; every frame must be one of the two projections
	// under the table's key.
	var scores, hashes []zsetTapped
	for i, c := range *tapped {
		if string(c.key) != "z" {
			t.Fatalf("chunk %d key %q, want the table key", i, c.key)
		}
		switch c.kind {
		case kindZsetScore | store.ChunkKindBit:
			scores = append(scores, c)
		case kindZset | store.ChunkKindBit:
			hashes = append(hashes, c)
		default:
			t.Fatalf("chunk %d kind 0x%02x, want a zset projection kind", i, c.kind)
		}
	}
	if len(scores) < 2 || len(hashes) < 2 {
		t.Fatalf("tap heard %d score and %d member chunks, want both packs to split", len(scores), len(hashes))
	}

	// The local tier keeps only the score copy: one directory descriptor and
	// one offset per score run, none for the member chunks.
	if got := n.cold.dir.Len(); got != len(scores) {
		t.Fatalf("local directory holds %d chunks, tap heard %d score runs", got, len(scores))
	}
	if got := len(n.cold.offs); got != len(scores) {
		t.Fatalf("offset table holds %d chunks, tap heard %d score runs", got, len(scores))
	}

	// Score runs: discs in nondecreasing byte order, each leading 8 bytes the
	// first pair's score key (the fold coordinate disc64 lifts), values the raw
	// score bits.
	scorePairs := map[string]uint64{}
	var prevDisc []byte
	for i, c := range scores {
		if len(c.disc) < 8 {
			t.Fatalf("score chunk %d disc is %d bytes, want at least the 8 score-key bytes", i, len(c.disc))
		}
		if prevDisc != nil && bytes.Compare(c.disc, prevDisc) < 0 {
			t.Fatalf("score chunk %d disc below its predecessor", i)
		}
		prevDisc = c.disc
		first := true
		var prevKey uint64
		ok := store.WalkPackedPairs(c.payload, c.flags, int(c.count), func(_ int, p store.PackedPair) bool {
			bits := binary.BigEndian.Uint64(p.Value)
			sk := scoreKey(math.Float64frombits(bits))
			if first {
				if got := binary.BigEndian.Uint64(c.disc); got != sk {
					t.Fatalf("score chunk %d disc leads with %d, want the first pair's score key %d", i, got, sk)
				}
				first = false
			} else if sk < prevKey {
				t.Fatalf("score chunk %d pairs out of score order", i)
			}
			prevKey = sk
			scorePairs[string(p.Field)] = bits
			return true
		})
		if !ok {
			t.Fatalf("score chunk %d payload is torn", i)
		}
	}

	// Member chunks: 8-byte discs in nondecreasing order matching each chunk's
	// first member's Disc, pairs sorted by the member coordinate, same values.
	hashPairs := map[string]uint64{}
	var prev uint64
	for i, c := range hashes {
		if len(c.disc) != 8 {
			t.Fatalf("member chunk %d disc is %d bytes, want 8", i, len(c.disc))
		}
		disc := binary.BigEndian.Uint64(c.disc)
		if disc < prev {
			t.Fatalf("member chunk %d disc %d below its predecessor %d", i, disc, prev)
		}
		prev = disc
		first := true
		var prevD uint64
		ok := store.WalkPackedPairs(c.payload, c.flags, int(c.count), func(_ int, p store.PackedPair) bool {
			d := obs1.Disc(p.Field)
			if first {
				if d != disc {
					t.Fatalf("member chunk %d disc %d, want its first member's Disc %d", i, disc, d)
				}
				first = false
			} else if d < prevD {
				t.Fatalf("member chunk %d pairs out of Disc order", i)
			}
			prevD = d
			hashPairs[string(p.Field)] = binary.BigEndian.Uint64(p.Value)
			return true
		})
		if !ok {
			t.Fatalf("member chunk %d payload is torn", i)
		}
	}

	// T-I3 seed: the two projections carry the identical pair multiset, and it
	// is exactly the demoted band.
	if len(scorePairs) != len(members) || len(hashPairs) != len(members) {
		t.Fatalf("projections decode %d and %d pairs, want %d", len(scorePairs), len(hashPairs), len(members))
	}
	for i, m := range members {
		sb, ok1 := scorePairs[string(m)]
		hb, ok2 := hashPairs[string(m)]
		if !ok1 || !ok2 {
			t.Fatalf("member %q missing from a projection (score %v, hash %v)", m, ok1, ok2)
		}
		if want := math.Float64bits(float64(i)); sb != want || hb != want {
			t.Fatalf("member %q bits %d/%d across projections, want %d", m, sb, hb, want)
		}
	}

	// The local locators still read back: every member streams in order with
	// the cold bytes preadd through the score runs.
	ms, scs := walkAll(n)
	if len(ms) != len(members) {
		t.Fatalf("each streamed %d after demote, want %d", len(ms), len(members))
	}
	for i := range ms {
		if !bytes.Equal(ms[i], members[i]) || scs[i] != float64(i) {
			t.Fatalf("rank %d after demote = %q/%v, want %q", i, ms[i], scs[i], members[i])
		}
	}
}
