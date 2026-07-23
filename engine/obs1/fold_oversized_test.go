package obs1_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// TestFolderOversizedRoundTrip is the strings-slice read-planning proof
// (spec 2064/obs1 doc 08 section 2): oversized values leave the real store
// through the staged drain with their bytes resolved into the frame, fold
// into segments (the widest as a jumbo block, past SegmentBlockSize), and
// each one serves cold in exactly one ranged GET: the keymap locator
// resolves to a block span that covers the whole chunk, and the decoded
// block holds the full value.
func TestFolderOversizedRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := store.Open(store.Options{
		ArenaBytes:       16 << 20,
		SegBytes:         256 << 10,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: 256 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	fx, km, segdir := newFoldDirFixture(t)
	s.SetFoldTap(fx.folder.Add)

	mkVal := func(seed byte, n int) []byte {
		v := make([]byte, n)
		for i := range v {
			v[i] = seed + byte(i*7)
		}
		return v
	}
	vals := map[string][]byte{
		"sep:small":   mkVal(1, 4<<10),        // separated band
		"chunk:block": mkVal(2, 100<<10),      // chunked, fits one 128 KiB block
		"chunk:jumbo": mkVal(3, 300<<10),      // chunked, folds as a jumbo block
		"emb:tiny":    []byte("small-enough"), // the already-working control
	}
	for k, v := range vals {
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	for i := range 4000 {
		k := fmt.Appendf(nil, "ballast:%05d", i)
		if err := s.Set(k, fmt.Appendf(nil, "v-%d", i)); err != nil {
			t.Fatalf("ballast %d: %v", i, err)
		}
	}

	buf := make([]byte, 0, 256<<10)
	for pass := 0; pass < 4096; pass++ {
		d := s.StageColdDrainDeep(buf)
		if d == nil || len(d.Buf()) == 0 {
			break
		}
		if _, werr := s.ColdWriteAt(d.Off(), d.Buf()); werr != nil {
			t.Fatalf("cold write: %v", werr)
		}
		s.CompleteColdDrain(d, true)
	}
	fx.folder.Flush()
	waitFor(t, "publish", func() bool {
		st := fx.folder.Stats()
		return st.SegmentsCut > 0 && uint64(len(fx.folder.Ledger())) == st.SegmentsCut
	})
	if st := fx.folder.Stats(); st.PointerSkipped != 0 {
		t.Fatalf("%d pointer frames crossed the fold tap unresolved", st.PointerSkipped)
	}

	for name, want := range vals {
		loc, ok := km.Lookup(obs1.Fingerprint([]byte(name)))
		if !ok {
			t.Fatalf("%s not in the keymap after fold", name)
		}
		ref, ok := segdir.Resolve(loc)
		if !ok {
			t.Fatalf("%s locator does not resolve", name)
		}
		// The one GET: the block span covers the whole chunk, jumbo or not.
		off, n := ref.Block.BlockSpan()
		raw, _, gerr := fx.sim.GetRange(ctx, ref.ObjKey, off, n)
		if gerr != nil {
			t.Fatalf("%s ranged GET: %v", name, gerr)
		}
		data, perr := obs1.ParseSegmentBlock(raw, ref.Block)
		if perr != nil {
			t.Fatalf("%s block parse: %v", name, perr)
		}
		total := binary.LittleEndian.Uint32(data[ref.OffInBlock:])
		var got []byte
		found := false
		werr := store.WalkStagedFrames(data[ref.OffInBlock:uint32(ref.OffInBlock)+total], func(f store.FoldFrame) error {
			if !f.Chunk {
				t.Fatalf("%s locator points at a bare frame", name)
			}
			return store.WalkStagedFrames(f.Payload, func(r store.FoldFrame) error {
				if r.Pointer {
					t.Fatalf("folded frame for %q is a pointer band", r.Key)
				}
				if string(r.Key) == name {
					found = true
					got = r.Payload
				}
				return nil
			})
		})
		if werr != nil {
			t.Fatalf("%s chunk walk: %v", name, werr)
		}
		if !found {
			t.Fatalf("%s not inside its resolved chunk", name)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s folded value: %d bytes, want %d", name, len(got), len(want))
		}
		if len(want) > obs1.SegmentBlockSize && int64(len(raw)) <= int64(obs1.SegmentBlockSize) {
			t.Fatalf("%s should have folded as a jumbo block, span was %d bytes", name, len(raw))
		}
	}
}
