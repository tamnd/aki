package obs1

import (
	"strings"
	"testing"
)

// dirFooter builds a minimal parsed-footer shape for Add: two blocks and
// chunks laid out across them.
func dirFooter(segSeq uint64, chunks ...SegmentChunkEntry) *SegmentFooter {
	return &SegmentFooter{
		SegSeq: segSeq,
		Blocks: []SegmentBlockEntry{
			{Offset: 32, StoredLen: 1000, RawLen: 1000, CRC: 0xAB},
			{Offset: 1048, StoredLen: 500, RawLen: 500, CRC: 0xCD},
		},
		Chunks: chunks,
	}
}

func TestDirectoryAddResolve(t *testing.T) {
	d := NewDirectory()
	if err := d.Add("db/t/seg/g003/01", dirFooter(1,
		SegmentChunkEntry{Block: 0, OffInBlock: 0, Kind: 0x81, FirstDisc: 10, Count: 3},
		SegmentChunkEntry{Block: 1, OffInBlock: 40, Kind: 0xFF, FirstDisc: 90, Count: 1},
	)); err != nil {
		t.Fatal(err)
	}
	if err := d.Add("db/t/seg/g003/02", dirFooter(2,
		SegmentChunkEntry{Block: 0, OffInBlock: 8, Kind: 0x81, FirstDisc: 5, Count: 2},
	)); err != nil {
		t.Fatal(err)
	}

	ref, ok := d.Resolve(KeyLoc{Seg: 1, Chunk: 1})
	if !ok {
		t.Fatal("resolve missed a live chunk")
	}
	if ref.ObjKey != "db/t/seg/g003/01" || ref.OffInBlock != 40 || ref.ChunkKind != 0xFF {
		t.Fatalf("ref %+v", ref)
	}
	if off, n := ref.Block.BlockSpan(); off != 1048 || n != 516 {
		t.Fatalf("block span %d+%d", off, n)
	}
	if ref, ok = d.Resolve(KeyLoc{Seg: 2, Chunk: 0}); !ok || ref.ObjKey != "db/t/seg/g003/02" {
		t.Fatalf("second segment ref %+v ok=%v", ref, ok)
	}

	if _, ok = d.Resolve(KeyLoc{Seg: 3, Chunk: 0}); ok {
		t.Fatal("resolved an unknown segment")
	}
	if _, ok = d.Resolve(KeyLoc{Seg: 1, Chunk: 2}); ok {
		t.Fatal("resolved a chunk past the index")
	}
	if _, ok = d.Resolve(KeyLoc{Seg: 1, Chunk: 0, Tier: 1}); ok {
		t.Fatal("resolved a non-segment tier")
	}
	if d.Segments() != 2 || d.Chunks() != 3 {
		t.Fatalf("segments %d chunks %d", d.Segments(), d.Chunks())
	}
}

func TestDirectoryAddRefuses(t *testing.T) {
	d := NewDirectory()
	cases := []struct {
		name string
		f    *SegmentFooter
		want string
	}{
		{"seq zero", dirFooter(0, SegmentChunkEntry{Block: 0}), "SegSeq 0"},
		{"seq wide", dirFooter(1<<32, SegmentChunkEntry{Block: 0}), "32 bits"},
		{"no chunks", dirFooter(4), "0 chunks"},
		{"chunk past blocks", dirFooter(5, SegmentChunkEntry{Block: 2}), "points at block"},
	}
	for _, c := range cases {
		err := d.Add("k", c.f)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: err %v", c.name, err)
		}
	}
	noBlocks := &SegmentFooter{SegSeq: 6, Chunks: []SegmentChunkEntry{{Block: 0}}}
	if err := d.Add("k", noBlocks); err == nil {
		t.Fatal("added a segment with no blocks")
	}
	if err := d.Add("k", dirFooter(7, SegmentChunkEntry{Block: 0, Count: 1})); err != nil {
		t.Fatal(err)
	}
	err := d.Add("k", dirFooter(7, SegmentChunkEntry{Block: 0, Count: 1}))
	if err == nil || !strings.Contains(err.Error(), "already holds") {
		t.Fatalf("duplicate add err %v", err)
	}
	if d.Segments() != 1 {
		t.Fatalf("segments %d after refusals", d.Segments())
	}
}

func TestDirectoryDropAndBytes(t *testing.T) {
	d := NewDirectory()
	if d.Bytes() != 0 {
		t.Fatalf("empty bytes %d", d.Bytes())
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if err := d.Add("db/t/seg/g003/x", dirFooter(seq,
			SegmentChunkEntry{Block: 0, Count: 1},
			SegmentChunkEntry{Block: 1, Count: 1},
		)); err != nil {
			t.Fatal(err)
		}
	}
	per := dirSegOverhead + len("db/t/seg/g003/x") + 2*segBlockEntry + 2*24
	if d.Bytes() != 3*per {
		t.Fatalf("bytes %d want %d", d.Bytes(), 3*per)
	}
	if !d.Drop(2) {
		t.Fatal("drop missed a live segment")
	}
	if d.Drop(2) {
		t.Fatal("dropped a segment twice")
	}
	if _, ok := d.Resolve(KeyLoc{Seg: 2, Chunk: 0}); ok {
		t.Fatal("resolved a dropped segment")
	}
	if d.Segments() != 2 || d.Chunks() != 4 || d.Bytes() != 2*per {
		t.Fatalf("segments %d chunks %d bytes %d after drop", d.Segments(), d.Chunks(), d.Bytes())
	}
	d.Drop(1)
	d.Drop(3)
	if d.Bytes() != 0 || d.Chunks() != 0 {
		t.Fatalf("bytes %d chunks %d after full drop", d.Bytes(), d.Chunks())
	}
}
