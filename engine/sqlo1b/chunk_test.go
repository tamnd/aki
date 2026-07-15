package sqlo1b

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cespare/xxhash/v2"
)

func TestChunkArithmetic(t *testing.T) {
	if chunkHdrSize+ChunkCap*chunkEntSize != ChunkSize {
		t.Fatalf("chunk layout %d+%d*%d != %d", chunkHdrSize, ChunkCap, chunkEntSize, ChunkSize)
	}
	if GroupSize/ChunkSize != chunksPerGroup {
		t.Fatalf("%d chunks to a group, const says %d", GroupSize/ChunkSize, chunksPerGroup)
	}
}

// TestChunkLayoutGolden pins the byte layout by hand: header fields
// at their offsets, entry 0 at byte 8, entry 1 at byte 20, all
// little-endian.
func TestChunkLayoutGolden(t *testing.T) {
	c := NewChunk(0xAABB_0000_0042)
	if err := c.InsertEntry(0x1234, 0x51, 0x0102030405060708); err != nil {
		t.Fatal(err)
	}
	if err := c.InsertEntry(0xFFFF, 0x02, 0x11); err != nil {
		t.Fatal(err)
	}
	b := c.Bytes()
	if got := binary.LittleEndian.Uint16(b[0:2]); got != 2 {
		t.Errorf("count %d, want 2", got)
	}
	if b[2] != 0 || b[3] != 0 {
		t.Errorf("cflags %#x reserved %#x, want zero", b[2], b[3])
	}
	if got := binary.LittleEndian.Uint32(b[4:8]); got != 0x0000_0042 {
		t.Errorf("chunk_no_lo %#x, want the low 32 bits 0x42", got)
	}
	want0 := []byte{0x34, 0x12, 0x51, 0x00, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}
	if !bytes.Equal(b[8:20], want0) {
		t.Errorf("entry 0 bytes % x, want % x", b[8:20], want0)
	}
	if b[20] != 0xFF || b[21] != 0xFF {
		t.Errorf("entry 1 fp bytes %#x %#x, want ff ff", b[20], b[21])
	}
	for i := 32; i < ChunkSize; i++ {
		if b[i] != 0 {
			t.Fatalf("byte %d is %#x past the live region", i, b[i])
		}
	}
}

func TestChunkRoundtrip(t *testing.T) {
	const chunkNo = uint64(7_000_000_001)
	c := NewChunk(chunkNo)
	type ent struct {
		fp, meta uint16
		vptr     uint64
	}
	var want []ent
	for i := range 40 {
		meta, err := MakeEntryMeta(uint8(i%16), uint8(i%4), i%5 == 0)
		if err != nil {
			t.Fatal(err)
		}
		e := ent{fp: uint16(i * 1543), meta: meta, vptr: uint64(i)<<24 | 7}
		want = append(want, e)
		if err := c.InsertEntry(e.fp, e.meta, e.vptr); err != nil {
			t.Fatal(err)
		}
	}
	p, err := ParseChunk(c.Bytes(), chunkNo)
	if err != nil {
		t.Fatal(err)
	}
	if p.Count() != len(want) {
		t.Fatalf("count %d, want %d", p.Count(), len(want))
	}
	for i, e := range want {
		fp, meta, vptr := p.EntryAt(i)
		if fp != e.fp || meta != e.meta || vptr != e.vptr {
			t.Fatalf("entry %d = (%#x, %#x, %#x), want (%#x, %#x, %#x)", i, fp, meta, vptr, e.fp, e.meta, e.vptr)
		}
		if MetaTypeTag(meta) != uint8(i%16) || MetaExpiryClass(meta) != uint8(i%4) || MetaRoot(meta) != (i%5 == 0) {
			t.Fatalf("entry %d meta fields diverged", i)
		}
	}
}

// TestChunkProbeCandidates pins the false-hit contract: every entry
// sharing the probed fingerprint is a candidate, yielded in slot
// order, and the caller stops the scan by returning false.
func TestChunkProbeCandidates(t *testing.T) {
	c := NewChunk(1)
	for i, fp := range []uint16{9, 700, 9, 41, 9} {
		if err := c.InsertEntry(fp, 0, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	var got []uint64
	c.Probe(9, func(i int, meta uint16, vptr uint64) bool {
		got = append(got, vptr)
		return true
	})
	if len(got) != 3 || got[0] != 0 || got[1] != 2 || got[2] != 4 {
		t.Fatalf("probe candidates %v, want [0 2 4] in slot order", got)
	}
	c.Probe(12345, func(int, uint16, uint64) bool {
		t.Fatal("absent fingerprint yielded a candidate")
		return false
	})
	var stops int
	c.Probe(9, func(int, uint16, uint64) bool {
		stops++
		return false
	})
	if stops != 1 {
		t.Fatalf("scan continued %d yields past a false return", stops)
	}
}

func TestChunkCapacity(t *testing.T) {
	c := NewChunk(3)
	for i := range ChunkCap {
		if err := c.InsertEntry(uint16(i), 0, uint64(i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := c.InsertEntry(0, 0, 0); err == nil {
		t.Fatal("entry 43 accepted")
	}
	if err := c.SetChain(0, 0); err == nil {
		t.Fatal("chain pointer accepted with 42 live entries")
	}

	pos, err := NewPos(9, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	ch := NewChunk(3)
	for i := range ChunkChainCap {
		if err := ch.InsertEntry(uint16(i), 0, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ch.SetChain(pos, 0xDEAD); err != nil {
		t.Fatal(err)
	}
	if err := ch.InsertEntry(0, 0, 0); err == nil {
		t.Fatal("entry 42 accepted on a chained chunk")
	}
}

func TestChunkChain(t *testing.T) {
	overflow := NewChunk(5)
	if err := overflow.InsertEntry(1, 0, 2); err != nil {
		t.Fatal(err)
	}
	pos, err := NewPos(12, 800, 3)
	if err != nil {
		t.Fatal(err)
	}
	check := ChunkCheck32(overflow.Bytes())

	c := NewChunk(5)
	if err := c.SetChain(pos, check); err != nil {
		t.Fatal(err)
	}
	if !c.Chained() {
		t.Fatal("chunk not chained after SetChain")
	}
	gotPos, gotCheck, err := c.ChainPtr()
	if err != nil {
		t.Fatal(err)
	}
	if gotPos != pos || gotCheck != check {
		t.Fatalf("chain pointer (%v, %#x), want (%v, %#x)", gotPos, gotCheck, pos, check)
	}
	if gotCheck != uint32(xxhash.Sum64(overflow.Bytes())) {
		t.Fatal("check word is not the truncated xxhash64 of the overflow image")
	}
	if _, err := ParseChunk(c.Bytes(), 5); err != nil {
		t.Fatalf("chained chunk fails strict parse: %v", err)
	}

	c.ClearChain()
	if c.Chained() {
		t.Fatal("chunk still chained after ClearChain")
	}
	if _, _, err := c.ChainPtr(); err == nil {
		t.Fatal("chain pointer read off an unchained chunk")
	}
	if _, err := ParseChunk(c.Bytes(), 5); err != nil {
		t.Fatalf("cleared chunk fails strict parse: %v", err)
	}

	badPos, err := NewPos(12, 800, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetChain(badPos, 0); err == nil {
		t.Fatal("chain pointer with slot 8 accepted, chunks sit 8 to a group")
	}
}

func TestChunkRemoveUpdate(t *testing.T) {
	c := NewChunk(2)
	for i := range 5 {
		if err := c.InsertEntry(uint16(100+i), 0, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.RemoveEntry(1); err != nil {
		t.Fatal(err)
	}
	fp, _, vptr := c.EntryAt(1)
	if fp != 104 || vptr != 4 {
		t.Fatalf("entry 1 = (%d, %d) after remove, want the swapped-in last entry (104, 4)", fp, vptr)
	}
	if _, err := ParseChunk(c.Bytes(), 2); err != nil {
		t.Fatalf("vacated slot broke strict parse: %v", err)
	}
	if err := c.RemoveEntry(4); err == nil {
		t.Fatal("removed an entry past count")
	}

	if err := c.SetEntry(0, 0x42, 0x999); err != nil {
		t.Fatal(err)
	}
	fp, meta, vptr := c.EntryAt(0)
	if fp != 100 || meta != 0x42 || vptr != 0x999 {
		t.Fatalf("entry 0 = (%d, %#x, %#x), want fp kept and (0x42, 0x999) set", fp, meta, vptr)
	}
	if err := c.SetEntry(9, 0, 0); err == nil {
		t.Fatal("updated an entry past count")
	}
	if err := c.SetEntry(0, 1<<7, 0); err == nil {
		t.Fatal("reserved meta bits accepted by SetEntry")
	}
	if err := c.InsertEntry(0, 1<<9, 0); err == nil {
		t.Fatal("reserved meta bits accepted by InsertEntry")
	}
}

func TestChunkParseRejects(t *testing.T) {
	base := func() []byte {
		c := NewChunk(77)
		for i := range 3 {
			if err := c.InsertEntry(uint16(i), 0, uint64(i)); err != nil {
				t.Fatal(err)
			}
		}
		return c.Bytes()
	}
	cases := []struct {
		name    string
		mutate  func(b []byte)
		chunkNo uint64
	}{
		{"count over 42", func(b []byte) { binary.LittleEndian.PutUint16(b[0:2], 43) }, 77},
		{"chained count over 41", func(b []byte) {
			b[2] |= CFlagChained
			binary.LittleEndian.PutUint16(b[0:2], 42)
		}, 77},
		{"unknown cflags", func(b []byte) { b[2] |= 1 << 3 }, 77},
		{"reserved header byte", func(b []byte) { b[3] = 1 }, 77},
		{"chunk number mismatch", func(b []byte) {}, 78},
		{"reserved meta bits", func(b []byte) { b[chunkHdrSize+2] |= 1 << 7 }, 77},
		{"garbage past live region", func(b []byte) { b[chunkHdrSize+5*chunkEntSize] = 9 }, 77},
		{"garbage in unchained chain slot", func(b []byte) { b[ChunkSize-1] = 1 }, 77},
		{"chain pointer slot past 7", func(b []byte) {
			b[2] |= CFlagChained
			binary.LittleEndian.PutUint64(b[ChunkSize-8:], 9)
		}, 77},
	}
	for _, tc := range cases {
		b := base()
		tc.mutate(b)
		if _, err := ParseChunk(b, tc.chunkNo); err == nil {
			t.Errorf("%s: parsed", tc.name)
		}
	}
	if _, err := ParseChunk(base()[:ChunkSize-1], 77); err == nil {
		t.Error("short image parsed")
	}
	if _, err := ParseChunk(base(), 77); err != nil {
		t.Errorf("legal image rejected: %v", err)
	}
	if _, err := ParseChunk(base(), 77+(1<<32)); err != nil {
		t.Errorf("chunk_no_lo must compare low 32 bits only: %v", err)
	}
}

func TestEntryMetaRejects(t *testing.T) {
	if _, err := MakeEntryMeta(16, 0, false); err == nil {
		t.Error("type tag 16 accepted")
	}
	if _, err := MakeEntryMeta(0, 4, false); err == nil {
		t.Error("expiry class 4 accepted")
	}
	meta, err := MakeEntryMeta(15, ExpClassFar, true)
	if err != nil {
		t.Fatal(err)
	}
	if MetaTypeTag(meta) != 15 || MetaExpiryClass(meta) != ExpClassFar || !MetaRoot(meta) {
		t.Fatalf("meta %#x fields diverged", meta)
	}
}

// TestFingerprintPlacementDisjoint pins the doc 8.3 bit split: the
// fingerprint is exactly bits 63..48 and placement exactly 47..0, so
// together they reconstruct the hash and neither leaks into the
// other.
func TestFingerprintPlacementDisjoint(t *testing.T) {
	for _, h := range []uint64{0, 1, placementMask, ^uint64(0), 0xDEADBEEF_12345678, KeyHash([]byte("k"))} {
		if got := uint64(Fingerprint(h))<<48 | PlacementBits(h); got != h {
			t.Fatalf("fingerprint and placement of %#x reassemble to %#x", h, got)
		}
	}
	if Fingerprint(placementMask) != 0 {
		t.Fatal("placement bits leaked into the fingerprint")
	}
	if PlacementBits(^uint64(0)) != placementMask {
		t.Fatal("fingerprint bits leaked into placement")
	}
}

// TestChunkFullPtrIntegration ties the chunk's integrity story to
// its referencer: the directory's full pointer must verify a clean
// image and catch any flip, because the chunk carries no checksum of
// its own.
func TestChunkFullPtrIntegration(t *testing.T) {
	c := NewChunk(9)
	if err := c.InsertEntry(1, 2, 3); err != nil {
		t.Fatal(err)
	}
	pos, err := NewPos(4, 100, 1)
	if err != nil {
		t.Fatal(err)
	}
	fp := MakeFullPtr(pos, c.Bytes())
	if err := fp.Verify(c.Bytes()); err != nil {
		t.Fatal(err)
	}
	c.Bytes()[100] ^= 1
	if err := fp.Verify(c.Bytes()); err == nil {
		t.Fatal("flipped chunk byte verified")
	}
}
