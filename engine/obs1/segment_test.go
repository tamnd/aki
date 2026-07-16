package obs1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// chunkFrame builds an f3-shaped frame: leading total u32, then filler.
func chunkFrame(n int) []byte {
	if n < 4 {
		panic("frame too small")
	}
	b := binary.LittleEndian.AppendUint32(nil, uint32(n))
	for len(b) < n {
		b = append(b, byte(len(b)))
	}
	return b
}

func sampleChunks() []SegmentChunk {
	return []SegmentChunk{
		{Key: []byte("user:1"), Kind: 1, FirstDisc: 0, Count: 1, LiveHint: 1, Data: chunkFrame(100)},
		{Key: []byte("zset:a"), Kind: 3, FirstDisc: 7, Count: 40, LiveHint: 38, Data: chunkFrame(900)},
		{Key: []byte("list:b"), Kind: 4, FirstDisc: 1, Count: 12, LiveHint: 12, Data: chunkFrame(300)},
	}
}

func sampleSegmentFooter() SegmentFooter {
	return SegmentFooter{Group: 5, Epoch: 3, SegSeq: 11, Level: 0, TTLClass: 0}
}

func mustSegment(t *testing.T, blockSize int) (*Segment, []byte) {
	t.Helper()
	keys := [][]byte{[]byte("user:1"), []byte("zset:a"), []byte("list:b")}
	seg, err := BuildSegment(sampleSegmentFooter(), sampleChunks(), keys, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	b, err := AppendSegment(nil, 9, seg)
	if err != nil {
		t.Fatal(err)
	}
	return seg, b
}

func TestSegmentRoundTrip(t *testing.T) {
	_, b := mustSegment(t, 0)
	seg, h, err := ParseSegment(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.Format != FormatSegment || h.Writer != 9 {
		t.Fatalf("header %+v", h)
	}
	f := seg.Footer
	if f.Group != 5 || f.SegSeq != 11 || len(f.Blocks) != 1 || len(f.Chunks) != 3 {
		t.Fatalf("footer %+v", f)
	}
	if f.NRecords != 53 || f.RawBytes != 1300 {
		t.Fatalf("derived totals %d records %d raw", f.NRecords, f.RawBytes)
	}
	if f.Chunks[1].OffInBlock != 100 || f.Chunks[2].OffInBlock != 1000 {
		t.Fatalf("chunk offsets %+v", f.Chunks)
	}
	again, err := AppendSegment(nil, h.Writer, seg)
	if err != nil || !bytes.Equal(again, b) {
		t.Fatalf("re-encode differs (err %v)", err)
	}
}

func TestSegmentPacking(t *testing.T) {
	// A 512-byte block size forces the 900-byte chunk into a jumbo block
	// of exactly its size and never splits a chunk across blocks.
	seg, b := mustSegment(t, 512)
	if len(seg.BlockData) != 3 {
		t.Fatalf("%d blocks, want 3", len(seg.BlockData))
	}
	if len(seg.BlockData[0]) != 100 || len(seg.BlockData[1]) != 900 || len(seg.BlockData[2]) != 300 {
		t.Fatalf("block sizes %d %d %d", len(seg.BlockData[0]), len(seg.BlockData[1]), len(seg.BlockData[2]))
	}
	for i, c := range seg.Footer.Chunks {
		if c.Block != uint32(i) || c.OffInBlock != 0 {
			t.Fatalf("chunk %d placed at block %d off %d", i, c.Block, c.OffInBlock)
		}
	}
	if _, _, err := ParseSegment(b); err != nil {
		t.Fatal(err)
	}
}

// TestSegmentRangedPath walks the serving route: tail, footer, then one
// block by its index entry, no whole-object parse.
func TestSegmentRangedPath(t *testing.T) {
	seg, b := mustSegment(t, 512)
	footerOff, footerLen, err := ParseTail(b[len(b)-TailSize:])
	if err != nil {
		t.Fatal(err)
	}
	f, err := ParseSegmentFooter(b[footerOff : footerOff+uint64(footerLen)])
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Blocks) != 3 || len(f.Chunks) != 3 {
		t.Fatalf("footer %+v", f)
	}
	off, n := f.Blocks[1].BlockSpan()
	data, err := ParseSegmentBlock(b[off:off+n], f.Blocks[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, seg.BlockData[1]) {
		t.Fatal("block data differs")
	}
	c := f.Chunks[1]
	total := binary.LittleEndian.Uint32(data[c.OffInBlock:])
	if total != 900 {
		t.Fatalf("chunk frame total %d", total)
	}
}

func TestSegmentBuilderRejects(t *testing.T) {
	f := sampleSegmentFooter()
	keys := [][]byte{[]byte("k")}
	if _, err := BuildSegment(f, nil, keys, 0); err == nil {
		t.Error("zero chunks accepted")
	}
	bad := []SegmentChunk{{Key: []byte("k"), Count: 1, Data: []byte{1, 2, 3, 4}}}
	binary.LittleEndian.PutUint32(bad[0].Data, 99)
	if _, err := BuildSegment(f, bad, keys, 0); err == nil {
		t.Error("lying frame total accepted")
	}
	zero := []SegmentChunk{{Key: []byte("k"), Count: 0, Data: chunkFrame(8)}}
	if _, err := BuildSegment(f, zero, keys, 0); err == nil {
		t.Error("zero-record chunk accepted")
	}
}

func TestSegmentAppendRejects(t *testing.T) {
	base := func() *Segment {
		seg, _ := mustSegment(t, 0)
		return seg
	}
	cases := map[string]func(*Segment){
		"level 2":                  func(s *Segment) { s.Footer.Level = 2 },
		"class 0 with expiry":      func(s *Segment) { s.Footer.MaxExpMS = 5 },
		"class 3 without expiry":   func(s *Segment) { s.Footer.TTLClass = 3 },
		"class 3 min past max":     func(s *Segment) { s.Footer.TTLClass = 3; s.Footer.MinExpMS = 9; s.Footer.MaxExpMS = 4 },
		"ragged bloom":             func(s *Segment) { s.Footer.Bloom = s.Footer.Bloom[:63] },
		"chunk past its block":     func(s *Segment) { s.Footer.Chunks[2].OffInBlock = 1 << 20 },
		"chunk at a missing block": func(s *Segment) { s.Footer.Chunks[2].Block = 9 },
		"overlapping chunks":       func(s *Segment) { s.Footer.Chunks[2].OffInBlock = 999 },
		"chunks out of order":      func(s *Segment) { s.Footer.Chunks[0], s.Footer.Chunks[2] = s.Footer.Chunks[2], s.Footer.Chunks[0] },
	}
	for name, mut := range cases {
		seg := base()
		mut(seg)
		if _, err := AppendSegment(nil, 1, seg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestSegmentParseRejects(t *testing.T) {
	_, good := mustSegment(t, 512)

	flip := func(name string, i int) {
		t.Helper()
		b := append([]byte(nil), good...)
		b[i] ^= 0x01
		if _, _, err := ParseSegment(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	flip("block header rawlen", HeaderSize)
	flip("block comp byte", HeaderSize+8)
	flip("block reserved byte", HeaderSize+9)
	flip("block data byte", HeaderSize+segBlockHdr+4)
	flip("tail crc", len(good)-1)
	footerOff, _, err := ParseTail(good[len(good)-TailSize:])
	if err != nil {
		t.Fatal(err)
	}
	flip("footer group byte", int(footerOff))
	flip("footer nrecords", len(good)-TailSize-12)

	if _, _, err := ParseSegment(good[:len(good)-1]); err == nil {
		t.Error("truncated object accepted")
	}
	crossType := AppendHeader(nil, Header{Format: FormatWAL, FVersion: 1, Writer: 1})
	crossType = append(crossType, good[HeaderSize:]...)
	if _, _, err := ParseSegment(crossType); err == nil {
		t.Error("cross-typed object accepted")
	}
}

func TestBloom(t *testing.T) {
	var keys [][]byte
	for i := range 1000 {
		keys = append(keys, fmt.Appendf(nil, "member:%d", i))
	}
	filter := BuildBloom(keys)
	if len(filter)%bloomBlockBytes != 0 {
		t.Fatalf("filter is %d bytes", len(filter))
	}
	for _, k := range keys {
		if !BloomMayContain(filter, k) {
			t.Fatalf("false negative on %q", k)
		}
	}
	fp := 0
	const probes = 20000
	for i := range probes {
		if BloomMayContain(filter, fmt.Appendf(nil, "absent:%d", i)) {
			fp++
		}
	}
	// 10 bits per key in a blocked layout lands near 1 to 2 percent; 5 is
	// the alarm threshold, not the target.
	if rate := float64(fp) / probes; rate > 0.05 {
		t.Fatalf("false positive rate %.4f", rate)
	}
	if !BloomMayContain(nil, []byte("k")) || !BloomMayContain(make([]byte, 63), []byte("k")) {
		t.Fatal("malformed filters must fail open")
	}
	if BuildBloom(nil) == nil || len(BuildBloom(nil)) != bloomBlockBytes {
		t.Fatal("empty filter must still be one block")
	}
}

func FuzzParseSegment(f *testing.F) {
	_, one := func() (*Segment, []byte) {
		keys := [][]byte{[]byte("k")}
		seg, _ := BuildSegment(sampleSegmentFooter(), []SegmentChunk{{Key: []byte("k"), Count: 1, Data: chunkFrame(16)}}, keys, 0)
		b, _ := AppendSegment(nil, 1, seg)
		return seg, b
	}()
	ttl := SegmentFooter{Group: 1, Epoch: 1, SegSeq: 2, Level: 1, TTLClass: 2, MinExpMS: 100, MaxExpMS: 900}
	seg2, _ := BuildSegment(ttl, sampleChunks(), [][]byte{[]byte("a"), []byte("b")}, 512)
	multi, _ := AppendSegment(nil, 1<<40, seg2)
	f.Add(one)
	f.Add(multi)
	f.Add(one[:HeaderSize])
	f.Add(multi[:len(multi)-1])
	f.Add(append(append([]byte(nil), one...), 0))
	for _, off := range []int{0, 16, HeaderSize, HeaderSize + 8, len(one) - TailSize, len(one) - 1} {
		b := append([]byte(nil), one...)
		b[off] ^= 0x80
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		seg, h, err := ParseSegment(b)
		if err != nil {
			return
		}
		again, err := AppendSegment(nil, h.Writer, seg)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("accepted bytes do not re-encode to the input (err %v)", err)
		}
	})
}
