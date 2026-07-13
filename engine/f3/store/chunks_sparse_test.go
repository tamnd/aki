package store

import (
	"bytes"
	"testing"
)

// TestSparseChunkHoleUnstored proves a SETBIT far past the end stores only the
// live covering chunk: the gap chunks are directory holes that consume no arena
// run bytes, so the arena's handed-out bytes stay near one chunk even though the
// logical value spans many chunks.
func TestSparseChunkHoleUnstored(t *testing.T) {
	s := New(4<<20, 1<<18)
	base0, _ := s.ArenaBytes()
	// A bit deep in chunk 16 of an otherwise empty key: the logical value is
	// past a megabyte, but only chunk 16 holds a run.
	off := int64(16)*ChunkSize*8 + 3
	if _, err := s.SetBit([]byte("dau"), off, 1, 1); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.StrLen([]byte("dau"), 1); n < 16*ChunkSize {
		t.Fatalf("StrLen = %d; want >= %d", n, 16*ChunkSize)
	}
	used, _ := s.ArenaBytes()
	// One live chunk plus its directory (17 pointers) is the whole footprint;
	// the 16 gap chunks would be another megabyte if they were stored.
	if grew := used - base0; grew > 3*ChunkSize {
		t.Fatalf("arena grew %d bytes for a one-chunk sparse value; holes not unstored", grew)
	}
	if got := s.GetBit([]byte("dau"), off, 1); got != 1 {
		t.Fatalf("GetBit set bit = %d; want 1", got)
	}
}

// TestSparseChunkReadsZero proves reads over holes return zeros on every read
// path: the whole-value read, GetBit across the gap, and a fresh stream.
func TestSparseChunkReadsZero(t *testing.T) {
	s := New(4<<20, 1<<18)
	hi := int64(8) * ChunkSize * 8  // first bit of chunk 8
	s.SetBit([]byte("k"), 3, 1, 1)  // chunk 0 live
	s.SetBit([]byte("k"), hi, 1, 1) // chunk 8 live, chunks 1..7 holes

	// Whole-value read: every bit but the two set ones reads 0, including the
	// hole chunks in between.
	v, _ := s.GetString([]byte("k"), 1, nil)
	set := map[int64]bool{3: true, hi: true}
	for _, off := range []int64{0, 1, 3, ChunkSize * 8, 4 * ChunkSize * 8, hi, hi + 1} {
		want := 0
		if set[off] {
			want = 1
		}
		if bitOf(v, off) != want {
			t.Fatalf("bit %d = %d; want %d", off, bitOf(v, off), want)
		}
	}
	// GetBit reads straight through a hole chunk.
	if got := s.GetBit([]byte("k"), 4*ChunkSize*8+11, 1); got != 0 {
		t.Fatalf("GetBit in hole = %d; want 0", got)
	}
	// The streamed read reconstructs the same bytes a materialized read gives.
	full, _, ok := s.GetStream([]byte("k"), 1, nil)
	if ok && full != nil {
		if !bytes.Equal(full, v) {
			t.Fatal("stream materialized read disagrees with GetString")
		}
	} else {
		cs := mustStream(t, s, []byte("k"))
		got := drainStream(t, cs)
		if !bytes.Equal(got, v) {
			t.Fatalf("streamed value len %d disagrees with GetString len %d", len(got), len(v))
		}
	}
}

// TestSparseHoleMaterializes proves a later write into a hole chunk turns it
// into a real run without disturbing its neighbors, and that dropping the key
// releases cleanly with holes present.
func TestSparseHoleMaterializes(t *testing.T) {
	s := New(4<<20, 1<<18)
	s.SetBit([]byte("k"), 3, 1, 1)
	hi := int64(8) * ChunkSize * 8
	s.SetBit([]byte("k"), hi, 1, 1)
	// Write into chunk 4, previously a hole.
	mid := int64(4)*ChunkSize*8 + 20
	if _, err := s.SetBit([]byte("k"), mid, 1, 1); err != nil {
		t.Fatal(err)
	}
	v, _ := s.GetString([]byte("k"), 1, nil)
	for _, off := range []int64{3, mid, hi} {
		if bitOf(v, off) != 1 {
			t.Fatalf("bit %d = 0 after materialize; want 1", off)
		}
	}
	// A neighbor that was a hole and stayed one still reads 0.
	if bitOf(v, 6*ChunkSize*8+1) != 0 {
		t.Fatal("neighbor hole not zero after materialize")
	}
	// Del drops every run and the directory without touching the holes.
	if !s.Del([]byte("k"), 1) {
		t.Fatal("Del missing key")
	}
	if s.Exists([]byte("k"), 1) {
		t.Fatal("key survived Del")
	}
}

// mustStream opens a ChunkStream for a chunked key or fails the test.
func mustStream(t *testing.T, s *Store, key []byte) *ChunkStream {
	t.Helper()
	_, cs, ok := s.GetStream(key, 1, nil)
	if !ok || cs == nil {
		t.Fatal("expected a chunk stream")
	}
	return cs
}

// drainStream reads a ChunkStream to exhaustion and returns the bytes.
func drainStream(t *testing.T, cs *ChunkStream) []byte {
	t.Helper()
	defer cs.Release()
	var out []byte
	buf := make([]byte, ChunkSize)
	for {
		n, err := cs.Next(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return out
		}
		out = append(out, buf[:n]...)
	}
}
