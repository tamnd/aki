package store

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"strconv"
	"testing"
)

// addrOf returns the arena offset the index holds for key, or 0 when absent. The
// codec tests frame a record straight from its arena offset, so they reach past
// the public API for it. now is 0 so the lookup never reaps.
func addrOf(s *Store, key string) uint64 {
	_, addr, _ := s.findLive(Hash([]byte(key)), []byte(key), 0)
	return addr
}

// rehydrate reconstructs a cold frame's value the way materialize reconstructs a
// resident record's: an int cell rendered to decimal, a separated run resolved
// through its framed pointer (arena or log), embedded bytes served as-is. It
// reads only from the frame (and, for a separated value, the run the pointer
// names), so a match against materialize(off) proves the frame carries
// everything the value needs to come back.
func (s *Store) rehydrate(f coldFrame, dst []byte) ([]byte, error) {
	switch {
	case f.flags&flagInt != 0:
		n := int64(binary.LittleEndian.Uint64(f.value))
		return strconv.AppendInt(dst[:0], n, 10), nil
	case f.flags&flagSep != 0:
		word := binary.LittleEndian.Uint64(f.value)
		vlen := binary.LittleEndian.Uint32(f.value[8:])
		if word&inLogBit != 0 {
			return s.vlog.readInto(word&runAddrMask, int(vlen), dst)
		}
		run := word & runAddrMask
		return append(dst[:0], s.arena.buf[run:run+uint64(vlen)]...), nil
	default:
		return append(dst[:0], f.value...), nil
	}
}

// checkFrame frames the record at key, decodes the frame, and asserts the header
// fields round-trip, the value region is byte-identical, and the frame rehydrates
// to the same bytes materialize serves. It returns the frame bytes so a caller
// can also test the self-delimiting walk over several of them.
func checkFrame(t *testing.T, s *Store, key string) []byte {
	t.Helper()
	off := addrOf(s, key)
	if off == 0 {
		t.Fatalf("%s: no record", key)
	}
	frame := s.frameRecord(off, nil)

	fr, n, err := decodeColdFrame(frame)
	if err != nil {
		t.Fatalf("%s: decode: %v", key, err)
	}
	if n != len(frame) {
		t.Fatalf("%s: decode consumed %d, frame is %d", key, n, len(frame))
	}
	if fr.kind != s.arena.buf[off+offKind] {
		t.Fatalf("%s: kind %d, want %d", key, fr.kind, s.arena.buf[off+offKind])
	}
	if fr.flags != s.recFlags(off) {
		t.Fatalf("%s: flags %#x, want %#x", key, fr.flags, s.recFlags(off))
	}
	if fr.vlen != uint32(s.vlen(off)) {
		t.Fatalf("%s: vlen %d, want %d", key, fr.vlen, s.vlen(off))
	}
	if string(fr.key) != key {
		t.Fatalf("%s: key %q round-tripped as %q", key, key, fr.key)
	}
	if !bytes.Equal(fr.value, s.valueRegion(off)) {
		t.Fatalf("%s: value region not byte-identical", key)
	}

	want, err := s.materialize(off, nil)
	if err != nil {
		t.Fatalf("%s: materialize: %v", key, err)
	}
	got, err := s.rehydrate(fr, nil)
	if err != nil {
		t.Fatalf("%s: rehydrate: %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: rehydrate %q, materialize %q", key, got, want)
	}
	return frame
}

// TestColdFrameRoundTrip covers every resident value band: an int cell, a small
// and a full-width embedded value, and a separated value whose run sits in the
// arena. Each frames, decodes, and rehydrates to exactly what materialize serves.
func TestColdFrameRoundTrip(t *testing.T) {
	s := New(16<<20, 0)
	cases := []struct {
		key string
		val []byte
	}{
		{"int", []byte("1234567890")},
		{"embed-small", []byte("hello world")},
		{"embed-1k", bytes.Repeat([]byte("x"), strInlineMax)},
		{"sep-arena", bytes.Repeat([]byte("y"), 4096)},
	}
	for _, c := range cases {
		if err := s.Set([]byte(c.key), c.val); err != nil {
			t.Fatalf("Set %s: %v", c.key, err)
		}
	}
	// Confirm the bands are what the test intends, so a band reshuffle cannot
	// quietly turn this into four embedded records.
	st := s.Stats()
	if st.Int != 1 || st.Separated != 1 {
		t.Fatalf("bands: int=%d separated=%d, want 1 and 1", st.Int, st.Separated)
	}
	for _, c := range cases {
		checkFrame(t, s, c.key)
	}
}

// TestColdFrameSeparatedLog frames a separated value whose bytes spilled to the
// value log: the frame carries the log pointer, and rehydrate brings the bytes
// back through one log read, the doubly-cold path doc 06 section 2.3 prices.
func TestColdFrameSeparatedLog(t *testing.T) {
	s, err := Open(Options{
		ArenaBytes:       1 << 20,
		SegBytes:         int(align8(maxRecordBytes)),
		VlogPath:         filepath.Join(t.TempDir(), "values.log"),
		ResidentCapBytes: 1, // every separated value spills
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	val := bytes.Repeat([]byte("z"), 4096)
	if err := s.Set([]byte("k"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	off := addrOf(s, "k")
	word, _, _ := s.readPtr(s.valueStart(off))
	if word&inLogBit == 0 {
		t.Fatal("value did not spill to the log; the cap gate is not engaged")
	}
	checkFrame(t, s, "k")
}

// TestColdFrameSelfDelimiting frames several records back to back into one buffer
// and walks it with no index, the region scan recovery leans on: each total
// prefix delimits the next frame, and the keys come out in append order.
func TestColdFrameSelfDelimiting(t *testing.T) {
	s := New(16<<20, 0)
	keys := []string{"a", "bb", "ccc", "int", "wide"}
	vals := [][]byte{
		[]byte("one"),
		[]byte("two"),
		bytes.Repeat([]byte("q"), 300),
		[]byte("42"),
		bytes.Repeat([]byte("w"), 4096),
	}
	var buf []byte
	for i, k := range keys {
		if err := s.Set([]byte(k), vals[i]); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		buf = s.frameRecord(addrOf(s, k), buf)
	}

	var got []string
	for len(buf) > 0 {
		fr, n, err := decodeColdFrame(buf)
		if err != nil {
			t.Fatalf("walk decode at %d bytes left: %v", len(buf), err)
		}
		got = append(got, string(fr.key))
		buf = buf[n:]
	}
	if len(got) != len(keys) {
		t.Fatalf("walk found %d frames, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("frame %d key %q, want %q", i, got[i], keys[i])
		}
	}
}

// TestColdFrameTornTail pins the recovery guard: a frame shorter than the header,
// a total that runs past the buffer (a torn write), and a total below the header
// all error instead of aliasing past the bytes.
func TestColdFrameTornTail(t *testing.T) {
	s := New(16<<20, 0)
	if err := s.Set([]byte("k"), []byte("value-bytes")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	frame := s.frameRecord(addrOf(s, "k"), nil)

	if _, _, err := decodeColdFrame(frame[:coldHdr-1]); err != errColdShort {
		t.Fatalf("short header: err %v, want errColdShort", err)
	}
	if _, _, err := decodeColdFrame(frame[:len(frame)-1]); err != errColdShort {
		t.Fatalf("truncated frame: err %v, want errColdShort", err)
	}

	bad := append([]byte(nil), frame...)
	binary.LittleEndian.PutUint32(bad[0:], coldHdr-1) // total below the header
	if _, _, err := decodeColdFrame(bad); err != errColdShort {
		t.Fatalf("undersized total: err %v, want errColdShort", err)
	}
}
