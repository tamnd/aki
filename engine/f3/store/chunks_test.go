package store

import (
	"bytes"
	"testing"
)

// These tests pin the chunked band: a value at or past strChunkMin splits
// into 64KiB chunk runs behind an arena-resident directory, reads back intact
// through every path, patches chunk-bounded under APPEND and SETRANGE, spills
// its chunks to the value log under a resident cap, streams through
// ChunkStream, and compacts.

// chunkVal builds a value of n bytes with position-dependent content so a
// chunk served from the wrong offset cannot pass an equality check.
func chunkVal(n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(i*7 + i>>10)
	}
	return v
}

// liveBytes sums the arena's per-segment live counters: the release
// accounting a delete or an overwrite drives. The bump cursors never rewind
// in this milestone, so this is the signal that outside bytes were unlinked.
func liveBytes(s *Store) int64 {
	var n int64
	for i := range s.arena.segs {
		n += s.arena.segs[i].live
	}
	return n
}

// newChunkStore opens a logless store with room for a few chunked values.
func newChunkStore(t testing.TB) *Store {
	t.Helper()
	segSize := int(align8(maxRecordBytes))
	return New(8+40*segSize, segSize)
}

// TestChunkedBandBoundary pins the band edge: one byte under strChunkMin is
// separated, at it chunked, and both read back intact.
func TestChunkedBandBoundary(t *testing.T) {
	s := newChunkStore(t)
	under := chunkVal(strChunkMin - 1)
	at := chunkVal(strChunkMin)
	mustSet(t, s, "under", under)
	mustSet(t, s, "at", at)
	st := s.Stats()
	if st.Separated != 1 || st.Chunked != 1 {
		t.Fatalf("stats = %+v, want one separated and one chunked", st)
	}
	checkGet(t, s, "under", under)
	checkGet(t, s, "at", at)
	if n, ok := s.StrLen([]byte("at"), 0); !ok || n != int64(strChunkMin) {
		t.Fatalf("StrLen = %d, %v", n, ok)
	}
}

// TestChunkedRoundtrip stores values across chunk-count edges, including a
// ragged final chunk, and reads each back.
func TestChunkedRoundtrip(t *testing.T) {
	s := newChunkStore(t)
	sizes := []int{strChunkMin, strChunkMin + 1, 2*strChunkSize - 1, 2 * strChunkSize, 3*strChunkSize + 12345}
	for i, n := range sizes {
		key := string(rune('a' + i))
		v := chunkVal(n)
		mustSet(t, s, key, v)
		checkGet(t, s, key, v)
	}
	if st := s.Stats(); st.Chunked != uint64(len(sizes)) {
		t.Fatalf("stats = %+v, want %d chunked", st, len(sizes))
	}
}

// TestChunkedFullReplaceReselects overwrites a chunked value with a short one
// and back: a full replace re-selects the band from scratch, and each
// transition releases the previous shape's outside bytes.
func TestChunkedFullReplaceReselects(t *testing.T) {
	s := newChunkStore(t)
	big := chunkVal(2*strChunkSize + 100)
	mustSet(t, s, "k", big)
	liveAfterBig := liveBytes(s)
	mustSet(t, s, "k", []byte("small"))
	if st := s.Stats(); st.Chunked != 0 || st.Embedded != 1 {
		t.Fatalf("stats after shrink = %+v, want embedded only", st)
	}
	checkGet(t, s, "k", []byte("small"))
	if live := liveBytes(s); live >= liveAfterBig-int64(len(big)) {
		t.Fatalf("live bytes %d did not drop from %d: chunks not released", live, liveAfterBig)
	}
	mustSet(t, s, "k", big)
	if st := s.Stats(); st.Chunked != 1 || st.Embedded != 0 {
		t.Fatalf("stats after regrow = %+v, want chunked only", st)
	}
	checkGet(t, s, "k", big)
}

// TestChunkedDelete removes a chunked key and checks the census and the
// arena accounting release everything.
func TestChunkedDelete(t *testing.T) {
	s := newChunkStore(t)
	before := liveBytes(s)
	mustSet(t, s, "k", chunkVal(3*strChunkSize+5))
	if !s.Delete([]byte("k")) {
		t.Fatal("Delete returned false")
	}
	if st := s.Stats(); st.Chunked != 0 {
		t.Fatalf("stats = %+v, want empty", st)
	}
	if live := liveBytes(s); live != before {
		t.Fatalf("live bytes = %d, want %d back to baseline", live, before)
	}
}

// TestChunkedAppend grows a chunked value in place: the final ragged chunk
// extends and fresh chunks land, untouched chunks keep their runs, and the
// record never republishes.
func TestChunkedAppend(t *testing.T) {
	s := newChunkStore(t)
	v := chunkVal(strChunkSize + 100)
	mustSet(t, s, "k", v)
	add := chunkVal(strChunkSize)
	n, err := s.Append([]byte("k"), add, 0)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	want := append(append([]byte{}, v...), add...)
	if n != int64(len(want)) {
		t.Fatalf("Append length = %d, want %d", n, len(want))
	}
	checkGet(t, s, "k", want)
	if st := s.Stats(); st.Chunked != 1 {
		t.Fatalf("stats = %+v, want one chunked", st)
	}
	// Many small appends across a chunk edge stay intact.
	for i := 0; i < 100; i++ {
		piece := chunkVal(1000)
		if _, err := s.Append([]byte("k"), piece, 0); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		want = append(want, piece...)
	}
	checkGet(t, s, "k", want)
}

// TestChunkedSetRange patches inside, across, and past the chunks, including
// a zero-filled gap past the old end.
func TestChunkedSetRange(t *testing.T) {
	s := newChunkStore(t)
	v := chunkVal(2*strChunkSize + 500)
	mustSet(t, s, "k", v)
	want := append([]byte{}, v...)

	// Inside one chunk.
	patch := bytes.Repeat([]byte("P"), 100)
	if _, err := s.SetRange([]byte("k"), 1000, patch, 0); err != nil {
		t.Fatalf("SetRange inside: %v", err)
	}
	copy(want[1000:], patch)
	checkGet(t, s, "k", want)

	// Across a chunk edge.
	if _, err := s.SetRange([]byte("k"), strChunkSize-50, patch, 0); err != nil {
		t.Fatalf("SetRange across: %v", err)
	}
	copy(want[strChunkSize-50:], patch)
	checkGet(t, s, "k", want)

	// Past the end with a gap: the fill must read zero even on reused arena
	// bytes, and the value grows a chunk.
	gapAt := len(want) + 200
	if n, err := s.SetRange([]byte("k"), gapAt, patch, 0); err != nil || n != int64(gapAt+len(patch)) {
		t.Fatalf("SetRange gap = %d, %v", n, err)
	}
	want = append(want, make([]byte, 200)...)
	want = append(want, patch...)
	checkGet(t, s, "k", want)
}

// TestChunkedTransitions crosses into the chunked band from each smaller
// shape by APPEND and by SETRANGE.
func TestChunkedTransitions(t *testing.T) {
	s := newChunkStore(t)

	// Embedded to chunked by APPEND.
	mustSet(t, s, "a", []byte("head-"))
	add := chunkVal(strChunkMin)
	wantA := append([]byte("head-"), add...)
	if _, err := s.Append([]byte("a"), add, 0); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	checkGet(t, s, "a", wantA)

	// Separated to chunked by APPEND.
	sep := chunkVal(strChunkMin - 10)
	mustSet(t, s, "b", sep)
	wantB := append(append([]byte{}, sep...), []byte("tailtailtail")...)
	if _, err := s.Append([]byte("b"), []byte("tailtailtail"), 0); err != nil {
		t.Fatalf("Append b: %v", err)
	}
	checkGet(t, s, "b", wantB)

	// Separated to chunked by SETRANGE past the threshold.
	mustSet(t, s, "c", sep)
	patch := bytes.Repeat([]byte("Q"), 64)
	if _, err := s.SetRange([]byte("c"), strChunkSize+100, patch, 0); err != nil {
		t.Fatalf("SetRange c: %v", err)
	}
	wantC := append([]byte{}, sep...)
	wantC = append(wantC, make([]byte, strChunkSize+100-len(sep))...)
	wantC = append(wantC, patch...)
	checkGet(t, s, "c", wantC)

	// Create-on-miss straight into the band.
	if _, err := s.Append([]byte("d"), add, 0); err != nil {
		t.Fatalf("Append d: %v", err)
	}
	checkGet(t, s, "d", add)

	if st := s.Stats(); st.Chunked != 4 {
		t.Fatalf("stats = %+v, want 4 chunked", st)
	}
}

// TestChunkedTTLCarries checks a deadline rides the band transition and the
// lazy reap releases every chunk.
func TestChunkedTTLCarries(t *testing.T) {
	s := newChunkStore(t)
	if err := s.SetString([]byte("k"), []byte("seed"), 1000, 5000, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	if _, err := s.Append([]byte("k"), chunkVal(strChunkMin), 1000); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if !s.Exists([]byte("k"), 4000) {
		t.Fatal("key absent before its deadline")
	}
	before := liveBytes(s)
	if s.Exists([]byte("k"), 6000) {
		t.Fatal("key present past its deadline")
	}
	if live := liveBytes(s); live >= before {
		t.Fatalf("live bytes = %d, want below %d after the reap", live, before)
	}
	if st := s.Stats(); st.Chunked != 0 {
		t.Fatalf("stats after reap = %+v, want empty", st)
	}
}

// TestChunkedSpillsToLog runs the band under a one-byte resident cap: every
// chunk lands in the value log, the run census counts per chunk, and
// overwrites feed the dead ledger.
func TestChunkedSpillsToLog(t *testing.T) {
	s := newLogStore(t, 1<<22)
	n := 2*strChunkSize + 300
	v := chunkVal(n)
	mustSet(t, s, "k", v)
	st := s.Stats()
	if st.Chunked != 1 || st.LogRuns != 3 {
		t.Fatalf("stats = %+v, want one chunked over three log runs", st)
	}
	if total, dead := s.LogBytes(); total != uint64(n) || dead != 0 {
		t.Fatalf("LogBytes = %d/%d, want %d/0", total, dead, n)
	}
	checkGet(t, s, "k", v)
	// A log chunk is immutable: a patch inside one rewrites that chunk only.
	patch := bytes.Repeat([]byte("Z"), 10)
	if _, err := s.SetRange([]byte("k"), 5, patch, 0); err != nil {
		t.Fatalf("SetRange: %v", err)
	}
	copy(v[5:], patch)
	checkGet(t, s, "k", v)
	if _, dead := s.LogBytes(); dead != strChunkSize {
		t.Fatalf("dead after one-chunk patch = %d, want %d", dead, strChunkSize)
	}
	// Delete releases every chunk into the dead ledger.
	if !s.Delete([]byte("k")) {
		t.Fatal("Delete returned false")
	}
	if _, dead := s.LogBytes(); dead != uint64(n)+strChunkSize {
		t.Fatalf("dead after delete = %d, want %d", dead, uint64(n)+strChunkSize)
	}
	if st := s.Stats(); st.Chunked != 0 || st.LogRuns != 0 {
		t.Fatalf("stats after delete = %+v, want empty", st)
	}
}

// TestCompactChunkedLog fills the log with chunked values, kills some, and
// checks CompactLog drops exactly the dead chunks while every live chunked
// value still reads back.
func TestCompactChunkedLog(t *testing.T) {
	s := newLogStore(t, 1<<22)
	keep := chunkVal(2*strChunkSize + 40)
	drop := chunkVal(strChunkSize + 7)
	mustSet(t, s, "keep", keep)
	mustSet(t, s, "drop", drop)
	if !s.Delete([]byte("drop")) {
		t.Fatal("Delete returned false")
	}
	if err := s.CompactLog(); err != nil {
		t.Fatalf("CompactLog: %v", err)
	}
	total, dead := s.LogBytes()
	if dead != 0 || total != uint64(len(keep)) {
		t.Fatalf("LogBytes after compact = %d/%d, want %d/0", total, dead, len(keep))
	}
	checkGet(t, s, "keep", keep)
	// The compacted chunks stay writable and readable.
	if _, err := s.Append([]byte("keep"), []byte("-after"), 0); err != nil {
		t.Fatalf("Append after compact: %v", err)
	}
	checkGet(t, s, "keep", append(append([]byte{}, keep...), []byte("-after")...))
}

// TestChunkStream reads a chunked value through the streaming source and
// checks the chunk sequence reassembles the exact value with every chunk at
// or under ChunkSize.
func TestChunkStream(t *testing.T) {
	s := newChunkStore(t)
	v := chunkVal(3*strChunkSize + 999)
	mustSet(t, s, "k", v)
	got, cs, ok := s.GetStream([]byte("k"), 0, nil)
	if !ok || cs == nil || len(got) != 0 {
		t.Fatalf("GetStream = %d bytes, %v, ok %v; want a stream", len(got), cs, ok)
	}
	if cs.Total() != int64(len(v)) {
		t.Fatalf("Total = %d, want %d", cs.Total(), len(v))
	}
	buf := make([]byte, ChunkSize)
	var out []byte
	for {
		n, err := cs.Next(buf)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if n == 0 {
			break
		}
		if n > ChunkSize {
			t.Fatalf("chunk of %d bytes over ChunkSize", n)
		}
		out = append(out, buf[:n]...)
	}
	if !bytes.Equal(out, v) {
		t.Fatalf("stream reassembled %d bytes, want %d", len(out), len(v))
	}
	// Point bands come back materialized with no stream.
	mustSet(t, s, "small", []byte("small"))
	got, cs, ok = s.GetStream([]byte("small"), 0, nil)
	if !ok || cs != nil || string(got) != "small" {
		t.Fatalf("GetStream small = %q, %v, %v", got, cs, ok)
	}
}

// TestChunkStreamFromLog streams a value whose chunks sit in the value log.
func TestChunkStreamFromLog(t *testing.T) {
	s := newLogStore(t, 1<<22)
	v := chunkVal(2*strChunkSize + 11)
	mustSet(t, s, "k", v)
	if st := s.Stats(); st.LogRuns != 3 {
		t.Fatalf("stats = %+v, want the chunks in the log", st)
	}
	_, cs, ok := s.GetStream([]byte("k"), 0, nil)
	if !ok || cs == nil {
		t.Fatal("GetStream did not return a stream")
	}
	buf := make([]byte, ChunkSize)
	var out []byte
	for {
		n, err := cs.Next(buf)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if n == 0 {
			break
		}
		out = append(out, buf[:n]...)
	}
	if !bytes.Equal(out, v) {
		t.Fatalf("stream reassembled %d bytes, want %d", len(out), len(v))
	}
}
