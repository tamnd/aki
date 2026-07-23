package store

import (
	"bytes"
	"testing"
)

// TestWalkStagedFrames walks a hand-built mixed stream: an embedded string
// record, an int-cell record, a separated-band pointer record, and a packed
// collection chunk, asserting the dispatch and classification the folder
// keys on.
func TestWalkStagedFrames(t *testing.T) {
	var buf []byte
	buf = appendColdFrame(buf, kindString, 0, 5, []byte("emb"), []byte("hello"), 0)
	cell := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	buf = appendColdFrame(buf, kindString, flagInt, 3, []byte("int"), cell, 0)
	ptr := bytes.Repeat([]byte{0xAB}, ptrSize)
	buf = appendColdFrame(buf, kindString, flagSep, 4096, []byte("sep"), ptr, 0)
	buf = appendChunkFrame(buf, 0x03|frameChunk, 0, 7, []byte("coll"), []byte("disc8bYt"), []byte("packed-blob"))

	var got []FoldFrame
	if err := WalkStagedFrames(buf, func(f FoldFrame) error {
		got = append(got, f)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("walked %d frames, want 4", len(got))
	}
	if got[0].Chunk || got[0].Pointer || string(got[0].Key) != "emb" || string(got[0].Payload) != "hello" || got[0].Count != 1 {
		t.Fatalf("embedded frame misread: %+v", got[0])
	}
	if got[1].Pointer || !bytes.Equal(got[1].Payload, cell) {
		t.Fatalf("int frame misread: %+v", got[1])
	}
	if !got[2].Pointer || got[2].Chunk || !bytes.Equal(got[2].Payload, ptr) {
		t.Fatalf("separated frame not classified as pointer: %+v", got[2])
	}
	c := got[3]
	if !c.Chunk || c.Kind != 0x03|ChunkKindBit || c.Count != 7 ||
		string(c.Key) != "coll" || string(c.Disc) != "disc8bYt" || string(c.Payload) != "packed-blob" {
		t.Fatalf("chunk frame misread: %+v", c)
	}
	if !bytes.Equal(c.Frame, buf[len(buf)-len(c.Frame):]) {
		t.Fatal("chunk Frame does not alias its whole frame")
	}
}

// TestWalkStagedFramesTorn truncates a frame mid-body: the walk must stop
// with the codec's error rather than misparse.
func TestWalkStagedFramesTorn(t *testing.T) {
	buf := appendColdFrame(nil, kindString, 0, 5, []byte("key"), []byte("value"), 0)
	if err := WalkStagedFrames(buf[:len(buf)-2], func(FoldFrame) error { return nil }); err == nil {
		t.Fatal("torn record frame walked clean")
	}
	buf = appendChunkFrame(nil, 0x03|frameChunk, 0, 1, []byte("k"), []byte("d"), []byte("p"))
	if err := WalkStagedFrames(buf[:len(buf)-1], func(FoldFrame) error { return nil }); err == nil {
		t.Fatal("torn chunk frame walked clean")
	}
}

// TestFoldBuildersRoundTrip pins the exported builders to the walker: a
// run chunk whose payload is two record frames must decode back through
// the same two-level walk the folder's tests and the read path use.
func TestFoldBuildersRoundTrip(t *testing.T) {
	var payload []byte
	payload = AppendRecordFrame(payload, kindString, 0, 1, []byte("a"), []byte("1"), 0)
	payload = AppendRecordFrame(payload, kindString, 0, 2, []byte("b"), []byte("22"), 0)
	chunk := AppendRunChunk(nil, kindString|ChunkKindBit, 0, 2, []byte("a"), []byte("fingerpr"), payload)

	var outer []FoldFrame
	if err := WalkStagedFrames(chunk, func(f FoldFrame) error {
		outer = append(outer, f)
		return nil
	}); err != nil {
		t.Fatalf("outer walk: %v", err)
	}
	if len(outer) != 1 || !outer[0].Chunk || outer[0].Count != 2 {
		t.Fatalf("run chunk misread: %+v", outer)
	}
	var keys []string
	if err := WalkStagedFrames(outer[0].Payload, func(f FoldFrame) error {
		keys = append(keys, string(f.Key))
		return nil
	}); err != nil {
		t.Fatalf("inner walk: %v", err)
	}
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("run payload keys = %v", keys)
	}
}

// TestTombstoneFrames pins the delete claim's shape: a tombstone frame
// walks with the Tombstone mark, an empty value region, and its own kind,
// a run chunk packed from tombstones carries the mark too, and ordinary
// record frames never do.
func TestTombstoneFrames(t *testing.T) {
	frame := AppendTombstoneFrame(nil, []byte("gone"))
	var got []FoldFrame
	if err := WalkStagedFrames(frame, func(f FoldFrame) error {
		got = append(got, f)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("walked %d frames, want 1", len(got))
	}
	f := got[0]
	if !f.Tombstone || f.Chunk || f.Pointer || f.Kind != KindTombstone ||
		f.Count != 1 || string(f.Key) != "gone" || len(f.Payload) != 0 {
		t.Fatalf("tombstone frame misread: %+v", f)
	}

	chunk := AppendRunChunk(nil, KindTombstone|ChunkKindBit, 0, 1, []byte("gone"), []byte("fingerpr"), frame)
	var outer FoldFrame
	if err := WalkStagedFrames(chunk, func(f FoldFrame) error {
		outer = f
		return nil
	}); err != nil {
		t.Fatalf("chunk walk: %v", err)
	}
	if !outer.Tombstone || !outer.Chunk || outer.Kind != KindTombstone|ChunkKindBit {
		t.Fatalf("tombstone run chunk misread: %+v", outer)
	}

	rec := AppendRecordFrame(nil, kindString, 0, 1, []byte("k"), []byte("v"), 0)
	if err := WalkStagedFrames(rec, func(f FoldFrame) error {
		if f.Tombstone {
			t.Fatalf("record frame classified as tombstone: %+v", f)
		}
		return nil
	}); err != nil {
		t.Fatalf("record walk: %v", err)
	}
}

// TestFoldTapSeesStagedFrames pins the tap's contract on a real store: it
// fires once per staged drain, before the pwrite, with exactly the staged
// bytes, and every record the drain will flip appears in the walk. The
// drain then completes normally, so the tap costs the migrator nothing.
func TestFoldTapSeesStagedFrames(t *testing.T) {
	const cap = 1 << 20
	s := migratorStore(t, cap)
	fillSmall(t, s, 30000)
	if !s.NeedsColdDrain() {
		t.Fatal("fixture did not cross the cap")
	}

	var tapped [][]byte
	s.SetFoldTap(func(frames []byte) {
		tapped = append(tapped, append([]byte(nil), frames...))
	})
	d := driveStage(t, s)
	if len(tapped) != 1 {
		t.Fatalf("tap fired %d times, want 1", len(tapped))
	}
	if !bytes.Equal(tapped[0], d.buf) {
		t.Fatal("tap bytes differ from the staged buffer")
	}
	walked := make(map[string]bool)
	if err := WalkStagedFrames(tapped[0], func(f FoldFrame) error {
		if f.Chunk {
			t.Fatal("whole-record drain staged a chunk frame")
		}
		walked[string(f.Key)] = true
		return nil
	}); err != nil {
		t.Fatalf("walk tapped buffer: %v", err)
	}
	for j := range d.flips {
		if !walked[string(flipKey(d, j))] {
			t.Fatalf("flip %d's key missing from the tapped walk", j)
		}
	}
	if _, err := s.ColdWriteAt(d.Off(), d.Buf()); err != nil {
		t.Fatalf("cold write: %v", err)
	}
	s.CompleteColdDrain(d, true)
}
