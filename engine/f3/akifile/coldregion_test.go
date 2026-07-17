package akifile

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// coldBlob builds a self-delimiting cold frame the way the store's coldframe
// codec does: a leading u32 total covering the whole frame, then the body. The
// cold region treats everything past the total as opaque, so a stand-in body is
// enough to pin the region's framing.
func coldBlob(body []byte) []byte {
	total := 4 + len(body)
	b := make([]byte, total)
	binary.LittleEndian.PutUint32(b, uint32(total))
	copy(b[4:], body)
	return b
}

// TestAppendColdFramesRoundTrip appends a batch of cold frames and reads each one
// back whole by its offset, the demote-then-cold-read the region exists for.
func TestAppendColdFramesRoundTrip(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	frames := [][]byte{
		coldBlob([]byte("alpha")),
		coldBlob(bytes.Repeat([]byte("y"), 4000)),
		coldBlob([]byte("last")),
	}
	offs, err := f.AppendColdFrames(0, 1, frames)
	if err != nil {
		t.Fatalf("append cold frames: %v", err)
	}
	if len(offs) != len(frames) {
		t.Fatalf("got %d offsets, want %d", len(offs), len(frames))
	}
	for i, off := range offs {
		got, err := f.ReadColdFrame(off, nil)
		if err != nil {
			t.Fatalf("read cold frame %d: %v", i, err)
		}
		if !bytes.Equal(got, frames[i]) {
			t.Fatalf("frame %d = %q, want %q", i, got, frames[i])
		}
	}
}

// TestAppendColdFramesShareOneSegment proves a batch packs into a single
// cold_chunk segment: the cursor advances by exactly one segment span and every
// offset lands inside it.
func TestAppendColdFramesShareOneSegment(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	start := f.Cursor()
	frames := [][]byte{coldBlob([]byte("one")), coldBlob([]byte("two")), coldBlob([]byte("three"))}
	offs, err := f.AppendColdFrames(0, 1, frames)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if span := f.Cursor() - start; span != SegmentAlign {
		t.Fatalf("batch spanned %d bytes, want one segment of %d", span, SegmentAlign)
	}
	for i, off := range offs {
		if off < start+SegHeaderLen || off >= f.Cursor() {
			t.Fatalf("offset %d = %d, outside the segment [%d,%d)", i, off, start+SegHeaderLen, f.Cursor())
		}
	}
}

// TestAppendColdFramesEmptyBatch writes no segment for an empty batch.
func TestAppendColdFramesEmptyBatch(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	start := f.Cursor()
	offs, err := f.AppendColdFrames(0, 1, nil)
	if err != nil || offs != nil {
		t.Fatalf("empty batch = %v/%v, want nil/nil", offs, err)
	}
	if f.Cursor() != start {
		t.Fatalf("cursor moved to %d on an empty batch", f.Cursor())
	}
}

// TestWalkColdFramesEnumeratesEveryFrame walks two cold_chunk segments and sees
// every frame in order at the offset a point read would use.
func TestWalkColdFramesEnumeratesEveryFrame(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	first := [][]byte{coldBlob([]byte("a")), coldBlob(bytes.Repeat([]byte("z"), 3000)), coldBlob([]byte("bb"))}
	second := [][]byte{coldBlob([]byte("ccc")), coldBlob([]byte("d"))}
	if _, err := f.AppendColdFrames(0, 1, first); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if _, err := f.AppendColdFrames(0, 2, second); err != nil {
		t.Fatalf("second append: %v", err)
	}

	want := append(append([][]byte{}, first...), second...)
	var got [][]byte
	err := f.WalkColdFrames(PageSize, func(off uint64, frame []byte) error {
		// The walk's offset must be the one a point read resolves.
		pr, err := f.ReadColdFrame(off, nil)
		if err != nil {
			return err
		}
		if !bytes.Equal(pr, frame) {
			t.Fatalf("point read at %d disagrees with the walk", off)
		}
		got = append(got, append([]byte{}, frame...))
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("walked %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("frame %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestWalkColdFramesSkipsOtherKinds confirms the walk descends into cold_chunk
// segments only, stepping over a value-log and a plain log segment between them.
func TestWalkColdFramesSkipsOtherKinds(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	if _, err := f.AppendColdFrames(0, 1, [][]byte{coldBlob([]byte("before"))}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if _, err := f.AppendValues(0, 2, [][]byte{[]byte("a value")}); err != nil {
		t.Fatalf("value append: %v", err)
	}
	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 3, Payload: []byte("a log record")}}); err != nil {
		t.Fatalf("log append: %v", err)
	}
	if _, err := f.AppendColdFrames(0, 4, [][]byte{coldBlob([]byte("after"))}); err != nil {
		t.Fatalf("second append: %v", err)
	}

	var got []string
	err := f.WalkColdFrames(PageSize, func(off uint64, frame []byte) error {
		got = append(got, string(frame[4:]))
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(got) != 2 || got[0] != "before" || got[1] != "after" {
		t.Fatalf("walked cold bodies %v, want [before after]", got)
	}
}
