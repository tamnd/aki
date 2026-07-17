package akifile

import (
	"bytes"
	"errors"
	"testing"
)

// TestAppendValuesRoundTrip appends a batch of values and reads each one back
// through its pointer, the write-then-point-read the value log exists for.
func TestAppendValuesRoundTrip(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	vals := [][]byte{
		[]byte("small"),
		bytes.Repeat([]byte("y"), 4000),
		[]byte("last"),
	}
	ptrs, err := f.AppendValues(0, 1, vals)
	if err != nil {
		t.Fatalf("append values: %v", err)
	}
	if len(ptrs) != len(vals) {
		t.Fatalf("got %d pointers, want %d", len(ptrs), len(vals))
	}
	for i, p := range ptrs {
		got, err := f.ReadValueAt(p, nil)
		if err != nil {
			t.Fatalf("read value %d: %v", i, err)
		}
		if !bytes.Equal(got, vals[i]) {
			t.Fatalf("value %d = %q, want %q", i, got, vals[i])
		}
	}
}

// TestAppendValuesShareOneSegment proves the batch packs into a single
// value_log segment: the cursor advances by exactly one segment span, and every
// pointer lands inside that segment's payload.
func TestAppendValuesShareOneSegment(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	start := f.Cursor()
	vals := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}
	ptrs, err := f.AppendValues(2, 7, vals)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// One segment: header plus the framed run, padded to the 4KiB boundary.
	if span := f.Cursor() - start; span != SegmentAlign {
		t.Fatalf("cursor advanced %d, want one segment span %d", span, SegmentAlign)
	}
	base := start + SegHeaderLen
	end := f.Cursor()
	for i, p := range ptrs {
		if p.ValueOff < base || p.ValueOff+uint64(p.ValueLen) > end {
			t.Fatalf("pointer %d off %d not inside the segment [%d,%d)", i, p.ValueOff, base, end)
		}
	}

	// The segment reads back as a value_log kind whose payload walks to the
	// same values, which is what recovery will do.
	h, payload, err := f.ReadSegmentAt(start)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if h.Kind != KindValueLog {
		t.Fatalf("segment kind = %d, want KindValueLog", h.Kind)
	}
	off := uint64(0)
	for i, want := range vals {
		vf, val, next, err := NextValueFrame(payload, off)
		if err != nil {
			t.Fatalf("walk frame %d: %v", i, err)
		}
		if !bytes.Equal(val, want) {
			t.Fatalf("walk value %d = %q, want %q", i, val, want)
		}
		if base+vf.ValueOff != ptrs[i].ValueOff {
			t.Fatalf("walk value %d file off = %d, pointer says %d", i, base+vf.ValueOff, ptrs[i].ValueOff)
		}
		off = next
	}
}

// TestAppendValuesEmptyWritesNothing leaves the file untouched for an empty
// batch: no wasted segment, no cursor move.
func TestAppendValuesEmptyWritesNothing(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	start := f.Cursor()
	ptrs, err := f.AppendValues(0, 1, nil)
	if err != nil || ptrs != nil {
		t.Fatalf("empty append = %v/%v, want nil/nil", ptrs, err)
	}
	if f.Cursor() != start {
		t.Fatalf("cursor moved to %d on an empty batch", f.Cursor())
	}
}

// TestReadValueAtCatchesTornBlob flips a byte in the value bytes on the device
// and confirms the pointer CRC catches it on read.
func TestReadValueAtCatchesTornBlob(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	ptrs, err := f.AppendValues(0, 1, [][]byte{[]byte("integrity-checked")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	dev.buf[ptrs[0].ValueOff+2] ^= 0xff
	if _, err := f.ReadValueAt(ptrs[0], nil); err != ErrChecksum {
		t.Fatalf("torn read err = %v, want ErrChecksum", err)
	}
}

// TestReadValueFrameAtResolvesBarePointer reads each value back from only its
// offset and length, the shape the value log's re-home hands the store: no stored
// CRC, the frame's own trailing sum is the guard.
func TestReadValueFrameAtResolvesBarePointer(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	vals := [][]byte{[]byte("a"), bytes.Repeat([]byte("z"), 3000), []byte("tail")}
	ptrs, err := f.AppendValues(0, 1, vals)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	var buf []byte
	for i, p := range ptrs {
		got, err := f.ReadValueFrameAt(p.ValueOff, p.ValueLen, buf)
		if err != nil {
			t.Fatalf("read value %d: %v", i, err)
		}
		if !bytes.Equal(got, vals[i]) {
			t.Fatalf("value %d = %q, want %q", i, got, vals[i])
		}
		buf = got[:0]
	}
}

// TestReadValueFrameAtCatchesTornBlob tears a value byte and confirms the frame's
// trailing CRC catches it, so a bare (off, len) pointer fails closed like a full
// ValuePointer would.
func TestReadValueFrameAtCatchesTornBlob(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	ptrs, err := f.AppendValues(0, 1, [][]byte{[]byte("integrity-checked")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	dev.buf[ptrs[0].ValueOff+2] ^= 0xff
	if _, err := f.ReadValueFrameAt(ptrs[0].ValueOff, ptrs[0].ValueLen, nil); err != ErrChecksum {
		t.Fatalf("torn read err = %v, want ErrChecksum", err)
	}
}

// TestCompactValuesRehomesLiveSubset appends a batch, then re-homes only the
// live subset into a fresh segment past the old one and confirms every new
// pointer resolves to its value, the reclaim the store's value-log compaction
// drives.
func TestCompactValuesRehomesLiveSubset(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	vals := [][]byte{[]byte("keep-a"), []byte("dead-1"), bytes.Repeat([]byte("k"), 3000), []byte("dead-2"), []byte("keep-b")}
	ptrs, err := f.AppendValues(0, 1, vals)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Indices 1 and 3 are dead; the rest are live and get re-homed.
	liveIdx := []int{0, 2, 4}
	live := make([]ValuePointer, len(liveIdx))
	for i, j := range liveIdx {
		live[i] = ptrs[j]
	}

	start := f.Cursor()
	got, err := f.CompactValues(0, 2, live)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if len(got) != len(live) {
		t.Fatalf("got %d pointers, want %d", len(got), len(live))
	}
	// The rewrite lands past every original value.
	for i, p := range got {
		if p.ValueOff < start {
			t.Fatalf("re-homed pointer %d off %d not past the old region start %d", i, p.ValueOff, start)
		}
		v, err := f.ReadValueAt(p, nil)
		if err != nil {
			t.Fatalf("read re-homed %d: %v", i, err)
		}
		if !bytes.Equal(v, vals[liveIdx[i]]) {
			t.Fatalf("re-homed %d = %q, want %q", i, v, vals[liveIdx[i]])
		}
	}
}

// TestCompactValuesEmptyWritesNothing leaves the file untouched for an empty
// live set: a segment with nothing live reclaims to nothing.
func TestCompactValuesEmptyWritesNothing(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	start := f.Cursor()
	got, err := f.CompactValues(0, 1, nil)
	if err != nil || got != nil {
		t.Fatalf("empty compact = %v/%v, want nil/nil", got, err)
	}
	if f.Cursor() != start {
		t.Fatalf("cursor moved to %d on an empty compact", f.Cursor())
	}
}

// TestCompactValuesFailsClosedOnTornSource tears a source value and confirms the
// compaction refuses rather than migrating rot into the fresh segment.
func TestCompactValuesFailsClosedOnTornSource(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	ptrs, err := f.AppendValues(0, 1, [][]byte{[]byte("intact"), []byte("will-tear")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	dev.buf[ptrs[1].ValueOff+1] ^= 0xff
	if _, err := f.CompactValues(0, 2, ptrs); err != ErrChecksum {
		t.Fatalf("compact of a torn source = %v, want ErrChecksum", err)
	}
}

// TestWalkValuesEnumeratesEveryFrame appends two value_log batches and confirms
// the walk yields every value in file order, each pointer resolving to its bytes:
// the candidate set the store's compaction maps against its index.
func TestWalkValuesEnumeratesEveryFrame(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	first := [][]byte{[]byte("a"), bytes.Repeat([]byte("z"), 3000), []byte("bb")}
	second := [][]byte{[]byte("ccc"), []byte("d")}
	if _, err := f.AppendValues(0, 1, first); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if _, err := f.AppendValues(0, 2, second); err != nil {
		t.Fatalf("second append: %v", err)
	}

	want := append(append([][]byte{}, first...), second...)
	var got [][]byte
	err := f.WalkValues(PageSize, func(p ValuePointer) error {
		v, err := f.ReadValueAt(p, nil)
		if err != nil {
			return err
		}
		got = append(got, append([]byte{}, v...))
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("walked %d values, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("value %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestWalkValuesSkipsOtherKinds interleaves a log segment between two value
// batches and confirms the walk descends into value_log segments only, so the
// log payload never reaches the frame parser.
func TestWalkValuesSkipsOtherKinds(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	if _, err := f.AppendValues(0, 1, [][]byte{[]byte("before")}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 2, Payload: []byte("not a value frame")}}); err != nil {
		t.Fatalf("log append: %v", err)
	}
	if _, err := f.AppendValues(0, 3, [][]byte{[]byte("after")}); err != nil {
		t.Fatalf("second append: %v", err)
	}

	var got []string
	if err := f.WalkValues(PageSize, func(p ValuePointer) error {
		v, err := f.ReadValueAt(p, nil)
		if err != nil {
			return err
		}
		got = append(got, string(v))
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(got) != 2 || got[0] != "before" || got[1] != "after" {
		t.Fatalf("walked %v, want [before after]", got)
	}
}

// TestWalkValuesRespectsFrom starts the walk past the first segment and confirms
// only the later batch's values are visited: the store resumes a partial scan
// from a known offset without rewalking what it already accounted.
func TestWalkValuesRespectsFrom(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	if _, err := f.AppendValues(0, 1, [][]byte{[]byte("early")}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	from := f.Cursor()
	if _, err := f.AppendValues(0, 2, [][]byte{[]byte("late")}); err != nil {
		t.Fatalf("second append: %v", err)
	}

	var got []string
	if err := f.WalkValues(from, func(p ValuePointer) error {
		v, err := f.ReadValueAt(p, nil)
		if err != nil {
			return err
		}
		got = append(got, string(v))
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(got) != 1 || got[0] != "late" {
		t.Fatalf("walked %v, want [late]", got)
	}
}

// TestWalkValuesPropagatesVisitError confirms a visit error stops the walk and
// surfaces unchanged, so a caller can abort the scan on the first live-set
// decision it cannot make.
func TestWalkValuesPropagatesVisitError(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	if _, err := f.AppendValues(0, 1, [][]byte{[]byte("a"), []byte("b"), []byte("c")}); err != nil {
		t.Fatalf("append: %v", err)
	}

	stop := errors.New("stop")
	seen := 0
	err := f.WalkValues(PageSize, func(ValuePointer) error {
		seen++
		if seen == 2 {
			return stop
		}
		return nil
	})
	if err != stop {
		t.Fatalf("walk err = %v, want stop", err)
	}
	if seen != 2 {
		t.Fatalf("visited %d values before the stop, want 2", seen)
	}
}

// TestWalkValuesEmptyRegionVisitsNothing walks a file with no value_log segments
// and confirms the walk completes without a visit.
func TestWalkValuesEmptyRegionVisitsNothing(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("log only")}}); err != nil {
		t.Fatalf("log append: %v", err)
	}
	visited := false
	if err := f.WalkValues(PageSize, func(ValuePointer) error {
		visited = true
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if visited {
		t.Fatalf("walk visited a value in a file with no value_log segments")
	}
}

// TestAppendValuesSurvivesReopen writes values, reopens the file so the cursor
// bootstraps from a tail scan, appends more, and reads pointers from both eras:
// the value_log segments are real segments a scan resumes past.
func TestAppendValuesSurvivesReopen(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncAlways, nil)

	first, err := f.AppendValues(0, 1, [][]byte{[]byte("before-reopen")})
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	g, err := OpenOnDevice(dev, OpenOptions{Sync: SyncAlways})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	second, err := g.AppendValues(1, 1, [][]byte{[]byte("after-reopen")})
	if err != nil {
		t.Fatalf("second append: %v", err)
	}
	if second[0].ValueOff <= first[0].ValueOff {
		t.Fatalf("reopened append at %d did not advance past %d", second[0].ValueOff, first[0].ValueOff)
	}
	for name, p := range map[string]ValuePointer{"before-reopen": first[0], "after-reopen": second[0]} {
		got, err := g.ReadValueAt(p, nil)
		if err != nil || string(got) != name {
			t.Fatalf("read %q = %q/%v", name, got, err)
		}
	}
}
