package akifile

import (
	"bytes"
	"errors"
	"testing"
)

// sampleRow builds a record row with a distinguishable key and fields, so a
// round trip catches a swapped or truncated field.
func sampleRow(key string, word uint64) RecordRow {
	return RecordRow{
		Flags:     0,
		ValueWord: word,
		ValueLen:  uint32(len(key)) + 100,
		ExpireAt:  word + 7,
		Key:       []byte(key),
	}
}

func rowsEqual(a, b RecordRow) bool {
	return a.Flags == b.Flags && a.ValueWord == b.ValueWord && a.ValueLen == b.ValueLen &&
		a.ExpireAt == b.ExpireAt && bytes.Equal(a.Key, b.Key)
}

// TestRecordFrameRoundTrip frames a row and reads it back both ways: ParseRecordBody
// off the frame's body span, and NextRecordFrame stepping the payload. Both must
// return every field intact, and the step offset must land on the next frame.
func TestRecordFrameRoundTrip(t *testing.T) {
	rows := []RecordRow{
		sampleRow("alpha", 1<<63|42),
		{Flags: RecFlagTombstone, Key: []byte("gone")}, // a delete row, empty value
		sampleRow("", 9),                               // an empty key is legal
		sampleRow("a-longer-key-here", 0xdeadbeef),
	}
	var payload []byte
	frames := make([]RecordFrame, len(rows))
	for i, r := range rows {
		payload, frames[i] = AppendRecordFrame(payload, r)
	}

	// Body-span decode at each frame.
	for i, fr := range frames {
		body := payload[fr.BodyOff : fr.BodyOff+uint64(fr.BodyLen)]
		got, err := ParseRecordBody(body)
		if err != nil {
			t.Fatalf("parse body %d: %v", i, err)
		}
		if !rowsEqual(got, rows[i]) {
			t.Fatalf("body %d = %+v, want %+v", i, got, rows[i])
		}
	}

	// Linear walk across the whole payload.
	cur := uint64(0)
	for i := range rows {
		fr, got, next, err := NextRecordFrame(payload, cur)
		if err != nil {
			t.Fatalf("next frame %d: %v", i, err)
		}
		if fr.FrameOff != cur {
			t.Fatalf("frame %d off = %d, want %d", i, fr.FrameOff, cur)
		}
		if !rowsEqual(got, rows[i]) {
			t.Fatalf("walk %d = %+v, want %+v", i, got, rows[i])
		}
		cur = next
	}
	if cur != uint64(len(payload)) {
		t.Fatalf("walk ended at %d, want payload end %d", cur, len(payload))
	}
}

// TestNextRecordFrameRejectsTorn corrupts a body byte and confirms the walk stops
// with ErrChecksum rather than handing back a rotted row, the torn-tail cut.
func TestNextRecordFrameRejectsTorn(t *testing.T) {
	payload, fr := AppendRecordFrame(nil, sampleRow("torn", 5))
	payload[fr.BodyOff+4] ^= 0xff // flip a value-word byte, past the varint
	if _, _, _, err := NextRecordFrame(payload, 0); err != ErrChecksum {
		t.Fatalf("torn frame err = %v, want ErrChecksum", err)
	}
}

// TestNextRecordFrameRejectsShortBody rejects a framed body length below the fixed
// header, a corrupt varint that would otherwise index past the payload.
func TestNextRecordFrameRejectsShortBody(t *testing.T) {
	// A body length of 3 (under recRowHdr) with a byte of payload.
	payload := append([]byte{0x03}, []byte{1, 2, 3, 0, 0, 0, 0}...)
	if _, _, _, err := NextRecordFrame(payload, 0); err != ErrShort {
		t.Fatalf("short body err = %v, want ErrShort", err)
	}
}

// TestRecordLogWriterCoalescesBatch stages several records and flushes once: the
// whole batch lands in a single log segment, Flush returns an address per record
// in stage order, and a WalkRecords over the file reads every row back at exactly
// those addresses. This ties the writer to the enumerator, the two halves the
// store's durable append and recovery lean on.
func TestRecordLogWriterCoalescesBatch(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewRecordLogWriter(f, 3)

	rows := []RecordRow{
		sampleRow("k0", 100),
		sampleRow("k1", 200),
		{Flags: RecFlagTombstone, Key: []byte("k2")},
	}
	for i, r := range rows {
		if got := w.Stage(r); got != i {
			t.Fatalf("stage %d returned index %d", i, got)
		}
	}
	if w.Staged() != len(rows) {
		t.Fatalf("staged = %d, want %d", w.Staged(), len(rows))
	}

	start := f.Cursor()
	addrs, err := w.Flush(9)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if span := f.Cursor() - start; span != SegmentAlign {
		t.Fatalf("flush spanned %d, want one segment %d", span, SegmentAlign)
	}
	if len(addrs) != len(rows) {
		t.Fatalf("got %d addresses, want %d", len(addrs), len(rows))
	}
	if w.Staged() != 0 || w.PendingBytes() != 0 {
		t.Fatalf("accumulator not reset after flush: staged %d pending %d", w.Staged(), w.PendingBytes())
	}

	var walkAddrs []uint64
	var walkRows []RecordRow
	if err := f.WalkRecords(PageSize, func(addr uint64, row RecordRow) error {
		walkAddrs = append(walkAddrs, addr)
		walkRows = append(walkRows, RecordRow{
			Flags: row.Flags, ValueWord: row.ValueWord, ValueLen: row.ValueLen,
			ExpireAt: row.ExpireAt, Key: append([]byte(nil), row.Key...),
		})
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(walkRows) != len(rows) {
		t.Fatalf("walk saw %d rows, want %d", len(walkRows), len(rows))
	}
	for i := range rows {
		if walkAddrs[i] != addrs[i] {
			t.Fatalf("row %d walk addr %d, flush addr %d", i, walkAddrs[i], addrs[i])
		}
		if !rowsEqual(walkRows[i], rows[i]) {
			t.Fatalf("row %d walked %+v, want %+v", i, walkRows[i], rows[i])
		}
	}
}

// TestRecordLogWriterReadsStagedBeforeFlush serves a staged record from the pending
// buffer before the segment is cut, the read-before-flush the in-batch resolve
// needs.
func TestRecordLogWriterReadsStagedBeforeFlush(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewRecordLogWriter(f, 0)

	want := sampleRow("staged", 77)
	idx := w.Stage(want)
	got, err := w.ReadStaged(idx)
	if err != nil {
		t.Fatalf("read staged: %v", err)
	}
	if !rowsEqual(got, want) {
		t.Fatalf("staged read = %+v, want %+v", got, want)
	}
	if _, err := w.ReadStaged(idx + 1); err != ErrShort {
		t.Fatalf("out-of-range staged err = %v, want ErrShort", err)
	}
}

// TestRecordLogWriterEmptyFlushHoldsSeq flushes an empty batch: no segment is cut
// and no address is returned, so the shard sequence only advances on a real
// record. This is what lets the caller drive Flush on a timer without minting
// empty segments.
func TestRecordLogWriterEmptyFlushHoldsSeq(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewRecordLogWriter(f, 1)

	start := f.Cursor()
	addrs, err := w.Flush(5)
	if err != nil {
		t.Fatalf("empty flush: %v", err)
	}
	if addrs != nil {
		t.Fatalf("empty flush returned %d addresses, want none", len(addrs))
	}
	if f.Cursor() != start {
		t.Fatalf("empty flush moved the cursor from %d to %d", start, f.Cursor())
	}
}

// TestWalkRecordsSkipsOtherKinds interleaves a value_log segment between two record
// batches and confirms WalkRecords descends only into log segments, skipping the
// other kinds sharing the append space.
func TestWalkRecordsSkipsOtherKinds(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewRecordLogWriter(f, 0)

	w.Stage(sampleRow("first", 1))
	if _, err := w.Flush(1); err != nil {
		t.Fatalf("flush batch 1: %v", err)
	}
	// A value_log segment in between, which the record walk must skip.
	if _, err := f.AppendValues(0, 2, [][]byte{[]byte("a value blob")}); err != nil {
		t.Fatalf("append values: %v", err)
	}
	w.Stage(sampleRow("second", 2))
	if _, err := w.Flush(3); err != nil {
		t.Fatalf("flush batch 2: %v", err)
	}

	var keys []string
	if err := f.WalkRecords(PageSize, func(_ uint64, row RecordRow) error {
		keys = append(keys, string(row.Key))
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(keys) != 2 || keys[0] != "first" || keys[1] != "second" {
		t.Fatalf("walked keys = %v, want [first second]", keys)
	}
}

// TestWalkRecordsRespectsFrom starts the walk past the first batch and confirms
// only the records at or after `from` are visited, the checkpoint-bounded replay
// recovery runs.
func TestWalkRecordsRespectsFrom(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewRecordLogWriter(f, 0)

	w.Stage(sampleRow("before", 1))
	if _, err := w.Flush(1); err != nil {
		t.Fatalf("flush before: %v", err)
	}
	from := f.Cursor()
	w.Stage(sampleRow("after", 2))
	if _, err := w.Flush(2); err != nil {
		t.Fatalf("flush after: %v", err)
	}

	var keys []string
	if err := f.WalkRecords(from, func(_ uint64, row RecordRow) error {
		keys = append(keys, string(row.Key))
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(keys) != 1 || keys[0] != "after" {
		t.Fatalf("walked keys = %v, want [after]", keys)
	}
}

// TestWalkRecordsPropagatesVisitError stops the walk when a visit fails and returns
// that error, so a store-side apply failure fails the restore rather than dropping
// a committed record.
func TestWalkRecordsPropagatesVisitError(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewRecordLogWriter(f, 0)

	w.Stage(sampleRow("one", 1))
	w.Stage(sampleRow("two", 2))
	if _, err := w.Flush(1); err != nil {
		t.Fatalf("flush: %v", err)
	}

	boom := errors.New("apply refused")
	seen := 0
	err := f.WalkRecords(PageSize, func(_ uint64, _ RecordRow) error {
		seen++
		return boom
	})
	if err != boom {
		t.Fatalf("walk err = %v, want boom", err)
	}
	if seen != 1 {
		t.Fatalf("visit called %d times, want 1 before the stop", seen)
	}
}

// TestReadRecordAtResolvesFlushAddress reads each record back at the absolute
// address Flush returned, the random-access deref a checkpoint's record_addr
// takes. Every field must survive the round trip through the file.
func TestReadRecordAtResolvesFlushAddress(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewRecordLogWriter(f, 2)

	rows := []RecordRow{
		sampleRow("point-a", 1<<63|11),
		{Flags: RecFlagTombstone, Key: []byte("point-b")},
		sampleRow("a-noticeably-longer-key", 0xcafef00d),
	}
	for _, r := range rows {
		w.Stage(r)
	}
	addrs, err := w.Flush(4)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	for i, addr := range addrs {
		got, err := f.ReadRecordAt(addr)
		if err != nil {
			t.Fatalf("read record %d at %d: %v", i, addr, err)
		}
		if !rowsEqual(got, rows[i]) {
			t.Fatalf("record %d = %+v, want %+v", i, got, rows[i])
		}
	}
}

// TestReadRecordAtDetectsTorn corrupts a record body in the file and confirms a
// point read fails ErrChecksum rather than returning a rotted row.
func TestReadRecordAtDetectsTorn(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewRecordLogWriter(f, 0)

	w.Stage(sampleRow("victim", 5))
	addrs, err := w.Flush(1)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Flip a byte inside the record body in the backing store. The address points
	// at the varint, so the body starts one byte in for this small frame.
	dev.buf[addrs[0]+2] ^= 0xff
	if _, err := f.ReadRecordAt(addrs[0]); err != ErrChecksum {
		t.Fatalf("torn point read err = %v, want ErrChecksum", err)
	}
}
