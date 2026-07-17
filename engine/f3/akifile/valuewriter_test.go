package akifile

import (
	"bytes"
	"testing"
)

// TestValueLogWriterCoalescesBatch stages several values and flushes once: the
// whole batch lands in a single value_log segment and each pointer resolves to
// its bytes, the coalescing the store's spill path re-homes onto.
func TestValueLogWriterCoalescesBatch(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewValueLogWriter(f, 3)

	vals := [][]byte{[]byte("alpha"), bytes.Repeat([]byte("b"), 2000), []byte("gamma")}
	for i, v := range vals {
		if got := w.Stage(v); got != i {
			t.Fatalf("stage %d returned index %d", i, got)
		}
	}
	if w.Staged() != len(vals) {
		t.Fatalf("staged = %d, want %d", w.Staged(), len(vals))
	}

	start := f.Cursor()
	ptrs, err := w.Flush(9)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if span := f.Cursor() - start; span != SegmentAlign {
		t.Fatalf("flush spanned %d, want one segment %d", span, SegmentAlign)
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
	if w.Staged() != 0 || w.PendingBytes() != 0 {
		t.Fatalf("accumulator not reset after flush: staged %d pending %d", w.Staged(), w.PendingBytes())
	}
}

// TestValueLogWriterReadsStagedBeforeFlush serves a staged value from the pending
// buffer before the segment is cut, the read-before-flush the scratch log gave the
// store.
func TestValueLogWriterReadsStagedBeforeFlush(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewValueLogWriter(f, 0)

	idx := w.Stage([]byte("staged-and-visible"))
	got, err := w.ReadStaged(idx)
	if err != nil {
		t.Fatalf("read staged: %v", err)
	}
	if string(got) != "staged-and-visible" {
		t.Fatalf("staged read = %q, want the staged bytes", got)
	}
	if _, err := w.ReadStaged(idx + 1); err != ErrShort {
		t.Fatalf("out-of-range staged read = %v, want ErrShort", err)
	}
}

// TestValueLogWriterEmptyFlushWritesNothing leaves the file untouched when no value
// is staged: no wasted segment, no cursor move.
func TestValueLogWriterEmptyFlushWritesNothing(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewValueLogWriter(f, 0)

	start := f.Cursor()
	ptrs, err := w.Flush(1)
	if err != nil || ptrs != nil {
		t.Fatalf("empty flush = %v/%v, want nil/nil", ptrs, err)
	}
	if f.Cursor() != start {
		t.Fatalf("cursor moved to %d on an empty flush", f.Cursor())
	}
}

// TestValueLogWriterSeparatesFlushes stages across two flushes and confirms the
// second segment sits past the first, so pointers from both batches resolve.
func TestValueLogWriterSeparatesFlushes(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	w := NewValueLogWriter(f, 1)

	w.Stage([]byte("first-batch"))
	firstPtrs, err := w.Flush(1)
	if err != nil {
		t.Fatalf("first flush: %v", err)
	}
	w.Stage([]byte("second-batch"))
	secondPtrs, err := w.Flush(2)
	if err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if secondPtrs[0].ValueOff <= firstPtrs[0].ValueOff {
		t.Fatalf("second batch at %d did not advance past the first at %d", secondPtrs[0].ValueOff, firstPtrs[0].ValueOff)
	}
	for name, p := range map[string]ValuePointer{"first-batch": firstPtrs[0], "second-batch": secondPtrs[0]} {
		got, err := f.ReadValueAt(p, nil)
		if err != nil || string(got) != name {
			t.Fatalf("read %q = %q/%v", name, got, err)
		}
	}
}
