package store

import (
	"bytes"
	"testing"
)

// TestLogSeamForwardsToScratchLog appends a value to the log and reads it back
// through the store's read seam: logReadInto returns the whole value and
// logReadFill serves an interior window, the two entry points every separated
// and chunked band now takes instead of reaching for s.vlog directly. It pins
// the seam against the scratch log so the value-log re-home can flip it onto
// the .aki adapter without disturbing the bands that call it.
func TestLogSeamForwardsToScratchLog(t *testing.T) {
	s := newLogStore(t, 1<<20)

	val := bytes.Repeat([]byte("abcdefgh"), 400) // 3200 bytes
	off, err := s.vlog.append(val)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	got, err := s.logReadInto(off, len(val), nil)
	if err != nil {
		t.Fatalf("logReadInto: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("logReadInto returned %d bytes, want %d", len(got), len(val))
	}

	win := make([]byte, 8)
	if err := s.logReadFill(off+16, win); err != nil {
		t.Fatalf("logReadFill: %v", err)
	}
	if want := val[16:24]; !bytes.Equal(win, want) {
		t.Fatalf("logReadFill = %q, want %q", win, want)
	}
}
