package f1raw

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// newColdStore builds a store with a cold value log in the test's temp dir and the
// given separation threshold, and registers Close so the log file is shut on teardown.
func newColdStore(t *testing.T, threshold int) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cold.log")
	s, err := NewWithCold(1<<10, 1<<20, path, threshold)
	if err != nil {
		t.Fatalf("NewWithCold: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// getString is Get with a fresh dst, returning the value as a string and presence.
func getString(t *testing.T, s *Store, key string) (string, bool) {
	t.Helper()
	v, ok := s.Get([]byte(key), nil)
	return string(v), ok
}

// TestColdSeparatedRoundtrip stores a value past the threshold and reads it back. The
// value must survive the trip out to the cold log and back in through the pointer.
func TestColdSeparatedRoundtrip(t *testing.T) {
	s := newColdStore(t, 512)
	big := strings.Repeat("x", 4096)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	off, _, _, _, found := s.find([]byte("k"), hash([]byte("k")), stringKind)
	if !found {
		t.Fatal("record not found after Set")
	}
	if !s.isSep(off) {
		t.Fatal("value over threshold was not separated to the cold log")
	}
	got, ok := getString(t, s, "k")
	if !ok || got != big {
		t.Fatalf("Get = (%q..., %v), want the 4096-byte value", got[:min(8, len(got))], ok)
	}
}

// TestColdThresholdBoundary checks the inline-versus-separated cutoff: a value at the
// threshold stays inline, one byte over is separated. Both must read back intact.
func TestColdThresholdBoundary(t *testing.T) {
	s := newColdStore(t, 512)

	atCap := strings.Repeat("a", 512)
	if err := s.Set([]byte("at"), []byte(atCap)); err != nil {
		t.Fatalf("Set at: %v", err)
	}
	offAt, _, _, _, _ := s.find([]byte("at"), hash([]byte("at")), stringKind)
	if s.isSep(offAt) {
		t.Fatal("value exactly at threshold should stay inline")
	}
	if got, ok := getString(t, s, "at"); !ok || got != atCap {
		t.Fatalf("Get at = (%v), want the inline value intact", ok)
	}

	over := strings.Repeat("b", 513)
	if err := s.Set([]byte("over"), []byte(over)); err != nil {
		t.Fatalf("Set over: %v", err)
	}
	offOver, _, _, _, _ := s.find([]byte("over"), hash([]byte("over")), stringKind)
	if !s.isSep(offOver) {
		t.Fatal("value one byte over threshold should be separated")
	}
	if got, ok := getString(t, s, "over"); !ok || got != over {
		t.Fatalf("Get over = (%v), want the separated value intact", ok)
	}
}

// TestColdOverwriteInlineToSeparated overwrites a small inline value with a large one.
// The record must flip to separated and read back the new large value, not the stale
// inline bytes.
func TestColdOverwriteInlineToSeparated(t *testing.T) {
	s := newColdStore(t, 512)
	if err := s.Set([]byte("k"), []byte("small")); err != nil {
		t.Fatalf("Set small: %v", err)
	}
	big := strings.Repeat("z", 2048)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set big: %v", err)
	}
	off, _, _, _, _ := s.find([]byte("k"), hash([]byte("k")), stringKind)
	if !s.isSep(off) {
		t.Fatal("overwrite with a large value should separate the record")
	}
	if got, ok := getString(t, s, "k"); !ok || got != big {
		t.Fatalf("Get after inline->separated overwrite wrong (ok=%v)", ok)
	}
}

// TestColdOverwriteSeparatedToInline overwrites a large separated value with a small
// one. The record must flip back to inline and read the new small value. The old cold
// bytes leaking is expected in M1 and does not affect correctness.
func TestColdOverwriteSeparatedToInline(t *testing.T) {
	s := newColdStore(t, 512)
	big := strings.Repeat("z", 2048)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set big: %v", err)
	}
	if err := s.Set([]byte("k"), []byte("small")); err != nil {
		t.Fatalf("Set small: %v", err)
	}
	off, _, _, _, _ := s.find([]byte("k"), hash([]byte("k")), stringKind)
	if s.isSep(off) {
		t.Fatal("overwrite with a small value should return the record to inline")
	}
	if got, ok := getString(t, s, "k"); !ok || got != "small" {
		t.Fatalf("Get after separated->inline overwrite = (%q, %v), want small", got, ok)
	}
}

// TestColdOverwriteSeparatedToSeparated overwrites one large value with another. Each
// separated write is a fresh cold record, so the second value must win.
func TestColdOverwriteSeparatedToSeparated(t *testing.T) {
	s := newColdStore(t, 512)
	first := bytes.Repeat([]byte("1"), 2048)
	second := bytes.Repeat([]byte("2"), 3072)
	if err := s.Set([]byte("k"), first); err != nil {
		t.Fatalf("Set first: %v", err)
	}
	if err := s.Set([]byte("k"), second); err != nil {
		t.Fatalf("Set second: %v", err)
	}
	got, ok := s.Get([]byte("k"), nil)
	if !ok || !bytes.Equal(got, second) {
		t.Fatal("second separated write should win")
	}
}

// TestColdDelete removes a separated key. The index entry drops and the key reads
// absent afterward.
func TestColdDelete(t *testing.T) {
	s := newColdStore(t, 512)
	big := strings.Repeat("z", 2048)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !s.Delete([]byte("k")) {
		t.Fatal("Delete of present separated key returned false")
	}
	if _, ok := s.Get([]byte("k"), nil); ok {
		t.Fatal("separated key still present after Delete")
	}
}

// TestColdIncrOnSeparated confirms INCR on a large separated value fails with ErrNotInt
// like any non-integer string, with no special-casing for the cold path: a multi-kilo
// byte blob is not a valid integer.
func TestColdIncrOnSeparated(t *testing.T) {
	s := newColdStore(t, 512)
	big := strings.Repeat("9", 2048)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := s.Incr([]byte("k"), 1); err != ErrNotInt {
		t.Fatalf("Incr on separated value = %v, want ErrNotInt", err)
	}
}

// TestColdInlinePathUnchanged is a guard that opening a cold log does not alter the
// small-value path: a sub-threshold value stays inline and behaves exactly as it does
// on a pure in-memory store.
func TestColdInlinePathUnchanged(t *testing.T) {
	s := newColdStore(t, 512)
	if err := s.Set([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	if err := s.Set([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Set v2: %v", err)
	}
	off, _, _, _, _ := s.find([]byte("k"), hash([]byte("k")), stringKind)
	if s.isSep(off) {
		t.Fatal("small value should never separate")
	}
	if got, ok := getString(t, s, "k"); !ok || got != "v2" {
		t.Fatalf("inline update path broke under a cold log: (%q, %v)", got, ok)
	}
}
