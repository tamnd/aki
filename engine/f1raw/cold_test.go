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

// TestColdRepeatedReadsDropCache drives many separated reads that each advise their
// range out of the page cache (FADV_DONTNEED on Linux, a no-op elsewhere) and confirms
// every value still reads back byte-identical. Correctness must not depend on whether
// the cache was dropped: dropping only affects residency, never the bytes returned, so
// re-reading a value whose pages were just advised away must still succeed.
func TestColdRepeatedReadsDropCache(t *testing.T) {
	s := newColdStore(t, 512)
	const n = 64
	val := strings.Repeat("q", 2048)
	for i := 0; i < n; i++ {
		if err := s.Set([]byte{'k', byte(i)}, []byte(val)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	for round := 0; round < 4; round++ {
		for i := 0; i < n; i++ {
			got, ok := s.Get([]byte{'k', byte(i)}, nil)
			if !ok || string(got) != val {
				t.Fatalf("round %d key %d read back wrong across a cache drop (ok=%v)", round, i, ok)
			}
		}
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

// TestColdKindSeparatedRoundtrip is the collection twin of TestColdSeparatedRoundtrip: a
// large element value (a big hash field) written through PutKind must separate to the cold
// log and read back byte-identical through GetKind's cold branch. This is the property that
// lets a collection of large values exceed memory while its index and field names stay
// resident.
func TestColdKindSeparatedRoundtrip(t *testing.T) {
	s := newColdStore(t, 512)
	big := bytes.Repeat([]byte("x"), 4096)
	created, err := s.PutKind([]byte("f"), big, benchKindHashField)
	if err != nil {
		t.Fatalf("PutKind: %v", err)
	}
	if !created {
		t.Fatal("first PutKind of a field should report created")
	}
	off, _, _, _, found := s.find([]byte("f"), hash([]byte("f")), benchKindHashField)
	if !found {
		t.Fatal("field record not found after PutKind")
	}
	if !s.isSep(off) {
		t.Fatal("field value over threshold was not separated to the cold log")
	}
	got, ok := s.GetKind([]byte("f"), nil, benchKindHashField)
	if !ok || !bytes.Equal(got, big) {
		t.Fatalf("GetKind = (%d bytes, %v), want the 4096-byte field value", len(got), ok)
	}
}

// TestColdKindOverwriteTransitions walks a field across every inline/separated transition,
// the collection twin of the three string overwrite tests. Each transition must read back
// the new value, never stale bytes, and the created flag must stay false for every
// overwrite of an existing field.
func TestColdKindOverwriteTransitions(t *testing.T) {
	s := newColdStore(t, 512)
	small := []byte("small")
	big := bytes.Repeat([]byte("z"), 2048)
	big2 := bytes.Repeat([]byte("w"), 3072)

	check := func(stage string, val []byte, wantSep bool) {
		off, _, _, _, found := s.find([]byte("f"), hash([]byte("f")), benchKindHashField)
		if !found {
			t.Fatalf("%s: field not found", stage)
		}
		if s.isSep(off) != wantSep {
			t.Fatalf("%s: isSep=%v, want %v", stage, s.isSep(off), wantSep)
		}
		got, ok := s.GetKind([]byte("f"), nil, benchKindHashField)
		if !ok || !bytes.Equal(got, val) {
			t.Fatalf("%s: GetKind wrong (ok=%v, len=%d)", stage, ok, len(got))
		}
	}
	put := func(stage string, val []byte, wantCreated bool) {
		created, err := s.PutKind([]byte("f"), val, benchKindHashField)
		if err != nil {
			t.Fatalf("%s: PutKind: %v", stage, err)
		}
		if created != wantCreated {
			t.Fatalf("%s: created=%v, want %v", stage, created, wantCreated)
		}
	}

	put("create-inline", small, true)
	check("create-inline", small, false)
	put("inline->sep", big, false)
	check("inline->sep", big, true)
	put("sep->sep", big2, false)
	check("sep->sep", big2, true)
	put("sep->inline", small, false)
	check("sep->inline", small, false)
}

// TestColdTakeKindSeparated confirms the fused read-then-delete a list pop runs resolves a
// separated element through the cold log: TakeKind must return the large value intact and
// then remove the row, so a popped large list element reads correctly before it vanishes.
func TestColdTakeKindSeparated(t *testing.T) {
	s := newColdStore(t, 512)
	big := bytes.Repeat([]byte("p"), 2048)
	if _, err := s.PutKind([]byte("e"), big, benchKindHashField); err != nil {
		t.Fatalf("PutKind: %v", err)
	}
	got, ok := s.TakeKind([]byte("e"), nil, benchKindHashField)
	if !ok || !bytes.Equal(got, big) {
		t.Fatalf("TakeKind = (%d bytes, %v), want the separated value intact", len(got), ok)
	}
	if s.ExistsKind([]byte("e"), benchKindHashField) {
		t.Fatal("element still present after TakeKind")
	}
}

// TestColdScanKVSeparated drives the value-carrying enumeration (the HGETALL path) over a
// hash whose field values are all separated to the cold log. The walk resolves each value
// through ValueAtLocked (which reports inline=false for a separated record) then the
// ReadValueAt cold fallback, so every field must read back its exact value. This is the
// path the ValueAtLocked doc promised but that had no separated-collection coverage until
// the collection cold tier existed.
func TestColdScanKVSeparated(t *testing.T) {
	s := newColdStore(t, 512)
	const n = 32
	const coll = "h:"
	want := make(map[string][]byte, n)
	var kb [64]byte
	for i := 0; i < n; i++ {
		k := collElemKey(kb[:], coll, uint64(i))
		v := bytes.Repeat([]byte{byte('a' + i%26)}, 1024)
		if _, err := s.PutKind(k, v, benchKindHashField); err != nil {
			t.Fatalf("PutKind %d: %v", i, err)
		}
		s.CollInsert(k, benchKindHashField)
		want[string(k)] = v
	}
	keys, offs, _ := s.CollScanKV([]byte(coll), nil, n, nil, nil)
	if len(keys) != n {
		t.Fatalf("CollScanKV returned %d keys, want %d", len(keys), n)
	}
	for i, off := range offs {
		val, inline := s.ValueAtLocked(off)
		if inline {
			t.Fatalf("field %d reported inline, want separated (value should be on the cold log)", i)
		}
		val = s.ReadValueAt(off, nil)
		w := want[string(keys[i])]
		if !bytes.Equal(val, w) {
			t.Fatalf("field %q read back wrong through the cold enumeration fallback", keys[i])
		}
	}
}
