package f1raw

import (
	"fmt"
	"path/filepath"
	"testing"
)

// newRecStore builds an in-memory store with a cold record region in the test's temp dir
// and registers Close so the region file is shut on teardown. It has no cold value log, so
// it exercises the plain (inline-value) migration and read path.
func newRecStore(t *testing.T) *Store {
	t.Helper()
	s := New(1<<12, 1<<20)
	if err := s.EnableColdRecords(filepath.Join(t.TempDir(), "recs.log")); err != nil {
		t.Fatalf("EnableColdRecords: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestColdRecordRoundtrip is the M1 gate: write a string record, migrate it to the cold
// record region, and read it back correctly through Get. It also confirms the entry is
// tagged cold (the record left the arena, not just its value), that a resident sibling is
// untouched by the migration, and that the region accounting reflects one frame.
func TestColdRecordRoundtrip(t *testing.T) {
	s := newRecStore(t)

	if err := s.Set([]byte("cold-key"), []byte("cold-value")); err != nil {
		t.Fatalf("Set cold-key: %v", err)
	}
	if err := s.Set([]byte("warm-key"), []byte("warm-value")); err != nil {
		t.Fatalf("Set warm-key: %v", err)
	}

	// Before migration both keys are resident and read from the arena.
	if v, ok := s.Get([]byte("cold-key"), nil); !ok || string(v) != "cold-value" {
		t.Fatalf("pre-migrate Get cold-key = %q,%v; want cold-value,true", v, ok)
	}
	if total, _ := s.ColdRecords(); total != 0 {
		t.Fatalf("region holds %d bytes before any migration, want 0", total)
	}

	// Migrate cold-key. Its index entry must flip to a cold address and its value must
	// still read back byte-for-byte, now served from the region.
	if !s.MigrateToCold([]byte("cold-key"), stringKind) {
		t.Fatal("MigrateToCold(cold-key) returned false")
	}
	if !s.entryIsCold(t, []byte("cold-key"), stringKind) {
		t.Fatal("cold-key index entry is not tagged cold after migration")
	}
	if v, ok := s.Get([]byte("cold-key"), nil); !ok || string(v) != "cold-value" {
		t.Fatalf("post-migrate Get cold-key = %q,%v; want cold-value,true", v, ok)
	}
	if total, _ := s.ColdRecords(); total == 0 {
		t.Fatal("region holds 0 bytes after migrating one record")
	}

	// The resident sibling was not disturbed by the migration.
	if s.entryIsCold(t, []byte("warm-key"), stringKind) {
		t.Fatal("warm-key was tagged cold; migration touched the wrong entry")
	}
	if v, ok := s.Get([]byte("warm-key"), nil); !ok || string(v) != "warm-value" {
		t.Fatalf("Get warm-key = %q,%v; want warm-value,true", v, ok)
	}

	// A migrate of an already-cold key is a no-op that reports success, and a migrate of a
	// missing key reports failure.
	if !s.MigrateToCold([]byte("cold-key"), stringKind) {
		t.Fatal("re-migrating an already-cold key returned false")
	}
	if s.MigrateToCold([]byte("absent-key"), stringKind) {
		t.Fatal("migrating a missing key returned true")
	}
}

// TestColdRecordManyRoundtrip migrates a batch of distinct records and reads every one back
// from the region, so a tag collision or a frame-offset arithmetic slip would show up as a
// wrong or missing value. Half the keys stay resident to confirm the two tiers coexist under
// one index.
func TestColdRecordManyRoundtrip(t *testing.T) {
	s := newRecStore(t)

	const n = 500
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		v := []byte(fmt.Sprintf("value-for-%05d", i))
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}
	// Migrate the even-indexed keys; leave the odd ones resident.
	for i := 0; i < n; i += 2 {
		if !s.MigrateToCold([]byte(fmt.Sprintf("k%05d", i)), stringKind) {
			t.Fatalf("MigrateToCold(k%05d) returned false", i)
		}
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		want := fmt.Sprintf("value-for-%05d", i)
		v, ok := s.Get(k, nil)
		if !ok || string(v) != want {
			t.Fatalf("Get %q = %q,%v; want %q,true", k, v, ok, want)
		}
		cold := s.entryIsCold(t, k, stringKind)
		if (i%2 == 0) != cold {
			t.Fatalf("key %q cold=%v, want %v", k, cold, i%2 == 0)
		}
	}
}

// TestColdRecordSeparatedValue migrates a record whose value had already been separated to
// the cold value log (flagSep set). The frame carries the 12-byte value pointer, and a read
// must chase it into the value log for the real bytes (D6, the doubly-cold case).
func TestColdRecordSeparatedValue(t *testing.T) {
	// A store with both a value log (threshold 8) and a record region.
	s, err := NewWithCold(1<<12, 1<<20, filepath.Join(t.TempDir(), "vals.log"), 8)
	if err != nil {
		t.Fatalf("NewWithCold: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.EnableColdRecords(filepath.Join(t.TempDir(), "recs.log")); err != nil {
		t.Fatalf("EnableColdRecords: %v", err)
	}

	big := "this value is well over the eight byte separation threshold"
	if err := s.Set([]byte("sep"), []byte(big)); err != nil {
		t.Fatalf("Set sep: %v", err)
	}
	// The value separated to the value log; the record is still resident holding a pointer.
	if !s.isSepKey(t, []byte("sep"), stringKind) {
		t.Fatal("large value did not separate to the value log")
	}
	if v, ok := s.Get([]byte("sep"), nil); !ok || string(v) != big {
		t.Fatalf("pre-migrate Get sep = %q,%v", v, ok)
	}

	// Now migrate the record. Reading it back is two preads: the frame, then the value log.
	if !s.MigrateToCold([]byte("sep"), stringKind) {
		t.Fatal("MigrateToCold(sep) returned false")
	}
	if !s.entryIsCold(t, []byte("sep"), stringKind) {
		t.Fatal("sep entry not tagged cold after migration")
	}
	if v, ok := s.Get([]byte("sep"), nil); !ok || string(v) != big {
		t.Fatalf("post-migrate Get sep = %q,%v; want the full value,true", v, ok)
	}
}

// TestColdRecordBringUpOnWrite is the M3 slice-5 gate for doc 21 section 9's tier-crossing write
// and delete paths. A cold record's frame is immutable, so a write to a cold key must not mutate it
// in place: it appends a fresh resident record and flips the entry back to the arena (write brings
// the record up), marking the old cold frame dead. Incr on a cold counter does the same after
// reading the current value from the region. A delete of a cold key drops the entry and marks the
// frame dead. Every step is checked through the public API plus the tier probe and the region's
// dead accounting, so a path that indexed the arena at a cold offset would panic or read garbage.
func TestColdRecordBringUpOnWrite(t *testing.T) {
	s := newRecStore(t)

	// A plain overwrite of a cold key brings it back up resident, reads the new value, and marks
	// the old frame dead.
	if err := s.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set k1: %v", err)
	}
	if !s.MigrateToCold([]byte("k1"), stringKind) {
		t.Fatal("MigrateToCold(k1) returned false")
	}
	if !s.entryIsCold(t, []byte("k1"), stringKind) {
		t.Fatal("k1 not cold after migration")
	}
	_, deadBefore := s.ColdRecords()
	if err := s.Set([]byte("k1"), []byte("v2-longer")); err != nil {
		t.Fatalf("Set k1 (bring-up): %v", err)
	}
	if s.entryIsCold(t, []byte("k1"), stringKind) {
		t.Fatal("k1 still cold after overwrite; write did not bring it up")
	}
	if v, ok := s.Get([]byte("k1"), nil); !ok || string(v) != "v2-longer" {
		t.Fatalf("Get k1 after bring-up = %q,%v; want v2-longer,true", v, ok)
	}
	if _, deadAfter := s.ColdRecords(); deadAfter <= deadBefore {
		t.Fatalf("cold dead bytes did not grow on bring-up: %d -> %d", deadBefore, deadAfter)
	}

	// Incr on a cold counter reads the region, adds, and republishes resident.
	if err := s.Set([]byte("ctr"), []byte("41")); err != nil {
		t.Fatalf("Set ctr: %v", err)
	}
	if !s.MigrateToCold([]byte("ctr"), stringKind) {
		t.Fatal("MigrateToCold(ctr) returned false")
	}
	if n, err := s.Incr([]byte("ctr"), 1); err != nil || n != 42 {
		t.Fatalf("Incr cold ctr = %d,%v; want 42,nil", n, err)
	}
	if s.entryIsCold(t, []byte("ctr"), stringKind) {
		t.Fatal("ctr still cold after Incr; write did not bring it up")
	}
	if v, ok := s.Get([]byte("ctr"), nil); !ok || string(v) != "42" {
		t.Fatalf("Get ctr after Incr = %q,%v; want 42,true", v, ok)
	}

	// A delete of a cold key drops the entry and marks its frame dead.
	if err := s.Set([]byte("gone"), []byte("bye")); err != nil {
		t.Fatalf("Set gone: %v", err)
	}
	if !s.MigrateToCold([]byte("gone"), stringKind) {
		t.Fatal("MigrateToCold(gone) returned false")
	}
	_, deadBefore = s.ColdRecords()
	if !s.Delete([]byte("gone")) {
		t.Fatal("Delete(cold gone) returned false")
	}
	if _, ok := s.Get([]byte("gone"), nil); ok {
		t.Fatal("Get gone after delete still found it")
	}
	if _, deadAfter := s.ColdRecords(); deadAfter <= deadBefore {
		t.Fatalf("cold dead bytes did not grow on cold delete: %d -> %d", deadBefore, deadAfter)
	}
}

// entryIsCold reports whether key's index entry in the given kind namespace carries a cold
// (tier-bit-set) address. It probes the index directly so a test can assert which tier a key
// landed in.
func (s *Store) entryIsCold(t *testing.T, key []byte, kind byte) bool {
	t.Helper()
	off, _, _, _, found := s.find(key, hash(key), kind)
	if !found {
		t.Fatalf("entryIsCold: key %q missing", key)
	}
	return off&tierBit != 0
}

// isSepKey reports whether key's resident record carries a separated (cold value pointer)
// cell. It is only meaningful before migration, when the record is still resident.
func (s *Store) isSepKey(t *testing.T, key []byte, kind byte) bool {
	t.Helper()
	off, _, _, _, found := s.find(key, hash(key), kind)
	if !found {
		t.Fatalf("isSepKey: key %q missing", key)
	}
	return off&tierBit == 0 && s.isSep(off)
}
