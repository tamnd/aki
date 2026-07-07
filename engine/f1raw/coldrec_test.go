package f1raw

import (
	"bytes"
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

// TestColdKindWriteDeleteTakeAcrossTiers is the collection twin of the string bring-up test: the
// element-per-row *Kind write, delete, and take paths must handle a cold (tier-bit-set) offset the
// same way Set/Delete handle a cold string. Once the background migrator admits a collection element
// kind (hash field, set member, list element), a live SADD/HSET/SREM/LPOP can land on a field whose
// record the migrator already sank cold, so PutKind's in-place branch, DeleteKind, and TakeKind each
// see a cold offset. Before the tier guard those indexed the arena at a tier-tagged offset and
// panicked out of range. This drives the flip directly with MigrateToCold to prove the point paths
// are tier-correct independent of the background migrator.
func TestColdKindWriteDeleteTakeAcrossTiers(t *testing.T) {
	s := newRecStore(t)

	// Overwrite a cold field: PutKind must bring it up resident, read the new value, and mark the
	// old frame dead rather than memcpy over the immutable cold frame.
	if _, err := s.PutKind([]byte("f-write"), []byte("v1"), benchKindHashField); err != nil {
		t.Fatalf("PutKind f-write: %v", err)
	}
	if !s.MigrateToCold([]byte("f-write"), benchKindHashField) {
		t.Fatal("MigrateToCold(f-write) returned false")
	}
	if !s.entryIsCold(t, []byte("f-write"), benchKindHashField) {
		t.Fatal("f-write not cold after migration")
	}
	_, deadBefore := s.ColdRecords()
	created, err := s.PutKind([]byte("f-write"), []byte("v2-longer"), benchKindHashField)
	if err != nil {
		t.Fatalf("PutKind f-write (bring-up): %v", err)
	}
	if created {
		t.Fatal("PutKind over an existing cold field reported created; want update")
	}
	if s.entryIsCold(t, []byte("f-write"), benchKindHashField) {
		t.Fatal("f-write still cold after overwrite; write did not bring it up")
	}
	if v, ok := s.GetKind([]byte("f-write"), nil, benchKindHashField); !ok || string(v) != "v2-longer" {
		t.Fatalf("GetKind f-write after bring-up = %q,%v; want v2-longer,true", v, ok)
	}
	if _, deadAfter := s.ColdRecords(); deadAfter <= deadBefore {
		t.Fatalf("cold dead bytes did not grow on field bring-up: %d -> %d", deadBefore, deadAfter)
	}

	// Delete a cold field: DeleteKind must drop the entry and mark its frame dead, not charge a
	// resident segment for bytes that never left the region.
	if _, err := s.PutKind([]byte("f-del"), []byte("d1"), benchKindHashField); err != nil {
		t.Fatalf("PutKind f-del: %v", err)
	}
	if !s.MigrateToCold([]byte("f-del"), benchKindHashField) {
		t.Fatal("MigrateToCold(f-del) returned false")
	}
	_, deadBefore = s.ColdRecords()
	if !s.DeleteKind([]byte("f-del"), benchKindHashField) {
		t.Fatal("DeleteKind(cold f-del) returned false")
	}
	if s.ExistsKind([]byte("f-del"), benchKindHashField) {
		t.Fatal("f-del still present after cold delete")
	}
	if _, deadAfter := s.ColdRecords(); deadAfter <= deadBefore {
		t.Fatalf("cold dead bytes did not grow on cold field delete: %d -> %d", deadBefore, deadAfter)
	}

	// Take a cold field: TakeKind must read the value through the cold frame, drop the entry, and
	// mark the frame dead, the fused read-then-delete a list pop rides.
	if _, err := s.PutKind([]byte("f-take"), []byte("t1-value"), benchKindHashField); err != nil {
		t.Fatalf("PutKind f-take: %v", err)
	}
	if !s.MigrateToCold([]byte("f-take"), benchKindHashField) {
		t.Fatal("MigrateToCold(f-take) returned false")
	}
	_, deadBefore = s.ColdRecords()
	v, ok := s.TakeKind([]byte("f-take"), nil, benchKindHashField)
	if !ok || string(v) != "t1-value" {
		t.Fatalf("TakeKind cold f-take = %q,%v; want t1-value,true", v, ok)
	}
	if s.ExistsKind([]byte("f-take"), benchKindHashField) {
		t.Fatal("f-take still present after cold take")
	}
	if _, deadAfter := s.ColdRecords(); deadAfter <= deadBefore {
		t.Fatalf("cold dead bytes did not grow on cold field take: %d -> %d", deadBefore, deadAfter)
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

// TestColdCollScanReresolvesMigrated is the D22 Option B gate: a value-carrying enumeration
// over a hash whose element records have partly migrated to the cold record region must return
// every field's value, resolving the migrated ones through the cold frame. The ordered-index
// node caches the element's resident offset, which the migration turned into dead space, so a
// scan that trusted that cached offset would read stale bytes (or dangle once the segment is
// reclaimed). scanBatch re-resolves each node through the tier-aware index, so a migrated
// element surfaces with its cold (tier-bit-set) address and ReadValueAt preads the frame. The
// migrator ships gated to strings, so this drives the flip directly with MigrateToCold to prove
// the enumeration path is correct the moment migratable() widens to collection kinds.
func TestColdCollScanReresolvesMigrated(t *testing.T) {
	s := newRecStore(t)
	const n = 24
	const coll = "h:"
	want := make(map[string][]byte, n)
	var kb [64]byte
	keysByIndex := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := collElemKey(kb[:], coll, uint64(i))
		v := []byte(fmt.Sprintf("field-value-%02d", i))
		if _, err := s.PutKind(k, v, benchKindHashField); err != nil {
			t.Fatalf("PutKind %d: %v", i, err)
		}
		s.CollInsert(k, benchKindHashField)
		kc := append([]byte(nil), k...)
		keysByIndex[i] = kc
		want[string(kc)] = v
	}

	// Migrate every third element cold; the rest stay resident. The scan must read both tiers.
	for i := 0; i < n; i += 3 {
		if !s.MigrateToCold(keysByIndex[i], benchKindHashField) {
			t.Fatalf("MigrateToCold field %d returned false", i)
		}
	}

	keys, offs, _ := s.CollScanKV([]byte(coll), nil, n, nil, nil)
	if len(keys) != n {
		t.Fatalf("CollScanKV returned %d keys, want %d", len(keys), n)
	}
	sawCold := false
	for i, off := range offs {
		val := s.ReadValueAt(off, nil)
		w := want[string(keys[i])]
		if !bytes.Equal(val, w) {
			t.Fatalf("field %q read back %q, want %q through the tier-aware enumeration", keys[i], val, w)
		}
		if off&tierBit != 0 {
			sawCold = true
			// A migrated element must report non-inline so a consumer that tries the zero-copy
			// path falls back to the cold pread rather than indexing the arena with a tagged offset.
			if _, inline := s.ValueAtLocked(off); inline {
				t.Fatalf("cold field %q reported inline, want the ReadValueAt fallback", keys[i])
			}
		}
	}
	if !sawCold {
		t.Fatal("no enumerated field re-resolved to a cold address; the migration or re-resolution did not engage")
	}
}

// TestColdCollScanReresolvesMigratedLongKey is the same D22 Option B gate as above, but every
// element's composite key is longer than onodeInlineCap (32), so the ordered-index node cannot
// hold the whole key inline and instead keeps its own full copy. The point of the test is that a
// long key stays arena-free too: once an element's record migrates cold and its resident bytes
// are reclaimed, nodeKey and cmpKey must still yield the key from the node's own storage, and
// nodeAddr must re-resolve the live cold address through the primary index by that key. If the
// node still read the key from the (reclaimed) arena offset, enumeration would either miss the
// element or read a foreign key, so the scan returning every field by its exact key and value is
// the proof the long-key path is arena-independent.
func TestColdCollScanReresolvesMigratedLongKey(t *testing.T) {
	s := newRecStore(t)
	const n = 24
	const coll = "h:"
	want := make(map[string][]byte, n)
	keysByIndex := make([][]byte, n)
	for i := 0; i < n; i++ {
		// A member long enough that coll+member exceeds onodeInlineCap, forcing the node's
		// full-key copy rather than the inline cache.
		member := fmt.Sprintf("member-with-a-deliberately-long-name-%03d", i)
		k := append([]byte(coll), member...)
		if len(k) <= onodeInlineCap {
			t.Fatalf("test key %q is %d bytes, not longer than the inline cap %d", k, len(k), onodeInlineCap)
		}
		v := []byte(fmt.Sprintf("field-value-%02d", i))
		if _, err := s.PutKind(k, v, benchKindHashField); err != nil {
			t.Fatalf("PutKind %d: %v", i, err)
		}
		s.CollInsert(k, benchKindHashField)
		keysByIndex[i] = k
		want[string(k)] = v
	}

	for i := 0; i < n; i += 3 {
		if !s.MigrateToCold(keysByIndex[i], benchKindHashField) {
			t.Fatalf("MigrateToCold field %d returned false", i)
		}
	}

	keys, offs, _ := s.CollScanKV([]byte(coll), nil, n, nil, nil)
	if len(keys) != n {
		t.Fatalf("CollScanKV returned %d keys, want %d", len(keys), n)
	}
	sawCold := false
	for i, off := range offs {
		w, ok := want[string(keys[i])]
		if !ok {
			t.Fatalf("enumeration returned unexpected key %q, the long-key node read a stale arena offset", keys[i])
		}
		val := s.ReadValueAt(off, nil)
		if !bytes.Equal(val, w) {
			t.Fatalf("field %q read back %q, want %q through the tier-aware enumeration", keys[i], val, w)
		}
		if off&tierBit != 0 {
			sawCold = true
		}
	}
	if !sawCold {
		t.Fatal("no enumerated long-key field re-resolved to a cold address; the migration or re-resolution did not engage")
	}
}

// fillSegHashFields is fillSegBig for the value-carrying hash-field kind: it writes churnVal
// element records under collection prefix coll until the current segment advances, maintaining the
// ordered index alongside each record so the drained hash still enumerates. It returns the segment
// the fields landed in and a map of composite key to value for exactly the fields that stayed in
// that segment, dropping the one field that spilled into the next segment so the returned set is
// precisely startSeg's live fields.
func fillSegHashFields(t *testing.T, s *Store, coll string) (startSeg uint64, want map[string][]byte) {
	t.Helper()
	want = make(map[string][]byte)
	startSeg = s.curSeg.Load()
	var kb [64]byte
	for i := 0; s.curSeg.Load() == startSeg; i++ {
		k := append([]byte(nil), collElemKey(kb[:], coll, uint64(i))...)
		v := churnVal(coll, i)
		if _, err := s.PutKind(k, v, benchKindHashField); err != nil {
			t.Fatalf("PutKind %q: %v", k, err)
		}
		if s.curSeg.Load() != startSeg {
			s.DeleteKind(k, benchKindHashField) // spilled into the next segment; keep the set exactly startSeg's
			break
		}
		s.CollInsert(k, benchKindHashField)
		want[string(k)] = v
	}
	if len(want) == 0 {
		t.Fatal("filled no hash fields before the segment advanced")
	}
	return startSeg, want
}

// TestMigratorDrainsAdmittedHashKind is the D22 Option B gate for the real background migrator (not
// the MigrateToCold test hook): once the server admits the hash-field kind through
// SetMigratableKindFunc, drainSegment must sink every live hash element in a full segment to the
// cold region, and every hash read path must resolve those elements across the tier boundary. The
// hash field's only secondary structure is the ordered index, whose nodes re-resolve their address
// through the tier-aware primary index on each access, so the migrator needs no per-node hook: the
// point read (GetKind), the value-carrying enumeration (CollScanKV + ReadValueAt), and the
// random-select-by-rank path (CollSelectAt) all follow the record cold on their own. Draining the
// whole segment to live == 0 with every field readable is the proof the policy widened the
// migrator's safe set past the string floor without breaking a single hash path.
func TestMigratorDrainsAdmittedHashKind(t *testing.T) {
	s := churnSegColdStore(t, 5)
	// Server policy: the hash-field kind is tier-safe (its ordered index re-resolves through the
	// primary index), so admit it; every other kind stays out and drains only if it is a string.
	s.SetMigratableKindFunc(func(kind byte) bool { return kind == benchKindHashField })

	const coll = "h:"
	seg0, want := fillSegHashFields(t, s, coll)
	// Advance past seg0 so it is a full, non-current segment, the only kind drainSegment drains.
	fillSegHashFields(t, s, "g:")
	if seg0 == s.curSeg.Load() {
		t.Fatal("seg0 is still the current segment; the fill did not advance off it")
	}

	s.drainSegment(seg0)

	// The segment emptied and retired: every live hash field left it for the cold region.
	if got := s.segs[seg0].live.Load(); got != 0 {
		t.Fatalf("seg %d live = %d after drain, want 0 (every admitted field should have migrated)", seg0, got)
	}

	// Point path: GetKind reads each field across the tier, and every field is genuinely cold now,
	// proving the migrator moved it rather than leaving it resident.
	for k, v := range want {
		if !s.entryIsCold(t, []byte(k), benchKindHashField) {
			t.Fatalf("field %q did not migrate cold; the admitted kind was not drained", k)
		}
		got, ok := s.GetKind([]byte(k), nil, benchKindHashField)
		if !ok || !bytes.Equal(got, v) {
			t.Fatalf("GetKind %q = %q,%v after drain; want %q,true", k, got, ok, v)
		}
	}

	// Enumeration path: CollScanKV re-resolves each node through the tier-aware index and hands back
	// the current (cold) offset, which ReadValueAt reads from the cold frame. Every field must come
	// back exactly once with its exact value; a stale resident offset would read foreign bytes.
	keys, offs, _ := s.CollScanKV([]byte(coll), nil, len(want)+8, nil, nil)
	if len(keys) != len(want) {
		t.Fatalf("CollScanKV returned %d fields, want %d", len(keys), len(want))
	}
	seen := make(map[string]bool, len(want))
	for i, k := range keys {
		v, ok := want[string(k)]
		if !ok {
			t.Fatalf("CollScanKV returned unexpected key %q (a stale resident offset)", k)
		}
		if seen[string(k)] {
			t.Fatalf("CollScanKV returned key %q twice", k)
		}
		seen[string(k)] = true
		if off := offs[i]; off&tierBit == 0 {
			t.Fatalf("CollScanKV re-resolved field %q to a resident offset, want cold after the drain", k)
		}
		if got := s.ReadValueAt(offs[i], nil); !bytes.Equal(got, v) {
			t.Fatalf("CollScanKV value for %q read back wrong across the tier", k)
		}
	}

	// Random-select path: CollSelectAt returns a member key by local rank arena-free, and GetKind
	// reads its value from the cold tier. Every local index in range must name a known field.
	for i := 0; i < len(want); i++ {
		k, ok := s.CollSelectAt([]byte(coll), i)
		if !ok {
			t.Fatalf("CollSelectAt(%d) not found, want a field", i)
		}
		v, ok := want[string(k)]
		if !ok {
			t.Fatalf("CollSelectAt(%d) returned unknown key %q", i, k)
		}
		got, ok := s.GetKind(k, nil, benchKindHashField)
		if !ok || !bytes.Equal(got, v) {
			t.Fatalf("CollSelectAt(%d) value for %q read back wrong across the tier", i, k)
		}
	}
}

// TestMigratorLeavesUnadmittedHashKindResident is the negative half of the policy gate: with no
// migratable policy registered the migrator is string-only, exactly the pre-policy behavior, so a
// hash field record must stay resident even when its segment is drained. This guards against a
// future default that silently sinks a kind whose secondary structures are not yet tier-safe (the
// set member vector is the live example): admitting a kind must be an explicit server opt-in, never
// the engine's own default.
func TestMigratorLeavesUnadmittedHashKindResident(t *testing.T) {
	s := churnSegColdStore(t, 5)
	// No SetMigratableKindFunc call: the default nil policy leaves the migrator string-only.
	const coll = "h:"
	seg0, want := fillSegHashFields(t, s, coll)
	fillSegHashFields(t, s, "g:")
	if seg0 == s.curSeg.Load() {
		t.Fatal("seg0 is still the current segment; the fill did not advance off it")
	}

	s.drainSegment(seg0)

	for k := range want {
		if s.entryIsCold(t, []byte(k), benchKindHashField) {
			t.Fatalf("field %q migrated cold with no migratable policy set; want resident", k)
		}
	}
}
