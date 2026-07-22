package store

import "testing"

// The store is the string type's index once a tiny collection can live inline in
// an arena record: Len, RangeKeys, VolatileKeys, GetString, and StrLen all read
// the string keyspace alone, so DBSIZE, KEYS, and a wrong-type GET stay correct
// when the set, hash, list, zset, and stream types home their tiny keys here and
// count them through CountCollKind. These tests drive PutCollBlob records against
// the string surface directly; the per-type routing that puts real collections on
// the path is a later slice, so this seam is exercised in isolation.

// Len counts string keys only: an inline collection record raises the true record
// total but not the string count, so DBSIZE unions the type arms without double
// counting a unified-keyspace key.
func TestLenExcludesCollRecords(t *testing.T) {
	s := newTestStore()
	if err := s.SetString([]byte("str:a"), []byte("v"), 0, 0, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	if err := s.PutCollBlob([]byte("set:a"), kindSet, 1, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	if err := s.PutCollBlob([]byte("hash:a"), kindHash, 1, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (the lone string key)", s.Len())
	}
	// CountCollKind adds each type's keys back; the union is the true total.
	setN, _ := s.CountCollKind(kindSet)
	hashN, _ := s.CountCollKind(kindHash)
	if total := uint64(s.Len()) + setN + hashN; total != 3 {
		t.Fatalf("unified keyspace total = %d, want 3", total)
	}
}

// RangeKeys walks the string arm only, so KEYS and SCAN with no TYPE do not list
// a collection key on the string arm (the type's own RangeCollKind lists it).
func TestRangeKeysSkipsCollRecords(t *testing.T) {
	s := newTestStore()
	if err := s.SetString([]byte("str:a"), []byte("v"), 0, 0, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	if err := s.PutCollBlob([]byte("set:a"), kindSet, 1, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	seen := map[string]bool{}
	s.RangeKeys(0, func(key []byte) bool {
		seen[string(key)] = true
		return true
	})
	if len(seen) != 1 || !seen["str:a"] {
		t.Fatalf("RangeKeys saw %v, want just str:a", seen)
	}
	if seen["set:a"] {
		t.Fatal("RangeKeys listed a collection key on the string arm")
	}
}

// VolatileKeys counts the string arm's TTLs only: a TTL'd collection is that
// type's expires figure (CountCollKind's withTTL), not the string store's.
func TestVolatileKeysSkipsCollRecords(t *testing.T) {
	s := newTestStore()
	if err := s.SetString([]byte("str:a"), []byte("v"), 100, 5000, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	if err := s.PutCollBlob([]byte("set:a"), kindSet, 1, []byte("payload!!"), 5000, 100); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	if n := s.VolatileKeys(); n != 1 {
		t.Fatalf("VolatileKeys = %d, want 1 (the string key's TTL alone)", n)
	}
	if _, withTTL := s.CountCollKind(kindSet); withTTL != 1 {
		t.Fatalf("CountCollKind(set) withTTL = %d, want 1", withTTL)
	}
}

// GetString and StrLen are string-arm-only: a collection key reads as absent, the
// pre-value answer a wrong-type GET or STRLEN gives, with no aliasing of the
// embedded blob as a string value.
func TestStringReadsAbsentForCollRecords(t *testing.T) {
	s := newTestStore()
	if err := s.PutCollBlob([]byte("set:a"), kindSet, 1, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	if v, ok := s.GetString([]byte("set:a"), 0, nil); ok {
		t.Fatalf("GetString on a collection key = (%q, true), want absent", v)
	}
	if n, ok := s.StrLen([]byte("set:a"), 0); ok {
		t.Fatalf("StrLen on a collection key = (%d, true), want absent", n)
	}
	// Exists still reports the key present: WRONGTYPE needs the key to be seen, so
	// the string-arm seam narrows the value reads, not the existence check.
	if !s.Exists([]byte("set:a"), 0) {
		t.Fatal("Exists reported a collection key absent; WRONGTYPE would misfire")
	}
}

// collCount stays exact across every record swap: a coll superseding a coll, a
// string superseding a coll, a delete, and a lazy expiry each move Len by the
// right amount, so the string count never drifts as tiny collections churn.
func TestCollCountBalancedAcrossSwaps(t *testing.T) {
	s := newTestStore()
	key := []byte("k")
	// A collection lands: Len stays 0 (no string key), the record total is 1.
	if err := s.PutCollBlob(key, kindSet, 1, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob set: %v", err)
	}
	if s.Len() != 0 {
		t.Fatalf("Len after coll put = %d, want 0", s.Len())
	}
	// A larger collection at the same key republishes (fresh record supersedes):
	// the drop and the publish net to no change in the coll count.
	big := make([]byte, 128)
	if err := s.PutCollBlob(key, kindSet, 1, big, 0, 0); err != nil {
		t.Fatalf("PutCollBlob big: %v", err)
	}
	if s.Len() != 0 {
		t.Fatalf("Len after coll republish = %d, want 0", s.Len())
	}
	if n, _ := s.CountCollKind(kindSet); n != 1 {
		t.Fatalf("CountCollKind(set) = %d, want 1 after republish", n)
	}
	// A string overwrites the collection at the same key: the coll leaves, the
	// string arrives, Len becomes 1.
	if err := s.SetString(key, []byte("now a string"), 0, 0, false); err != nil {
		t.Fatalf("SetString over coll: %v", err)
	}
	if s.Len() != 1 {
		t.Fatalf("Len after string-over-coll = %d, want 1", s.Len())
	}
	if n, _ := s.CountCollKind(kindSet); n != 0 {
		t.Fatalf("CountCollKind(set) = %d, want 0 after string overwrite", n)
	}
	// Back to a collection, then delete it: Len returns to 0 and the coll count
	// clears, proving the delete path decrements through dropRecord.
	if err := s.PutCollBlob(key, kindSet, 1, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob set again: %v", err)
	}
	if !s.Del(key, 0) {
		t.Fatal("Del reported no key to drop")
	}
	if s.Len() != 0 {
		t.Fatalf("Len after coll delete = %d, want 0", s.Len())
	}
	if n, _ := s.CountCollKind(kindSet); n != 0 {
		t.Fatalf("CountCollKind(set) = %d, want 0 after delete", n)
	}
}

// A lazy expiry that reaps a collection record decrements the coll count through
// the shared dropRecord exit, so the string count and the coll subset both stay
// exact when a TTL'd collection is reaped by a touch.
func TestCollCountBalancedOnLazyExpiry(t *testing.T) {
	s := newTestStore()
	if err := s.PutCollBlob([]byte("set:ttl"), kindSet, 1, []byte("payload!!"), 1000, 500); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	if n, _ := s.CountCollKind(kindSet); n != 1 {
		t.Fatalf("CountCollKind(set) = %d, want 1 before expiry", n)
	}
	// A keyed touch past the deadline reaps the record.
	if _, _, _, ok := s.GetCollBlob([]byte("set:ttl"), 2000); ok {
		t.Fatal("GetCollBlob past deadline returned present")
	}
	if n, _ := s.CountCollKind(kindSet); n != 0 {
		t.Fatalf("CountCollKind(set) = %d, want 0 after lazy expiry", n)
	}
	if s.Len() != 0 {
		t.Fatalf("Len after lazy expiry = %d, want 0", s.Len())
	}
}

// PutCollBlob's in-place path flips a string record to a collection without
// touching the index, so it owns the coll-count increment: Len drops by one when
// a string is rewritten in place as a tiny collection.
func TestPutCollBlobInPlaceFlipsCount(t *testing.T) {
	s := newTestStore()
	key := []byte("k")
	// A string with room to spare for the collection blob, so the coll put lands
	// in place rather than republishing.
	if err := s.SetString(key, []byte("a string value with slack"), 0, 0, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	if s.Len() != 1 {
		t.Fatalf("Len after string put = %d, want 1", s.Len())
	}
	if err := s.PutCollBlob(key, kindSet, 1, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob in place: %v", err)
	}
	if s.Len() != 0 {
		t.Fatalf("Len after in-place flip = %d, want 0", s.Len())
	}
	if n, _ := s.CountCollKind(kindSet); n != 1 {
		t.Fatalf("CountCollKind(set) = %d, want 1 after in-place flip", n)
	}
	// A second in-place coll rewrite must not double count: still one collection.
	if err := s.PutCollBlob(key, kindSet, 2, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob in place 2: %v", err)
	}
	if n, _ := s.CountCollKind(kindSet); n != 1 {
		t.Fatalf("CountCollKind(set) = %d, want 1 after second in-place rewrite", n)
	}
	if s.Len() != 0 {
		t.Fatalf("Len after second in-place rewrite = %d, want 0", s.Len())
	}
}

// Reset zeroes the coll count with the record count, so a flushed store reports a
// clean string keyspace and does not carry a stale coll subtrahend.
func TestResetZeroesCollCount(t *testing.T) {
	s := newTestStore()
	if err := s.PutCollBlob([]byte("set:a"), kindSet, 1, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	if err := s.SetString([]byte("str:a"), []byte("v"), 0, 0, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	s.Reset()
	if s.Len() != 0 {
		t.Fatalf("Len after Reset = %d, want 0", s.Len())
	}
	// A fresh string after the flush counts from a clean base, not a negative one.
	if err := s.SetString([]byte("str:b"), []byte("v"), 0, 0, false); err != nil {
		t.Fatalf("SetString after Reset: %v", err)
	}
	if s.Len() != 1 {
		t.Fatalf("Len after Reset then one string = %d, want 1", s.Len())
	}
}
