package f1raw

import "testing"

// TestExistsAnyKey pins the one property the lock-free RPOPLPUSH/LMOVE missing-source
// shortcut relies on: ExistsAnyKey answers "does this bare key exist under any kind"
// with a single probe-chain walk. A key present as a string or as a collection header
// row must read as present, a truly missing key must read as absent, and a key that
// exists only under composite element keys must NOT make its bare name read as present
// (element rows hash their own composite bytes and never collide with the bare key).
func TestExistsAnyKey(t *testing.T) {
	s := New(1<<10, 1<<20)

	if err := s.Set([]byte("str"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := s.PutKind([]byte("hdr"), []byte("meta"), benchKindHashField); err != nil {
		t.Fatalf("PutKind header: %v", err)
	}
	// An element row lives under a composite key, not the bare collection name.
	var kb [64]byte
	if _, err := s.PutKind(collElemKey(kb[:], "coll", 7), []byte("e"), benchKindZsetMember); err != nil {
		t.Fatalf("PutKind elem: %v", err)
	}

	if !s.ExistsAnyKey([]byte("str")) {
		t.Fatalf("string key should exist under any kind")
	}
	if !s.ExistsAnyKey([]byte("hdr")) {
		t.Fatalf("collection header key should exist under any kind")
	}
	if s.ExistsAnyKey([]byte("missing")) {
		t.Fatalf("missing key must read as absent")
	}
	// The bare collection name has no header row here, only element rows under composite
	// keys, so the bare name must read as absent: that is what lets the RPOPLPUSH shortcut
	// treat a drained source (header gone, element rows may linger) as truly missing only
	// when nothing is left under the bare key.
	if s.ExistsAnyKey([]byte("coll")) {
		t.Fatalf("bare collection name with only element rows must read as absent")
	}

	// After deleting the string, its key must read as absent again.
	if !s.Delete([]byte("str")) {
		t.Fatalf("Delete string returned false")
	}
	if s.ExistsAnyKey([]byte("str")) {
		t.Fatalf("deleted string key must read as absent")
	}
}
