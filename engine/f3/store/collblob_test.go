package store

import (
	"bytes"
	"testing"
)

// isCollKind bounds every collection kind and excludes the string kind and the
// zero byte a reused arena offset reads as.
func TestIsCollKind(t *testing.T) {
	for _, k := range []byte{kindSet, kindHash, kindList, kindZSet, kindStream} {
		if !isCollKind(k) {
			t.Fatalf("isCollKind(%#x) = false, want true", k)
		}
	}
	for _, k := range []byte{0x00, kindString, kindStream + 1} {
		if isCollKind(k) {
			t.Fatalf("isCollKind(%#x) = true, want false", k)
		}
	}
}

// A put then get round-trips the blob, kind, and per-collection bits, and the
// record reads as absent to a string get so the two never alias a value.
func TestCollBlobRoundTrip(t *testing.T) {
	s := newTestStore()
	key := []byte("s:1")
	blob := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	if err := s.PutCollBlob(key, kindSet, 0x1234, blob, 0, 100); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	got, kind, bits, ok := s.GetCollBlob(key, 100)
	if !ok {
		t.Fatal("GetCollBlob: not present after put")
	}
	if kind != kindSet {
		t.Fatalf("kind = %#x, want %#x", kind, kindSet)
	}
	if bits != 0x1234 {
		t.Fatalf("bits = %#x, want 0x1234", bits)
	}
	if !bytes.Equal(got, blob) {
		t.Fatalf("blob = %v, want %v", got, blob)
	}
	if k, ok := s.CollKind(key, 100); !ok || k != kindSet {
		t.Fatalf("CollKind = (%#x, %v), want (%#x, true)", k, ok, kindSet)
	}
	// CollKind is the discriminant the routing slice gates string reads on: a
	// plain string key reads as not-a-collection. The string read path itself
	// is not rewired in this inert slice, so GetString still returns the raw
	// embedded bytes; the dual-home WRONGTYPE handling lands with per-type
	// routing.
	if err := s.SetString([]byte("str:1"), []byte("plain"), 100, 0, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	if _, ok := s.CollKind([]byte("str:1"), 100); ok {
		t.Fatal("CollKind reported a plain string as a collection")
	}
}

// A second put at the same key with a fitting blob rewrites in place (no fresh
// record), and one with a larger blob republishes; both read back correctly.
func TestCollBlobRewrite(t *testing.T) {
	s := newTestStore()
	key := []byte("h:1")
	if err := s.PutCollBlob(key, kindHash, 1, []byte("aaaaaaaa"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob 1: %v", err)
	}
	// Same size, fits the reserved capacity: in place, kind and bits update.
	if err := s.PutCollBlob(key, kindHash, 2, []byte("bbbbbbbb"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob 2: %v", err)
	}
	got, kind, bits, ok := s.GetCollBlob(key, 0)
	if !ok || kind != kindHash || bits != 2 || !bytes.Equal(got, []byte("bbbbbbbb")) {
		t.Fatalf("after in-place rewrite: (%q, %#x, %d, %v)", got, kind, bits, ok)
	}
	// Larger blob: republish under a fresh capacity.
	big := bytes.Repeat([]byte("z"), 64)
	if err := s.PutCollBlob(key, kindHash, 3, big, 0, 0); err != nil {
		t.Fatalf("PutCollBlob 3: %v", err)
	}
	got, _, bits, ok = s.GetCollBlob(key, 0)
	if !ok || bits != 3 || !bytes.Equal(got, big) {
		t.Fatalf("after republish: (%q, %d, %v)", got, bits, ok)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (one key across rewrites)", s.Len())
	}
}

// A TTL'd collection record is reaped by a touch past its deadline, the same
// lazy-expiry rule strings follow.
func TestCollBlobExpiry(t *testing.T) {
	s := newTestStore()
	key := []byte("z:1")
	if err := s.PutCollBlob(key, kindZSet, 0, []byte("payload!!"), 1000, 500); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	if _, _, _, ok := s.GetCollBlob(key, 999); !ok {
		t.Fatal("collection reaped before its deadline")
	}
	if _, _, _, ok := s.GetCollBlob(key, 1001); ok {
		t.Fatal("collection survived past its deadline")
	}
}

// A blob past the inline ceiling is refused, the caller's cue to promote.
func TestCollBlobTooBig(t *testing.T) {
	s := New(2<<20, 1<<18)
	if err := s.PutCollBlob([]byte("k"), kindList, 0, make([]byte, collInlineMax+1), 0, 0); err != ErrTooBig {
		t.Fatalf("oversize PutCollBlob = %v, want ErrTooBig", err)
	}
}
