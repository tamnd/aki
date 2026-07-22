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

// PeekCollBlob returns the blob, kind, bits, and deadline without reaping an
// expired record, so the routing layer can own the ordered expired event, and
// without stamping the clock. It reads a live record and a past-deadline record
// alike, and reports a string or missing key as absent.
func TestPeekCollBlob(t *testing.T) {
	s := newTestStore()
	key := []byte("s:peek")
	blob := []byte{9, 8, 7, 6, 5, 4, 3, 2}
	if err := s.PutCollBlob(key, kindSet, 0x00FF, blob, 2000, 500); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	got, kind, bits, at, ok := s.PeekCollBlob(key)
	if !ok || kind != kindSet || bits != 0x00FF || at != 2000 || !bytes.Equal(got, blob) {
		t.Fatalf("peek = (%q, %#x, %#x, %d, %v)", got, kind, bits, at, ok)
	}
	// A past-deadline record still peeks: peek does not reap, so the caller sees
	// the deadline and fires the event itself.
	if _, _, _, at, ok := s.PeekCollBlob(key); !ok || at != 2000 {
		t.Fatalf("peek after deadline: (at %d, ok %v), want the record still present", at, ok)
	}
	// A later keyed get past the deadline still reaps, proving peek left the
	// lazy-expiry path intact.
	if _, _, _, ok := s.GetCollBlob(key, 3000); ok {
		t.Fatal("GetCollBlob past deadline returned present")
	}
	// A plain string key is absent to a collection peek.
	if err := s.SetString([]byte("str:peek"), []byte("plain"), 0, 0, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	if _, _, _, _, ok := s.PeekCollBlob([]byte("str:peek")); ok {
		t.Fatal("peek reported a plain string as a collection")
	}
	if _, _, _, _, ok := s.PeekCollBlob([]byte("nope")); ok {
		t.Fatal("peek reported a missing key as present")
	}
}

// SetCollBits overwrites the bits word in place, moving no bytes and touching no
// other field, and refuses a string or missing key.
func TestSetCollBits(t *testing.T) {
	s := newTestStore()
	key := []byte("h:bits")
	blob := []byte("abcdefgh")
	if err := s.PutCollBlob(key, kindHash, 0x0001, blob, 0, 0); err != nil {
		t.Fatalf("PutCollBlob: %v", err)
	}
	if !s.SetCollBits(key, 0xBEEF) {
		t.Fatal("SetCollBits reported no record to write")
	}
	got, kind, bits, ok := s.GetCollBlob(key, 0)
	if !ok || kind != kindHash || bits != 0xBEEF || !bytes.Equal(got, blob) {
		t.Fatalf("after SetCollBits: (%q, %#x, %#x, %v)", got, kind, bits, ok)
	}
	if err := s.SetString([]byte("str:bits"), []byte("x"), 0, 0, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	if s.SetCollBits([]byte("str:bits"), 1) {
		t.Fatal("SetCollBits wrote a plain string record")
	}
	if s.SetCollBits([]byte("missing"), 1) {
		t.Fatal("SetCollBits wrote a missing key")
	}
}

// RangeCollKind visits exactly the live records of the requested kind, skips
// other kinds and strings, stops early when fn returns false, and skips a
// past-deadline record when now is non-zero.
func TestRangeCollKind(t *testing.T) {
	s := newTestStore()
	for _, k := range []string{"a", "b", "c"} {
		if err := s.PutCollBlob([]byte("set:"+k), kindSet, 0, []byte("payload!!"), 0, 0); err != nil {
			t.Fatalf("PutCollBlob set:%s: %v", k, err)
		}
	}
	if err := s.PutCollBlob([]byte("hash:z"), kindHash, 0, []byte("payload!!"), 0, 0); err != nil {
		t.Fatalf("PutCollBlob hash:z: %v", err)
	}
	if err := s.SetString([]byte("str:x"), []byte("v"), 0, 0, false); err != nil {
		t.Fatalf("SetString: %v", err)
	}
	// One set carries a past deadline; with now non-zero it drops from the walk.
	if err := s.PutCollBlob([]byte("set:ttl"), kindSet, 0, []byte("payload!!"), 1000, 500); err != nil {
		t.Fatalf("PutCollBlob set:ttl: %v", err)
	}

	seen := map[string]bool{}
	s.RangeCollKind(kindSet, 2000, func(key []byte) bool {
		seen[string(key)] = true
		return true
	})
	want := map[string]bool{"set:a": true, "set:b": true, "set:c": true}
	if len(seen) != len(want) {
		t.Fatalf("RangeCollKind(set) saw %v, want %v", seen, want)
	}
	for k := range want {
		if !seen[k] {
			t.Fatalf("RangeCollKind(set) missed %s (saw %v)", k, seen)
		}
	}
	// now == 0 counts the expired record too, the map-size basis.
	n := 0
	s.RangeCollKind(kindSet, 0, func(key []byte) bool { n++; return true })
	if n != 4 {
		t.Fatalf("RangeCollKind(set, now=0) saw %d, want 4 (expired included)", n)
	}
	// Early stop after the first key.
	count := 0
	s.RangeCollKind(kindSet, 0, func(key []byte) bool { count++; return false })
	if count != 1 {
		t.Fatalf("RangeCollKind early stop visited %d, want 1", count)
	}
}

// CountCollKind returns the per-kind key total and the count carrying a
// deadline, counting a lazily-expired record until a keyed read drops it.
func TestCountCollKind(t *testing.T) {
	s := newTestStore()
	for _, k := range []string{"a", "b"} {
		if err := s.PutCollBlob([]byte("set:"+k), kindSet, 0, []byte("payload!!"), 0, 0); err != nil {
			t.Fatalf("PutCollBlob set:%s: %v", k, err)
		}
	}
	if err := s.PutCollBlob([]byte("set:ttl"), kindSet, 0, []byte("payload!!"), 1000, 500); err != nil {
		t.Fatalf("PutCollBlob set:ttl: %v", err)
	}
	if err := s.PutCollBlob([]byte("hash:z"), kindHash, 0, []byte("payload!!"), 5000, 0); err != nil {
		t.Fatalf("PutCollBlob hash:z: %v", err)
	}
	total, withTTL := s.CountCollKind(kindSet)
	if total != 3 || withTTL != 1 {
		t.Fatalf("CountCollKind(set) = (%d, %d), want (3, 1)", total, withTTL)
	}
	total, withTTL = s.CountCollKind(kindHash)
	if total != 1 || withTTL != 1 {
		t.Fatalf("CountCollKind(hash) = (%d, %d), want (1, 1)", total, withTTL)
	}
	if total, withTTL := s.CountCollKind(kindZSet); total != 0 || withTTL != 0 {
		t.Fatalf("CountCollKind(zset) = (%d, %d), want (0, 0)", total, withTTL)
	}
}
