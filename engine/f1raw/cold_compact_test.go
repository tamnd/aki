package f1raw

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// These tests cover Compact, the cold-log reclamation that rewrites the live separated
// values forward into a fresh log and drops the dead bytes a same-key overwrite or a
// delete left behind. What matters is that it reclaims every dead byte (tail falls to the
// live size, dead falls to zero) while preserving every live value byte-for-byte, across
// strings and collection elements, inline records left untouched, and that it is a safe
// no-op with no cold log and idempotent when run twice. Compact carries the same quiesced
// contract as Reset (no concurrent traffic), so these are single-goroutine tests.

// newColdStoreSized is newColdStore with an explicit primary-bucket count, so a test can
// force overflow chains by asking for a small index against many keys.
func newColdStoreSized(t *testing.T, threshold, indexBuckets int) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cold.log")
	s, err := NewWithCold(indexBuckets, 1<<20, path, threshold)
	if err != nil {
		t.Fatalf("NewWithCold: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestCompactReclaimsDead overwrites one large separated key several times, leaving all but
// the last value dead in the log, then compacts. After compaction the log holds only the
// live value: tail equals the last value's length, dead is zero, and the key still reads the
// last value.
func TestCompactReclaimsDead(t *testing.T) {
	s := newColdStore(t, 512)
	sizes := []int{2048, 3072, 1024, 4096, 2560}
	var last []byte
	for _, sz := range sizes {
		last = bytes.Repeat([]byte("v"), sz)
		if err := s.Set([]byte("k"), last); err != nil {
			t.Fatalf("Set %d: %v", sz, err)
		}
	}
	total, dead := s.ColdBytes()
	if dead == 0 || dead >= total {
		t.Fatalf("before compact: total=%d dead=%d, want 0 < dead < total", total, dead)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	total, dead = s.ColdBytes()
	if dead != 0 {
		t.Fatalf("dead after compact = %d, want 0", dead)
	}
	if total != uint64(len(last)) {
		t.Fatalf("total after compact = %d, want %d (only the live value)", total, len(last))
	}
	got, ok := s.Get([]byte("k"), nil)
	if !ok || !bytes.Equal(got, last) {
		t.Fatalf("Get after compact = (%d bytes, %v), want the live value intact", len(got), ok)
	}
}

// TestCompactPreservesLiveStrings writes several separated string keys, overwrites some so
// the log carries dead bytes, then compacts and checks every key still reads its latest
// value and the log tail equals the sum of the live values with dead at zero.
func TestCompactPreservesLiveStrings(t *testing.T) {
	s := newColdStore(t, 512)
	want := map[string][]byte{}
	// First round: install a value for every key.
	for i := 0; i < 16; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := bytes.Repeat([]byte{byte('a' + i)}, 1024+i*64)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		want[k] = v
	}
	// Second round: overwrite the even keys, leaving their first value dead.
	for i := 0; i < 16; i += 2 {
		k := fmt.Sprintf("k%02d", i)
		v := bytes.Repeat([]byte{byte('A' + i)}, 2048+i*32)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("overwrite %s: %v", k, err)
		}
		want[k] = v
	}
	if _, dead := s.ColdBytes(); dead == 0 {
		t.Fatal("expected dead bytes from the overwrites before compact")
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	var liveSum uint64
	for k, v := range want {
		got, ok := s.Get([]byte(k), nil)
		if !ok || !bytes.Equal(got, v) {
			t.Fatalf("Get %s after compact = (%d bytes, %v), want %d bytes intact", k, len(got), ok, len(v))
		}
		liveSum += uint64(len(v))
	}
	total, dead := s.ColdBytes()
	if dead != 0 {
		t.Fatalf("dead after compact = %d, want 0", dead)
	}
	if total != liveSum {
		t.Fatalf("total after compact = %d, want %d (sum of live values)", total, liveSum)
	}
}

// TestCompactPreservesCollections writes several large separated hash fields, deletes some,
// then compacts and checks the surviving fields still read their values and the deleted ones
// stay gone, with the log reclaimed to the surviving size.
func TestCompactPreservesCollections(t *testing.T) {
	s := newColdStore(t, 512)
	live := map[string][]byte{}
	for i := 0; i < 12; i++ {
		f := fmt.Sprintf("f%02d", i)
		v := bytes.Repeat([]byte{byte('m' + i)}, 1500+i*100)
		if _, err := s.PutKind([]byte(f), v, benchKindHashField); err != nil {
			t.Fatalf("PutKind %s: %v", f, err)
		}
		live[f] = v
	}
	// Delete every third field; its cold bytes become dead.
	for i := 0; i < 12; i += 3 {
		f := fmt.Sprintf("f%02d", i)
		if !s.DeleteKind([]byte(f), benchKindHashField) {
			t.Fatalf("DeleteKind %s returned false", f)
		}
		delete(live, f)
	}
	if _, dead := s.ColdBytes(); dead == 0 {
		t.Fatal("expected dead bytes from the deletes before compact")
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	var liveSum uint64
	for f, v := range live {
		got, ok := s.GetKind([]byte(f), nil, benchKindHashField)
		if !ok || !bytes.Equal(got, v) {
			t.Fatalf("GetKind %s after compact = (%d bytes, %v), want %d bytes intact", f, len(got), ok, len(v))
		}
		liveSum += uint64(len(v))
	}
	// A deleted field must still be absent after compaction.
	if _, ok := s.GetKind([]byte("f00"), nil, benchKindHashField); ok {
		t.Fatal("deleted field f00 present after compact")
	}
	total, dead := s.ColdBytes()
	if dead != 0 || total != liveSum {
		t.Fatalf("after compact total=%d dead=%d, want %d/0", total, dead, liveSum)
	}
}

// TestCompactAllDead deletes the only separated key before compacting. Nothing is live, so
// the fresh log is empty: tail and dead both fall to zero and the key stays absent.
func TestCompactAllDead(t *testing.T) {
	s := newColdStore(t, 512)
	big := strings.Repeat("z", 4096)
	if err := s.Set([]byte("k"), []byte(big)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !s.Delete([]byte("k")) {
		t.Fatal("Delete returned false")
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	total, dead := s.ColdBytes()
	if total != 0 || dead != 0 {
		t.Fatalf("after compacting an all-dead log total=%d dead=%d, want 0/0", total, dead)
	}
	if _, ok := s.Get([]byte("k"), nil); ok {
		t.Fatal("deleted key present after compact")
	}
}

// TestCompactNoColdLog confirms Compact on a pure in-memory store (no cold log) is a safe
// no-op: it returns nil, does not panic, and leaves the store readable.
func TestCompactNoColdLog(t *testing.T) {
	s := New(1<<10, 1<<20)
	defer s.Close()
	if err := s.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact on no-cold-log store: %v", err)
	}
	if got, ok := s.Get([]byte("k"), nil); !ok || string(got) != "v" {
		t.Fatalf("Get after no-op compact = (%q, %v), want v", got, ok)
	}
}

// TestCompactIdempotent compacts a clean log (no dead bytes) and then compacts again. Both
// leave every live value intact, tail at the live size, and dead at zero: a second compaction
// of an already-live log changes nothing observable.
func TestCompactIdempotent(t *testing.T) {
	s := newColdStore(t, 512)
	want := map[string][]byte{}
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%d", i)
		v := bytes.Repeat([]byte{byte('a' + i)}, 1024+i*128)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		want[k] = v
	}
	var liveSum uint64
	for _, v := range want {
		liveSum += uint64(len(v))
	}
	for round := 0; round < 3; round++ {
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact round %d: %v", round, err)
		}
		total, dead := s.ColdBytes()
		if dead != 0 || total != liveSum {
			t.Fatalf("round %d: total=%d dead=%d, want %d/0", round, total, dead, liveSum)
		}
		for k, v := range want {
			got, ok := s.Get([]byte(k), nil)
			if !ok || !bytes.Equal(got, v) {
				t.Fatalf("round %d Get %s = (%d bytes, %v), want intact", round, k, len(got), ok)
			}
		}
	}
}

// TestCompactMixedInlineSeparated stores a mix of small inline values (below the threshold,
// never in the log) and large separated values, overwrites some of each, then compacts. The
// inline values are untouched by compaction and the separated ones are reclaimed: the log
// tail equals only the sum of the live separated values.
func TestCompactMixedInlineSeparated(t *testing.T) {
	s := newColdStore(t, 512)
	inline := map[string][]byte{}
	sep := map[string][]byte{}
	for i := 0; i < 10; i++ {
		ik := fmt.Sprintf("i%d", i)
		iv := bytes.Repeat([]byte{byte('a' + i)}, 32) // below threshold, stays inline
		if err := s.Set([]byte(ik), iv); err != nil {
			t.Fatalf("Set inline %s: %v", ik, err)
		}
		inline[ik] = iv

		sk := fmt.Sprintf("s%d", i)
		sv := bytes.Repeat([]byte{byte('A' + i)}, 1024+i*64) // above threshold, separated
		if err := s.Set([]byte(sk), sv); err != nil {
			t.Fatalf("Set separated %s: %v", sk, err)
		}
		sep[sk] = sv
	}
	// Overwrite the first few of each kind, leaving dead cold bytes for the separated ones and
	// nothing in the log for the inline ones.
	for i := 0; i < 4; i++ {
		ik := fmt.Sprintf("i%d", i)
		iv := bytes.Repeat([]byte{byte('z')}, 40)
		if err := s.Set([]byte(ik), iv); err != nil {
			t.Fatalf("overwrite inline %s: %v", ik, err)
		}
		inline[ik] = iv

		sk := fmt.Sprintf("s%d", i)
		sv := bytes.Repeat([]byte{byte('Z')}, 2048+i*64)
		if err := s.Set([]byte(sk), sv); err != nil {
			t.Fatalf("overwrite separated %s: %v", sk, err)
		}
		sep[sk] = sv
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for ik, iv := range inline {
		got, ok := s.Get([]byte(ik), nil)
		if !ok || !bytes.Equal(got, iv) {
			t.Fatalf("inline Get %s after compact = (%d bytes, %v), want intact", ik, len(got), ok)
		}
	}
	var liveSep uint64
	for sk, sv := range sep {
		got, ok := s.Get([]byte(sk), nil)
		if !ok || !bytes.Equal(got, sv) {
			t.Fatalf("separated Get %s after compact = (%d bytes, %v), want intact", sk, len(got), ok)
		}
		liveSep += uint64(len(sv))
	}
	total, dead := s.ColdBytes()
	if dead != 0 {
		t.Fatalf("dead after compact = %d, want 0", dead)
	}
	if total != liveSep {
		t.Fatalf("total after compact = %d, want %d (only live separated values, no inline)", total, liveSep)
	}
}

// TestCompactSurvivesOverflowChain forces the primary buckets to spill into overflow buckets
// by writing far more separated keys than a bucket holds, then compacts. Every key, including
// those reachable only through an overflow link, must survive: the walk follows the whole
// chain, not just the primary bucket.
func TestCompactSurvivesOverflowChain(t *testing.T) {
	// A small index (many keys per bucket) guarantees overflow chains.
	s := newColdStoreSized(t, 512, 1<<4)
	want := map[string][]byte{}
	const n = 400
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := bytes.Repeat([]byte{byte(i)}, 600+i%64)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		want[k] = v
	}
	// Overwrite half, seeding dead bytes across the overflow chains.
	for i := 0; i < n; i += 2 {
		k := fmt.Sprintf("key-%04d", i)
		v := bytes.Repeat([]byte{byte(i + 1)}, 700+i%48)
		if err := s.Set([]byte(k), v); err != nil {
			t.Fatalf("overwrite %s: %v", k, err)
		}
		want[k] = v
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	var liveSum uint64
	for k, v := range want {
		got, ok := s.Get([]byte(k), nil)
		if !ok || !bytes.Equal(got, v) {
			t.Fatalf("Get %s after compact = (%d bytes, %v), want intact", k, len(got), ok)
		}
		liveSum += uint64(len(v))
	}
	total, dead := s.ColdBytes()
	if dead != 0 || total != liveSum {
		t.Fatalf("after compact total=%d dead=%d, want %d/0", total, dead, liveSum)
	}
}
