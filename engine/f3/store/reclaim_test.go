package store

import (
	"bytes"
	"testing"
)

// sepVal builds a separated-band value: over strInlineMax, under strChunkMin.
func sepVal(c byte, n int) []byte {
	return bytes.Repeat([]byte{c}, n)
}

// TestSetSepReplaceInPlace pins the separated-band full-replace path: a SET
// over a separated record whose new value fits the run's reserved capacity
// rewrites the run in place, keeping the record, the run, and the arena fill
// exactly where they were. This is what makes sustained same-size overwrite
// steady-state instead of an arena-full death.
func TestSetSepReplaceInPlace(t *testing.T) {
	s := testStore(t, 4)
	key := []byte("sep")
	if err := s.Set(key, sepVal('a', 4096)); err != nil {
		t.Fatal(err)
	}
	_, addr, _ := s.findEntry(Hash(key), key)
	if addr == 0 {
		t.Fatal("key missing after SET")
	}
	word, _, vcap := s.readPtr(s.valueStart(addr))
	fill := s.arena.used()

	for i := 0; i < 100; i++ {
		if err := s.Set(key, sepVal(byte('b'+i%20), 4096)); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}
	_, addr2, _ := s.findEntry(Hash(key), key)
	if addr2 != addr {
		t.Fatalf("record moved on same-size replace: %d -> %d", addr, addr2)
	}
	w2, _, c2 := s.readPtr(s.valueStart(addr))
	if w2 != word || c2 != vcap {
		t.Fatalf("run moved on same-size replace: %x/%d -> %x/%d", word, vcap, w2, c2)
	}
	if got := s.arena.used(); got != fill {
		t.Fatalf("arena fill grew from %d to %d across in-place replaces", fill, got)
	}
	mustGet(t, s, key, string(sepVal(byte('b'+99%20), 4096)))
}

// TestSetSepReplaceGrows pins the other half: a replace past the run's
// capacity keeps the record, swaps in a fresh run, and charges the old run's
// bytes dead in its segment.
func TestSetSepReplaceGrows(t *testing.T) {
	s := testStore(t, 4)
	key := []byte("sep")
	if err := s.Set(key, sepVal('a', 4096)); err != nil {
		t.Fatal(err)
	}
	_, addr, _ := s.findEntry(Hash(key), key)
	live := s.arena.live()
	if err := s.Set(key, sepVal('b', 8192)); err != nil {
		t.Fatal(err)
	}
	_, addr2, _ := s.findEntry(Hash(key), key)
	if addr2 != addr {
		t.Fatalf("record republished on a run-only grow: %d -> %d", addr, addr2)
	}
	mustGet(t, s, key, string(sepVal('b', 8192)))
	// The ledger swapped a 4096 charge for an 8192 one; the difference is the
	// exact growth, meaning the old run was charged back where it lay.
	if got := s.arena.live(); got != live+4096 {
		t.Fatalf("live charge %d, want %d", got, live+4096)
	}
}

// TestSetSepReplaceTTL pins the deadline rules across the in-place separated
// replace: KEEPTTL carries the deadline, a plain SET on a slotted record
// clears it in place, and a SET with a deadline on a slotless record still
// republishes into a slotted one.
func TestSetSepReplaceTTL(t *testing.T) {
	s := testStore(t, 4)
	key := []byte("sep")
	now := int64(1_000)
	if err := s.SetString(key, sepVal('a', 2048), now, now+60_000, false); err != nil {
		t.Fatal(err)
	}
	_, addr, _ := s.findEntry(Hash(key), key)

	// KEEPTTL replace stays in place and keeps the deadline.
	if err := s.SetString(key, sepVal('b', 2048), now, 0, true); err != nil {
		t.Fatal(err)
	}
	_, addr2, _ := s.findEntry(Hash(key), key)
	if addr2 != addr {
		t.Fatal("KEEPTTL replace republished the record")
	}
	if at := s.expireAt(addr2); at != now+60_000 {
		t.Fatalf("deadline %d, want %d", at, now+60_000)
	}

	// A plain SET clears the deadline in the slot, still in place.
	if err := s.SetString(key, sepVal('c', 2048), now, 0, false); err != nil {
		t.Fatal(err)
	}
	_, addr3, _ := s.findEntry(Hash(key), key)
	if addr3 != addr {
		t.Fatal("deadline-clearing replace republished the record")
	}
	if at := s.expireAt(addr3); at != 0 {
		t.Fatalf("deadline survived a plain SET: %d", at)
	}

	// A slotless record cannot take a deadline in place: fresh key without a
	// TTL, then SET with one must republish into a slotted record.
	k2 := []byte("sep2")
	if err := s.Set(k2, sepVal('a', 2048)); err != nil {
		t.Fatal(err)
	}
	_, b1, _ := s.findEntry(Hash(k2), k2)
	if err := s.SetString(k2, sepVal('b', 2048), now, now+5_000, false); err != nil {
		t.Fatal(err)
	}
	_, b2, _ := s.findEntry(Hash(k2), k2)
	if b2 == b1 {
		t.Fatal("slotless record took a deadline without republishing")
	}
	if at := s.expireAt(b2); at != now+5_000 {
		t.Fatalf("deadline %d, want %d", at, now+5_000)
	}
	mustGet(t, s, k2, string(sepVal('b', 2048)))
}

// TestSetSepReplaceBandChange pins that a full replace which leaves the
// separated band still republishes and re-selects from scratch.
func TestSetSepReplaceBandChange(t *testing.T) {
	s := testStore(t, 4)
	key := []byte("sep")
	if err := s.Set(key, sepVal('a', 2048)); err != nil {
		t.Fatal(err)
	}
	_, addr, _ := s.findEntry(Hash(key), key)
	if err := s.Set(key, []byte("small")); err != nil {
		t.Fatal(err)
	}
	_, addr2, _ := s.findEntry(Hash(key), key)
	if addr2 == addr {
		t.Fatal("band change reused the separated record")
	}
	mustGet(t, s, key, "small")
	st := s.Stats()
	if st.Embedded != 1 || st.Separated != 0 {
		t.Fatalf("band census emb=%d sep=%d, want 1/0", st.Embedded, st.Separated)
	}
}
