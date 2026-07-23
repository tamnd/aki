package store

import (
	"fmt"
	"path/filepath"
	"testing"
)

// coldStore opens a store with a cold region so the migrator has somewhere to
// demote. The arena is roomy and the resident cap is off: this slice's demotion
// is driven by DemoteCold, not the residency hand, so the two do not interact.
func coldStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(Options{
		ArenaBytes: 16 << 20,
		SegBytes:   256 << 10,
		VlogPath:   filepath.Join(dir, "vlog"),
		ColdPath:   filepath.Join(dir, "cold"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// slotIsCold reports whether the live entry for key resolves to a cold-tier
// slot, the tier bit the resolver reads. It is the test's window on the index
// word without exporting one.
func (s *Store) slotIsCold(key []byte) bool {
	slot, _, _ := s.findEntry(Hash(key), key)
	return slot != nil && slotCold(*slot)
}

// assertCensus pins the band census invariant: every live key is either in one
// of the four resident bands or in the cold count, and nothing is double
// counted. A demotion moves a key from its band to the cold count, a bring-up
// moves it back, so the sum tracks the key count through both.
func assertCensus(t *testing.T, s *Store) {
	t.Helper()
	b := s.Stats()
	sum := b.Int + b.Embedded + b.Separated + b.Chunked + s.Cold().Records
	if sum != uint64(s.Len()) {
		t.Fatalf("census drift: bands+cold=%d, keys=%d (int=%d emb=%d sep=%d chunk=%d cold=%d)",
			sum, s.Len(), b.Int, b.Embedded, b.Separated, b.Chunked, s.Cold().Records)
	}
}

// TestColdDemoteReadsBack demotes int and embedded string keys, then reads each
// one back straight from its frame: the slot flips to cold, GetString and StrLen
// answer from the pread, and the key still exists. A cold read leaves the key
// cold, so the record count in the cold tier does not move on read.
func TestColdDemoteReadsBack(t *testing.T) {
	s := coldStore(t)
	cases := map[string]string{
		"int:small":  "42",
		"int:neg":    "-1000000",
		"emb:hello":  "hello world",
		"emb:binary": string([]byte{0, 1, 2, 3, 0xff, 0x00}),
	}
	for k, v := range cases {
		if err := s.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}
	if got := s.DemoteCold(); got != len(cases) {
		t.Fatalf("DemoteCold moved %d, want %d", got, len(cases))
	}
	if s.Cold().Records != uint64(len(cases)) {
		t.Fatalf("cold records = %d, want %d", s.Cold().Records, len(cases))
	}
	assertCensus(t, s)

	var dst []byte
	for k, v := range cases {
		if !s.slotIsCold([]byte(k)) {
			t.Fatalf("%q did not flip to cold", k)
		}
		if !s.Exists([]byte(k), 0) {
			t.Fatalf("%q missing after demote", k)
		}
		dst, _ = s.GetString([]byte(k), 0, dst)
		if string(dst) != v {
			t.Fatalf("cold read %q = %q, want %q", k, dst, v)
		}
		if n, ok := s.StrLen([]byte(k), 0); !ok || n != int64(len(v)) {
			t.Fatalf("cold strlen %q = %d,%v, want %d", k, n, ok, len(v))
		}
		// A read keeps a cold key cold.
		if !s.slotIsCold([]byte(k)) {
			t.Fatalf("%q left cold on read", k)
		}
	}
	if s.Cold().Records != uint64(len(cases)) {
		t.Fatalf("reads changed cold count to %d", s.Cold().Records)
	}
}

// TestColdBringUpOnWrite pins doc 06 section 7.3: a write to a cold key brings
// it back to the arena unconditionally, whatever the mutator. Each sub-case
// demotes one key, mutates it, and checks the slot is resident again, the value
// is the mutation's result, and the cold count fell by one.
func TestColdBringUpOnWrite(t *testing.T) {
	writes := []struct {
		name  string
		seed  string
		apply func(s *Store, k []byte)
		want  string
	}{
		{"set", "old", func(s *Store, k []byte) { _ = s.Set(k, []byte("new")) }, "new"},
		{"incr", "10", func(s *Store, k []byte) {
			if _, err := s.IncrBy(k, 5, 0); err != nil {
				t.Fatalf("incr: %v", err)
			}
		}, "15"},
		{"append", "ab", func(s *Store, k []byte) {
			if _, err := s.Append(k, []byte("cd"), 0); err != nil {
				t.Fatalf("append: %v", err)
			}
		}, "abcd"},
		{"setrange", "hello", func(s *Store, k []byte) {
			if _, err := s.SetRange(k, 1, []byte("xy"), 0); err != nil {
				t.Fatalf("setrange: %v", err)
			}
		}, "hxylo"},
	}
	for _, w := range writes {
		t.Run(w.name, func(t *testing.T) {
			s := coldStore(t)
			k := []byte("k")
			if err := s.Set(k, []byte(w.seed)); err != nil {
				t.Fatal(err)
			}
			if !s.DemoteKey(k) {
				t.Fatal("DemoteKey did not move the key")
			}
			if !s.slotIsCold(k) {
				t.Fatal("key not cold after demote")
			}
			w.apply(s, k)
			if s.slotIsCold(k) {
				t.Fatal("key stayed cold after a write")
			}
			if s.Cold().Records != 0 {
				t.Fatalf("cold count = %d after bring-up, want 0", s.Cold().Records)
			}
			var dst []byte
			dst, ok := s.GetString(k, 0, dst)
			if !ok || string(dst) != w.want {
				t.Fatalf("after %s: %q, want %q", w.name, dst, w.want)
			}
			assertCensus(t, s)
		})
	}
}

// TestColdDel deletes a cold key: it reports present, the key vanishes, and the
// live count and the cold count both fall by one with no arena drop (the frame
// is simply unreferenced).
func TestColdDel(t *testing.T) {
	s := coldStore(t)
	k := []byte("gone")
	if err := s.Set(k, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if !s.DemoteKey(k) {
		t.Fatal("demote failed")
	}
	before := s.Len()
	if !s.Del(k, 0) {
		t.Fatal("Del of a cold key reported absent")
	}
	if s.Exists(k, 0) {
		t.Fatal("key still present after Del")
	}
	if s.Len() != before-1 {
		t.Fatalf("count = %d, want %d", s.Len(), before-1)
	}
	if s.Cold().Records != 0 {
		t.Fatalf("cold count = %d after Del, want 0", s.Cold().Records)
	}
	assertCensus(t, s)
}

// TestColdDemoteSkipsIneligible checks the demotable gate: a separated-band
// value keeps its arena residency because its run would dangle, while a TTL
// key demotes with its deadline in the frame's trailing expiry word and the
// plain key demotes as always.
func TestColdDemoteSkipsIneligible(t *testing.T) {
	s := coldStore(t)
	if err := s.Set([]byte("plain"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetString([]byte("ttl"), []byte("y"), 1, 1<<40, false); err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 4<<10) // past strInlineMax, separated band
	for i := range big {
		big[i] = 'z'
	}
	if err := s.Set([]byte("sep"), big); err != nil {
		t.Fatal(err)
	}
	if got := s.DemoteCold(); got != 2 {
		t.Fatalf("DemoteCold moved %d, want 2 (the plain and TTL keys)", got)
	}
	if !s.slotIsCold([]byte("ttl")) {
		t.Fatal("the TTL key was not demoted")
	}
	if s.slotIsCold([]byte("sep")) {
		t.Fatal("a separated key was demoted")
	}
	if !s.slotIsCold([]byte("plain")) {
		t.Fatal("the plain key was not demoted")
	}
	if v, ok := s.GetString([]byte("ttl"), 2, nil); !ok || string(v) != "y" {
		t.Fatalf("cold TTL key before its deadline read %q %v, want y", v, ok)
	}
	assertCensus(t, s)
}

// TestColdSurvivesSplit demotes a wide key set, then forces the index to split
// under new inserts. A split redistributes cold entries through the frame key
// (entryKeyHash never touches the arena for them), so every cold key must still
// resolve to its value on the far side of the splits.
func TestColdSurvivesSplit(t *testing.T) {
	s := coldStore(t)
	const n = 4000
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "cold:%06d", i)
		v := fmt.Appendf(nil, "val-%d", i)
		if err := s.Set(k, v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if got := s.DemoteCold(); got != n {
		t.Fatalf("demoted %d, want %d", got, n)
	}
	splitsBefore := s.Splits()
	// New resident keys drive more splits with cold entries already in place.
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "hot:%06d", i)
		if err := s.Set(k, []byte("h")); err != nil {
			t.Fatalf("hot set %d: %v", i, err)
		}
	}
	if s.Splits() == splitsBefore {
		t.Fatal("no splits ran; the fixture proves nothing")
	}
	var dst []byte
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "cold:%06d", i)
		want := fmt.Sprintf("val-%d", i)
		if !s.slotIsCold(k) {
			t.Fatalf("%s lost its cold tier across a split", k)
		}
		dst, ok := s.GetString(k, 0, dst)
		if !ok || string(dst) != want {
			t.Fatalf("cold %s = %q,%v after split, want %q", k, dst, ok, want)
		}
	}
	assertCensus(t, s)
}

// TestColdNoRegionNoOp confirms a store with no cold region treats demotion as a
// no-op rather than a fault: DemoteCold moves nothing and DemoteKey reports
// false, so the tier check is inert until a region is configured.
func TestColdNoRegionNoOp(t *testing.T) {
	s := New(16<<20, 0)
	if err := s.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if got := s.DemoteCold(); got != 0 {
		t.Fatalf("DemoteCold without a region moved %d", got)
	}
	if s.DemoteKey([]byte("k")) {
		t.Fatal("DemoteKey without a region reported a move")
	}
	var dst []byte
	dst, ok := s.GetString([]byte("k"), 0, dst)
	if !ok || string(dst) != "v" {
		t.Fatalf("value corrupted: %q,%v", dst, ok)
	}
}
