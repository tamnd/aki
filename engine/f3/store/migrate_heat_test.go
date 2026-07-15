package store

import (
	"fmt"
	"path/filepath"
	"testing"
)

// slotVisitedFor reports whether the live entry for key carries the index-word
// visited bit, the test's window on the SIEVE mark the migrator honors.
func (s *Store) slotVisitedFor(key []byte) bool {
	slot, _, _ := s.findEntry(Hash(key), key)
	return slot != nil && slotVisited(*slot)
}

// TestTouchSlotGatedOnLTM is the L9 guard for the read path: a resident read
// sets the visited bit only when LTM is engaged. With no resident cap the tier
// is compiled in but off, so the read must not touch the index word.
func TestTouchSlotGatedOnLTM(t *testing.T) {
	// LTM off: a vlog but no cap, so ltmOn is false and the read leaves the word
	// clean, the byte-identical M0 path.
	off, err := Open(Options{
		ArenaBytes: 16 << 20,
		SegBytes:   256 << 10,
		VlogPath:   filepath.Join(t.TempDir(), "vlog"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = off.Close() })
	if err := off.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	var dst []byte
	if _, ok := off.GetString([]byte("k"), 0, dst); !ok {
		t.Fatal("key missing with LTM off")
	}
	if off.slotVisitedFor([]byte("k")) {
		t.Fatal("read set the visited bit with LTM off")
	}

	// LTM on: a resident cap engages the tier, so the same read sets the bit.
	on := migratorStore(t, 8<<20)
	if err := on.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if on.slotVisitedFor([]byte("k")) {
		t.Fatal("fresh write left the visited bit set")
	}
	if _, ok := on.GetString([]byte("k"), 0, dst); !ok {
		t.Fatal("key missing with LTM on")
	}
	if !on.slotVisitedFor([]byte("k")) {
		t.Fatal("read did not set the visited bit with LTM on")
	}
}

// TestMigrateSecondChanceSparesReadRecords is the SIEVE mechanism in unit form:
// with every resident record read once (the visited bit set on all), one
// migration pass demotes nothing, because a visited record earns a second
// chance and only its bit is cleared. A second pass with no reads in between
// finds the bits spent and sinks records, so the reprieve is exactly one pass.
func TestMigrateSecondChanceSparesReadRecords(t *testing.T) {
	const cap = 1 << 20
	s := migratorStore(t, cap)
	const n = 40000
	fillSmall(t, s, n)
	if s.arena.live() <= cap {
		t.Fatalf("fixture did not cross the cap: live=%d cap=%d", s.arena.live(), cap)
	}

	// Read every key: the whole resident set now carries the visited bit.
	var dst []byte
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		if _, ok := s.GetString(k, 0, dst); !ok {
			t.Fatalf("key %d missing before migration", i)
		}
	}

	// One pass over an all-visited set demotes nothing: every record spends its
	// second chance instead of sinking.
	if moved := s.MigrateCold(); moved != 0 {
		t.Fatalf("first pass demoted %d records with every slot visited, want 0", moved)
	}
	if s.Cold().Records != 0 {
		t.Fatalf("cold records = %d after an all-visited pass, want 0", s.Cold().Records)
	}

	// The reprieve is spent: the same pressure with no reads in between now sinks
	// records, because the bits the first pass cleared are not re-earned.
	if moved := s.MigrateCold(); moved == 0 {
		t.Fatal("second pass demoted nothing; the reprieve was not spent")
	}
	if s.Cold().Records == 0 {
		t.Fatal("nothing went cold on the second pass")
	}
	assertCensus(t, s)
}

// TestMigrateSecondChanceKeepsReReadHot is the product claim the bit exists for:
// a working set re-read before every drain pass stays resident while the cold
// tail sinks, so a read-hot write-cold record is never demoted only to pay a
// pread on its next read. The hot keys keep re-earning their reprieve; the rest
// have none and sink until the arena reaches the low-water.
func TestMigrateSecondChanceKeepsReReadHot(t *testing.T) {
	const cap = 1 << 20
	s := migratorStore(t, cap)
	const n = 40000
	fillSmall(t, s, n)

	// A small working set re-read before each drain pass.
	const hot = 100
	var dst []byte
	converged := false
	for pass := 0; pass < 128; pass++ {
		for i := 0; i < hot; i++ {
			k := fmt.Appendf(nil, "k:%07d", i)
			if _, ok := s.GetString(k, 0, dst); !ok {
				t.Fatalf("hot key %d missing", i)
			}
		}
		moved := s.MigrateCold()
		s.CompactArena()
		if moved == 0 {
			converged = true
			break
		}
	}
	if !converged {
		t.Fatal("migration did not converge")
	}
	if s.Cold().Records == 0 {
		t.Fatal("the cold tail never sank")
	}
	// Every re-read key stayed resident and answers from the arena.
	for i := 0; i < hot; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		if s.slotIsCold(k) {
			t.Fatalf("re-read hot key %d was demoted", i)
		}
		dst, ok := s.GetString(k, 0, dst)
		if !ok || string(dst) != wantVal(i) {
			t.Fatalf("hot key %d = %q,%v, want %q", i, dst, ok, wantVal(i))
		}
	}
	assertCensus(t, s)
}
