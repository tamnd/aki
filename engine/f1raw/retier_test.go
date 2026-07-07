package f1raw

import (
	"fmt"
	"path/filepath"
	"testing"
)

// newSetRecStore builds an in-memory store with a cold record region and admits kindSetMember to
// the vector-member (Option A) migration path, so MigrateToCold repairs the dense member vector's
// cached offset in place rather than treating the row as a plain string flip. It is the set analogue
// of newRecStore.
func newSetRecStore(t *testing.T) *Store {
	t.Helper()
	s := New(1<<12, 1<<20)
	if err := s.EnableColdRecords(filepath.Join(t.TempDir(), "recs.log")); err != nil {
		t.Fatalf("EnableColdRecords: %v", err)
	}
	s.SetVecMemberKindFunc(func(k byte) bool { return k == tKindSetMember })
	t.Cleanup(func() { s.Close() })
	return s
}

// TestColdSetMemberRetierDrawAndScan is the D22 Option A gate (spec 2064/f1_rewrite_ltm/22 M1): a set
// whose member rows have partly migrated to the cold record region must draw and enumerate every
// member, with the migrated ones resolving through keyAtTiered's cold branch and the resident ones
// staying zero-copy. The dense member vector caches raw arena offsets with no keys, so MigrateToCold's
// vec-member branch must retier the cached slot as the record moves; if it did not, the vector slot
// would hold a stale resident offset that a draw would read past a reclaimed segment. Driving the flip
// by hand with MigrateToCold proves the one hand-mover retiers correctly before the background mover
// (M2) shares the same helper.
func TestColdSetMemberRetierDrawAndScan(t *testing.T) {
	s := newSetRecStore(t)
	const n = 24
	const skey = "s"
	prefix := setPrefixBytes(skey)
	s.CollRandEnsure(prefix)

	members := make([]string, n)
	want := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("m%02d", i)
		members[i] = m
		want[string(memberKeyBytes(skey, m))] = true
		mk := memberKeyBytes(skey, m)
		if _, err := s.PutKind(mk, nil, tKindSetMember); err != nil {
			t.Fatalf("PutKind(%q): %v", m, err)
		}
		s.CollRandInsert(prefix, mk, tKindSetMember)
	}

	// Migrate every third member cold; the rest stay resident. The draw and the scan must read both.
	migrated := make(map[string]bool)
	for i := 0; i < n; i += 3 {
		mk := memberKeyBytes(skey, members[i])
		if !s.MigrateToCold(mk, tKindSetMember) {
			t.Fatalf("MigrateToCold member %d returned false", i)
		}
		if !s.entryIsCold(t, mk, tKindSetMember) {
			t.Fatalf("member %d index entry is not tagged cold after migration", i)
		}
		migrated[string(mk)] = true
	}

	// The vector still holds n slots and the migrated members' slots now carry cold (tier-bit) addresses
	// rather than the reclaimed resident offset. This is the retier the draw depends on.
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	v := sh.get(prefix)
	if v == nil {
		sh.mu.Unlock()
		t.Fatal("no member vector after building the set")
	}
	if len(v.slots) != n {
		sh.mu.Unlock()
		t.Fatalf("vector holds %d slots, want %d", len(v.slots), n)
	}
	coldSlots := 0
	for _, off := range v.slots {
		if off&tierBit != 0 {
			coldSlots++
		}
	}
	sh.mu.Unlock()
	if coldSlots != len(migrated) {
		t.Fatalf("vector has %d cold slots, want %d migrated", coldSlots, len(migrated))
	}

	// Enumerate the whole set top-down: every member must surface exactly once by its composite key,
	// the migrated ones resolved through keyAtTiered's cold pread and the resident ones zero-copy.
	keys, _ := s.SetVecScanDown(prefix, -1, n, nil)
	if len(keys) != n {
		t.Fatalf("SetVecScanDown returned %d keys, want %d", len(keys), n)
	}
	seen := make(map[string]bool, n)
	sawCold := false
	for _, k := range keys {
		ks := string(k)
		if !want[ks] {
			t.Fatalf("enumeration returned unexpected key %q", k)
		}
		if seen[ks] {
			t.Fatalf("enumeration returned key %q twice", k)
		}
		seen[ks] = true
		if migrated[ks] {
			sawCold = true
		}
	}
	if len(seen) != n {
		t.Fatalf("enumeration covered %d distinct members, want %d", len(seen), n)
	}
	if !sawCold {
		t.Fatal("no enumerated member resolved to a migrated (cold) row; the retier did not engage")
	}

	// Draw enough times to hit every slot with high probability; every draw must land on a live member,
	// including the cold ones the vector now points at through their tier-tagged address.
	var rng testRNG
	drewCold := false
	for i := 0; i < n*40; i++ {
		k, ok := s.CollRandSelect(prefix, rng.next())
		if !ok {
			t.Fatal("CollRandSelect reported empty on a non-empty set")
		}
		ks := string(k)
		if !want[ks] {
			t.Fatalf("CollRandSelect drew a non-member key %q", k)
		}
		if migrated[ks] {
			drewCold = true
		}
	}
	if !drewCold {
		t.Fatal("no random draw landed on a migrated (cold) member across many draws; the cold slots are unreachable")
	}
}
