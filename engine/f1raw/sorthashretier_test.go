package f1raw

import (
	"fmt"
	"path/filepath"
	"testing"
)

// This file is the slice 4d gate (spec 2064/f1_rewrite_ltm/24): the per-partition sorted-hash arrays
// must survive a member's cold migration the same way the dense member vector does. Before slice 4d the
// migrator repaired the vector's cached offset in place but left the sorted array holding the stale
// resident offset, so SortedHashMergeStable reported false on the segmented arena and the driver stayed
// on the probe. Now flipVecMember journals a remove(oldOff)+add(newAddr) retier through the fold
// facility beside the vector's own retier, so after a sync the sorted entry names the cold tier-tagged
// address and the merge resolves it through keyAtTiered's cold branch.

// newFoldSetRecStore builds a cold-record store that admits kindSetMember to the Option A vector
// migration path with the sorted-hash folder running, so a hand MigrateToCold flips the vector and
// journals the sorted array's retier through the same flipVecMember the background migrator uses.
func newFoldSetRecStore(t *testing.T) *Store {
	t.Helper()
	s := New(1<<12, 1<<20)
	if err := s.EnableColdRecords(filepath.Join(t.TempDir(), "recs.log")); err != nil {
		t.Fatalf("EnableColdRecords: %v", err)
	}
	s.SetVecMemberKindFunc(func(k byte) bool { return k == tKindSetMember })
	s.EnableSortedHashFold()
	t.Cleanup(func() { s.Close() })
	return s
}

// sortedEntryFor finds the sorted-array entry for member m in the partition at prefix and returns its
// recorded offset, or false when the member has no entry. It resolves each candidate offset to its
// member bytes with keyAtTiered so a bare hash collision is not mistaken for the member, exactly as the
// merge's byte-confirm does.
func sortedEntryFor(s *Store, prefix []byte, m string) (uint64, bool) {
	snap := s.SortedHashSnapshot(prefix)
	if snap == nil {
		return 0, false
	}
	mh := hash([]byte(m))
	for i := range snap.h {
		if snap.h[i] != mh {
			continue
		}
		k := s.keyAtTiered(snap.off[i], nil)
		if len(k) >= len(prefix) && string(k[len(prefix):]) == m {
			return snap.off[i], true
		}
	}
	return 0, false
}

// TestSortedRetierUnderHandMigration proves flipVecMember rewrites the sorted array's cached offset to
// the cold address as a member migrates: it builds two sets that share members through the real add
// path with the folder on, hand-migrates some shared members cold, and asserts each migrated member's
// sorted entry now carries the tier bit (so the retier journaled and the folder rebuilt it) while the
// two-pointer merge still yields exactly the shared members, the migrated ones resolved through the
// cold branch.
func TestSortedRetierUnderHandMigration(t *testing.T) {
	s := newFoldSetRecStore(t)

	pa := append([]byte(nil), setPrefixBytes("A")...)
	pb := append([]byte(nil), setPrefixBytes("B")...)

	var aMembers, bMembers []string
	shared := map[string]bool{}
	for i := range 240 {
		m := fmt.Sprintf("x%04d", i)
		inA := i%2 == 0 || i%3 == 0
		inB := i%3 == 0 || i%5 == 0
		if inA {
			aMembers = append(aMembers, m)
		}
		if inB {
			bMembers = append(bMembers, m)
		}
		if inA && inB {
			shared[m] = true
		}
	}
	for _, m := range aMembers {
		addMember(t, s, pa, "A", m)
	}
	for _, m := range bMembers {
		addMember(t, s, pb, "B", m)
	}

	// Migrate a slice of the shared members of both sets cold by hand. Each MigrateToCold flows through
	// migrateVecMember -> flipVecMember, which retiers the vector slot and journals the sorted array's
	// remove(oldOff)+add(coldAddr) under the same shard mutex.
	migrated := map[string]bool{}
	i := 0
	for m := range shared {
		if i%2 == 0 {
			if !s.MigrateToCold(memberKeyBytes("A", m), tKindSetMember) {
				t.Fatalf("MigrateToCold A/%s returned false", m)
			}
			if !s.MigrateToCold(memberKeyBytes("B", m), tKindSetMember) {
				t.Fatalf("MigrateToCold B/%s returned false", m)
			}
			migrated[m] = true
		}
		i++
	}
	if len(migrated) == 0 {
		t.Fatal("test built no migrated members")
	}

	s.SyncSortedHashes()
	if !s.SortedHashCurrent(pa) || !s.SortedHashCurrent(pb) {
		t.Fatal("a partition is not current after migration + sync; the retier journal never folded")
	}

	// Every migrated member's sorted entry in both arrays must now be the cold tier-tagged address, not
	// the stale resident offset. Without the slice 4d retier the entry would keep the resident offset and
	// this tier-bit check would fail.
	for m := range migrated {
		for _, pfx := range [][]byte{pa, pb} {
			off, ok := sortedEntryFor(s, pfx, m)
			if !ok {
				t.Fatalf("migrated member %q missing from its sorted array", m)
			}
			if off&tierBit == 0 {
				t.Fatalf("migrated member %q sorted entry off=%#x is not cold; the retier did not rewrite it", m, off)
			}
		}
	}

	// A member that stayed resident keeps a resident (no tier bit) sorted entry.
	for _, m := range aMembers {
		if migrated[m] || !shared[m] {
			continue
		}
		off, ok := sortedEntryFor(s, pa, m)
		if !ok {
			t.Fatalf("resident shared member %q missing from A's sorted array", m)
		}
		if off&tierBit != 0 {
			t.Fatalf("resident member %q sorted entry off=%#x is unexpectedly cold", m, off)
		}
		break
	}

	// The merge over the two arrays must still be exactly the shared set, with the migrated members
	// resolved through keyAtTiered's cold pread.
	snapA := s.SortedHashSnapshot(pa)
	snapB := s.SortedHashSnapshot(pb)
	confirm := s.sortedMergeConfirm(len(pa), len(pb))
	got := map[string]struct{}{}
	sawCold := false
	intersectEmit(snapA, snapB, confirm, func(offA uint64) {
		k := s.keyAtTiered(offA, nil)
		mm := string(k[len(pa):])
		got[mm] = struct{}{}
		if offA&tierBit != 0 {
			sawCold = true
		}
	})
	if len(got) != len(shared) {
		t.Fatalf("merge size %d, want %d", len(got), len(shared))
	}
	for m := range shared {
		if _, ok := got[m]; !ok {
			t.Fatalf("merge missing shared member %q", m)
		}
	}
	if !sawCold {
		t.Fatal("no merged member resolved through the cold address; the migrated members were not read cold")
	}
}

// TestShRetierPeekSkipsUnfoldedPartition pins the peek discipline: shRetier journals only for a
// partition the fold facility has already materialized. A prefix the algebra path never touched has no
// sorted array to repair, so shRetier must be a no-op that neither panics nor creates fold state, which
// is what keeps a migration of a never-merged set off the fold machinery.
func TestShRetierPeekSkipsUnfoldedPartition(t *testing.T) {
	s := New(1<<12, 1<<20)
	s.EnableSortedHashFold()
	defer s.Close()

	prefix := setPrefixBytes("never-merged")
	if s.shReg.partIf(prefix) != nil {
		t.Fatal("partIf returned state for a prefix that was never journaled")
	}
	// A retier against it journals nothing and creates no state.
	s.shRetier(prefix, hash([]byte("m")), 4096, 8192|tierBit)
	if s.shReg.partIf(prefix) != nil {
		t.Fatal("shRetier created fold state for an unfolded partition")
	}
}
