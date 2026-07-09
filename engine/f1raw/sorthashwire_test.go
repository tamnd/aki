package f1raw

import (
	"fmt"
	"sort"
	"testing"
)

// This file proves the fold facility maintains a correct sorted array when it is driven by the real
// set write primitives (CollRandInsert/CollRandRemove/CollPartRandInsert), not a synthetic delta
// stream. sorthashfold_test.go tests the folder against hand-built deltas; here the deltas come from
// the same PutKind + CollRandInsert path the server's SADD runs, so the test also pins the two
// wiring invariants the merge depends on: the sorted array holds the hash of the member bytes alone
// (so the same member in two different sets aligns under the two-pointer merge), and an insert and a
// later remove of the same member net out.

// liveMembers resolves the arena offset of each named member of a set through the authoritative hash
// index, the same offset the write path records, so a test can predict the sorted (hash, off) array
// the fold should publish. It returns the members that are actually present, keyed by member string.
func liveMembers(t *testing.T, s *Store, skey string, members []string) map[string]uint64 {
	t.Helper()
	live := map[string]uint64{}
	for _, m := range members {
		mk := memberKeyBytes(skey, m)
		off, _, _, _, found := s.find(mk, hash(mk), tKindSetMember)
		if found {
			live[m] = off
		}
	}
	return live
}

// wantWireSnap builds the ascending (hash, off) arrays the fold should converge to for a set, using
// hash over the member bytes alone (the wiring invariant) and the offsets liveMembers resolved.
func wantWireSnap(live map[string]uint64) (h, off []uint64) {
	type pair struct {
		h, off uint64
	}
	ps := make([]pair, 0, len(live))
	for m, o := range live {
		ps = append(ps, pair{h: hash([]byte(m)), off: o})
	}
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].h != ps[j].h {
			return ps[i].h < ps[j].h
		}
		return ps[i].off < ps[j].off
	})
	h = make([]uint64, len(ps))
	off = make([]uint64, len(ps))
	for i, p := range ps {
		h[i] = p.h
		off[i] = p.off
	}
	return h, off
}

// TestWireSAddBuildsSortedArray drives a set through the real add primitive with the folder on and
// asserts the published snapshot is the members in member-hash order, keyed on the member bytes and
// not the composite key. It is the wiring's definition of done: a SADD maintains the sorted array.
func TestWireSAddBuildsSortedArray(t *testing.T) {
	s := New(1024, 1<<20)
	s.EnableSortedHashFold()
	defer s.Close()

	prefix := setPrefixBytes("s")
	members := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		members = append(members, fmt.Sprintf("m%04d", i))
	}
	for _, m := range members {
		addMember(t, s, prefix, "s", m)
	}

	s.SyncSortedHashes()
	if !s.SortedHashCurrent(prefix) {
		t.Fatal("not current after add + sync")
	}
	snap := s.SortedHashSnapshot(prefix)
	if snap == nil {
		t.Fatal("no snapshot after add")
	}
	wh, wo := wantWireSnap(liveMembers(t, s, "s", members))
	if !equalU64(snap.h, wh) || !equalU64(snap.off, wo) {
		t.Fatalf("sorted array diverged: got %d entries, want %d", len(snap.h), len(wh))
	}
}

// TestWireSRemNetsOut adds a set, removes half through the real remove primitive, and asserts the
// published snapshot reflects only the survivors. It pins that a remove journals the same offset the
// add recorded, so the fold drops exactly the removed member's entry.
func TestWireSRemNetsOut(t *testing.T) {
	s := New(1024, 1<<20)
	s.EnableSortedHashFold()
	defer s.Close()

	prefix := setPrefixBytes("s")
	members := make([]string, 0, 120)
	for i := 0; i < 120; i++ {
		members = append(members, fmt.Sprintf("k%03d", i))
	}
	for _, m := range members {
		addMember(t, s, prefix, "s", m)
	}
	survivors := make([]string, 0, len(members))
	for i, m := range members {
		if i%2 == 0 {
			removeMember(t, s, prefix, "s", m)
		} else {
			survivors = append(survivors, m)
		}
	}

	s.SyncSortedHashes()
	snap := s.SortedHashSnapshot(prefix)
	wh, wo := wantWireSnap(liveMembers(t, s, "s", survivors))
	if !equalU64(snap.h, wh) || !equalU64(snap.off, wo) {
		t.Fatalf("after remove diverged: got %d entries, want %d", len(snap.h), len(wh))
	}
}

// TestWireIntersectAcrossSets is the end-to-end slice 1a+1b check: two sets built through the real
// add path share some members, and the two-pointer merge over their published snapshots yields
// exactly the shared members. It only works because each set hashes its members identically, the
// wiring invariant, so a shared member lands at the same hash key in both sorted arrays.
func TestWireIntersectAcrossSets(t *testing.T) {
	s := New(1024, 1<<20)
	s.EnableSortedHashFold()
	defer s.Close()

	prefixA := setPrefixBytes("A")
	// setPrefixBytes reuses one buffer, so snapshot the two prefixes into their own storage before
	// building the second set, or the first prefix would be overwritten under the second's bytes.
	pa := append([]byte(nil), prefixA...)
	prefixB := setPrefixBytes("B")
	pb := append([]byte(nil), prefixB...)

	var aMembers, bMembers []string
	shared := map[string]uint64{}
	for i := 0; i < 300; i++ {
		m := fmt.Sprintf("x%04d", i)
		if i%2 == 0 || i%3 == 0 {
			aMembers = append(aMembers, m)
		}
		if i%3 == 0 || i%5 == 0 {
			bMembers = append(bMembers, m)
		}
		if (i%2 == 0 || i%3 == 0) && (i%3 == 0 || i%5 == 0) {
			shared[m] = 0
		}
	}
	for _, m := range aMembers {
		addMember(t, s, pa, "A", m)
	}
	for _, m := range bMembers {
		addMember(t, s, pb, "B", m)
	}

	s.SyncSortedHashes()
	snapA := s.SortedHashSnapshot(pa)
	snapB := s.SortedHashSnapshot(pb)

	// Confirm two offsets name the same member by comparing their composite key tails, exactly the
	// byte-confirm the real merge uses to reject a bare hash collision.
	confirm := func(offA, offB uint64) bool {
		ka := s.keyAtTiered(offA, nil)
		kb := s.keyAtTiered(offB, nil)
		ma := ka[len(pa):]
		mb := kb[len(pb):]
		return string(ma) == string(mb)
	}
	got := map[string]struct{}{}
	intersectEmit(snapA, snapB, confirm, func(offA uint64) {
		k := s.keyAtTiered(offA, nil)
		got[string(k[len(pa):])] = struct{}{}
	})

	if len(got) != len(shared) {
		t.Fatalf("intersect size %d, want %d", len(got), len(shared))
	}
	for m := range shared {
		if _, ok := got[m]; !ok {
			t.Fatalf("intersect missing shared member %q", m)
		}
	}
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
