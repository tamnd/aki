package f1raw

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// packKeys packs composite keys end to end the way the coalesced delete drain does, returning
// the buffer and the cumulative-length ends slice CollRemovePacked and removeManyLive consume.
func packKeys(keys ...[]byte) ([]byte, []int) {
	var buf []byte
	ends := make([]int, 0, len(keys))
	for _, k := range keys {
		buf = append(buf, k...)
		ends = append(ends, len(buf))
	}
	return buf, ends
}

// waitTombDrained spins until the folder has spliced every queued tombstone, so a test can
// assert the post-drain structure without racing the background goroutine. It fails the test
// rather than hanging if the folder never catches up.
func waitTombDrained(t *testing.T, s *Store) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.tombPend.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("folder never drained: tombPend still %d", s.tombPend.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// A deferred removal must be invisible to enumeration the instant it is queued (the liveness
// filter hides the dead node before the folder runs), and the folder must then splice the node
// so the order-statistic index agrees with the surviving sorted members.
func TestDeferredRemovalHidesThenSplices(t *testing.T) {
	s := New(1<<16, 1<<20)
	s.EnableDeferredRemoval()
	defer s.Close()

	insert := func(member string) {
		k := collKey("h", member)
		if _, err := s.PutKind(k, []byte("v"), kindTestField); err != nil {
			t.Fatal(err)
		}
		s.CollInsert(k, kindTestField)
	}
	for _, m := range []string{"a", "b", "c", "d"} {
		insert(m)
	}

	// Delete b through the deferred path: drop the hash record, queue the ordered-index splice.
	bk := collKey("h", "b")
	if !s.DeleteKind(bk, kindTestField) {
		t.Fatal("DeleteKind(b) reported absent")
	}
	buf, ends := packKeys(bk)
	s.CollRemovePacked(buf, ends, kindTestField)

	prefix := collPrefix("h")
	// Even if the folder has not run yet, enumeration must already exclude b.
	got := scanAll(s, prefix, 2)
	want := [][]byte{collKey("h", "a"), collKey("h", "c"), collKey("h", "d")}
	if len(got) != len(want) {
		t.Fatalf("immediate scan got %d keys, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("immediate scan key %d: got %q want %q", i, got[i], want[i])
		}
	}

	// After the folder splices, rank-based select must agree with the survivors in order.
	waitTombDrained(t, s)
	wantMembers := []string{"a", "c", "d"}
	for i, w := range wantMembers {
		k, ok := s.CollSelectAt(prefix, i)
		if !ok || string(k[len(prefix):]) != w {
			t.Fatalf("CollSelectAt(%d) = %q,%v want %q", i, k[len(prefix):], ok, w)
		}
	}
	if _, ok := s.CollSelectAt(prefix, len(wantMembers)); ok {
		t.Fatalf("CollSelectAt(%d) present past cardinality", len(wantMembers))
	}
}

// The re-add hazard: a member is deleted (its splice queued with the node still present), then
// added back before the folder runs. The folder's under-lock liveness re-check must see the
// fresh record and keep the node, so the re-added member survives. Drive removeManyLive directly
// so the interleaving is deterministic rather than racing the background goroutine.
func TestDeferredRemovalReAddKeepsNode(t *testing.T) {
	s := New(1<<16, 1<<20)

	put := func(member, val string) {
		k := collKey("h", member)
		created, err := s.PutKind(k, []byte(val), kindTestField)
		if err != nil {
			t.Fatal(err)
		}
		if created {
			s.CollInsert(k, kindTestField)
		}
	}
	put("a", "1")
	put("b", "2")
	put("c", "3")

	bk := collKey("h", "b")
	// Delete b from the hash index; the ordered-index node stays, its splice deferred.
	if !s.DeleteKind(bk, kindTestField) {
		t.Fatal("DeleteKind(b) reported absent")
	}
	// Re-add b with a fresh value before the deferred splice runs. PutKind republishes the
	// record and CollInsert's same-key branch refreshes the existing node to it.
	put("b", "22")

	// Now the folder runs: removeManyLive must skip b because its record is live again.
	buf, ends := packKeys(bk)
	s.oidx.removeManyLive(buf, ends, kindTestField)

	prefix := collPrefix("h")
	got := scanAll(s, prefix, 8)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %d members after re-add, want %d: %q", len(got), len(want), got)
	}
	for i, w := range want {
		if !bytes.Equal(got[i], collKey("h", w)) {
			t.Fatalf("member %d: got %q want %q", i, got[i], collKey("h", w))
		}
	}
	// b must resolve to the re-added value, not the deleted one.
	v, ok := s.GetKind(bk, nil, kindTestField)
	if !ok || string(v) != "22" {
		t.Fatalf("b resolved to %q,%v want 22", v, ok)
	}
}

// A genuinely dead key (deleted and not re-added) must be spliced by removeManyLive, so the
// node is gone and the order-statistic count drops. This is the common case the re-add check
// must not suppress.
func TestDeferredRemovalDeadKeySpliced(t *testing.T) {
	s := New(1<<16, 1<<20)
	put := func(member string) {
		k := collKey("h", member)
		if _, err := s.PutKind(k, []byte("v"), kindTestField); err != nil {
			t.Fatal(err)
		}
		s.CollInsert(k, kindTestField)
	}
	put("a")
	put("b")
	put("c")

	bk := collKey("h", "b")
	if !s.DeleteKind(bk, kindTestField) {
		t.Fatal("DeleteKind(b) reported absent")
	}
	buf, ends := packKeys(bk)
	s.oidx.removeManyLive(buf, ends, kindTestField)

	prefix := collPrefix("h")
	got := scanAll(s, prefix, 8)
	if len(got) != 2 || !bytes.Equal(got[0], collKey("h", "a")) || !bytes.Equal(got[1], collKey("h", "c")) {
		t.Fatalf("survivors after splice: %q, want a,c", got)
	}
	// The node is unlinked, so selection sees only two members.
	if _, ok := s.CollSelectAt(prefix, 2); ok {
		t.Fatal("CollSelectAt(2) present after b spliced")
	}
}

// SyncPendingRemovals must leave the order-statistic index exact against the sorted live set,
// so a rank-based select taken right after it agrees element for element. This is the guard
// SPOP/SRANDMEMBER/HRANDFIELD lean on when deletes were deferred.
func TestSyncPendingRemovalsExactRank(t *testing.T) {
	s := New(1<<18, 1<<22)
	s.EnableDeferredRemoval()
	defer s.Close()

	prefix := collPrefix("s")
	live := map[string]bool{}
	add := func(m string) {
		k := collKey("s", m)
		created, err := s.PutKind(k, nil, kindTestField)
		if err != nil {
			t.Fatal(err)
		}
		if created {
			s.CollInsert(k, kindTestField)
		}
		live[m] = true
	}
	deferDel := func(m string) {
		k := collKey("s", m)
		if s.DeleteKind(k, kindTestField) {
			buf, ends := packKeys(k)
			s.CollRemovePacked(buf, ends, kindTestField)
		}
		delete(live, m)
	}

	for i := 0; i < 500; i++ {
		add(fmt.Sprintf("m%04d", i))
	}
	// Defer-delete a scattered subset so several skip levels must re-bridge their spans.
	for i := 0; i < 500; i += 3 {
		deferDel(fmt.Sprintf("m%04d", i))
	}

	// Reconcile, then rank-select must match the sorted survivors exactly.
	s.SyncPendingRemovals()
	if s.tombPend.Load() != 0 {
		t.Fatalf("tombPend %d after SyncPendingRemovals, want 0", s.tombPend.Load())
	}

	var want []string
	for m := range live {
		want = append(want, m)
	}
	sort.Strings(want)
	for i, w := range want {
		k, ok := s.CollSelectAt(prefix, i)
		if !ok || string(k[len(prefix):]) != w {
			t.Fatalf("CollSelectAt(%d) = %q,%v want %q", i, k[len(prefix):], ok, w)
		}
	}
	if _, ok := s.CollSelectAt(prefix, len(want)); ok {
		t.Fatalf("CollSelectAt(%d) present past cardinality", len(want))
	}
}

// Close must drain any queued tombstones before the folder stops, so no removal is silently
// dropped when the store shuts down.
func TestDeferredRemovalCloseDrains(t *testing.T) {
	s := New(1<<16, 1<<20)
	s.EnableDeferredRemoval()

	for i := 0; i < 200; i++ {
		k := collKey("h", fmt.Sprintf("m%03d", i))
		if _, err := s.PutKind(k, []byte("v"), kindTestField); err != nil {
			t.Fatal(err)
		}
		s.CollInsert(k, kindTestField)
	}
	// Queue a burst of deferred deletes, then close immediately without waiting for the folder.
	for i := 0; i < 200; i += 2 {
		k := collKey("h", fmt.Sprintf("m%03d", i))
		if s.DeleteKind(k, kindTestField) {
			buf, ends := packKeys(k)
			s.CollRemovePacked(buf, ends, kindTestField)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if s.tombPend.Load() != 0 {
		t.Fatalf("tombPend %d after Close, want 0 (final drain missed a batch)", s.tombPend.Load())
	}
	// Every odd member survives, every even one is gone; the spliced index proves the drain ran.
	prefix := collPrefix("h")
	got := scanAll(s, prefix, 64)
	if len(got) != 100 {
		t.Fatalf("got %d survivors after Close, want 100", len(got))
	}
}

// The live folder must be safe against foreground deletes, re-adds, and SyncPendingRemovals all
// hitting the same key concurrently. This is the interleaving folderMu and removeManyLive's
// under-lock re-check exist for: run it under -race to prove no torn splice and no lost live node.
// After the churn quiesces and a final sync reconciles, the ordered index must match the members
// that are still live in the hash index exactly.
func TestDeferredRemovalConcurrentFolderChurn(t *testing.T) {
	s := New(1<<18, 1<<22)
	s.EnableDeferredRemoval()
	defer s.Close()

	const n = 400
	member := func(i int) []byte { return collKey("h", fmt.Sprintf("m%04d", i)) }

	// Seed a full collection.
	for i := 0; i < n; i++ {
		k := member(i)
		if _, err := s.PutKind(k, []byte("v"), kindTestField); err != nil {
			t.Fatal(err)
		}
		s.CollInsert(k, kindTestField)
	}

	// live tracks ground truth under its own lock; the workers below are the only mutators and
	// each serializes a given index through the stripe of shared mutation, so live stays truthful.
	var mu sync.Mutex
	live := make([]bool, n)
	for i := range live {
		live[i] = true
	}

	var wg sync.WaitGroup
	// Deleter: defer-removes even members it finds live.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 40; round++ {
			for i := 0; i < n; i += 2 {
				mu.Lock()
				if live[i] {
					k := member(i)
					if s.DeleteKind(k, kindTestField) {
						buf, ends := packKeys(k)
						s.CollRemovePacked(buf, ends, kindTestField)
						live[i] = false
					}
				}
				mu.Unlock()
			}
		}
	}()
	// Re-adder: brings even members back, racing the folder's liveness re-check.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 40; round++ {
			for i := 0; i < n; i += 2 {
				mu.Lock()
				if !live[i] {
					k := member(i)
					created, err := s.PutKind(k, []byte("v"), kindTestField)
					if err != nil {
						mu.Unlock()
						panic(err)
					}
					if created {
						s.CollInsert(k, kindTestField)
					}
					live[i] = true
				}
				mu.Unlock()
			}
		}
	}()
	// Syncer: forces foreground drains that must block on any in-flight folder splice.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 200; round++ {
			s.SyncPendingRemovals()
		}
	}()
	wg.Wait()

	// Quiesce: reconcile every deferred splice, then the index must equal the live set exactly.
	s.SyncPendingRemovals()
	if s.tombPend.Load() != 0 {
		t.Fatalf("tombPend %d after final sync, want 0", s.tombPend.Load())
	}
	var want []string
	for i := 0; i < n; i++ {
		if live[i] {
			want = append(want, fmt.Sprintf("m%04d", i))
		}
	}
	sort.Strings(want)
	prefix := collPrefix("h")
	for i, w := range want {
		k, ok := s.CollSelectAt(prefix, i)
		if !ok || string(k[len(prefix):]) != w {
			t.Fatalf("CollSelectAt(%d) = %q,%v want %q", i, sliceTail(k, len(prefix)), ok, w)
		}
	}
	if _, ok := s.CollSelectAt(prefix, len(want)); ok {
		t.Fatalf("CollSelectAt(%d) present past live cardinality %d", len(want), len(want))
	}
}

// sliceTail returns k past plen, or a marker when k is too short, so a failed assertion prints
// something readable instead of panicking on a bad slice.
func sliceTail(k []byte, plen int) string {
	if len(k) < plen {
		return "<short>"
	}
	return string(k[plen:])
}
