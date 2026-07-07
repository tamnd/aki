package f1raw

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
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

// setChurnColdStore builds a segmented store with a cold record region that admits kindSetMember to
// both the vector-member (Option A retier) path and the background migrator's policy, so a set member
// row the migrator picks up during drainSegment is retiered in place through migrateVecMember rather
// than left as a stale resident offset. It is the M2 analogue of newSetRecStore on the reclaimable
// segmented arena the -race fuzz needs.
func setChurnColdStore(t *testing.T, nSeg int) *Store {
	t.Helper()
	s := churnSegColdStore(t, nSeg)
	s.SetVecMemberKindFunc(func(k byte) bool { return k == tKindSetMember })
	s.SetMigratableKindFunc(func(k byte) bool { return k == tKindSetMember })
	return s
}

// setMemberLen makes each set member big enough that a segment holds only tens of them, so the fuzz
// set spills across several segments and the migrator has full segments to drain, the set-member
// analogue of churnValLen.
const setMemberLen = 2600

// bigSetMember builds a distinct, verifiable member string of setMemberLen bytes. A torn read from a
// reused segment produces bytes outside this fixed shape, so a membership check against the universe
// catches a use-after-free the race detector might only have surfaced as a data race.
func bigSetMember(i int) string {
	b := make([]byte, setMemberLen)
	head := fmt.Sprintf("mem-%06d:", i)
	copy(b, head)
	for j := len(head); j < len(b); j++ {
		b[j] = byte('a' + (i+j)%26)
	}
	return string(b)
}

// TestSetMemberRetierRaceUnderMigrator is the D22 Option A M2 -race gate (spec 2064/f1_rewrite_ltm/22
// section 5, section 10 M2): the background migrator drains set member rows cold through drainRecord's
// kindSetMember dispatch while writers churn SADD/SREM on the same set and readers draw and scan it
// lock-free. The migrator retiers each moved member's cached vector slot in place under the shard
// mutex (race 1 and 2) and the draws hold the reader epoch across the offset-to-key resolution so a
// retiered-and-reclaimed segment cannot be reused under a draw mid-read (race 3). Under -race the run
// must be clean, no draw or scan may return a key outside the fixed member universe (which a torn read
// would), and at quiesce the vector must hold exactly the reconciled membership with its density
// invariant intact and every member still resolvable across whatever tier it settled in.
func TestSetMemberRetierRaceUnderMigrator(t *testing.T) {
	s := setChurnColdStore(t, 12)
	// Drain aggressively so members keep moving cold under the readers rather than staying resident:
	// a low high-water keeps the migrator retiering throughout the churn, maximizing the race surface.
	s.migHiNum, s.migLoNum = 20, 10
	s.EnableMigrator()

	const skey = "s"
	const universe = 300
	prefix := setPrefixBytes(skey)

	// The fixed universe of composite member keys. A draw or scan may only ever return one of these.
	memberKeys := make([][]byte, universe)
	want := make(map[string]bool, universe)
	for i := 0; i < universe; i++ {
		mk := memberKeyBytes(skey, bigSetMember(i))
		memberKeys[i] = mk
		want[string(mk)] = true
	}

	// writeMu serializes writers to the one set key, the engine-test stand-in for the server's per-key
	// stripe lock: SADD's PutKind+CollRandInsert and SREM's CollRandRemove+DeleteKind never interleave,
	// so the vector and the primary index stay consistent exactly as they do on the wire. The migrator
	// does not take it, so it races the vector mutation on the shard mutex and the lock-free draws,
	// which is the surface section 5 races 1 through 3 cover.
	var writeMu sync.Mutex
	sadd := func(mk []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		created, err := s.PutKind(mk, nil, tKindSetMember)
		if err != nil {
			return err
		}
		if created {
			s.CollRandInsert(prefix, mk, tKindSetMember)
		}
		return nil
	}
	srem := func(mk []byte) {
		writeMu.Lock()
		defer writeMu.Unlock()
		s.CollRandRemove(prefix, mk, tKindSetMember)
		s.DeleteKind(mk, tKindSetMember)
	}

	// Seed the whole universe so the set spans several segments before the churn starts.
	for _, mk := range memberKeys {
		if err := sadd(mk); err != nil {
			t.Fatalf("seed SADD: %v", err)
		}
	}

	var stop atomic.Bool
	var firstErr atomic.Pointer[string]
	fail := func(msg string) { firstErr.CompareAndSwap(nil, &msg) }
	var ops atomic.Int64
	var wg sync.WaitGroup

	// Readers: lock-free draws and downward scans, every returned key checked against the universe.
	const readers = 6
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := testRNG(uint64(r)*2654435761 + 1)
			var scan [][]byte
			for j := 0; !stop.Load(); j++ {
				if j&1 == 0 {
					if k, ok := s.CollRandSelect(prefix, rng.next()); ok && !want[string(k)] {
						fail(fmt.Sprintf("CollRandSelect drew non-universe key of len %d", len(k)))
						return
					}
					continue
				}
				hi := -1
				for !stop.Load() {
					var next int
					scan, next = s.SetVecScanDown(prefix, hi, 32, scan[:0])
					for _, k := range scan {
						if !want[string(k)] {
							fail(fmt.Sprintf("SetVecScanDown returned non-universe key of len %d", len(k)))
							return
						}
					}
					if next == 0 || len(scan) == 0 {
						break
					}
					hi = next
				}
			}
		}(r)
	}

	// Writers: random SADD/SREM over the universe against the one set, serialized by writeMu. A full
	// arena during a churn SADD is transient backpressure the migrator relieves, so it retries rather
	// than fails; a genuine stall would still time out inside PutKind's waitForSegment.
	const writers = 4
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := testRNG(uint64(w)*40503 + 7)
			for !stop.Load() && firstErr.Load() == nil {
				mk := memberKeys[int(rng.next()%universe)]
				if rng.next()&1 == 0 {
					srem(mk)
				} else if err := sadd(mk); err != nil && err != ErrFull {
					fail(fmt.Sprintf("churn SADD: %v", err))
					return
				}
				ops.Add(1)
			}
		}(w)
	}

	for ops.Load() < 20000 && firstErr.Load() == nil {
		runtime.Gosched()
	}
	stop.Store(true)
	wg.Wait()
	if msg := firstErr.Load(); msg != nil {
		t.Fatal(*msg)
	}

	// Reconcile to the whole universe, quiesce, then assert the vector is exactly the universe with its
	// density invariant intact and every member resolvable across whatever tier the churn left it in.
	for _, mk := range memberKeys {
		created, err := s.PutKind(mk, nil, tKindSetMember)
		if err != nil {
			t.Fatalf("reconcile PutKind: %v", err)
		}
		if created {
			s.CollRandInsert(prefix, mk, tKindSetMember)
		}
	}

	if got := s.SetVecLen(prefix); got != universe {
		t.Fatalf("SetVecLen = %d after reconcile, want %d", got, universe)
	}

	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	v := sh.get(prefix)
	if v == nil {
		sh.mu.Unlock()
		t.Fatal("no member vector after reconcile")
	}
	if len(v.slots) != len(v.back) {
		n, m := len(v.slots), len(v.back)
		sh.mu.Unlock()
		t.Fatalf("density broken: %d slots, %d back entries", n, m)
	}
	for i, off := range v.slots {
		if v.back[off] != i {
			bi := v.back[off]
			sh.mu.Unlock()
			t.Fatalf("density broken: back[slots[%d]] = %d, want %d", i, bi, i)
		}
	}
	nslots := len(v.slots)
	sh.mu.Unlock()
	if nslots != universe {
		t.Fatalf("vector holds %d slots after reconcile, want %d", nslots, universe)
	}

	var all [][]byte
	hi := -1
	for {
		var next int
		all, next = s.SetVecScanDown(prefix, hi, 64, all)
		if next == 0 {
			break
		}
		hi = next
	}
	seen := make(map[string]bool, universe)
	for _, k := range all {
		ks := string(k)
		if !want[ks] {
			t.Fatalf("final scan returned non-universe key of len %d", len(k))
		}
		if seen[ks] {
			t.Fatalf("final scan returned a duplicate member")
		}
		seen[ks] = true
	}
	if len(seen) != universe {
		t.Fatalf("final scan covered %d distinct members, want %d", len(seen), universe)
	}
	for _, mk := range memberKeys {
		if !s.ExistsKind(mk, tKindSetMember) {
			t.Fatalf("a member is not present after reconcile")
		}
	}
}
