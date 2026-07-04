package f1srv

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Slice 6a-2 builds engageSetPartitions, the one-way grow that re-homes every member of a hot set into
// a larger partition layout, plus the lockMemberPartition and setMemberProbe retry guards that let a
// routed single-member op run correctly while a migration of the same key is in flight (spec
// 2064/f1_rewrite_ltm/19 section 11.6). These tests pin the two properties the migration promises: a
// grow preserves the exact member set and cardinality, and a live migration is invisible to concurrent
// single-member SADD/SREM/SISMEMBER, which never see a half-migrated set.

// migrateConn builds a server whose partition count comes from the per-key registry (forceP left 0),
// so engageSetPartitions drives P through engageP the way the slice-6b trigger will, not through the
// whole-server test hook. The set starts unpartitioned (no registry entry, P=1) until a migration
// engages it.
func migrateConn(t testing.TB) (*Server, *connState) {
	t.Helper()
	srv := New(Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64})
	return srv, bareConn(srv)
}

// TestEngageSetPartitionsGrowsPreserveMembers loads a set at P=1, then grows it through 2, 4, and 8 in
// sequence, asserting after every grow that SCARD, SMEMBERS, and a membership probe of a present and an
// absent member all match the pre-grow answer, and that the registry now routes the set through the new
// P. Each grow re-homes members whose partition byte changes and leaves the rest in place, so matching
// the pre-grow reply proves the re-home neither drops a member nor resurrects a deleted row, and reading
// the header count back proves the cardinality word survived the header rewrite intact.
func TestEngageSetPartitionsGrowsPreserveMembers(t *testing.T) {
	srv, c := migrateConn(t)
	defer srv.Close()

	members := make([]string, 300)
	for i := range members {
		members[i] = fmt.Sprintf("m:%05d", i)
	}
	loadSet(t, c, "hot", members)

	wantCard := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "hot")
	wantMembers := strings.Join(smembersSorted(t, c, "hot"), "\x00")

	for _, p := range []int{2, 4, 8} {
		c.engageSetPartitions([]byte("hot"), p)
		if got := srv.partitionP([]byte("hot")); got != p {
			t.Fatalf("after grow to %d, partitionP = %d", p, got)
		}
		if got := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "hot"); got != wantCard {
			t.Fatalf("P=%d SCARD = %q, want %q", p, got, wantCard)
		}
		if got := strings.Join(smembersSorted(t, c, "hot"), "\x00"); got != wantMembers {
			t.Fatalf("P=%d SMEMBERS differ from pre-grow", p)
		}
		if got := call(c, func(c *connState, a [][]byte) { c.cmdSIsMember(a) }, "SISMEMBER", "hot", "m:00123"); got != ":1\r\n" {
			t.Fatalf("P=%d SISMEMBER present = %q, want :1", p, got)
		}
		if got := call(c, func(c *connState, a [][]byte) { c.cmdSIsMember(a) }, "SISMEMBER", "hot", "absent"); got != ":0\r\n" {
			t.Fatalf("P=%d SISMEMBER absent = %q, want :0", p, got)
		}
	}
}

// TestEngageSetPartitionsIsANoOpDownward pins the one-way rule: a migration only ever raises P, so an
// engage to a P at or below the current one leaves the layout and the registry untouched. This is the
// guard that lets the migration freeze [0, newP) and know it covers every member's old and new home.
func TestEngageSetPartitionsIsANoOpDownward(t *testing.T) {
	srv, c := migrateConn(t)
	defer srv.Close()

	loadSet(t, c, "hot", []string{"a", "b", "c", "d"})
	c.engageSetPartitions([]byte("hot"), 4)
	if got := srv.partitionP([]byte("hot")); got != 4 {
		t.Fatalf("engage to 4 left partitionP = %d", got)
	}
	// An engage to the same P and to a lower P are both no-ops.
	c.engageSetPartitions([]byte("hot"), 4)
	c.engageSetPartitions([]byte("hot"), 2)
	c.engageSetPartitions([]byte("hot"), 1)
	if got := srv.partitionP([]byte("hot")); got != 4 {
		t.Fatalf("downward engage changed partitionP to %d, want 4", got)
	}
}

// TestEngagedSetKeyOpsCarryPartitionRegistry pins the registry maintenance the key ops perform around
// an engaged set: RENAME moves a partitioned source to a destination that must inherit the source's P
// and route correctly, leaving the source unregistered; COPY replicates the layout into a destination
// that must also inherit P; and DEL drops the registry entry so a set recreated under the name starts
// fresh at P=1. Without carrying P the destination's rows sit at partition homes the registry-driven
// reads never visit, so SMEMBERS matching the source proves the carry.
func TestEngagedSetKeyOpsCarryPartitionRegistry(t *testing.T) {
	srv, c := migrateConn(t)
	defer srv.Close()

	members := make([]string, 200)
	for i := range members {
		members[i] = fmt.Sprintf("m:%05d", i)
	}
	loadSet(t, c, "src", members)
	c.engageSetPartitions([]byte("src"), 4)
	want := strings.Join(smembersSorted(t, c, "src"), "\x00")

	// COPY carries P into dst and leaves src engaged.
	call(c, func(c *connState, a [][]byte) { c.cmdCopy(a) }, "COPY", "src", "cp")
	if got := srv.partitionP([]byte("cp")); got != 4 {
		t.Fatalf("COPY dst partitionP = %d, want 4", got)
	}
	if got := srv.partitionP([]byte("src")); got != 4 {
		t.Fatalf("COPY left src partitionP = %d, want 4", got)
	}
	if got := strings.Join(smembersSorted(t, c, "cp"), "\x00"); got != want {
		t.Fatalf("COPY dst SMEMBERS differ from src")
	}

	// RENAME carries P into dst and unregisters src.
	call(c, func(c *connState, a [][]byte) { c.cmdRename(a) }, "RENAME", "src", "dst")
	if got := srv.partitionP([]byte("dst")); got != 4 {
		t.Fatalf("RENAME dst partitionP = %d, want 4", got)
	}
	if got := srv.partitionP([]byte("src")); got != 1 {
		t.Fatalf("RENAME left src partitionP = %d, want 1", got)
	}
	if got := strings.Join(smembersSorted(t, c, "dst"), "\x00"); got != want {
		t.Fatalf("RENAME dst SMEMBERS differ from src")
	}

	// DEL drops the registry entry so the name reads back unpartitioned.
	call(c, func(c *connState, a [][]byte) { c.cmdDel(a) }, "DEL", "dst")
	if got := srv.partitionP([]byte("dst")); got != 1 {
		t.Fatalf("DEL left dst partitionP = %d, want 1", got)
	}
}

// TestEngageSetPartitionsRaceWithSingleMemberOps runs a live migration that grows one hot key through
// 2, 4, 8 while many goroutines hammer single-member ops on the same key, and asserts the concurrent
// ops never observe a half-migrated set and the final state is exactly correct. Three disjoint member
// namespaces make the final set deterministic regardless of interleaving:
//
//   - base members are loaded up front and never touched, so a probing worker that reads any base
//     member as absent means the migration made a present member momentarily unfindable, which the
//     insert-new-before-delete-old ordering must prevent; the test fails on the first such miss.
//   - keep members are added by writer workers and never removed, so they must all be present at the end.
//   - drop members are added then removed by the same worker, so they must all be absent at the end.
//
// After every worker and the migration join, SCARD and SMEMBERS must equal base plus keep exactly, and
// the key must have landed at P=8. Run under -race this also proves the lock discipline is cycle-free.
func TestEngageSetPartitionsRaceWithSingleMemberOps(t *testing.T) {
	srv, setup := migrateConn(t)
	defer srv.Close()

	const (
		baseN   = 400
		writers = 8
		perW    = 150
	)
	base := make([]string, baseN)
	for i := range base {
		base[i] = fmt.Sprintf("base:%05d", i)
	}
	loadSet(t, setup, "hot", base)

	var probeMiss int64
	var wg sync.WaitGroup

	// The migration goroutine grows the key through the three steps, yielding between steps so the
	// grows land amid in-flight writes rather than all before them.
	wg.Add(1)
	go func() {
		defer wg.Done()
		mc := bareConn(srv)
		for _, p := range []int{2, 4, 8} {
			for i := 0; i < 50; i++ {
				runtime.Gosched()
			}
			mc.engageSetPartitions([]byte("hot"), p)
		}
	}()

	// Probe workers read base members that are present for the whole run; a miss is a correctness bug.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			pc := bareConn(srv)
			for i := 0; i < perW*4; i++ {
				m := base[(w*7+i*13)%baseN]
				if call(pc, func(c *connState, a [][]byte) { c.cmdSIsMember(a) }, "SISMEMBER", "hot", m) != ":1\r\n" {
					atomic.AddInt64(&probeMiss, 1)
				}
			}
		}(w)
	}

	// Writer workers add keep members (stay) and add-then-remove drop members (go), each in a private
	// namespace so the final membership is deterministic no matter how the ops interleave with the grow.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			wc := bareConn(srv)
			for i := 0; i < perW; i++ {
				keep := fmt.Sprintf("keep:%02d:%05d", w, i)
				call(wc, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, "SADD", "hot", keep)
				drop := fmt.Sprintf("drop:%02d:%05d", w, i)
				call(wc, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, "SADD", "hot", drop)
				call(wc, func(c *connState, a [][]byte) { c.cmdSRem(a) }, "SREM", "hot", drop)
			}
		}(w)
	}

	wg.Wait()

	if probeMiss != 0 {
		t.Fatalf("%d base-member probes missed during migration (half-migrated set observed)", probeMiss)
	}
	if got := srv.partitionP([]byte("hot")); got != 8 {
		t.Fatalf("final partitionP = %d, want 8", got)
	}

	want := make([]string, 0, baseN+writers*perW)
	want = append(want, base...)
	for w := 0; w < writers; w++ {
		for i := 0; i < perW; i++ {
			want = append(want, fmt.Sprintf("keep:%02d:%05d", w, i))
		}
	}
	sort.Strings(want)

	if got := call(setup, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "hot"); got != fmt.Sprintf(":%d\r\n", len(want)) {
		t.Fatalf("final SCARD = %q, want :%d", got, len(want))
	}
	if got := smembersSorted(t, setup, "hot"); strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("final SMEMBERS has %d members, want %d", len(got), len(want))
	}
}
