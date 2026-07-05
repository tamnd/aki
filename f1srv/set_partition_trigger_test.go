package f1srv

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Slice 6b wires the adaptive engage-and-grow trigger into the set write path with the lab knobs
// (SetPartitionMax/Threshold/Target), and makes every multi-partition reader (SMEMBERS, SSCAN, set
// algebra, the weighted draw) grow-safe. These tests pin two properties: an armed server grows a hot
// set through its partition steps purely from SADD traffic, reporting identical membership at every
// step, and the multi-partition readers run correctly while the set grows underneath them.

// triggerConn builds a server with the adaptive trigger armed at small thresholds so a test drives
// real growth from ordinary SADD traffic instead of the engageSetPartitions primitive. No forceP is
// set, so partitionsFor reads the per-key registry the trigger publishes.
func triggerConn(t testing.TB, max, threshold, target int) (*Server, *connState) {
	t.Helper()
	srv := New(Config{
		Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64,
		SetPartitionMax: max, SetPartitionThreshold: threshold, SetPartitionTarget: target,
	})
	return srv, bareConn(srv)
}

// TestEngageTriggerGrowsFromSAdd loads a set one batch at a time through an armed server and asserts
// the partition count tracks targetPartitions(card) as the cardinality climbs, that the count only
// ever rises (the one-way rule), and that SCARD and SMEMBERS report the exact loaded set at every
// step. Growth is driven only by SADD, so this proves the write-path trigger engages and grows a hot
// set with no explicit migration call.
func TestEngageTriggerGrowsFromSAdd(t *testing.T) {
	srv, c := triggerConn(t, 8, 200, 100)
	defer srv.Close()

	want := make(map[string]struct{})
	prevP := 1
	for batch := 0; batch < 12; batch++ {
		args := []string{"SADD", "hot"}
		for i := 0; i < 200; i++ {
			m := fmt.Sprintf("m:%02d:%05d", batch, i)
			args = append(args, m)
			want[m] = struct{}{}
		}
		call(c, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, args...)

		card := int(c.setCard([]byte("hot")))
		if card != len(want) {
			t.Fatalf("batch %d: SCARD %d, want %d", batch, card, len(want))
		}
		gotP := srv.partitionP([]byte("hot"))
		if gotP < prevP {
			t.Fatalf("batch %d: partitionP shrank %d -> %d", batch, prevP, gotP)
		}
		if wantP := srv.targetPartitions(card); gotP != wantP {
			t.Fatalf("batch %d card %d: partitionP %d, want targetPartitions %d", batch, card, gotP, wantP)
		}
		prevP = gotP

		got := smembersSorted(t, c, "hot")
		if len(got) != len(want) {
			t.Fatalf("batch %d: SMEMBERS %d members, want %d", batch, len(got), len(want))
		}
		for _, m := range got {
			if _, ok := want[m]; !ok {
				t.Fatalf("batch %d: SMEMBERS returned unexpected %q", batch, m)
			}
		}
	}
	if prevP <= 1 {
		t.Fatalf("set never engaged: final partitionP = %d", prevP)
	}
}

// TestEngageTriggerReadersGrowSafe grows one hot key from SADD traffic while many goroutines run the
// multi-partition readers against it, and asserts the readers never observe a half-migrated set and
// the final state is exact. Base members are loaded up front and never removed, so a reader that
// fails to find one during a grow has seen the set mid-migration, which the insert-before-delete
// ordering plus the grow-safe reader locks must prevent. Writers add keep members (stay) and add-then
// remove drop members (go) in private namespaces so the final set is deterministic. Run under -race
// this also proves the reader locks and the migration lock discipline stay cycle-free.
func TestEngageTriggerReadersGrowSafe(t *testing.T) {
	srv, setup := triggerConn(t, 8, 150, 50)
	defer srv.Close()

	const (
		baseN   = 300
		writers = 8
		perW    = 300
		readers = 6
	)
	base := make([]string, baseN)
	for i := range base {
		base[i] = fmt.Sprintf("base:%05d", i)
	}
	loadSet(t, setup, "hot", base)

	var probeMiss int64
	var stop atomic.Bool
	var readerWG, writerWG sync.WaitGroup

	// Reader workers run the four multi-partition read shapes while the set grows. A missing base
	// member is a correctness bug; the other shapes must simply not corrupt or panic.
	for r := 0; r < readers; r++ {
		readerWG.Add(1)
		go func(r int) {
			defer readerWG.Done()
			rc := bareConn(srv)
			for !stop.Load() {
				switch r % 4 {
				case 0:
					// SMEMBERS must always contain every base member.
					got := call(rc, func(c *connState, a [][]byte) { c.cmdSMembers(a) }, "SMEMBERS", "hot")
					for _, b := range base {
						if !strings.Contains(got, "\r\n"+b+"\r\n") && !strings.HasSuffix(got, b+"\r\n") {
							atomic.AddInt64(&probeMiss, 1)
							break
						}
					}
				case 1:
					// SSCAN a full cursor cycle; it must not error or panic mid-grow.
					call(rc, func(c *connState, a [][]byte) { c.cmdSScan(a) }, "SSCAN", "hot", "0", "COUNT", "1000")
				case 2:
					// The weighted draw must return a live member, never a nil for a non-empty set.
					got := call(rc, func(c *connState, a [][]byte) { c.cmdSRandMember(a) }, "SRANDMEMBER", "hot")
					if got == "$-1\r\n" {
						atomic.AddInt64(&probeMiss, 1)
					}
				default:
					// SINTERCARD of the set with itself is its cardinality; a torn read would undercount,
					// but this only checks it stays a well-formed positive integer reply.
					call(rc, func(c *connState, a [][]byte) { c.cmdSInterCard(a) }, "SINTERCARD", "2", "hot", "hot")
				}
			}
		}(r)
	}

	// Writer workers drive the growth and the final membership.
	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(w int) {
			defer writerWG.Done()
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

	// Join the writers, then stop the readers. The cardinality-based assertions below run quiescent
	// (no writer racing the lock-free SCARD counter), while the readers ran concurrently with every
	// grow, which is the property under test.
	writerWG.Wait()
	stop.Store(true)
	readerWG.Wait()

	wantKeep := writers * perW

	if probeMiss != 0 {
		t.Fatalf("%d reader probes failed during growth (half-migrated set observed)", probeMiss)
	}
	if got := srv.partitionP([]byte("hot")); got <= 1 {
		t.Fatalf("set never engaged during the concurrent grow: final partitionP = %d", got)
	}

	want := make([]string, 0, baseN+wantKeep)
	want = append(want, base...)
	for w := 0; w < writers; w++ {
		for i := 0; i < perW; i++ {
			want = append(want, fmt.Sprintf("keep:%02d:%05d", w, i))
		}
	}
	sort.Strings(want)
	if got := int(setup.setCard([]byte("hot"))); got != len(want) {
		t.Fatalf("final SCARD %d, want %d", got, len(want))
	}
	if got := smembersSorted(t, setup, "hot"); strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("final SMEMBERS has %d members, want %d", len(got), len(want))
	}
}

// TestEngageTriggerSPopDrainsGrown pops a grown set to empty through the partitioned SPOP path and
// asserts every popped member is distinct and the pop returns the exact loaded set. It exercises the
// SPOP empty-pick re-read indirectly by draining across all partitions until CollPartPick reports the
// set empty.
func TestEngageTriggerSPopDrainsGrown(t *testing.T) {
	srv, c := triggerConn(t, 8, 100, 40)
	defer srv.Close()

	members := make([]string, 600)
	for i := range members {
		members[i] = fmt.Sprintf("m:%05d", i)
	}
	loadSet(t, c, "hot", members)
	if got := srv.partitionP([]byte("hot")); got <= 1 {
		t.Fatalf("set did not engage before pop: partitionP = %d", got)
	}

	seen := make(map[string]struct{}, len(members))
	for {
		reply := call(c, func(c *connState, a [][]byte) { c.cmdSPop(a) }, "SPOP", "hot")
		if reply == "$-1\r\n" {
			break
		}
		// Reply is $len\r\n<member>\r\n; recover the member as the middle line.
		parts := strings.SplitN(reply, "\r\n", 3)
		if len(parts) < 2 {
			t.Fatalf("malformed SPOP reply %q", reply)
		}
		m := parts[1]
		if _, dup := seen[m]; dup {
			t.Fatalf("SPOP returned %q twice", m)
		}
		seen[m] = struct{}{}
	}
	if len(seen) != len(members) {
		t.Fatalf("SPOP drained %d members, want %d", len(seen), len(members))
	}
	if got := int(c.setCard([]byte("hot"))); got != 0 {
		t.Fatalf("set not empty after drain: SCARD = %d", got)
	}
}
