package f1srv

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Slice 5a routes SMEMBERS and SSCAN over the P partitions of a set (spec 2064/f1_rewrite_ltm/19
// section 6.7 and 6.8). Because the partition byte sits between the length-prefixed set key and the
// member, the whole-set prefix bounds every partition's rows in one contiguous ordered run, so a
// single prefix walk sweeps all P partitions. The tests pin that the enumeration is partition
// transparent: SMEMBERS returns exactly the member set the unpartitioned path does, SSCAN's full
// cycle visits each member once across partition boundaries, MATCH filters on the recovered member
// bytes, and both still reject a string key with WRONGTYPE.

// parseSscan decodes an SSCAN reply (*2 then a bulk cursor then an array of member bulks) into the
// resume cursor and the member payloads. It fails the test on a malformed reply so a shape bug
// surfaces loudly rather than as a silent miscount.
func parseSscan(t *testing.T, s string) (cursor string, members []string) {
	t.Helper()
	if !strings.HasPrefix(s, "*2\r\n") {
		t.Fatalf("not a two-element SSCAN reply: %q", s)
	}
	rest := s[len("*2\r\n"):]
	// The cursor bulk.
	if len(rest) == 0 || rest[0] != '$' {
		t.Fatalf("SSCAN cursor is not a bulk: %q", s)
	}
	j := strings.Index(rest, "\r\n")
	if j < 0 {
		t.Fatalf("truncated cursor header: %q", s)
	}
	l, err := strconv.Atoi(rest[1:j])
	if err != nil {
		t.Fatalf("bad cursor length in %q: %v", s, err)
	}
	cursor = rest[j+2 : j+2+l]
	rest = rest[j+2+l+2:]
	// The member array reuses the array-of-bulks decoder.
	members = parseArrayBulks(t, rest)
	return cursor, members
}

// smembersSorted runs SMEMBERS through the routed path and returns the members sorted, since section
// 6.7 leaves the concatenation order across partitions unspecified. Sorting makes the comparison
// order independent while still catching a missing, extra, or corrupted member.
func smembersSorted(t *testing.T, c *connState, key string) []string {
	t.Helper()
	out := parseArrayBulks(t, call(c, func(c *connState, a [][]byte) { c.cmdSMembers(a) }, "SMEMBERS", key))
	sort.Strings(out)
	return out
}

// TestSetMembersPartitionIdentical loads the same 300 members into a set at P=1, 2, 4, 8 and asserts
// SMEMBERS returns the identical member set (order aside) at every P. P=1 is the unpartitioned walk,
// so matching it proves the routed multi-partition walk enumerates every partition's rows and strips
// the partition byte back to the exact member. 300 members spread across every partition of P=8, and
// a scattered removal leaves live and dead rows interleaved so a stale row would show up as a diff.
func TestSetMembersPartitionIdentical(t *testing.T) {
	members := make([]string, 300)
	for i := range members {
		members[i] = fmt.Sprintf("member:%04d", i)
	}

	load := func(p int) []string {
		srv := newPartServer(t, p)
		defer srv.Close()
		c := bareConn(srv)
		// Add in batches so members land across every partition.
		for b := 0; b < len(members); b += 50 {
			args := append([]string{"SADD", "hot"}, members[b:b+50]...)
			call(c, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, args...)
		}
		// Remove a scattered tenth so live and tombstoned rows interleave under the prefix.
		rem := []string{"SREM", "hot"}
		for i := 0; i < len(members); i += 10 {
			rem = append(rem, members[i])
		}
		call(c, func(c *connState, a [][]byte) { c.cmdSRem(a) }, rem...)
		return smembersSorted(t, c, "hot")
	}

	ref := load(1)
	if len(ref) != 270 {
		t.Fatalf("P=1 SMEMBERS returned %d members, want 270 after removing 30", len(ref))
	}
	for _, p := range []int{2, 4, 8} {
		got := load(p)
		if len(got) != len(ref) {
			t.Fatalf("P=%d SMEMBERS returned %d members, want %d (P=1)", p, len(got), len(ref))
		}
		for i := range ref {
			if got[i] != ref[i] {
				t.Fatalf("P=%d SMEMBERS member %d = %q, want %q (P=1)", p, i, got[i], ref[i])
			}
		}
	}
}

// TestSetScanPartitionFullCycle drives SSCAN with a small COUNT to force many resume hops and asserts
// the full cycle visits every member exactly once across partition boundaries at P=1, 2, 4, 8. The
// cursor is aki's hex of the full composite key, which already orders by (partition, member), so a
// resume lands on the next row regardless of which partition it falls in. A duplicate or a dropped
// member would mean the cursor does not carry the partition context across a boundary.
func TestSetScanPartitionFullCycle(t *testing.T) {
	want := map[string]bool{}
	for i := 0; i < 200; i++ {
		want[fmt.Sprintf("m%04d", i)] = true
	}

	for _, p := range []int{1, 2, 4, 8} {
		srv := newPartServer(t, p)
		c := bareConn(srv)
		args := []string{"SADD", "hot"}
		for m := range want {
			args = append(args, m)
		}
		call(c, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, args...)

		seen := map[string]int{}
		cursor := "0"
		hops := 0
		for {
			cur, members := parseSscan(t, call(c, func(c *connState, a [][]byte) { c.cmdSScan(a) },
				"SSCAN", "hot", cursor, "COUNT", "7"))
			for _, m := range members {
				seen[m]++
			}
			cursor = cur
			hops++
			if cursor == "0" {
				break
			}
			if hops > 10000 {
				t.Fatalf("P=%d SSCAN did not terminate", p)
			}
		}
		if len(seen) != len(want) {
			t.Fatalf("P=%d SSCAN saw %d distinct members, want %d", p, len(seen), len(want))
		}
		for m := range want {
			if seen[m] != 1 {
				t.Fatalf("P=%d SSCAN saw member %q %d times, want 1", p, m, seen[m])
			}
		}
		srv.Close()
	}
}

// TestSetScanPartitionMatch checks SSCAN's MATCH filters on the recovered member bytes, not the raw
// composite row, under partitioning. The pattern user:* must match only the user members even though
// the stored row carries a length prefix, the set key, and a partition byte ahead of the member, so
// a filter that forgot to strip the partition byte would match nothing. Every scan step filters the
// same way, so the union over the full cycle is exactly the user members.
func TestSetScanPartitionMatch(t *testing.T) {
	for _, p := range []int{1, 4, 8} {
		srv := newPartServer(t, p)
		c := bareConn(srv)
		args := []string{"SADD", "hot"}
		for i := 0; i < 60; i++ {
			args = append(args, fmt.Sprintf("user:%03d", i), fmt.Sprintf("bot:%03d", i))
		}
		call(c, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, args...)

		seen := map[string]bool{}
		cursor := "0"
		for {
			cur, members := parseSscan(t, call(c, func(c *connState, a [][]byte) { c.cmdSScan(a) },
				"SSCAN", "hot", cursor, "COUNT", "9", "MATCH", "user:*"))
			for _, m := range members {
				if !strings.HasPrefix(m, "user:") {
					t.Fatalf("P=%d SSCAN MATCH user:* returned non-matching %q", p, m)
				}
				seen[m] = true
			}
			cursor = cur
			if cursor == "0" {
				break
			}
		}
		if len(seen) != 60 {
			t.Fatalf("P=%d SSCAN MATCH user:* saw %d members, want 60", p, len(seen))
		}
		srv.Close()
	}
}

// TestSetMembersPartitionConcurrentWriters streams SMEMBERS off a partitioned set while many
// goroutines churn it with SADD and SREM, so the race detector exercises the exact hazard the shared
// partition locks guard: a member row removed and its arena offset reused while the reader is copying
// it out. The reader takes every distinct partition stripe's read lock (streamSetPart), which
// excludes the routed writers for the span of the two passes, so no read observes a half-overwritten
// row. The test asserts nothing about contents, only that no data race or crash occurs and the reader
// always returns a well-formed reply, since the live set shifts under it.
func TestSetMembersPartitionConcurrentWriters(t *testing.T) {
	srv := newPartServer(t, 8)
	defer srv.Close()

	// Seed a base so the reader always has rows to stream from the first read on.
	seed := bareConn(srv)
	seedArgs := []string{"SADD", "hot"}
	for i := 0; i < 500; i++ {
		seedArgs = append(seedArgs, fmt.Sprintf("m%05d", i))
	}
	call(seed, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, seedArgs...)

	done := make(chan struct{})
	const writers = 6
	errCh := make(chan error, writers+1)
	for w := 0; w < writers; w++ {
		go func(base int) {
			c := bareConn(srv)
			i := 0
			for {
				select {
				case <-done:
					errCh <- nil
					return
				default:
				}
				// Churn a rolling window unique to this writer so adds and removes both hit live rows
				// and the arena recycles offsets under the reader.
				m := fmt.Sprintf("w%02d-%08d", base, base*1_000_000+(i%2000))
				call(c, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, "SADD", "hot", m)
				call(c, func(c *connState, a [][]byte) { c.cmdSRem(a) }, "SREM", "hot", m)
				i++
			}
		}(w)
	}

	reader := bareConn(srv)
	for r := 0; r < 400; r++ {
		out := call(reader, func(c *connState, a [][]byte) { c.cmdSMembers(a) }, "SMEMBERS", "hot")
		if len(out) == 0 || out[0] != '*' {
			close(done)
			t.Fatalf("SMEMBERS returned a non-array reply under concurrent writers: %q", out[:min(len(out), 40)])
		}
	}
	close(done)
	for i := 0; i < writers; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

// TestSetScanCompletenessUnderConcurrentSrem pins the guarantee that lets SSCAN enumerate off the
// dense member vector once the set type is off the ordered index (spec 2064/f1_rewrite_ltm/20): an
// SSCAN full cycle driven lock-free while writers churn the set with SADD and SREM must still return
// every member that is present for the whole scan at least once. A stable member is only lost if a
// concurrent swap-remove moves an unvisited member into an already-visited slot; SetVecScanDown walks
// the vector high index to low precisely so that can never happen (a remove moves the tail, always at
// or above the cursor, into the hole), so the never-removed members are the floor the SCAN contract
// promises. The churn set is disjoint from the stable set and is repeatedly removed and re-added, so
// the vector shrinks and grows under the reader and swap-remove fires against live rows the whole time.
// Runs at P=1 (the whole-set vector) and P=8 (per-partition vectors), the two vector layouts.
func TestSetScanCompletenessUnderConcurrentSrem(t *testing.T) {
	for _, p := range []int{1, 8} {
		srv := newPartServer(t, p)

		// Seed the stable members that must every one be seen, plus an initial churn population so the
		// first scan already walks a mixed vector.
		seed := bareConn(srv)
		stable := make([]string, 400)
		seedArgs := []string{"SADD", "hot"}
		for i := range stable {
			stable[i] = fmt.Sprintf("keep:%04d", i)
			seedArgs = append(seedArgs, stable[i])
		}
		call(seed, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, seedArgs...)

		done := make(chan struct{})
		const writers = 6
		errCh := make(chan error, writers)
		for w := 0; w < writers; w++ {
			go func(base int) {
				c := bareConn(srv)
				i := 0
				for {
					select {
					case <-done:
						errCh <- nil
						return
					default:
					}
					// A rolling window of churn members unique to this writer: the SADD grows the vector,
					// the SREM swap-removes a live row, so the reader always races both against live rows.
					m := fmt.Sprintf("churn:%02d:%08d", base, i%2000)
					call(c, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, "SADD", "hot", m)
					call(c, func(c *connState, a [][]byte) { c.cmdSRem(a) }, "SREM", "hot", m)
					i++
				}
			}(w)
		}

		// Drive a full SSCAN cycle with a small COUNT to force many lock-free resume hops while the churn
		// runs. Every stable member must appear at least once across the cycle.
		reader := bareConn(srv)
		seen := map[string]bool{}
		cursor := "0"
		hops := 0
		for {
			cur, members := parseSscan(t, call(reader, func(c *connState, a [][]byte) { c.cmdSScan(a) },
				"SSCAN", "hot", cursor, "COUNT", "11"))
			for _, m := range members {
				if strings.HasPrefix(m, "keep:") {
					seen[m] = true
				}
			}
			cursor = cur
			hops++
			if cursor == "0" {
				break
			}
			if hops > 100000 {
				close(done)
				t.Fatalf("P=%d SSCAN did not terminate", p)
			}
		}
		close(done)
		for i := 0; i < writers; i++ {
			if err := <-errCh; err != nil {
				t.Fatal(err)
			}
		}

		if len(seen) != len(stable) {
			var missing []string
			for _, m := range stable {
				if !seen[m] {
					missing = append(missing, m)
				}
			}
			sort.Strings(missing)
			show := missing
			if len(show) > 10 {
				show = show[:10]
			}
			t.Fatalf("P=%d SSCAN saw %d of %d stable members under concurrent SREM, missing %d (first: %v)",
				p, len(seen), len(stable), len(missing), show)
		}
		srv.Close()
	}
}

// TestSetDropClearsMembersFromVector locks the regression that dropping a set must delete its member
// rows by walking the dense member vector, not the ordered index. Since the set type no longer indexes
// its members there (spec 2064/f1_rewrite_ltm/20), a CollScan-driven drop finds nothing and leaks every
// row, and a later SISMEMBER (a point ExistsKind on the row) reads the leaked set as still present. It
// seeds a set, DELs it, and asserts SISMEMBER, SCARD, and SMEMBERS all report empty, then re-adds to
// confirm the name is reusable. Runs at P=1 (whole-set vector) and P=4 (per-partition vectors), the two
// layouts dropSetMembersLocked branches on.
func TestSetDropClearsMembersFromVector(t *testing.T) {
	for _, p := range []int{1, 4} {
		srv := newPartServer(t, p)
		c := bareConn(srv)

		members := make([]string, 50)
		addArgs := []string{"SADD", "hot"}
		for i := range members {
			members[i] = fmt.Sprintf("m:%04d", i)
			addArgs = append(addArgs, members[i])
		}
		call(c, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, addArgs...)
		if got := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "hot"); got != ":50\r\n" {
			t.Fatalf("P=%d SCARD after seed = %q, want :50", p, got)
		}

		call(c, func(c *connState, a [][]byte) { c.cmdDel(a) }, "DEL", "hot")

		if got := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "hot"); got != ":0\r\n" {
			t.Fatalf("P=%d SCARD after DEL = %q, want :0", p, got)
		}
		if got := call(c, func(c *connState, a [][]byte) { c.cmdSMembers(a) }, "SMEMBERS", "hot"); got != "*0\r\n" {
			t.Fatalf("P=%d SMEMBERS after DEL = %q, want empty array", p, got)
		}
		for _, m := range members {
			if got := call(c, func(c *connState, a [][]byte) { c.cmdSIsMember(a) }, "SISMEMBER", "hot", m); got != ":0\r\n" {
				t.Fatalf("P=%d SISMEMBER %q after DEL = %q, want :0 (leaked member row)", p, m, got)
			}
		}

		// The name is reusable: a fresh SADD builds a new set with only the re-added member.
		call(c, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, "SADD", "hot", "again")
		if got := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "hot"); got != ":1\r\n" {
			t.Fatalf("P=%d SCARD after re-add = %q, want :1", p, got)
		}
		if got := call(c, func(c *connState, a [][]byte) { c.cmdSIsMember(a) }, "SISMEMBER", "hot", members[0]); got != ":0\r\n" {
			t.Fatalf("P=%d SISMEMBER old member after re-add = %q, want :0", p, got)
		}
		srv.Close()
	}
}

// TestSetEnumPartitionWrongType confirms the routed SMEMBERS and SSCAN still reject a string key with
// WRONGTYPE, the same guard the unpartitioned path applies, so partitioning never opens a type hole.
func TestSetEnumPartitionWrongType(t *testing.T) {
	srv := newPartServer(t, 8)
	defer srv.Close()
	c := bareConn(srv)
	call(c, func(c *connState, a [][]byte) { c.cmdSet(a) }, "SET", "str", "v")

	if got := call(c, func(c *connState, a [][]byte) { c.cmdSMembers(a) }, "SMEMBERS", "str"); !strings.HasPrefix(got, "-WRONGTYPE") {
		t.Fatalf("SMEMBERS on string key = %q, want WRONGTYPE", got)
	}
	if got := call(c, func(c *connState, a [][]byte) { c.cmdSScan(a) }, "SSCAN", "str", "0"); !strings.HasPrefix(got, "-WRONGTYPE") {
		t.Fatalf("SSCAN on string key = %q, want WRONGTYPE", got)
	}
}
