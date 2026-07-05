package f1srv

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/f1raw"
)

// Slice 4 routes SPOP and SRANDMEMBER through the weighted-partition draw (spec 2064/f1_rewrite_ltm/
// 19 sections 6.5 and 6.6). Three properties must hold before the routed draw is trusted: it draws
// every live member with probability exactly 1/total even when the partitions are lopsided (the
// weighted scheme's whole point, section 2.2), the without-replacement forms return distinct members
// while the with-replacement form allows repeats, and splitting the destructive draw across P
// partition locks lets single-key SPOP throughput rise with P. The uniformity test pins the first,
// the distinct/replacement tests the second, the contention microbenchmark the third.

// membersForPartitions returns member names deliberately bucketed so partition q holds exactly
// targets[q] of them, by generating sequential names and routing each through f1raw.PartitionOf
// until each partition's quota is filled. It is how the uniformity test builds a genuinely skewed
// set: the natural hash spread is even, so to prove skew-invariance the test must force lopsided
// partitions rather than hope for them.
func membersForPartitions(p int, targets []int) []string {
	need := make([]int, p)
	copy(need, targets)
	remaining := 0
	for _, t := range targets {
		remaining += t
	}
	out := make([]string, 0, remaining)
	for i := 0; remaining > 0; i++ {
		m := fmt.Sprintf("u%07d", i)
		q := f1raw.PartitionOf([]byte(m), p)
		if need[q] > 0 {
			out = append(out, m)
			need[q]--
			remaining--
		}
	}
	return out
}

// parseBulk pulls the payload out of a RESP bulk-string reply ($<n>\r\n<payload>\r\n), reporting
// false for a nil reply ($-1\r\n or the RESP3 _\r\n) so a test reads a drawn member without carrying
// a full RESP decoder.
func parseBulk(s string) (string, bool) {
	if s == "$-1\r\n" || s == "_\r\n" {
		return "", false
	}
	if len(s) == 0 || s[0] != '$' {
		return "", false
	}
	i := strings.Index(s, "\r\n")
	if i < 0 {
		return "", false
	}
	return strings.TrimSuffix(s[i+2:], "\r\n"), true
}

// parseArrayBulks decodes a RESP array of bulk strings (*<n>\r\n then n bulks), returning the
// payloads in order. A nil element decodes to the empty string. It fails the test on a malformed
// reply so a shape bug surfaces as a clear failure rather than a silent miscount.
func parseArrayBulks(t *testing.T, s string) []string {
	t.Helper()
	if len(s) == 0 || s[0] != '*' {
		t.Fatalf("not an array reply: %q", s)
	}
	i := strings.Index(s, "\r\n")
	if i < 0 {
		t.Fatalf("truncated array header: %q", s)
	}
	n, err := strconv.Atoi(s[1:i])
	if err != nil {
		t.Fatalf("bad array count in %q: %v", s, err)
	}
	rest := s[i+2:]
	out := make([]string, 0, n)
	for k := 0; k < n; k++ {
		if strings.HasPrefix(rest, "$-1\r\n") {
			out = append(out, "")
			rest = rest[5:]
			continue
		}
		j := strings.Index(rest, "\r\n")
		if j < 0 || len(rest) == 0 || rest[0] != '$' {
			t.Fatalf("bad bulk element %d in %q", k, s)
		}
		l, err := strconv.Atoi(rest[1:j])
		if err != nil {
			t.Fatalf("bad bulk length element %d in %q: %v", k, s, err)
		}
		payload := rest[j+2 : j+2+l]
		out = append(out, payload)
		rest = rest[j+2+l+2:]
	}
	return out
}

func srandmemberFn(c *connState, a [][]byte) { c.cmdSRandMember(a) }
func spopFn(c *connState, a [][]byte)        { c.cmdSPop(a) }
func saddFn(c *connState, a [][]byte)        { c.cmdSAdd(a) }
func scardFn(c *connState, a [][]byte)       { c.cmdSCard(a) }

// TestSetPartitionDrawUniform builds a set with deliberately lopsided partitions and draws one member
// through the routed no-count SRANDMEMBER many times, asserting every live member comes out with
// frequency close to draws/total. This is the slice-4 DoD "a statistical test confirms every live
// member is drawn with probability 1/total within sampling tolerance across skewed partition sizes":
// the weighted scheme's uniformity is exactly what cancels the skew, so a fat partition's members
// must appear no more often than a thin partition's despite the fat partition being picked more often.
func TestSetPartitionDrawUniform(t *testing.T) {
	const p = 8
	// Lopsided on purpose: an 18x spread between the fattest and thinnest partition.
	targets := []int{45, 5, 30, 9, 18, 6, 40, 12}
	members := membersForPartitions(p, targets)
	total := len(members)

	srv := newPartServer(t, p)
	defer srv.Close()
	c := bareConn(srv)
	call(c, saddFn, append([]string{"SADD", "hot"}, members...)...)

	// Sanity: the partitions really are skewed, so the test proves skew-invariance rather than
	// riding an accidentally even split.
	minT, maxT := targets[0], targets[0]
	for _, tg := range targets {
		if tg < minT {
			minT = tg
		}
		if tg > maxT {
			maxT = tg
		}
	}
	if maxT < minT*3 {
		t.Fatalf("targets not skewed enough: min %d max %d", minT, maxT)
	}

	const perMember = 300
	draws := perMember * total
	tally := make(map[string]int, total)
	for i := 0; i < draws; i++ {
		m, ok := parseBulk(call(c, srandmemberFn, "SRANDMEMBER", "hot"))
		if !ok {
			t.Fatalf("draw %d returned nil on a non-empty set", i)
		}
		tally[m]++
	}

	// Every live member must have been drawn, and none outside the set.
	if len(tally) != total {
		t.Fatalf("drew %d distinct members, want all %d", len(tally), total)
	}
	want := float64(draws) / float64(total)
	member := make(map[string]struct{}, total)
	for _, m := range members {
		member[m] = struct{}{}
	}
	// perMember=300 gives an expected 300 per member with stddev ~17, so a +/-40% band is ~7 sigma:
	// a real skew leak (a partition's members over- or under-drawn) blows past it, random noise does
	// not.
	lo, hi := want*0.6, want*1.4
	for m, got := range tally {
		if _, live := member[m]; !live {
			t.Fatalf("drew member %q that is not in the set", m)
		}
		if float64(got) < lo || float64(got) > hi {
			t.Fatalf("member %q drawn %d times, want within [%.0f, %.0f] (expected %.0f): skew leak",
				m, got, lo, hi, want)
		}
	}
}

// TestSetPartitionDrawDistinct pins the without-replacement forms: SRANDMEMBER with a positive count
// returns distinct members capped at the cardinality, and SPOP with a count returns distinct members
// and actually removes them. Both route through the weighted draw under P>1.
func TestSetPartitionDrawDistinct(t *testing.T) {
	for _, p := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("P=%d", p), func(t *testing.T) {
			srv := newPartServer(t, p)
			defer srv.Close()
			c := bareConn(srv)
			const card = 100
			members := make([]string, card)
			for i := range members {
				members[i] = fmt.Sprintf("m%04d", i)
			}
			live := make(map[string]struct{}, card)
			for _, m := range members {
				live[m] = struct{}{}
			}
			call(c, saddFn, append([]string{"SADD", "hot"}, members...)...)

			// SRANDMEMBER positive count: distinct, all live, exactly count when count <= card.
			got := parseArrayBulks(t, call(c, srandmemberFn, "SRANDMEMBER", "hot", "40"))
			assertDrawDistinctSubset(t, "SRANDMEMBER 40", got, 40, live)
			// count past the cardinality caps at card, still distinct.
			got = parseArrayBulks(t, call(c, srandmemberFn, "SRANDMEMBER", "hot", "1000"))
			assertDrawDistinctSubset(t, "SRANDMEMBER 1000", got, card, live)
			// The set is untouched by the reads.
			if s := call(c, scardFn, "SCARD", "hot"); s != ":100\r\n" {
				t.Fatalf("SCARD after SRANDMEMBER reads = %q, want :100", s)
			}

			// SPOP count: distinct, all live, and destructive.
			popped := parseArrayBulks(t, call(c, spopFn, "SPOP", "hot", "30"))
			assertDrawDistinctSubset(t, "SPOP 30", popped, 30, live)
			if s := call(c, scardFn, "SCARD", "hot"); s != ":70\r\n" {
				t.Fatalf("SCARD after SPOP 30 = %q, want :70", s)
			}
			// The popped members are gone: popping them again cannot return them.
			for _, m := range popped {
				delete(live, m)
			}
			// SPOP past the remaining cardinality returns the whole rest and deletes the set.
			rest := parseArrayBulks(t, call(c, spopFn, "SPOP", "hot", "1000"))
			assertDrawDistinctSubset(t, "SPOP 1000", rest, 70, live)
			if s := call(c, scardFn, "SCARD", "hot"); s != ":0\r\n" {
				t.Fatalf("SCARD after draining SPOP = %q, want :0", s)
			}
		})
	}
}

// assertDrawDistinctSubset checks a drawn slice has exactly wantLen elements, no duplicates, and every
// element is a live member. It is the without-replacement invariant the positive-count forms owe.
func assertDrawDistinctSubset(t *testing.T, what string, got []string, wantLen int, live map[string]struct{}) {
	t.Helper()
	if len(got) != wantLen {
		t.Fatalf("%s returned %d members, want %d", what, len(got), wantLen)
	}
	seen := make(map[string]struct{}, len(got))
	for _, m := range got {
		if _, dup := seen[m]; dup {
			t.Fatalf("%s returned duplicate member %q (without-replacement violated)", what, m)
		}
		seen[m] = struct{}{}
		if _, ok := live[m]; !ok {
			t.Fatalf("%s returned member %q not in the set", what, m)
		}
	}
}

// TestSetPartitionDrawReplacement pins the with-replacement form: SRANDMEMBER with a negative count
// returns exactly abs(count) members, uncapped by the cardinality, so a count larger than the set
// forces repeats. It routes through the weighted draw under P>1.
func TestSetPartitionDrawReplacement(t *testing.T) {
	const p = 4
	srv := newPartServer(t, p)
	defer srv.Close()
	c := bareConn(srv)
	const card = 12
	members := make([]string, card)
	live := make(map[string]struct{}, card)
	for i := range members {
		members[i] = fmt.Sprintf("m%02d", i)
		live[members[i]] = struct{}{}
	}
	call(c, saddFn, append([]string{"SADD", "hot"}, members...)...)

	// abs(count) draws with replacement, uncapped: ask for 3x the cardinality.
	n := card * 3
	got := parseArrayBulks(t, call(c, srandmemberFn, "SRANDMEMBER", "hot", strconv.Itoa(-n)))
	if len(got) != n {
		t.Fatalf("SRANDMEMBER -%d returned %d members, want %d (uncapped with replacement)", n, len(got), n)
	}
	distinct := make(map[string]struct{}, len(got))
	for _, m := range got {
		if _, ok := live[m]; !ok {
			t.Fatalf("SRANDMEMBER -%d returned member %q not in the set", n, m)
		}
		distinct[m] = struct{}{}
	}
	// 36 draws from a 12-member set must repeat: more draws than members.
	if len(distinct) == len(got) {
		t.Fatalf("SRANDMEMBER -%d returned all-distinct members, expected repeats with replacement", n)
	}
}

// TestSetPartitionDrawEmpty pins the missing-and-empty replies through the routed path: no-count
// SRANDMEMBER and SPOP on a missing key return nil, and the count forms return an empty array, exactly
// as the unpartitioned path does.
func TestSetPartitionDrawEmpty(t *testing.T) {
	const p = 4
	srv := newPartServer(t, p)
	defer srv.Close()
	c := bareConn(srv)

	if r := call(c, srandmemberFn, "SRANDMEMBER", "missing"); r != "$-1\r\n" {
		t.Fatalf("SRANDMEMBER missing = %q, want $-1", r)
	}
	if r := call(c, spopFn, "SPOP", "missing"); r != "$-1\r\n" {
		t.Fatalf("SPOP missing = %q, want $-1", r)
	}
	if r := call(c, srandmemberFn, "SRANDMEMBER", "missing", "5"); r != "*0\r\n" {
		t.Fatalf("SRANDMEMBER missing 5 = %q, want *0", r)
	}
	if r := call(c, srandmemberFn, "SRANDMEMBER", "missing", "-5"); r != "*0\r\n" {
		t.Fatalf("SRANDMEMBER missing -5 = %q, want *0", r)
	}
	if r := call(c, spopFn, "SPOP", "missing", "5"); r != "*0\r\n" {
		t.Fatalf("SPOP missing 5 = %q, want *0", r)
	}
}

// BenchmarkSetPartitionPopContention hammers one hot key with concurrent SPOP from many goroutines,
// re-adding each popped member so the set never drains, and reports the per-op cost as P rises. At
// P=1 every pop serializes on the set's single stripe lock; at P>1 pops routing to different
// partitions take different partition locks and run on different cores, which is the write-scaling
// the partitioning exists to buy. The re-add keeps the cardinality steady so the measurement is the
// draw path, not a shrinking set.
//
// Measured (32-thread box, cpu=32): the per-op cost does not yet fall with P; it rises, ~177ns at P=1
// to ~275ns at P=16. The cause is the weighted draw's count read, which resolves each partition's
// vector with a shard-map lookup (weightedCounts -> collPartVec -> sh.get), so it costs O(P) map
// lookups per draw, and that tax grows with P faster than splitting the tiny pop critical section
// across P locks saves at this critical-section size. The spec's intended count source is the
// per-partition atomic counter (doc 19 section 5), an O(P) atomic-load read with no map lookups; a
// following slice caches the P vector pointers per set so the count read becomes those atomic loads.
// That cache must coordinate with CollRandDrop and RENAME invalidation, so it is its own slice, not
// folded into the draw routing this benchmark exercises. The correctness the routing owes (uniformity
// across skew, distinct without-replacement, repeats with replacement, empty replies) is pinned by
// the tests above and holds under the race detector; the contention scaling waits on that cache.
func BenchmarkSetPartitionPopContention(b *testing.B) {
	for _, p := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("P=%d", p), func(b *testing.B) {
			srv := newPartServer(b, p)
			defer srv.Close()
			key := "hot"
			c0 := bareConn(srv)
			fill := []string{"SADD", key}
			for i := 0; i < 8192; i++ {
				fill = append(fill, fmt.Sprintf("m%08d", i))
			}
			call(c0, saddFn, fill...)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				c := bareConn(srv)
				spop := [][]byte{[]byte("SPOP"), []byte(key)}
				add := [][]byte{[]byte("SADD"), []byte(key), nil}
				memberBuf := make([]byte, 0, 32)
				for pb.Next() {
					c.out = c.out[:0]
					c.cmdSPop(spop)
					m, ok := parseBulkBytes(c.out)
					if !ok {
						continue
					}
					// Copy the popped member out of the reply buffer before SADD reuses that buffer,
					// then re-add it so the set stays the same size across the run.
					memberBuf = append(memberBuf[:0], m...)
					add[2] = memberBuf
					c.out = c.out[:0]
					c.cmdSAdd(add)
				}
			})
		})
	}
}

// BenchmarkSetPartitionPopDrain mirrors the wire SPOP workload the two-box evidence measured: one
// large hot key drained by many concurrent poppers with no re-add, so the pop path is measured in
// isolation instead of mixed with SADD's own ordered-index insert. Run it with a fixed iteration
// count below the prefill size (go test -benchtime=400000x) so the set never empties and every
// iteration is a real pop. The set is large (prefillDrain members) on purpose: the ordered-index
// splice the pop drives is O(log n) held under the store's single global index lock, so its cost and
// the contention it creates grow with the set, which an 8192-member cache-resident set hides. The
// deferred-splice pop (this file's fix) takes that splice off the reply path, so the poppers stay on
// their split partition stripes instead of re-serializing on the one index lock.
const prefillDrain = 1 << 20

func BenchmarkSetPartitionPopDrain(b *testing.B) {
	for _, p := range []int{1, 8} {
		b.Run(fmt.Sprintf("P=%d", p), func(b *testing.B) {
			srv := newPartServer(b, p)
			defer srv.Close()
			key := "hot"
			c0 := bareConn(srv)
			// Prefill in chunks so one SADD argv does not balloon to a million elements.
			const chunk = 4096
			for base := 0; base < prefillDrain; base += chunk {
				args := make([]string, 0, chunk+2)
				args = append(args, "SADD", key)
				for i := base; i < base+chunk && i < prefillDrain; i++ {
					args = append(args, fmt.Sprintf("ele:%012d", i))
				}
				call(c0, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, args...)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				c := bareConn(srv)
				spop := [][]byte{[]byte("SPOP"), []byte(key)}
				for pb.Next() {
					c.out = c.out[:0]
					c.cmdSPop(spop)
				}
			})
		})
	}
}

// parseBulkBytes is the allocation-free byte form of parseBulk for the hot benchmark loop: it returns
// the member payload as a subslice of the reply buffer rather than a fresh string.
func parseBulkBytes(s []byte) ([]byte, bool) {
	if len(s) == 0 || s[0] != '$' {
		return nil, false
	}
	i := indexCRLF(s)
	if i < 0 {
		return nil, false
	}
	body := s[i+2:]
	if len(body) < 2 {
		return nil, false
	}
	return body[:len(body)-2], true
}

func indexCRLF(s []byte) int {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '\r' && s[i+1] == '\n' {
			return i
		}
	}
	return -1
}
