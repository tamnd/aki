package set

import (
	"math/bits"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The partitioned band tests (spec 2064/f3/11 section 4, lab 04). They pin the
// four properties the band owes: the frozen engagement and P formula, the
// one-way transition that survives an SPOP drain, the exactly-uniform weighted
// draw over skewed partitions, and the SSCAN cursor composed across partitions
// and across a mid-scan split. The threshold is a var so the tests engage the
// band at a few hundred members instead of 262144, the same test-seam pattern
// algebraMaintain uses; deriveP reads it, so the whole P walk scales down with
// it.

// withThreshold lowers the engagement threshold for one test and restores it on
// cleanup, so the band engages cheaply without the production 262144-member
// build. Tests in this package do not run in parallel, so the shared var is safe.
func withThreshold(t *testing.T, n int) {
	t.Helper()
	old := partitionThreshold
	partitionThreshold = n
	t.Cleanup(func() { partitionThreshold = old })
}

// TestDeriveP pins the frozen P formula at the production threshold (lab 04): P4
// up to 4x the target, then doubling as the count crosses target times P, and
// floored at 4 so P=2 never occurs. It also sweeps a wide range and asserts P is
// always a power of two and never 2.
func TestDeriveP(t *testing.T) {
	withThreshold(t, partTarget) // pin the production threshold regardless of run order
	cases := []struct {
		card, want int
	}{
		{0, 4},
		{1, 4},
		{partTarget, 4},
		{4 * partTarget, 4},
		{4*partTarget + 1, 8},
		{8 * partTarget, 8},
		{8*partTarget + 1, 16},
		{16 * partTarget, 16},
		{16*partTarget + 1, 32},
	}
	for _, tc := range cases {
		if got := deriveP(tc.card); got != tc.want {
			t.Errorf("deriveP(%d) = %d, want %d", tc.card, got, tc.want)
		}
	}
	// The whole operating range: P is a power of two, at least 4, and never 2.
	for card := 0; card <= 64*partTarget; card += partTarget / 4 {
		p := deriveP(card)
		if p == 2 {
			t.Fatalf("deriveP(%d) = 2, the floor must skip P=2 (L5)", card)
		}
		if p < partFloorP {
			t.Fatalf("deriveP(%d) = %d, below the floor %d", card, p, partFloorP)
		}
		if bits.OnesCount(uint(p)) != 1 {
			t.Fatalf("deriveP(%d) = %d, not a power of two", card, p)
		}
	}
}

// TestPartOfRange checks the router lands every hash in [0, P) and uses only the
// top log2(P) bits, so placement is independent of the sub-table's own low-bit
// hashing.
func TestPartOfRange(t *testing.T) {
	for _, p := range []int{4, 8, 16, 32} {
		for _, h := range []uint64{0, 1, 1 << 40, ^uint64(0), 0x8000000000000000, 0x1234567890abcdef} {
			idx := partOf(h, p)
			if idx < 0 || idx >= p {
				t.Fatalf("partOf(%#x, %d) = %d, out of range [0,%d)", h, p, idx, p)
			}
		}
		// The top bits alone decide the partition: a hash of all-ones lands in the
		// last partition, a hash of zero in the first.
		if got := partOf(^uint64(0), p); got != p-1 {
			t.Errorf("partOf(all-ones, %d) = %d, want %d", p, got, p-1)
		}
		if got := partOf(0, p); got != 0 {
			t.Errorf("partOf(0, %d) = %d, want 0", p, got)
		}
	}
}

// TestPartitionEngagement grows a native set through the threshold and checks the
// one-way transition: the encoding flips to partitioned exactly at the threshold,
// OBJECT ENCODING still reports hashtable (Redis parity), and every member
// survives the split.
func TestPartitionEngagement(t *testing.T) {
	const threshold = 300
	withThreshold(t, threshold)
	all := members16(threshold + 200)

	s := newSet([]byte("seed-not-an-int"))
	for i, m := range all {
		s.add(m)
		card := i + 1
		switch {
		case card < threshold && s.enc == encPartitioned:
			t.Fatalf("engaged the partitioned band at card %d, before the threshold %d", card, threshold)
		case card >= threshold && s.enc != encPartitioned:
			t.Fatalf("card %d at or past the threshold %d but enc = %s", card, threshold, s.enc)
		}
	}
	if s.enc.String() != "hashtable" {
		t.Fatalf("OBJECT ENCODING = %q, want hashtable for Redis parity", s.enc.String())
	}
	if s.card() != len(all) {
		t.Fatalf("card = %d after the split, want %d", s.card(), len(all))
	}
	for _, m := range all {
		if !s.has(m) {
			t.Fatalf("member %x lost across the engagement split", m)
		}
	}
}

// TestPartitionOneWayDrain drains a partitioned set below the threshold with SPOP
// and checks it never converts back down (F4): the encoding stays partitioned all
// the way to empty, and every member comes back exactly once.
func TestPartitionOneWayDrain(t *testing.T) {
	const threshold = 256
	withThreshold(t, threshold)
	all := members16(threshold + 100)
	s := buildHT(all)
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned after a build past the threshold", s.enc)
	}

	g := newTestReg()
	var sc [64]byte
	seen := map[string]int{}
	for s.card() > 0 {
		m := string(s.popOne(g, sc[:]))
		if seen[m] != 0 {
			t.Fatalf("SPOP returned %x twice, the drain is not a uniform sample without replacement", m)
		}
		seen[m]++
		if s.enc != encPartitioned {
			t.Fatalf("enc = %s at card %d, a partitioned set must not convert back down (F4)", s.enc, s.card())
		}
	}
	if len(seen) != len(all) {
		t.Fatalf("drain covered %d members, want %d", len(seen), len(all))
	}
}

// TestPartitionPointOps checks SADD/SREM/SISMEMBER/SCARD over the band: present
// members hit, absent ones miss, removal reports presence and drops the count,
// and a re-add after a remove lands again.
func TestPartitionPointOps(t *testing.T) {
	const threshold = 128
	withThreshold(t, threshold)
	all := members16(500)
	s := buildHT(all)
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned", s.enc)
	}

	for _, m := range all {
		if !s.has(m) {
			t.Fatalf("present member %x missed", m)
		}
	}
	absent := members16(1000)[500:]
	for _, m := range absent {
		if s.has(m) {
			t.Fatalf("absent member %x reported present", m)
		}
	}
	// Remove the first hundred, confirm they go and the count tracks.
	for _, m := range all[:100] {
		if !s.rem(m) {
			t.Fatalf("rem(%x) reported not present", m)
		}
	}
	if s.rem(all[0]) {
		t.Fatal("second rem of the same member reported present")
	}
	if s.card() != 400 {
		t.Fatalf("card = %d after removing 100 of 500, want 400", s.card())
	}
	// Re-add lands and lifts the count back.
	if !s.add(all[0]) {
		t.Fatal("re-add of a removed member reported already present")
	}
	if s.card() != 401 {
		t.Fatalf("card = %d after one re-add, want 401", s.card())
	}
}

// TestPartitionAtBijection sweeps every flat draw index and checks the weighted
// locate maps [0, total) onto the members one-to-one, even with a fully drained
// partition in the middle: the branchless prefix scan must skip a zero-width
// partition and still cover every present member exactly once. This is the
// structural exactness the chi-squared draw test then confirms statistically.
func TestPartitionAtBijection(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(1024)
	s := buildHT(all)
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned", s.enc)
	}
	pt := s.part

	// Drain partition 0 completely to force a zero-width partition the locate scan
	// must step over.
	drained := 0
	for _, m := range all {
		if partOf(store.Hash(m), len(pt.parts)) == 0 {
			s.rem(m)
			drained++
		}
	}
	if drained == 0 {
		t.Fatal("no member routed to partition 0; pick a different corpus")
	}
	if pt.counts[0] != 0 {
		t.Fatalf("partition 0 count = %d after draining, want 0", pt.counts[0])
	}

	// The live set, by each().
	live := map[string]bool{}
	s.each(func(m []byte) { live[string(m)] = true })
	if len(live) != s.card() {
		t.Fatalf("each visited %d members, card is %d", len(live), s.card())
	}

	// at(i) over [0, card) must be a permutation of the live members.
	hit := map[string]int{}
	for i := 0; i < s.card(); i++ {
		hit[string(pt.at(i))]++
	}
	if len(hit) != len(live) {
		t.Fatalf("at swept %d distinct members over %d indices, want %d", len(hit), s.card(), len(live))
	}
	for m, c := range hit {
		if !live[m] {
			t.Fatalf("at returned %x, which is not a live member", m)
		}
		if c != 1 {
			t.Fatalf("at returned %x %d times, want exactly once (the mapping must be a bijection)", m, c)
		}
	}
}

// TestPartitionDrawUniform runs the exactly-uniform weighted draw over a
// partitioned set on a fixed PCG and checks every member is drawn with equal
// frequency, including after a partition is drained to zero so the per-partition
// weights are skewed. Uniformity here is F15 across the partition boundary: a
// member in a light partition is exactly as likely as one in a heavy partition.
func TestPartitionDrawUniform(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(768)
	s := buildHT(all)
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned", s.enc)
	}

	// Skew the weights: drain one partition and thin another, so the counts differ
	// widely across partitions and the prefix-sum weighting is actually exercised.
	pt := s.part
	for _, m := range all {
		switch partOf(store.Hash(m), len(pt.parts)) {
		case 0:
			s.rem(m) // partition 0 emptied
		case 1:
			if pt.counts[1] > 3 {
				s.rem(m) // partition 1 thinned to a handful
			}
		}
	}

	card := s.card()
	index := map[string]int{}
	s.each(func(m []byte) { index[string(m)] = len(index) })
	if len(index) != card {
		t.Fatalf("each visited %d, card %d", len(index), card)
	}

	g := newTestReg()
	const perCat = 300
	hits := make([]int, card)
	var sc [64]byte
	for i := 0; i < card*perCat; i++ {
		m := string(s.drawOne(g, sc[:]))
		j, ok := index[m]
		if !ok {
			t.Fatalf("draw returned %x, not a live member", m)
		}
		hits[j]++
	}
	for j, h := range hits {
		if h == 0 {
			t.Fatalf("member at index %d was never drawn across %d draws", j, card*perCat)
		}
	}
	df := float64(card - 1)
	if stat := chiSquare(hits); stat > 2*df {
		t.Fatalf("chi-squared %.1f over %d skewed-partition categories, want < %.1f", stat, card, 2*df)
	}
}

// TestPartitionScanFullPass pages a static partitioned set to completion and
// checks the composed cursor returns every member exactly once and terminates.
// drainScan walks s.scanPage, which dispatches to the partition walk.
func TestPartitionScanFullPass(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(500)
	s := buildHT(all)
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned", s.enc)
	}
	seen, pages := drainScan(s, 16)
	guard := s.card()/16 + 8
	if pages > guard {
		t.Fatalf("scan ran %d pages, past the %d-page termination bound", pages, guard)
	}
	if len(seen) != len(all) {
		t.Fatalf("static scan saw %d distinct members, want %d", len(seen), len(all))
	}
	for _, m := range all {
		if seen[string(m)] != 1 {
			t.Fatalf("member %x seen %d times on a static scan, want exactly 1", m, seen[string(m)])
		}
	}
}

// TestPartitionScanGrowthMidScan grows a partitioned set across a scan hard
// enough to trigger a split (P doubles), and checks every member present
// throughout still comes back. The split bumps pgen, so the carried cursor goes
// stale and the scan restarts, which is how at-least-once is preserved across a
// repartition.
func TestPartitionScanGrowthMidScan(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	// Start at 4x threshold (P4) and grow past 4x so the target reaches P8.
	orig := members16(4 * threshold)
	extra := members16(9 * threshold)[4*threshold:]
	s := buildHT(orig)
	startP := len(s.part.parts)

	seen := map[string]int{}
	var cur uint64
	grown := false
	pages := 0
	for {
		pages++
		next := s.scanPage(cur, 16, nil, func(m []byte) { seen[string(m)]++ })
		if !grown && next != 0 {
			for _, m := range extra {
				s.add(m)
			}
			grown = true
		}
		if next == 0 {
			break
		}
		cur = next
		if pages > 400 {
			t.Fatalf("scan ran %d pages without terminating after growth", pages)
		}
	}
	if !grown {
		t.Fatal("scan finished before the mid-scan growth fired")
	}
	if len(s.part.parts) <= startP {
		t.Fatalf("growth did not repartition: P stayed at %d", startP)
	}
	for _, m := range orig {
		if seen[string(m)] == 0 {
			t.Fatalf("member %x present throughout was never returned across the split", m)
		}
	}
}

// TestScanEngagementMidScan crosses the engagement threshold mid-scan: a native
// set carried a downward cursor whose implicit generation is 0, so once the set
// splits into the partitioned band the stale cursor restarts the scan and every
// member that was present throughout still comes back at least once.
func TestScanEngagementMidScan(t *testing.T) {
	const threshold = 300
	withThreshold(t, threshold)
	orig := members16(threshold - 1)
	extra := members16(threshold + 100)[threshold-1:]
	s := buildHT(orig)
	if s.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable before the threshold", s.enc)
	}

	seen := map[string]int{}
	var cur uint64
	crossed := false
	pages := 0
	for {
		pages++
		next := s.scanPage(cur, 16, nil, func(m []byte) { seen[string(m)]++ })
		if !crossed && next != 0 {
			for _, m := range extra {
				s.add(m)
			}
			crossed = true
		}
		if next == 0 {
			break
		}
		cur = next
		if pages > 400 {
			t.Fatalf("scan ran %d pages without terminating after engagement", pages)
		}
	}
	if !crossed {
		t.Fatal("scan finished before the engagement crossing fired")
	}
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned after crossing the threshold mid-scan", s.enc)
	}
	for _, m := range orig {
		if seen[string(m)] == 0 {
			t.Fatalf("member %x present before and after engagement was never returned", m)
		}
	}
}

// TestPartitionScanRemoveMidScan churns a partitioned set with swap-remove during
// a scan and checks every member that is never removed still comes back, and the
// scan terminates. Swap-remove stays within a partition, so the carried native
// proof holds per partition and composes across the walk.
func TestPartitionScanRemoveMidScan(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(600)
	s := buildHT(all)
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned", s.enc)
	}
	stable := all[:300]
	churn := all[300:]

	seen := map[string]int{}
	var cur uint64
	fired := false
	pages := 0
	for {
		pages++
		next := s.scanPage(cur, 16, nil, func(m []byte) { seen[string(m)]++ })
		if !fired && next != 0 {
			for _, m := range churn {
				s.rem(m)
			}
			fired = true
		}
		if next == 0 {
			break
		}
		cur = next
		if pages > 200 {
			t.Fatalf("scan ran %d pages without terminating after churn", pages)
		}
	}
	if !fired {
		t.Fatal("scan finished before the mid-scan removal fired")
	}
	for _, m := range stable {
		if seen[string(m)] == 0 {
			t.Fatalf("stable member %x was skipped by the churned partitioned scan", m)
		}
	}
}

// TestPartitionScanMatch checks MATCH composes with the partitioned cursor: only
// matching members come back, and all of them do, across every partition.
func TestPartitionScanMatch(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	s := newSet([]byte("seed-not-int"))
	want := map[string]bool{}
	for i := 0; i < 400; i++ {
		user := []byte("user:" + itoa(int64(i)))
		s.add(user)
		want[string(user)] = true
		s.add([]byte("admin:" + itoa(int64(i))))
	}
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned", s.enc)
	}
	seen := map[string]int{}
	var cur uint64
	pages := 0
	for {
		pages++
		next := s.scanPage(cur, 16, []byte("user:*"), func(m []byte) { seen[string(m)]++ })
		if next == 0 {
			break
		}
		cur = next
		if pages > 200 {
			t.Fatalf("MATCH scan ran %d pages without terminating", pages)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("matched %d members, want %d user:* members", len(seen), len(want))
	}
	for k := range want {
		if seen[k] == 0 {
			t.Fatalf("user member %q did not survive the partitioned MATCH scan", k)
		}
	}
}

// TestPartitionStorePreBuild checks a STORE destination that crosses the
// threshold at build time is born partitioned, not left as one oversized native
// table (doc 11 section 7): storeResult routes through partitionedSet, and every
// emitted member lands.
func TestPartitionStorePreBuild(t *testing.T) {
	const threshold = 256
	withThreshold(t, threshold)
	all := members16(threshold + 300)
	result := storeResult(len(all), func(emit func(m []byte)) {
		for _, m := range all {
			emit(m)
		}
		// Emit a run of duplicates: the destination table is the only dedup, so the
		// result cardinality must still be the distinct count.
		for _, m := range all[:50] {
			emit(m)
		}
	})
	if result == nil {
		t.Fatal("storeResult returned nil for a non-empty union")
	}
	if result.enc != encPartitioned {
		t.Fatalf("destination enc = %s, want partitioned when the build crosses the threshold", result.enc)
	}
	if result.card() != len(all) {
		t.Fatalf("destination card = %d, want %d distinct members", result.card(), len(all))
	}
	for _, m := range all {
		if !result.has(m) {
			t.Fatalf("member %x missing from the pre-partitioned destination", m)
		}
	}
}

// TestPartitionMembersStream drains the streamed SMEMBERS reply for a partitioned
// set and checks it frames exactly the set: one array header for the whole set,
// then every member across every partition, once each. The tiny drain buffer
// forces the encoder to straddle chunk boundaries mid-element.
func TestPartitionMembersStream(t *testing.T) {
	const threshold = 64
	withThreshold(t, threshold)
	all := members16(500)
	s := buildHT(all)
	if s.enc != encPartitioned {
		t.Fatalf("enc = %s, want partitioned", s.enc)
	}
	total := s.part.membersTotal()
	src := s.part.pinMembersStream()
	defer src.Release()

	dst := make([]byte, 37) // deliberately not a frame multiple
	var out []byte
	for int64(len(out)) < total {
		n, err := src.Next(dst)
		if err != nil {
			t.Fatalf("stream Next: %v", err)
		}
		if n == 0 {
			break
		}
		out = append(out, dst[:n]...)
	}
	if int64(len(out)) != total {
		t.Fatalf("stream produced %d bytes, membersTotal said %d", len(out), total)
	}
	got := parseArray(t, out)
	if len(got) != len(all) {
		t.Fatalf("stream framed %d members, want %d", len(got), len(all))
	}
	seen := map[string]int{}
	for _, m := range got {
		seen[m]++
	}
	for _, m := range all {
		if seen[string(m)] != 1 {
			t.Fatalf("member %x framed %d times in the stream, want exactly 1", m, seen[string(m)])
		}
	}
}
