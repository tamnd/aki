package f1srv

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f1raw"
)

// Set algebra (SINTER/SUNION/SDIFF and SINTERCARD) reads each source set by enumerating its
// dense member vector, never the global ordered index and never a whole-source materialize
// (spec 2064/f1_rewrite_ltm/20). A set owes no member order, so the algebra does not need one:
// SINTER drives off the smallest source and point-probes the rest through the hash index, SDIFF
// walks the first source and rejects any member the others hold, and SUNION enumerates every
// source and deduplicates through a seen-set. None of the three depends on the sources arriving
// in sorted order, which is what lets them read the unordered vector instead of the ordered index.
//
// The RESP2 array count has to precede the elements, so all three buffer the qualifying members
// (arena-stable subslices) in one pass and frame the reply from the buffer length, then encode in a
// second pass. Buffering rather than streaming with a deferred-length header is deliberate and
// measured: SINTER and SDIFF are memory-bound on the per-member point-probe into the shared composite
// index, and interleaving reply encoding into the probe loop evicts the index cache lines the next
// probe needs, so the two-phase form runs ~15% faster than streaming (see labs/setalgebra). SUNION
// owes an O(union) seen-set to deduplicate regardless (exactly as Redis's dict-backed SUNION does), so
// buffering the distinct members it discovers is one cheap slice next to walking the sources twice,
// which is what the old count-then-emit form did; single-pass buffering runs a large SUNION ~2x faster.
//
// Locking: an algebra read takes every source set's stripe lock (distinct stripes, in
// ascending index order so it can never deadlock against another multi-key write) for the
// span of the operation, so the sets it reads cannot change under it. setVecEach reads the
// vectors under those already-held locks and takes none of its own.

// setVecEach enumerates every live member of set skey, calling emit with each member (the bare
// member bytes, an arena-stable subslice). It reads the set's dense member vector, not the ordered
// index (spec 2064/f1_rewrite_ltm/20 section 6): an unpartitioned set walks its one whole-set
// vector, a partitioned set walks its P partition vectors in turn. It resolves a partitioned set's
// vectors through the descriptor (SetPartVec*, the same path streamSet and the draw use) so a vector
// this enumeration builds is registered for descriptor-driven teardown and cannot leak past a DEL or
// a grow (section 6.1). emit returns false to stop early, and setVecEach then returns false so a
// caller like SINTERCARD's LIMIT or an intersection driver can cut the walk short. The caller holds
// every source's stripe lock, so the layout and the vectors are stable and setVecEach locks nothing.
//
// Buffer discipline: setVecEach owns its bounding prefix, freshly allocated rather than borrowed from
// the connection's pbuf/ppbuf scratch, because a probing or storing emit reuses those same scratch
// buffers (setMemberExists builds into kbuf, but storeAlgebra's insert builds the destination base
// into ppbuf and the destination prefix into pbuf). A borrowed prefix would be clobbered mid-walk by
// such an emit, dropping every member past the first scan batch; a walk that owns its prefix survives
// any emit. Each yielded member points into the immutable arena, so it stays valid after the scan
// buffer refills and after any probe or store.
func (c *connState) setVecEach(skey []byte, emit func([]byte) bool) bool {
	scan := make([][]byte, 0, hashScanBatch)
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	if p := c.partitionsFor(skey); p > 1 {
		// base = uvarint(len(skey)) | skey | <partByte placeholder>, the partition-scan prefix
		// SetPartVecScanDown rewrites the last byte of per partition (matching partScanBase).
		base := make([]byte, 0, n+len(skey)+1)
		base = append(base, tmp[:n]...)
		base = append(base, skey...)
		base = append(base, 0)
		moff := len(base) // member starts past uvarint(len)|skey|byte(part)
		for part := 0; part < p; part++ {
			hi := -1
			for {
				keys, next := c.srv.store.SetPartVecScanDown(base, p, part, hi, hashScanBatch, scan[:0])
				for _, k := range keys {
					if !emit(k[moff:]) {
						return false
					}
				}
				if next == 0 {
					break
				}
				hi = next
			}
		}
		return true
	}
	// prefix = uvarint(len(skey)) | skey, the whole-set bounding prefix (matching setPrefix).
	prefix := make([]byte, 0, n+len(skey))
	prefix = append(prefix, tmp[:n]...)
	prefix = append(prefix, skey...)
	plen := len(prefix)
	hi := -1
	for {
		keys, next := c.srv.store.SetVecScanDown(prefix, hi, hashScanBatch, scan[:0])
		for _, k := range keys {
			if !emit(k[plen:]) {
				return false
			}
		}
		if next == 0 {
			break
		}
		hi = next
	}
	return true
}

// lockStripes takes the stripe locks for every distinct key in keys, in ascending stripe
// index order so a multi-key read can never deadlock against SMOVE or another algebra call
// that touches an overlapping key set, and returns an unlock closure. A partitioned key
// contributes every one of its partition stripes (stripePart per partition), because its
// member writers hold per-partition stripe write locks, not the whole-key stripe, so a
// whole-key stripe alone would not exclude them. An unpartitioned key contributes its one
// whole-key stripe. Stripes are deduplicated (two partitions or two keys can hash to one
// stripe) and taken in ascending index order, the same global order lockSetPartitionsShared
// uses, so exclusive algebra locks and shared SMEMBERS locks over overlapping partition
// stripes acquire in one order and never form a cycle. The distinct-stripe set stays small,
// so the linear dedup and insertion sort cost nothing measurable.
// When adaptive partitioning is off no key's P ever changes, so one pass locks the exact stripe set.
// When it is armed a migration could grow one of these keys after this reads its P but before it
// takes the stripes, leaving the key's new partitions unlocked. The retry re-reads every key's P
// under the acquired locks and, if any grew, releases and redoes the acquisition over the wider
// layout. It converges because P only ever rises and is bounded by the configured cap, and it takes
// the same ascending stripe-index order every iteration, so a migration holding an overlapping
// superset of stripes and this call can never form a cycle.
func (c *connState) lockStripes(keys [][]byte) func() {
	for {
		idxs := make([]uint32, 0, len(keys))
		add := func(s uint32) {
			for _, e := range idxs {
				if e == s {
					return
				}
			}
			idxs = append(idxs, s)
		}
		ps := make([]int, len(keys))
		for i, k := range keys {
			p := c.partitionsFor(k)
			ps[i] = p
			if p > 1 {
				for part := 0; part < p; part++ {
					add(c.srv.stripePart(k, part))
				}
			} else {
				add(c.srv.stripe(k))
			}
		}
		for i := 1; i < len(idxs); i++ {
			for j := i; j > 0 && idxs[j] < idxs[j-1]; j-- {
				idxs[j], idxs[j-1] = idxs[j-1], idxs[j]
			}
		}
		for _, s := range idxs {
			c.srv.incrMu[s].Lock()
		}
		if c.srv.setPartMax > 1 {
			stale := false
			for i, k := range keys {
				if c.partitionsFor(k) != ps[i] {
					stale = true
					break
				}
			}
			if stale {
				for i := len(idxs) - 1; i >= 0; i-- {
					c.srv.incrMu[idxs[i]].Unlock()
				}
				continue
			}
		}
		return func() {
			for i := len(idxs) - 1; i >= 0; i-- {
				c.srv.incrMu[idxs[i]].Unlock()
			}
		}
	}
}

// anyStringConflict reports whether any of the keys is held by a plain string, in which
// case the whole algebra command is WRONGTYPE. It probes the string namespace only, so it
// never trips over a set's own header or member rows.
func (c *connState) anyStringConflict(keys [][]byte) bool {
	for _, k := range keys {
		if c.stringConflict(k) {
			return true
		}
	}
	return false
}

// sunionEach calls emit once for each distinct member across all source sets. It enumerates every
// source's member vector and deduplicates through a seen-set keyed by the member bytes, so a member
// several sources share is emitted exactly once. The seen-set is O(distinct union) in memory, the
// same cost Redis's dict-backed SUNION pays; there is no sorted-merge shortcut because the vector is
// unordered. emit returns false to stop early; the read SUNION never does, but SUNIONSTORE's insert
// can fail and stop the walk.
func (c *connState) sunionEach(keys [][]byte, emit func([]byte) bool) {
	seen := make(map[string]struct{}, algebraBufCap(c.summedCard(keys)))
	for _, k := range keys {
		stop := false
		c.setVecEach(k, func(m []byte) bool {
			if _, ok := seen[string(m)]; ok {
				return true
			}
			seen[string(m)] = struct{}{}
			if !emit(m) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
		}
	}
}

// sunionEachRaw enumerates every source's members with no deduplication, for SUNIONSTORE. The store
// path's insert already dedups through the destination index (PutKind reports isNew, and count, the
// encoding fold and the sorted-hash buckets all advance only on a new member), so a member several
// sources share is stored once whether or not the walk filters the duplicate first. sunionEach's
// seen-set is therefore pure overhead on the store path: it hashes and stores every member into a
// throwaway Go map sized to the union, an O(union) allocation and hash pass that dominated
// SUNIONSTORE and grew worse with cardinality (the map rehashes as it fills). Dropping it hands the
// raw concatenation straight to insert, which is the dedup the read form's map was duplicating.
// Correctness rests on insert being idempotent per member, which it is. emit returns false to stop
// early when insert hits a write error, the same stop signal sunionEach honors.
func (c *connState) sunionEachRaw(keys [][]byte, emit func([]byte) bool) {
	for _, k := range keys {
		stop := false
		c.setVecEach(k, func(m []byte) bool {
			if !emit(m) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
		}
	}
}

// setMemberExists reports whether member is in set skey, routing the probe to the member's
// partition when skey is partitioned (spec 2064/f1_rewrite_ltm/19 section 6.9). The
// intersection and difference drivers probe non-driver sources one member at a time, and a
// partitioned source stores that member only under its routed partition key, so an
// unpartitioned memberKey probe would miss it. For an unpartitioned set it is byte-identical
// to the plain probe. member is an arena-stable driver member, so building the composite key
// into the per-connection scratch is safe: the result is consumed before the next probe.
func (c *connState) setMemberExists(skey, member []byte) bool {
	if p := c.partitionsFor(skey); p > 1 {
		part := f1raw.PartitionOf(member, p)
		return c.srv.store.ExistsKind(c.partMemberKey(skey, member, part, p), kindSetMember)
	}
	return c.srv.store.ExistsKind(c.memberKey(skey, member), kindSetMember)
}

// algebraBufMaxCap caps a speculative result-buffer preallocation. Sizing a buffer to the exact
// upper bound kills the append growth for a realistic result, but a pathological case (intersecting
// two huge sets whose real overlap is tiny, or a union whose sources barely differ) would otherwise
// preallocate tens of millions of slots the result never fills. The cap bounds that waste: past it
// the buffer starts smaller and doubles a few times, whose cost is negligible against a result that
// large. Eight million slots is 64 MiB of header slice, comfortably above any realistic in-memory
// result and well under a run that would blow the larger-than-memory budget.
const algebraBufMaxCap = 8 << 20

// algebraBufCap turns a cardinality upper bound into a preallocation size, clamped to
// algebraBufMaxCap so a huge bound cannot request an unbounded speculative slice.
func algebraBufCap(card uint64) int {
	if card > algebraBufMaxCap {
		return algebraBufMaxCap
	}
	return int(card)
}

// summedCard returns the sum of the source cardinalities, the upper bound on a union's size (the
// union can hold no more distinct members than the total across its sources). A union sizes its
// seen-set and result buffer with it. The sum saturates at the uint64 ceiling rather than
// wrapping, which only matters for cardinalities no real keyspace reaches; algebraBufCap clamps
// the result to a sane preallocation regardless.
func (c *connState) summedCard(keys [][]byte) uint64 {
	var total uint64
	for _, k := range keys {
		card := c.setCard(k)
		if total+card < total {
			return ^uint64(0)
		}
		total += card
	}
	return total
}

// sinterEach yields every member present in all source sets and returns early when emit returns
// false (SINTERCARD's LIMIT). It drives off the smallest source, chosen from the O(1) header
// cardinalities, and point-probes every other source through the hash index for each of the
// smallest source's members, so the work is bounded by the smallest source. Any empty source
// makes the intersection empty and it yields nothing.
//
// The ordered-index era had a second strategy, a sorted k-way merge over the sources that cost the
// sum of the cardinalities with no per-member probe. That merge existed only because every set's
// members sat in one sort order under the global ordered index (spec 2064/f1_rewrite_ltm/20). The
// dense member vector is unordered, so there is no sorted-merge form to fall back to; SINTER always
// probes off the smallest source, which is the same strategy Redis uses.
func (c *connState) sinterEach(keys [][]byte, emit func([]byte) bool) {
	minCard := ^uint64(0)
	driverIdx := 0
	for i, k := range keys {
		card := c.setCard(k)
		if card == 0 {
			return // an empty source means an empty intersection
		}
		if card < minCard {
			minCard = card
			driverIdx = i
		}
	}
	c.sinterProbeEach(keys, driverIdx, emit)
}

// sinterProbeEach yields the intersection by enumerating the smallest source (driverIdx, already
// chosen from the header counts) and point-probing every other source for each member. The work is
// bounded by the smallest source. A lone source (no other source to probe) yields all its members,
// which is the intersection of one set with itself. emit returns false to stop early.
func (c *connState) sinterProbeEach(keys [][]byte, driverIdx int, emit func([]byte) bool) {
	c.setVecEach(keys[driverIdx], func(m []byte) bool {
		for i, k := range keys {
			if i == driverIdx {
				continue
			}
			if !c.setMemberExists(k, m) {
				return true // not in every source, skip but keep walking the driver
			}
		}
		return emit(m)
	})
}

// sdiffEach walks the first source set and calls emit for each member none of the other sources
// hold, in the first set's enumeration order (spec section 5). SDIFF is not commutative, so the
// first key is always the driver and the rest are probed through the hash index. The result is
// bounded by the first set.
func (c *connState) sdiffEach(keys [][]byte, emit func([]byte) bool) {
	rest := keys[1:]
	c.setVecEach(keys[0], func(m []byte) bool {
		for _, k := range rest {
			if c.setMemberExists(k, m) {
				return true // present in a later source, not in the difference
			}
		}
		return emit(m)
	})
}

// The sorted-hash merge (spec 2064/f1_rewrite_ltm/24) is the structural win the smallest-source probe
// cannot reach: when the folder is on, each set keeps a per-partition array of its member hashes in
// ascending order off the reply path, so an intersection of two sets becomes a two-pointer merge over
// those arrays, work proportional to the sum of the two cardinalities with no per-member point-probe
// into the shared composite index. Redis and Valkey cannot run it: their sets carry no maintained
// sorted view a merge could walk. The merge is fenced to the case where it is both correct and a clear
// win: exactly two sources, both large enough to amortize the fold (setMergeFloor), and comparable in
// size (setMergeMaxRatio) since a wildly asymmetric pair is cheaper to probe off the tiny source. Every
// other shape stays on the doc-20 probe, which is always correct. When eligible the driver holds the
// sources' stripe locks (the caller took them), forces a synchronous fold so the arrays are current,
// then runs the two-pointer merge per partition, fanning the partitions across workers for P>1.
//
// Two operands at the same partition count P pair partition for partition: a shared member lands in the
// same partition index in both (section 4). Two operands at different P do not, because a member routes
// to hash & (P-1) and the same hash picks a different index at different P. Doc 19 grows P with
// cardinality, so even a comparable pair can straddle a power-of-two boundary and sit at different P.
// The driver handles that mixed-P case by re-partitioning the smaller-P operand's already-sorted arrays
// up into the larger operand's P in one O(|small|) pass (a stable bucket-split, since P is a power of
// two and routing is the low bits), then running the same-P merge against the larger operand's real
// partitions (section 5.1). The far-smaller asymmetric pair never reaches the merge at all: the ratio
// gate routes it to the probe, which is at the random-probe floor and needs no re-partition.

const (
	// setMergeFloor is the smallest source cardinality the merge engages at. The GamingPC three-way gate
	// showed the point-probe into the global open-addressed index is not competitive against
	// Redis/Valkey's cache-hot per-set dict even at a few hundred members: at 256 members SINTER ran
	// 0.64x and SINTERCARD 0.64x on the probe and 1.28x / 1.78x once the merge engaged. The old 1024
	// floor was set by the labs/seteager single-key micro (spec 2064/24 section 7), which measured the
	// merge's fold and setup cost in isolation without the probe's cross-set cache-miss penalty a live
	// three-way run pays; against real Redis the merge wins well below 1024. 128 is the crossover the
	// labs/setmergefloor sweep and the GamingPC A/B agree on: at and above it the merge wins, below it
	// (16-member sets) the per-call merge setup outweighs the tiny probe and the driver stays on the
	// probe. See labs/setmergefloor for the small-N merge-vs-probe crossover the floor rests on.
	setMergeFloor = 128
	// setMergeMaxRatio caps how lopsided the two sources may be for the merge to engage. The probe's
	// cost tracks the smaller source while the merge walks both arrays, so once the larger source is
	// more than this many times the smaller, probing off the tiny source wins and the driver stays on
	// the probe. Settled by the labs/seteager driver sweep (spec 2064/f1_rewrite_ltm/24 section 7): the
	// single-thread merge stays ahead through a 4:1 ratio and the probe overtakes by 7:1, so 7 is the
	// crossover. It is conservative on top of that because the real merge fans across P workers while
	// the probe runs single-threaded off the small source, so the parallel merge's true crossover sits
	// at or past the single-thread number.
	setMergeMaxRatio = 7
	// setFanOutFloor is the per-partition element count at or above which the merge fans its P partition
	// pairs across shard workers; below it the P merges run inline on the calling goroutine. The
	// labs/seteager sweep (section 7) put fan-out break-even near 64 members per partition on a 10-core
	// box; 128 is one doubling above it, so the driver spends goroutines only where the parallelism is a
	// clear win and not a coin-flip that adds dispatch variance for no gain. The driver estimates the
	// per-partition count as the smaller source's cardinality divided by P.
	setFanOutFloor = 128
	// setAlgebraOffloadFloor is the larger source cardinality at or above which a big-reply set-algebra
	// read (SINTER/SUNION/SDIFF) hands itself off the epoll reactor loop to a park goroutine
	// (offloadSetAlgebra). Below it the compute is cheap enough that the inline path on the loop beats the
	// handoff's goroutine spawn and two epoll interest re-arms. At or above it the inline compute plus the
	// large multibulk reply write stall the loop long enough to starve the other connections it serves,
	// which is where the inline reactor SINTER dipped to 0.62x of Redis at the 256-member size while flat
	// SET held 2.74x; the handoff moves that work onto a goroutine the Go scheduler spreads across cores
	// so the loop stays responsive and the algebra parallelizes. The threshold matches setMergeFloor's
	// larger neighbour: it sits at the size where the reply grew past the loop's tolerance in the gate
	// profile.
	setAlgebraOffloadFloor = 256
	// setAlgebraCountOffloadFloor is the same handoff threshold for the count form (SINTERCARD), which
	// returns a single integer instead of a multibulk reply. With no large reply to move off the loop, the
	// handoff pays for itself only once the pure intersection compute alone stalls the loop, which happens
	// at a larger cardinality than the reply-carrying forms. The GamingPC crossover sweep put it right at
	// 512: at 256 members offloading dropped SINTERCARD to 0.99x of Redis versus 1.22x inline, while from
	// 512 up it wins (1.26x vs 1.22x at 512, 2.27x vs 1.74x at 2048, 2.46x vs 2.25x at 4096). So the read
	// forms offload from 256 but the count form waits for 512.
	setAlgebraCountOffloadFloor = 512
)

// setMergePrefix builds the sorted-array registry prefix for one partition of set skey, matching the
// prefix the fold registers a member under (sorthashfold.go, randvec.go, partdraw.go): uvarint(len(skey))
// | skey for an unpartitioned set (p == 1), and that run plus the partition byte for one partition of a
// partitioned set (p > 1). It allocates a fresh buffer rather than borrowing the connection's kbuf/pbuf/
// ppbuf scratch, because the merge holds several prefixes live at once (two per partition, and every
// partition's pair concurrently under the fan-out) and the scratch buffers back single-use command keys.
func setMergePrefix(skey []byte, part, p int) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	sz := n + len(skey)
	if p > 1 {
		sz++
	}
	b := make([]byte, 0, sz)
	b = append(b, tmp[:n]...)
	b = append(b, skey...)
	if p > 1 {
		b = append(b, byte(part))
	}
	return b
}

// mergePlan is the resolved sorted-hash merge shape for a two-source algebra command, produced by
// setMergeEligible. p is the partition count the merge runs at (the larger of the two operands' P for
// the mixed case, the shared P otherwise) and lo is the smaller operand's cardinality, which the
// fan-out floor divides by p. When mixed is false the merge pairs keys[0] and keys[1] partition for
// partition. When mixed is true the two operands sit at different P: the merge re-partitions the
// smaller-P operand (smallKey, at pSmall) up into p target buckets and pairs each against the larger-P
// operand (realKey), with bigIsA recording whether the larger-P operand is keys[0], the A operand SDIFF
// keeps as its minuend.
type mergePlan struct {
	p        int
	lo       int
	mixed    bool
	realKey  []byte
	smallKey []byte
	pSmall   int
	bigIsA   bool
}

// setMergeEligible resolves the merge plan for keys and reports whether the sorted-hash merge should
// run. It requires the feature flag and the folder both on, offsets that stay resolvable (the
// non-segmented arena; the merge holds raw offsets the segmented migrator would reclaim, see
// SortedHashMergeStable), exactly two sources, both cardinalities at or above setMergeFloor, and a size
// ratio no wider than setMergeMaxRatio. Equal-P operands merge partition for partition; different-P
// operands re-partition the smaller-P side up into the larger P (the mixed path). The smaller
// cardinality (plan.lo) lets the caller estimate the per-partition element count (lo/p) for the fan-out
// floor. Any miss returns false and the caller keeps the doc-20 probe.
func (c *connState) setMergeEligible(keys [][]byte) (mergePlan, bool) {
	var plan mergePlan
	if !c.srv.setAlgebraMerge || len(keys) != 2 {
		return plan, false
	}
	if !c.srv.store.SortedHashEnabled() || !c.srv.store.SortedHashMergeStable() {
		return plan, false
	}
	c0 := c.setCard(keys[0])
	c1 := c.setCard(keys[1])
	if c0 < setMergeFloor || c1 < setMergeFloor {
		return plan, false
	}
	hi, small := c0, c1
	if small > hi {
		hi, small = small, hi
	}
	if hi/small > setMergeMaxRatio {
		return plan, false
	}
	plan.lo = int(small)
	pA := c.partitionsFor(keys[0])
	pB := c.partitionsFor(keys[1])
	if pA == pB {
		plan.p = pA
		return plan, true
	}
	// Mixed P: the merge re-partitions the smaller-P operand up into the larger operand's P. Because
	// P only ever grows (doc 19) and is a power of two, the larger operand is the real target layout
	// and the smaller-P operand splits into it. bigIsA fixes the A/B order SDIFF depends on.
	plan.mixed = true
	if pA >= pB {
		plan.p = pA
		plan.pSmall = pB
		plan.realKey = keys[0]
		plan.smallKey = keys[1]
		plan.bigIsA = true
	} else {
		plan.p = pB
		plan.pSmall = pA
		plan.realKey = keys[1]
		plan.smallKey = keys[0]
		plan.bigIsA = false
	}
	return plan, true
}

// mergeFanWorkers is the worker cap the merge fans P partition pairs across, applying the fan-out floor
// (section 7): when the estimated per-partition element count (the smaller source's cardinality over P)
// is below setFanOutFloor, the P merges run inline on the calling goroutine (one worker), because the
// goroutine dispatch would outweigh the merges; at or above it the driver fans across every shard
// worker. lo is the smaller cardinality setMergeEligible returned.
func (c *connState) mergeFanWorkers(p, lo int) int {
	if p <= 1 || lo/p < setFanOutFloor {
		return 1
	}
	return c.srv.execShards
}

// fanPartitions runs fn once for each partition index in [0, p), across a bounded pool of at most
// maxWorkers goroutines (further capped at min(p, execShards)), and returns when every partition has
// completed. A single-partition or single-worker fan runs inline with no goroutine, so the common
// unpartitioned merge and any fan below the fan-out floor stay on the calling goroutine. Workers pull
// the next partition index off one shared atomic counter, so the work balances without a per-partition
// goroutine. The caller holds the sources' stripe locks across the whole fan, so every worker reads a
// stable layout.
func (c *connState) fanPartitions(p, maxWorkers int, fn func(part int)) {
	workers := c.srv.execShards
	if workers > maxWorkers {
		workers = maxWorkers
	}
	if workers > p {
		workers = p
	}
	if workers < 1 {
		workers = 1
	}
	if workers == 1 {
		for part := 0; part < p; part++ {
			fn(part)
		}
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				part := int(next.Add(1)) - 1
				if part >= p {
					return
				}
				fn(part)
			}
		}()
	}
	wg.Wait()
}

// setMergeCollect runs a two-source per-partition merge emitter across the shared partition count and
// concatenates the members it yields, or returns nil and false to fall back to the probe. It is the
// shared body of the intersect, diff, and union drivers, which differ only in the engine method they
// pass as part (SetSortedIntersectPart/SetSortedDiffPart/SetSortedUnionPart): each appends its
// partition's members to the buffer it is given and returns false when that partition's array is not
// current. setMergeCollect forces a synchronous fold so the arrays reflect every SADD/SREM (the caller
// holds the stripe locks, so no new write can land after the sync), then runs part once for an
// unpartitioned set or once per partition fanned across workers for a partitioned one. A not-current
// partition (which the held locks make unexpected) aborts the whole merge back to the probe rather than
// return a partial result. Each partition's members go into its own buffer so the fan-out never races on
// a shared slice, and the buffers concatenate into the result. The members are arena-stable subslices,
// valid after the merge returns.
func (c *connState) setMergeCollect(keys [][]byte, part func(pa, pb []byte, emit func([]byte)) bool, mixed mixedPartFn) ([][]byte, bool) {
	plan, ok := c.setMergeEligible(keys)
	if !ok {
		return nil, false
	}
	// Hold one reader epoch across the whole merge so the larger-than-memory regime is safe: a member
	// the migrator moves cold mid-merge keeps its resident bytes pinned until the merge resolves them,
	// and the mixed-P view's offsets, captured in one call and dereferenced in the per-target merges,
	// stay covered by a single hold no per-call pin could span. It is taken before the reconciling sync
	// and released when this returns; the segmented keyAtTiered hands back copies, so the members
	// outlive it. On the in-memory arena it is the zero guard and costs nothing.
	mp := c.srv.store.PinMerge()
	defer mp.Unpin()
	c.srv.store.SyncSortedHashes()
	if plan.mixed {
		return c.mergeCollectMixed(plan, mixed)
	}
	p, lo := plan.p, plan.lo
	// lo is the smaller source's cardinality, an exact upper bound for intersect and diff and a close
	// lower bound for union. Presizing the collect buffers to it (and to lo/p per partition) drops the
	// growslice churn the SINTER profile charged at ~12% on the hot P=1 path, where every matched member
	// otherwise reallocated the result slice from zero.
	if p == 1 {
		pa := setMergePrefix(keys[0], 0, 1)
		pb := setMergePrefix(keys[1], 0, 1)
		out := make([][]byte, 0, lo)
		if !part(pa, pb, func(m []byte) {
			out = append(out, m)
		}) {
			return nil, false
		}
		return out, true
	}
	perPart := lo/p + 1
	parts := make([][][]byte, p)
	var aborted atomic.Bool
	c.fanPartitions(p, c.mergeFanWorkers(p, lo), func(idx int) {
		pa := setMergePrefix(keys[0], idx, p)
		pb := setMergePrefix(keys[1], idx, p)
		local := make([][]byte, 0, perPart)
		if !part(pa, pb, func(m []byte) {
			local = append(local, m)
		}) {
			aborted.Store(true)
			return
		}
		parts[idx] = local
	})
	if aborted.Load() {
		return nil, false
	}
	total := 0
	for _, pp := range parts {
		total += len(pp)
	}
	out := make([][]byte, 0, total)
	for _, pp := range parts {
		out = append(out, pp...)
	}
	return out, true
}

// mixedPartFn is the per-target merge emitter the mixed-P path fans across the larger operand's
// partitions. realPrefix names the larger operand's target partition, realIsA says whether that
// operand is keys[0] (the A operand SDIFF keeps as its minuend; commutative ops ignore it), view is the
// smaller operand re-partitioned into the larger P, and target is the partition index into the view. It
// returns false when the larger operand's target partition is not current, which aborts the merge to
// the probe.
type mixedPartFn func(realPrefix []byte, realIsA bool, view *f1raw.RepartView, target int, emit func([]byte)) bool

// buildRepartView re-partitions the smaller-P operand's sorted arrays up into the larger operand's P,
// the O(|small|) bucket-split the mixed-P merge reads. It names the smaller operand's source partitions
// with the same setMergePrefix format the fold registers members under, so the engine snapshots them by
// the registry key, and returns false if any source is not current so the caller falls back to the
// probe rather than merge a stale view.
func (c *connState) buildRepartView(plan mergePlan) (*f1raw.RepartView, bool) {
	srcPrefixes := make([][]byte, plan.pSmall)
	for i := range plan.pSmall {
		srcPrefixes[i] = setMergePrefix(plan.smallKey, i, plan.pSmall)
	}
	return c.srv.store.SortedRepartition(srcPrefixes, plan.p)
}

// mergeCollectMixed runs the mixed-P merge: it builds the smaller operand's re-partitioned view once,
// then fans the larger operand's p partitions across workers, pairing each real partition against the
// view's matching target bucket through the op-specific mixed emitter. It concatenates the per-target
// members exactly as the same-P collector does, and aborts to the probe if the view build or any target
// merge reports not-current. The members are arena-stable subslices, valid after the merge returns.
func (c *connState) mergeCollectMixed(plan mergePlan, mixed mixedPartFn) ([][]byte, bool) {
	view, ok := c.buildRepartView(plan)
	if !ok {
		return nil, false
	}
	p := plan.p
	perPart := plan.lo/p + 1
	parts := make([][][]byte, p)
	var aborted atomic.Bool
	c.fanPartitions(p, c.mergeFanWorkers(p, plan.lo), func(target int) {
		realPrefix := setMergePrefix(plan.realKey, target, p)
		local := make([][]byte, 0, perPart)
		if !mixed(realPrefix, plan.bigIsA, view, target, func(m []byte) {
			local = append(local, m)
		}) {
			aborted.Store(true)
			return
		}
		parts[target] = local
	})
	if aborted.Load() {
		return nil, false
	}
	total := 0
	for _, pp := range parts {
		total += len(pp)
	}
	out := make([][]byte, 0, total)
	for _, pp := range parts {
		out = append(out, pp...)
	}
	return out, true
}

// setMergeIntersect computes SINTER's result through the sorted-hash merge when keys are eligible,
// returning the shared members and true, or nil and false to fall back to the smallest-source probe.
func (c *connState) setMergeIntersect(keys [][]byte) ([][]byte, bool) {
	return c.setMergeCollect(keys, c.srv.store.SetSortedIntersectPart,
		func(realPrefix []byte, _ bool, view *f1raw.RepartView, target int, emit func([]byte)) bool {
			return c.srv.store.SetSortedIntersectMixed(realPrefix, view, target, emit)
		})
}

// setMergeDiff computes SDIFF's result (the first source minus the second) through the sorted-hash
// merge when keys are eligible, returning the surviving members and true, or nil and false to fall back
// to the probe. SDIFF is not commutative, so the driver always treats keys[0] as A and keys[1] as B, the
// same order the engine's diffEmit assumes.
func (c *connState) setMergeDiff(keys [][]byte) ([][]byte, bool) {
	return c.setMergeCollect(keys, c.srv.store.SetSortedDiffPart,
		func(realPrefix []byte, realIsA bool, view *f1raw.RepartView, target int, emit func([]byte)) bool {
			return c.srv.store.SetSortedDiffMixed(realPrefix, realIsA, view, target, emit)
		})
}

// setMergeUnion computes SUNION's result through the sorted-hash merge when keys are eligible, returning
// the distinct union and true, or nil and false to fall back to the seen-set probe. The merge streams
// both sorted arrays with no O(union) dictionary, which is the win over the probe form's per-member map
// insert.
func (c *connState) setMergeUnion(keys [][]byte) ([][]byte, bool) {
	return c.setMergeCollect(keys, c.srv.store.SetSortedUnionPart,
		func(realPrefix []byte, _ bool, view *f1raw.RepartView, target int, emit func([]byte)) bool {
			return c.srv.store.SetSortedUnionMixed(realPrefix, view, target, emit)
		})
}

// setMergeIntersectCard computes SINTERCARD's count through the sorted-hash merge when keys are
// eligible, returning the count and true, or 0 and false to fall back to the probe. It mirrors
// setMergeIntersect but counts without materializing members: each partition intersection is disjoint
// (a member routes to exactly one partition), so the per-partition counts sum to the whole intersection
// size, and the command's LIMIT passes to each partition as an early-stop cap with the sum capped again
// at LIMIT. A not-current partition aborts to the probe.
func (c *connState) setMergeIntersectCard(keys [][]byte, limit int) (int, bool) {
	plan, ok := c.setMergeEligible(keys)
	if !ok {
		return 0, false
	}
	// Hold one reader epoch across the whole count merge, the same larger-than-memory guard the
	// materializing collector takes: a member migrated cold mid-merge stays pinned until it is counted,
	// and the mixed-P view's captured offsets stay covered across the per-target counts. A no-op on the
	// in-memory arena.
	mp := c.srv.store.PinMerge()
	defer mp.Unpin()
	c.srv.store.SyncSortedHashes()
	capTo := func(n int) int {
		if limit > 0 && n > limit {
			return limit
		}
		return n
	}
	if plan.mixed {
		return c.mergeIntersectCardMixed(plan, limit, capTo)
	}
	p, lo := plan.p, plan.lo
	if p == 1 {
		pa := setMergePrefix(keys[0], 0, 1)
		pb := setMergePrefix(keys[1], 0, 1)
		n, cur := c.srv.store.SetSortedIntersectCountPart(pa, pb, limit)
		if !cur {
			return 0, false
		}
		return capTo(n), true
	}
	var total atomic.Int64
	var aborted atomic.Bool
	c.fanPartitions(p, c.mergeFanWorkers(p, lo), func(part int) {
		pa := setMergePrefix(keys[0], part, p)
		pb := setMergePrefix(keys[1], part, p)
		n, cur := c.srv.store.SetSortedIntersectCountPart(pa, pb, limit)
		if !cur {
			aborted.Store(true)
			return
		}
		total.Add(int64(n))
	})
	if aborted.Load() {
		return 0, false
	}
	return capTo(int(total.Load())), true
}

// mergeIntersectCardMixed counts SINTERCARD's intersection through the mixed-P merge: it builds the
// smaller operand's re-partitioned view once, then fans the larger operand's p partitions across
// workers, summing each target's disjoint count through SetSortedIntersectCountMixed with LIMIT as the
// per-target early-stop cap and the sum capped again at LIMIT. It aborts to the probe if the view build
// or any target reports not-current.
func (c *connState) mergeIntersectCardMixed(plan mergePlan, limit int, capTo func(int) int) (int, bool) {
	view, ok := c.buildRepartView(plan)
	if !ok {
		return 0, false
	}
	p := plan.p
	var total atomic.Int64
	var aborted atomic.Bool
	c.fanPartitions(p, c.mergeFanWorkers(p, plan.lo), func(target int) {
		realPrefix := setMergePrefix(plan.realKey, target, p)
		n, cur := c.srv.store.SetSortedIntersectCountMixed(realPrefix, view, target, limit)
		if !cur {
			aborted.Store(true)
			return
		}
		total.Add(int64(n))
	})
	if aborted.Load() {
		return 0, false
	}
	return capTo(int(total.Load())), true
}

// offloadSetAlgebra hands a heavy multi-source set-algebra read off the epoll reactor loop to a park
// goroutine when the connection is being driven by the loop itself. The reactor drains every command in
// a batch inline on the loop goroutine, so a large SINTER/SUNION/SDIFF/SINTERCARD runs its compute and
// writes its multibulk reply on the loop, stalling the loop long enough to starve the other connections
// it serves; that is where the inline reactor SINTER dipped to 0.62x of Redis at the 256-member size
// while flat SET (a tiny reply) held 2.74x. Reusing the blocking-command park facility, begin flushes
// any pipelined replies, disarms reads on this connection, and reruns the command on a dedicated
// goroutine the Go scheduler spreads across cores, so the loop stays responsive and the algebra
// parallelizes. rerun must close over a dup of argv because the reactor reuses the read buffer once the
// loop resumes.
//
// It offloads only on the loop's own pass (blockable false): the rerun runs with blockable true, so the
// re-entrant call falls through here and executes inline on the park goroutine, and the goroutine net
// driver (blockable always true, park nil) never offloads at all. The gate is a cheap unlocked O(P)
// cardinality read per source, a heuristic for "is this reply/compute big enough to be worth the two
// epoll interest re-arms and the goroutine spawn"; being racy is fine because it only steers the command
// between two correct execution paths.
func (c *connState) offloadSetAlgebra(keys [][]byte, floor int, rerun func()) bool {
	if c.blockable || c.park == nil || !c.srv.setAlgebraOffload {
		return false
	}
	if !c.setAlgebraHeavy(keys, floor) {
		return false
	}
	c.park.begin(rerun)
	return true
}

// setAlgebraHeavy reports whether any source set's cardinality reaches floor, the size at or above which
// running the read inline on the reactor loop would stall it. The big-reply read forms pass
// setAlgebraOffloadFloor; the count form (SINTERCARD) passes the higher setAlgebraCountOffloadFloor
// because its single-integer reply carries nothing off the loop, so its handoff only pays once the pure
// compute is heavy. setCard is the same unlocked atomic O(P) read setMergeEligible uses, safe to call
// here without the stripe locks.
func (c *connState) setAlgebraHeavy(keys [][]byte, floor int) bool {
	for _, k := range keys {
		if c.setCard(k) >= uint64(floor) {
			return true
		}
	}
	return false
}

// cmdSInter answers SINTER by buffering the members present in every source (arena-stable subslices)
// and framing the reply from the buffer length. It deliberately buffers rather than streaming each
// member into the reply as it is found: SINTER's cost is almost entirely the point-probe into the
// shared composite index (spec 2064/f1_rewrite_ltm/20), which is memory-bound on the index cache
// lines. Encoding a member into the reply buffer between probes would evict those lines and slow the
// probe; buffering keeps the probe loop's footprint tiny and cache-hot, then encodes in one tight
// pass over the buffer afterward. Measured, the two-phase form runs a large SINTER ~15% faster than
// streaming the members inline. Each buffered member points into the immutable arena and stays valid
// while the driver cursor refills its scan batch.
func (c *connState) cmdSInter(argv [][]byte) {
	// SINTER key [key ...]
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'sinter' command")
		return
	}
	keys := argv[1:]
	if c.offloadSetAlgebra(keys, setAlgebraOffloadFloor, func() { c.cmdSInter(dupArgv(argv)) }) {
		return
	}
	unlock := c.lockStripes(keys)
	if c.anyStringConflict(keys) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	if merged, ok := c.setMergeIntersect(keys); ok {
		c.writeBulkArray(merged)
		unlock()
		return
	}
	out := make([][]byte, 0)
	c.sinterEach(keys, func(m []byte) bool {
		out = append(out, m)
		return true
	})
	c.writeBulkArray(out)
	unlock()
}

// cmdSDiff answers SDIFF by buffering the first set's members that no other source holds and framing
// from the buffer length. It buffers rather than streams for the same reason as SINTER: the cost is
// the per-member point-probe into the shared composite index, so keeping the probe loop's cache
// footprint minimal and encoding the buffer in a separate pass afterward runs faster than interleaving
// reply encoding into the probe.
func (c *connState) cmdSDiff(argv [][]byte) {
	// SDIFF key [key ...]
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'sdiff' command")
		return
	}
	keys := argv[1:]
	if c.offloadSetAlgebra(keys, setAlgebraOffloadFloor, func() { c.cmdSDiff(dupArgv(argv)) }) {
		return
	}
	unlock := c.lockStripes(keys)
	if c.anyStringConflict(keys) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	if merged, ok := c.setMergeDiff(keys); ok {
		c.writeBulkArray(merged)
		unlock()
		return
	}
	out := make([][]byte, 0)
	c.sdiffEach(keys, func(m []byte) bool {
		out = append(out, m)
		return true
	})
	c.writeBulkArray(out)
	unlock()
}

// cmdSUnion answers SUNION by buffering the distinct union once and framing the reply from the
// buffer length. It replaces the old two-pass form that walked every source and rebuilt the whole
// O(union) seen-set twice, once to count for the array header and once to emit, which doubled the
// dominant dedup cost; a large SUNION runs about twice as fast walking the sources a single time.
// The union already owes an O(union) seen-set to deduplicate (exactly what Redis's dict-backed
// SUNION pays), so buffering the distinct members it discovers costs one slice of arena-stable
// subslices, not a second copy of the data. Both the seen-set and the buffer are sized to the summed
// source cardinalities, the union's upper bound, so neither grows and rehashes mid-walk.
func (c *connState) cmdSUnion(argv [][]byte) {
	// SUNION key [key ...]
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'sunion' command")
		return
	}
	keys := argv[1:]
	if c.offloadSetAlgebra(keys, setAlgebraOffloadFloor, func() { c.cmdSUnion(dupArgv(argv)) }) {
		return
	}
	unlock := c.lockStripes(keys)
	if c.anyStringConflict(keys) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	if merged, ok := c.setMergeUnion(keys); ok {
		c.writeBulkArray(merged)
		unlock()
		return
	}
	out := make([][]byte, 0, algebraBufCap(c.summedCard(keys)))
	c.sunionEach(keys, func(m []byte) bool {
		out = append(out, m)
		return true
	})
	c.writeBulkArray(out)
	unlock()
}

// cmdSInterCard answers SINTERCARD numkeys key [key ...] [LIMIT limit]: it counts the
// intersection with the smallest-set-first probe and stops as soon as it reaches a positive
// LIMIT, so a bounded existence check on huge sets never walks the whole intersection. LIMIT
// 0 means no limit (count them all).
func (c *connState) cmdSInterCard(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'sintercard' command")
		return
	}
	numkeys, err := atoi64(argv[1])
	if err != nil {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	if numkeys <= 0 {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	nk := int(numkeys)
	if 2+nk > len(argv) {
		c.writeErr("ERR Number of keys can't be greater than number of args")
		return
	}
	keys := argv[2 : 2+nk]
	limit := 0
	rest := argv[2+nk:]
	if len(rest) > 0 {
		if len(rest) != 2 || !eqFold(rest[0], "LIMIT") {
			c.writeErr("ERR syntax error")
			return
		}
		l, err := atoi64(rest[1])
		if err != nil || l < 0 {
			c.writeErr("ERR LIMIT can't be negative")
			return
		}
		limit = int(l)
	}
	if c.offloadSetAlgebra(keys, setAlgebraCountOffloadFloor, func() { c.cmdSInterCard(dupArgv(argv)) }) {
		return
	}
	unlock := c.lockStripes(keys)
	if c.anyStringConflict(keys) {
		unlock()
		c.writeErr(wrongType)
		return
	}
	if n, ok := c.setMergeIntersectCard(keys, limit); ok {
		unlock()
		c.writeInt(int64(n))
		return
	}
	count := 0
	c.sinterEach(keys, func([]byte) bool {
		count++
		if limit > 0 && count >= limit {
			return false
		}
		return true
	})
	unlock()
	c.writeInt(int64(count))
}
