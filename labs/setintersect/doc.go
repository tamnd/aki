// Package setintersect is a lab that asks a first-principles question about SINTER
// on f1raw and answers it with numbers: aki probes ONE shared composite index for
// every member of the driver set, where Redis probes a small per-set dict. Is that
// shared-index probe why SINTER lags, and does rebuilding a compact per-operation
// probe table over the non-driver source win it back? The measured answer is no,
// and the reason it is no is the useful part.
//
// # The setup
//
// The fixture is SINTER(A, B), |A| = |B| = 1<<20, overlapping by half, the shape
// the real f1srv BenchmarkSInterBig loads. Every strategy walks A and decides
// membership in B. Three membership structures are modeled, none importing aki:
//
//   - globalIndex: one open-addressed table over the composite key
//     uvarint(len(skey))|skey|member, arena-backed, atomic slots. This is f1raw's
//     shared index. A dilute knob pads it with other keys' members so its working
//     set is bigger than the two operands, the production condition.
//   - fpTable: a fresh table of (fingerprint, member) built per operation over B,
//     member-only hash, byte-confirmed on a fingerprint hit. This is the "redesign
//     from scratch" candidate.
//   - map[string]struct{}: the reference for a member-only probe into a per-set
//     structure, i.e. the shape Redis has.
//
// # What the numbers say (Apple M4, GOMAXPROCS=10, 1<<20 per set)
//
//	BenchmarkGlobalProbe               40 ms   shared index holds only A,B (probe only)
//	BenchmarkGlobalProbeDiluted        73 ms   shared index also holds 8x other keys
//	BenchmarkPartitionedProbe          44 ms   doc 19 routed probe, undiluted index
//	BenchmarkPartitionedProbeDiluted   77 ms   doc 19 routed probe, 8x diluted index
//	BenchmarkCompactFingerprint        74 ms   build table over B, then probe
//	BenchmarkCompactFingerprintProbeOnly 41 ms   probe only, table already built
//	BenchmarkRedisDict                153 ms   build map over B, then probe
//	BenchmarkFullSInter                43 ms   probe + buffer + RESP-encode the ~1M hits
//	BenchmarkEncodeOnly                 3 ms   serialize an already-known ~1M result
//	BenchmarkMergeIntersect            12 ms   two-pointer merge of two resident sorted arrays
//	BenchmarkMergeIntersectWithSort   450 ms   same merge, but sorting both sources per call
//
// Six facts fall straight out.
//
// First: with the shared index holding only the two operands (GlobalProbe, 40 ms)
// the composite-key probe is already as fast as a bare member-only probe
// (CompactFingerprintProbeOnly, 41 ms). The composite prefix, the arena
// indirection, and the atomic slot loads together cost almost nothing here. So the
// thing everyone points at, "aki rehashes a fat composite key and chases an arena
// pointer," is not where the time goes when the index is small.
//
// Second: the per-operation rebuild does NOT win. CompactFingerprint (74 ms) equals
// GlobalProbeDiluted (73 ms). The rebuild buys a cache-local probe (41 ms vs the
// diluted 73 ms, a real ~32 ms saving) and then spends every bit of it building the
// table (build = 74 - 41 = ~33 ms). The O(|B|) build cost is exactly the size of the
// O(|A|) probe saving, because |A| = |B|, so it is a wash by construction. Rebuilding
// a probe structure per operation cannot beat a resident one when the build is the
// same order as the work it accelerates.
//
// Third, and this is what reframes the whole SINTER effort: the reply is not the
// bottleneck. FullSInter (43 ms) is GlobalProbe (40 ms) plus a 3 ms
// (BenchmarkEncodeOnly) serialization of the ~1M-member result. The probe is 93% of
// the command and the RESP encode is 7%. Every lever this lab could pull on the
// reply path is capped at that 7%. SINTER is memory-latency-bound on the probe,
// full stop.
//
// Fourth: that probe is already at the machine's random-probe floor. 40 ms for ~1M
// probes is ~40 ns each, which is a cache-miss latency, and it is what a bare
// member-only Go table costs too (CompactFingerprintProbeOnly). A tuned C dict probe
// is the same order. There is no data-structure change that makes 1M random probes
// into a multi-MB table 2x cheaper, because the cost is DRAM latency, not
// instructions.
//
// Fifth, and this one retracts a guess an earlier draft of this file made: doc 19's
// per-member partition routing is NOT the hidden cost. setMemberExists on a
// partitioned set (f1srv/set_algebra.go:217) does not probe a smaller table; it still
// probes the one shared index, it just routes first, computing PartitionOf (a second
// full member hash) and building a partition-qualified composite key one byte longer
// than the plain one. BenchmarkPartitionedProbe puts exactly that routing on top of
// BenchmarkGlobalProbe and it costs ~10% on the undiluted index (44 ms vs 40 ms) and
// essentially nothing once the shared index is diluted to production size (77 ms vs
// 73 ms). The extra member hash hides in the DRAM stall the probe already pays. So
// routing is cheap; the earlier draft named it as the leading suspect for the real
// gap and the measurement clears it. This is the whole reason the lab exists: the
// suspected 2x cost was a ~10%-and-vanishing one.
//
// Sixth, and this is the constructive one, the one that says how the gate is won: stop
// probing. Every strategy so far reads the non-driver set at random, one DRAM miss per
// member, and so does Redis and so does Valkey, so no probe can beat them 2x. A merge
// of two sorted hash arrays reads both sets sequentially, cursors only moving forward,
// so the prefetcher serves it from cache. BenchmarkMergeIntersect is 12 ms against
// GlobalProbe's 40 ms, ~3-5x depending on the run, comfortably past the gate, on the
// exact same two sets. The merge touches ~2x more elements and still wins by a wide
// margin because sequential streaming is ~10x cheaper per element than a random probe.
// This is the access pattern Redis structurally cannot use: its set is an unordered
// dict, it has nothing sorted to merge. aki can, because it can hold a large set in
// hash order.
//
// The one condition, and it is the whole design question: the merge only wins if the
// sets are ALREADY sorted. BenchmarkMergeIntersectWithSort, which sorts both sources
// per call, is 450 ms, ~10x SLOWER than the probe. So a set must be kept in hash order
// incrementally, on its writes, not re-sorted per operation. That ordered
// representation is exactly the SET oindex spec 2064 doc 20 dropped to win the point
// ops (SPOP/SMEMBERS/SSCAN off the unordered dense vector). The 450 ms is a full
// comparison re-sort with interface dispatch; an engine keeping order incrementally
// (a skiplist or B-tree of member hashes, as the dropped oindex was) pays O(log n) per
// write and ~0 per SINTER, leaving the 12 ms merge as the operative number.
//
// # The consequence for the real redesign
//
// The negative levers this lab tested against the isolated SINTER bench are all small:
// per-op rebuild is a wash, composite-vs-member-only is ~5%, partition routing is ~10%
// and vanishes under cache pressure, reply encode is 7%. None is a 2x, so no amount of
// probe-path tuning reaches the gate; the best it does is close f1srv's current 0.35x
// toward parity by cutting the SPI dispatch, arena indirection, driver selection, and
// result-buffer GC the plain-Go probe models omit. Parity, not 2x.
//
// The 2x is the merge. The redesign the numbers point to is: bring back a hash-ordered
// representation for the SET type, but scoped so it does not undo doc 20's point-op win.
// Two shapes fit. One, keep the unordered dense vector for SPOP/SMEMBERS/SSCAN and add a
// separate sorted dense []uint64 of member hashes maintained on writes, consumed only by
// the algebra path, which is a merge over hashes with a byte-confirm on ties. Two, build
// the sorted array lazily on the first algebra call against a set and cache it with
// write-invalidation, so a read-heavy algebra set amortizes the one sort over many
// merges (the 450 ms sort pays for itself after ~11 SINTERs against the same operands).
// Either way the algebra hot loop becomes the 12 ms merge, not the 40 ms probe, and
// large symmetric SINTER clears 2x over both rivals. The asymmetric case (one small
// source) still wants the probe off the small driver, so the real command picks merge
// when both sources are large and comparable, probe when one is much smaller, the same
// adaptive choice the ordered-index era made (spec 2064, task 263) before the oindex
// drop deleted the merge arm.
//
// The reference RedisDict (153 ms) is a reminder that the per-set structure is not
// magic either: a Go map rebuilt per call is 3.7x slower than aki's resident probe.
//
// The real code these numbers inform is aki/f1srv/set_algebra.go (cmdSInter,
// cmdSDiff, sinterEach) and the f1raw SET index in aki/engine/f1raw.
//
// Numbers observed on an Apple M4 (GOMAXPROCS=10); re-run to reproduce on yours.
package setintersect
