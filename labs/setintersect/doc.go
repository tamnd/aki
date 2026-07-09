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
//	BenchmarkCompactFingerprint        74 ms   build table over B, then probe
//	BenchmarkCompactFingerprintProbeOnly 41 ms   probe only, table already built
//	BenchmarkRedisDict                153 ms   build map over B, then probe
//	BenchmarkFullSInter                43 ms   probe + buffer + RESP-encode the ~1M hits
//	BenchmarkEncodeOnly                 3 ms   serialize an already-known ~1M result
//
// Four facts fall straight out.
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
// # The consequence for the real redesign
//
// Put together, the levers this lab tested against the isolated SINTER bench (the
// one the 2x gate runs) are all small or negative: per-op rebuild is a wash,
// composite-vs-member-only is ~5%, reply encode is 7%. None of them is a 2x. So when
// the real f1srv SINTER runs at ~0.35x of Valkey on two 2<<20 sets, the gap is not
// in the shape of the index or the reply; it is the layers the real probe carries
// that this minimal model strips away: per-member partition routing (doc 19 splits a
// 2M set into many partition vectors, and setMemberExists re-routes every probe),
// composite-key reconstruction per member, interface dispatch, and bounds checks.
// The redesign that pays is a direct, minimal, member-only resident probe path for
// the algebra hot loop, one that bypasses partition routing and composite
// reconstruction, i.e. make the real probe cost the 40 ns this lab shows is
// available. That closes toward parity; it does not manufacture a 2x, because at
// equal set sizes both engines do ~1M DRAM-latency probes.
//
// The honest ceiling: a clean 2x over Valkey on a large equal-size SINTER is not a
// data-structure result. It needs either an algorithm that avoids random probing
// (a sorted merge-intersection, which doc 20 dropped when it removed the SET oindex)
// or a workload where one source is much smaller. That is the decision this lab
// hands up: cut f1srv's probe overhead to the 40 ns floor for a parity-class win,
// and treat a headline 2x on large symmetric SINTER as out of reach without
// re-introducing set ordering.
//
// The reference RedisDict (153 ms) is a reminder that the per-set structure is not
// magic either: a Go map rebuilt per call is 3.7x slower than aki's resident probe.
//
// The real code these numbers inform is aki/f1srv/set_algebra.go (cmdSInter,
// cmdSDiff) and the f1raw SET index in aki/engine/f1raw.
//
// Numbers observed on an Apple M4 (GOMAXPROCS=10); re-run to reproduce on yours.
package setintersect
