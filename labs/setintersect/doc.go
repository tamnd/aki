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
//	BenchmarkGlobalProbe               43 ms   shared index holds only A,B
//	BenchmarkGlobalProbeDiluted        73 ms   shared index also holds 8x other keys
//	BenchmarkCompactFingerprint        74 ms   build table over B, then probe
//	BenchmarkCompactFingerprintProbeOnly 41 ms   probe only, table already built
//	BenchmarkRedisDict                153 ms   build map over B, then probe
//
// Two facts fall straight out.
//
// First: with the shared index holding only the two operands (GlobalProbe, 43 ms)
// the composite-key probe is already as fast as a bare member-only probe
// (CompactFingerprintProbeOnly, 41 ms). The composite prefix, the arena
// indirection, and the atomic slot loads together cost almost nothing here. So the
// thing everyone points at, "aki rehashes a fat composite key and chases an arena
// pointer," is not where the time goes when the index is small.
//
// Second, and this is the finding: the per-operation rebuild does NOT win.
// CompactFingerprint (74 ms) equals GlobalProbeDiluted (73 ms). The rebuild buys a
// cache-local probe (41 ms vs the diluted 73 ms, a real ~32 ms saving) and then
// spends every bit of it building the table (build = 74 - 41 = ~33 ms). The O(|B|)
// build cost is exactly the size of the O(|A|) probe saving, because |A| = |B|, so
// it is a wash by construction. Rebuilding a probe structure per operation cannot
// beat a resident one when the build is the same order as the work it accelerates.
//
// # The consequence for the real redesign
//
// The isolated SINTER benchmark (and the 2x gate that runs on it) loads only the
// operands, so it is the GlobalProbe, 43 ms, world, not the diluted one. Against
// that benchmark a per-operation fingerprint table is pure loss: it adds a 33 ms
// build and 64 MB of allocation to save a cache miss that is not being paid. The
// lab killed a redesign that would have regressed the very number it was meant to
// improve. That is lesson 3 of the sibling setalgebra lab happening again: measure
// the real condition before you rebuild for it.
//
// The dilution column is where a member-only structure genuinely wins (73 -> 41 ms,
// ~1.8x), but only a RESIDENT per-set index captures it, one maintained
// incrementally on SADD so no build is charged per operation. CompactFingerprintProbeOnly
// is the model of that resident per-set index: 41 ms, cache-local, member-only.
// Moving f1raw's SET from one shared composite index to per-set member-only indexes
// is the only shape that reaches ~2x on a full keyspace, and it is a large change
// that does nothing for the isolated bench. That trade is the decision this lab
// hands to the redesign, with the price of each side attached.
//
// The reference RedisDict (153 ms) is a reminder that Redis's advantage is not
// magic: a Go map rebuilt per call is 3.7x slower than aki's resident probe. aki
// already beats the naive per-set-structure floor; the open question is only
// whether cache residency under a real keyspace is worth an architectural change.
//
// The real code these numbers inform is aki/f1srv/set_algebra.go (cmdSInter,
// cmdSDiff) and the f1raw SET index in aki/engine/f1raw.
//
// Numbers observed on an Apple M4 (GOMAXPROCS=10); re-run to reproduce on yours.
package setintersect
