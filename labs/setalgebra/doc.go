// Package setalgebra is a lab: it isolates two lessons from the f1raw SET
// algebra optimization slice (SINTER/SUNION/SDIFF, spec 2064/f1_rewrite_ltm/20)
// so the reasoning behind the shape of those commands stays reproducible.
//
// # The setup
//
// SINTER on f1raw drives off the smallest source set and, for each of its
// members, point-probes every other source through a shared composite hash
// index. On two 500k-member sets that is ~1M probes into an index far larger
// than L2, so the command is memory-bound on the index cache lines: a clean CPU
// profile put ~34% of the whole command in sync/atomic.(*Uint64).Load (the
// lock-free slot reads) and ~64% cumulative in the probe. The reply framing
// (writeBulk) was ~14%.
//
// SUNION is different: it has no probe, it enumerates every source and
// deduplicates through an O(union) seen-set, so it is bound by the map, not by
// a random-access index.
//
// # Lesson 1: buffer then encode beats streaming, for a probe-bound command
//
// The obvious "improvement" is a deferred-length reply: stream each qualifying
// member straight into the output buffer as the probe finds it and splice the
// array header in afterward, so nothing is buffered. It removes a slice of
// arena pointers and, the theory went, the GC write barrier those pointer
// stores fire. It made SINTER ~15% SLOWER.
//
// The reason is cache and a shift, not barriers. Encoding a member between probes
// (writeBulk touches a growing output buffer and runs strconv for the length)
// evicts the index cache lines the very next probe needs, and the streaming form
// then pays a whole-payload memmove to splice the array header in once the count
// is known. Buffering the qualifying members in a tiny append loop keeps the
// probe loop's footprint minimal and the index hot, writes the header first (no
// shift), and encodes in one tight pass afterward. BenchmarkProbeInterleaved runs
// ~12% slower than BenchmarkProbeTwoPhase, the same direction and rough size as
// the real SINTER's ~15%.
//
// So SINTER and SDIFF keep the buffer-then-encode form. Streaming is the right
// instinct for a command that is CPU-bound in the reply (many small elements,
// cheap to produce); it is the wrong one for a command that is memory-bound in
// the producer.
//
// # Lesson 2: one walk beats two, for a dedup-bound command
//
// SUNION's array header needs the distinct count up front. The old form learned
// it by walking every source twice: once building the whole seen-set to count,
// then rebuilding the whole seen-set again to emit. Since the seen-set build is
// the dominant cost, doing it twice roughly doubled the command. Walking once,
// buffering the distinct members, and framing from the buffer length runs a
// large SUNION about twice as fast. BenchmarkUnionTwoPass vs
// BenchmarkUnionSinglePass reproduces it. Here buffering is not the enemy: the
// command already owes an O(union) seen-set, so one more slice of the same
// members it already found is cheap next to walking the sources a second time.
//
// The two lessons look contradictory (one says do not stream, the other says do
// not double-walk) but they share a root: spend the expensive resource once.
// For SINTER that resource is index cache residency, so isolate the probe. For
// SUNION it is the dedup map build, so isolate it to a single pass.
//
// # Lesson 3: a contaminated profile blames the wrong line
//
// An early profile of this same command put 44% of the time in the pointer
// append and named the GC write barrier as the culprit, which is what motivated
// the streaming rewrite that then regressed. That profile was contaminated: the
// benchmark's one-time set-loading phase (SADD of a million members) ran inside
// the profiled region, and its memmove and growth landed on the append line. A
// re-profile with a high -benchtime, so the timed command dominates the setup,
// moved the cost to where it actually was: the probe. The lab's benchmarks all
// build their fixtures before ResetTimer for the same reason.
//
// The real code these lessons informed is aki/f1srv/set_algebra.go
// (cmdSInter, cmdSDiff, cmdSUnion).
//
// Numbers observed on an Apple M4 (GOMAXPROCS=4); re-run to reproduce on yours.
package setalgebra
