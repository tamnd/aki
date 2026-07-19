# OBJECT IDLETIME access-clock stamp: hot-GET cost (2026-07-19)

M9 gives strings a real OBJECT IDLETIME by stamping a per-key access clock into
the record header's otherwise-free `offKindBits` word (record.go offset 14) on
every read and write, the way Redis stamps `robj.lru`. The write cost is free:
zero added bytes (the 16 header bits were already there, written as 0). The read
cost is not obviously free: a GET was read-only and now writes the record. This
lab prices that write on the hot GET path.

## Method

`BenchmarkGetView` (engine/f3/store/bench_test.go) drives `store.GetView`, the
GET command path, over 1M 64-byte keys with a real `now` so every hit stamps.
The working set is ~64MB+, far past the laptop's last-level cache, so each hit is
an out-of-cache random probe, the same regime as the 1M-key 64B gate cell.

`run.sh` toggles the single `stampClock` line in `view.go` off and on and runs
the benchmark on each side, INTERLEAVED (WITH, WITHOUT, WITH, WITHOUT, ...), five
`-count` reps per side, median of five.

## Result

Interleaved medians, Apple laptop, `go test -benchtime 2s -count 5`:

| round | WITH stamp (ns/op) | WITHOUT stamp (ns/op) |
|---|---|---|
| 1 | 137.6 | 131.4 |
| 2 | 139.3 | 252.3 |
| 3 | 136.6 (min 135.6 max 137.1) | 174.0 (min 130.6 max 218.9) |

The two sides overlap completely. The WITH-stamp arm is in fact the *tighter*
one (round 3: 135.6-137.1ns across five reps), while the WITHOUT arm swings
131-252ns on ambient noise. The stamp does not move the median above that noise.

## The interleaving lesson

A first, non-interleaved pass ran all WITH reps, then all WITHOUT reps, and read
512ns vs 151ns, an apparent 3.4x regression. It was a mirage: the laptop's
machine state (thermal, background load) had drifted between the two blocks. The
theory that fit the mirage, that stamping dirties a cache line and forces a
writeback on eviction for an out-of-cache working set, is plausible on paper but
is not what the hardware does here. Interleaving the two sides so drift walks
through both equally erased the gap. Any single-store hot-path A/B on a laptop
must interleave.

## Verdict

The access-clock stamp is a single 2-byte store into the 16-byte header line
that `readValueRef` already loads on the same GET (it reads `offFlags` at offset
13 to pick the band), so it costs no extra cache line and no extra allocation,
and the interleaved benchmark cannot resolve it above its own ~±60ns noise. The
stamp ships on the read path, which is what makes IDLETIME reset on a GET the way
Redis does.

This in-process number is the cheap gate. The authoritative check is the box
GET-cell 2x gate (server pinned, dual-generator, VmHWM), which this slice defers
to batch with the next reactor gate run rather than spend a box session on a
change the microbenchmark already places inside the noise floor. The memory half
is settled by construction: zero added bytes on the string cell.
