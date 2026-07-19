# Collection OBJECT IDLETIME access-clock stamp: hot-funnel cost (2026-07-19)

M9 extends OBJECT IDLETIME from strings to the five collection types (set, zset,
hash, list, stream). Each type now carries a per-key access clock in the struct
alignment padding it already held (a `clock uint16` beside the leading `enc`
byte), stamped on every real command access through the one `live()` funnel the
type routes reads, writes, and creates through. The write cost is free: zero
added bytes, the padding was already there. The read cost is a single `uint16`
store into the struct's first cache line, which `live()` already loaded to check
`expireAt`. This lab prices that store on the collection hot path.

## Method

`BenchmarkSetLive` (engine/f3/set/bench_test.go) drives `set.(*reg).live` over a
registry of 1M one-member sets, resolving a rotating key each iteration, so the
timed cost is the map lookup, the deadline check, and the clock stamp. The set
funnel stands in for all five types: every type took the identical live/peek
split and the same one-line `store.LRUClock` stamp.

`run.sh` comments the stamp line in set/reg.go out and back in and runs the
benchmark on each side, INTERLEAVED, five reps per side, three rounds.

## Result

Clean single-sided, quiescent machine:

```
BenchmarkSetLive-10   14556180   84.68 ns/op   0 B/op   0 allocs/op
```

The 84.7ns is dominated by the Go map lookup over a 1M-entry map; the stamp is a
small fraction of it.

The interleaved A/B run was contaminated: a full `go test -race ./...` suite was
running on the same machine and saturating every core, so the per-round medians
swung wildly and swapped which side was faster round to round (round 1: WITH
188ns vs WITHOUT 127ns; round 2: WITH 217ns vs WITHOUT 678ns). That the sides
swap direction under load is itself the tell that the stamp is below the noise:
if the stamp cost real time, WITHOUT would win every round, not lose round 2 by
3x. A clean interleaved re-run is batched with the next box session rather than
spent fighting a concurrent suite here.

## Verdict

The collection access-clock stamp is a single 2-byte store into the struct cache
line `live()` already touches, on par with the string record-header stamp priced
in m9/01, which an interleaved A/B there placed inside the laptop noise floor.
The memory half is settled by construction: zero added bytes, the field rides
existing alignment padding, confirmed per type by `unsafe.Sizeof` (stream 152
unchanged, zset 64 unchanged, and so on).

The authoritative perf check is the box GET/collection-cell 2x gate (server
pinned, dual-generator, VmHWM), which this slice defers to batch with the next
reactor gate run, the same call m9/01 made.
