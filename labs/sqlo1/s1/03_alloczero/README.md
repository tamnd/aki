# Lab 03: alloczero, the zero-allocation gate

Part of milestone S1 (tamnd/aki#711, spec 2064/sqlo1 doc 04 section 16).
Doc 04's rule is that steady-state allocation is a bug: every hot-path object is preallocated or pooled at startup, and every hot-path PR is gated on allocs/op == 0.
This lab is that gate, and it satisfies the S1 exit-gate item "alloczero green in CI".

## What it is

Unlike labs 01 and 02 this is not a model sweep: it imports engine/sqlo1 and measures the real code with testing.AllocsPerRun.
The test fails CI the moment any gated operation allocates, which makes the doc 04 rule enforceable instead of aspirational.
`go run .` prints the current allocs/op table; the test asserts every row is exactly 0.

The gate grows with the engine.
A new hot path joins by adding a probe: a closure owning its preallocated state (buffers, argument slices) outside the measured function, the way the real connection loop owns its buffers.

## Gated today: the wire path

Command parsing with a reused argument slice and reply building into a presized buffer, separately and as GET- and SET-shaped round trips, including a 16-deep pipelined burst and 4KiB values.

Landing the gate surfaced its first bug, fixed in the same PR: ParseCommand allocated a fresh argument slice per command.
It is now append-style (pass args[:0], keep the returned slice), and the slice comes back on every path, including ErrIncomplete, so the connection loop's capacity survives partial reads and the steady state never allocates.
The server's connection loop now carries one argument slice for its whole life.

## Not gated yet, and why

- Hot-tier point ops (GET/SET through the header table): the tier does not exist yet; its probes register when the header-table and eviction slices land, which is what the S1 exit-gate item means by "zero steady-state allocs on GET/SET-shaped ops through the hot tier".
- The S0 placeholder server dispatch: it clones keys and builds one-op batches by design, and doc 04 calls it no performance statement. Gating it would pin placeholder code.
- The live-heap to VmHWM slack tracking doc 04 mentions alongside this lab: that is a runtime measurement, not an allocs/op assertion, and it arrives with the budget-caps slice.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-15.

| op | allocs/op |
|---|---|
| parse GET | 0 |
| parse SET 16B | 0 |
| parse SET 4KiB | 0 |
| parse 16-deep GET pipeline | 0 |
| append simple OK | 0 |
| append integer | 0 |
| append bulk 16B | 0 |
| append bulk 4KiB | 0 |
| append null bulk | 0 |
| GET round trip (parse + bulk reply) | 0 |
| SET round trip (parse + OK reply) | 0 |
