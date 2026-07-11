# Lab 05: timer-park pricing

Part of the M3 list milestone, slice 8 PR 3 (spec 2064/f3 doc 03 section 9, issue #545).
The PR adds a per-worker deadline heap and wires it into the owner loop: fireTimers runs the due deadlines each pass, and the idle park, when a deadline is armed, waits on a reusable timer alongside the waker channel instead of a plain channel receive.
Both sit next to the P1 latency path, so this lab prices them before the new constant lands in tuning.go.

## Questions

1. What does the timed park cost over the plain park, and is that cost paid only when a timer is armed?
The design rule is that a worker with no armed timer parks exactly as it does today, so the no-timer control here must match the plain park within noise.
The timed arm pays a timer Reset plus a two-case select on every park, so this names that overhead in ns.

2. Where should timerFireCap sit?
When many deadlines come due at once the owner fires them in capped batches, one batch per loop pass.
A small cap keeps each pass short, so less time is stolen from command processing, but it spreads the tail delivery across more passes.
A large cap delivers the whole burst in one pass but holds the loop longer.
The sweep fires a fixed burst at caps {16, 32, 64, 128} and reports both the whole-burst time and the longest single pass, so the knee is visible.

## Method

The park primitive and the deadline heap are unexported in the shard package, so this lab reprices them standalone, the way lab 03 reprices the waiter FIFO.
The park is the same three-state waker (running, spinning, parked) with the same claim-then-send wake; the plain arm is the no-timer control (`store parked`, `<-ch`), and the timed arm is `store parked`, arm the reusable timer with the drain-before-reset idiom, then `select` over the channel and the timer.
The timer's deadline is set an hour out so it never actually fires during the measurement, so the wake always comes through the channel and the two arms differ only by the timer machinery.
The fire-batch sweep uses the same min-heap shape timer.go ships, arms a burst of already-due deadlines, and drains them in capped passes exactly as the owner loop does.

macOS has no raw futex, so the park is a Go channel receive; the absolute park floor on Linux differs (a futex wake is a ~1-2us syscall path), and the overhead this lab measures is the delta between the two park shapes, which is transport independent.

Run it with:

```
go run ./labs/f3/m3/05_timer_park/
go test ./labs/f3/m3/05_timer_park/
```

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-12, one process, machine otherwise quiet.
The round-trip cells are dominated by the goroutine wake latency (~3.6us here), and the timed-park overhead is the small delta on top; run to run it lands between about -10 and +300 ns, in the noise of the round trip.
The fire-batch cells are steadier and the shape is consistent across runs.

Timed-park overhead, park-then-wake round trip, no timer ever fires:

| park shape | ns/round trip | wake p50 ns | wake p99 ns |
|---|---|---|---|
| plain (no-timer control) | 3592 | 4000 | 9000 |
| timed (armed) | 3898 | 3000 | 13000 |

Timed-park overhead over the plain park: about 0 to 300 ns/park across runs, below the ~3.6us wake round trip it rides on.

Fire-batch cap sweep, 4096 simultaneously-due timers drained in capped passes:

| cap | passes | whole-burst us | longest pass us |
|---|---|---|---|
| 16 | 256 | 26.79 | 0.12 |
| 32 | 128 | 22.79 | 0.25 |
| 64 | 64 | 20.46 | 0.33 |
| 128 | 32 | 18.79 | 0.62 |

## FROZEN VERDICT

Frozen timerFireCap: 64.

The whole-burst time falls as the cap grows, because fewer passes means less per-pass overhead paid over the same fired work, but the return flattens past 64: going 64 to 128 saves under 2 us on the whole 4096-timer burst while it nearly doubles the longest single pass, from 0.33 us to 0.62 us.
The longest single pass is the command-starvation term, the stretch the owner loop spends firing timers instead of running commands, and it grows roughly linearly with the cap.
So 64 is the knee: it captures almost all of the batching win (20.46 us versus 18.79 us at 128, versus 26.79 us at 16) while keeping the single pass to a third of a microsecond.
This also matches the doc 03 section 6 hint of a 64-cap batch and sits at twice drainPassCap, the command batch cap, which is the right relation: a timeout burst is rarer than a command burst, so a firing pass may fire a few more deadlines than a drain pass runs commands before it yields.

Frozen timed-park cost: within noise of the plain park, single to low-hundreds of ns, and paid only when a timer is armed.

The no-timer control is the plain park byte for byte, and it lands on top of the timed arm within run-to-run noise, so the P1 park path is untouched when no blocking command is parked on the worker.
The timed arm adds a timer Reset and a two-case select, which the sweep prices at roughly 0 to 300 ns, well under the ~3.6 us the wake round trip already costs, so even when a worker is parked on a deadline the added latency to a producer wake is negligible.
The absolute round-trip floor is the macOS channel park and will differ on the gate box (a Linux futex wake is a ~1-2 us syscall path), but the overhead frozen here is a delta between two park shapes on the same transport, so it carries.

No ceiling on the park duration was added.
A far-future deadline does not delay shutdown, because Stop wakes every worker through wk.wake, which lands on the channel branch of the timed-park select and returns at once; the deadline itself is never waited out.
So the timed park sleeps to the exact next deadline with no clamp, and nothing in this lab argues for one.
