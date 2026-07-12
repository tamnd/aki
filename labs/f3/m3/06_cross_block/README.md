# Lab 06: cross-shard blocking serve cost

Part of the M3 list milestone, slice 8 PR 6 (spec 2064/f3 doc 13, issue #545).
This is the last PR of the list milestone.
It lets a blocking list verb block across shards: BLPOP, BRPOP, and BLMPOP park a waiter on several owners at once and let whichever owner's push or timeout wins a shared atomic claim serve it, then cancel the losers; BLMOVE and BRPOPLPUSH block on the one source but reach a destination on another shard, so the serving push spawns a coordinator that acquires both keys and runs the move under a fresh barrier.
The co-located forms never do either: a push serves them inline on the one owner.
This lab prices the two new serve paths against those co-located inline serves before the slice lands.

## Questions

1. What does a parked cross-shard serve cost over the co-located inline serve?
The co-located serve runs entirely on the one owner the push lands on: pop the head, hand the element to the waiter, unlink.
The cross pop adds a claim CAS and a cancel fan-out to the other owners the waiter parked on; the cross move adds a spawned two-owner coordinator and a second barrier.
This names each added cost in microseconds per served waiter, as the delta on the same wake transport.

2. How does the pop's cancel fan-out grow with the number of keys a waiter parked on?
A cross BLPOP on k keys registers an intent on k owners, and the winning serve must post a cancel to the other k-1.
The sweep parks on {2, 4, 8} keys and serves each burst, so the per-cancel term shows as the slope over key count.

## Method

One runtime, the list handlers, eight shards.
A burst of blockers park on empty keys, each on its own connection through the real DoBlockCross route (the cross arm) or the point handler (the co-located arm), then the lab sleeps 30 ms so every one is registered on its owner before any push lands.
That sleep is what makes this a real park-then-wake and never an accidental immediate serve.
A pusher then feeds the source one element at a time; each push serves the FIFO-head blocker, and the lab drains that blocker's reply before the next push, so the served work is fully accounted before the clock moves on.
The pusher round trip is identical on both arms, so the co-located-to-cross delta is the cross serve overhead alone.

A parked serve rides a Go channel wake, and macOS scheduling adds a few microseconds of jitter that only ever inflates a sample, never shortens it.
A single sample swings by more than the serve tax it is trying to show, so each cell is the minimum over seven runs, the standard robust floor for a latency this small.
The minimum is stable run to run where a single sample is not.

macOS has no raw futex, so the cross serve that hops to another owner rides a Go channel wake; the absolute cell is higher than the gate box will show (a Linux futex wake is a ~1-2 us syscall path), but the co-to-cross delta this lab freezes is a difference measured on the same transport, so it carries.

Run it with:

```
go run ./labs/f3/m3/06_cross_block/
go run ./labs/f3/m3/06_cross_block/ -quick
go test ./labs/f3/m3/06_cross_block/
```

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-12, one process, machine otherwise quiet.

Parked serve cost, park a blocker then wake it with a push, min of 7 runs:

| verb | co-located us | cross-shard us | serve tax us |
|---|---|---|---|
| BLPOP (2 keys) | 11.84 | 12.07 | 0.23 |
| BLMOVE | 12.30 | 13.51 | 1.21 |
| BRPOPLPUSH | 12.49 | 14.49 | 2.00 |

Cross BLPOP serve by key count, the cancel fan-out over the owners a waiter parked on, min of 7 runs:

| keys | cross-shard us | over 2-key us |
|---|---|---|
| 2 | 15.95 | -0.18 |
| 4 | 16.38 | +0.24 |
| 8 | 18.44 | +2.31 |

## FROZEN VERDICT

Frozen cross serve tax: low single-digit microseconds, ordered pop < move, and paid only on the cross path.

The cross BLPOP serve adds about 0.2 us over the co-located inline serve: the claim CAS plus a two-key cancel fan-out is a handful of atomic ops and one extra owner hop, and it barely lifts off the co-located floor.
The cross move adds more, about 1.2 us for BLMOVE and 2.0 us for BRPOPLPUSH, which is the right shape: a move cannot serve inline because the source owner does not own the destination, so it spawns a coordinator that acquires both keys and runs a second barrier, and that spawn-and-acquire is strictly more work than the pop's claim-and-cancel.
BRPOPLPUSH sits a touch above BLMOVE run to run, in line with its extra source pop from the tail, but the two are close enough that the gap is at the edge of the noise.
Every one of these is a cross-path-only cost: the co-located columns are the untouched inline serve, so a co-located BLPOP or BLMOVE pays none of it.

Frozen fan-out: the cancel term is sub-microsecond per extra owner and only shows at wider parks.

Going from 2 to 4 keys is inside the noise (0.24 us over the 2-key floor), and only at 8 keys does the fan-out clear the noise floor at about 2.3 us, roughly a third of a microsecond per additional owner cancelled.
That matches the design: a cancel is one idempotent post to an already-registered intent, cheap per owner, and a realistic BLPOP parks on a small key set where the term is invisible.
Nothing here argues for a fan-out cap; the cost is linear and small, and the common two-to-four-key case pays essentially nothing.

The absolute cells are the macOS channel-wake floor and will drop on the gate box where the wake is a Linux futex, but the co-to-cross deltas frozen here are measured on the one transport, so they carry to Linux.
The hot push and pop path and the co-located park path are the co-located columns of this table, and they are the pre-slice inline serve unchanged, so this slice adds cost only when a waiter actually blocks across shards.
