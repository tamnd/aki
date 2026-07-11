# Lab 03: BLPOP/BRPOP waiter-set wakeup fairness

Part of the M3 list milestone.
This lab lands before the blocking-list slice so the slice bakes a settled waiter-set representation, not a guess.

## Question

Doc 13 (13-list-model.md) section 5.12 parks a blocked list client (BLPOP, BRPOP, BLMOVE, BLMPOP, LMPOP) in the owner shard's waiter set for the key and arms a timer-heap entry.
A push to the key checks that set inline, one resident lookup on a structure only the owner shard touches, and serves the longest-waiting client FIFO, building the reply in the push's own epoch.
No polling, no cross-shard wakeup storm.

The spec's chosen representation is an intrusive doubly linked list of per-connection wait nodes hanging off the key's record, allocated from the shard arena, zero maps, where FIFO order is just the link order so "fairness costs nothing".
This lab tests that sentence.
The question is whether serving strict FIFO by link order actually costs nothing against the plausible alternatives, and what the wake-to-reply cost is under a push storm, which is the constant the M3 gate's BLPOP latency row is checked against.
The gate row is a latency row, not a throughput row (F16 regime honesty): "BLPOP wake | 16 waiters, push storm | wake-to-reply p99 < rival p99".

## Method

In-process, no server, no wire, no engine import.
Three waiter-set representations, all serving strict FIFO (longest-waiting first), each with park, wake and timeout unlink:

1. Intrusive doubly linked list, the spec choice.
Nodes come from a preallocated arena that models the resident shard arena, addressed by a uint32 index, with no Go pointers into the set and no map.
Park appends at the tail, wake dequeues the head, timeout unlinks any node from the middle, and each is O(1).
2. Ring/slice FIFO queue.
Park appends to a slice that reallocates and copies as it grows, wake advances a head cursor, and timeout finds the victim by scan and removes it by shifting the tail down, which is O(n).
3. Map of connection to node, keyed by conn id, FIFO by an auxiliary order slice.
This is the allocation and hash cost the spec deliberately avoids: every park hashes and allocates a node, every wake hashes to confirm the head is still live.

The sweep is over waiter count N in {1, 4, 16, 64, 256, 1024}.
For each N and each representation it reads three columns:
park ns per waiter, wake ns per waiter under a push storm that drains the set oldest first, and timeout-unlink ns per waiter when waiters are removed from random middle positions.
Park is measured with each structure's honest growth behavior: the arena is preallocated because the shard arena is resident, while the ring and the map grow from empty so their reallocation and hashing cost shows.

Two extra models sit beside the sweep.
The multi-element push serves k of N waiters in one shot (`RPUSH k v1 v2 v3` against three waiters hands v1 and v2 to the two oldest and parks nothing), covered by the property test.
The multi-key sibling unlink registers one client on m keys, and the first wake unlinks all m siblings so the client is never woken twice, priced as a function of m to show it is O(m) not O(N).

`go run .` runs the whole sweep; `-quick` shrinks the op counts for a fast check.
Timing uses a deterministic seed so the shuffle order is reproducible.

## What the doc predicts, and what this lab tests

- Intrusive doubly linked list, FIFO by link order, zero maps (section 5.12). Tested by the sweep; confirmed.
- Wake is a local dequeue plus a reply flush, flat in the waiter count. Tested by the wakeIL column, which is flat across N.
- Timeout unlinks a timed-out waiter O(1) from the middle (doubly linked). Tested by the toIL column against toRng.
- One push serving k waiters before any bytes touch the structure, parking nothing. Tested by TestMultiElementServe.
- Multi-key first wake unlinks all m siblings, O(m). Tested by TestMultiKeySiblingUnlink and priced by the multi-key table.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process.
All columns are nanoseconds per waiter except the multi-key table, which is nanoseconds per wake.
Representative of two back-to-back runs; the numbers wander run to run from cache and scheduler noise, so the orderings and the shape versus N are the signal, not the last digit.

Waiter-set sweep:

| N | parkIL | parkRng | parkMap | wakeIL | wakeRng | wakeMap | toIL | toRng | toMap |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 14 | 16 | 86 | 13 | 1.1 | 17 | 12 | 1.7 | 16 |
| 4 | 5.8 | 10 | 32 | 7.4 | 0.9 | 13 | 10 | 7.0 | 15 |
| 16 | 4.3 | 5.1 | 41 | 5.5 | 0.6 | 15 | 9.4 | 12.7 | 14 |
| 64 | 4.1 | 3.8 | 38 | 4.7 | 1.4 | 15 | 5.3 | 23.2 | 15 |
| 256 | 3.9 | 4.2 | 37 | 3.8 | 0.8 | 14 | 4.5 | 54.2 | 15 |
| 1024 | 4.1 | 4.4 | 37 | 3.6 | 1.0 | 18 | 4.7 | 171.3 | 17 |

Multi-key sibling unlink, one client on m keys, 256 background waiters per key:

| m | wakeNs |
|---|---|
| 1 | ~150 |
| 2 | ~230 |
| 4 | ~350 |
| 8 | ~390 |
| 16 | ~680 |

The small-N rows carry per-round setup overhead (arena and slice construction dominate a set of one), so read the shape from the N=64 row upward where the per-waiter cost has settled.

## Reading the sweep

The intrusive list is flat in N on every column it owns.
Its park settles at ~4 ns, its wake at ~3.6 to 3.8 ns, and its timeout unlink at ~4.7 ns, and none of the three move as N goes from 64 to 1024.
That is the whole claim: head dequeue is O(1) regardless of set size, and middle unlink is O(1) because the list is doubly linked, so a 1024-deep waiter set wakes and times out at the same cost as a 16-deep one.

The ring is the interesting rival because its pure wake is the cheapest number in the table, ~1 ns, since wake there is just a cursor advance with no node to unlink.
But that cheapness is borrowed against the timeout path, and the loan comes due there: toRng climbs 7, 13, 23, 54, 171 ns as N goes 4, 16, 64, 256, 1024, a clean O(n) line, because removing a timed-out waiter from the middle means a scan to find it and a shift to close the gap.
At N=1024 the ring times out a waiter at 171 ns against the intrusive list's 4.7 ns, a 36x gap, and BLPOP with a timeout is the common case, not the exception, so the timeout path is not a corner to discount.
The ring also cannot reclaim the slot of a timed-out waiter without that same O(n) shift, and it reallocates and copies the whole backing slice as it grows, both of which the arena-backed intrusive list avoids with an O(1) freelist push.

The map is beaten on the two columns that run hottest.
Its park is ~37 ns against ~4 ns for the other two, a ~9x tax paid on every single BLPOP because each park hashes the conn id and allocates a node, and its wake is ~15 ns against ~4 ns because each wake hashes to confirm the head is still live.
The map buys an O(1) timeout unlink, ~15 ns and flat, which is its one win, but it is a win over the ring, not over the intrusive list, which already has an O(1) unlink at a third of the cost and without the per-park hash and allocation.
This is exactly the allocation and hash cost section 5.12 says to avoid, and the sweep prices it: the map is never the cheapest on any column, and it is the most expensive on the two that fire on every blocked client.

The multi-key table confirms the sibling unlink is O(m), not O(N).
With 256 background waiters on each of m keys, the wake cost tracks m (roughly 150 ns at m=1 rising to ~680 ns at m=16) and does not track the 256 background waiters, because the first wake walks only the m-node sibling ring and unlinks each sibling from its own key's list in O(1).
A client blocked on 16 keys pays for 16 unlinks at wake, not for the hundreds of unrelated waiters sharing those keys, which is the property that keeps multi-key BLPOP honest under load.

## The wake-to-reply constant

The M3 gate row is wake-to-reply p99 at 16 waiters under a push storm.
The waiter-set contribution to that path is the wakeIL column, which is ~3.8 to 5.5 ns per wake at N=16 and stays flat as N grows.
So the frozen constant is: the intrusive-list waiter set contributes on the order of 4 ns per wake to wake-to-reply, flat across N up to 1024, which is far below the reply encode and socket flush that make up the rest of the path.
The gate should be read against rival wake-to-reply end to end, and this lab's result is that the fairness bookkeeping is not where that budget goes: a strict-FIFO wake costs single-digit nanoseconds and does not grow with the depth of the waiter set, so the p99 is set by the transport, not by the policy.

## Darwin caveat

These constants are measured on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The representation decision rests on the O(1)-versus-O(n) shape of the columns, which is structural and platform-independent: the flat intrusive-list columns and the linear toRng line survive any platform change because they are complexity, not constants.
The absolute wake-to-reply constant gets its Linux confirmation at the M3 gate run on GamingPC before the gate row is read, but the choice of representation does not wait on that number, since no plausible constant shift closes a 36x timeout gap or turns a per-park hash into free.

## Verdict

Frozen for the blocking-list slice: the intrusive doubly linked FIFO waiter set, exactly as doc 13 section 5.12 states.
It is the only representation that is O(1) on all three of park, wake and timeout unlink at once, and it is also the only one that reclaims a timed-out waiter's memory in O(1) and supports the O(m) multi-key sibling unlink, both of which the milestone needs.

- Intrusive list, chosen. Park ~4 ns, wake ~4 ns, timeout unlink ~5 ns, all flat in N to 1024. FIFO is link order so it is free. Nodes from the shard arena with an O(1) freelist, zero maps, addressed by index.
- Ring/slice, rejected. Its ~1 ns wake is the cheapest single number in the table but it is borrowed: timeout unlink is O(n), 171 ns at N=1024 against the intrusive list's 5 ns, and it cannot reclaim a timed-out slot or grow without an O(n) copy. BLPOP with a timeout is common, so the timeout path decides this, and the ring loses it by 36x.
- Map, rejected. It pays a ~9x park tax (~37 ns against ~4 ns) and a ~4x wake tax on every blocked client for the hash and the per-node allocation, and its one win, an O(1) timeout unlink, is a win the intrusive list already has at a third of the cost. This is the allocation and hash cost section 5.12 names, and the sweep confirms it is real.

The gate constant: the intrusive-list waiter set adds ~4 ns per wake to wake-to-reply, flat in N, so the M3 gate's BLPOP latency row is decided by transport, not by the fairness policy.

What the slice should bake in: per-connection wait nodes from the shard arena addressed by index, an intrusive doubly linked list per key with head and tail, FIFO serve by head dequeue, O(1) middle unlink on timeout paired with the timer-heap entry, a serve-up-to-k path so a multi-element push serves the k oldest and parks nothing, and a circular sibling ring across the keys of a multi-key waiter so the first wake unlinks all m siblings in O(m).

## Reproduce

```
go run ./labs/f3/m3/03_wakeup_fairness
go test ./labs/f3/m3/03_wakeup_fairness
```
