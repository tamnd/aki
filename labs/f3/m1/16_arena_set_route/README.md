# Lab m1/16: the tiny-set arena-route throughput cost

Lab m1/15 proved the storage footprint: a tiny set inline in one arena record
clears the memory bar at 0.41x the Go-heap wall. This slice wires the set command
funnel onto that record, and the wiring changes the per-command shape a tiny set
pays.

The old home keeps the set live in the Go-heap registry map `g.m`, so a command
is a map lookup and an in-place mutation of a struct that stays put between
commands. The arena home keeps the set inline in a store record, so a command
must resolve it out of the record (`PeekCollBlob`, then copy the blob into the
reusable scratch the way `resolveTouch`/`loadInline` do), mutate the copy, and
write it back (`PutCollBlob`, the `commit` republish).

That record round-trip is the price the memory win charges on the write path: one
peek, one small-blob copy, one republish per mutating command versus the map's
in-place edit. This lab prices that overhead against the real
`engine/f3/store` code, so the routing lands only if the per-command cost is a
bounded small absolute figure a tiny set can afford.

## Method

Two arms, single-member sets (member `"hello"`), no server, no wire. Each runs
the same SADD-of-one-member cycle over a population of distinct tiny sets, so the
working set does not collapse to one hot key and the numbers reflect the gate's
2M-set spread.

- **wall**: a real `map[string]*wallSet` of the tiny-set shape, mutated in place
  (the resolved struct's blob is appended to, no write-back), the shape a
  registry-homed SADD runs.
- **arena**: the real `store.Store` driven through the funnel's two round-trips:
  `PeekCollBlob` to resolve, a blob copy into scratch (the `loadInline`), the
  member add, and `PutCollBlob` to commit the republish.

Read ns per mutating command for each arm and the arena route's absolute
overhead.

## Result (GamingPC-class dev box, reps 3, 1M commands/rep)

```
     count     wall ns/cmd    arena ns/cmd  arena overhead
    100000            84.2           120.3          +36.1
    500000           171.3           185.7          +14.4
   1000000           196.4           266.9          +70.6
   2000000           236.7           267.5          +30.8
```

Both arms are dominated by the keyspace lookup at these populations; the arena
route adds a bounded **~15-70 ns per mutating command** for the peek, the
tiny-blob copy, and the republish, a small fraction over the map baseline and a
fixed absolute cost that does not grow with the population. That is the bill the
memory win pays: lab 15's 0.41x per-set footprint, which the single-shard box RSS
the gate cell reads is made of.

## What this does and does not settle

This bounds the per-command CPU cost the routing flip adds; it does not itself run
the gate. The memory bar (lab 15) is the reason to pay the overhead, and the
box-measured single-shard RSS at the 2M cell is the follow-on confirmation once
the routing lands. See `engine/f3/set/reg.go` `commit` and `resolveTouch`, and
`engine/f3/store/collblob.go`.
