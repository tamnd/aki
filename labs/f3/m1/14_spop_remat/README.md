# M1 lab 14: SPOP removal by drawn index vs re-find by value

SPOP draws a uniform member index, copies the member for the reply, then removes
it. The removal used `rem(m)`, which re-finds the member by value: on the listpack
band it walks the packed entries with a memcmp per step until it reaches the drawn
position, then splices. The `at()` call that copied the member for the reply had
already walked to that same position. So one SPOP paid two walks to the drawn
index, the second one carrying the compare.

Its sibling SREM is handed the member by the client, so it walks once and clears
4.10x redis on the gate cell. SPOP, paying the extra re-find walk, sat at 1.92x
redis (median of 1.88/1.96/1.88), the one collection-mutate near-miss left after
the write-floor correction.

The change keeps the drawn index and removes with `remAt(i)`, which splices at the
known position and skips the compare/search. It is byte-identical: the member the
draw selected is the member removed.

## Sweep (go run ., ns per pop over a full drain)

```
size   card       by-value     by-index  speedup
8      8              43ns         24ns    1.79x
16     8              46ns         28ns    1.64x
32     8              56ns         38ns    1.47x
64     8              86ns         61ns    1.41x
8      32             59ns         27ns    2.19x
16     32             63ns         33ns    1.91x
32     32             86ns         44ns    1.95x
64     32            106ns         72ns    1.47x
8      128           117ns         41ns    2.85x
16     128           124ns         48ns    2.85x
32     128           142ns         65ns    2.18x
64     128           181ns         98ns    1.85x
```

The removal step is 1.4x-2.85x cheaper, widening with cardinality (the re-find
walk grows with the set while the by-index splice is bounded by the drawn
position). On the 1-member gate cell the saving is the removed memcmp plus the
skipped ParseInt/binary-search on the intset band; on the small multi-member band
it is the whole second walk.

## Safety

`TestRemovalIdentical` drains sets of every listpack size and cardinality both
ways under the same draw sequence and asserts the packed slab stays identical
after each pop, so `remAt` removes exactly the member the value re-find would
have. `TestByIndexNoCompare` confirms `packOffset` and `packIndex` agree on the
drawn entry's offset (the by-index path reaches it without the compare).

## Verdict

Removing by the drawn index is strictly less work than re-finding by value and is
byte-identical. Applied to `engine/f3/set/draw.go` (`popOne`) and
`engine/f3/set/set.go` (`remAt`). Box re-measure of the SPOP gate cell confirms
the row over 2x.
