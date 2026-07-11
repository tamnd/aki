# Lab 02: the inline set threshold

Part of M1 (issue #543), the set milestone; doc 11 section 3 is the gate.

## Question

The inline band keeps a set as one packed blob and answers membership without a hash table: a sorted integer array by binary search (intset-class) or a length-prefixed member blob by linear scan (listpack-class).
It converts one way to the native member table once it breaches a cap, and doc 11 section 3 freezes those caps at Redis's defaults so OBJECT ENCODING parity holds: set-max-intset-entries 512, set-max-listpack-entries 128, set-max-listpack-value 64.
The slice bakes those three numbers.
Before it does, this lab prices the two inline representations against the member table they would convert to, so the caps are a measured crossover and not an inherited guess.
The rule the caps have to satisfy: a set stays inline only while its point ops sit at or under the structure it would become, or, where they do not, only while the absolute cost stays buried under the per-command fixed cost so the slower scan never gates.

## Method

In-process, no server, no wire.
The member table is the next slice (lab 01 settles its load factor and probe stepping), so this lab stands in for it with a Go `map[string]struct{}`.
That is the pessimistic proxy: a real open-addressed tag-probed table is faster and allocation-free, so if the packed blob beats even a map up to the cap it beats the native table too.
For each cell the lab builds the structure at a cardinality and, for listpack, a member width, then times the adversarial membership op: a lookup of an absent member, which forces the listpack scan to walk every entry and the binary search to full depth.
The miss is the conservative price the conversion threshold has to cover; a hit averages half a scan.
Five reps per cell, median reported, four million probes per rep.

## Results

Apple M4 (darwin/arm64, macOS 15.7.8), go1.26.5, median ns/op on the absent-member probe.

intset-class, sorted int64 binary search against the map proxy:

| card | intset | map |
|---|---|---|
| 1 | 2.28 | 2.51 |
| 8 | 2.03 | 4.01 |
| 32 | 2.78 | 5.11 |
| 128 | 3.43 | 4.35 |
| 256 | 3.86 | 4.03 |
| 512 | 4.27 | 4.58 |
| 1024 | 4.77 | 4.62 |

listpack-class, packed-blob linear scan with the one-byte tag reject, 8B members:

| card | listpack | map |
|---|---|---|
| 1 | 3.06 | 7.32 |
| 4 | 3.95 | 7.87 |
| 8 | 5.84 | 8.49 |
| 16 | 9.89 | 5.77 |
| 32 | 17.96 | 5.93 |
| 64 | 36.10 | 5.64 |
| 128 | 131.58 | 6.70 |
| 256 | 217.35 | 5.80 |

listpack-class at the value cap, 64B members:

| card | listpack | map |
|---|---|---|
| 1 | 5.52 | 10.77 |
| 8 | 8.08 | 11.67 |
| 16 | 12.62 | 8.84 |
| 64 | 37.24 | 10.26 |
| 128 | 140.00 | 9.23 |

The two representations behave differently and the caps land differently as a result.

Binary search over the sorted integer array stays flat-log: it holds 2 to 4ns from one member to 512 and ties or beats the map proxy the whole way, only reaching parity past 1024.
The intset cap of 512 is not a performance ceiling at all; it is where Redis switches encodings, and the structure is still winning there.

The listpack linear scan crosses the map early.
At 8 members the scan (5.84ns) is still under the map (8.49ns); at 16 it passes it (9.89 vs 5.77ns); by the 128-member cap it is 20x the map (131.58 vs 6.70ns).
Member width moves the miss cost only modestly because the tag reject skips most entries without a compare, so a 64B set at the cap (140ns) is close to an 8B set at the cap (131ns).
So the listpack cap of 128 is well above the point where a hash probe would be the faster structure, which is about 8 to 16 members.

## Verdict, frozen

The slice bakes the three Redis caps, and the reasons are now measured, not inherited.

intset entries 512: binary search over the sorted array beats the member-table proxy from 1 to 512 members and only reaches parity past 1024, so the cap is conservative on speed and set purely by OBJECT ENCODING parity.
The intset stays the right structure for its whole band.

listpack value 64B: the tag reject keeps the miss cost nearly width-flat, and 64B holds a packed entry within a couple of cache lines; the cap is set by parity and the memory bar, not by a scan cliff.

listpack entries 128: kept at Redis's default for OBJECT ENCODING parity, even though the linear-scan crossover against a hash probe is about 8 to 16 members, an order of magnitude below the cap.
The blob is not the faster structure from 16 to 128 members; it is the cheaper-in-memory structure whose absolute cost stays buried.
At the 128 cap a miss is about 130 to 140ns, which is dominated by the per-command fixed cost, the parse, dispatch, and reply that doc 11 section 3.4 names as the thing that dominates at this size and doc 03 budgets in the hundreds of nanoseconds.
The scan never sits on a hot inner loop; it is one membership test per command against a set that a real deployment keeps tiny, and the member table waits at 128 for the case where the set does not stay tiny.
The one thing this frozen number does buy is the seam: the slice converts to the native band exactly at 128 entries or 64B, so the 20x gap the table above shows past the cap is never paid.

If a later measurement wants inline point ops that also win on speed, the lever is not moving the cap (that breaks OBJECT ENCODING parity) but making the inline listpack scan branch on a per-entry SWAR tag block, which is a separate change with its own lab.
That is out of scope here; the slice ships the parity caps.

## Confirmation owed

These are darwin/arm64 numbers on an Apple M4.
The gate box (i9-13900K, linux/amd64) is busy with the M0 arena-rss work, so the Linux confirmation of the two crossovers (intset flat to 512, listpack crossover at 8 to 16) is deferred to the M1 gate run.
The verdict does not hinge on the box: the intset result is a binary search beating a map, and the listpack result is a linear scan losing to a map past a dozen entries, both of which are architecture-independent in direction; only the exact crossover cardinality can shift, and the frozen caps have an order of magnitude of margin over it.

## Reproduce

```
go run ./labs/f3/m1/02_inline_threshold/
```

Flags: `-iters` probes per rep (default 4,000,000), `-reps` reps per cell (default 5, median reported).
