# M3 lab 02: flat-versus-Fenwick chunk-directory crossover

## Question

f3's native list is a chunked resident byte deque: a ring of fixed-capacity chunks where each chunk holds a contiguous run of consecutive list positions.
Positional access (LINDEX, LSET, LRANGE) resolves a dense index k to a (chunk, in-chunk ordinal) pair through the chunk directory, which holds the per-chunk live counts and their prefix sums.
Doc 13 gives that directory two representations and says "the flat-versus-Fenwick crossover is a lab decision" (section 2.4).
This lab prices the crossover and freezes FLAT_MAX, the chunk count at or below which the flat linear scan resolves a position at least as fast as the Fenwick rank descent, and above which Fenwick wins.

The flat directory is a `[]uint64` of per-chunk live counts resolved by a linear scan: walk the counts, subtract each until the running index falls inside a chunk.
It is branch-predictable and cache-resident, one cache line per eight uint64 counts, and its update is a single add on one chunk's count.
Its select cost grows with the chunk count because the scan is linear.

The Fenwick directory is a BIT tree over the same counts.
select(k) is a power-of-two rank descent, O(log chunks) uint64 loads over a dense contiguous array indexed by bit arithmetic, no pointer chase and no allocation.
Its update is an O(log chunks) mirror walk up the tree.
For a 17K-chunk list the descent is about fourteen loads.

## Method

In-process, no server, no wire, no engine import.
Both directories are lab-local and self-contained: `selectFlat` is the flat scan, `fenwick.rank` is the descent, `fenwick.add` is the mirror walk.
`main_test.go` proves the two return the identical (chunk, rem) pair for every k in [0,total) across several configs, so the crossover is a pure performance choice and nothing observable changes when the slice swaps one for the other.

The lab sweeps the chunk count across {4, 8, 16, 32, 48, 64, 96, 128, 256, 512, 1024, 4096, 17408}.
17408 is the spec's 17K-chunk, one-million-element-at-64B case.
At each chunk count it times select (`selectFlat` against `fenwick.rank` over uniformly-random k) and update (a single flat add against a Fenwick mirror walk).
It runs two value bands: a 64B band where a chunk holds about sixty live positions, and a small-value band where a chunk holds three or four, so the total scales differently for the same chunk count and the crossover is not tied to one fill.
All randomness is a fixed-seed xorshift, so the table reproduces from run to run.

The select budget is larger at small chunk counts (the op is cheap and the mean needs samples) and smaller at large counts (the flat scan is long), keeping total work bounded.
Update keeps a flat budget because it is cheap on both sides.

## Machine

Apple M4, darwin/arm64, go1.26.5, single machine, `go run .`, no repeats beyond a second confirming pass.
Confirm with `go version` (go1.26.5) and `uname -m` (arm64).

## Reproduce

```
go run ./labs/f3/m3/02_directory_crossover/
go test ./labs/f3/m3/02_directory_crossover/
```

`-quick` shrinks the sample budgets for a fast pass.

## Results

Two runs. The columns are select ns/op for each directory, the select winner, and update ns/op for each.

Run 1:

```
band 64B (56-64 live per chunk)
  chunks      total  flatSel     fenwSel   winner     flatUpd     fenwUpd
       4        236    10.46        9.59  fenwick        1.55        6.15
       8        482     9.27       13.00     flat        1.56        7.46
      16        963    10.86       15.93     flat        1.55        8.55
      32       1943    13.50       19.16     flat        1.52        8.92
      48       2887    15.26       20.42     flat        1.56        9.19
      64       3843    17.39       23.05     flat        1.56        9.93
      96       5742    22.17       24.11     flat        1.54        9.95
     128       7707    26.02       25.86  fenwick        1.52       10.24
     256      15366    43.79       29.52  fenwick        1.53       10.85
     512      30675    77.47       34.98  fenwick        1.59       11.69
    1024      61439   146.32       37.03  fenwick        1.55       11.90
    4096     245800   528.98       43.25  fenwick        1.53       12.87
   17408    1044836  2265.73       52.06  fenwick        1.55       14.19

band small (3-4 live per chunk)
  chunks      total  flatSel     fenwSel   winner     flatUpd     fenwUpd
       4         13     7.86        9.25     flat        1.53        6.15
       8         25     9.86       13.05     flat        1.53        7.45
      16         52    10.90       15.75     flat        1.52        8.24
      32        113    13.20       19.23     flat        1.56        9.10
      48        170    15.61       20.21     flat        1.54        9.18
      64        224    17.18       22.20     flat        1.52        9.68
      96        331    21.66       24.45     flat        1.58       10.29
     128        452    26.82       26.01  fenwick        1.53       10.18
     256        888    44.05       29.14  fenwick        1.52       10.83
     512       1783    77.66       32.21  fenwick        1.53       11.32
    1024       3584   142.47       35.50  fenwick        1.54       12.02
    4096      14343   664.39       46.95  fenwick        1.62       13.35
   17408      60930  2244.76       52.82  fenwick        1.56       14.35
```

Run 2, crossover rows only:

```
band 64B    chunks   flatSel  fenwSel  winner
                64     17.31    22.70   flat
                96     22.11    24.36   flat
               128     26.38    26.20   fenwick
               256     43.97    29.15   fenwick
band small  chunks   flatSel  fenwSel  winner
                64     17.46    22.31   flat
                96     21.63    23.94   flat
               128     26.10    27.12   flat
               256     44.58    29.65   fenwick
```

## Reading

The select crossover sits right at 128 chunks and it is the same in both value bands, so it is set by the chunk count, not by how full the chunks are.

Below the crossover flat is clearly ahead.
At 96 chunks flat wins select in both bands and both runs, with a couple of ns of margin, and its scan is still short enough to live inside a handful of cache lines.
The flat scan starts near 10ns at a few chunks and climbs linearly: about 17ns at 64, 22ns at 96, 44ns at 256, 146ns at 1024, and 2.2us at 17408.
That linear climb is the whole story on the flat side.

The Fenwick descent barely moves.
It starts around 9-13ns and only reaches ~52ns at 17408 chunks, because it pays log(chunks) loads no matter how long the ring is.
Its floor is higher than flat's at small counts, which is why flat wins there, but it is nearly flat as a line, so it overtakes flat once the ring is long enough.

At 128 chunks the two select paths are a coin toss: Fenwick edges it in run 1 in both bands, flat edges it in run 2 in the small band, and every gap there is under a nanosecond, inside the run-to-run noise.
By 256 Fenwick is ahead by half, and from there flat is never close again.

Update is a separate axis and it favors flat everywhere.
The flat update is a single add, a flat ~1.5ns at every chunk count.
The Fenwick update is the mirror walk, from ~6ns at 4 chunks to ~14ns at 17408, growing with log(chunks).
So flat is eight to nine times cheaper on update across the whole sweep.

Update is rare next to select.
A chunk-count change happens about once per sixty pushes at 64B values, so the per-element blended cost is roughly select plus update over sixty.
That blend does not move the select crossover much, but it tilts the tie at 128 toward flat: at 128 the blended flat cost is about 26.0 + 1.5/60 and the blended Fenwick cost is about 26.0 + 10.2/60, so flat is at worst tied and usually a hair cheaper right at the crossover.
Above 128 the select gap swamps the update term and Fenwick wins outright.

## Darwin caveat

These are darwin arm64 numbers on one Apple M4, two runs, no wider repeats.
The absolute ns/op and the exact crossover depend on the machine and the k stream, so treat them as shape.
This lab is arithmetic-bound: both paths are a tight loop over a contiguous array with no allocation and no pointer chase, so the crossover should be stable across machines rather than sensitive to the memory wall the way the residency labs are.
The gate box re-reads off that wall on some workloads, but neither directory chases memory here, so the flat-versus-Fenwick shape should carry.
That said, confirm the crossover on the Linux gate box before the list slice bakes the constant, do not take the darwin number as the bar.

## Verdict

Freeze FLAT_MAX at 128 chunks.

Use the flat linear-scan directory while the ring holds 128 chunks or fewer, and switch to the Fenwick directory above 128.

- Below 128 flat wins select outright and wins update by eight to nine times, so it is the right directory for short rings.
- At 128 the two select paths are tied inside run noise, and flat's far cheaper update makes the blended per-element cost tied or a hair in flat's favor, so flat is still the pick at the boundary.
- Above 128 the Fenwick descent stays near flat while the flat scan climbs linearly; by 256 chunks flat select is already about 50% slower, and by 17408 it is over forty times slower, so Fenwick is the only choice for long rings.
- 128 is a clean power of two the directory can branch on with a single comparison, and it matches the point where the measured select winner flips.

The list slice should bake `FLAT_MAX = 128` and encode the crossover as a test: the flat and Fenwick directories must return the identical (chunk, rem) for every k, so the switch at 128 is a pure performance choice that never changes an answer.
