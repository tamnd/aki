# spop: SPOP sampling and the whole-segment removal threshold

Milestone T3 lab 02 (spec 2064/sqlo1 doc 08 sections 2 and 5).

## Question

Two constants for T3 slice 4.
First, SE-I4: the planned SPOP allocator draws count distinct global positions over the fence's prefix counts (a sparse partial Fisher-Yates, O(count) regardless of set size), which yields exact hypergeometric per-segment takes with no distribution sampling; the lab must show this is uniform over members and that the cheap null (count spread evenly over segments) is not, since median splits and lazy merges leave occupancy anywhere between half and full.
Second, the large-count strategy: editing every touched segment approaches a full rewrite of the set as the pop grows, and at some point emitting the popped members and bulk-rebuilding the remainder into fully packed fresh segments (doc 09 section 6 pattern, old plane retired by one genbump) is cheaper. Where is the crossover, in units the engine can check in O(1)?

## Method

No store underneath: the model builds the segmented set through the real insert-split path (fh median splits at 503 entries, the 4032-byte segment over 3-byte-header short members), churns a quarter of the members out and back in to widen the occupancy spread, then prices both arms per pop count without mutating, so every trial samples the same population.
Uniformity is chi-square over per-member pop tallies with the finite-population correction: SPOP draws without replacement, so tallies are Bernoulli sums with variance deflated by (1 - count/N), and the uncorrected statistic reads impossibly even (z about -5 at count/N of 5 percent).
Cost is write frames and payload bytes: an emptied segment is one delete frame with no payload, an edited segment rewrites its remainder, the rebuild arm writes ceil(remaining/503) packed segments plus root and genbump.
Tests pin the build invariants, the allocator contract (takes sum to count, picks distinct and in range), the pass/fail oracle, and the cost edges (full pop = all deletes on one arm, bare root delete on the other).

## Run

    ./run.sh            # members {5e4, 2e5, 1e6}, uniformity + cost sweep
    go run . -quick     # smoke
    go test ./...       # invariants, allocator, oracle, cost edges

## Results (local, 2026-07-16, deterministic and box-independent)

Uniformity, 200 trials at count = N/20:

| members | segments | positions z | even-null z |
|---|---|---|---|
| 50000 | 129 | -1.01 | 26.5 |
| 200000 | 523 | 0.24 | 71.3 |
| 1000000 | 2885 | -0.61 | 447.1 |

The position allocator sits inside |z| < 3 at every size while the even null fails louder as segment count grows; occupancy after churn spans roughly 230..503 of 503, so the null's bias is structural, not noise.
Allocator throughput is 58-89 ns per popped position across sizes.

Cost crossover (bytes, edit vs rebuild): edit wins clearly while a meaningful share of segments goes untouched, rebuild wins past the point where the pop touches essentially every segment.
In count-per-segment units the bytes crossover lands between 8 and 16 takes per segment at every size: at 129 segments the flip is between count 256 and 1024, at 523 segments count 4096 still edits by 0.06 percent and 10000 rebuilds, at 2885 segments the flip sits inside the 4096..50000 gap.
A segment's untouched probability is about exp(-count/segments), so by 8 takes per segment the untouched share is e^-8 and both arms write nearly the same payload; the arms differ by under 1 percent near the boundary, which makes the constant insensitive.
Frames tell the same story harder: at the boundary rebuild already needs fewer frames (packed segments versus churned occupancy), and at 99 percent pop it is 524 frames versus 6 at 200k members.

## Verdict

Slice 4 ships the position allocator as designed; whole-segment removal needs no threshold of its own, it falls out whenever a segment's take equals its live count.
The rebuild switch is count >= 8 x fence length, both sides O(1) from the root: below it edit in place, at or above it emit the popped members and bulk-build the remainder (count == cardinality stays the trivial full-delete).
Any factor in 8..16 is defensible; 8 is chosen because rebuild is already ahead on frames there and repacking also heals churn occupancy for free.
SSCAN cursors survive the rebuild because they are fh-based, not segment-id-based.
