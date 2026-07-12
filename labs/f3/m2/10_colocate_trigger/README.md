# M2 lab 10: the slab co-location trigger, when an ordered read should reorder

## Question

Lab 09 froze the layout: architecture A (reorder the member slab into rank order, leave the records in insertion order) captures 76..85 percent of the ZRANGE scatter penalty, ZSCAN-safe and with no added memory.
This lab settles the other open decision of the co-location plan (milestones/M2-zrange-colocation-plan.md, open decision 3): WHEN to run that reorder.
The reorder is O(card), so a trigger that fires it too eagerly turns a read/write mix into O(card)-per-read thrash, and one that fires too late leaves the scatter in place.
The build-then-read shape the gate measures (all ZADD, then many ZRANGE, no deletes) never trips the existing churn rebuild, so the reorder needs a new trigger, and the trigger needs a predicate that wins on that shape without thrashing on any other.

## Method

The engine predicate under test (zset/skiplist.go `maybeColocate`) is two gates:

- **divergence gate**: reorder only once at least `card/D` members were inserted or rescored since the last reorder (`D = 8`), so a slab that is still almost entirely in place is left alone.
- **read gate**: reorder only once ordered reads have streamed at least `card/R` elements since the last reorder (`R = 1`), so the O(card) reorder is amortized over the reads that benefit and a write-heavy, rarely read store never pays for a reorder it cannot recover.

Three real kernels are timed at the 1M gate cell (no engine import, the record and slab shapes mirror `zset/skiplist.go`, and `main_test.go` proves the two walks emit identical bytes): a scattered walk (today's layout), an architecture-A walk (records scattered, slab rank-ordered, what a co-located store reads), and a whole-slab reorder (the exact work `colocateSlab` does).
A deterministic read/write op stream then drives each predicate using those measured per-element costs, starting from a fully scattered slab (the incremental-ZADD build).
A read serves each element scattered with probability `divergence/card` and sequential otherwise, the honest expected split for a partially co-located slab.
The two-gate predicate is compared against a naive divergence-only predicate (no read gate), the never-reorder baseline, and the always-sorted ideal.

## Result (Apple M4, go1.26, card 1M, member 8B, window 100)

```
measured: scattered read 28.769 ns/e, arch-A read 15.133 ns/e, reorder 12.254 ns/e

Table 1: net ns per read op by write fraction (100000 reads)
   writeFrac      baseline        naive     two-gate        ideal   naive_reord gate_reord
    0.000000      2876.89      1635.88      1772.22      1513.35            1          1
    0.010000      2876.89      1636.57      1772.78      1513.35            1          1
    0.100000      2876.89      1643.46      1778.36      1513.35            1          1
    0.500000      2876.89      1704.06      1827.45      1513.35            1          1
    0.990000      2876.89     11401.26      3482.45      1513.35           80         10
    0.999900      2876.89    944265.88      4096.04      1513.35         7693         10
    0.999995      2876.89  12255130.05      4101.87      1513.35       100000         10

Table 2: read-heavy (writeFrac 0), effect of the read-gate divisor R
     R       ns/read     reorders   reads_to_win
     1      1772.22            1          10000
     2      1704.04            1           5000
     4      1669.96            1           2500
     8      1652.91            1           1250
```

Three readings.

**On the gate shape and every read-heavy mix, the predicate captures the win with one reorder.** From pure reads through a 50 percent write fraction the two-gate predicate reorders exactly once and settles at 1772..1827 ns per read op against the 2877 baseline (1.6x) and the 1513 ideal, the arch-A layout amortized over the run. The divergence gate alone keeps reorders rare here: the slab has to diverge by an eighth of the cardinality before any reorder, so an occasional write cannot trigger one.

**The read gate is what caps the write-heavy tail.** Past a write fraction where more than `card/8` writes fall between consecutive reads (the last three rows), the naive divergence-only predicate reorders on nearly every read and blows up to 11k, then 944k, then 12.2M ns per read op (4260x the baseline): it pays a full O(card) reorder to serve one window. The two-gate predicate holds the same regime to 3482..4102 ns (at most 1.43x the baseline) with ten reorders, because its read gate refuses to reorder until reads have streamed a cardinality of elements, so a reorder is amortized to at most one sequential copy per read element no matter how the writes fall. That bound, not "never worse than baseline", is the guarantee, and it turns a 4260x cliff into a 1.4x shoulder.

**R trades warm-up length for the steady state and the tightness of the bound.** At `R = 1` the first reorder waits 10000 reads (a cardinality of window-100 reads) and the steady state is 1772; larger R reorders sooner (`R = 8` at 1250 reads) for a small steady-state gain (1653), but the write-heavy amortized reorder cost scales with R (one reorder per `card/R` read elements), so a bigger R is a proportionally looser thrash bound. `R = 1` is the conservative amortized choice.

## Verdict (frozen)

**Two gates: divergence `card/8`, read `card` (`D = 8`, `R = 1`).**

- The **divergence gate at `card/8`** is the primary trigger and carries every read-heavy mix on its own: it fires one reorder on a diverged, read store and ignores a near-sorted one, landing the arch-A win (1.6x here) with a single O(card) pass.
- The **read gate at `card`** is the safety rail for the write-heavy tail: without it the divergence-only predicate is a 4260x cliff once writes crowd out reads; with it the worst case is a 1.4x shoulder, because the reorder is amortized to one sequential copy per read element. It costs a warm-up of one cardinality of reads before the first reorder on the gate shape, absorbed by any benchmark that warms up.
- **R stays at 1** (conservative amortization) rather than a larger value that shortens the warm-up but loosens the write-heavy bound proportionally. If the GamingPC box A/B shows the warm-up eating the ZRANGE win on a short measured phase, R is the one knob to raise, and Table 2 prices it.

The end-to-end effect is measured by a box A/B of the `zrange_c10k`/`zrange_c1m` aki-bench cells on the old versus new f3srv; this lab only fixes the trigger the engine change carries.
The measured constants drift with dev-box load (the arch-A and reorder costs especially), but the policy columns, reorder counts and the naive blow-up, are arithmetic on the op stream and independent of the timing.
