# HLL recompute kernel: cold PFCOUNT cost and histogram build shape

A PFCOUNT whose cache-stale bit is clear returns the cached 8-byte count and
costs a point read. When the bit is set (any PFADD since the last count changed
a register) PFCOUNT has to recompute: walk all 16384 six-bit registers, fold
them into a 64-entry value histogram, and run the closed-form Ertl estimator
over that histogram. This lab prices that cold path so the slice knows what it is
baking in before it writes the kernel.

Two things matter. First, how big the bounded scan is: the whole recompute is a
12288-byte register walk plus a fixed 52-entry fold, no allocation. That is the
ceiling on a cold PFCOUNT and it does not grow with cardinality. Second, how the
histogram build should read the packed array. The naive build extracts one
register at a time through the straddling shift-or macro, 16384 times. The word
build reads 12 bytes at a time and peels 16 registers per step with fixed shifts,
the layout Redis's dense reghisto fast path uses. Both feed the identical
histogram and the identical estimate, so the build shape is a pure speedup lever.

```
trueCard          naive       word  speedup     estNaive      estWord
100            37.692µs   27.624µs    1.36x          100          100
1000           29.838µs   27.370µs    1.09x         1000         1000
10000          24.965µs   23.849µs    1.05x         9967         9967
100000         15.121µs   11.896µs    1.27x        99394        99394
1000000        15.448µs   11.856µs    1.30x       998032       998032
```

`estNaive == estWord` on every row: the two builds are the same histogram, so the
word path only moves the build cost, never the answer. The estimates track the
true cardinality within the HLL standard error (1.04/sqrt(16384) ~= 0.8 percent),
which the test pins at four standard errors of slack.

The word build wins by 1.05x to 1.36x here, modest because a 12KiB walk is small
enough that both shapes stay in L1 and memory latency is not the bottleneck. The
point the slice needs is the shape, not the multiplier: the recompute reads the
array word-at-a-time into a stack histogram, the estimator is a fixed fold over
that histogram, and the whole thing is a bounded 12KiB scan with no allocation.
So the cache write on the first recompute is the only reason a cold PFCOUNT is a
write at all, and every warm PFCOUNT after it is a point read.

## Estimator constant

The Ertl normalizer is `0.5/ln(2)` = 0.7213475204444817, the same constant Redis
ships as `HLL_ALPHA_INF`. An early draft used the Euler-Mascheroni constant by
mistake and every estimate came out a flat 0.8x low (0.5772/0.72135), a clean
reminder that this is the one number the port cannot get approximately right.

## Run

```
go run ./labs/f3/m6/05_hll_recompute
go test ./labs/f3/m6/05_hll_recompute
```

`main_test.go` checks the packed 6-bit get/set roundtrip, that the word and naive
histogram builds are byte-for-byte the same over a range of fills, that the
ported estimator lands within the HLL standard error of the true cardinality, and
that an all-zero array estimates zero.
