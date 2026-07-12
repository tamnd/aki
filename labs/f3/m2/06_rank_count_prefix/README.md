# Lab 06: ZRANK count-prefix accumulation

Part of issue #544, the M2 zset milestone.
This lab lands with the count-prefix slice (engine/f3/struct Tree.bCountPrefix) so the kernel that slice bakes is measured, not assumed.
It answers whether the ZRANK descent's count accumulation is a lever on the zrank_zipf_c1m gate cell, which the box measured at 1.08x.

## Question

A counted B+ tree ZRANK (engine/f3/struct Tree.Rank) descends root-to-leaf, and at every interior level it adds the subtree counts of the children strictly left of the routed child: `acc += sum(bCount(node, 0..c))`.
The per-element reader `bCount(node, i)` recomputes the block slice and the count-array base offset and runs a count-width switch on every child.
A level touches up to arity-1 = 15 counts, a 1M-entry tree is about 5 levels deep, so one ZRANK pays that per-element arithmetic on the order of tens of times.

On a uniform-random ZRANK the descent blocks are cold, the op is memory-latency-bound, and that arithmetic hides under the cache misses.
On a zipf ZRANK the hot members reuse the same few descent blocks, they stay L1-resident, the op turns compute-bound, and the per-element overhead becomes the whole delta.
That is the zrank_zipf_c1m cell: a compute-bound cell where a constant factor can move the number.

So the question is which accumulation form to bake, and how much of any kernel win survives into a real ZRANK.

## Method

In-process, no server, no wire, no engine import, the M1 lab-05 and M2 lab-05 house style.
The lab models one interior branch block byte-identically to engine/f3/struct tree.go: a 256-byte block, arity 16, u32 subtree counts packed at the frozen countOff (192, the block's last cache line).
It sums the first c child counts three ways and cross-checks all three return the identical prefix (main_test.go TestKernelsAgree, c across the full range including the odd lengths the SWAR tail handles).

- `perElem`: the old form, `bCount` in a loop, block base and count-width switch recomputed per child.
- `hoisted`: block base and switch lifted above the loop, a bare strided read per child.
- `swar`: two u32 counts per u64 load summed in split 32-bit lanes, then the lanes folded, half the loop trips. Exact for any valid tree because each lane sums disjoint child subtrees, so each lane is at most the node subtree total, at most the tree entry count, below 2^32, and the low lane never carries into the high.

Two residency arms bracket the gate reality:

- `hot`: one branch block reused every op, L1-resident, the compute-bound zipf-hot descent. Where a kernel win must show.
- `cold`: a 16MiB ring of branch blocks, a pseudo-random block per op (a Knuth multiplicative stride, no PRNG in the timed loop), every read a miss, the memory-latency-bound uniform-random descent. The bound on what this slice can move on the uniform cells.

Swept over prefix length c in {1,2,4,8,15}, the routed child index, plus a full-descent arm of five prefix sums at representative routed children, one ZRANK's worth of accumulation.
`go run .` runs the whole sweep; `-quick` runs a smaller one.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-12, one process.
ns is nanoseconds per prefix-sum; `hoist_spd` is perElem/hoisted, `swar_spd` is perElem/swar, above 1.0 means faster.
Absolute ns wander about 10-15% run to run from cache and scheduling noise, so the ordering and the hot-cold gap are the signal.

```
== hot (L1, zipf-hot descent) ==
  c     perElem    hoisted       swar    hoist_spd   swar_spd
  1       2.059      1.997      1.961        1.03x      1.05x
  2       2.665      2.325      2.251        1.15x      1.18x
  4       3.322      3.384      2.747        0.98x      1.21x
  8       5.325      5.787      3.763        0.92x      1.41x
 15       9.889      9.817      5.633        1.01x      1.76x

== cold (random block, uniform descent) ==
  c     perElem    hoisted       swar    hoist_spd   swar_spd
  1       4.413      4.472      4.848        0.99x      0.91x
  2       5.396      5.940      5.331        0.91x      1.01x
  4       7.557      8.376      6.295        0.90x      1.20x
  8      11.337     11.162      8.410        1.02x      1.35x
 15      17.594     18.771     11.719        0.94x      1.50x

== full descent (5 levels, per-ZRANK accumulation) ==
arm                             perElem    hoisted       swar
hot (L1, zipf-hot)               31.392     28.536     20.741
cold (random, uniform)           63.117     59.651     42.825
```

## Verdict

The plain scalar hoist is noise: hoist_spd scatters around 1.0x (0.90x to 1.15x) on both arms.
The Go compiler already inlines `bCount` and lifts the offset arithmetic in the lab, where countOff is a package constant.
A scalar-hoist-only change is a perf no-op and would be dishonest to ship as a perf slice.

The SWAR two-counts-per-load kernel is the real lever, and it scales with c: 1.05x at c=1 up to 1.76x at c=15 on the hot arm, 1.51x on the full-descent hot accumulation (31.4ns to 20.7ns).
The hoist matters only because it enables SWAR: you cannot pack-load without a hoisted base.
So `Tree.bCountPrefix` ships the SWAR form for the production u32 width and keeps the scalar loop for the off-path u16 and u64 widths.

The win does not reach the zrank_zipf_c1m 2x gate, and this lab says why with numbers.
The count accumulation is a fraction of a full ZRANK, which also pays the route binary search per level, the leaf search, and the descent's memory access.
A 1.5x on the accumulation dilutes into a single-digit-percent end-to-end ZRANK gain, so 1.08x moves to roughly the mid-1.1x, not toward 2x.
The remaining gap is structural: a counted B+ tree ZRANK and a skiplist rank are both O(log n) and both cache-resident on the zipf hot set, so they sit at descent parity, and no count-accumulation constant factor closes a 2x on a parity descent.
This is the honest datapoint: the accumulation is not the ZRANK bottleneck, and effort toward the zrank 2x belongs on the descent structure or the dispatch floor, not here.

Frozen: `bCountPrefix` SWAR for u32; scalar for u16/u64. The end-to-end box A/B of the zrank_zipf_c1m aki-bench cell on the old versus new f3srv binary is pending the M2/M3 gate campaign freeing the box; this lab isolates the mechanism and bounds its ceiling.
