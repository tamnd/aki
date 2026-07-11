# M2 lab 02: subtree-count width

## Question

An interior node of the counted B+ tree stores, next to each child ordinal, the number of live entries in that child's subtree.
Those counts are the whole order-statistic machinery: rank sums them left of the search path, select descends by them.
Doc 12 sizes the count at 4 bytes, "capping a subtree at ~4 billion entries, which covers the collection cap with headroom" (line 234).
This lab sweeps the width, u16 against u32 against u64, on lab 01's frozen node size, and freezes the width.
The width sets the interior arity, because a 256-byte branch fits `branchSz / (8 separator + 4 ordinal + w count)` children.
So the sweep answers two coupled questions: does a narrower count overflow at the cardinalities a zset must hold, and what does each width cost in arity, tree height and memory.

## Method

In-process, no server, no wire, no engine import.
The tree is lab 01's counted B+ tree over fixed-size blocks in a flat arena, with the count width parameterised and the node size frozen at a 256-byte branch and a 512-byte leaf.
Keys are 8-byte sortable score prefixes, distinct so each stands for a distinct member.
For each width and cardinality the lab builds a random-insert tree, walks it to find the largest true subtree count any interior node must hold, and checks whether the stored counts still match the real subtree sizes.
A width that overflows truncates silently on the way in, so the stored count no longer equals the real one, and that inconsistency is the disqualifier.
It then times rank, select and insert, and reports interior bytes per entry.
Axes: width {u16, u32, u64}, cardinality {1k, 10k, 100k, 1M, 4M}.

## Results

Darwin, arm64, go test off, `go run .`, one machine, single run.

```
width      card  ari  lvl   maxCount    ovf    ok  rankNs   selNs   insNs     ibpe
u16        1000   18    3        269     no   yes    94.3    35.1   128.9     1.28
u32        1000   16    3        310     no   yes    93.3    37.6   118.9     1.28
u64        1000   12    3        235     no   yes    84.9    35.2   103.5     1.79

u16       10000   18    4       2897     no   yes   122.2    54.4   155.0     1.10
u32       10000   16    4       2924     no   yes   129.5    55.5   168.4     1.15
u64       10000   12    4       2266     no   yes   122.2    53.5   161.1     1.61

u16      100000   18    5      50185     no   yes   171.6    68.7   201.8     1.00
u32      100000   16    5      47368     no   yes   157.7    73.3   191.9     1.13
u64      100000   12    5      20085     no   yes   149.4    68.3   194.4     1.56

u16     1000000   18    6     559631    YES    NO       -       -   352.9     1.00
u32     1000000   16    6     486697     no   yes   313.7   150.1   343.7     1.14
u64     1000000   12    6     137751     no   yes   329.6   145.2   314.1     1.57

u16     4000000   18    6     761207    YES    NO       -       -   409.2     1.01
u32     4000000   16    6     524356     no   yes   406.3   198.0   457.1     1.14
u64     4000000   12    7    1192665     no   yes   400.2   206.8   429.0     1.57
```

`maxCount` is the largest true subtree count in the tree, the value the widest interior slot must hold.
`ovf` is set when that count passes the width's ceiling (65535, ~4.29e9, ~1.8e19).
`ok` is whether the stored counts still match the real subtree sizes; once a width overflows they do not.
`ibpe` is live interior-arena bytes divided by the real entry count, so it stays honest even when an overflowed tree miscounts its own cardinality.
Rank and select are skipped for an overflowed arm, because truncated counts return wrong ranks and can walk select off the end of a leaf.

## Reading

u16 holds until the cardinality where a single root-child subtree passes 65535, then it breaks.
The break is not gradual: for this key stream the root gains children that each stay just under the ceiling up to ~820k entries, then the root adds a level and its children jump past 440k, so `maxCount` steps from 56k straight to 559k at 1M.
Past that point every order-statistic query on a u16 tree is wrong, so u16 is disqualified for a store whose zsets reach into the millions.
Its lower arity cost (ibpe ~1.0, the cheapest interior overhead of the three) does not buy anything once the counts it stores are corrupt.

u64 is always correct but always the most expensive.
At arity 12 it carries ~1.57 interior bytes per entry, about 0.4B per entry more than u32, and at 4M it needs a seventh level where u32 and u16 still fit in six, so descent visits one more node.
The extra headroom, a ceiling of ~1.8e19, is far beyond any cardinality a single owner-local zset can reach, so it is memory and a tree level spent on range that cannot occur.

u32 is correct at every cardinality in the sweep, tops out at `maxCount` 524356 at 4M against a ceiling near 4.29e9, and carries ~1.14 interior bytes per entry at arity 16.
It sits between the two on every axis and is the only width that is both correct at scale and not paying for range it will never use.

## The bar

The milestone bar is PRED-F3-M2-ZSETMEM: tree overhead in the 2-3B per-entry band, over 5B blocks the milestone.
Interior nodes are a small fraction of that band.
The count width moves interior overhead by ~0.4B per entry between u32 and u64, and u32's ~1.14B per entry is well inside budget.
The bulk of the F14 band is the leaf level, its amortized headers and its 0.9-fill slack, which lab 01 froze by widening the leaf to 512 bytes.
So the width is chosen on correctness first, then on descent cost, not on pressure against the memory bar: every width the sweep tests is cheap in interior bytes, but only u32 is correct at scale without spending an extra tree level.

## Darwin caveat

These are darwin arm64 numbers on one machine, single run, no repeats.
The absolute ns/op and the exact overflow cardinality depend on the key stream and the machine, so treat them as shape, not as a bar to hold.
The overflow verdict is structural, not timing: a u16 count cannot represent a subtree larger than 65535 on any machine, and a million-entry zset produces such subtrees.
Confirm the arity, height and memory shape on the Linux gate box before the tree slice lands.

## Verdict

Freeze the subtree-count width at u32, 4 bytes, the width doc 12 already names.

- u16 is disqualified: a root-child subtree passes 65535 somewhere below 1M entries, the count truncates silently, and rank and select return wrong answers from there on. Million-entry zsets are in scope, so u16 is out on correctness, not memory.
- u64 is rejected: it is correct but costs ~0.4B per entry more interior overhead than u32 and pushes a seventh tree level at 4M, buying a ceiling (~1.8e19) far past any reachable cardinality.
- u32 gives arity 16, matching the doc 12 interior layout, a ceiling near 4.29e9 that covers the collection cap with headroom, and ~1.14B per entry of interior overhead.

The tree slice must encode these as tests:

- Arity is a pure function of the frozen node size and the count width: 256-byte branch gives arity 18 at u16, 16 at u32, 12 at u64. Pin it so a layout change cannot silently move it.
- A count at the width's ceiling round-trips and a count one past it truncates. This is the silent corruption the width choice guards against, so assert it directly on the count accessors.
- At a cardinality large enough that a root-child subtree passes 65535, a u16 tree reports inconsistent stored counts while a u32 tree of the same keys stays exact and ranks correctly. This is the disqualifier that fixes the width at u32.
- On a tree below any width's ceiling, rank and select agree with a sorted-slice model for every width, so the width changes only the layout that stores the counts, never the order-statistic answers.
