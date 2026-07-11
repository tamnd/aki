# Lab 03: score codec

Part of M2 (issue #544), the zset milestone; doc 12 section 3 is the gate.
This lab lands before the dual-write slice so that slice bakes a settled order-key form, a settled ZSCORE round-trip path, and a compare-cost number, not a guess carried from the inline band.

## Question

The native-band tree keys on (sortable score, member) so that memcmp on the tree's byte keys equals the zset total order (doc 12 sections 2.3, 3.1).
Raw IEEE-754 doubles do not byte-sort: the sign bit is high and negative magnitudes run backwards.
Doc 12 section 3.1 prescribes the order-preserving transform, flip the sign bit for a non-negative double and invert every bit for a negative one, so -inf is the smallest eight bytes, +inf the largest, and ordinary doubles fall in numeric order between.

Three questions the tree slice and the dual-write slice need answered before they commit:

- Order-key form.
  Should the tree key on the transformed u64 (encode once at insert, compare as an integer) or keep the raw float64 in the node and compare with a float less?
  And when the doc draws separators and leaf entries as an 8-byte big-endian score prefix followed by the member bytes (section 2.3), should a descent compare that whole composite with one bytes.Compare, or compare the 8-byte prefix first and fall to the member only on a score tie?
- Transform cost.
  What does encode cost per insert and decode cost per range-bound recovery, and is the transform exactly invertible for every non-NaN float64?
- Total-order edge cases.
  Does the literal transform honor the contract at -0.0 versus +0.0 (Redis orders them equal), at the infinities, and for the 52-bit geohash integers M6 rides (doc 12 section 7)?

## Method

In-process, no server, no wire, no engine import.
Lab-local kernels model the tree's three candidate order-key forms over the same underlying scores and members, so the three descent kernels do identical comparison work and only the compared representation differs.

- rawFloat keeps the score as a float64 in the node and compares with a float less, member bytes on a tie.
- u64 keys on the transformed integer and compares the u64, member bytes on a tie.
- byteComposite stores the 8-byte big-endian sortable prefix followed by the member bytes as one blob and compares with a single bytes.Compare, the form section 2.3 draws.

The descent kernel walks depth interior nodes of 15 separators each, linear-scanning the separators the way an arity-16 B+ node does without SIMD, so the timed loop is comparison-bound.
Depth is the arity-16 interior height at each cardinality (3 at 1k, 4 at 10k, 5 at 1M).
Two regimes: distinct scores, where the score compare decides and the member is never touched, and a fully tied band at score 0, the autocomplete and lex shape of section 3.2, where every compare falls through to the member bytes (the 24-byte tied members carry a shared prefix so the memcmp must walk past it).
The transform is timed separately, encode and decode, over 20M random doubles.
Correctness lives in main_test.go: exact round-trip over the special doubles plus two million random bit patterns, total-order agreement against a reference comparator over the special and random score pairs, the signed-zero collapse, the infinity placement, and the geo exactness-and-order sweep.

## Results

Apple M4 (darwin/arm64, macOS 15.7.8), go1.26.5, median ns/descent over 7 reps, 2M descents per rep.
A descent is depth times 15 comparisons, so per-comparison cost is the ns/descent divided by 45 (1k), 60 (10k), or 75 (1M).

Distinct scores (the common case):

| card | depth | member | rawFloat | u64 | byteComposite |
|---|---|---|---|---|---|
| 1000 | 3 | 8B | 31.3 | 31.1 | 80.1 |
| 10000 | 4 | 8B | 44.3 | 44.4 | 113.5 |
| 1000000 | 5 | 8B | 61.4 | 61.3 | 160.2 |
| 1000 | 3 | 24B | 35.9 | 36.0 | 92.2 |
| 10000 | 4 | 24B | 47.8 | 47.6 | 122.0 |
| 1000000 | 5 | 24B | 55.8 | 56.0 | 142.1 |

Fully tied band at score 0 (the lex and autocomplete shape):

| card | depth | member | rawFloat | u64 | byteComposite |
|---|---|---|---|---|---|
| 1000 | 3 | 8B | 103.6 | 114.0 | 100.5 |
| 10000 | 4 | 8B | 147.1 | 149.5 | 131.0 |
| 1000000 | 5 | 8B | 180.1 | 189.0 | 167.8 |
| 1000 | 3 | 24B | 116.1 | 110.7 | 116.7 |
| 10000 | 4 | 24B | 141.7 | 145.4 | 142.8 |
| 1000000 | 5 | 24B | 227.5 | 231.8 | 220.3 |

Transform: encode 0.54 ns/op, decode 0.35 ns/op.

Three things the table says, and one the tests say.

First, u64 and rawFloat are the same cost at descent time.
Across every distinct row the two sit inside a couple of percent of each other, which is measurement noise on a busy box (absolute ns wander run to run, the raw-versus-u64 gap does not).
Comparing a transformed u64 is not more expensive than comparing the raw double, because both are one register compare, so keying the tree on the sortable form costs nothing on the read path.
That matters because the sortable form is what lets the separators and leaf keys be plain 8-byte big-endian values that memcmp in order, which the range walks and the cold-chunk directory both want.

Second, the monolithic byteComposite compare is 2 to 2.7x slower in the distinct case.
A single bytes.Compare over the 8-byte prefix plus the member drags the member bytes through the comparison setup on every separator even though the score prefix already decided the branch, and at 8-byte members that setup is most of the cost.
So the node does not store one composite key and memcmp it; it compares the 8-byte score prefix as a u64 first and consults the member only when two prefixes tie, which is exactly the section 2.3 design (the 8-byte separator prefix routes the common case and member tie-break bytes live in a spill slot for the tied bands).

Third, the composite only pulls even in the fully tied band.
When every score is equal the split form pays a dead score compare on every separator before it reaches the member, so byteComposite's single memcmp is a few percent cheaper (100.5 against 103.6 at 1k/8B, 167.8 against 180.1 at 1M/8B).
This is the lex-only pathology of section 3.2, and the margin is small and only appears when the whole set shares one score, so it does not justify making the common distinct-score descent 2x slower; the spill-slot tie-break handles the tied band without changing the node's primary key form.

Fourth, from the tests: the transform is a bijection.
scoreFromKey(scoreKeyNaive(f)) reproduces f bit for bit for all two million random patterns and every special double, the infinities land at the ends of the key space, and every 52-bit geohash integer survives exactly and orders as an integer, so the geo groundwork holds and M6 rides this codec unchanged.

## The signed-zero trap

The one real hazard the lab surfaced, and the reason the codec is not just the doc's code block copied in.

The transform exactly as written in section 3.1 (flip the sign bit for non-negative, invert all bits for negative) does not collapse -0.0 and +0.0.
It maps +0.0 to 0x8000000000000000 and -0.0 to 0x7fffffffffffffff, one key below, so a member stored at -0.0 sorts strictly before a member stored at +0.0 regardless of the member bytes.
Redis orders the two zeros as equal, so two members carrying them must order by member bytes, and the naive key breaks that: member "b" at -0.0 would sort before member "a" at +0.0, contradicting the by-member order.

The fix is one line: normalize signed zero to +0.0 before the transform (`if f == 0 { f = 0 }`, true for both zeros), so both map to 0x8000000000000000 and the tie falls to the member.
This is what scoreKey does and scoreKeyNaive deliberately does not, and TestSignedZeroCollapses pins both the fix and the hazard so the dual-write slice cannot quietly drop the normalization.

The consequence for ZSCORE: the tree key legitimately loses the sign of a zero, so the tree cannot be the source of truth for what ZSCORE echoes.
The member hash keeps the raw IEEE-754 bits per doc 12 section 2.6, and ZSCORE formats from those, so a member added at -0.0 still reports "-0" while its tree key sorts as +0.0.
The inline band already does exactly this (it stores raw bits in the blob, zset.go lines 52 to 60), and the native band's split (sortable key in the tree, raw bits in the hash) is the same contract carried across the two structures.

## The frozen verdict

Frozen for the tree slice and the dual-write slice:

- Order-key form: the sortable u64 (doc 12 section 3.1 transform, with signed-zero normalization), stored as the 8-byte big-endian score prefix in separators and leaf entries.
  The node compares the 8-byte prefix first and consults the member bytes only on a prefix tie, the section 2.3 spill-slot form; it does not store one composite key and memcmp the whole thing, because that is 2 to 2.7x slower in the common distinct-score descent and only breaks even in the fully tied lex band.
- Compare cost: keying on the sortable u64 costs the same as comparing the raw float64, inside noise on every distinct row, so the sortable form is free on the read path.
  A counted descent lands at roughly 30 to 60 ns for 8-byte members over the 1k-to-1M cardinality band, about 0.7 to 0.85 ns per separator comparison, inside the section 2.5 grounding of a 40 to 80 ns counted descent at 10k.
- Transform cost: encode 0.54 ns/op, decode 0.35 ns/op, paid once per insert (encode) and only on a range-bound recovery or cold-chunk directory key (decode), never on ZSCORE.
- Round-trip guarantee: scoreFromKey inverts the transform bit-exactly for every non-NaN float64 (special doubles plus two million random patterns), the infinities sort to the ends, and NaN never reaches the encoder because the parser rejects it at the door.
- Signed zero: normalize to +0.0 before the transform so -0.0 and +0.0 collapse to one key; ZSCORE round-trips the sign from the raw bits kept in the member hash, not from the tree key.
- Geo: 52-bit geohash integers stored as float64 survive the codec exactly and order as integers, so the section 7 geo range scans key on the same prefix with no special case.

What the dual-write slice must encode as tests:

- Round-trip of the raw bits through the member hash for -0.0, +0.0, both infinities, the float64 extremes, and a random sweep, checked bit-exact (ZSCORE formats "-0" for a stored -0.0).
- Total-order agreement: for the special and random score pairs, the tree key order equals the (score ascending, -0.0 == +0.0, ties by member) reference order.
- The signed-zero collapse specifically: two members at -0.0 and +0.0 order by member bytes, not by the sign of the zero, and the tree key does not place -0.0 below +0.0.
- Infinity placement: -inf sorts below and +inf above every finite score.
- NaN rejection at the parse door, before either structure is touched.
- Geo exactness and order: 52-bit geohash integers round-trip exactly and their key order matches their integer order.

## Darwin caveat

These numbers are on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The absolute ns wander run to run because the box is loaded and each run rebuilds its trees under the Go allocator, but the verdict does not rest on the absolute ns.
It rests on the within-row A/B, which is stable: rawFloat and u64 stay inside a couple of percent on every distinct row, and byteComposite stays 2 to 2.7x behind them there, on every run.
The transform is a handful of integer ops with no memory traffic, so its sub-nanosecond cost is platform-independent, and the round-trip, total-order, and geo properties are exact bit facts that do not depend on the box at all.
The Linux confirmation of the absolute descent ns rides the M2 tree-node lab's gate run on GamingPC, which sweeps the same node layouts this codec keys into.
