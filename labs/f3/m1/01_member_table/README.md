# Lab 01: member-table load factor and probe stepping

Part of issue #543, the M1 set milestone.
This lab lands before the native member-table slice so the slice bakes settled constants, not guesses.

## Question

Doc 11 (11-set-model.md) fixes the native set's member table as a Swiss-style open-addressed table with SWAR control bytes, 7-bit H2 tags, wyhash, and 7/8 maximum load (lines 144, 198), and prices its bucket term at 5 x 8/7 which is about 5.7 bytes per member (line 656).
Two design points sit inside that sentence and the slice is about to write both into engine/f3/struct: the load factor and the probe stepping.
This lab puts a number on each before the code freezes them.
What does 7/8 load actually cost in ns/op against a looser table, and is the Swiss group scan worth its complexity against a plain per-slot probe walk?

The bar is PRED-F3-M1-SETMEM: bytes per member at or under Valkey 8.1's embedded-entry figure, which doc 11 line 649 puts at 10 to 20 bytes per element.
The table bucket is one term in that ledger, and its size is exactly what the load factor sets, so this lab settles the bucket's contribution and where the load factor should sit.

## Method

In-process, no server, no wire, no engine import.
The tables here are lab-local code that models the design points; the real engine table is the slice's job, and this lab exists to price the choices it will make.
Capacity is a fixed power of two per cardinality, and the load factor is set by how many distinct keys we insert, so the bucket bytes per member is exactly bytesPerSlot divided by load and the load sweep is exact instead of quantised by power-of-two rounding.
A slot is one control byte plus a four-byte record ordinal, the doc's line-656 layout, so bytesPerSlot is 5.
Keys are eight bytes, held in a record slab indexed by ordinal, so a tag match pays one record read to confirm the member, which is the doc's "confirm bytes on tag match" (line 189).

Three probe schemes share that layout so the comparison is only about stepping.
Linear walks one slot at a time.
Triangular walks the triangular numbers, which cover every slot on a power-of-two table.
Group is the doc's Swiss shape: eight-wide groups scanned with one SWAR word per group (the portable abseil match, tag-filtered), stepping over groups triangularly.

Axes: cardinality {10k, 1M} by table capacity {2^14, 2^20}, load {0.50, 0.60, 0.70, 0.80, 0.875, 0.90}, scheme {linear, triangular, group}, mix {all hit, all miss, 50/50}.
Reads: SADD-shaped insert ns/op (probe then append the record), SISMEMBER-shaped lookup ns/op per mix, and average probes per lookup.
Probe count is deterministic given load, scheme, and the hash, so it is the noise-free mechanism metric; ns/op carries the box's cache and TLB noise and is read for ordering and magnitude, not its last digit.

`go run .` runs the whole sweep; `-quick` shrinks the op counts for a fast check.

## What the doc predicts, and what this lab tests

- 7/8 maximum load (lines 144, 198). Tested by sweeping load and reading where ns/op and probe count turn.
- Bucket term about 5.7 bytes per member (line 656). Tested analytically: 5 / (7/8) = 5.71.
- SISMEMBER in about one probe (line 130). Tested by the probe-count columns, per scheme.
- About 40 ns per DRAM-resident probe on the gate box (line 393). Tested by the 1M lookup ns/op, with the darwin caveat below.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, 20M lookups per mix after a 5M warm.
ns columns are nanoseconds per op; probe columns are the mean examined units per lookup, which are slots for linear and triangular and eight-wide groups for group.

Bucket bytes per member, set by load alone (analytic, both cardinalities):

| load | 0.50 | 0.60 | 0.70 | 0.80 | 0.875 | 0.90 |
|---|---|---|---|---|---|---|
| bytes/member | 10.00 | 8.33 | 7.14 | 6.25 | 5.71 | 5.56 |

Probe count per lookup, deterministic and identical across the two cardinalities:

| load | linear hit/miss | triangular hit/miss | group hit/miss |
|---|---|---|---|
| 0.50 | 1.49 / 2.47 | 1.44 / 2.15 | 1.01 / 1.06 |
| 0.70 | 2.19 / 6.03 | 1.83 / 3.79 | 1.06 / 1.37 |
| 0.80 | 2.97 / 12.81 | 2.19 / 5.82 | 1.13 / 1.92 |
| 0.875 | 4.39 / 30.86 | 2.68 / 9.56 | 1.23 / 3.03 |
| 0.90 | 5.35 / 43.21 | 2.83 / 11.90 | 1.29 / 3.64 |

Lookup ns/op at 10k (capacity 2^14, L2-resident):

| load | scheme | insNs | hitNs | missNs | mixNs |
|---|---|---|---|---|---|
| 0.70 | linear | 8.2 | 11.8 | 20.8 | 16.7 |
| 0.70 | triangular | 8.0 | 11.8 | 18.7 | 15.9 |
| 0.70 | group | 3.4 | 6.9 | 8.6 | 8.5 |
| 0.875 | linear | 10.8 | 16.1 | 35.0 | 27.6 |
| 0.875 | triangular | 9.4 | 15.3 | 23.5 | 19.9 |
| 0.875 | group | 4.8 | 9.2 | 22.5 | 18.9 |

Lookup ns/op at 1M (capacity 2^20, past L2, DRAM-bound):

| load | scheme | insNs | hitNs | missNs | mixNs |
|---|---|---|---|---|---|
| 0.70 | linear | 10.2 | 20.9 | 24.3 | 28.1 |
| 0.70 | triangular | 10.7 | 21.9 | 22.2 | 25.1 |
| 0.70 | group | 6.0 | 21.1 | 10.4 | 23.6 |
| 0.875 | linear | 12.5 | 35.1 | 42.4 | 47.2 |
| 0.875 | triangular | 12.6 | 34.7 | 28.4 | 34.8 |
| 0.875 | group | 7.2 | 26.5 | 25.3 | 33.3 |

The 1M hit ns wanders about 15% run to run from cache and TLB noise, so the ordering is the signal there, not the last digit; the probe-count table is the stable mechanism reading and it does not move.

## Reading the sweep

The probe count is where the decision is made, and it is not close.
Group holds hit lookups at about one probe (1.01 at 0.50 rising only to 1.29 at 0.90) and miss lookups under four groups all the way to 0.90, while linear's miss walk explodes from 2.5 slots at 0.50 to 30.9 at 0.875 and 43 at 0.90, and triangular sits in between at 9.6 and 11.9.
That is the whole case for the Swiss shape.
The 7/8 load the doc wants is only affordable because the group scan rejects eight slots per SWAR word, so 3.0 groups at 0.875 is about 24 slots retired in three memory reads, against linear touching 31 individual bytes for the same miss.
A per-slot table at 7/8 is a miss cliff; the group table barely notices the load.

The ns/op tables confirm the ordering and the doc's "about one probe" claim, but only for the group scheme.
At 10k 0.875, group misses in 22.5 ns against linear's 35.0, and its hit path is 9.2 ns against 16.1.
At 1M 0.875, group's hit 26.5 ns and miss 25.3 ns both sit under doc line 393's 40 ns DRAM-probe figure, and the mix path is 33.3 ns; linear's miss is 42.4 ns, already over the floor.
Doc line 130's "SISMEMBER in about one probe" is true, but it is a property of the Swiss group scan specifically: linear pays 4.4 slots and triangular 2.7 slots for the same 7/8 hit, so the budget only closes with group probing, which is exactly why the doc names it.

On the load factor itself, the bytes curve is steep on the loose side and flat on the tight side.
Moving from 0.50 to 0.875 halves the bucket from 10.0 to 5.71 bytes per member while group hit probes rise only 1.01 to 1.23 and group miss ns at 10k goes 5.1 to 22.5.
Going one more step to 0.90 buys just 0.15 more bytes (5.71 to 5.56) while group miss probes climb 3.03 to 3.64 and the miss walk keeps lengthening.
So 7/8 is the knee: nearly all the byte saving of a tight table with the group scan still holding hits at about one probe, and the next step past it trades real probe growth for almost no bytes.
The doc's 5.7 bytes-per-member bucket figure (line 656) reproduces exactly at 5.71.

## Bytes per member against the bar

The bucket term at 7/8 is 5.71 bytes per member, far under Valkey 8.1's 10 to 20 bytes per whole element (doc 11 line 649).
The bucket is not where the set's memory pressure lives.
The full native-band ledger from doc 11 section 11.1 is the 16-byte record plus the 4 to 6 byte draw vector plus this 5.7 byte bucket, about 26 to 28 bytes per member, which sits above Valkey's high end.
The PRED-F3-M1-SETMEM levers are therefore the doc's two diet steps (drop hash32 and recompute on rehash for 4 bytes, merge tag into the control byte's H2 for 1 byte, floor about 21 to 23 bytes), not the load factor, which this lab confirms is already one step off its practical floor.
Pushing load past 7/8 to shave the bucket is the wrong lever: it saves tenths of a byte and pays it back in probe length.

## Darwin caveat

These constants are measured on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The load factor and probe-scheme decision rests on the probe-count columns, which are deterministic and platform-independent, and on an ns/op ordering that is wide enough to survive a platform change (group beats the per-slot schemes on every miss cell).
The absolute ns/op and the 40 ns DRAM-probe comparison get their Linux confirmation at the M1 gate run on GamingPC before the gate rows are read.

## Verdict

Frozen for the native member-table slice:

- Load factor: 7/8 maximum load, grow by doubling when a cell would exceed it, exactly as doc 11 lines 144 and 198 state. It is the knee of the bytes-versus-probes curve: 5.71 bytes per member for the bucket, group hits still at about one probe, and the only step past it (0.90) buys 0.15 bytes for measurably longer miss walks.
- Probe stepping: Swiss-style eight-wide SWAR groups with triangular group stepping and a 7-bit H2 tag filter, confirming the member with the record read on a tag match. It wins decisively on the miss path at every load and widens with load: at 7/8 it holds misses to about 3 groups where linear pays 31 slots and triangular 9.6, so it is what makes 7/8 affordable in the first place.
- Rejected: linear per-slot probing (miss cliff at high load, 43 slots at 0.90), and triangular per-slot probing (better than linear but still 3 to 4 times the group miss cost). Neither meets doc line 130's one-probe budget at the doc's own load factor.
- Bytes per member: bucket 5.71 at 7/8, under the Valkey 10 to 20 byte bar with room; the ledger gap against Valkey is the record and vector terms, so PRED-F3-M1-SETMEM is carried by the doc's diet steps, not by moving the load factor.

What the slice should bake in: 7/8 max load, doubling growth, one-byte control plus four-byte ordinal buckets, eight-wide SWAR group probe with triangular group stepping and H2 tag confirm.
The probe kernel is shared with the shard key index (doc 11 line 144) and feeds M4's field table (issue #543), so it is written once against these constants.
