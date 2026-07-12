# Lab 01: field-table load factor and confirm cost by field-name length

Part of issue #546, the M4 hash milestone, slice 2.
M1's lab 01 froze the shared table kernel; this lab checks that the freeze still holds once the confirm is the hash field table's variable-length one, and it prices the new axis that freeze did not cover.

## Question

M1's lab 01 (labs/f3/m1/01_member_table) settled the shared `struct.Table` kernel: 7/8 maximum load, doubling growth, one-byte control plus four-byte ordinal buckets, and an eight-wide SWAR group probe with triangular group stepping and a 7-bit H2 tag.
It settled that on set members that are fixed eight-byte words, so the confirm on a tag hit was one eight-byte compare.
M4's field table (engine/f3/hash/field.go) reuses that exact kernel through `Find`, `Insert`, and `Delete`, but its confirm (`ftable.Match`) is a variable-length `bytes.Equal` over field-name bytes in a slab, and hash field names are variable-length strings, not fixed eight-byte words.

So the question: does M1's 7/8 plus Swiss-group verdict still hold for the hash field-table workload once the confirm is a variable-length `bytes.Equal` across realistic field-name lengths?
The milestone states this lab inherits M1's verdict unless refuted.
The job is therefore to price the one thing M1 did not measure, the confirm cost by field-name length, and either confirm the inheritance or lay out the refutation with the numbers.

## Method

In-process, no server, no wire.
The tables here are lab-local code that models the design points; the real engine table is field.go plus struct.Table, and this lab exists to check the choices those already made against a workload M1's lab did not run.
A slot is one control byte plus a four-byte record ordinal, so bytesPerSlot is 5, the same layout M1 priced and the same layout struct.Table carries, which means the bucket bytes per field is exactly bytesPerSlot divided by load and reproduces M1's curve by construction.
Field names live in a record slab addressed by ordinal through an offset and a length, exactly like `fentry`, so a tag match pays one variable-length `bytes.Equal` to confirm the field, which is the field table's real `Match`.

Two fidelity choices separate this lab from M1's.
The hash is `store.Hash` (wyhash), the engine's own hasher that field.go calls, not a lab-local splitmix, so a name's tag and home group are the ones the field table actually computes and the confirm reads the real slab bytes.
The confirm is a `bytes.Equal` over the stored name, not a register compare of two uint64 words, so its cost is the true field-table confirm and it grows with the field-name length.
Both choices raise the absolute ns against M1's register-resident model, because a lookup here hashes real bytes and reads a name out of a slab that spills to DRAM at 1M fields; that is the point, since the field table pays exactly those reads and M1's uint64 members hid them.

Three probe schemes share the layout so the comparison is only about stepping.
Linear walks one slot at a time, triangular walks the triangular numbers, and group is the doc's Swiss shape: eight-wide groups scanned with one SWAR word per group, stepping over groups triangularly, with the H2 tag filtering candidates before the confirm runs.

Axes: cardinality {10k, 1M} by table capacity {2^14, 2^20}, field-name length class {short 8B, medium 24B, long 64B}, load {0.50, 0.60, 0.70, 0.80, 0.875, 0.90}, scheme {linear, triangular, group}, mix {all hit, all miss, 50/50}.
Reads: HSET-shaped insert ns/op (probe then append the record), HGET/HEXISTS-shaped lookup ns/op per mix, mean probes per lookup, and bucket bytes per field.
Probe count is deterministic given load, scheme, and the hashes, so it is the noise-free mechanism metric; ns/op carries the box's cache and TLB noise and is read for ordering and magnitude, not its last digit.

`go run .` runs the whole sweep; `-quick` shrinks the op counts for a fast check, which is what the test drives.

## What M1 froze, and what this lab tests

- 7/8 maximum load, doubling growth, one-byte control plus four-byte ordinal buckets, SWAR group probe with triangular stepping and an H2 tag confirm (M1 lab 01 verdict, now in struct.Table). Tested by re-running the load and scheme sweep with the field table's real confirm and hasher.
- Bucket term about 5.7 bytes per field (5 / (7/8) = 5.71). Tested analytically, identical to M1 because the slot layout is identical.
- The new axis M1 did not have: the confirm is a variable-length `bytes.Equal`, not an 8-byte compare. Tested by sweeping the field-name length class and reading the hit path, where the confirm runs, against the miss path, where it does not.

## Results

Apple M4 (darwin/arm64), go 1.26.5, 2026-07-12, one process, 5M lookups per mix after a 1.25M warm.
ns columns are nanoseconds per op; probe columns are the mean examined units per lookup, which are slots for linear and triangular and eight-wide groups for group.

Bucket bytes per field, set by load alone (analytic, both cardinalities, every length class, identical to M1):

| load | 0.50 | 0.60 | 0.70 | 0.80 | 0.875 | 0.90 |
|---|---|---|---|---|---|---|
| bytes/field | 10.00 | 8.33 | 7.14 | 6.25 | 5.71 | 5.56 |

Probe count per lookup, deterministic and identical across the two cardinalities and the three length classes to within sampling noise (values below are the 1M short cell):

| load | linear hit/miss | triangular hit/miss | group hit/miss |
|---|---|---|---|
| 0.50 | 1.50 / 2.51 | 1.44 / 2.17 | 1.01 / 1.06 |
| 0.70 | 2.18 / 6.06 | 1.85 / 3.82 | 1.06 / 1.38 |
| 0.80 | 2.99 / 12.97 | 2.19 / 5.87 | 1.13 / 1.90 |
| 0.875 | 4.58 / 33.48 | 2.66 / 9.62 | 1.23 / 2.94 |
| 0.90 | 5.58 / 51.44 | 2.89 / 12.15 | 1.29 / 3.63 |

This table is M1's probe-count table again within noise, which is the first half of the answer: the probe mechanism does not know or care that the confirm changed, because the probe walks control bytes and ordinals and never touches a field name until the tag matches.

The new signal, confirm cost by field-name length, read on the group scheme where the probe is held near one and the ns is dominated by the confirm.
Group hit and miss ns/op by length class:

10k (capacity 2^14, table and slab L2-resident):

| load | short hit | med hit | long hit | short miss | med miss | long miss |
|---|---|---|---|---|---|---|
| 0.70 | 13.2 | 18.5 | 22.9 | 12.0 | 14.8 | 21.1 |
| 0.875 | 17.5 | 20.2 | 27.1 | 25.5 | 30.6 | 39.6 |
| 0.90 | 16.1 | 20.8 | 25.9 | 28.4 | 32.6 | 40.3 |

1M (capacity 2^20, slab spills past L2, DRAM-bound):

| load | short hit | med hit | long hit | short miss | med miss | long miss |
|---|---|---|---|---|---|---|
| 0.70 | 64.9 | 102.3 | 120.4 | 13.8 | 17.2 | 23.2 |
| 0.875 | 72.4 | 111.0 | 133.1 | 32.7 | 37.7 | 44.1 |
| 0.90 | 72.9 | 107.1 | 135.2 | 38.0 | 40.2 | 51.4 |

The scheme comparison on the miss path, which is where M1's verdict was earned, at 1M and the two tight loads (short / long):

| load | linear miss | triangular miss | group miss |
|---|---|---|---|
| 0.875 | 51.4 / 74.5 | 37.5 / 47.0 | 32.7 / 44.1 |
| 0.90 | 65.5 / 93.2 | 39.6 / 51.3 | 38.0 / 51.4 |

The 1M hit ns wanders about 10 to 15 percent run to run from cache and TLB noise, so the ordering and the length trend are the signal there, not the last digit; the probe-count table is the stable mechanism reading and it does not move.

## Reading the sweep

The load-factor knee reproduces M1 exactly, and it should, because the knee is a bytes-versus-probes tradeoff and the confirm sits outside both terms.
The bytes curve is identical (5.71 at 7/8, 5.56 at 0.90, a 0.15 byte gain for the last step).
The probe counts are M1's within noise: group holds hits at about one probe (1.01 at 0.50 to 1.29 at 0.90) and misses under four groups all the way to 0.90, while linear's miss walk explodes from 2.5 slots at 0.50 to 33.5 at 0.875 and 51.4 at 0.90.
Going from 0.875 to 0.90 still buys 0.15 bytes for a measurably longer miss walk (group miss 2.94 to 3.63 groups, linear miss 33.5 to 51.4 slots), so 7/8 is still the knee.
Nothing about the variable-length confirm touches this, because the confirm runs once per hit regardless of load and never on the miss path that sets the knee.

The new signal is confirm cost by field-name length, and it splits cleanly by cardinality.
At 10k the whole table and its name slab are L2-resident, so a hit's confirm reads the stored name out of cache and the hit path lifts only a few ns from short to long (group hit 17.5 to 27.1 at 0.875), most of which is hashing the longer probe key rather than the compare.
At 1M the slab spills to DRAM (a 1M-field slab of 64-byte names is 64MB), so a hit's confirm pays a DRAM read of the stored name, and the hit path lifts sharply as names grow: group hit at 0.875 goes 72.4ns short, 111.0ns medium, 133.1ns long.

That long-name lift is the lab's own contribution, so it is worth isolating from the shared cost of hashing a longer probe key.
The miss path pays that shared hashing cost but never the confirm, so the miss lift by length is the hashing term alone: at 1M 0.875 the miss goes 32.7 short to 44.1 long, a 11.4ns lift.
Subtract that from the hit lift (133.1 minus 72.4 is 60.7ns) and the confirm itself accounts for about 49ns of the long-name hit at 1M, which is the DRAM read of the 64-byte name plus the longer `bytes.Equal`.
The short 8-byte case is exactly M1's member size, so the honest headline is: moving the confirm from M1's 8-byte word to a 64-byte field name adds about 49ns to a DRAM-resident hit and almost nothing to a cache-resident one, and adds nothing measurable to any miss.

One consequence is worth pinning because slice 5 will lean on it.
At 1M with long names the hit path is confirm-dominated and the probe scheme barely shows: group hit 133.1ns against linear hit 133.8ns at 0.875, even though linear walks 4.58 probes to group's 1.23.
The probes are cache-resident control words and cheap next to the one DRAM read of the field bytes, so at scale HGET on a hit is a memory-bandwidth problem on the field slab, not a probe problem.
This is the same wall doc 10 section 8.2 names for HGETALL, and this hit-path baseline is the row slice 5 inherits when it profiles the m=1000 HGETALL memory bandwidth explicitly.

The scheme decision, though, is made on the miss path, and there the variable-length confirm changes nothing because it does not run.
Group wins the miss path decisively at every load and every length: at 1M 0.90 group misses in 38 to 51ns against linear's 65 to 93ns, and the miss probe count (3.63 groups against 51.4 slots at 0.90) is the M1 mechanism reading unchanged.
So M1's argument for the group scheme carries over intact: it was always a miss-path argument, and the confirm lives on the hit path.

## Bytes per field against the bar

The bucket term at 7/8 is 5.71 bytes per field, identical to M1's member-table bucket because the slot layout is identical (one control byte plus a four-byte ordinal).
That is not where the hash's memory pressure lives; the field record (`fentry` plus the field and value bytes in the slab) is, and doc 10 section 10 prices that ledger.
This lab confirms the bucket contribution to the F14 memory column is the same 5.71 bytes the set pays, and that pushing load past 7/8 to shave it is the wrong lever here for the same reason it was wrong for the set: it saves tenths of a byte and pays it back in miss-walk length.

## Darwin caveat

These numbers are measured on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The load-factor and probe-scheme decision rests on the probe-count columns, which are deterministic and platform-independent, and on an ns/op ordering wide enough to survive a platform change (group beats the per-slot schemes on every miss cell).
The confirm-cost-by-length finding rests on the 10k-versus-1M split, which is an argument about cache residency and DRAM bandwidth that holds on any box with a cache hierarchy, though the absolute ns of the DRAM read will differ on the gate box.
The absolute ns/op and the DRAM-read figure get their Linux confirmation at the M4 gate run on GamingPC before the gate rows are read.

## Verdict

Frozen for the M4 field-table slice, and the field table bakes NO NEW constant, because the kernel is shared and M1 already froze it:

- Load factor: 7/8 maximum load with doubling growth, inherited from M1 unchanged. The bytes curve and the probe counts reproduce M1's within noise, and the knee is confirm-independent by construction because the confirm runs once per hit and never on the miss path that sets the knee. 7/8 is still the knee: the step to 0.90 buys 0.15 bytes for a longer miss walk.
- Probe stepping: eight-wide SWAR group probe with triangular group stepping and a 7-bit H2 tag confirm, inherited from M1 unchanged. It wins the miss path decisively at every load and every length class (group miss 3.63 groups against linear 51.4 slots at 0.90), and the variable-length confirm does not touch that win because the confirm does not run on a miss.
- Buckets: one-byte control plus four-byte ordinal, 5.71 bytes per field at 7/8, identical to M1's member-table bucket.

The inheritance holds. Nothing in the variable-length confirm refutes any M1 constant.

The lab's own contribution, the confirm cost by field-name length, which M1's fixed 8-byte member could not measure:

- On a hit the field table pays a variable-length `bytes.Equal` over the stored name plus the read that fetches it. Field-name length barely moves this when the slab is cache-resident (group hit 17.5 to 27.1ns short to long at 10k, 0.875), and moves it sharply when the slab spills to DRAM (group hit 72.4 to 133.1ns short to long at 1M, 0.875).
- Isolated from the shared cost of hashing a longer probe key (which the miss path also pays), the long 64-byte confirm adds about 49ns over the short 8-byte confirm on a DRAM-resident hit, and about nothing on a cache-resident one.
- On a miss the confirm does not run, so field-name length adds only the longer-probe-key hashing term (about 11ns short to long at 1M), never a confirm.
- At scale a hit is confirm-dominated and probe-scheme-insensitive (group and linear hit within 1ns at 1M long, 0.875, despite a 4x probe-count gap), which makes HGET on a large hash a field-slab memory-bandwidth problem. This hit-path baseline is the row slice 5 inherits for the HGETALL m=1000 memory-bandwidth profile.

What the slice keeps from M1, written once against these constants and shared: 7/8 max load, doubling growth, one-byte control plus four-byte ordinal buckets, eight-wide SWAR group probe with triangular group stepping and H2 tag confirm.
What is new here is only the confirm shape, and the numbers say it costs on the hit path in proportion to the field-name length and the slab's cache residency, and costs nothing on the miss path that decides the probe scheme.
