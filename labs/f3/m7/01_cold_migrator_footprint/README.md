# Lab 01: cold-migrator resident footprint, small keys past a cap vs an all-RAM rival

Part of issue #549, the M7 LTM milestone, lab 01, the whole-record cold migrator (doc 06 sections 2.4 and 8.1). This is the lab the migrator slice depends on: it settles that demoting whole int and embedded records to the cold region bounds aki's resident footprint below a rival that keeps every key in RAM, per the labs-per-perf-change rule and the M7 memory bar (aki holds less RAM than the rivals, ideally half).

## Question

The residency hand (resid.go) bounds the separated band by spilling a cold value's run to the value log while its record stays resident. It cannot touch the int and embedded bands, whose value bytes live inside the record with nowhere to spill. A workload of many small keys therefore pins the whole record set in the arena, and once admission parks the fill at the cap the store cannot take a new key. The whole-record migrator (migrate.go) is the missing valve: it demotes the coldest whole records to the shard's cold region on disk, so the arena fill tracks the cap while the index still names every key.

Redis and Valkey have no such tier: every key sits in RAM with its dict entry, its object header, and its SDS strings for as long as it is live. The claim the slice bakes in is that past the cap aki's RAM tracks the index plus the cap, not the dataset, so a small-key set far past the cap uses far less memory and floors at the index share. It also makes the dataset admissible at all: without the migrator the arena alone ErrFulls once the fill reaches the cap. The questions: how far under the rival does a past-cap set land, where does the ratio floor, and does a larger value deepen the win.

## Method

In-process, no server, no wire, no engine import, the lab-local model the other f3 labs use. The record geometry (16-byte header, 8-byte alignment, the int cell and embedded value bands) and the cold-frame header (12 bytes) match the store, so aki's resident figures are the store's, not a stand-in. `akiRAM` charges the index one entry per key resident or cold, plus the bounded arena the resident records fill; the cold frames live on disk and cost no RAM. `rivalRAM` charges every key whole in RAM.

Every rounding is against aki's win. The index is charged a full 8-byte slot at a half-full load factor (16 B/key), which overstates it since real load runs denser. The rival's per-key overhead is a bare dict entry, object header, and one SDS header (48 B/key) without the allocator size-class rounding a real redis pays, which understates it. So aki's win here is a floor.

`go run .` runs the whole sweep. `-quick` shrinks the sweep-A counts for the shared runner. `TestFitsUnderCapIsAdmissible`, `TestPastCapBoundsResident`, `TestConvergesToIndexShare`, `TestMigratorMakesPastCapAdmissible`, and `TestLargerValueDeepensWin` are what CI drives.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-15, one process. Record header 16 B, int cell 8 B, cold frame header 12 B, index 16 B/key, rival 48 B/key overhead.

Sweep A, a 64 MiB cap and an 8-byte embedded value, rising key count (`noMig` is where the arena alone would ErrFull):

| keys | resident | cold | akiRAM | akiDisk | rivalRAM | aki/rival | noMig |
|---|---|---|---|---|---|---|---|
| 100000 | 100000 | 0 | 5.34MiB | 0B | 6.87MiB | 0.7778 | ok |
| 1000000 | 1000000 | 0 | 53.41MiB | 0B | 68.66MiB | 0.7778 | ok |
| 4000000 | 1677721 | 2322279 | 125.04MiB | 79.73MiB | 274.66MiB | 0.4552 | ErrFull |
| 16000000 | 1677721 | 14322279 | 308.14MiB | 491.72MiB | 1.07GiB | 0.2805 | ErrFull |
| 64000000 | 1677721 | 62322279 | 1.02GiB | 2.09GiB | 4.29GiB | 0.2368 | ErrFull |

While the whole set fits the cap the two track (aki holds it all in the arena too, and still wins on the leaner index). Once the set crosses the cap the migrator moves the overflow to disk: the resident count freezes at what the cap holds and aki's RAM tracks the index plus the cap while the rival keeps growing. At 64M keys aki holds 1.02 GiB against the rival's 4.29 GiB, 0.24x, and holds a dataset the arena alone cannot admit at all.

Sweep B, 16M keys and a 64 MiB cap, rising value size:

| valueLen | resident | akiRAM | rivalRAM | aki/rival | floor |
|---|---|---|---|---|---|
| 8 | 1677721 | 308.14MiB | 1.07GiB | 0.2805 | 0.2222 |
| 32 | 1048576 | 308.14MiB | 1.43GiB | 0.2104 | 0.1667 |
| 128 | 419430 | 308.14MiB | 2.86GiB | 0.1052 | 0.0833 |
| 512 | 123361 | 308.14MiB | 8.58GiB | 0.0351 | 0.0278 |

A larger value costs the rival more RAM per key and pushes more bytes to disk on demotion, so the win deepens: at a 512-byte value aki holds 0.035x the rival's RAM, 28x less, because those value bytes live on disk for aki and in RAM for the rival.

Sweep C, 16M keys and an 8-byte value, rising cap:

| cap | resident | coldFrac | akiRAM | aki/rival |
|---|---|---|---|---|
| 16.00MiB | 419430 | 97.4% | 260.14MiB | 0.2368 |
| 64.00MiB | 1677721 | 89.5% | 308.14MiB | 0.2805 |
| 256.00MiB | 6710886 | 58.1% | 500.14MiB | 0.4552 |
| 1.00GiB | 16000000 | 0.0% | 854.49MiB | 0.7778 |

The cap is the RAM knob: a larger working set is resident, so aki's RAM rises toward the rival, but every byte of value the cap cannot hold is a byte the rival keeps and aki does not. At a 16 MiB cap 97.4% of the set is cold and aki holds under a quarter of the rival.

Floor, from the per-key model: at a 16-byte key and 8-byte value the bounded arena washes out as the set grows and aki converges to 0.2222 of the rival's RAM, 4.5x less.

## Verdict

The whole-record migrator bounds the resident footprint of a small-key workload the residency hand cannot. Past the cap aki's RAM tracks the index plus the cap, not the dataset: 0.24x the rival at 64M small keys, deepening to 0.035x as the value grows, flooring at the index share (0.2222). And it makes past-cap datasets admissible at all, where the arena alone ErrFulls. The tier earns its place on the memory bar, so the slice lands the migrator.
