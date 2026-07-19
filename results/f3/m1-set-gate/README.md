# M1 set gate on the tip (dcef1d8c)

GamingPC, 2026-07-19. f3srv gate config `GOGC=20 -shards 8 -rep-cap 512 -net reactor -net-loops 0`, pinned cores 4-17.
Rivals CF16-frozen (redis io6, valkey io4). Ratio is min(aki/redis, aki/valkey), 2x bar.

## Throughput

The set point ops clear 2x on both rivals under the dual-generator protocol.

| row | workload | aki | redis | valkey | vs redis | vs valkey | verdict |
|---|---|---|---|---|---|---|---|
| M1-G1 | SISMEMBER 64B 1M-key | 10.77M | 3.69M | 3.00M | 2.92x | 3.59x | PASS |
| M1-G2 | SADD 1M distinct sets | 9.13M | 3.37M | 2.91M | 2.71x | 3.14x | PASS |

M1-G2 is the write mirror of the passing SISMEMBER read.
Note the harness caveat: aki-bench's native `sadd` workload adds to a single hot set and its single generator caps aki, reading 1.40x (a hot-key + single-generator understatement, same class as measurement lesson 0).
The gate cell is 1M distinct single-member sets, driven by two `redis-benchmark` generators with a custom `SADD s:__rand_int__ m` command so the KEY randomizes across the 8 shards.
Under that protocol the write clears 2x cleanly.

## Memory (M1-G10 tiny-set): structural live-heap wall

The 1M single-member-set cell fails the memory bar and it is a live-heap wall, not GC slack.

Peak VmHWM after the SADD load, sweeping GOGC:

| GOGC | SADD | VmHWM MiB | vs worse rival (redis 89.7) |
|---|---|---|---|
| 20 | 9.14M | 206.2 | 2.30x |
| 10 | 9.13M | 210.8 | 2.35x |
| 5 | 9.13M | 201.3 | 2.24x |
| redis | - | - | 89.7 |
| valkey | - | - | 90.1 |

VmHWM is flat across GOGC 20 to 5 at unchanged throughput, so the peak is almost all live (non-collectable) heap, not GC headroom.
The live footprint per single-member set is a `map[string]*set` entry (~70 B: map bucket slot + key backing) plus a separately heap-allocated 80 B `set` struct plus ~8 B of member data, about 160 B, none of it collectable.
Redis stores the same one-member set in one embedded-dict entry plus a one-element intset, about 90 B total, so it sits at ~90 MiB where aki sits at ~200.

Why GOGC does not help: the string cell wins from GOGC because its data rides an off-heap arena and only the ~50 MiB heap fabric is scanned, so trimming headroom is free.
A tiny-set keyspace is the opposite: the data IS the heap (1M live map entries and structs), GC has nothing to reclaim, and the peak is the live set itself.

Slimming levers and their ceilings, measured or estimated:
- Struct slim, unify ht/part/acct/cold pointers behind side handles, 80 B to ~48 B: saves ~32 MiB, lands ~174 MiB (still ~1.9x).
- Map value-inline, `map[string]*set` to `map[string]set`, removes 1M separate allocations: est ~30-40 MiB, lands ~140 MiB (still ~1.5x).
- Neither, nor both together, reaches parity.

Parity with redis on tiny collections needs arena-embedding the small collection's bytes in the same record the string store uses, keyed in one shared index instead of a per-type `map[string]*coll`.
That is the keyspace-unification slice the set registry already names as future work (`engine/f3/set/reg.go`: "full cross-type unification ... lands with the keyspace slice").
So M1-G10 is a STRUCTURAL miss under the current per-type-map architecture, with a known close path (keyspace unification), tracked as a follow-up engine arc that unblocks the M2/M4/M5 tiny-collection memory rows at the same time.

## Accounting gap found

f3srv `INFO memory` reports `used_memory` 65,824 bytes for 1M loaded sets: collection bytes are not in the memory accounting (only the string arena is).
This is an M9 metric gap (MEMORY USAGE / used_memory should count collections), separate from the gate, noted for M9.
