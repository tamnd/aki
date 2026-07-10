# Lab: used_memory vs allocator-held bytes vs redis

Spec 2064/f3, M0 gate follow-up, lab 12, issue #542.

## The question

The native-Linux gate follow-up caught INFO used_memory undercounting on bursty SET churn: 52MB reported where redis 8.8 said 220MB under identical load, so every SET-cell memory column in the gate table was unreliable.
used_memory was defined as index tables plus the arena's live charge, which misses the dead-but-uncompacted bytes republish churn strands behind live neighbors: real resident pages the compactor has not drained yet.
redis's used_memory is what its allocator holds for the dataset, dead-space slack included, so the doc 18 apples-to-apples figure has to be the arena analogue: the touched-segment fill (live plus dead plus the reuse slack behind the bump cursors) plus the index tables.
Is the allocator-held definition the one that lines up with redis and with the process's own footprint on the churn workload?

## Method

The workload is lab 10's pass two, the shape that produced the undercount: 1M keys, value size a coin flip between 512B (embedded) and 4KiB (separated) so about half the overwrites change band and republish the record, a pinned eighth written at fill and never again, churn to 2x arena turnover in written bytes.

`go run .` runs it in process over `engine/f3/store` with the emulated worker boundaries (tightness check per 1024-op batch, idle reclaim trigger every 64 batches at the 1MiB floor, the shipped 1/4 victim threshold) and prints both used_memory definitions plus maxrss at fill and after churn.
`go run . -engine redis -addr 127.0.0.1:6390` drives the byte-identical op stream over RESP against a running redis-server and reads INFO used_memory and used_memory_rss at the same two points.

## Results

Apple M4 (4P + 6E), 24GiB, macOS, Go 1.26, quiet box.
redis 8.8.0 (`--save '' --appendonly no`, libc malloc build).
One run per engine; a second store run moved the churned fill under 1 percent.

| figure | at fill | after churn |
|---|---|---|
| aki live-only (old used_memory) | 2359.6MB | 2360.1MB |
| aki allocator-held (new used_memory) | 2359.6MB | 2777.5MB |
| aki maxrss | 2407.0MB | 3200.4MB |
| redis used_memory | 2629.4MB | 2635.6MB |
| redis used_memory_rss | 1193.2MB | 2821.9MB |

The live-only figure does not move under churn at all: the 417MB dead share the compactor is deliberately leaving in place (the 1/4 threshold trades that slack for pause and throughput, lab 10) is invisible to it, and no churn regime can ever show.
The allocator-held figure lands within 5 percent of redis's used_memory on the identical dataset and tracks the footprint growth the kernel sees.
The gate follow-up's 4x gap (52MB vs 220MB) was this same hole before the reclaim slice landed, when the dead share grew without bound; at the shipped threshold the steady-state undercount is about 15 percent of the dataset, still a lie in exactly the direction that flatters aki.
maxrss runs about 15 percent above the account in both engines (Go runtime and scratch on our side, allocator slack on theirs), which is the RSS-next-to-used_memory story doc 18 section 1.5 already requires.

## Verdict (frozen)

used_memory is index bytes plus the arena's touched-segment fill (`MemLedger.UsedMemory = IndexBytes + ArenaAllocBytes`): allocator-held, the figure comparable to redis INFO used_memory per doc 18.
Live-only is still reported (arena_live_bytes) so the dead share stays readable, and vlog bytes stay out (disk, doc 16).
Frozen in engine/f3/store/mem.go; the churn regression lives in engine/f3/store/ledger_test.go (TestUsedMemoryChurnBursty) with the full-walk invariant tests beside it.
