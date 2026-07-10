# M0 run 3: honest write cardinality and the LTM residency verdict, 2026-07-10

Third box run for the M0 perf round, at aki `19c08a4` (adds #573 hot-set LTM residency) and aki-bench `b171fde` (adds #42 per-connection key streams).
This is the first run where write cells exercise their named cardinality: aki-bench#42 fixed all connections issuing the identical key stream, so every earlier SET/INCR row (gate run and m0-rerun, all servers alike) really measured ~40-90k effective keys, not 1M.
Protocol as m0-rerun: warm 3s, 3 timed windows, FLUSHALL between reps on all servers, servers cpus 0-7, generator 8-15, redis 8.8.0 / valkey 9.1.0 io-threads 4, ratios are min of per-harness ratios.
Raw per-rep data in m0-run3/, provenance in m0-run3/binaries.sha256 and env.txt.

## Headline table

| cell | m0-rerun | run3 | note |
|---|---|---|---|
| SET 64B 1M P16 | 0.96x | 1.13x | rivals lose more than aki at honest cardinality |
| INCR 1M P16 | 1.10x | 1.27x | |
| SET 1KiB 1M P16 | 1.06x | 1.17x | |
| GET 64B 1M P16 | 1.24x | 1.12x | see the regression note below |
| GET 1KiB 1M P16 | 1.00x | 1.58x | rerun's redis ab number was cache-flattered by the stream bug |

distinct_keys_est self-check: every uniform window reported 1,000,000 distinct keys, +0.0% vs the sampling expectation; zero coverage warnings.
The GET cells were expected to be controls and their rival rb numbers are bit-identical to m0-rerun, which confirms rb as the control; the rival ab GET numbers moved because the old identical-stream bug made ab GETs cache-resident for everyone (redis ab GET 1KiB fell from 4.27M to 2.10M, exactly its rb number).

## Regression found: aki rb GETs down 8-12% vs m0-rerun

aki rb GET 64B 5.71M to 5.00M and GET 1KiB 3.63M to 3.33M on an identical harness and box, while rival rb numbers did not move at all.
The only aki change in the window is #573, and the suspect is the residency mark: GET now sets the flagVisited bit in the record header, turning a read-only path into one that dirties a cache line per hit.
Open item for the next slice: make the mark check-before-store (only write the bit when it is clear) or sample it; the zipf LTM hit ratio argues the policy survives either.

## LTM strings (2M x 1032B, aki 512MiB resident cap, rivals maxmemory 512mb allkeys-lfu)

| cell | aki value-bearing | best rival value-bearing | ratio | rival hit% |
|---|---|---|---|---|
| GET uniform | 787k (100% hits) | 663k | 1.19x | 20.1 |
| GET zipf s=0.99 | 1.82M (100% hits) | 1.30M | 1.40x | 39.7 |
| SET uniform | 192k | 1.04M | 0.18x | 100 |

- The residency slice works: zipf resident-hit ratio 0.934 (uniform 0.694), and aki answers 100% of GETs where capped rivals under allkeys-lfu answer 20-40% and serve the rest as nils.
- The open LTM weakness is the spill write path: SET on a full store runs at 0.18x with p99 14ms, about 5x slower than rival evict-to-make-room. Next LTM slice.
- redis-benchmark was not run on LTM cells by design; it counts rival nils as ops, the accounting bug aki-bench#41 fixed.

## Memory bar (RSS under 2x rivals)

Sampled VmRSS at 1s during the LTM cells: aki 962/973/1253 MiB (get uniform / get zipf / set) vs best rival 595/691/534 MiB.
aki-to-rival ratios 1.39x / 1.41x / 1.49x: the bar passes on all three LTM cells, on a real kernel with releasePages active.
Against the cap itself aki reads sit at ~1.9x (Go heap plus the 2M-key index on top of the arena fill bound of cap + cap/8 + 512KiB); the SET cell peaks at 2.45x cap, close to the darwin no-release high-water mark, which is the spill-path slice's problem to shrink.
Write-cell bytes/key now reads 113.4 at SET 64B (was 8.2 in m0-rerun); that is not growth, it is 1M keys actually existing.

## State of the round after run 3

Standing: all headliners between 1.12x and 1.58x, aki over valkey everywhere, memory bar green on LTM, no crashes, no generator-bound windows.
Remaining to 2.0x on uniform point cells: the reactor pull-forward (notes/Spec 2064/f3/m10-pullforward.md), plus the two items this run opened, the flagVisited read-path regression and the LTM spill write path.
