# M1 set-algebra gate, 10k band

Connect-mode aki-bench run on the GamingPC gate box (2026-07-19).
f3srv gate config (`GOGC=20 GOMAXPROCS=14 -shards 8 -arena-mib 512 -net reactor`, pinned 4-17) vs CF16-frozen rivals (redis io-threads 6, valkey io-threads 4, both `--save '' --appendonly no`, pinned 4-17), aki-bench generators pinned 18-31.
Cell: card 10k sets, P16, 8s window + 3s warm, `-gate 2.0 -cpu-split=false`.
The 10k band is the representative R1 cardinality cell; the 1M all-vs-all band is a pathological stress cell (p50 seconds/op) and is not the gate band.

## Read algebra (PASS 2x, faithful)

Sane server-bound latencies (aki not generator-capped), the credible M1-G7 result.

| workload | aki ops/s | redis | valkey | vsR | vsV | min | verdict |
|---|---|---|---|---|---|---|---|
| SINTER | 5330 | 2371 | 2353 | 2.25x | 2.27x | 2.25x | PASS |
| SUNION | 1438 | 487 | 437 | 2.96x | 3.29x | 2.96x | PASS |
| SDIFF | 5863 | 1103 | 1217 | 5.32x | 4.82x | 4.82x | PASS |
| SINTERCARD | 7944 | 3208 | 3954 | 2.48x | 2.01x | 2.01x | PASS |

min-of-min across the read-algebra rows is 2.01x (SINTERCARD vs valkey). All clear 2x.

## STORE algebra (aki-favorable, NOT a faithful cell)

| workload | aki ops/s | redis | valkey | aki p50 | redis p50 |
|---|---|---|---|---|---|
| SINTERSTORE | 1799830 | 1230 | 1470 | 390us | 634912us |
| SUNIONSTORE | 1867333 | 618 | 550 | - | - |
| SDIFFSTORE | 1754272 | 1111 | 1409 | - | - |

aki wins by ~1200-3000x, but this is NOT banked as an honest 2x pass.
redis at 0.63s per 10k SINTERSTORE is pathological, so the cell is not equal-work: the `*store` probe drives the same destination / lets sources grow, so the rivals rebuild an ever-larger dest each op while aki takes a fast path.
A faithful STORE gate needs a fixed-cardinality rotating-destination cell (lab m1/13, owed).
The read-algebra rows above already prove the set-algebra compute path clears 2x, so M1 is not blocked on this.
