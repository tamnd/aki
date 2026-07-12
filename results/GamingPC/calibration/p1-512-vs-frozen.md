# P1/512 point ops vs CF16-frozen rivals (honest re-measure)

GamingPC, 2026-07-12. aki = the #653 adaptive conn-writer-spin build (branch
m0-conn-spin-adaptive, commit 6a82a29). Rivals at their CF16-frozen io-threads
(io-threads.txt): redis 8.8.0 io-threads=6, valkey 9.1.0 io-threads=4. Gate
split: server cores 4-17 (8 shards, GOMAXPROCS 14), client 18-31.

## Why this run exists

The M0 conn-spin lab (labs/f3/m0/22_conn_spin) reported P1/512 SET 1.77x, GET
1.71x. That gate ran both rivals at `--io-threads 4`. The CF16 calibration
(io-threads.txt) then found redis peaks at io-threads=6, ~14-16% above its io=4
throughput, and at P1/512 specifically io-threads 4->6 lifts redis from ~0.75M
to ~1.26M (io-threads parallelize the 512-connection socket reads, the exact
work the lever and H1 targeted). So the 1.77x was against an under-tuned redis
and had to be re-measured.

## Client-thread validation (P1/512, single rep, n=6M)

aki is client-bound at the lab's `--threads 8`; the rivals are server-bound at
every thread count. So `--threads 8` under-measured aki and inflated nothing on
the rival side.

| client --threads | aki SET | redis6 SET | valkey4 SET |
|---|---|---|---|
| 8  | 1.41M | 1.26M | 0.73M |
| 14 | 1.71M | 1.09M | 0.56M |
| 20 | 1.60M | 1.26M | 0.50M |

aki rises 1.41M -> 1.71M as the client stops being the bottleneck; redis/valkey
do not rise (server-bound). redis is noisy across thread counts (1.09-1.26M);
its honest best is ~1.26M.

## Gate (median-of-3, client --threads 16, n=8M, 64B, P1/512c)

threads=16 is one harness config for all three, past aki's client-bound knee,
where the rivals are already server-bound, so it is fair to the rivals.

| | aki | redis io=6 | valkey io=4 | min ratio |
|---|---|---|---|---|
| SET | 1,681,026 | 1,101,777 | 541,922 | **1.53x** |
| GET | 1,680,319 | 1,141,227 | 556,022 | **1.47x** |

redis binds both. At threads=16 redis sits at a ~1.10M local dip; crediting
redis its cross-config best (~1.26M) the SET ratio is ~1.36x and GET ~1.33x.

## Verdict

Against CF16-tuned rivals, aki's P1/512 point-op advantage is **~1.4-1.5x
same-config, ~1.35x crediting redis its best client config**. NOT 2x, and NOT
the 1.77x the io=4 lab reported. aki's own throughput (~1.68M) is unchanged and
consistent with the lab; the correction is entirely on the rival side. The
conn-spin lever remains a correct adaptive change (it is what lifts aki to
~1.68M at 512c), but it does not clear the M0 point-op gate against a properly
tuned redis. The gate stays unmet; the honest P1/512 deficit toward 2x is ~1.35x
not ~1.77x.
