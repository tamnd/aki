# M0 arena-RSS A/B: the 64B memory breach, the fix, and the tightened bar

Campaign for issue #542 and lab 20 (labs/f3/m0/20_arena_rss), the 64B RSS follow-up the reactor campaign flagged (results/f3/m0-reactor-ab.md, PRED-8).
One gate-box session, 2026-07-11: GamingPC WSL2, kernel 6.18 microsoft-standard-WSL2, 32 threads, 56GiB, servers taskset 0-7, generator 8-15.
Four aki arms resident at once plus both rivals: osingle and oreactor are the before build (main 49ab548, heap-backed arena), nsingle and nreactor are the after build (this branch, mapped arena plus leased reactor buffers), redis 8.8.0 and valkey 9.1.0 with io-threads 4.
The arms alternate within every rep so cross-arm deltas are same-session, and every aki-bench invocation measures its arm and both rivals in the same call, so per-arm ratios are same-invocation.
Protocol as run 3: warm 3s, 3 timed 8s windows, none discarded, FLUSHALL between reps on all servers, 1M keys uniform, p99, VmRSS, VmHWM, distinct_keys_est, and used_memory captured.
Throughput ratios are the min of per-harness ratios (aki-ab over rival-ab, aki-rb over rival-rb, each harness compared with itself, never mixed).
Raw cell outputs, meta files, the machine summary, and the lab 20 dumps live under results/f3/m0-rss/ (matrix/cells, matrix/summary.txt, matrix/env.txt, matrix/lab20.*.txt); the summarizer is results/f3/m0-rss/rss_summarize.py.
Binary provenance: f3srv-old sha256 7227be35, f3srv-new sha256 8c20c956, both cross-built linux/amd64 from this tree.

## The bar changed mid-campaign

The standing bar was aki RSS under 2x a rival on gate cells.
It was tightened during this slice to a same-data in-memory-fit bar: aki should hold the same dataset in no more RAM than the leaner rival, resident and peak, at or below 1.0x, ideally near 0.5x, because a store that costs more RAM than redis or valkey has no LTM pitch.
This report judges the after-state against the new bar honestly, reports where it still misses, and names the levers to close the rest.
The 2x-ceiling column is kept alongside so the improvement over the reactor campaign's 3.2-4.2x breach is legible.

## Throughput: the gate is not regressed

HEAD ratio is min(ab, rb), each min over both rivals; before is osingle/oreactor, after is nsingle/nreactor.

| cell | single before | single after | reactor before | reactor after |
|---|---|---|---|---|
| get_64b_p16_c512 | 0.81x | 0.80x | 1.42x | 1.41x |
| set_64b_p16_c512 | 1.28x | 1.25x | 1.77x | 1.77x |
| get_1k_p16_c512 | 1.30x | 1.32x | 1.85x | 1.96x |
| set_1k_p16_c512 | 1.29x | 1.15x | 1.34x | 1.46x |
| get_64b_p1_c512 | 0.73x | 0.84x | 0.93x | 0.93x |
| set_64b_p1_c512 | 0.81x | 0.75x | 0.93x | 0.93x |

The reactor arm, the gate winner, is unchanged or better in every cell (get_1k 1.85 to 1.96, set_1k 1.34 to 1.46, the rest flat).
Two single-arm cells move more than 3 percent: set_1k_p16 single 1.29 to 1.15 and set_64b_p1 single 0.81 to 0.75.
Both are ab-only swings with a flat redis-benchmark ratio in the same cell (set_1k rb 1.40 both, set_64b_p1 rb 0.81 both), so the server's own throughput did not move and the swing is session and interleave variance inside the box's 13 percent envelope, not a code effect.
That the arena-map change cannot touch steady throughput is structural: it changes only how New allocates the arena backing, not the Set or Get path, and the buffer-leasing change is reactor-only, so the goroutine-single arm runs the same hot code before and after.
The single-arm cell that improved for the same reason, get_64b_p1 0.73 to 0.84, is the same variance in the other direction.

## Memory: resident and peak, same 1M-key dataset

loadRSS is each server's own post-benchmark idle RSS with the 1M keys resident; peakHWM is the max VmHWM the server reached in the cell; both bars are against the leaner rival, goal at or below 1.0x.
ledgB/key is the server's own used_memory divided by distinct keys, the intrinsic per-key density independent of buffers and reservation.

| cell | server | loadRSS MiB | steady bar | peakHWM MiB | peak bar | ledg B/key |
|---|---|---|---|---|---|---|
| get_64b_p16 | redis | 146 | - | 150 | - | 127.5 |
| get_64b_p16 | valkey | 125 | - | 126 | - | 115.1 |
| get_64b_p16 | osingle (before) | 524 | 4.20x | 524 | 4.17x | 113.4 |
| get_64b_p16 | nsingle (after) | 341 | 2.73x | 353 | 2.81x | 113.4 |
| get_64b_p16 | oreactor (before) | 731 | 5.86x | 731 | 5.82x | 113.4 |
| get_64b_p16 | nreactor (after) | 288 | 2.31x | 296 | 2.36x | 113.4 |
| set_64b_p16 | redis | 152 | - | 157 | - | 127.5 |
| set_64b_p16 | valkey | 127 | - | 130 | - | 115.1 |
| set_64b_p16 | osingle (before) | 405 | 3.19x | 405 | 3.11x | 113.4 |
| set_64b_p16 | nsingle (after) | 315 | 2.48x | 318 | 2.45x | 113.4 |
| set_64b_p16 | nreactor (after) | 187 | 1.47x | 222 | 1.70x | 113.4 |
| get_1k_p16 | redis | 1354 | - | 1360 | - | 1327.5 |
| get_1k_p16 | valkey | 1412 | - | 1496 | - | 1331.1 |
| get_1k_p16 | osingle (before) | 3111 | 2.30x | 3111 | 2.29x | 1073.4 |
| get_1k_p16 | nsingle (after) | 2127 | 1.57x | 2268 | 1.67x | 1073.4 |
| get_1k_p16 | nreactor (after) | 2059 | 1.52x | 2117 | 1.56x | 1073.4 |
| set_1k_p16 | redis | 1322 | - | 1332 | - | 1327.5 |
| set_1k_p16 | valkey | 1326 | - | 1588 | - | 1331.1 |
| set_1k_p16 | osingle (before) | 1381 | 1.04x | 1381 | 1.04x | 1073.4 |
| set_1k_p16 | nsingle (after) | 1181 | 0.89x PASS | 1209 | 0.91x PASS | 1073.4 |
| set_1k_p16 | nreactor (after) | 1153 | 0.87x PASS | 1158 | 0.87x PASS | 1073.4 |
| get_64b_p1 | valkey | 124 | - | 125 | - | 115.1 |
| get_64b_p1 | osingle (before) | 292 | 2.35x | 292 | 2.33x | 113.6 |
| get_64b_p1 | nsingle (after) | 228 | 1.83x | 228 | 1.82x | 113.5 |
| get_64b_p1 | oreactor (before) | 602 | 4.84x | 602 | 4.80x | 113.5 |
| get_64b_p1 | nreactor (after) | 167 | 1.35x | 167 | 1.33x | 113.5 |
| set_64b_p1 | valkey | 125 | - | 126 | - | 115.1 |
| set_64b_p1 | nsingle (after) | 233 | 1.86x | 248 | 1.97x | 113.5 |
| set_64b_p1 | nreactor (after) | 171 | 1.36x | 171 | 1.36x | 113.5 |

The new bar is met on set_1k on both after arms (0.87-0.91x resident and peak) and missed everywhere else.
The old 2x ceiling is cleared on every 1KiB and 64B-P1 cell and on set_64b_p16 reactor, and still missed on the 64B-P16 GET cells and on set_64b_p16 single.

## The key finding: aki's records are the densest, its resident footprint is not

The intrinsic density says aki already wins: ledger bytes per key is 113.4 for aki against redis 127.5 and valkey 115.1 at 64B, and 1073.4 for aki against redis 1327.5 and valkey 1331.1 at 1KiB.
So the 64B RSS miss is not the data structure; it is resident overhead that sits on top of a dense store.

Where the 64B bytes go, taking nsingle get_64b at 341 MiB loadRSS for 1M keys against a 113 MiB ledger:

- Live data, arena records plus index: about 113 MiB (the ledger), of which the arena itself is about 70 MiB on a single write-once fill (lab 20 arenaUsed 60.7 MiB, ledger 69.8 MiB).
- Arena dead-record slack: the dominant term, about 200 MiB here. The GET cell's redis-benchmark preload writes 4M sets over 1M keys, and the store is a log-structured append arena, so every overwrite appends a live record and marks the old one dead; four-fold write amplification leaves roughly three dead records per live one resident until a segment crosses its compaction threshold. redis and valkey overwrite in place and never carry this.
- Per-shard reservation touched pages and GC headroom: the smaller remainder, and the part the arena-map fix already tamed (see below).

The write-once footprint, no overwrite, is close to a pass: lab 20's mapped arm fills 1M 64B keys and flushes back to 114-146 MiB resident, next to redis's 146 MiB.
1KiB SET passes because the 1KiB value dominates the per-key cost, aki's denser encoding shows through (1073 against 1330 B/key), and the SET cells carry no 4M-overwrite preload, so there is little dead slack.
The through-line: aki loses the 64B same-data bar on overwrite-driven dead slack and small-value reservation overhead, not on how tightly it stores a key.

## What each change contributed

The arena-map change (engine/f3/store/arena_map_unix.go) moves the arena backing off the Go heap, and it helps every arm because it is in the shared store.
Isolating it by the two goroutine-single arms: get_64b_p16 524 to 341 MiB (-35 percent), get_1k_p16 3111 to 2127 MiB (-984 MiB), set_64b_p16 405 to 315, set_1k_p16 1381 to 1181, get_64b_p1 292 to 228.
The mechanism is lab 20: a make([]byte) arena counts its full 4-shard-times-512MiB reservation as live heap, the pacing goal lands past 4GiB, the collector runs 3 times in the whole run, and per-rep garbage becomes permanent RSS, the residual climbing 548 to 1091 to 1634 MiB; mapped, the heap holds only the 75-139 MiB substrate, the collector runs 19 then 37 then 50 times, and the post-flush residual is flat at 114-146 MiB.

The buffer-leasing change (f3srv/drivers/reactor_linux.go) is reactor-only and it deletes the reactor's idle-connection buffer excess that PRED-8 flagged.
Before, the reactor cost more than single: oreactor minus osingle was +207 MiB at get_64b_p16 and +310 MiB at get_64b_p1, the per-conn read and reply buffers of 512 mostly-idle connections.
After, the reactor costs less than single: nreactor minus nsingle is -53 MiB at get_64b_p16 and -61 MiB at get_64b_p1, because a cleanly parked connection now holds no buffers.
The m0-reactor-ab +193 MiB P1/512 reactor excess is gone: that campaign read the reactor at 521 MiB and 4.18x on get_64b_p1, this one reads nreactor at 167 MiB and 1.35x.
Peak tracks it: nreactor peak HWM is at or below nsingle's in every cell, so the leasing gives back transient pages too, not just steady ones.

## Verdict

The throughput gate holds: the reactor arm is unregressed or better on every gate cell, and the two single-arm ab dips are variance with a flat rb companion, off the hot path by construction.
The old under-2x ceiling that the reactor campaign breached at 3.2-4.2x is cleared on 1KiB and on 64B-P1 and reduced to 2.3-2.8x on the worst remaining cell, 64B-P16 GET.
The tightened same-data bar is met only where the value dominates, 1KiB SET at 0.87-0.91x resident and peak, and missed on 64B (1.35-2.81x) and 1KiB GET (1.52-1.67x).
The honest reading: this slice removed the pathological GC-pacing retention and the reactor's idle-buffer excess, a large step, but aki still uses more RAM than the leaner rival for a small-value dataset under overwrite, so the LTM memory pitch is not yet won at 64B.

## Follow-ups, with rough expected savings

- Arena dead-record compaction under sustained overwrite (biggest lever). Return a segment's dead pages via MADV_DONTNEED once its dead ratio crosses a threshold, instead of waiting for a full Reset, so the 4x-overwrite slack does not stay resident. Expected: 64B GET loadRSS about 341 to about 150 MiB, near redis, which would clear the new bar. This trades write throughput for memory (each compaction pass copies live records), so it must be paced not to regress the SET gate and needs its own lab and A/B; flagged, not taken in this slice.
- Small-value reservation and index right-sizing. The per-shard arena reservation and the dashtable directory carry touched-but-cold pages that matter most when the value is 64B; an elastic-grow arena and a tighter initial index would shave the remainder after compaction. Expected: tens of MiB at 64B, secondary to the compaction lever.
- Reply and read buffer cap under many active connections. Leasing removed the idle-conn excess; a global stock-buffer cap would trim the active-conn peak on the 512-conn shape further. Expected: small, tens of MiB on peakHWM, relevant to the peak bar.

The 64B gap is now a bounded, named engineering target rather than a mystery, and the compaction lever is the next slice's plan.

## Provenance and re-validation

One session, same-session interleaved, absolutes not comparable across sessions (the box swings about 13 percent); only the in-session ratios and cross-arm deltas above are credited.
No crash or correctness failure in any cell; driver stamps verified per arm via INFO net_driver (osingle/nsingle goroutine, oreactor/nreactor reactor); distinct_keys_est 1M in every cell.
Re-validation triggers: any change to the arena backing, the store allocation path, or the reactor buffer discipline reruns this 6-cell subset; a Go runtime GC or scavenger change reopens the pacing term; a rival major release reopens the density comparison.
