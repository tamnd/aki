# M0 reactor A/B: three drivers, loop-count freeze, and the provisional verdict

Campaign note for the M10 pull-forward endgame (notes/Spec 2064/f3/m10-pullforward.md slices 6 and 7, plus the owed lab 18 box sweep), one gate-box session series.
Box and protocol as run 3 (results/f3/m0-run3.md): GamingPC i9-13900K WSL2, servers taskset 0-7, generator 8-15, redis 8.8.0 and valkey 9.1.0 io-threads 4, warm 3s, 3 timed 8s windows, none discarded, FLUSHALL between reps on all servers, 1M keys uniform, p99 and VmRSS captured, distinct_keys_est sanity-checked.
Ratios are the min of per-harness ratios (aki-ab/rival-ab and aki-rb/rival-rb, each harness compared with itself, never mixed), per m0-rerun's ratio discipline.
Cross-arm deltas are credited only from same-session interleaved runs; the three aki arms alternate within each cell and the rivals are measured in the same session.

## Predictions (filed before any box run)

Filed 2026-07-11, before the first campaign session, per F21.
The standing numbers they move from are run 3's (aki 19c08a4): GET 64B P16/512 1.12x, SET 64B 1.13x, GET 1KiB 1.58x, SET 1KiB 1.17x, all min-of-harness, goroutine driver.

- PRED-1, the headline: reactor GET 64B P16/512 lands 1.5-1.65x min-of-harness.
  This is the m10-pullforward slice 4 prediction carried to its A/B: the wake band (~383 ns/op at the pre-lab-11 reference) collapses to under ~80, and the plan's arithmetic put that at 1.5-1.65x from the 1.24x rerun reading; run 3 re-read the same cell at 1.12x inside the box's 13% session envelope, so the band is quoted against the protocol, not against one session.
- PRED-2, SET 64B P16/512 on the reactor: 1.25-1.45x.
  Same wire mechanism as PRED-1 but the engine write cost is a larger share of the op, so the relative gain is smaller.
- PRED-3, 1KiB P16/512 on the reactor: GET 1.55-1.8x, SET 1.15-1.35x.
  The 1KiB rows carry more bytes per syscall already, so the wake/syscall band is a smaller fraction and the reactor moves them less than the 64B rows.
- PRED-4, P1 rows (recorded-only, per PRED-F3-M0-P1REC): at P1/512 the reactor beats goroutine-single arm-to-arm by 1.1x or better (doc 08 section 4.2's wakeup-batching shape: one epoll_wait drains many ready fds); at P1/50 the two sit within 0.95-1.05x of each other (a pass rarely finds more than one ready fd, lab 18's container P1 rows already showed the fold not engaging).
- PRED-5, goroutine-pair vs goroutine-single: single is never worse; at P1 single wins by up to 1.1x (one wake/park pair deleted per round, the slice 2 mechanism), at P16 they sit within 3%.
- PRED-6, loop-count knee (lab 19): throughput rises to 4 loops and flattens or dips at 8; the winner is 4, which on this box is both readings of doc 08 section 4.2 (shards = 4 and cores minus shards = 8 - 4 = 4).
  The disambiguation arm at shards=5 (loops 3 vs 5) reads which formula the knee follows; prediction: the knee follows cores minus shards, because loops compete with unpinned owners for the same 8 cores and oversubscription manufactures churn (the section 4.2 argument), so loops=3 >= loops=5 at shards=5.
- PRED-7, lab 18 box sweep: the fold mechanism reproduces on the box; at P16/512 loop wakes/op <= 0.03 against conn wakes/op ~0.19 (yield >= 8x), widening with conns exactly as the container table; at P1 the two counters sit near each other.
- PRED-8, memory bar: aki RSS stays under 2x the best rival on every P16/512 gate cell on all three arms; the reactor's per-conn read buffers at 512 conns do not force the doc 08 section 6.2 buffer-leasing slice at this connection count.
- PRED-9, the F16 reading: the reactor does NOT meet the 0.95x-elsewhere clause across every regime it does not win (expected soft spot: P1/50 or a SET regime), so goroutine-single stays the default everywhere and the reactor stays a flag with its wins recorded.

## Loop-count lab (slice 6, labs/f3/m0/19_loop_count)

Ran first, 2026-07-11, aki c76d6c0 build; full tables and the sweep protocol live in the lab README.
The knee is 3 loops on the 8-cpu server mask at every shard count tried (3, 4, 5): GET 64B P16/512 reads 2.05 / 6.14 / 6.64 / 6.65 / 4.70 / 3.47 Mops at loops 1/2/3/4/6/8, an interleaved second pass reproduces it within 0.1%, and an 8-thread-generator rerun rules out a generator ceiling.
The 3-vs-4 tie on GET breaks toward 3 on SET (6.64 vs 6.14 Mops, +8%) and on p99 (1.38 vs 1.56 ms).
The doc 08 section 4.2 spec amendment this resolves: neither M = shards (loses at shards=5: 5 loops 5.70 vs 3 loops 6.65 Mops) nor M = cores minus shards (loses at shards=3: 5 loops 5.70 vs 3 loops 6.65, and on SET/p99 at shards=4); the loop count follows the core budget alone, the 2/5 network share of the doc 03 section 2.2 split, the complement of shard.DefaultShards' 3/5.
Frozen in code: Options.NetLoops <= 0 takes max(1, GOMAXPROCS*2/5), which is 3 on the gate mask (f3srv/drivers/reactor_linux.go defaultNetLoops, pinned by TestDefaultNetLoops).
Oversubscription is the predicted failure mode confirmed: every loop past the knee costs throughput (-29% at 6, -48% at 8) and p99 (2.4x at 8).
PRED-6 judgment: half hit; the knee-then-dip shape and the oversubscription mechanism landed, and loops=5 at shards=5 lost as predicted, but the predicted winner was 4 and the measured winner is 3, and the disambiguation arm falsified both spec formulas instead of electing cores-minus-shards.
RSS at 512 conns moves with loop count (516 to 701 MiB across the sweep), not with connection buffers; at the frozen default it reads 568 MiB, so section 6.2 buffer leasing is not forced at this connection count and stays a named follow-up for the 10k-conn shape (PRED-8's lab-19 half holds so far).

## Lab 18 box sweep (owed)

Delivered 2026-07-11 at aki b7fb698; the box table and the old-arm A/B live in labs/f3/m0/18_wake_batch/README.md, container numbers now labeled superseded.
The fold mechanism reproduces on the box and wider than the container showed: at P16/512 loop wakes are 0.013/op against conn wakes 0.121/op, a 9.23x yield, and the P1 low-conn rows sit near 1x as filed.
PRED-7 judgment: hit on both clauses (yield >= 8x and loop wakes <= 0.03 at the gate shape; P1 counters converge at low conns).
The owed old-vs-new throughput A/B (d81a66b pre-batch arm vs main, both -net reactor -net-loops 3, GET 64B P16/512, 3 interleaved reps) reads old 6.642-6.647 Mops p99 1.39-1.46 ms against new 6.649 Mops p99 1.391 ms in all three reps: a tie at the harness plateau with a steadier tail, because at P16 the unbatched path only paid ~0.12 eventfd writes/op and the batch's own headroom at that depth is small.
A second old-vs-new A/B at GET 64B P1/512, where the unbatched path pays a full 1.0 eventfd write per op, shows where the fold actually pays: both arms plateau at 855-856 krps (the client limits at P1) but the batched arm cuts avg latency 8% (0.423 vs 0.454-0.464 ms) and p99 6% (0.855-0.871 vs 0.903-0.919 ms).
The plan's 1.5-1.65x figure was always the whole-reactor-vs-rivals claim, and that is judged by the slice 7 matrix below, not by this arm pair.

## A/B matrix (slice 7)

Ran 2026-07-11 at aki b7fb698 in one box session: five resident servers (aki single on 7111, redis 7112, valkey 7113, aki pair 7114, aki reactor 7115, all taskset 0-7), the three aki arms interleaved per rep inside every cell, and the frozen -net-loops 3 on the reactor.
Sixteen cells: {GET, SET} x {64B, 1KiB} x {P16, P1} x {512, 50 conns}; each cell ran 3 aki-bench reps per arm (every rep measures the arm and both rivals in the same invocation) plus redis-benchmark --threads 4 against all five servers.
Driver stamps were verified per arm via INFO net_driver, distinct_keys_est passed the sanity check in every cell, and no crash or correctness failure appeared anywhere in the campaign.
The P1 rows are recorded-only per PRED-F3-M0-P1REC; the gate rows are the P16/512 block.
Raw cell outputs, meta files, and the machine summary live under results/f3/m0-reactor-ab/ (matrix/cells, matrix/summary.txt, matrix/env.txt).

| cell | arm | aki ab ops/s | ab ratio | rb ratio | headline | aki p99 us (ab) | aki RSS MiB | RSS vs rival |
|---|---|---|---|---|---|---|---|---|
| get_64b_p16_c512 | single | 4,842,780 | 1.02x | 1.06x | 1.02x | 3,721 | 478 | 3.81x |
| get_64b_p16_c512 | pair | 4,275,003 | 0.90x | 1.06x | 0.90x | 4,139 | 479 | 3.82x |
| get_64b_p16_c512 | reactor | 7,096,034 | 1.50x | 1.50x | 1.50x | 2,591 | 509 | 4.06x |
| get_1k_p16_c512 | single | 3,189,552 | 1.53x | 1.58x | 1.53x | 5,423 | 1,804 | 1.39x |
| get_1k_p16_c512 | pair | 2,930,743 | 1.39x | 1.46x | 1.39x | 5,644 | 1,807 | 1.39x |
| get_1k_p16_c512 | reactor | 3,799,053 | 1.82x | 1.90x | 1.82x | 4,235 | 1,909 | 1.47x |
| set_64b_p16_c512 | single | 6,496,765 | 1.75x | 1.64x | 1.64x | 2,722 | 405 | 3.18x |
| set_64b_p16_c512 | pair | 5,994,399 | 1.62x | 1.53x | 1.53x | 2,859 | 408 | 3.20x |
| set_64b_p16_c512 | reactor | 7,022,143 | 1.91x | 1.77x | 1.77x | 2,533 | 473 | 3.71x |
| set_1k_p16_c512 | single | 3,181,996 | 1.02x | 1.08x | 1.02x | 5,308 | 1,357 | 1.03x |
| set_1k_p16_c512 | pair | 3,201,565 | 1.04x | 1.08x | 1.04x | 5,198 | 1,361 | 1.03x |
| set_1k_p16_c512 | reactor | 4,144,915 | 1.34x | 1.55x | 1.34x | 3,817 | 1,348 | 1.02x |
| get_64b_p16_c50 | single | 2,980,488 | 0.79x | 1.05x | 0.79x | 605 | 242 | 1.98x |
| get_64b_p16_c50 | pair | 3,418,442 | 0.91x | 1.24x | 0.91x | 553 | 242 | 1.98x |
| get_64b_p16_c50 | reactor | 3,160,803 | 0.84x | 1.05x | 0.84x | 556 | 249 | 2.04x |
| get_1k_p16_c50 | single | 2,853,346 | 1.36x | 1.58x | 1.36x | 629 | 1,160 | 0.90x |
| get_1k_p16_c50 | pair | 2,914,006 | 1.38x | 1.73x | 1.38x | 618 | 1,154 | 0.90x |
| get_1k_p16_c50 | reactor | 2,595,218 | 1.23x | 1.46x | 1.23x | 663 | 1,207 | 0.94x |
| set_64b_p16_c50 | single | 3,007,205 | 1.02x | 1.42x | 1.02x | 599 | 234 | 1.91x |
| set_64b_p16_c50 | pair | 3,396,082 | 1.15x | 1.59x | 1.15x | 561 | 234 | 1.92x |
| set_64b_p16_c50 | reactor | 3,170,660 | 1.08x | 1.35x | 1.08x | 558 | 247 | 2.02x |
| set_1k_p16_c50 | single | 2,674,662 | 1.04x | 1.55x | 1.04x | 658 | 1,155 | 0.87x |
| set_1k_p16_c50 | pair | 3,032,895 | 1.18x | 1.55x | 1.18x | 592 | 1,156 | 0.87x |
| set_1k_p16_c50 | reactor | 2,726,243 | 1.06x | 1.42x | 1.06x | 656 | 1,157 | 0.88x |
| get_64b_p1_c512 | single | 754,759 | 0.70x | 0.72x | 0.70x | 2,662 | 328 | 2.64x |
| get_64b_p1_c512 | pair | 683,023 | 0.63x | 0.76x | 0.63x | 3,185 | 331 | 2.66x |
| get_64b_p1_c512 | reactor | 1,010,118 | 0.93x | 0.93x | 0.93x | 1,006 | 521 | 4.18x |
| get_1k_p1_c512 | single | 557,687 | 0.53x | 0.59x | 0.53x | 4,039 | 1,181 | 0.92x |
| get_1k_p1_c512 | pair | 620,852 | 0.59x | 0.68x | 0.59x | 3,609 | 1,185 | 0.92x |
| get_1k_p1_c512 | reactor | 961,274 | 0.91x | 0.93x | 0.91x | 1,060 | 1,449 | 1.13x |
| set_64b_p1_c512 | single | 830,994 | 0.81x | 0.76x | 0.76x | 2,357 | 328 | 2.67x |
| set_64b_p1_c512 | pair | 768,326 | 0.75x | 0.76x | 0.75x | 2,593 | 331 | 2.70x |
| set_64b_p1_c512 | reactor | 1,010,807 | 0.97x | 0.93x | 0.93x | 1,007 | 521 | 4.24x |
| set_1k_p1_c512 | single | 834,831 | 0.87x | 0.78x | 0.78x | 2,055 | 1,245 | 0.95x |
| set_1k_p1_c512 | pair | 808,263 | 0.85x | 0.87x | 0.85x | 2,488 | 1,248 | 0.96x |
| set_1k_p1_c512 | reactor | 966,225 | 1.02x | 1.00x | 1.00x | 1,070 | 1,428 | 1.10x |
| get_64b_p1_c50 | single | 342,572 | 0.53x | 0.83x | 0.53x | 451 | 227 | 1.86x |
| get_64b_p1_c50 | pair | 358,246 | 0.55x | 0.63x | 0.55x | 459 | 227 | 1.86x |
| get_64b_p1_c50 | reactor | 274,151 | 0.42x | 0.45x | 0.42x | 459 | 232 | 1.90x |
| get_1k_p1_c50 | single | 338,851 | 0.55x | 0.71x | 0.55x | 452 | 1,092 | 0.85x |
| get_1k_p1_c50 | pair | 342,196 | 0.54x | 0.71x | 0.54x | 466 | 1,092 | 0.85x |
| get_1k_p1_c50 | reactor | 271,065 | 0.43x | 0.42x | 0.42x | 460 | 1,168 | 0.91x |
| set_64b_p1_c50 | single | 339,686 | 0.55x | 0.71x | 0.55x | 462 | 219 | 1.81x |
| set_64b_p1_c50 | pair | 359,271 | 0.57x | 0.63x | 0.57x | 458 | 221 | 1.83x |
| set_64b_p1_c50 | reactor | 274,613 | 0.44x | 0.42x | 0.42x | 455 | 222 | 1.83x |
| set_1k_p1_c50 | single | 329,331 | 0.59x | 1.00x | 0.59x | 469 | 1,071 | 0.90x |
| set_1k_p1_c50 | pair | 341,170 | 0.61x | 0.88x | 0.61x | 473 | 1,076 | 0.90x |
| set_1k_p1_c50 | reactor | 269,834 | 0.48x | 0.58x | 0.48x | 465 | 1,030 | 0.86x |

Cross-session note: this session runs about a fifth below run 3 in absolute ops on the 64B P16/512 cells for aki and rivals alike (single-arm GET reads 4.84 against run 3's 6.26 Mops with bit-identical rival binaries), so absolutes are not comparable across sessions and only the in-session ratios above are credited; the single-arm ratio itself moved 1.12x to 1.02x, inside m0-rerun's 13% session envelope.
RSS is the arm's post-aki-bench peak; aki RSS climbs across reps within a cell (freed arena pages are not returned after FLUSHALL) and the rb preload pushes it higher still, so the column is the harness-consistent post-ab state.

What the table says, regime by regime.
At P16/512, the gate block, the reactor sweeps all four cells: GET 64B 1.50x, SET 64B 1.77x, GET 1KiB 1.82x, SET 1KiB 1.34x, with the best p99 of any arm in every cell.
At P1/512 the reactor closes most of the goroutine deficit (GET 64B 0.70x to 0.93x, GET 1KiB 0.53x to 0.91x, SET 1KiB to 1.00x) and cuts aki p99 to about 1.0 ms from the goroutine arms' 2.1-3.6 ms, but never beats the rivals.
At P16/50 the pair arm is the best aki arm in all four cells and the reactor sits below both goroutine arms; the GET 64B cell is a loss for every arm (valkey ab 3.75-3.78 Mops against aki 2.98-3.42).
At P1/50 the reactor collapses to 0.42-0.48x, below single's 0.53-0.59x (arm-to-arm about 0.80x), because with 50 conns spread over 3 loops a drain pass folds almost nothing and every op pays a loop wake; valkey's io-threads dominate this regime outright at 559-652 kops against aki's 270-359.

## Prediction judgments

- PRED-1: hit, at the floor of the band; reactor GET 64B P16/512 reads 1.50x on both harnesses against the filed 1.5-1.65x.
- PRED-2: miss high; SET 64B P16/512 reads 1.77x against the filed 1.25-1.45x, because the rivals' in-session SET numbers sit lower relative to GET than the band assumed (the single arm alone already reads 1.64x).
- PRED-3: half hit; SET 1KiB 1.34x lands inside 1.15-1.35x, GET 1KiB 1.82x grazes past the 1.55-1.8x top edge and is scored a miss high by the letter.
- PRED-4: half hit; at P1/512 the reactor beats single arm-to-arm by 1.34x (filed: 1.1x or better), but the P1/50 parity clause missed badly, reactor at about 0.80x of single instead of 0.95-1.05x.
- PRED-5: miss; pair beats single in all four P16/50 cells (up to +15% on ab) and in several P1 cells, and at P16/512 the two sit 8-12% apart on GET instead of within 3%.
- PRED-6: half hit; the knee-then-dip shape, the oversubscription mechanism, and loops-5-loses landed, but the winner is 3, not 4, and the disambiguation arms falsified both spec formulas (judged in the loop-count section above).
- PRED-7: hit; box yield 9.23x at the gate shape with loop wakes 0.013/op, P1 counters converge at low conns (judged in the lab 18 section above).
- PRED-8: miss on the bar clause, hit on the leasing clause; every 64B cell breaches the 2x RSS bar on every arm (3.2-4.2x at P16/512, see the memory section below) while the 1KiB rows pass at 0.85-1.47x, and the reactor's 512-conn buffers alone still do not force the doc 08 section 6.2 leasing slice.
- PRED-9: hit; the reactor fails the 0.95x-elsewhere clause in both c50 regimes, so goroutine-single stays the default (the F16 reading below).

## Verdict

### F16 default decision

Doc 08 section 4.4 (F16): a driver becomes a cross-regime default only if it wins its regime and is no worse than 0.95x of the best arm in the regimes it does not win.
The reactor wins P16/512 and P1/512 outright but reads 0.89-0.93x of pair at P16/50 and 0.76-0.80x of the best goroutine arm at P1/50, so it fails the clause and stays behind -net reactor with its wins recorded here.
The pair arm wins P16/50 but reads 0.88x of single on GET 64B P16/512, so it fails the same clause and stays behind -conn-shape pair.
Reading stated explicitly: goroutine-single remains the sole default; nothing in this matrix met the promotion bar.

### Provisional verdict, per doc 08 sections 5.4 and 5.7

The M0 verdict is provisional and string-scoped: it covers GET/SET/INCR-class string ops on this box, this kernel, and these rival builds, and nothing else inherits it.
Re-validation triggers, per section 5.7: any change to the driver seam, the owner hop, the spin policy, or the batch grouper reruns the 4-cell subset (GET/SET x 64B/1KiB at P16/512); a Go runtime netpoller or scheduler change, a kernel change, or a rival major release reopens the H1 term; collection types carry their own P1 rows and do not inherit the string verdict.
Against section 5.4's two-verdict rule the campaign closes neither: 2x at P1 does not hold on any driver (best P1 cell is 1.00x), and the unreachability verdict is not yet earned because the io_uring arm has not been built.

### Memory bar

The standing bar is aki RSS under 2x the best rival on gate cells.
Every 64B cell breaches it on every arm: 3.2-4.2x at P16/512, 2.6-4.2x at P1/512, 1.8-2.0x at c50, against rivals holding the same 1M x 64B set in 128-138 MiB.
The breach is mostly pre-existing arena behavior, not a reactor cost: the 512 MiB-per-shard arena config commits pages as reps fill it and FLUSHALL does not return them, so the goroutine baseline already sits at 3.8x on the headline cell, consistent with run 3's post-run reading.
The reactor adds real buffer weight on top at 512 conns: +31 MiB at P16/512 (509 vs 478 MiB) and +193 MiB at P1/512 (521 vs 328 MiB), the per-conn read buffers and reply slack of doc 08 section 6.2.
All 1KiB cells pass the bar (0.85-1.47x), where the arena reserve is proportionate to the data.
Flagged as owed work: arena page return after FLUSHALL (or a right-sized gate config) to make the 64B bar readable, and section 6.2 buffer leasing before any 10k-conn shape; neither is a slice of this campaign.

### Honest ceiling

The reactor closed the wake band and nothing else: the headline moved 1.12x to 1.50x at GET 64B P16/512, exactly the floor of the plan's 1.5-1.65x arithmetic, and the gate's 2x is still 33% away on that cell.
What remains at P16/512 is the syscall floor the m10 plan named: the reactor still pays epoll_wait plus one read and one write per batch, and no scheduling change deletes those.
At P1 the reactor proves the point from the other side: it deletes the wake/park tax (p99 drops 2.7x to about 1 ms at 512 conns) and still tops out at 0.93-1.00x of rivals, because at depth 1 the two syscalls per op are the op.
P1/50 is not ceiling but a real aki loss: valkey serves 559-652 kops where aki serves at best 359, and the reactor makes it worse, so that regime needs an idea (per-loop adaptive wake suppression or conn-count-aware loop collapse), not a bigger hammer.
Next per section 5.4: build the io_uring driver (M10 slice 3 proper), whose multishot recv deletes the read syscall and whose SQ batching deletes most writes; if that arm cannot reach 2x at P1 on the 4-cell subset, publish the H1 ceiling with these tables as the evidence.

### Spec amendment record (doc 08 section 4.2)

Recorded here rather than edited into the spec, per campaign rules.
Section 4.2's two loop-count formulas (M = shards; M = cores minus shards) are both falsified by lab 19: the knee is 3 loops at shards 3, 4, and 5 on the 8-cpu server mask, so the loop count follows the core budget alone.
The amended rule, frozen in code: NetLoops defaults to max(1, GOMAXPROCS x 2/5), the network share of the doc 03 section 2.2 core split, independent of the shard count (labs/f3/m0/19_loop_count, f3srv/drivers/reactor_linux.go defaultNetLoops).
