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

Ran first, 2026-07-11, aki c76d0 build; full tables and the sweep protocol live in the lab README.
The knee is 3 loops on the 8-cpu server mask at every shard count tried (3, 4, 5): GET 64B P16/512 reads 2.05 / 6.14 / 6.64 / 6.65 / 4.70 / 3.47 Mops at loops 1/2/3/4/6/8, an interleaved second pass reproduces it within 0.1%, and an 8-thread-generator rerun rules out a generator ceiling.
The 3-vs-4 tie on GET breaks toward 3 on SET (6.64 vs 6.14 Mops, +8%) and on p99 (1.38 vs 1.56 ms).
The doc 08 section 4.2 spec amendment this resolves: neither M = shards (loses at shards=5: 5 loops 5.70 vs 3 loops 6.65 Mops) nor M = cores minus shards (loses at shards=3: 5 loops 5.70 vs 3 loops 6.65, and on SET/p99 at shards=4); the loop count follows the core budget alone, the 2/5 network share of the doc 03 section 2.2 split, the complement of shard.DefaultShards' 3/5.
Frozen in code: Options.NetLoops <= 0 takes max(1, GOMAXPROCS*2/5), which is 3 on the gate mask (f3srv/drivers/reactor_linux.go defaultNetLoops, pinned by TestDefaultNetLoops).
Oversubscription is the predicted failure mode confirmed: every loop past the knee costs throughput (-29% at 6, -48% at 8) and p99 (2.4x at 8).
PRED-6 judgment: half hit; the knee-then-dip shape and the oversubscription mechanism landed, and loops=5 at shards=5 lost as predicted, but the predicted winner was 4 and the measured winner is 3, and the disambiguation arm falsified both spec formulas instead of electing cores-minus-shards.
RSS at 512 conns moves with loop count (516 to 701 MiB across the sweep), not with connection buffers; at the frozen default it reads 568 MiB, so section 6.2 buffer leasing is not forced at this connection count and stays a named follow-up for the 10k-conn shape (PRED-8's lab-19 half holds so far).

## Lab 18 box sweep (owed)

Pending; box table replaces the provisional container label in labs/f3/m0/18_wake_batch/README.md.

## A/B matrix (slice 7)

Pending.

## Verdict

Pending; written per doc 08 sections 5.4 and 5.7 after the matrix.
