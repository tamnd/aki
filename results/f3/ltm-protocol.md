# Larger-than-memory benchmark protocol

Issue tamnd/aki#542.
This note redesigns the larger-than-memory (LTM) benchmark so it measures a fair thing, and it records the accounting the harness now carries to enforce it.
The protocol landed first with only a local plumbing smoke, marked below as not a result.
The box run happened on 2026-07-11 at aki 7688c00; its numbers are in the "Box run" section and the raw per-cell artifacts sit next to this note under results/f3/ltm-gate/.

## What the M0 LTM pair actually measured

The M0 gate ran one LTM posture (results/f3/m0-gate.md, "LTM pair").
It pitted aki holding the whole keyspace at 1032-byte values, a 512 MiB resident cap split 128 MiB across four shards, values in the vlog on NVMe, against redis 8.8 and valkey 9.1 at maxmemory 512mb with maxmemory-policy allkeys-lfu.
The rivals never touched disk.
They evicted roughly half the keyspace and answered from RAM, with used_memory pinned at 525 MB and zero read_bytes, while aki honored retention and read 6.7 GB from the vlog during the GET reps.
Measured that way aki came out at 0.01x on GET, 40k ops/s against 4.85M, and 0.02x on SET, with p99 at 328 ms.

Those ratios compare serving the data against serving almost none of it.
A GET for a key the rival evicted returns nil, and a nil GET still completes as an op, so the rival's throughput counts replies that carry no data.
The row is honest about the raw numbers and lopsided in what they mean.

Two real problems live underneath the unfairness and survive any reframing.
The vlog read path is about 100x too slow to gate, 40k ops/s over four shards on NVMe, which points at synchronous single-read behavior with no batching or readahead.
And aki RSS reached 2.27 GB against a 512 MiB resident-cap configuration, so the cap does not bound total process memory: the index and the per-connection buffers live outside it.
The new protocol is built so those two problems show up as themselves, not as a throughput ratio that folds eviction and slowness together.

## Principle: count data-bearing work, and measure coverage

A served op is a reply that carries the value the workload wrote.
A nil or a miss is not served work, and it is counted separately.
aki-bench already splits this: value_ops_per_sec excludes nils, hit_ratio and nil_replies are recorded, and redis-benchmark stays banned on LTM cells because it counts nils as ops.

Throughput alone cannot tell a fast server from one that dropped the data, so every LTM cell now also carries a dataset-coverage number.
After the timed window, aki-bench samples random keys across the full keyspace, GETs each, and reports the fraction that come back with the exact written length and content.
A rival capped at maxmemory answers nil there for every key it evicted, so a run that posted high ops while silently losing half its dataset reads as a coverage well under 100 percent instead of as throughput.
The probe runs on its own connection off any timed path, in connect mode, so it reads every server in the cell including the rivals.

## Two arms per LTM cell

Each LTM cell runs twice, because there are two fair questions and one posture cannot answer both.

Equal-cap arm.
All three servers get the same 512 MiB memory ceiling.
The rivals evict under allkeys-lfu; aki spills to the vlog.
The comparison is data-bearing throughput and dataset coverage and peak memory together, never raw ops, because the rivals' ops here include the nils they return for evicted keys.
The honest reading is that a rival can be faster per op while serving a quarter of the data, and its cost per key it can still serve is then several times aki's.

Equal-data arm.
The rivals get a maxmemory large enough to hold the entire dataset with noeviction, so they keep 100 percent coverage.
aki keeps the same small 512 MiB resident cap and serves the rest off the vlog.
This is the product pitch: same coverage, far less resident memory.
At equal coverage the throughput ratio is a clean comparison, and it sits next to peak memory and bytes per retrievable key, which is where aki is supposed to win.

## Memory metrics on every cell

Every server on every cell reports steady resident set (VmRSS) and peak resident set (VmHWM).
Peak matters because the LTM pitch is a memory-ceiling claim, and a peak that spikes above a rival during load breaks the claim even when the settled RSS is lower.
On the box the runner reads both from /proc/<pid>/status for all three servers, since aki-bench in connect mode has no rival PID to read; aki-bench reads VmHWM directly in launch mode and in its own tests.

Each cell also reports bytes per retrievable key: peak memory over the keys the server can still serve, which is coverage fraction times the keyspace.
This is the fair memory-efficiency figure.
A rival that evicted three quarters of the dataset looks cheap per stored key and expensive per retrievable key, because most of what it stored is gone.

## Proposed gate (PROPOSAL, written before the box run)

This section is a proposal, not a decision.
It was written down so the box run had an explicit bar to check against; the run has now happened and the "Box run" section below checks against it.

The equal-cap arm does not gate on a throughput ratio.
It is a coverage-and-cost row: aki must serve materially higher coverage than an evicting rival at the same cap, and its bytes per retrievable key must be lower.
A throughput ratio on this arm is only quoted alongside the coverage gap that produced it, never on its own.

The equal-data arm is the one that can gate on speed, because coverage is equal by construction.
The proposed bar is that at 100 percent coverage on both sides, aki's data-bearing throughput is within a stated fraction of the rivals while its peak resident set is a stated fraction of theirs.
The exact multipliers are left blank here on purpose; the vlog read path at 40k ops/s must first be fixed, and the numbers that set the bar have to come from the box, not from this note.
Until the read path clears its own floor, the equal-data throughput arm is expected to fail, and that failure is the signal, not a surprise.

## Exact box-run commands (as run on 2026-07-11)

Build the two binaries into the run directory, then launch the runner under a session that survives disconnect.

```
# on the gate box, from a checkout of aki-bench at the #43 branch tip
mkdir -p /root/f3gate/ltm-gate/bin
GOFLAGS=-trimpath go build -o /root/f3gate/ltm-gate/bin/aki-bench ./cmd/aki-bench
# f3srv from the aki repo at the commit under test
(cd /root/aki && GOFLAGS=-trimpath go build -o /root/f3gate/ltm-gate/bin/f3srv ./cmd/f3srv)

# copy the runner and summarizer next to the run dir
cp /root/aki/results/f3/ltm-gate/ltmgate.sh    /root/f3gate/ltm-gate/
cp /root/aki/results/f3/ltm-gate/ltmsummary.py /root/f3gate/ltm-gate/

# run: server cpus 0-7, generator 8-15, resumable via done.list
setsid bash /root/f3gate/ltm-gate/ltmgate.sh >/root/f3gate/ltm-gate/run.log 2>&1 &

# after it finishes, summarize
python3 /root/f3gate/ltm-gate/ltmsummary.py /root/f3gate/ltm-gate/cells
```

The runner brings up f3srv with four shards, a 256 MiB arena, a 128 MiB per-shard resident cap, and a vlog directory, and the rivals with io-threads 4.
The equal-cap cells set the rivals to maxmemory 512mb allkeys-lfu; the equal-data cells set them to maxmemory 4096mb noeviction, comfortably above the roughly 2 GB dataset of 2M keys at 1032 bytes.
It resets each server's VmHWM with clear_refs before every rep so the peak is the peak reached during that rep, samples VmRSS and VmHWM every second, and runs aki-bench with -coverage-probe 100000.

## Local smoke (not a result)

This is a plumbing check on a laptop, not a benchmark, and none of it is a gate number.
It exists only to prove the accounting works before the code reaches the box.

A local redis was capped at maxmemory 32mb with allkeys-lru, and aki-bench drove a set workload of 200k keys at 1032 bytes, about 200 MB, far past the cap, then probed coverage.

```
target  version       ops/sec  vops/sec  hit%   cov%   ...
redis   redis 8.8.0   611717   611717    100.0  9.9    ...
redis server window: maxmemory=32 MB policy=allkeys-lru evicted=2589004 hits=1971 misses=18029
redis dataset coverage: 1971/20000 retrievable (9.9%), 18029 missing
```

The redis posted 611k data-bearing ops at a 100 percent hit rate during the write window, and the coverage probe then found only 9.9 percent of the keyspace retrievable, with 18029 of 20000 sampled keys missing and 2.58M evictions.
That is the M0 failure in miniature: throughput and hit rate look healthy while the dataset is mostly gone, and the coverage number is what catches it.
The uncapped servers in the same run probed at 100 percent.
VmHWM read as empty in this smoke because it is Linux-only and connect mode has no PID to read; the /proc parser is exercised by the target package's Linux test and by the box runner, and the nils-as-misses split is exercised by the load package's coverage tests.

## Box run (2026-07-11, aki 7688c00, aki-bench dd86538)

Gate box GamingPC, i9-13900K, 56 GiB RAM, WSL2 kernel 6.18.33.2, NVMe-backed ext4; redis 8.8.0 and valkey 9.1.0, io-threads 4; server pinned to cpus 0-7, generator to 8-15.
Both arms ran, three interleaved reps per cell, aki-bench as the sole harness, coverage probe 100000 keys per rep.
An earlier pass finished the same morning at aki 9591cd7, before the tip moved; it is archived on the box under ltm-gate/prepause-9591cd7 and nothing below quotes it.
Raw artifacts (per-rep json and stdout, meta with per-rep VmRSS/VmHWM/used_memory/dbsize, 1s memory samples, run log, env) are committed under results/f3/ltm-gate/, with the reduced table in ltm-gate/summary.txt.

Medians of the three reps; vops is data-bearing throughput, peak is max VmHWM across reps, B/retr key is peak bytes over retrievable keys.
Ratios quoted for a verdict are the min across reps, not the median.

```
== equal-cap (all three at 512 MiB; rivals allkeys-lfu) ==
cell              server      vops/sec   cov%   peak MB   B/retr key   p99 us
get_uniform       aki          930,768  100.0       755          396   11,330
                  redis        662,272   20.1       594        1,547      606
                  valkey       651,796   20.1       695        1,812      530
get_zipf          aki        2,010,514  100.0       738          387    1,078
                  redis      1,247,722   20.0       598        1,571      617
                  valkey     1,275,294   20.2       709        1,843      562
set_uniform       aki          324,818   96.2       799          435   13,009
                  redis      1,052,960   20.1       532        1,391    1,434
                  valkey       925,127   20.2       925        2,402    1,631

== equal-data (rivals 4096 MB noeviction; aki keeps the 512 MiB cap) ==
cell              server      vops/sec   cov%   peak MB   B/retr key   p99 us
get_uniform       aki          925,804  100.0       682          358   11,665
                  redis      1,986,521  100.0     2,567        1,346      805
                  valkey     1,948,882  100.0     2,575        1,350      774
get_zipf          aki        1,989,225  100.0       683          358    1,088
                  redis      2,313,289  100.0     2,567        1,346      770
                  valkey     2,424,296  100.0     2,575        1,350      667
set_uniform       aki          320,017   96.0       738          403   13,320
                  redis      2,502,736  100.0     2,674        1,402      713
                  valkey     2,080,171  100.0     2,718        1,425      754
```

The 96 percent aki coverage on the set cells is not data loss.
The set workload draws keys uniformly for 20 seconds with no preload, so a server only holds the keys its own throughput managed to draw; aki at 320k ops/s makes about 6.4M draws over a 2M keyspace, and 1-exp(-6.4/2) predicts 96.1 percent, which is what the probe found and what aki's own dbsize (1.92M) confirms.
The rivals make roughly 50M draws in the same window and cover everything.
The probe misses are keys the generator never wrote to aki, not keys aki dropped.

Equal-cap reading.
This arm does not gate on throughput, per the proposal, and the coverage-and-cost row now has numbers.
At the same 512 MiB ceiling the rivals kept 20 percent of the dataset and aki kept all of it, while aki also posted higher data-bearing GET throughput: min-of-reps 1.40x redis and 1.42x valkey on uniform, 1.56x and 1.57x on zipfian.
Bytes per retrievable key is 387-435 for aki against 1,391-2,402 for the rivals, a 3.5x to 5.5x cost advantage.
On SET aki is 0.30x redis min-of-reps, which is the write path under spill, priced honestly next to the coverage it buys.
One honest flag: aki's peak resident set (738-799 MB) sits above the nominal 512 MiB cap and above redis's peak on two of three cells, because the index and connection buffers still live outside the cap.
The overshoot is now about 1.5x the cap where M0 measured 4.4x (2.27 GB), but the cap still does not bound the process.

Equal-data reading.
Coverage is equal by construction, so the throughput ratio is clean.
aki serves the full 2 GB dataset from a 682-738 MB peak against the rivals' 2,567-2,718 MB, which is 0.25x-0.27x the rivals' peak memory and 358-403 bytes per retrievable key against their 1,346-1,425.
That clears the product-pitch memory bar (under 0.5x) with room.
The throughput cost of that memory is workload-shaped: min-of-reps 0.82x valkey and 0.86x redis on zipfian GET, 0.46x-0.47x on uniform GET, and 0.12x-0.15x on SET.
The vlog read path that measured 40k ops/s in M0 now posts 926k data-bearing uniform GETs per second, about 23x better, though its p99 (11.7 ms) is still an order of magnitude above the rivals' (0.8 ms).

Verdict against the proposal.
The equal-cap bar passes: materially higher coverage (100 vs 20 percent) at lower bytes per retrievable key on every cell.
The equal-data arm was expected to fail on throughput until the read path cleared its floor; it no longer fails outright, and the observed floors are the numbers a future bar should be set from: zipfian GET at 0.8x rival throughput for 0.27x rival memory is a defensible trade today, uniform GET at 0.46x is close, and SET at 0.12x is the gap that owns the next slice.
Candidate multipliers for review, not set here: equal-data GET at or above 0.75x zipfian and 0.5x uniform, SET at or above 0.5x, peak memory at or below 0.5x, and the equal-cap peak overshoot brought under the configured cap.

## Follow-ups this run points at

The write path under spill is the largest gap: 0.12x-0.33x across both arms while GET holds 0.8x-1.6x.
The resident cap still does not bound total RSS; the overshoot shrank from 4.4x to about 1.5x but the ceiling claim wants the cap to be the ceiling.
Uniform GET p99 at 11-12 ms is vlog read latency, and it caps the uniform arm at about 0.5x even after the 23x throughput recovery.
