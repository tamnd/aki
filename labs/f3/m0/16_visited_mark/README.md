# Lab 16: the visited mark's price on the resident GET path

Part of issue #542, the m0-run3 regression report.

## Question

Run 3 (results/f3/m0-run3.md) measured aki's redis-benchmark GET 8 to 12 percent under the 514d57b re-run (64B 5.71M to 5.00M, 1KiB 3.63M to 3.33M) with the rivals bit-identical, and the only aki change in the window was the hot-set residency slice (#573).
The suspect on file was the flagVisited mark: a header-byte store per GET hit that dirties a cache line a read-only GET used to leave clean, at a 1M-key footprint where every line is cold.
Two questions, then.
First, what does the mark actually cost on a resident GET, priced in isolation with the shipped check-then-set and with the feared store-on-every-hit variant?
Second, can the mark explain run 3 at all?

## What the code says before any measurement

The headline GET cells cannot execute the mark.
They run f3srv with only --arena-mib, no vlog, so ltmOn is false and the whole residency read path is dormant; on top of that, 64B and 1KiB values are the embedded band, which never reaches touchResident even with a vlog.
And touchResident has been check-then-set since the first #573 commit, so the store-per-hit variant never shipped.
The residues on the headline path are the boundary-rate MaybeDemote and ResidentOver guards, two loads per 1024-command drain.

## Setup

One cell per invocation.
1M keys of 1032B separated values (about 1GiB) filled under a 2GiB cap, so everything stays resident: no spills, no promotion, no log traffic, the mark isolated from every other residency cost.
The worker boundary hook runs verbatim at its real cadence and declines every pass, matching the headline regime.
Warm 2M reads, measure 6M.
Marks: off is TuneResidency(ResidOff), the pre-#573 read path; check is the shipped check-then-set; always is TuneMarkAlways(true), the run3 suspect.

## Results

Gate box (i9-13900K, linux/amd64, taskset -c 4), 3 reps per cell, M gets/s:

| mark | uniform | zipfian s=0.99 |
|---|---|---|
| off | 3.40 / 3.34 / 3.39 | 4.37 / 4.38 / 4.35 |
| check | 3.36 / 3.37 / 3.37 | 4.37 / 4.39 / 4.37 |
| always | 3.37 / 3.32 / 3.39 | 4.39 / 4.38 / 4.35 |

Apple M4 (darwin/arm64), 3 reps per cell, M gets/s:

| mark | uniform | zipfian s=0.99 |
|---|---|---|
| off | 2.49 / 2.35 / 2.28 | 3.79 / 3.47 / 3.61 |
| check | 2.45 / 2.31 / 2.43 | 3.61 / 3.47 / 3.44 |
| always | 2.44 / 2.29 / 2.45 | 3.62 / 3.31 / 3.60 |

The three variants tie on both machines: the box spread within a mark is under 0.6 percent and the between-mark spread sits inside it.
Even the store-on-every-hit variant is free at this footprint, because the GET already pulls the header line to read the flags and vlen, so the extra store dirties a line the value copy path owns anyway.

CI companion: BenchmarkGetResident in engine/f3/store runs the check variant's loop as a Go benchmark so a future cost shows up in ordinary benchstat runs.

## The wire numbers run 3 blamed on the mark

In-process BenchmarkGet on the box, 10 counts each side: 514d57b 137.4n plus or minus 2 percent, 19c08a4 138.6n plus or minus 0 percent, p=0.170, no difference.

Interleaved redis-benchmark A/B on the gate box with run 3's exact cell parameters (1M keys, 512 conns, P16, taskset split 0-7/8-15), three arms: f3srv built at 514d57b (old), at 19c08a4 (new), and the byte-identical binary run 3 itself ran (run3bin).
Median of 4 reps, M ops/s:

| arm | GET 64B | GET 1KiB |
|---|---|---|
| old (514d57b) | 4.696 | 3.324 |
| new (19c08a4) | 4.695 | 3.325 |
| run3bin | 4.696 | 3.324 |

Identical within 0.1 percent.
The same run3 binary measured 5.32M in one session and 4.70M in another within the hour, a 13 percent same-binary swing that brackets the whole reported delta.
The session-state artifact is visible inside one run: the first cells after the box went quiet measured 4.97 to 4.99M, then every later cell from any of the three binaries settled at 4.695 to 4.698M.
Replaying run 3's protocol shape (the A/B phase before the GET cells) moved nothing either: old and new stayed identical with and without the history.

## Residency quality check

Lab 13's frozen cells, rerun on this branch, match the frozen verdict exactly (the generators are deterministic, so policy outcomes must be and are bit-stable):

| cell | log reads/get | hit | frozen |
|---|---|---|---|
| zipfian 512MiB dk8 | 0.2162 | 78.38% | 0.216 / 78.4% |
| uniform 512MiB dk8 | 0.8298 | 17.02% | 0.830 / 17.0% |

## Verdict

There is no code regression.
The mark cannot run on the headline cells (no vlog, embedded band), the shipped check-then-set and even the never-shipped always-store variant price at zero on the resident path, and the three binaries including run 3's own are indistinguishable on run 3's own hardware and parameters.
Run 3's GET rows caught a session-state measurement artifact worth about 6 to 13 percent, direction set by which session a binary landed in; its p99 tails were 3x the re-run's, which says the same thing.
Frozen: touchResident stays check-then-set as free insurance, gate runs need same-session interleaved A/B before a delta is attributed to a commit, and this lab plus BenchmarkGetResident are the standing meters if the mark is ever suspected again.
