# M0 port-lab confirmations, gate box

The last M0 lab box: rerun the ported components' microbenchmarks against their f1 originals on the gate box and confirm nothing slowed down in transit (doc 19 section 2.1, M0 checklist last lab item). Engine-only lab run, no rivals.

## Provenance

- Box: GamingPC, 13th Gen Intel Core i9-13900K (Raptor Lake, 8 P-cores + 16 E-cores, 32 CPUs with HT), 56GB RAM. The spec's port table guessed this box was Zen 4; it is not, and every number below is Raptor Lake.
- OS: WSL2 Debian on Windows 11, kernel 6.18.33.2-microsoft-standard-WSL2. WSL2 is a confound worth naming: memory latency and scheduler wakes both run through a VM layer, so absolute ns are this environment's, not bare-metal Linux's; the f1-vs-f3 ratios are same-environment and clean.
- Toolchain: go1.26.0 linux/amd64, benchstat from golang.org/x/perf.
- Tree: aki fc4a79f5bde2c7911b2772ff46ec1f4ced166a1b (origin/main), `go build ./...` and `go test ./engine/f3/... ./f3srv/...` green on the box before any run.
- Pinning: every ns-scale row ran under `taskset -c 2` (one P-core thread); labs used the masks their sections name.
- Method: `-benchtime 2s -count 5`, medians reported (benchstat wants six samples for a confidence interval; the five-run spreads were under 2 percent on every row except f1 Get at 1M, which spread 54.1-61.0ns).

One honesty note on shapes: f1 never shipped a probe, arena-append, or dtoa microbenchmark. The f3 benches in engine/f3/store/bench_test.go state they mirror the f1 harness shapes, so this run wrote the mirrors into the f1 side as uncommitted scratch: BenchmarkProbe as `s.find(k, hash(k), stringKind)` over the same filledStore(1M, 64B), BenchmarkArenaAppend as `allocRec` + `initRecord` over the same 256MB arena with reset-on-full, and BenchmarkFormatScore over an identical 16-value corpus (int fast path and grisu2 mixed) in both f1srv and f3srv/resp. Same keys, same sizes, same loop bodies; the diff between sides is the engine under test.

## P1 index probe

| bench | f1 (ns/op) | f3 (ns/op) | f3/f1 |
|---|---|---|---|
| Probe, 1M keys, hit path | 38.30 | 136.2 | 3.56x slower |
| Probe, 4k keys (cache-resident), hit path | 17.83 | 20.09 | 1.13x slower |
| Probe, 4k keys, absent key (tag-reject path) | 16.16 | 16.79 | 1.04x slower |
| Get, 1M keys | 57.43 | 140.3 | 2.44x slower |
| Set, 1M keys, in-place | 55.03 | 142.4 | 2.59x slower |

All rows 0 allocs/op on both sides.

On the recorded ~3.3ns: the port table's "probe ~3.3ns tag path" traces to note 357 (doc 04 section 2), which timed a full 7-entry bucket's tag-filtered scan alone, warm, with no hash and no key build, on the laptop. Neither engine's shipped probe benchmark measures that shape, and this run could not reproduce the 3.3ns class on either side: netting the measured 1.94ns key-build-plus-hash overhead out of the tag-reject rows leaves a ~14.2ns (f1) vs ~14.8ns (f3) resident walk. So the 3.3ns figure is a component bound from a design study, not a number the ported bench can match, and the honest confirmation is the relative one: on identical shapes the f3 tag path is within 4 percent of f1's (0.6ns, which is the dashtable directory hop), so the port did not lose the tag-path constant.

The 1M-key hit rows are a real and expected gap, not a port regression: f1's index is one flat bucket array (hash to bucket to record, two dependent misses), while f3's dashtable adds a directory-to-segment hop, a third dependent load that costs ~100ns when everything misses. That trade is priced into the design; the drain hides it with batched prefetch (F6), and lab 04's rerun on this box (below) shows the pipelined probe at 45ns/probe full read path at depth 16 against 150.7ns serial, i.e. the amortized f3 probe beats f1's serial 1M probe path once the drain shape is on. Serial one-at-a-time probing is the shape f3 explicitly does not optimize for.

Verdict: tag path match (within 4 percent); serial hit path at 1M miss against f1 (3.6x, dashtable directory indirection, by design and recovered by drain prefetch); flagged rather than hidden.

## P3 arena append

| bench | f1 (ns/op) | f3 (ns/op) | f3/f1 |
|---|---|---|---|
| ArenaAppend (bump + header + 16B key + 64B value) | 13.48 | 9.965 | 0.74x, 26 percent faster |

The subtraction did what the port table said it would: the atomic bump tail became a plain bump and the seqlock ver init dropped, and the append got faster by about the cost of the removed atomic. Verdict: beat.

## P12 dtoa

| bench | f1 (ns/op) | f3 (ns/op) | f3/f1 |
|---|---|---|---|
| FormatScore, 16-value mixed corpus | 25.36 | 25.47 | 1.004x |

The port is byte-identical code (formatScore became FormatScore) and the numbers agree inside run noise. Verdict: match.

## Lab reruns owed to this box

Full tables and per-lab verdicts live in each lab README's new gate-box section; the summaries:

- labs/f3/m0/01_shard_count with real affinity: 10.3x at 8 shards pinned to 8 P-cores, 38.3x at 32 shards on all 32 CPUs, per-shard throughput peaks exactly at shards = cores, and the oversubscribed rows' gains are the fixed-1M-keyspace working-set shrink artifact, not scheduling. Verdict: one shard per data-plane core confirmed with honest pinning; the doc 03 default freezes.
- labs/f3/m0/03_spin_park on Linux: the runtime's channel park (futex-backed underneath) costs ~45us p50 wake under WSL2, worse than the laptop's ~10-16us and nowhere near the 1-2us raw-futex assumption, with a much tighter p99 (~100-140us). Parks vanish once the window reaches the arrival gap, same curve as the laptop. Verdict: 4us adaptive window stands and earns more here than doc 03 assumed; the raw-futex crossover stays open because the harness has no futex(2) park mode.
- labs/f3/m0/04_prefetch_depth on Raptor Lake (not the Zen 4 the spec guessed): depth 16 gives 3.35x over serial at 1M keys (45.0ns/probe) and 3.4-3.5x at 10M, knee at 2-8, no pollution at 16. Verdict: depth 16 confirmed on the second microarchitecture.

## Confirmation standard

The standard was: f3 matches or beats f1 on this box, and the probe tag path sits in the ~3.3ns class the port table records.

- P1 probe: tag path match against f1 (within 4 percent on identical shapes); the ~3.3ns class does not reproduce on either engine because it was a component-level design-study number, and the serial 1M hit path is honestly slower than f1 (the priced-in directory hop, amortized away at drain depth 16). Call: match on the tag path, miss on the serial hit path, with the miss explained and owned by the drain design rather than the port.
- P3 arena append: beat (9.97 vs 13.48ns).
- P12 dtoa: match (25.47 vs 25.36ns).
