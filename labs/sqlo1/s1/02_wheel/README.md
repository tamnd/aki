# Lab 02: timer-wheel churn strategy and level width

Part of milestone S1 (tamnd/aki#711, spec 2064/sqlo1 doc 11 section 3.1).
This is the lab the timer-wheel slice depends on: doc 11 fixes 3 levels of 256 slots and 8 bytes per volatile resident key, and leaves open what an EXPIRE rewrite does with the old bucket entry.

## Question

Under heavy EXPIRE rewrite traffic (GT/LT flag churn) at 10M volatile resident keys, what does a rewrite do with the stale entry, and does the level width matter?
Scanning the old bucket to remove the entry is analytically dead and not priced: upper-level buckets hold O(keys/width) entries (tens of thousands at 10M keys), so scan-eager EXPIRE is O(bucket) and would not terminate here.
The two viable designs are lazy invalidation (leave the stale entry behind; the reaper compares each entry's filed expiry against the authoritative header expiry when its bucket comes due and drops mismatches; costs entry bloat proportional to churn) and eager removal via a per-key backpointer (O(1) swap-delete; costs 8 more bytes per volatile key, a dearer rewrite, and a backpointer write on every entry a cascade moves).

## Method

In-process, no server, no engine import, the lab-local model the labs rule requires.
Entries are 8 bytes (key id plus filed expiry tick), matching the doc 11 budget; the authoritative expiry lives in a flat array standing in for the hot header's expireLo.
Load N volatile keys with expiries uniform over a common horizon of 2^18 ticks (all widths file the same TTL population, so cascade traffic is comparable), churn with EXPIRE rewrites on uniformly chosen keys (half GT extends toward the horizon, half LT shortens toward now), then drain ticks until every key has reaped, panicking if any key leaks past 4x the horizon.
Tick size never appears in the sweep because the wheel operates in ticks: a smaller tick trades horizon for precision at identical per-op cost, so width and churn strategy are the real cost movers.
Divergences from the real wheel: keys are dense int32 ids instead of header slots, the churn phase runs at a frozen clock instead of interleaved with reaping, and there is no overflow list because every expiry sits under the horizon by construction.

`go run .` prints the full sweep; `-quick` shrinks it for the shared runner. The tests pin the correctness and structural verdicts in a small configuration for CI.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-15. 10M volatile keys, 4M EXPIRE rewrites, horizon 262144 ticks.

Sweep A, churn strategy (width 256, 3 levels):

| strategy | churn ns/op | entries after churn | stale frac | drain ms | cascade moves | max batch |
|---|---|---|---|---|---|---|
| lazy | 54 | 13999734 | 0.286 | 228 | 17187242 | 176 |
| eager-backptr | 89 | 10000000 | 0.000 | 280 | 17187237 | 176 |

Sweep B, level width (lazy, 3 levels, same key and TTL population):

| width | churn ns/op | drain ms | cascade moves | max batch |
|---|---|---|---|---|
| 64 | 47 | 279 | 19434672 | 176 |
| 128 | 50 | 232 | 19010557 | 176 |
| 256 | 52 | 233 | 17187242 | 176 |
| 512 | 51 | 204 | 9939702 | 176 |

## Verdict

- Lazy invalidation wins and the entry stays 8 bytes with no backpointer. The rewrite itself is 1.6x cheaper (54 vs 89 ns/op), because the backpointered design pays an unfile plus a backpointer store on every file, including the 17M cascade re-files, which is also why eager drains slower at this scale despite walking 29 percent fewer entries. Doc 11 section 3.1 amended pointing here.
- Lazy bloat is exactly churn-proportional and self-draining: one stale entry per effective rewrite (28.6 percent here at 0.4 rewrites per key), each dropped at its old bucket's first visit, and stale entries never ride a cascade (live cascade traffic matches the eager wheel to within a few moves in 17M). Steady-state overhead is the rewrite rate times the old filing's residual time, bounded by the horizon; a workload would need sustained multi-rewrite-per-key churn to double the wheel's RAM, and even then the overhead is 8-byte entries, cheaper than the backpointer's unconditional 8 bytes on every volatile key.
- Width is not a churn lever: rewrite cost is flat from 64 to 512. Cascade traffic only drops when a wider wheel covers the TTL population in fewer levels (512 halves the moves here because two levels reach the whole horizon), and even the tallest cascade bill amortizes to under 2 moves per key per lifetime. The doc 11 default of 256 x 3 stays: it reaches an 18-hour horizon at a 1-second tick in level granularities that keep boundary bursts small, and nothing in the sweep argues for paying 2x the slot count for width 512.
- Reap batches stay small at scale: the worst single-tick due batch was 176 keys at 10M keys over the horizon, so doc 11's bounded 64-per-pass reaper lags by at most a couple of passes on the worst tick.
