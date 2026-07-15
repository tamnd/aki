# Lab 01: hot-tier promotion probability, sample size, and timestamp count

Part of milestone S1 (tamnd/aki#711, spec 2064/sqlo1 doc 04 sections 4, 8, and the section 16 config table).
This is the lab the WATT-lite eviction slice depends on: doc 04 names three defaults as lab verdicts, and this lab prices them before any slice bakes them in.

## Question

Doc 04 defaults the cold-read promotion probability to 0.5 (the 2-Tree validated number), the eviction sample size K to 64, and the header to two access timestamps per class (half of LeanStore's WATT, because header bytes are the enemy and a third pair costs 8 bytes on every record).
Which of those survive contact with traces, and does the WATT-lite scoring earn its 16 header bytes over a one-bit clock at all?

## Method

In-process, no server, no wire, no engine import, the lab-local model the labs rule requires.
The hot tier is a dense slot array behind a key-to-slot map, the same shape as the real header table, with coarse ticks (one per 1024 ops) standing in for the 1-second clock.
Eviction samples K resident slots uniformly and drops the lowest-worth 10 percent of the sample, where worth is the WATT access-rate estimate n_stamps/(age of oldest stamp) with writes weighed 2x; the baseline is a plain clock with one ref bit and a second-chance hand.
Cold reads promote with probability D; a hit in the ghost ring (1/16 of capacity, evicted keys' timestamps) promotes always and restores the stamps; writes always enter the tier because the write path makes any state dirty.
Traces: zipfian point ops over 1M keys against a 64k tier, and the same with periodic 128k-key sequential scan bursts, each in a 10 percent-write and a read-only arm, because the write path is a second door into the tier and a D verdict taken on one mix alone would overfit it.
The reported hit ratio counts point reads only.
Divergence from the real tier: every entry is evictable here, where the real tier pins dirty records until drain; that spares the lab a drain scheduler and biases no policy over another.

`go run .` prints the full sweep; `-quick` shrinks it for the shared runner. The tests pin the qualitative verdicts in quick configuration for CI.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-15. Capacity 65536, ghost ring 4096, tick every 1024 ops.

Sweep A, promotion probability D (watt2, K=64):

| trace | D=0 | D=0.125 | D=0.25 | D=0.5 | D=0.75 | D=1.0 |
|---|---|---|---|---|---|---|
| zipfian | 0.7137 | 0.7136 | 0.7086 | 0.6999 | 0.6938 | 0.6890 |
| zipfian-ro | 0.0000 | 0.7186 | 0.7117 | 0.6982 | 0.6899 | 0.6841 |
| scan-mix | 0.7114 | 0.6935 | 0.6756 | 0.6617 | 0.6603 | 0.6605 |
| scan-mix-ro | 0.0000 | 0.6929 | 0.6675 | 0.6463 | 0.6480 | 0.6521 |

Sweep B, sample size K (watt2, D=0.125): flat.
Every arm moves less than 0.1pp between K=16 and K=256.

Sweep C, policy at the verdict D=0.125 (K=64):

| trace | clock | watt2 | watt3 |
|---|---|---|---|
| zipfian | 0.7168 | 0.7136 | 0.7155 |
| zipfian-ro | 0.7187 | 0.7186 | 0.7203 |
| scan-mix | 0.6937 | 0.6935 | 0.6960 |
| scan-mix-ro | 0.6950 | 0.6929 | 0.6966 |

For contrast, at doc 04's original D=0.5 the policies did separate: clock 0.6483 vs watt2 0.6617 on scan-mix, a 1.3pp WATT win that vanishes once D filters the pollution at the door.

## Verdict

- The promotion probability is the load-bearing constant and 0.5 is wrong for this design: D=0.125 beats it on every arm, by 1.4pp on zipfian and 4.7pp on read-only scan-mix, and beats unconditional promotion by up to 4.1pp. The 2-Tree 0.5 was validated without a ghost ring; ours restores a genuinely re-read key with its history on the second touch, so the coin's only job is filtering one-hit wonders, and a stingier coin filters better. D=0 is not an option: on a read-only trace the coin is the only door into the tier (the write path is the other one, and a mix with writes leans on it, which is why D=0 leads the write-bearing arms). --promote-p default moves to 0.125, doc 04 sections 4 and 16 amended pointing here.
- K is flat from 16 to 256 (under 0.1pp). K=64 stays, and the flatness licenses shrinking it later if sampling ever shows up in a profile.
- The third timestamp buys at most 0.4pp here (0.7pp at D=0.5), under the bar for 8 bytes on every record. Two stamps per class stays.
- At the verdict D the eviction scoring itself is a wash: clock and watt2 sit within 0.3pp everywhere, because coarse 1-second ticks compress recency so far that a key seen once this tick scores like a key seen every tick, and the door filter does the real work. WATT-lite stays for now on robustness grounds (it was 1.3pp ahead at the D the design shipped with, and the timestamps already live in the header for drain and expiry), but the slice should treat the scoring as replaceable, not load-bearing.
