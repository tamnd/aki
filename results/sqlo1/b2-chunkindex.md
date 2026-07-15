# chunkindex verdict: 0.45 B of RAM per cold key, split policy lf85

Lab: labs/sqlo1/b2/01_chunkindex (#787), verdict run 2026-07-16, seed 1, raw rows in b2-chunkindex.csv next to this note.
The lab prices RAM and occupancy arithmetic only, so this ran locally; the machine does not touch the numbers.
Predictions were filed in b2-predictions.md before the run; outcomes and the causal story for the failed one are recorded there.

## Headline

The cold index costs 0.45 heap bytes per cold key at 10^9 keys under the recommended policy, measured on a real resident directory, not derived from the ledger.
Every policy at every scale from 10^6 to 10^9 lands between 0.40 and 0.52 B per key, 4x under the 2 B kill line and about 2x under the 1 B target, so the milestone headline number holds with room.
The exact-mode oracle (real xxhash placement, stored hashes) matches counts mode within 0.1% on buckets and within noise on the chain tail at 1e7, which is what makes the 1e9 counts rows trustworthy.

## The finding: overflow-driven fill has no steady state

The doc 03 section 8.5 policy splits only on overflow, and its fill does not settle: it sweeps from 0.82 right after a doubling wave completes to 0.97 just before the next one finishes, every doubling, forever.
The chain tail is superlinear in fill, so where a sweep point lands in the cycle decides everything: chain2 (buckets with 2 or more overflow links) is 0.0001% at the 1e8 point (fill 0.82) and 1.32% at the 1e7 point (fill 0.97), 13x over the 0.1% red line.
A growing store passes through the bad region once per doubling, so this is not a corner case, it is a phase every deployment revisits.

lf85 (split on overflow, and also whenever load factor crosses 0.85) caps the sweep at 0.85 and holds chain2 at or under 0.043% at every sweep point, for at most 0.05 B/key over the doc policy.
lf75 buys chain2 under 0.01% but costs 0.51 to 0.52 B/key and does not buy what matters more (see below), so the extra chunks are wasted.

## Single-link chains are structural, price them, do not chase them

No tested policy makes single-link chains rare: doc 29 to 35%, lf85 18 to 27%, lf75 5 to 23% depending on cycle position, all at the same nominal fill.
The chains come from bucket-size dispersion, not aggregate load: repeated coin-flip halvings leave the size distribution far wider than Poisson (p95 bucket size 65 against a mean of 36 at lf85, 1e9), so the mass past 42 stays in the tens of percent even at moderate fill.
Pushing the chained rate under 10% at all cycle points would need a load-factor target near 0.65 and about 0.59 B/key, and the probe still has to handle chains correctly, so the slice buys nothing by paying for rarity.

The read path prices the chain instead: probe the base chunk's fingerprints first and read the overflow link only on a base miss with the chain flag set.
Present keys mostly resolve in the base chunk (only entries past slot 41 live in links); absent keys on a chained bucket always pay the link read, and lf85's 18 to 27% chained rate bounds that tax.
The 3-group-read ceiling stands for unchained buckets, a chained probe is exactly 4, and chain2 at or under 0.043% bounds the 5-read tail at one probe in ~2300 at the worst cycle point.

## Riders

Split storms need no pacing: the worst 65536-insert window across all runs splits 3832 times (5.8% of inserts), each split a local CoW of two or three 512 B chunks, so the drain absorbs it as ordinary write traffic.
The 16-bit fingerprint false-hit rate measured 0.0625% on absent probes and 0.0661% on present probes against a predicted 0.0620% (mean occupied slots scanned over 65536); each false hit costs one wasted record read resolved by a full key compare, well under the noise floor of the read path.
Store-level confirmation of the false-hit rate arrives with the read-path slice, which also carries the IO-count test.

## What the cold-index slices bake

- Split policy: overflow split plus load-factor split at 0.85 (lf85). The doc 8.5 overflow-only default is rejected for crossing the chain2 red line every doubling cycle.
- Chunk geometry unchanged: 512 B chunks, 42 entries, 41 plus pointer when chained, 4 per group; the lab's arithmetic tests pin them.
- Probe order: base fingerprints first, link only on miss with the chain flag; the IO-count test asserts 3 reads unchained, 4 on a forced chain.
- No split pacing machinery.
- RAM ledger line for doc 14: 0.45 B/key measured at 10^9 (lf85), directory resident, against the 0.38 B/key ledger floor at perfect fill.
