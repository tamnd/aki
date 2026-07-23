# streamcatchup

## Question

Doc 08 section 7 claims catch-up consumers on cold streams are bandwidth-bound, not request-bound: chunks are dense ID-range runs, XREAD from an old ID plans cold ranges with readahead, and XRANGE bills the ledger's ceil(bytes / 16 MiB) row.
The lab spec asks for consumer catch-up from ID 0 on a cold 10M-entry stream, gating bandwidth-bound behavior and readahead effectiveness, plus a million-entry PEL fold and reclaim.
Where is the XREAD batch planning knee, and what does readahead buy over per-batch block fetches?

## Method

Entries carry (ms, seq) IDs, four per millisecond, with 70 B packed bodies; the projection packs them in ID order into real store chunk frames (16 KiB payload target) aligned to 128 KiB blocks on a counting sim, disc = 16-byte big-endian (ms, seq).
Cells per decade (10^5, 10^6, 10^7 entries):

- catchup_readahead: XREAD from 0 walks the whole cold span in coalesced 16 MiB GETs regardless of COUNT, then every entry is verified byte-exact in ID order.
- perblock_cN for COUNT in 10, 100, 1000, 8000 over a 100k-entry span: the counter-arm with no readahead and no cross-batch cache (the block cache is a deferred doc 05 follow-up), so each batch fetches the block-aligned span covering its own entries and adjacent batches re-pay shared boundary blocks.
- xrange_k100 and xrange_k10000: a k-entry window from the stream's middle, covering chunk span in coalesced GETs.
- amp_ratio: framed object bytes over entry payload bytes, the block-padding term.

Then a 1M-entry PEL folds as its own chunk kind under the stream key (disc = entry ID, payload = consumer, delivery count, last delivery ms, the doc 08 consumer-group shape), and reclaim acks everything: a chunk whose whole ID range is acked drops by manifest without a read, the doc 06 free case.

## Envelope disclosure

Stream fold emission does not exist yet; the O1b stream entry emission PR was write-log durability op frames, so this is a model lab in the zsetdual stance: the chunk frames are the real store codec (store.AppendRunChunk on kindStream 0x05, the engine's as-built kind byte), while entry packing, the directory, and the range planning are lab-local models the stream slice replaces with the landed planes.
The PEL sub-kind flag is lab-local (the hexpire pattern).
The per-block counter-arm bills one range GET per contiguous block span per batch, which is generous to that arm; a per-block-GET implementation would bill more requests, not fewer.

## Prediction (PRED-OBS1-O2B-STREAMCATCHUP)

Filed before the scored run.

1. amp_ratio lands in 1.10 to 1.20 at every decade, the block-padding term the zsetdual lab measured at 14 percent.
2. catchup_readahead takes exactly ceil(object bytes / 16 MiB) GETs at every decade (the printed expect value), with every entry byte-exact in ID order; at 10^7 that is roughly 48 GETs for 10M entries, at least 200k entries per request, which is bandwidth-bound.
3. The knee sits at the block capacity (roughly 1900 entries of this shape per 128 KiB block): over the 100k span, perblock_c10 takes exactly 10,000 GETs with a byte bill 120x to 250x the span payload, c100 takes 1000 GETs at 10x to 30x, c1000 takes 100 GETs at 2x to 4x, and c8000 takes 13 GETs at 1.2x to 1.8x, converging on the bandwidth-bound bill only past the knee.
4. xrange_k100 and xrange_k10000 take exactly 1 GET at every decade (both windows fit one 16 MiB coalesce), entries exact.
5. The 1M PEL folds at 30 to 40 B per entry, totals at most 6 percent of the 10M stream's bytes, and reclaim drops every chunk whole with residual 0.

Kill line: a readahead GET count off the exact ceil, any decode or order mismatch (the lab dies on one), a knee inversion (c8000 byte bill above 2x bandwidth-bound or c10 below 50x), or a PEL residual after full ack.

## Calibration disclosure

A quick 50k-entry configuration shaped the harness before this prediction was filed: amp 1.1375, readahead 1 GET as expected, the c10/c1000/c8000 byte bills at roughly 189x/3.0x/1.4x of the span payload over a 20k span, xrange cells at 1 GET, PEL at 36.26 B per entry with residual 0.
The 10^5 to 10^7 scored decades had not been run when the bands above were set.

## Run

```
./run.sh
```

## Results

(scored run pending)

## Verdict

(pending)
