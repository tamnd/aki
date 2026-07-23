# xpel: PEL segment size and pending-surface latency

Lab 03 for milestone T6 (spec 2064/sqlo1 doc 10, tracking issue tamnd/aki#723).
Prices the kind 5 PEL segment cut thresholds and the fence shape before the PEL segments slice bakes them.

## What it models

The doc 10 PEL shape resident, no store underneath: entries of { varint dms, varint dseq, consumer_idx u8, flags u8, delivery_count u32, delivery_time u64 } in ID order inside segments behind the group record's fence of 28 B entries.
The WAL column is modeled arithmetic under W2 and W4: a delivery batch bills the amended tail segment plus the fence edit, an ACK batch bills each touched segment or a tombstone plus the fence edit, a claim bills the touched segments plus the group record.
Two fence shapes compete: inline (the whole fence in the group record, rebilled on every edit) and paged (a 28 B page-index entry per 4 KiB fence page in the group record, one touched page billed per edit).
An oracle test drives the model against a flat reference through random deliver, ack, and claim interleavings, roundtrips the codec, and asserts entry runs are never billed (X-I3's lab half).

## Arms

- deliver: batched XREADGROUP (>) into a growing PEL, 10^2..10^6 pending.
- ack: prefill then drain by XACK batches, delivery order and random order.
- claim: full XAUTOCLAIM sweep behind the cursor, batch 100.
- scan: XPENDING extended full-range walks with the idle filter.
- encode: real codec bytes and nanoseconds per entry against the naive no-delta baseline.

Run `./run.sh > xpel.csv` for the full sweep (about two minutes).

## Verdict (local, Apple Silicon, 2026-07-23)

Bake seg_max = 4096 encoded bytes, pcap = 1024 entries as a backstop that never binds at that byte cap, and page the fence past the inline threshold.

The inline fence does not survive large PELs.
The group record is rebilled on every delivery and ACK batch, so its fence makes the bill linear in pending: at 10^6 pending and seg_max 4096 a delivery costs 5.7 KB WAL per entry with 96 percent of it fence rebill, 22.7 KB at seg_max 1024, and the group record itself is 110 KB.
The paged fence flattens it: 680 B per delivered entry and 663 B per acked entry at 10^6 pending, within 6 percent of the 10^5 figures, and the group record stays under 1 KB.
Below about 10^4 pending inline is cheaper (286 B versus 642 B per delivery at 10^4), so the slice should keep the fence inline until it outgrows the inline budget and page after, the same threshold dance the stream fence and T2 already do.

Cap choice inside the paged shape: 2048 edges 4096 by 10 percent on the FIFO write bill (1215 versus 1343 B per entry summed over deliver plus ack at 10^6) but 4096 matches it on random-order ACK (3246 versus 3315 B at 10^5), beats it on the full-walk scan (1.45 ms versus 2.13 ms per 10^6-entry XPENDING), halves the segment count (3938 versus 7937 at 10^6, 27 versus 55 fence pages), and lines up with every other 4 KiB record in the format.
Bigger caps lose outright: 16384 pays 415 ns per FIFO ack (versus 143) and 8.2 KB per random ack because every touched ID cluster rewrites a 15 KB segment.

Latency floors for the pending-surface slice: FIFO ack 146 ns, random ack 904 ns, XAUTOCLAIM 11 ns per claimed entry at batch 100, XPENDING full walk 1.4 ns per entry, all at seg_max 4096 paged and 10^6 pending.
Batch sensitivity is the usual amortization: single-ID XACK pays the whole fence edit alone (6.3 KB paged), batch 100 pays 63 B per ID.

Codec: 16.07 B per pending entry against the 30 B naive baseline (ratio 0.54), 4 ns per entry, confirming the doc 10 16-20 B claim.

CSV columns: mix, segmax, pcap, pending, batch, order, fence, workload, ops, ns_op, frames_op, walB_op, then per-workload extras (deliver/ack: fence share of WAL bytes, cuts or rewrites per 1000 ops; shape: entries per segment, segments, group record bytes; scan: ns per call, segments; encode: bytes per entry, naive bytes, ratio, segments).
