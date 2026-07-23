# bigcollection: cardinality flatness and the chunk-size verdict

## Question

Doc 08 promises that collection cardinality never touches the cold point-read bill: HGET on a 10^8-element hash costs the same one GET of one block as on a 10^3-element one, because the directory does the finding in RAM.
This lab sweeps five decades of cardinality and the 4-32 KiB chunk band, and pins the set-algebra streaming claim, before the chunk-codec slice bakes the chunk target.

## Method

Same disclosed models as the typepoint lab: real store chunk frames on the counting sim, lab-local element packing, directory at the as-built 24 B per entry.
Corpora build from a sorted (disc, index) table without holding element blobs, so cardinality goes to 10^8 in local RAM.
The hash sweep runs 10^3 to 10^7 (15 B fields, 64 B values); the 10^8 decade rides the set corpus, which doc 08 defines as a valueless hash with identical planning.
The chunk-size sweep holds a million-element hash fixed and moves the target across 4, 8, 16, 32 KiB.
SINTER streams a million-member set against a two-million-member superset: the transport unit is the doc 05 coalesced 16 MiB range, but the merge decodes and holds one 128 KiB block per operand at a time, which is the doc 08 working-set claim; the lab reports both the merge window and the transport peak the sim forces by returning whole buffers.

## Prediction (PRED-OBS1-O2A-BIGCOLL, filed before the scored run)

1. GETs per op is exactly 1.0000 at every cardinality from 10^3 to 10^8 with every element found; bytes per op is within 1% of one 128 KiB block once the object exceeds 16 MiB, with smaller objects clipping below (the whole-object fetch is smaller than a block at 10^3).
2. Directory share per element at 16 KiB chunks is flat in cardinality: 0.12 to 0.15 B at 10^3 (quantization reads high), 0.125 to 0.135 from 10^4 through 10^7 for the hash, 0.030 to 0.033 for the 10^8 set.
3. Scan requests equal ceil(object bytes / 16 MiB) exactly at every row.
4. The chunk-size sweep moves only the directory share, halving per doubling (0.48-0.52 / 0.24-0.27 / 0.125-0.135 / 0.062-0.068), while GETs per op stays 1.0000 across the band.
5. SINTER: matches exactly the smaller set's cardinality, requests exactly the ceil identity summed over both operands, merge window at most 384 KiB (two blocks plus their decoded discs), transport peak two ranges at most 32 MiB.

Kill line: any point cell off 1.0000 GETs per op, or a directory share that grows with cardinality, breaks the ledger's flatness story and stops the slice order.

## Calibration disclosure

A -quick run at 10^5 hash and 10^6 set elements executed during development after the bands were derived from the entry arithmetic; it matched them (1.0000 everywhere, share 0.127 at 16 KiB, sinter window 341 KiB, requests on the identity).

## Run

    ./run.sh

## Results

Pending.
