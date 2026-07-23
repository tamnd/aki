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

Scored on the M4 box, bigcollection.csv checked in.

Flatness holds across the full five decades: 1.0000 GETs per op with 100% found at every cardinality from 10^3 hash to 10^8 set, bytes per op at exactly 128.0 KiB once the object clears 16 MiB, with the predicted clipping below (85.1 KiB at 10^3).
Directory share at 16 KiB chunks: 0.144 at 10^3, 0.127 flat from 10^4 through 10^7, 0.031 for the 10^8 set, all inside their bands; cardinality never moves it.
Scan requests sat exactly on the ceil identity at every row, up to 143 requests over the 2.2 GiB set object.
The chunk sweep moved only the share, 0.500 / 0.253 / 0.127 / 0.064, halving per doubling as predicted, with GETs per op pinned at 1.0000 across the band.
SINTER found exactly the million common members in 5 requests (the identity over both operands), merge window 341 KiB against the 384 KiB two-blocks-plus-discs cap, transport peak the two 16 MiB ranges the sim forces.

## Verdict

PRED-OBS1-O2A-BIGCOLL: HIT, all five bands.
Cardinality is not a cost axis on the cold point path, the chunk target trades only directory RAM, and set algebra streams at the doc 08 working set.
