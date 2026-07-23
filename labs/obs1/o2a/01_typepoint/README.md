# typepoint: the O2a ledger cells measured before the slices bake them

## Question

Doc 08 section 9 claims each cold point op on the O2a types costs exactly one GET of one block, HGETALL is a coalesced scan at ceil(bytes / 16 MiB), a definitive miss costs nothing, and the directory's resident share is about 0.3 B per element.
This lab measures every one of those cells before the chunk-codec, strings, hashes, and sets slices bake the formats.

## Method

The chunk and record frames are the real store codec (AppendRunChunk, AppendRecordFrame) and every read is a real ranged GET against the counting sim, so the request and byte columns are measured.
One million elements per corpus: a hash of 15 B fields with 64 B values under one collection key, a set of the same members, and a million 64 B strings packed as whole-record run chunks in fingerprint order.
Chunks cut at 16 KiB and never span 128 KiB blocks; the directory is a RAM model at the as-built 24 B per chunk entry, the string keymap a RAM map at the as-built 16 B.
Point reads plan through directory binary search (or the keymap for strings), fetch the one covering block, parse the real chunk frame, and must find their element.
The element packing inside a collection chunk payload is lab-local; the hash and set slices bake the real one and re-run this lab if the widths move.

## Prediction (PRED-OBS1-O2A-TYPEPOINT, filed before the scored run)

1. string GET, HGET, and SISMEMBER each cost exactly 1.0 GETs per op with bytes per op within 2% of one 128 KiB block, and find their element 100% of the time.
2. The definitive miss costs exactly 0 GETs and 0 bytes.
3. HGETALL over the million-field hash costs exactly ceil(object bytes / 16 MiB) requests, 6 at this corpus.
4. Directory share per element at 24 B entries and 88 B packed elements: 0.45 to 0.55 at 4 KiB chunks, 0.22 to 0.28 at 8, 0.11 to 0.14 at 16, 0.055 to 0.07 at 32; so the ledger's ~0.3 row holds from 8 KiB chunks up and the 4 KiB floor breaks it.

Kill line: any point cell above 1.0 GETs per op means the chunk-boundary rule or the planner model is wrong and the slice order stops until it is understood.

## Calibration disclosure

A -quick run at 65k elements executed during development, after the bands were derived from the entry arithmetic but before this file was committed; it matched every band (1.0000 GETs per op everywhere, dirshare 0.522 / 0.264 / 0.133 / 0.067).

## Run

    ./run.sh

## Results

Pending.
