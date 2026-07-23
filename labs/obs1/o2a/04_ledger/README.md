# ledger: the O2a ledger cells measured on the landed engine

## Question

The typepoint lab priced the doc 08 section 9 ledger on a disclosed model before the slices landed.
This lab re-measures the same cells on the landed engine end to end, which is what PRED-OBS1-O2A-LEDGER claims: a real server builds the corpus over RESP, real pressure demotes and folds it to a counting sim bucket, and the reads run through the rebuilt keymap, the directory, and the cold reader, the exact plane the serving node uses.

## Method

A durability-booted server (four shards, 16 MiB arenas, 2 MiB resident cap, 5 ms flush and 20 ms fold cadences, 4 MiB segments) takes 2000 strings, one 20000 field hash, and one 20000 member set over RESP, batched under the command size cap.
The resident cap sits above the built corpus deliberately: a cap the build itself crosses demotes the collections mid-build, scattering small chunk generations across many segments, and the newest-segment floor then misses the older generations, the exact envelope violation the disclosure below names.
Ballast rounds and EXISTS boundary spins pressure the shards until the folder ledger's Places show every corpus key in a published segment, the same coldness proof the conformance suite uses.
Scoring then runs serially through Booted.Keymaps, Booted.Dirs, and Booted.Cold against the counting bucket: every string through Keymap.Lookup and ColdReader.Fetch, every hash field and set member through ColdReader.FetchField, misses through an absent fingerprint and an absent field, with the sim's request and byte counters read around each op.
Resident cells are measured, not modeled: Keymap.Bytes over live keys, Directory.Bytes per chunk times the collection's chunk count over its elements.

## Envelope disclosure

ResolveField plans within the one segment the keymap locator pins; cross-segment and cross-generation field shadowing belong to the doc 06 rewrite.
The lab stays inside that envelope deliberately: pressure arrives only after the build, so each collection demotes in one pass (the set is a single sub-table below the 262144 partition threshold, the hash is one table) and its chunks fold together into one segment, which the 4 MiB segment target holds comfortably.
The chunks are cut at the baked 16 KiB target (#1313).

## Prediction (PRED-OBS1-O2A-LEDGER, filed before the scored run)

1. string GET, HGET, and SISMEMBER each cost exactly 1.0000 GETs per serial op with 100% found, matching the ledger's one-GET point rows on the landed plane.
2. The string miss answers at the keymap for exactly 0 GETs; the field miss costs at most 1 GET and answers definitively absent.
3. Bytes per point op land between 64 and 132 KiB, one ranged GET of one 128 KiB block with tail blocks shorter.
4. Keymap share lands between 16 and 64 B per live key: 16 B per slot as built, times the open table's load factor.
5. The directory's collection share lands at or under 0.10 B per element, far inside the ledger's ~0.3 row, because the corpus packs ~12 B pairs into 16 KiB chunks; the row's ~0.3 figure is for the wider elements the typepoint lab priced.
6. The cold reader finishes with zero errors and zero unresolved locators, and misses only from the deliberate absent probes.

Kill line: any point cell above 1.0 GETs per op or below 100% found means the landed field plane does not serve the O2a surface inside its stated envelope, and the exit gate stops until it is understood.

## Calibration disclosure

Quick 200/1500/600 passes executed during harness development, after the bands were derived from the entry arithmetic but before this file was committed; the first cut used an 8 KiB cap and measured the mid-build demotion violation directly (59 chunk generations, half the fields unreachable), which produced the cap paragraph above.
One full-corpus execution also ran pre-commit while tuning the ballast economics; the bands stand as first derived and the scored run below is a fresh execution.

## Run

    ./run.sh

## Results

Pending the scored run.
