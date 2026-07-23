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

Scored run (ledger.csv), 2000 strings, 20000 hash fields, 20000 set members, folded after 66 pressure rounds:

| cell | ops | GETs | GETs/op | KiB/op | found |
|---|---|---|---|---|---|
| string_get | 2000 | 2000 | 1.0000 | 2.7 | 100.0% |
| string_miss | 100 | 0 | 0.0000 | 0.0 | 100.0% |
| hget | 20000 | 20000 | 1.0000 | 102.2 | 100.0% |
| hget_miss | 100 | 100 | 1.0000 | 108.0 | 100.0% |
| sismember | 20000 | 20000 | 1.0000 | 93.0 | 100.0% |

Keymap: 175265 live keys in 4194304 B, 23.93 B per key.
Directory collection share: 29 chunks over 40000 elements, 0.1033 B per element.
Cold reader: 42100 fetches, 42100 block GETs, 0 attached, 0 unresolved, 100 misses (the deliberate absent probes), 0 errors.

## Verdict

Hit, with two disclosed edges.

Bands 1, 2, 4, and 6 hit exactly: every point cell is 1.0000 GETs per op at 100% found, the string miss answers at the keymap for zero requests, the field miss is one definitive GET, the keymap sits at 23.93 B per live key inside the 16-64 band, and the cold reader is clean.
Band 3 missed low on string_get at 2.7 KiB per op: the strings are small and ride the staged drains, so they fold into small run blocks rather than filling a 128 KiB block, and one GET of one whole block is a few KiB, not tens.
The band as written baked in a full-block assumption the corpus does not create; the ledger claim underneath, one GET of at most one block per point op, holds exactly.
The collection cells (hget at 102.2, sismember at 93.0) land inside the band because their chunks pack full blocks.
Band 5 missed by 3%: 0.1033 vs the at-or-under 0.10 line.
The directory rounds 29 chunks up from the raw byte math and the corpus is small enough for that rounding to show; the ledger's ~0.3 row holds with 3x headroom, which is what the exit gate row asks.

The kill line did not fire: no point cell above 1.0 GETs per op, none below 100% found.
The landed plane serves the O2a ledger inside its stated envelope.
