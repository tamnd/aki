# Lab 04: set and list cold chunk packing factor

Part of issue #549, the M7 LTM milestone, lab 04, the chunk packing factor per type the spec lists as a lab knob (doc 06 sections 6.2 and 6.3). It is the per-perf-change lab the demotion packs owe: the set slices PR D2 and PR E were the first to pack real members into cold chunks, and the list demote pass (PR D2 of the list cold-chunk row) packs whole chunks; the per-perf-change rule wants each memory-density claim measured, not asserted.

## Question

A set that outgrows its inline bands is a native heap structure the arena budget cannot see, so its cold tier is not the whole-record migrator but a demotion pass that packs many members into one cold chunk and keeps a resident directory over those chunks (set/cold.go). The spec sizes the win two ways and this lab confirms both against the shipped encoding:

- **Packing factor**: how many members a 4 KiB payload holds, and what the frame overhead comes to. Section 6.2 targets ~170 members per chunk at a 20-byte member with ~1% frame overhead.
- **Resident cold metadata per element**: what stays in RAM per cold member once the bytes are on disk. Section 6.3 targets 0.16 to 0.32 bytes per element, against the ~13 bytes per element f1's shipped element-per-row cold layout paid (section 6.1), the cost the chunk form exists to amortize away.

The third figure is the product pitch: the retier-free demote keeps the record and its draw-vector slot resident and moves only the member's slab bytes to the chunk on disk, so the resident footprint per member drops by the slab fraction. The lab reports that freed fraction across member widths.

## Method

In-process, no server, no wire, no engine import, the lab-local model the other f3 labs use. The model mirrors the shipped encoding exactly rather than approximating it, so its numbers are the code's numbers:

- a member packs as an unsigned-varint length then its raw bytes (set/cold.go `appendEntry`), so a `w`-byte member costs `uvarintLen(w)+w` payload bytes;
- a chunk fills until the running payload reaches the 4 KiB byte target and overruns it by at most one member, because the flush check runs after the append (set/cold.go `htable.demote`);
- the on-disk frame adds a fixed 16-byte header plus the collection key and the 8-byte first discriminator (store/chunkframe.go), none of it resident;
- resident cold metadata per chunk is one 48-byte directory descriptor plus an 8-byte offset-table slot (set/cold.go `coldChunks.residentBytes`, tier/directory.go `descBytes`);
- a demoted record stays resident (12-byte record plus a 4-byte draw-vector slot) and only its slab bytes leave, so resident-per-member falls from `record + vec + slab` to `record + vec + directory share`.

The list model mirrors list/cold.go `demote` the same way: a `w`-byte frame packs as `uvarintLen(w)+w` payload bytes (the same frame the resident blob stores), a chunk seals on the 4 KiB blob budget or the 128-element cap whichever binds first (native.go `canAppendTail`), the cold frame carries the same 16-byte header plus key plus an 8-byte demote sequence, and the only resident cost a demoted chunk leaves is a 48-byte directory descriptor (list/cold.go `listCold.residentBytes`), with no offset table and no per-element record. The `listFootprint` constant mirrors native.go `chunkFootprint`, and a test pins it against the source derivation.

The constants are copied from the packages they mirror, and the test asserts the derived numbers against the spec's table, so a drift between a mirrored constant and its source surfaces as a failing invariant rather than a silently wrong verdict. `go run .` runs both width sweeps; `-quick` shrinks them for the shared runner. `TestSetRowMeetsSpec`, `TestOverheadStaysLowAcrossWidths`, `TestMetadataInSpecBand`, `TestFreedFractionGrowsWithWidth`, `TestChunkFitsLocatorCeiling`, `TestListWholeChunkFreesNearlyAll`, `TestListChunkMetaUnderSet`, `TestListChunkFitsElemCap`, and `TestListFootprintMatchesSource` are what CI drives.

## Results

Model output, deterministic, 4096-byte payload target, 16-byte key:

| member bytes | members/chunk | frame overhead | resident meta B/member | vs element-per-row | resident B/member | freed to disk |
|---|---|---|---|---|---|---|
| 8 | 456 | 0.97% | 0.123 | 106x | 16 | 33% |
| 16 | 241 | 0.97% | 0.232 | 56x | 16 | 49% |
| 20 | 196 | 0.96% | 0.286 | 46x | 16 | 55% |
| 32 | 125 | 0.96% | 0.448 | 29x | 16 | 66% |
| 64 | 64 | 0.95% | 0.875 | 15x | 17 | 79% |
| 128 | 32 | 0.95% | 1.750 | 7x | 18 | 88% |
| 256 | 16 | 0.96% | 3.500 | 4x | 20 | 93% |

The 20-byte set row packs 196 members per 4 KiB chunk, above section 6.2's ~170 target: the shipped uvarint length prefix (one byte for a 20-byte member) is leaner than the listpack-class per-element overhead the spec estimate assumed, so the chunk holds more, not fewer. Frame overhead holds at about 0.96% across the whole sweep, on the spec's ~1% mark, because the fixed 40-byte header-plus-key-plus-discriminator amortizes over a full 4 KiB payload at every width.

Resident cold metadata at 20 bytes is 0.286 bytes per member, inside the spec's 0.16-to-0.32 band and 46x below element-per-row's 13 bytes. The amortization narrows as members widen (a 256-byte member packs only 16 per chunk, so its directory share climbs to 3.5 bytes), which is the per-type knob the spec flags: the fixed 4 KiB payload amortizes best on small elements, and the widest bands are where a larger payload or value-log separation would earn its keep, a decision each type's doc inherits.

The freed fraction is the memory pitch, and it grows with the member width because the retained record and vector slot are fixed 16 bytes while the freed slab grows: a 20-byte member frees 55% of its resident footprint to disk (36 bytes down to 16 resident), and a 256-byte member frees 93%.

## List column

A list is not a set of members but a ring of chunks, and its demote pass sheds a whole interior chunk at once (list/cold.go `demote`): the chunk's live frames are already contiguous and in order, so the pass copies them straight into one cold frame keyed by the per-list demote sequence, then releases the whole blob and directory. Two things fall away that the set keeps. There is no per-element resident record: a list carries position by layout, not per-member records, so nothing per element stays behind. And there is no offset table: a read rides the ring handle's cold offset, not a directory slot, so the directory keeps one descriptor per chunk and nothing per element. The only resident cost a demoted list chunk leaves is its share of that 48-byte descriptor.

The list packing has one difference from the set. A list chunk seals on whichever binds first, the 4 KiB blob budget or the 128-element cap (native.go `canAppendTail`), so a narrow member hits the element cap well before the byte target and packs 128 frames, not the 456 the byte-bound set fits. That lifts the list frame overhead for small members (a 40-byte header over a 1152-byte payload is 3.4%, not the set's ~1%) and it makes the per-member directory share coarser, but neither is the headline.

Model output, deterministic, 4096-byte payload target, 16-byte key:

| member bytes | frames/chunk | frame overhead | resident meta B/member | resident B/member | freed to disk |
|---|---|---|---|---|---|
| 8 | 128 | 3.36% | 0.375 | 0.375 | 98.9% |
| 16 | 128 | 1.81% | 0.375 | 0.375 | 98.9% |
| 20 | 128 | 1.47% | 0.375 | 0.375 | 98.9% |
| 32 | 124 | 0.97% | 0.387 | 0.387 | 98.9% |
| 64 | 63 | 0.97% | 0.762 | 0.762 | 98.9% |
| 128 | 31 | 0.98% | 1.548 | 1.548 | 98.9% |
| 256 | 15 | 1.02% | 3.200 | 3.200 | 98.9% |

The freed fraction is 98.9% at every width, a flat line where the set's climbs from 33% to 93%. The reason is structural: a list demote keeps no per-element resident state, so the resident cost after a demote is only the descriptor's 1/members share, and the freed footprint is the chunk's 1/members share, so their ratio is a constant `(4352 - 48) / 4352` independent of the member width. Where the set frees the slab but keeps the record and the draw-vector slot resident, the list frees the whole chunk and keeps a descriptor. Per chunk the list also keeps less resident metadata than the set (48 bytes, the descriptor alone, against the set's 56 bytes of descriptor plus offset slot), because the offset table is gone.

## Verdict

The shipped set cold chunk encoding meets or beats every packing target the spec sizes against. At the 20-byte design point it packs 196 members per 4 KiB chunk (above the ~170 target) at 0.96% frame overhead (on the ~1% target), holds resident cold metadata to 0.286 bytes per member (inside the 0.16-0.32 band, 46x under element-per-row's 13 bytes), and frees 55% of a member's resident footprint to disk on demote. The memory bar the product pitch rests on is met: a demoted set costs less RAM for the same data, and the wider the members the larger the saving. The per-type payload knob stays open for the wide-element bands (128 bytes and up), where the fixed 4 KiB payload amortizes less and a bigger payload or value-log separation is the lever, exactly as the spec defers it to each type's doc.

The list whole-chunk demote is the leaner of the two. Because it keeps no per-element resident record and no offset table, only one descriptor per chunk, it frees 98.9% of a chunk's resident footprint at every member width, a flat saving where the set's grows from a third to most of the footprint. The trade is a coarser element cap (128 frames per chunk when a narrow member hits the ceiling before the byte target), which lifts the small-member frame overhead to a few percent, still a rounding error against the resident saving. For the list the memory bar is met with room to spare: a demoted list chunk is essentially free of resident cost, holding a fraction of a byte per element.
