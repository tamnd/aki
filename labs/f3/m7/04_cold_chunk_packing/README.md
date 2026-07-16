# Lab 04: set, list, hash, and stream cold chunk packing factor

Part of issue #549, the M7 LTM milestone, lab 04, the chunk packing factor per type the spec lists as a lab knob (doc 06 sections 6.2 and 6.3). It is the per-perf-change lab the demotion packs owe: the set slices PR D2 and PR E were the first to pack real members into cold chunks, the list demote pass (PR D2 of the list cold-chunk row) packs whole chunks, the hash demote pass (PR D2 of the hash cold-chunk row) packs field-and-value pairs while keeping the fields resident, and the stream demote pass (the stream cold-chunk row, slice D) spills a whole packed block with no repack at all; the per-perf-change rule wants each memory-density claim measured, not asserted.

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

The hash model mirrors hash/cold.go `ftable.demote`: a hash is key-value, so the demote sheds only the value and keeps the field bytes resident for a zero-pread probe. An entry packs the pair as `uvarint(flen)+field+uvarint(vlen)+value` (hash/cold.go `appendEntry`), so a `w`-byte field and `w`-byte value cost `2*(uvarintLen(w)+w)` payload bytes; a chunk fills until the running payload reaches the 4 KiB target and overruns by at most one pair. The resident cost per field is the same 48-byte descriptor plus an 8-byte offset slot the set keeps (the hash directory is byte-lex over the field hash, so it needs the offset table a read seeks by), but the resident-after figure still carries the field bytes and the 20-byte field entry plus the 4-byte vector slot, because only the value leaves. That is the design crux the model has to show: the hash frees strictly less than the set for the same width, the price of keeping the probe resident.

The stream model mirrors stream/cold.go `demote`: a stream block's blob is already the master-delta wire form a cold read decodes, so the demote spills it whole through store.AppendChunk with no repack, and the cold payload is the resident blob byte-for-byte (packing factor 1.0). An entry costs the flag byte, its id delta against the block first id (a same-schema entry, so no field names, block.go `appendEntry`), and the value with its length prefix; the block seals on the 4 KiB blob budget or the 128-entry cap whichever binds first (block.go). Like the list a stream carries read order on the block handle, so a demoted block keeps no offset table, only one demote-sequence descriptor; unlike the list it keeps the block's header cell and master schema resident, so covers and the directory floor resolve a cold block without a pread. So the stream frees the blob (~96% of a byte-bound block) rather than the list's whole ~98.9%, the price of pread-free block resolution.

The constants are copied from the packages they mirror, and the test asserts the derived numbers against the spec's table, so a drift between a mirrored constant and its source surfaces as a failing invariant rather than a silently wrong verdict. `go run .` runs all four width sweeps; `-quick` shrinks them for the shared runner. `TestSetRowMeetsSpec`, `TestOverheadStaysLowAcrossWidths`, `TestMetadataInSpecBand`, `TestFreedFractionGrowsWithWidth`, `TestChunkFitsLocatorCeiling`, `TestListWholeChunkFreesNearlyAll`, `TestListChunkMetaUnderSet`, `TestListChunkFitsElemCap`, `TestListFootprintMatchesSource`, `TestHashRowPacksPairs`, `TestHashKeepsFieldResident`, `TestHashFreedGrowsWithWidth`, `TestHashChunkFitsLocatorCeiling`, `TestStreamPayloadIsBlobVerbatim`, `TestStreamWholeBlockFreesLargeMajority`, `TestStreamKeepsNoOffsetTable`, and `TestStreamBlockFitsCap` are what CI drives.

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

## Hash column

A hash is key-value, not a bag of members, so its demote cannot shed the whole record the way the set does. The field is the probe key and the value is the weight the native band exists to hold (a value outgrew the 64-byte inline cap, the collection analog of f3's string-band value-log split), so the pass keeps every field byte resident and sheds only the value into the cold chunk (hash/cold.go `ftable.demote`). That keeps HEXISTS, HKEYS, HSTRLEN, and the field-table probe at zero preads; only HGET and HVALS pread the owning chunk. The cold frame still packs the field-and-value pair, so an M8 recovery walk can rebuild the field table from the cold region alone, which means the field is duplicated: resident in the slab, and on disk in the frame.

Model output, deterministic, 4096-byte payload target, 16-byte key. The width column is the field-and-value pair width (both the same), so the pair is twice the set member at the same width:

| f+v bytes | pairs/chunk | frame overhead | resident meta B/field | vs element-per-row | resident B/field | freed to disk |
|---|---|---|---|---|---|---|
| 8 | 228 | 0.97% | 0.246 | 53x | 32 | 19% |
| 16 | 121 | 0.96% | 0.463 | 28x | 40 | 28% |
| 20 | 98 | 0.96% | 0.571 | 23x | 45 | 30% |
| 32 | 63 | 0.95% | 0.889 | 15x | 57 | 35% |
| 64 | 32 | 0.95% | 1.750 | 7x | 90 | 41% |
| 128 | 16 | 0.95% | 3.500 | 4x | 156 | 44% |
| 256 | 8 | 0.96% | 7.000 | 2x | 287 | 46% |

The freed fraction is the hash's honest trade. At 20 bytes it frees 30%, where the set at the same member width frees 55%: the set moves the whole 20-byte member to disk, the hash moves only the 20-byte value and keeps the 20-byte field plus the field entry resident for the zero-pread probe. The freed fraction still climbs with width (a wider value is a larger share of the pair, so 256 bytes frees 46% against 8 bytes' 19%), but it stays capped below the set at every width, the price of the resident probe. Frame overhead holds at about 0.96%, on the set's mark, because the pair is packed to the same 4 KiB payload. Per-field resident metadata is coarser than the set's per-member (0.571 against 0.286 at 20 bytes) only because a pair is twice a member, so a chunk holds half as many entries and the same 56-byte descriptor-plus-offset share splits over fewer of them.

## Stream column

A stream is neither a bag of members nor a ring of value slabs but an append log of packed blocks, and a block's blob is already the exact master-delta wire form a cold read decodes. So the stream demote is the simplest of the four: it does not repack at all (stream/cold.go `demote`). The pass spills the whole blob through store.AppendChunk as one cold chunk keyed by the block first id, and the cold payload is the resident blob byte-for-byte, packing factor 1.0. There is no discriminator sort, no frame rewrite, no value split; the move is a pointer handoff plus one append.

Like the list a stream carries read order on the block handle (a range seek lands on the handle, which holds the cold offset), so a demoted block keeps no offset table, only one demote-sequence descriptor. Unlike the list it keeps the block's header cell and master schema resident, so `covers` and the directory floor resolve a cold block without a pread and a range walk preads only the blocks its window actually crosses. That resident residue is the one difference from the list's near-free demote.

Model output, deterministic, 4096-byte payload target, 16-byte key. The width column is the entry value byte length, a single field:

| value bytes | entries/block | frame overhead | resident meta B/entry | vs element-per-row | resident B/entry | freed to disk |
|---|---|---|---|---|---|---|
| 8 | 128 | 3.02% | 0.375 | 35x | 1.125 | 91.2% |
| 16 | 128 | 1.84% | 0.375 | 35x | 1.125 | 94.6% |
| 20 | 128 | 1.54% | 0.375 | 35x | 1.125 | 95.5% |
| 32 | 113 | 1.17% | 0.425 | 31x | 1.274 | 96.5% |
| 64 | 60 | 1.16% | 0.800 | 16x | 2.400 | 96.6% |
| 128 | 30 | 1.19% | 1.600 | 8x | 4.800 | 96.5% |
| 256 | 15 | 1.21% | 3.200 | 4x | 9.600 | 96.4% |

The freed fraction climbs from 91.2% at an 8-byte value to about 96% once the block is byte-bound. The reason is the entry cap: a narrow value seals the block on the 128-entry ceiling well before the 4 KiB budget, so an 8-byte block holds only 1539 blob bytes and the fixed 96-byte header cell it keeps resident is a larger share; a 20-byte value already fills toward the budget and a 32-byte value is byte-bound, so the header cell shrinks to a rounding error against the shed blob. The stream frees a touch less than the list's flat 98.9% at every width, and that gap is deliberate: the list keeps only a descriptor, the stream keeps the descriptor plus the header cell and master schema so a cold block resolves and a range walk stays pread-free except on the blocks it truly crosses. Frame overhead is the tightest of the series once byte-bound (1.16% at 64 bytes), because the payload is the blob verbatim with only the cold header over it, and the resident cold metadata is 0.375 bytes per entry at the design point, 35x below element-per-row.

## Verdict

The shipped set cold chunk encoding meets or beats every packing target the spec sizes against. At the 20-byte design point it packs 196 members per 4 KiB chunk (above the ~170 target) at 0.96% frame overhead (on the ~1% target), holds resident cold metadata to 0.286 bytes per member (inside the 0.16-0.32 band, 46x under element-per-row's 13 bytes), and frees 55% of a member's resident footprint to disk on demote. The memory bar the product pitch rests on is met: a demoted set costs less RAM for the same data, and the wider the members the larger the saving. The per-type payload knob stays open for the wide-element bands (128 bytes and up), where the fixed 4 KiB payload amortizes less and a bigger payload or value-log separation is the lever, exactly as the spec defers it to each type's doc.

The list whole-chunk demote is the leaner of the two. Because it keeps no per-element resident record and no offset table, only one descriptor per chunk, it frees 98.9% of a chunk's resident footprint at every member width, a flat saving where the set's grows from a third to most of the footprint. The trade is a coarser element cap (128 frames per chunk when a narrow member hits the ceiling before the byte target), which lifts the small-member frame overhead to a few percent, still a rounding error against the resident saving. For the list the memory bar is met with room to spare: a demoted list chunk is essentially free of resident cost, holding a fraction of a byte per element.

The hash demote is the deliberate middle. It frees less than either the set or the list, 30% at the 20-byte design point against the set's 55%, because it keeps the field bytes resident so the field-table probe, HEXISTS, HKEYS, and HSTRLEN stay at zero preads. That is the trade the type wants: a hash is read by field far more than by value, so paying resident field bytes to keep the probe local while shedding the value the band exists to hold is the right cut. Frame overhead sits on the set's 0.96% mark and the freed fraction still grows with the value width (19% at 8 bytes to 46% at 256), so the wider the value the more the shed earns. The memory bar is met on the axis that matters for a hash: the value bytes leave RAM for disk while the resident probe stays fast, and a run of wide-valued fields frees a growing share as the values widen.

The stream demote is the tightest packing and the simplest move. Its block blob is already the wire form, so the demote spills it whole with no repack, at packing factor 1.0 and the leanest frame overhead of the series once byte-bound (1.16% at 64 bytes). It frees ~96% of a byte-bound block, a shade under the list's 98.9%, because it keeps the block header cell and master schema resident to resolve a cold block without a pread; a range walk then preads only the blocks its window crosses and the append path never touches a cold block at all. The memory bar is met with room to spare: a demoted stream block holds a fraction of a byte per entry resident, and because a stream's read heat is the tail and its cold weight is the old front, the never-demote-the-tail policy sheds exactly the bytes a stream stops reading while keeping XADD and XREAD $ zero-pread.
