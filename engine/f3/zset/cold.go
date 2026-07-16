package zset

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/engine/f3/tier"
)

// The zset cold chunk form (spec 2064/f3/06 sections 6 and 7,
// milestones/M7-slice-cold-chunk-zset-plan.md): a zset's native band is a native
// heap structure the store's arena budget cannot see, so its cold tier is a
// demotion pass that packs many members into one cold chunk and keeps a resident
// directory over those chunks, the same shape the set form (set/cold.go) uses. It
// reuses the store's frame codec and region (store/coldchunk.go) and the
// type-agnostic directory (tier/directory.go); only the discriminator and the
// packed element shape are the zset's own.
//
// Two things are the zset's own, both from its substrate (skiplist.go):
//
//   - The score is a resident record field (natRecord.bits), so cold demotion frees
//     only the member slab bytes, never the score, and ZSCORE and ZRANK of a cold
//     member stay resident zero-pread answers. The cold payload is member bytes
//     only, the same length-prefixed encoding the set packs.
//   - The discriminator is score order, not hash order: a zset's logical order is
//     score then member, so a cold chunk covers a contiguous score band. The
//     discriminator is the sortable score key (codec.go scoreKey) as eight
//     big-endian bytes followed by the member bytes, so the directory's
//     byte-lexicographic order equals the zset's (score, member) order with no
//     per-type comparator.
//
// This file is the store-side encoding and the directory-backed reader; the demote
// pass, the record retier, and the read-path wiring land in the following slices
// (plan PRs D2, E), and the trigger composition in F.

// kindZset is the collection kind byte a zset chunk carries, a plain kind below
// frameChunk (store.AppendChunk sets the recovery bit itself), distinct from the
// set's kindSet so an M8 recovery walk dispatches a cold chunk to the right
// registry.
const kindZset byte = 0x02

// The chunk locator packs a cold record's loc within the low 31 bits: the high
// coldSlotBits index the zset's offset table (which chunk) and the low
// coldEntryBits index the member within the chunk's payload (which element). Bit
// 31 is the tierCold flag on natRecord.loc, so a resident loc (a slab offset) and
// a cold loc (a locator) never collide and the resident path masks the flag off
// without ever setting it. The entry index reads the exact packed member without
// scanning the chunk and bounds a chunk's element count so the index fits.
const (
	// tierCold is natRecord.loc's high bit: set means loc is a chunk locator, clear
	// means loc is a slab offset. A single zset's slab never approaches 2 GiB and
	// demotion only ever frees slab, so bit 31 is free for the flag and the resident
	// path masks it off without ever setting it (a zset with no cold tier is
	// byte-identical to M0). The demote pass (D2) sets it; this slice defines it and
	// the locator codec the flagged loc carries.
	tierCold = uint32(1) << 31

	coldEntryBits = 12
	maxChunkEntry = 1 << coldEntryBits // 4096 members per chunk, the entry-index ceiling
	coldEntryMask = maxChunkEntry - 1
	// The locator lives in loc bits 0..30; bit 31 is tierCold, so the slot field is
	// the 19 bits between the entry field and the flag, an offset-table ceiling of
	// ~512K chunks per zset. The demote pass (D2) enforces both ceilings when it
	// packs; this slice defines the split the locator codec reads back.
)

func packLoc(slot, entry uint32) uint32 { return slot<<coldEntryBits | entry }
func locSlot(loc uint32) uint32         { return (loc &^ tierCold) >> coldEntryBits }
func locEntry(loc uint32) uint32        { return loc & coldEntryMask }

// coldChunks is a zset's cold-tier state. The directory answers "which chunk owns
// this (score, member)" for the read paths, offs is the append-only offset table a
// record's locator slot indexes so a locator survives a directory reorder, and
// scratch is the pread buffer every cold read reuses so a cold read allocates
// nothing on the steady path. Owner-local, so nothing locks.
type coldChunks struct {
	st      *store.Store
	dir     tier.Directory
	offs    []uint64
	scratch []byte
}

// member resolves a cold record's locator to its packed member bytes: it preads the
// owning chunk into the shared scratch and returns the member at the locator's entry
// index. The bytes alias scratch and are valid only until the next cold read, the
// single-call lifetime the resident slab alias already carries. It reports false on
// a torn frame or an out-of-range locator, which a caller treats as a miss.
func (c *coldChunks) member(loc uint32) ([]byte, bool) {
	slot := int(locSlot(loc))
	if slot >= len(c.offs) {
		return nil, false
	}
	ck, buf, ok := c.st.ReadChunk(c.offs[slot], c.scratch)
	c.scratch = buf
	if !ok {
		return nil, false
	}
	return chunkEntry(ck.Payload, int(locEntry(loc)))
}

// markDirty flags the chunk owning disc as needing a repack, the resident record a
// cold remove leaves behind (the member's bytes stay packed in the frame until the
// promotion-and-repack pass reclaims them, spec 2064/f3/06 section 6.5). It is a
// directory-only mark; the frame is untouched.
func (c *coldChunks) markDirty(disc []byte) {
	if idx, ok := c.dir.Floor(disc); ok {
		_, _, status := c.dir.At(idx)
		c.dir.SetStatus(idx, status|tier.DescDirty)
	}
}

// residentBytes is the cold state's own resident footprint: the directory and the
// offset table, the two structures that grow with the cold chunk count. A demoted
// zset counts it against the slab it freed (zset.residentBytes), so the demote loop
// reads the true remaining figure. The pread scratch is left out on purpose: it is
// one bounded chunk-sized buffer that grows on a cold read, not a mutation, so
// counting it would drift the running total between command boundaries for a figure
// too small to move the demotion decision.
func (c *coldChunks) residentBytes() uint64 {
	return uint64(c.dir.Bytes()) + uint64(cap(c.offs))*8
}

// discOf renders a member's cold discriminator: the eight-byte big-endian sortable
// score key followed by the member bytes, so the directory's byte-lexicographic
// order is the zset's (score, member) order. The score key is codec.go's
// order-preserving transform, so an integer compare on the leading eight bytes
// equals the score order and the trailing member bytes break an equal-score tie the
// same way the tree does.
func discOf(scoreKey uint64, member []byte) []byte {
	d := make([]byte, 8+len(member))
	binary.BigEndian.PutUint64(d[:8], scoreKey)
	copy(d[8:], member)
	return d
}

// appendEntry packs one member into a chunk payload: an unsigned-varint length then
// the raw bytes. The entry index a locator carries is the ordinal position in this
// stream, which chunkEntry walks to. The score is not packed; it stays resident in
// the member's record.
func appendEntry(payload, m []byte) []byte {
	payload = binary.AppendUvarint(payload, uint64(len(m)))
	return append(payload, m...)
}

// chunkEntry returns the idx-th member packed in payload, or false when the payload
// is torn or shorter than idx+1 entries. It walks the length-prefixed stream; a
// chunk holds at most maxChunkEntry members, so the walk is bounded.
func chunkEntry(payload []byte, idx int) ([]byte, bool) {
	p := payload
	for i := 0; ; i++ {
		n, w := binary.Uvarint(p)
		if w <= 0 || uint64(len(p)-w) < n {
			return nil, false
		}
		p = p[w:]
		if i == idx {
			return p[:n], true
		}
		p = p[n:]
	}
}
