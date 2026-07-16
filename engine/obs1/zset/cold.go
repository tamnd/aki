package zset

import (
	"encoding/binary"
	"math"

	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/engine/obs1/tier"
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
	// ~512K chunks per zset. The demote pass enforces both ceilings when it packs;
	// the codec above reads the split back.
	maxColdSlot = 1 << (31 - coldEntryBits) // offset-table ceiling, ~512K chunks per zset

	// chunkByteTarget is the payload fill the demote pass packs a chunk to before
	// flushing, so a chunk amortizes its frame header and directory slot over many
	// members. A member that would overshoot still lands (the check is post-append),
	// so the target is a floor on the fill, not a hard cap on the frame.
	chunkByteTarget = 4096
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

// demote packs the coldest resident members of the native band into cold chunks,
// retiers their records to chunk locators, and marks their slab bytes dead for the
// churn rebuild to reclaim. The quantum is a contiguous rank window from the low-rank
// (coldest by the first-cut policy) end: it walks the tree in score order, gathers up
// to quantum resident members, packs them into chunks filled to the byte target in
// that same order, appends every chunk to the cold region, and only on a clean append
// of all of them commits the directory descriptors and the record retier. Packing in
// rank order makes each chunk a contiguous score band, so the directory's Floor and
// RankBefore locate a cold member by score or rank in one search. A refused append
// leaves the band fully resident (the orphan frames the append-only region holds are
// dead space the compactor reclaims), so demotion degrades to a no-op rather than a
// torn band. It returns the number of members demoted.
func (n *nativeStore) demote(st *store.Store, key []byte, quantum int) int {
	if quantum <= 0 {
		return 0
	}
	if n.cold == nil {
		n.cold = &coldChunks{st: st}
	}
	cc := n.cold

	// Gather the coldest resident members in score order, skipping any already cold
	// from an earlier quantum, up to the quantum. The walk yields records by rank and
	// never compares keys, so it preads nothing.
	type slot struct {
		ord  uint32
		bits uint64
	}
	var ents []slot
	n.tree.WalkFromRank(0, func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		if r.loc&tierCold != 0 {
			return true // already cold, keep scanning for a resident member
		}
		ents = append(ents, slot{ord: ref, bits: r.bits})
		return len(ents) < quantum
	})
	if len(ents) == 0 {
		return 0
	}

	// Pack and append every chunk first, collecting the placements; commit the
	// directory and the retier only after all appends succeed. The first member of a
	// chunk supplies the discriminator, the score key plus member bytes, so the
	// directory orders the chunks by (score, member). discOf and appendEntry both copy
	// their input, so the placements stay valid after the retier rewrites loc below.
	type placed struct {
		off  uint64
		disc []byte
		ords []uint32
	}
	var chunks []placed
	var payload []byte
	var ords []uint32
	var disc []byte
	for i, e := range ents {
		if len(chunks)+1 > maxColdSlot {
			break // offset-table ceiling: leave the rest resident for the next quantum
		}
		r := &n.recs[e.ord]
		m := n.slab[r.loc : r.loc+r.mlen]
		if len(ords) == 0 {
			disc = discOf(scoreKey(math.Float64frombits(e.bits)), m)
		}
		payload = appendEntry(payload, m)
		ords = append(ords, e.ord)
		full := len(payload) >= chunkByteTarget || len(ords) >= maxChunkEntry
		if full || i == len(ents)-1 {
			off, ok := cc.st.AppendChunk(kindZset, 0, uint16(len(ords)), key, disc, payload)
			if !ok {
				return 0 // broken region: abandon, the band stays fully resident
			}
			chunks = append(chunks, placed{off: off, disc: disc, ords: append([]uint32(nil), ords...)})
			payload = payload[:0]
			ords = ords[:0]
		}
	}

	// Commit: one directory descriptor and offset-table slot per chunk, retier every
	// packed record to its locator with the cold bit set, and mark its slab bytes dead
	// so the churn rebuild compacts them out. The record keeps its ordinal, so the
	// tree ref and the hash slot stay valid; only loc changes.
	demoted := 0
	for _, c := range chunks {
		s := uint32(len(cc.offs))
		cc.offs = append(cc.offs, c.off)
		cc.dir.Insert(c.disc, uint32(len(c.ords)), c.off)
		for j, ord := range c.ords {
			r := &n.recs[ord]
			n.deadBytes += int(r.mlen)
			r.loc = packLoc(s, uint32(j)) | tierCold
			demoted++
		}
	}
	n.maybeRebuild()
	return demoted
}

// promoteOnWrite brings a cold member's whole chunk resident when a write confirms
// the member, the bring-up of spec 2064/f3/06 sections 6.5 and 7.3. A ZADD or
// ZINCRBY that finds an existing member had to pread its chunk to confirm it (the
// hash probe's Match reads a cold member's bytes), which signals the chunk's
// neighbors are hot, so the whole chunk lands resident rather than one member at a
// time. It is a no-op when the band has demoted nothing (cold is nil), keeping a
// zset with no cold tier on the exact M0 write path (the L9 zero-delta contract).
func (n *nativeStore) promoteOnWrite(m []byte) {
	if n.cold == nil {
		return
	}
	ord, ok := n.tbl.Find(store.Hash(m), m, n)
	if !ok {
		return
	}
	if n.recs[ord].loc&tierCold != 0 {
		n.promote(ord)
	}
}

// promote brings the whole cold chunk owning the record at ord back into the native
// band, the write-path bring-up of spec 2064/f3/06 sections 6.5 and 7.3. In-place
// chunk patching is ruled out because it would make cold frames mutable, which
// recovery and compaction depend on staying immutable (section 6.5), so the whole
// chunk is re-seated at once.
//
// The retier-free record survives the round trip untouched on the same table probe
// and the same tree ref: promotion only preads the chunk once, re-seats each of its
// live members' bytes back into the slab, clears each record's cold tier bit, and
// drops the chunk's directory descriptor (its frame is now dead space the compactor
// reclaims). It walks the tree, so a member removed from the band while cold is
// skipped for free (its ordinal left the tree); its stale locator dies with its
// ordinal at the next rebuild. It reports whether the chunk was promoted, false when
// the record is not cold, its locator is out of range, the pread tore, or the
// directory and the offset table have drifted.
func (n *nativeStore) promote(ord uint32) bool {
	r := &n.recs[ord]
	if r.loc&tierCold == 0 {
		return false
	}
	cc := n.cold
	slot := int(locSlot(r.loc))
	if slot >= len(cc.offs) {
		return false
	}
	off := cc.offs[slot]
	ck, buf, ok := cc.st.ReadChunk(off, cc.scratch)
	cc.scratch = buf
	if !ok {
		return false
	}
	// Locate the chunk's descriptor by its first discriminator: chunks cover
	// disjoint score bands, so a Floor on the chunk's own first (score, member)
	// lands on it exactly. Guard on the offset matching so a drifted directory aborts
	// the promotion rather than dropping the wrong descriptor.
	idx, found := cc.dir.Floor(ck.Disc)
	if !found {
		return false
	}
	if dOff, _, _ := cc.dir.At(idx); dOff != off {
		return false
	}
	// Re-seat every live member that points into this chunk. The locator carries the
	// entry index, so the packed payload is read positionally with no score decode and
	// no table probe; the appended slab bytes copy out of the pread buffer, which the
	// slab append never aliases (a distinct buffer), so a later entry reads it intact.
	n.tree.Each(func(_ uint64, ref uint32) bool {
		rr := &n.recs[ref]
		if rr.loc&tierCold == 0 || int(locSlot(rr.loc)) != slot {
			return true
		}
		mm, ok := chunkEntry(ck.Payload, int(locEntry(rr.loc)))
		if !ok {
			return true // a torn entry stays cold; its read path still preads it
		}
		rr.loc = uint32(len(n.slab))
		n.slab = append(n.slab, mm...)
		return true
	})
	cc.dir.Remove(idx)
	return true
}
