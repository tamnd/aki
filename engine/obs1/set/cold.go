package set

import (
	"encoding/binary"
	"sort"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/engine/obs1/tier"
)

// The set cold chunk form (spec 2064/f3/06 sections 6 and 7): a set is a native
// heap structure the store's arena budget cannot see, so its cold tier is not the
// whole-record migrator but a demotion pass that packs many members into one cold
// chunk and keeps a resident directory over those chunks. Under memory pressure
// the demote loop drives one quantum out of the native heap: the coldest sub-table
// is walked in fold-discriminator order, its members are packed into chunks
// appended to the cold region through the fold seam (store.AppendChunkFold), a
// directory descriptor is added per chunk, and every packed member's record is
// retiered in place, its slab bytes freed.
//
// The retier is the crux (doc 06 section 7.2). A demoted member's record STAYS in
// the native table, tagged cold, so the draw vector, the vslots, and the table's
// probe are all untouched: a split never moves a cold member, SPOP still draws it,
// and membership still finds it on the same probe a resident member takes. Only two
// things change on the record: the tierCold band bit goes on, and loc stops being a
// slab offset and becomes a chunk locator (which chunk, and the member's index in
// it). The member bytes leave the slab, which the pass frees, and the read paths
// (member.go Match, bytesOf) pread the owning chunk to recover the bytes.
//
// This is the set half of the per-type form doc 06 section 6.5 shares across the
// collection types: the store owns the region, the frame, and the frameChunk
// recovery bit (store/coldchunk.go); the directory is type-agnostic (tier package);
// and the set owns the discriminator (obs1.Disc of the member) and the payload
// encoding (the shared packed-pair codec with every value empty, doc 08's
// valueless hash reuse). The zset, list, and stream forms reuse all three,
// changing only the discriminator and the packed element shape.

// kindSet is the collection kind byte a set chunk carries, a plain kind below
// frameChunk (store.AppendChunk sets the recovery bit itself). An M8 recovery walk
// reads it to dispatch a cold set chunk back into the set registry.
const kindSet byte = 0x01

// The chunk locator packs a cold record's loc: the high bits index the set's
// offset table (which chunk), the low coldEntryBits index the member within the
// chunk's payload (which element). The member index lets a draw or a rank read the
// exact packed member without scanning the whole chunk, and it bounds a chunk's
// element count so the index fits.
const (
	coldEntryBits   = 12
	maxChunkEntry   = 1 << coldEntryBits // 4096 members per chunk, the entry-index ceiling
	coldEntryMask   = maxChunkEntry - 1
	maxColdSlot     = 1 << (32 - coldEntryBits) // offset-table ceiling, ~1M chunks per set
	chunkByteTarget = obs1.ChunkTargetDefault   // the doc 08 baked target (#1299), was the f3 port's 4096
)

func packLoc(slot, entry uint32) uint32 { return slot<<coldEntryBits | entry }
func locSlot(loc uint32) uint32         { return loc >> coldEntryBits }
func locEntry(loc uint32) uint32        { return loc & coldEntryMask }

// coldChunks is a set's cold-tier state, shared across a partitioned set's
// sub-tables. The directory answers "which chunk owns this discriminator" for the
// read paths a later slice adds; offs is the append-only offset table a record's
// locator slot indexes, so a locator survives a directory reorder; and scratch is
// the pread buffer every cold read reuses, so a cold read allocates nothing on the
// steady path. Owner-local, so nothing locks.
type coldChunks struct {
	st      *store.Store
	dir     tier.Directory
	offs    []uint64
	scratch []byte
}

// member resolves a cold record's locator to its packed bytes: it preads the
// owning chunk into the shared scratch and returns the member at the locator's
// entry index. The bytes alias scratch and are valid only until the next cold
// read, the single-call lifetime the resident slab alias already carries. It
// reports false on a torn frame or an out-of-range locator, which a caller treats
// as a miss.
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
	p, ok := store.PackedPairAt(ck.Payload, ck.Flags, int(ck.Count), int(locEntry(loc)))
	if !ok {
		return nil, false
	}
	return p.Field, true
}

// markDirty flags the chunk owning the member at disc as needing a repack, the
// resident record a cold remove leaves behind (the member's bytes stay packed in
// the frame until the promotion-and-repack pass reclaims them, spec 2064/f3/06
// section 6.5). It is a directory-only mark; the frame is untouched. On a
// partitioned set whose partitions demoted separately the floor can land on a
// neighbouring partition's chunk (their fold-coordinate ranges interleave), which
// mis-aims the repack hint but never a read; the repack pass re-walks anyway.
func (c *coldChunks) markDirty(disc uint64) {
	if idx, ok := c.dir.Floor(discOf(disc)); ok {
		_, _, status := c.dir.At(idx)
		c.dir.SetStatus(idx, status|tier.DescDirty)
	}
}

// residentBytes is the cold state's own resident footprint: the directory and the
// offset table, the two structures that grow with the cold chunk count. A demoted
// set counts it against the slab it freed (set.residentBytes), so the demote loop
// reads the true remaining figure. The pread scratch is left out on purpose: it is
// one bounded chunk-sized buffer that grows on a cold read, not a mutation, so
// counting it would drift the running total between command boundaries for a figure
// too small to move the demotion decision (the same reason the estimate drops the
// fixed per-set overheads, doc 06 section 6.3).
func (c *coldChunks) residentBytes() uint64 {
	return uint64(c.dir.Bytes()) + uint64(cap(c.offs))*8
}

// discOf renders a member's fold discriminator as eight big-endian bytes, so
// the directory's byte-lexicographic order is the members' fold-coordinate
// order (the ordering contract of tier/directory.go), the same bytes the
// chunk frame carries to the segment folder. The table probe and the
// partitioned band's routing stay on store.Hash; only the cold tier speaks
// the fold coordinate, so a partitioned set's per-partition demotes can
// interleave their ranges in the set-wide directory (see markDirty).
func discOf(disc uint64) []byte {
	var d [8]byte
	binary.BigEndian.PutUint64(d[:], disc)
	return d[:]
}

// memberDisc is the fold plane's shared coordinate for a member (doc 08:
// sets discriminate on the member), the same u64 the keymap fingerprint and
// the segment bloom use, which is what lets a folded set chunk be planned by
// the same directory floor a hash field uses.
func memberDisc(m []byte) uint64 { return obs1.Disc(m) }

// demote packs one quantum of the set at key into the cold region and returns the
// members demoted. It is the directly-callable core the worker's demote loop drives
// (PR F) and the retier test exercises: it creates the set's shared cold state on
// first demote, picks one sub-table, packs it, and reconciles the running total. It
// returns 0 when the key is absent, the band is inline (too small to demote), the
// chosen sub-table is already cold, or the cold region refused the append.
func (g *reg) demote(cx *shard.Ctx, key []byte) int {
	s := g.m[string(key)]
	if s == nil {
		return 0
	}
	n := s.demote(cx.St, key)
	if n > 0 {
		g.note(s)
	}
	return n
}

// demote packs one native sub-table of the set into cold chunks. The inline bands
// stay resident (an intset or a listpack is below one chunk's worth); the native
// band demotes its one table, and the partitioned band demotes the first sub-table
// that still holds resident members, a whole partition per quantum. Epoch-coldest
// partition selection is a later trigger refinement (PR F); this first pass sweeps
// partitions in index order, which drains the set one bounded partition at a time.
func (s *set) demote(st *store.Store, key []byte) int {
	if s.enc != encHashtable && s.enc != encPartitioned {
		return 0
	}
	if s.cold == nil {
		s.cold = &coldChunks{st: st}
	}
	if s.enc == encHashtable {
		return s.ht.demote(s.cold, key)
	}
	for _, h := range s.part.parts {
		if n := h.demote(s.cold, key); n > 0 {
			return n
		}
	}
	return 0
}

// demote packs this table's resident members into cold chunks, retiers their
// records, and frees the slab. It gathers every resident member in hash order,
// fills chunks to the byte target (or the entry ceiling), appends each to the cold
// region, and only on a clean append of every chunk commits the directory
// descriptors and the record retier. A refused append leaves the table fully
// resident (the orphan frames the append-only region already holds are dead space
// the compactor reclaims), so demotion degrades to a no-op rather than a torn set.
func (h *htable) demote(cc *coldChunks, key []byte) int {
	type entry struct {
		hash uint64
		ord  uint32
	}
	var ents []entry
	for _, ord := range h.vec {
		r := &h.recs[ord]
		if r.band&tierCold != 0 {
			continue // already cold from an earlier quantum
		}
		ents = append(ents, entry{memberDisc(h.slab[r.loc : r.loc+uint32(r.mlen)]), ord})
	}
	if len(ents) == 0 {
		return 0
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].hash < ents[j].hash })

	// Pack and append every chunk first, collecting the placements; only commit the
	// directory and the retier once all appends succeed. firstDisc is the
	// discriminator of the chunk currently filling. Members pack through the
	// shared codec valueless (doc 08's valueless hash reuse: an empty value and
	// no expiry per entry), and the append goes through the fold seam, so the
	// same bytes the local directory keys reach the segment folder and a folded
	// set chunk decodes under the one packed-pair rule every planner reads.
	type placed struct {
		off  uint64
		disc []byte
		ords []uint32
	}
	var chunks []placed
	var pk store.ChunkPacker
	var ords []uint32
	var firstDisc uint64
	for i, e := range ents {
		if len(chunks)+1 > maxColdSlot {
			break // offset-table ceiling: leave the rest resident for the next quantum
		}
		if len(ords) == 0 {
			firstDisc = e.hash
		}
		r := &h.recs[e.ord]
		pk.Add(h.slab[r.loc:r.loc+uint32(r.mlen)], nil, 0)
		ords = append(ords, e.ord)
		full := pk.Bytes() >= chunkByteTarget || pk.Count() >= maxChunkEntry
		if full || i == len(ents)-1 {
			payload, flags := pk.Finish()
			off, ok := cc.st.AppendChunkFold(kindSet, flags, uint16(pk.Count()), key, discOf(firstDisc), payload)
			if !ok {
				return 0 // broken region: abandon, the table stays fully resident
			}
			chunks = append(chunks, placed{off: off, disc: discOf(firstDisc), ords: append([]uint32(nil), ords...)})
			pk.Reset()
			ords = ords[:0]
		}
	}

	// Commit: register the shared cold handle, add a descriptor and offset-table slot
	// per chunk, and retier every packed record to its locator. The slab bytes are
	// now unreachable, so drop the whole slab and reset its dead count.
	h.cold = cc
	n := 0
	for _, c := range chunks {
		slot := uint32(len(cc.offs))
		cc.offs = append(cc.offs, c.off)
		cc.dir.Insert(c.disc, uint32(len(c.ords)), c.off)
		for j, ord := range c.ords {
			r := &h.recs[ord]
			r.band |= tierCold
			r.loc = packLoc(slot, uint32(j))
			n++
		}
	}
	h.slab = nil
	h.dead = 0
	return n
}

// promote brings the whole cold chunk owning the record at ord back into the
// native structure, the write-path bring-up of spec 2064/f3/06 sections 6.5 and
// 7.3. A write that had to read a cold chunk to confirm a member (a re-added
// member whose record is cold) signals its neighbors are hot, so the whole chunk
// lands resident rather than one member at a time; in-place chunk patching is
// ruled out because it would make cold frames mutable, which recovery and
// compaction depend on staying immutable (section 6.5).
//
// The retier-free record survives the round trip untouched on the same table
// probe: promotion only preads the chunk once, re-seats each of its live members'
// bytes back into the slab, clears each record's cold tier bit, and drops the
// chunk's directory descriptor (its frame is now dead space the compactor
// reclaims). It walks the draw vector, so a member SREM removed from the table
// while cold is skipped for free (its ordinal left the vector); its stale locator
// stays until the ordinal is reused. It reports whether the chunk was promoted,
// false when the record is not cold, its locator is out of range, the pread tore,
// or the directory and the offset table have drifted.
func (h *htable) promote(ord uint32) bool {
	r := &h.recs[ord]
	if r.band&tierCold == 0 {
		return false
	}
	cc := h.cold
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
	// Locate the chunk's descriptor by its first discriminator: chunks partition
	// the hash space with no overlap, so a Floor on the chunk's own first member
	// lands on it exactly. Guard on the offset matching so a drifted directory
	// aborts the promotion rather than dropping the wrong descriptor.
	idx, found := cc.dir.Floor(ck.Disc)
	if !found {
		return false
	}
	if dOff, _, _ := cc.dir.At(idx); dOff != off {
		return false
	}
	// Re-seat every live member that points into this chunk. The locator carries
	// the entry index, so the packed payload is read positionally with no re-hash
	// and no table probe; the appended slab bytes copy out of the pread buffer
	// before the next entry reads it.
	for _, o := range h.vec {
		rr := &h.recs[o]
		if rr.band&tierCold == 0 || int(locSlot(rr.loc)) != slot {
			continue
		}
		p, ok := store.PackedPairAt(ck.Payload, ck.Flags, int(ck.Count), int(locEntry(rr.loc)))
		if !ok {
			continue // a torn entry stays cold; its read path still preads it
		}
		rr.loc = uint32(len(h.slab))
		h.slab = append(h.slab, p.Field...)
		rr.band &^= tierCold
	}
	cc.dir.Remove(idx)
	return true
}
