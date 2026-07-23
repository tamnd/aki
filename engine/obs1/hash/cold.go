package hash

import (
	"encoding/binary"
	"sort"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/engine/obs1/tier"
)

// The hash cold chunk form (spec 2064/f3/06 sections 6 and 7, cross-referenced to
// 10-hash-model.md). A hash is a native heap structure the store's arena budget
// cannot see, so its cold tier is a demotion pass that packs many fields into one
// cold chunk and keeps a resident directory over those chunks, the same shape the
// set, zset, and list forms take. Under memory pressure the demote loop drives one
// quantum out of the native heap: the resident records are walked in field-hash
// order, their field-and-value pairs are packed into chunks appended to the cold
// region (store.AppendChunk), a directory descriptor is added per chunk keyed by
// field hash, and every packed record's value bytes are shed from the slab.
//
// The crux is what stays resident (doc 06 section 7.2, and the key-value split f3
// already runs at the string band through the value log). A hash is a map from a
// field to a value, so the field is the probe key and the value is the weight: the
// native band exists precisely because a value breached the inline cap. So a demote
// keeps the FIELD bytes resident and sheds only the VALUE. The record stays on the
// same table probe with its field intact, so HEXISTS, the table lookup, HKEYS, and
// HSTRLEN all answer with zero preads; only the value read (HGET, HVALS) preads the
// owning chunk. Two things change on a demoted record: the tierCold band bit goes
// on, and voff stops being a slab offset and becomes a chunk locator (which chunk,
// and the field's index in it). The field's foff, flen, and vlen stay valid, so the
// probe and the value length are still resident.
//
// The chunk frame packs the field-and-value pair, not the value alone, so an M8
// recovery walk (doc 07) can rebuild the field table from the cold region with no
// resident state: the field bytes it re-seats and the field hash the directory
// orders by both live in the frame. The store owns the region, the frame, and the
// frameChunk recovery bit (store/coldchunk.go); the directory is type-agnostic
// (tier package); the hash owns the discriminator (field hash) and the payload
// encoding (length-prefixed field-value pairs in field-hash order).

// kindHash is the collection kind byte a hash chunk carries, a plain kind below
// frameChunk (store.AppendChunk sets the recovery bit itself). It follows the set
// (0x01), zset (0x02), and list (0x03) kinds; an M8 recovery walk reads it to
// dispatch a cold hash chunk back into the hash registry.
const kindHash byte = 0x04

// The chunk locator packs a cold record's voff: the high bits index the hash's
// offset table (which chunk), the low coldEntryBits index the field within the
// chunk's payload (which entry). The entry index reads the exact packed pair
// without scanning the chunk, and it bounds a chunk's field count so the index
// fits. foff is left untouched by a demote (the field stays in the slab), so only
// voff carries the locator.
const (
	coldEntryBits   = 12
	maxChunkEntry   = 1 << coldEntryBits // 4096 fields per chunk, the entry-index ceiling
	coldEntryMask   = maxChunkEntry - 1
	maxColdSlot     = 1 << (32 - coldEntryBits) // offset-table ceiling, ~1M chunks per hash
	chunkByteTarget = obs1.ChunkTargetDefault   // the doc 08 baked target (#1299), was the f3 port's 4096
)

// tierCold is the fentry.band bit marking a record whose value has been shed to a
// cold chunk. It is the only band bit today; a record with band 0 is fully
// resident, the state every record holds when the hash runs no cold tier (L9).
const tierCold = 0x01

func packLoc(slot, entry uint32) uint32 { return slot<<coldEntryBits | entry }
func locSlot(loc uint32) uint32         { return loc >> coldEntryBits }
func locEntry(loc uint32) uint32        { return loc & coldEntryMask }

// coldChunks is a hash's cold-tier state, built on the first demote. The directory
// answers "which chunk owns this field hash" for the promote path (PR E); offs is
// the append-only offset table a record's locator slot indexes, so a locator
// survives a directory reorder; and scratch is the pread buffer every cold read
// reuses, so a cold value read allocates nothing on the steady path. Owner-local,
// so nothing locks.
type coldChunks struct {
	st      *store.Store
	dir     tier.Directory
	offs    []uint64
	scratch []byte
}

// value resolves a cold record's locator to its value bytes: it preads the owning
// chunk into the shared scratch and returns the value of the pair at the locator's
// entry index. The bytes alias scratch and are valid only until the next cold read,
// the single-call lifetime the resident slab alias already carries. It reports
// false on a torn frame or an out-of-range locator, which a caller treats as a
// miss.
func (c *coldChunks) value(loc uint32) ([]byte, bool) {
	slot := int(locSlot(loc))
	if slot >= len(c.offs) {
		return nil, false
	}
	ck, buf, ok := c.st.ReadChunk(c.offs[slot], c.scratch)
	c.scratch = buf
	if !ok {
		return nil, false
	}
	p, ok := store.PackedPairAt(ck.Payload, ck.Flags, ck.Count, int(locEntry(loc)))
	return p.Value, ok
}

// pair resolves a cold record's locator to both its field and value bytes, the
// promote path's re-seat read (PR E) and the M8 recovery unit. Both slices alias
// the shared scratch, valid until the next cold read.
func (c *coldChunks) pair(loc uint32) (field, value []byte, ok bool) {
	slot := int(locSlot(loc))
	if slot >= len(c.offs) {
		return nil, nil, false
	}
	ck, buf, ok := c.st.ReadChunk(c.offs[slot], c.scratch)
	c.scratch = buf
	if !ok {
		return nil, nil, false
	}
	p, ok := store.PackedPairAt(ck.Payload, ck.Flags, ck.Count, int(locEntry(loc)))
	return p.Field, p.Value, ok
}

// markDirty flags the chunk owning the field at hash as needing a repack, the
// resident record a cold delete leaves behind (the value bytes stay packed in the
// frame until the promotion-and-repack pass reclaims them, spec 2064/f3/06 section
// 6.5). It is a directory-only mark; the frame is untouched.
func (c *coldChunks) markDirty(disc uint64) {
	if idx, ok := c.dir.Floor(discOf(disc)); ok {
		_, _, status := c.dir.At(idx)
		c.dir.SetStatus(idx, status|tier.DescDirty)
	}
}

// residentBytes is the cold state's own resident footprint: the directory and the
// offset table, the two structures that grow with the cold chunk count. A demoted
// hash counts it against the value bytes it freed (ftable.residentBytes), so the
// demote loop reads the true remaining figure. The pread scratch is left out on
// purpose: it is one bounded chunk-sized buffer that grows on a cold read, not a
// mutation, so counting it would drift the running total between command boundaries
// for a figure too small to move the demotion decision (the same reason the
// estimate drops the fixed per-hash overheads, doc 06 section 6.3).
func (c *coldChunks) residentBytes() uint64 {
	return uint64(c.dir.Bytes()) + uint64(cap(c.offs))*8
}

// discOf renders a field's fold discriminator as eight big-endian bytes, so the
// directory's byte-lexicographic order is the discriminators' numeric order (the
// ordering contract of tier/directory.go, and the same big-endian rule the fold
// plane's disc64 lifts). A hash has no partitioned band, so one directory spans
// the whole native table and the discs stay cleanly ordered.
func discOf(disc uint64) []byte {
	var d [8]byte
	binary.BigEndian.PutUint64(d[:], disc)
	return d[:]
}

// fieldDisc is the fold plane's shared coordinate for a field name
// (obs1.Disc, the keymap and bloom h1): the demoter packs and keys its
// cold directory in this order so the chunks it appends carry
// discriminators a segment planner can floor against. The table's own
// probe stays on store.Hash; only the cold tier speaks the fold
// coordinate.
func fieldDisc(field []byte) uint64 { return obs1.Disc(field) }

// demote packs the hash at key into the cold region and returns the fields whose
// values were shed. It is the directly-callable core the worker's demote loop drives
// (PR F) and the retier test exercises: it dispatches on the encoding, and reconciles
// the running total on a shed. An inline listpack hash is below one chunk's worth, so
// it never demotes; only the native band sheds. It returns 0 when the key is absent,
// the band is inline, the table holds no resident values, or the cold region refused
// the append.
func (g *reg) demote(cx *shard.Ctx, key []byte) int {
	h := g.m[string(key)]
	if h == nil {
		return 0
	}
	n := h.demote(cx.St, key)
	if n > 0 {
		g.note(h)
	}
	return n
}

// demote sheds the native band's values into cold chunks. The inline band stays
// resident; the native band demotes its one table. A hash sheds its whole table in a
// pass, the same shape the set's single native table takes, since a hash carries no
// partitioned band to sweep one piece at a time.
func (h *hash) demote(st *store.Store, key []byte) int {
	if h.enc != encHashtable {
		return 0
	}
	return h.ft.demote(st, key)
}

// hasResident reports whether the table still holds a record whose value a demote
// can move to the cold tier: a record whose value is not already cold. A hash whose
// every value has left for the cold tier keeps its field bytes resident forever (the
// probe key never leaves), so its footprint stays positive, but a demote would shed
// nothing; the trigger uses this to skip it as a victim so a fully-cold hash never
// wins the pick and stalls the loop. It early-returns on the first resident record,
// so a hash with any resident value costs one record's band check.
func (f *ftable) hasResident() bool {
	for _, ord := range f.vec {
		if f.ents[ord].band&tierCold == 0 {
			return true
		}
	}
	return false
}

// demote sheds this table's resident values into cold chunks, retiers their records,
// and rebuilds the slab to hold only the field bytes. It is the crux of the hash cold
// form: unlike the set, which packs whole members and drops the slab, a hash keeps
// its field bytes resident (the probe key) and packs the field-and-value pair while
// shedding only the value bytes. So it gathers every record whose value is still
// resident, in field-hash order, fills chunks to the byte target (or the entry
// ceiling), appends each to the cold region, and only on a clean append of every
// chunk commits the directory descriptors and the record retier. A refused append
// leaves the table fully resident (the orphan frames the append-only region already
// holds are dead space the compactor reclaims), so demotion degrades to a no-op
// rather than a torn hash.
func (f *ftable) demote(st *store.Store, key []byte) int {
	type entry struct {
		hash uint64
		ord  uint32
	}
	var ents []entry
	for _, ord := range f.vec {
		e := &f.ents[ord]
		if e.band&tierCold != 0 {
			continue // already cold from an earlier pass
		}
		ents = append(ents, entry{fieldDisc(f.slab[e.foff : e.foff+uint32(e.flen)]), ord})
	}
	if len(ents) == 0 {
		return 0
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].hash < ents[j].hash })

	if f.cold == nil {
		f.cold = &coldChunks{st: st}
	}
	cc := f.cold

	// Pack and append every chunk first, collecting the placements; only commit the
	// directory and the retier once all appends succeed. firstDisc is the
	// discriminator of the chunk currently filling. The pack runs through the
	// shared codec (store.ChunkPacker) with each field's inline expiry, so a
	// chunk with a TTL bearer leaves under ChunkFlagTTLBitmap and one with none
	// packs byte-identical to the plain form, and the append goes through the
	// fold seam: the same bytes the local directory keys reach the segment
	// folder, which is what makes this hash readable cold on the object store.
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
		if len(cc.offs)+len(chunks)+1 > maxColdSlot {
			break // offset-table ceiling: leave the rest resident for the next pass
		}
		if len(ords) == 0 {
			firstDisc = e.hash
		}
		r := &f.ents[e.ord]
		var exp uint64
		if f.exp != nil {
			exp = f.exp[e.ord]
		}
		pk.Add(f.slab[r.foff:r.foff+uint32(r.flen)], f.slab[r.voff:r.voff+r.vlen], exp)
		ords = append(ords, e.ord)
		full := pk.Bytes() >= chunkByteTarget || pk.Count() >= maxChunkEntry
		if full || i == len(ents)-1 {
			payload, flags := pk.Finish()
			off, ok := st.AppendChunkFold(kindHash, flags, uint16(pk.Count()), key, discOf(firstDisc), payload)
			if !ok {
				return 0 // broken region: abandon, the table stays fully resident
			}
			chunks = append(chunks, placed{off: off, disc: discOf(firstDisc), ords: append([]uint32(nil), ords...)})
			pk.Reset()
			ords = ords[:0]
		}
	}

	// Commit: add a descriptor and offset-table slot per chunk, and retier every
	// packed record so its value reads from the chunk. The field bytes stay resident,
	// so voff becomes the locator while foff is left for the slab rebuild below.
	n := 0
	for _, c := range chunks {
		slot := uint32(len(cc.offs))
		cc.offs = append(cc.offs, c.off)
		cc.dir.Insert(c.disc, uint32(len(c.ords)), c.off)
		for j, ord := range c.ords {
			e := &f.ents[ord]
			e.band |= tierCold
			e.voff = packLoc(slot, uint32(j))
			n++
		}
	}

	// Rebuild the slab to hold only the field bytes: the shed values are unreachable
	// now, so a fresh slab of just the fields reclaims their space and leaves the
	// probe intact. Size the fresh slab to the field bytes exactly (not the old
	// field-and-value length) so its capacity, the figure residentBytes counts,
	// reflects the shed. Walk the draw vector so every live record's foff is
	// repointed, including records already cold from an earlier pass (their fields
	// never left).
	fieldBytes := 0
	for _, ord := range f.vec {
		fieldBytes += int(f.ents[ord].flen)
	}
	packed := make([]byte, 0, fieldBytes)
	for _, ord := range f.vec {
		e := &f.ents[ord]
		foff := uint32(len(packed))
		packed = append(packed, f.slab[e.foff:e.foff+uint32(e.flen)]...)
		e.foff = foff
	}
	f.slab = packed
	f.dead = 0
	return n
}

// promote brings the chunk owning the record at ord back into the resident slab:
// one pread of the owning chunk, then every live record whose value is packed in
// that chunk has its value re-seated at the slab tail and its cold band cleared, and
// the chunk's directory descriptor is dropped. It is the confirming-write bring-up
// (spec 2064/f3/06 section 7.3): an HSET or HINCRBY that lands on a cold field
// signals the region turned hot, so the whole chunk's values return to RAM until a
// later demote finds the table cold again under pressure. The field bytes never left
// the slab, so only the value is re-seated; foff, flen, and vlen stay as they were.
// A torn pread or an out-of-range locator leaves the records cold, which a later read
// or promote retries; the caller treats a promote as best-effort and never blocks on
// it. It is a no-op on a resident record (band 0), so a confirming write to a hot
// field costs one band check.
func (f *ftable) promote(ord uint32) {
	if f.ents[ord].band&tierCold == 0 {
		return
	}
	cc := f.cold
	slot := locSlot(f.ents[ord].voff)
	if int(slot) >= len(cc.offs) {
		return
	}
	off := cc.offs[slot]
	ck, buf, ok := cc.st.ReadChunk(off, cc.scratch)
	cc.scratch = buf
	if !ok {
		return
	}
	// Re-seat every live record whose value is packed in this chunk. Walk the draw
	// vector so a record deleted while cold (its ordinal on the free list) is skipped,
	// its stale locator dying with the ordinal. The re-seated value bytes alias the
	// pread scratch, which the slab append copies, so no aliasing survives the loop.
	for _, o := range f.vec {
		r := &f.ents[o]
		if r.band&tierCold == 0 || locSlot(r.voff) != slot {
			continue
		}
		p, ok := store.PackedPairAt(ck.Payload, ck.Flags, ck.Count, int(locEntry(r.voff)))
		if !ok {
			continue
		}
		r.voff = uint32(len(f.slab))
		f.slab = append(f.slab, p.Value...)
		r.vlen = uint32(len(p.Value))
		r.band &^= tierCold
	}
	// Drop the chunk's directory descriptor; its offset-table slot becomes a tombstone
	// (no record points at it now), reclaimed with the cold region at M8. The frame is
	// left immutable for recovery, so the promotion never rewrites it.
	if idx, ok := cc.dir.Floor(ck.Disc); ok {
		if dOff, _, _ := cc.dir.At(idx); dOff == off {
			cc.dir.Remove(idx)
		}
	}
}
