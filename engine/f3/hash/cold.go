package hash

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/engine/f3/tier"
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
	chunkByteTarget = 4096                      // pack until the payload reaches this, then flush
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
	_, val, ok := chunkEntry(ck.Payload, int(locEntry(loc)))
	return val, ok
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
	return chunkEntry(ck.Payload, int(locEntry(loc)))
}

// markDirty flags the chunk owning the field at hash as needing a repack, the
// resident record a cold delete leaves behind (the value bytes stay packed in the
// frame until the promotion-and-repack pass reclaims them, spec 2064/f3/06 section
// 6.5). It is a directory-only mark; the frame is untouched.
func (c *coldChunks) markDirty(hash uint64) {
	if idx, ok := c.dir.Floor(discOf(hash)); ok {
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

// discOf renders a field hash as an eight-byte big-endian discriminator, so the
// directory's byte-lexicographic order is the fields' hash order (the ordering
// contract of tier/directory.go). A hash has no partitioned band, so one directory
// spans the whole native table and the discs stay cleanly ordered.
func discOf(hash uint64) []byte {
	var d [8]byte
	binary.BigEndian.PutUint64(d[:], hash)
	return d[:]
}

// appendEntry packs one field-value pair into a chunk payload: an unsigned-varint
// field length, the field bytes, an unsigned-varint value length, then the value
// bytes. The field rides along for the M8 recovery walk and the promote re-seat;
// the value is what a cold read returns. The entry index a locator carries is the
// ordinal position of the pair in this stream, which chunkEntry walks to.
func appendEntry(payload, field, value []byte) []byte {
	payload = binary.AppendUvarint(payload, uint64(len(field)))
	payload = append(payload, field...)
	payload = binary.AppendUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

// chunkEntry returns the field and value of the idx-th pair packed in payload, or
// false when the payload is torn or shorter than idx+1 pairs. It walks the
// length-prefixed stream; a chunk holds at most maxChunkEntry pairs, so the walk is
// bounded.
func chunkEntry(payload []byte, idx int) (field, value []byte, ok bool) {
	p := payload
	for i := 0; ; i++ {
		fn, w := binary.Uvarint(p)
		if w <= 0 || uint64(len(p)-w) < fn {
			return nil, nil, false
		}
		p = p[w:]
		f := p[:fn]
		p = p[fn:]
		vn, w := binary.Uvarint(p)
		if w <= 0 || uint64(len(p)-w) < vn {
			return nil, nil, false
		}
		p = p[w:]
		v := p[:vn]
		p = p[vn:]
		if i == idx {
			return f, v, true
		}
	}
}
