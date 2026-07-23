// The collection field extractor (spec 2064/obs1 doc 08 section 3): the
// read-side counterpart of extractColdRecord for packed collection
// chunks. A planner resolved a field's owning chunk through
// Directory.ResolveField; this file walks the fetched block's chunk
// frame, decodes the packed pairs under the frame's own flags and count,
// and applies the inline field-TTL lazy rule with the caller's clock, so
// a cold answer about an expired field agrees with the hot tier's
// (invariant T-I5).
package obs1

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/tamnd/aki/engine/obs1/store"
)

// ColdField is one extracted collection field. Found false means the
// resolved chunk does not hold the field or its inline expiry has fired,
// both of which read as absent. Exp is the absolute unix-ms expiry, zero
// for none.
type ColdField struct {
	Found bool
	Value []byte // copied out of the fetch buffer
	Exp   uint64
}

// coldCollChunk decodes the collection chunk frame at off inside a
// decoded block, shared by the point and scan extractors. A run chunk
// cannot answer a field read, so it errors exactly as extractColdRecord
// errors on the inverse.
func coldCollChunk(data []byte, off uint32) (store.FoldFrame, error) {
	if int(off)+4 > len(data) {
		return store.FoldFrame{}, fmt.Errorf("obs1: chunk offset %d past the block", off)
	}
	total := binary.LittleEndian.Uint32(data[off:])
	if total < 4 || int(off)+int(total) > len(data) {
		return store.FoldFrame{}, fmt.Errorf("obs1: chunk frame total %d runs past the block", total)
	}
	var outer store.FoldFrame
	if err := store.WalkStagedFrames(data[off:off+total], func(f store.FoldFrame) error {
		outer = f
		return nil
	}); err != nil {
		return store.FoldFrame{}, err
	}
	if !outer.Chunk || outer.Flags&store.ChunkFlagRun != 0 {
		return store.FoldFrame{}, fmt.Errorf("obs1: field read points at a non-collection chunk (kind 0x%02x)", outer.Kind)
	}
	return outer, nil
}

// ExtractColdField finds field inside the collection chunk at off,
// applying the lazy expiry rule at nowMs. A missing field is Found
// false with a nil error: the planner floored to this chunk by
// discriminator, so absence here is definitive for the cold tier and
// the overlay has already had its say.
func ExtractColdField(data []byte, off uint32, field []byte, nowMs int64) (ColdField, error) {
	outer, err := coldCollChunk(data, off)
	if err != nil {
		return ColdField{}, err
	}
	var out ColdField
	ok := store.WalkPackedPairs(outer.Payload, outer.Flags, int(outer.Count), func(_ int, p store.PackedPair) bool {
		if !bytes.Equal(p.Field, field) {
			return true
		}
		if p.Exp != 0 && p.Exp <= uint64(nowMs) {
			return false // fired inline expiry reads as absent
		}
		out = ColdField{Found: true, Value: append([]byte(nil), p.Value...), Exp: p.Exp}
		return false
	})
	if !ok {
		return ColdField{}, fmt.Errorf("obs1: collection chunk payload is torn")
	}
	return out, nil
}

// WalkColdFields yields every live pair of the collection chunk at off
// in pack order, the HGETALL and scan unit: fields whose inline expiry
// fired at nowMs are skipped. The pair's slices alias the block buffer;
// a consumer that outlives the walk copies out.
func WalkColdFields(data []byte, off uint32, nowMs int64, fn func(p store.PackedPair) error) error {
	outer, err := coldCollChunk(data, off)
	if err != nil {
		return err
	}
	var ferr error
	ok := store.WalkPackedPairs(outer.Payload, outer.Flags, int(outer.Count), func(_ int, p store.PackedPair) bool {
		if p.Exp != 0 && p.Exp <= uint64(nowMs) {
			return true
		}
		ferr = fn(p)
		return ferr == nil
	})
	if ferr != nil {
		return ferr
	}
	if !ok {
		return fmt.Errorf("obs1: collection chunk payload is torn")
	}
	return nil
}

// HashElem encodes one field as a merge element payload: a single packed
// pair in the bitmap framing (count 1), self-describing enough that
// MergeShadow's identity callback can compare two elements from either
// side without carrying flags out of band.
func HashElem(dst []byte, field, value []byte, exp uint64) []byte {
	var p store.ChunkPacker
	p.Add(field, value, exp)
	if exp == 0 {
		// Force the bitmap framing even without a bearer so every element
		// decodes under one rule; a one-element bitmap is a single byte.
		payload, _ := p.Finish()
		return append(append(dst, 0), payload...)
	}
	payload, _ := p.Finish()
	return append(dst, payload...)
}

// HashElemPair decodes a merge element back into its pair.
func HashElemPair(data []byte) (store.PackedPair, bool) {
	return store.PackedPairAt(data, store.ChunkFlagTTLBitmap, 1, 0)
}

// HashSame is the MergeShadow identity callback for hashes (doc 08
// section 1): two elements are the same member when their field names
// match, whatever their values or expiries, so an overlay write shadows
// the cold copy of its field and nothing else in a discriminator
// collision.
func HashSame(cold, ov []byte) bool {
	a, aok := HashElemPair(cold)
	b, bok := HashElemPair(ov)
	return aok && bok && bytes.Equal(a.Field, b.Field)
}
