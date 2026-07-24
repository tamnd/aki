package sqlo1

import (
	"bytes"
	"context"
)

// Rename moves src's value to dst, RENAME's storage half. existed is
// false when src is missing, which the server answers as no such key.
// nx makes it RENAMENX: done comes back false when dst already holds
// a value and nothing moves. A same-key rename is checked after src
// existence like Redis does, so it answers done for RENAME and not
// done for RENAMENX, dst being src.
//
// The move is one root record whatever the collection size, doc 12
// section 2.2: rooth is minted at root creation and never derived
// from the key name, so every segment subkey stays valid under the
// new name and no plane is touched. dst's old value dies with DEL's
// retire semantics, its bump riding the root image written below.
// The crash story follows Move's frame group: when either name is
// already dirty the tier flushes first, so the dst image and the src
// tombstone stay contiguous in one drain batch and a torn tail
// replays the rename all-or-nothing; the dst write goes first, so
// the value is under at least one name at every batch boundary. The
// src tombstone goes through Tiered.Del, not Str.Del: the plane must
// survive the old name's death. src's expiry moves with the value,
// stamped onto dst after the write and cleared when src had none,
// since the fresh dst header may otherwise inherit dst's old TTL.
func (s *Str) Rename(ctx context.Context, src, dst []byte, nx bool) (existed, done bool, err error) {
	v, root, expMs, ok, err := s.t.LookupEntry(ctx, src)
	if err != nil || !ok {
		return false, false, err
	}
	if bytes.Equal(src, dst) {
		return true, !nx, nil
	}

	// Copy the payload out while the src view is live; the dst reads
	// below recycle the arena bytes v aliases. A root's true tag comes
	// off its sub byte, so the image lands under dst with the nibble
	// the type layers wrote, not the flattened promotion tag.
	s.val = append(s.val[:0], v...)
	tag := TagString
	if root {
		rt, _, err := sniffRoot(s.val)
		if err != nil {
			return true, false, err
		}
		tag = rt | TagRoot
	}

	dm, err := s.metaOf(ctx, dst)
	if err != nil {
		return true, false, err
	}
	if nx && dm.exists {
		return true, false, nil
	}

	// The flush guard, Move's contiguity rule: a name that is already
	// dirty holds an early drain-queue position, and a batch cut
	// between the pair would commit the src tombstone without the dst
	// image. Flushing first gives both writes fresh tail positions.
	if s.t.ht.dirtyKey(src) || s.t.ht.dirtyKey(dst) {
		if err := s.t.Flush(ctx); err != nil {
			return true, false, err
		}
	}

	// dst's old plane retires exactly like Str.Del, the bump riding
	// the image written next.
	if dm.rope || (dm.otherType && !dm.planeless) {
		s.retire(dst, dm.root)
	}
	if err := s.t.Set(ctx, dst, s.val, tag); err != nil {
		return true, false, err
	}
	if _, err := s.t.ExpireAt(ctx, dst, expMs); err != nil {
		return true, false, err
	}
	if _, err := s.t.Del(ctx, src); err != nil {
		return true, false, err
	}
	return true, true, nil
}
