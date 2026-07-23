package obs1

// The chunk codec common core (spec 2064/obs1 doc 08 section 1): the
// pieces every per-type cold slice shares. A chunk holds one
// collection's elements packed in discriminator order; the
// discriminator is the per-type sort key inside the collection, and
// every type derives it from the same coordinate the keymap, bloom
// filters, and segment sort already use, so one hash of an element
// name answers in every structure that might hold it. Partial
// residency is the normal state for a big collection, and the merge
// rule is uniform across types: for any discriminator range the hot
// overlay shadows the cold chunks. MergeShadow is that rule as code;
// the strings, hashes, and sets slices feed it their streams rather
// than re-deriving the shadowing each time.

import "errors"

// Chunk target bounds (doc 08 section 1): packed element bytes per
// chunk, the f3 4 KiB unit relaxed upward because the fetch unit is
// the 128 KiB block, not the chunk. The default and the floor are
// measured, not chosen: the typepoint and big-collection labs (#1290,
// #1291) put the directory's resident share at 0.13 B per element at
// 16 KiB, hold the per-type ledger's ~0.3 B row from 8 KiB up, and
// break it at 4 KiB, with the point-read bill flat across the whole
// band, so chunk size trades only directory RAM.
const (
	ChunkTargetMin     = 4 << 10
	ChunkTargetDefault = 16 << 10
	ChunkTargetMax     = 32 << 10
)

// ClampChunkTarget folds a configured chunk target into the doc 08
// band: zero or negative means the default, out-of-band values pin to
// the nearer bound. Config layers apply it; the folder and the
// demoters trust their callers and accept what they are given, which
// is how tests force cuts with tiny targets.
func ClampChunkTarget(n int) int {
	switch {
	case n <= 0:
		return ChunkTargetDefault
	case n < ChunkTargetMin:
		return ChunkTargetMin
	case n > ChunkTargetMax:
		return ChunkTargetMax
	}
	return n
}

// Disc is the shared discriminator coordinate: the same hash the
// keymap fingerprint, the segment bloom, and the fold sort use
// (bloomHash h1, the #1266 as-built deviation from the spec's wyhash,
// kept so every structure agrees on one coordinate per name). Strings
// discriminate on the record key, hashes on the field name, sets on
// the member; each type slice passes the right name and gets a value
// that binary-searches the same directory the fold built.
func Disc(name []byte) uint64 {
	h1, _ := bloomHash(name)
	return h1
}

// Elem is one collection element in the merge coordinate: its
// discriminator, its type-opaque packed bytes, and whether it is a
// deletion claim. The merge never inspects Data beyond handing it to
// the identity callback.
type Elem struct {
	Disc uint64
	Data []byte
	Dead bool
}

// ElemIter yields a stream of elements in nondecreasing Disc order;
// ok false ends the stream.
type ElemIter func() (e Elem, ok bool)

// ErrDiscOrder reports a stream that went backward in discriminator
// order: a packing or planning bug upstream, never a data state, so
// the merge stops loudly instead of shadowing the wrong range.
var ErrDiscOrder = errors.New("obs1: element stream out of discriminator order")

// MergeShadow merges a cold stream against a hot overlay stream, both
// in nondecreasing Disc order, and yields the live merged view in the
// same order: doc 08's uniform rule that the overlay shadows the
// chunks. Overlay elements replace cold elements of the same identity,
// dead overlay elements suppress their cold copy and are never
// yielded, and cold elements with no overlay claim pass through.
// Discriminators are hashes and can collide, so identity inside an
// equal-Disc tie is decided by same, which compares the two packed
// payloads (the type slice knows where the name lives); ties buffer
// only the colliding run, which stays collision-sized. Within a tie
// unshadowed cold elements yield before overlay elements, a fixed rule
// so scans are deterministic. A non-nil error from yield stops the
// merge and is returned.
func MergeShadow(cold, overlay ElemIter, same func(cold, ov []byte) bool, yield func(Elem) error) error {
	type side struct {
		next ElemIter
		cur  Elem
		ok   bool
		last uint64
	}
	advance := func(s *side) error {
		if s.cur, s.ok = s.next(); s.ok {
			if s.cur.Disc < s.last {
				return ErrDiscOrder
			}
			s.last = s.cur.Disc
		}
		return nil
	}
	c := &side{next: cold}
	o := &side{next: overlay}
	if err := advance(c); err != nil {
		return err
	}
	if err := advance(o); err != nil {
		return err
	}
	emit := func(e Elem) error {
		if e.Dead {
			return nil
		}
		return yield(e)
	}
	var cTie, oTie []Elem
	for c.ok || o.ok {
		switch {
		case !o.ok || (c.ok && c.cur.Disc < o.cur.Disc):
			if err := emit(c.cur); err != nil {
				return err
			}
			if err := advance(c); err != nil {
				return err
			}
		case !c.ok || o.cur.Disc < c.cur.Disc:
			if err := emit(o.cur); err != nil {
				return err
			}
			if err := advance(o); err != nil {
				return err
			}
		default:
			d := c.cur.Disc
			cTie, oTie = cTie[:0], oTie[:0]
			for c.ok && c.cur.Disc == d {
				cTie = append(cTie, c.cur)
				if err := advance(c); err != nil {
					return err
				}
			}
			for o.ok && o.cur.Disc == d {
				oTie = append(oTie, o.cur)
				if err := advance(o); err != nil {
					return err
				}
			}
			for _, ce := range cTie {
				claimed := false
				for _, oe := range oTie {
					if same(ce.Data, oe.Data) {
						claimed = true
						break
					}
				}
				if !claimed {
					if err := emit(ce); err != nil {
						return err
					}
				}
			}
			for _, oe := range oTie {
				if err := emit(oe); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// SliceIter adapts a slice of elements to an ElemIter, the shape every
// resident overlay snapshot and every decoded chunk takes before it
// meets the merge.
func SliceIter(elems []Elem) ElemIter {
	i := 0
	return func() (Elem, bool) {
		if i >= len(elems) {
			return Elem{}, false
		}
		e := elems[i]
		i++
		return e, true
	}
}
