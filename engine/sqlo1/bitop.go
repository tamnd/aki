package sqlo1

// BITOP, doc 05 section 3.1: the result streams through fixed-size
// stripes of the destination's chunk grid, so memory stays constant
// in both the value lengths and the source count. Each source's
// overlap with the current stripe folds into the accumulator the
// moment it is read, and result chunks that come out all zeros are
// never written at all: an absent chunk under a covering total_len
// already reads as zeros (S-I4), so an AND of mostly-disjoint bitmaps
// costs almost no chunk records.

import (
	"context"
	"encoding/binary"
)

// BITOP operators.
const (
	bitopAnd = iota
	bitopOr
	bitopXor
	bitopNot
)

// bitopSrc is what the stripe loop keeps of a source after the meta
// scan: its logical length and, for ropes, the decoded root. Plain
// values are re-read per stripe instead of held, because Lookup bytes
// die on the next store call and copying them out would scale memory
// with the source count.
type bitopSrc struct {
	byteLen uint64
	rope    bool
	root    ropeRoot
}

// BitOp applies op over the source values, shorter sources
// zero-padded to the longest, and stores the result at dest,
// returning the result length in bytes. An empty result deletes dest,
// per Redis. The destination is always a fresh plane (or a fresh
// plain record), never an in-place rewrite, which is what makes dest
// appearing among its own sources safe: every read goes to the old
// plane while the new one is built unreferenced, and the old plane
// retires with the root swap. Like every non-SET write path the layer
// preserves a live dest expiry; the command layer owns BITOP's
// discard-the-TTL rule.
func (s *Str) BitOp(ctx context.Context, op int, dest []byte, srcs [][]byte) (int64, error) {
	s.bitopSrcs = s.bitopSrcs[:0]
	maxLen := uint64(0)
	for _, k := range srcs {
		v, root, _, ok, err := s.t.LookupEntry(ctx, k)
		if err != nil {
			return 0, err
		}
		var m bitopSrc
		if ok && root {
			r, err := decodeRopeRoot(v)
			if err != nil {
				return 0, err
			}
			m = bitopSrc{byteLen: r.totalLen, rope: true, root: r}
		} else if ok {
			m.byteLen = uint64(len(v))
		}
		s.bitopSrcs = append(s.bitopSrcs, m)
		maxLen = max(maxLen, m.byteLen)
	}
	dm, err := s.metaOf(ctx, dest)
	if err != nil {
		return 0, err
	}
	if maxLen == 0 {
		if dm.exists {
			if dm.rope {
				s.retire(dest, dm.root)
			}
			if _, err := s.t.Del(ctx, dest); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	if !s.needsRope(dest, int(maxLen)) {
		// One stripe is the whole result; the bound is the rope
		// boundary itself, so the accumulator stays small.
		if err := s.bitopStripe(ctx, op, srcs, 0, maxLen); err != nil {
			return 0, err
		}
		if dm.rope {
			s.retire(dest, dm.root)
		}
		if err := s.t.Set(ctx, dest, s.bitopAcc, TagString); err != nil {
			return 0, err
		}
		return int64(maxLen), s.restamp(ctx, dest, dm.expMs)
	}
	rooth, err := s.nextRooth(ctx)
	if err != nil {
		return 0, err
	}
	cs := uint64(1) << s.cfg.Log2Chunk
	stripe := cs * strReadRound
	for off := uint64(0); off < maxLen; off += stripe {
		sl := min(stripe, maxLen-off)
		if err := s.bitopStripe(ctx, op, srcs, off, sl); err != nil {
			return 0, err
		}
		for co := uint64(0); co < sl; co += cs {
			sub := s.bitopAcc[co:min(co+cs, sl)]
			if allZero(sub) {
				continue
			}
			putChunkKey(s.kbuf[:], rooth, (off+co)>>s.cfg.Log2Chunk)
			if err := s.t.SetGen(ctx, s.kbuf[:], sub, TagString, 1); err != nil {
				return 0, err
			}
		}
	}
	// The setRope barrier: every chunk of the new plane is durable
	// before the root that references it can drain.
	if err := s.t.Flush(ctx); err != nil {
		return 0, err
	}
	if dm.rope {
		s.retire(dest, dm.root)
	}
	s.rootBuf = appendRopeRoot(s.rootBuf[:0], ropeRoot{
		log2chunk:  s.cfg.Log2Chunk,
		rootgen:    1,
		rooth:      rooth,
		totalLen:   maxLen,
		chunkCount: (maxLen + cs - 1) >> s.cfg.Log2Chunk,
	})
	if err := s.t.Set(ctx, dest, s.rootBuf, TagString|TagRoot); err != nil {
		return 0, err
	}
	return int64(maxLen), s.restamp(ctx, dest, dm.expMs)
}

// bitopStripe folds every source's overlap with the absolute window
// [off, off+n) into s.bitopAcc[:n]. The first source seeds the
// accumulator zero-extended; a later source past its length
// contributes zeros, which only AND has to act on. NOT is a
// post-pass, since it takes exactly one source.
func (s *Str) bitopStripe(ctx context.Context, op int, srcs [][]byte, off, n uint64) error {
	s.bitopAcc = grow(s.bitopAcc, int(n))
	acc := s.bitopAcc
	for i, k := range srcs {
		m := s.bitopSrcs[i]
		var w []byte
		if m.byteLen > off {
			hi := min(off+n, m.byteLen)
			if m.rope {
				out, _, err := s.readRopeRange(ctx, m.root, off, hi)
				if err != nil {
					return err
				}
				w = out
			} else {
				// Aliased bytes, folded before the next store call.
				v, _, ok, err := s.t.Lookup(ctx, k)
				if err != nil {
					return err
				}
				if ok && uint64(len(v)) > off {
					w = v[off:min(hi, uint64(len(v)))]
				}
			}
		}
		if i == 0 {
			seeded := copy(acc, w)
			clear(acc[seeded:])
			continue
		}
		switch op {
		case bitopAnd:
			for j := range w {
				acc[j] &= w[j]
			}
			clear(acc[len(w):])
		case bitopOr:
			for j := range w {
				acc[j] |= w[j]
			}
		case bitopXor:
			for j := range w {
				acc[j] ^= w[j]
			}
		}
	}
	if op == bitopNot {
		for j := range acc {
			acc[j] = ^acc[j]
		}
	}
	return nil
}

// allZero reports whether p is entirely zero bytes.
func allZero(p []byte) bool {
	for len(p) >= 8 {
		if binary.LittleEndian.Uint64(p) != 0 {
			return false
		}
		p = p[8:]
	}
	for _, b := range p {
		if b != 0 {
			return false
		}
	}
	return true
}
