package sqlo1

import (
	"context"
	"encoding/binary"
	"math/bits"
)

// The popcount cache (doc 05 section 3.2) and the operators it
// serves, BITCOUNT and BITPOS. Segment j of a rope's cache holds one
// u32 popcount per chunk for chunks [j*1024, (j+1)*1024), under
// subkey kind 2 of the same rooth, written with the same generation,
// so the cache lives and dies with the plane it describes.
//
// The doc places cache maintenance at drain time; this implementation
// maintains it at write time instead: every rope chunk write whose
// key has a cache updates the covering entry in the same command.
// That meets the doc's goal (the cache is exact with no query-time
// delta bookkeeping, because tier reads see the entry and the chunk
// move together) without teaching the drain about types. The cost is
// one entry read-modify-write per touched chunk, the same class as
// the chunk write it rides; the bitcount lab arbitrates if that ever
// shows up.
//
// The cache comes into being on the first whole-value BITCOUNT or
// BITPOS over a rope (pc_seg_count stays 0 until bitmap ops occur),
// so keys that never see a bit query never pay for one. A full-value
// rewrite mints a fresh plane with no cache and the next whole-value
// query rebuilds it.

const (
	// pcKind is the subkey kind of popcount cache segments.
	pcKind = 2

	// pcChunksPerSeg is how many chunk entries one segment covers;
	// 1024 u32 entries make a segment exactly one 4 KiB group.
	pcChunksPerSeg = 1024
)

// putPCKey writes the subkey of popcount segment segid under rooth.
func putPCKey(dst []byte, rooth, segid uint64) {
	binary.LittleEndian.PutUint64(dst, rooth)
	dst[8] = pcKind
	var seg [8]byte
	binary.LittleEndian.PutUint64(seg[:], segid)
	copy(dst[9:SubkeySize], seg[:7])
}

// popcountBytes counts set bits in p.
func popcountBytes(p []byte) int64 {
	var n int64
	for len(p) >= 8 {
		n += int64(bits.OnesCount64(binary.LittleEndian.Uint64(p)))
		p = p[8:]
	}
	for _, b := range p {
		n += int64(bits.OnesCount8(b))
	}
	return n
}

// popcountBits counts set bits of p in the inclusive bit window
// [lo, hi], MSB-first within each byte. The caller guarantees
// hi < len(p)*8.
func popcountBits(p []byte, lo, hi uint64) int64 {
	b0, b1 := lo>>3, hi>>3
	if b0 == b1 {
		m := byte(0xFF>>(lo&7)) & byte(0xFF<<(7-hi&7))
		return int64(bits.OnesCount8(p[b0] & m))
	}
	n := int64(bits.OnesCount8(p[b0] & (0xFF >> (lo & 7))))
	n += int64(bits.OnesCount8(p[b1] & (0xFF << (7 - hi&7))))
	return n + popcountBytes(p[b0+1:b1])
}

// findBit returns the absolute index of the first bit equal to bit in
// the inclusive window [lo, hi] of p, or -1. The caller guarantees
// hi < len(p)*8.
func findBit(p []byte, lo, hi uint64, bit int) int64 {
	for b := lo >> 3; b <= hi>>3; b++ {
		x := p[b]
		if bit == 0 {
			x = ^x
		}
		if b == lo>>3 {
			x &= 0xFF >> (lo & 7)
		}
		if b == hi>>3 {
			x &= 0xFF << (7 - hi&7)
		}
		if x != 0 {
			return int64(b<<3) + int64(bits.LeadingZeros8(x))
		}
	}
	return -1
}

// findBitChunk is findBit over one chunk's stored bytes plus the lazy
// zero tail: bits past len(p)*8 up to the window's end read as clear.
func findBitChunk(p []byte, lo, hi uint64, bit int) int64 {
	if stored := uint64(len(p)) * 8; stored > 0 && lo < stored {
		if i := findBit(p, lo, min(hi, stored-1), bit); i >= 0 {
			return i
		}
	}
	if bit == 0 && hi >= uint64(len(p))*8 {
		return int64(max(lo, uint64(len(p))*8))
	}
	return -1
}

// pcUpdate sets chunk's cache entry to cnt, growing or creating the
// covering segment as needed. Callers skip it entirely when the root
// carries no cache, so uncached keys pay nothing.
func (s *Str) pcUpdate(ctx context.Context, r ropeRoot, chunk uint64, cnt uint32) error {
	putPCKey(s.pckbuf[:], r.rooth, chunk/pcChunksPerSeg)
	s.pcKeys = append(s.pcKeys[:0], s.pckbuf[:])
	out, err := s.t.BatchGet(ctx, s.pcKeys, s.pcVals)
	s.pcVals = out[:0]
	if err != nil {
		return err
	}
	idx := int(chunk%pcChunksPerSeg) * 4
	old := out[0]
	if idx+4 <= len(old) && binary.LittleEndian.Uint32(old[idx:]) == cnt {
		return nil
	}
	s.pcScratch = grow(s.pcScratch, max(len(old), idx+4))
	n := copy(s.pcScratch, old)
	clear(s.pcScratch[n:])
	binary.LittleEndian.PutUint32(s.pcScratch[idx:], cnt)
	return s.t.SetGen(ctx, s.pckbuf[:], s.pcScratch, TagString, r.rootgen)
}

// ensurePC builds the popcount cache for a rope that has none: one
// pass over the chunks in read rounds, one segment record per 1024
// chunks, a flush barrier, then the root rewrite that commits
// pc_seg_count. A crash before the root leaves orphan segments on the
// live plane that the next build overwrites. Returns the updated
// root.
func (s *Str) ensurePC(ctx context.Context, key []byte, r ropeRoot, expMs int64) (ropeRoot, error) {
	if cap(s.chunkKeys) < strReadRound {
		s.chunkKeys = make([][]byte, strReadRound)
		for i := range s.chunkKeys {
			s.chunkKeys[i] = make([]byte, SubkeySize)
		}
	}
	segCount := (r.chunkCount + pcChunksPerSeg - 1) / pcChunksPerSeg
	for seg := range segCount {
		lo := seg * pcChunksPerSeg
		hi := min(lo+pcChunksPerSeg, r.chunkCount)
		s.pcScratch = grow(s.pcScratch, int(hi-lo)*4)
		for base := lo; base < hi; base += strReadRound {
			n := min(strReadRound, hi-base)
			keys := s.chunkKeys[:n]
			for j := range keys {
				putChunkKey(keys[j], r.rooth, base+uint64(j))
			}
			out, err := s.t.BatchGet(ctx, keys, s.chunkVals)
			s.chunkVals = out[:0]
			if err != nil {
				return r, err
			}
			for j, cv := range out {
				binary.LittleEndian.PutUint32(s.pcScratch[(base-lo+uint64(j))*4:], uint32(popcountBytes(cv)))
			}
		}
		putPCKey(s.pckbuf[:], r.rooth, seg)
		if err := s.t.SetGen(ctx, s.pckbuf[:], s.pcScratch, TagString, r.rootgen); err != nil {
			return r, err
		}
	}
	// The barrier, same discipline as setRope: every segment is ahead
	// of the root that declares the cache live, so a crash prefix
	// never reads a claimed-but-absent segment as zero counts.
	if err := s.t.Flush(ctx); err != nil {
		return r, err
	}
	r.pcSegCount = segCount
	s.rootBuf = appendRopeRoot(s.rootBuf[:0], r)
	if err := s.t.Set(ctx, key, s.rootBuf, TagString|TagRoot); err != nil {
		return r, err
	}
	return r, s.restamp(ctx, key, expMs)
}

// loadPCEntries reads the cache entries for chunks [c0, c1] in one
// coalesced segment round into s.pcEntries. Entries a short or absent
// segment does not cover read as zero, matching the lazy chunks they
// describe.
func (s *Str) loadPCEntries(ctx context.Context, r ropeRoot, c0, c1 uint64) ([]uint32, error) {
	s0, s1 := c0/pcChunksPerSeg, c1/pcChunksPerSeg
	if cap(s.pcKeys) < int(s1-s0+1) {
		s.pcKeys = make([][]byte, s1-s0+1)
	}
	s.pcKeys = s.pcKeys[:s1-s0+1]
	s.pcKeyBuf = grow(s.pcKeyBuf, int(s1-s0+1)*SubkeySize)
	for i := range s.pcKeys {
		k := s.pcKeyBuf[i*SubkeySize : (i+1)*SubkeySize]
		putPCKey(k, r.rooth, s0+uint64(i))
		s.pcKeys[i] = k
	}
	out, err := s.t.BatchGet(ctx, s.pcKeys, s.pcVals)
	s.pcVals = out[:0]
	if err != nil {
		return nil, err
	}
	if cap(s.pcEntries) < int(c1-c0+1) {
		s.pcEntries = make([]uint32, c1-c0+1)
	}
	s.pcEntries = s.pcEntries[:c1-c0+1]
	for i := range s.pcEntries {
		c := c0 + uint64(i)
		v := out[c/pcChunksPerSeg-s0]
		idx := int(c%pcChunksPerSeg) * 4
		if idx+4 <= len(v) {
			s.pcEntries[i] = binary.LittleEndian.Uint32(v[idx:])
		} else {
			s.pcEntries[i] = 0
		}
	}
	return s.pcEntries, nil
}

// bitRange is a BITCOUNT or BITPOS range argument set, unresolved:
// start and end are as given (negative counts from the tail), bitUnit
// selects the BIT form, ranged says any range was given at all, and
// endGiven distinguishes BITPOS's explicit-end semantics for clear
// bits.
type bitRange struct {
	start, end int64
	bitUnit    bool
	ranged     bool
	endGiven   bool
}

// resolve clamps br against a value of byteLen bytes and returns the
// inclusive bit window.
func (br bitRange) resolve(byteLen uint64) (lo, hi uint64, some bool) {
	if !br.ranged {
		if byteLen == 0 {
			return 0, 0, false
		}
		return 0, byteLen*8 - 1, true
	}
	n := int64(byteLen)
	if br.bitUnit {
		n = int64(byteLen * 8)
	}
	l, h, some := clampRange(br.start, br.end, n)
	if !some {
		return 0, 0, false
	}
	if br.bitUnit {
		return uint64(l), uint64(h) - 1, true
	}
	return uint64(l) * 8, uint64(h)*8 - 1, true
}

// wholeValue reports whether the resolved window covers every bit,
// which is what makes an uncached BITCOUNT or BITPOS build the cache.
func wholeValue(lo, hi, byteLen uint64) bool {
	return lo == 0 && byteLen > 0 && hi == byteLen*8-1
}

// BitCount counts set bits in key's value over br. A missing key
// counts zero. On a rope the interior chunks answer from the popcount
// cache in one segment round and only the window's edge chunks are
// read (S-I3); an uncached rope builds the cache first when the
// window is the whole value, and otherwise reads just the chunks the
// window overlaps.
func (s *Str) BitCount(ctx context.Context, key []byte, br bitRange) (int64, error) {
	v, root, expMs, ok, err := s.t.LookupEntry(ctx, key)
	if err != nil || !ok {
		return 0, err
	}
	if !root {
		lo, hi, some := br.resolve(uint64(len(v)))
		if !some {
			return 0, nil
		}
		return popcountBits(v, lo, hi), nil
	}
	r, err := decodeRopeRoot(v)
	if err != nil {
		return 0, err
	}
	lo, hi, some := br.resolve(r.totalLen)
	if !some {
		return 0, nil
	}
	if r.pcSegCount == 0 && wholeValue(lo, hi, r.totalLen) {
		if r, err = s.ensurePC(ctx, key, r, expMs); err != nil {
			return 0, err
		}
	}
	cs8 := r.chunkSize() * 8
	c0, c1 := lo/cs8, hi/cs8
	var entries []uint32
	if r.pcSegCount > 0 {
		if entries, err = s.loadPCEntries(ctx, r, c0, c1); err != nil {
			return 0, err
		}
	}
	var total int64
	for c := c0; c <= c1; {
		clo, chi := max(lo, c*cs8), min(hi, (c+1)*cs8-1)
		chunkEnd := min((c+1)*cs8, r.totalLen*8) - 1
		if entries != nil && clo == c*cs8 && chi == chunkEnd {
			total += int64(entries[c-c0])
			c++
			continue
		}
		// An edge chunk, or an uncached window: read a round of chunks
		// and count their overlap directly. Short and absent chunks
		// contribute their stored bytes only; the lazy tail is zeros.
		n := min(uint64(strReadRound), c1-c+1)
		if entries != nil {
			n = 1
		}
		if cap(s.chunkKeys) < strReadRound {
			s.chunkKeys = make([][]byte, strReadRound)
			for i := range s.chunkKeys {
				s.chunkKeys[i] = make([]byte, SubkeySize)
			}
		}
		keys := s.chunkKeys[:n]
		for j := range keys {
			putChunkKey(keys[j], r.rooth, c+uint64(j))
		}
		out, err := s.t.BatchGet(ctx, keys, s.chunkVals)
		s.chunkVals = out[:0]
		if err != nil {
			return 0, err
		}
		for j, cv := range out {
			cc := c + uint64(j)
			cclo, cchi := max(lo, cc*cs8)-cc*cs8, min(hi, (cc+1)*cs8-1)-cc*cs8
			if stored := uint64(len(cv)) * 8; stored > 0 && cclo < stored {
				total += popcountBits(cv, cclo, min(cchi, stored-1))
			}
		}
		c += n
	}
	return total, nil
}

// BitPos returns the absolute index of the first bit equal to bit in
// key's value over br, with Redis's edges: a missing key is 0 for
// clear and -1 for set, an empty window is -1, and a clear-bit search
// that exhausts a window whose end was implicit answers just past the
// value. On a rope the cache skips uniform chunks exactly (a chunk's
// entry says all-zero or all-one against its logical size), so at
// most one chunk is read.
func (s *Str) BitPos(ctx context.Context, key []byte, bit int, br bitRange) (int64, error) {
	v, root, expMs, ok, err := s.t.LookupEntry(ctx, key)
	if err != nil {
		return 0, err
	}
	if !ok {
		if bit == 0 {
			return 0, nil
		}
		return -1, nil
	}
	if !root {
		lo, hi, some := br.resolve(uint64(len(v)))
		if !some {
			return -1, nil
		}
		if i := findBit(v, lo, hi, bit); i >= 0 {
			return i, nil
		}
		if bit == 0 && !br.endGiven {
			return int64(len(v)) * 8, nil
		}
		return -1, nil
	}
	r, err := decodeRopeRoot(v)
	if err != nil {
		return 0, err
	}
	lo, hi, some := br.resolve(r.totalLen)
	if !some {
		return -1, nil
	}
	if r.pcSegCount == 0 && wholeValue(lo, hi, r.totalLen) {
		if r, err = s.ensurePC(ctx, key, r, expMs); err != nil {
			return 0, err
		}
	}
	cs8 := r.chunkSize() * 8
	c0, c1 := lo/cs8, hi/cs8
	var entries []uint32
	if r.pcSegCount > 0 {
		if entries, err = s.loadPCEntries(ctx, r, c0, c1); err != nil {
			return 0, err
		}
	}
	for c := c0; c <= c1; c++ {
		clo, chi := max(lo, c*cs8), min(hi, (c+1)*cs8-1)
		chunkEnd := min((c+1)*cs8, r.totalLen*8) - 1
		if entries != nil && clo == c*cs8 && chi == chunkEnd {
			pc, full := uint64(entries[c-c0]), chunkEnd-c*cs8+1
			if bit == 1 {
				if pc == 0 {
					continue
				}
				if pc == full {
					return int64(clo), nil
				}
			} else {
				if pc == full {
					continue
				}
				if pc == 0 {
					return int64(clo), nil
				}
			}
		}
		// A mixed chunk holds the answer by construction; an edge or
		// uncached chunk may not, and the walk continues.
		putChunkKey(s.kbuf[:], r.rooth, c)
		s.pcKeys = append(s.pcKeys[:0], s.kbuf[:])
		out, err := s.t.BatchGet(ctx, s.pcKeys, s.pcVals)
		s.pcVals = out[:0]
		if err != nil {
			return 0, err
		}
		if i := findBitChunk(out[0], clo-c*cs8, chi-c*cs8, bit); i >= 0 {
			return int64(c*cs8) + i, nil
		}
	}
	if bit == 0 && !br.endGiven {
		return int64(r.totalLen) * 8, nil
	}
	return -1, nil
}
