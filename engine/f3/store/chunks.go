package store

import (
	"encoding/binary"
	"errors"
)

// errChunkRead is returned when a chunk's logged bytes cannot be read back
// during a rewrite. The value's remaining chunks are intact; the write that
// needed the bytes fails.
var errChunkRead = errors.New("store: chunk read failed")

// The chunked band (spec 2064/f3/09 section 2): a value at or past
// strChunkMin never exists contiguously in the store. Its bytes split into
// strChunkSize chunks, each chunk its own run under the same arena-or-log
// placement rule the separated band uses, located by a chunk directory: a
// flat arena block of 16-byte run pointers, one per chunk, in value order.
// The record's value area holds one pointer to the directory (word: the
// directory's arena offset, always resident; vlen: the live chunk count;
// vcap: the directory's entry capacity), and the record's own vlen carries
// the value's total byte length.
//
// The threshold is contract-bound to the streaming reply path: no chunk is
// bigger than strChunkSize, so a reader serves the value chunk by chunk with
// its footprint bounded to a fixed window and never materializes the whole
// value, and APPEND and SETRANGE patch by chunk under the same bound.

// ChunkSize is the chunked band's chunk width, exported so the streaming
// reply window can size its slot buffers to exactly one chunk.
const ChunkSize = strChunkSize

// chunkCount is how many chunks a value of n bytes takes.
func chunkCount(n uint64) uint32 {
	return uint32((n + strChunkSize - 1) / strChunkSize)
}

// stage returns the one-chunk staging buffer, allocated on first use.
func (s *Store) stage() []byte {
	if s.cbuf == nil {
		s.cbuf = make([]byte, strChunkSize)
	}
	return s.cbuf
}

// composeChunk builds chunk k's bytes for the logical patched value (old with
// val overwritten at offset and zeros in any gap) into the staging buffer.
// base is the chunk's start position and clen its length in the new value.
func (s *Store) composeChunk(base, clen int, old []byte, offset int, val []byte) []byte {
	dst := s.stage()[:clen]
	oldClen := 0
	if base < len(old) {
		oldClen = len(old) - base
		if oldClen > clen {
			oldClen = clen
		}
		copy(dst[:oldClen], old[base:base+oldClen])
	}
	if oldClen < clen {
		clear(dst[oldClen:clen])
	}
	overlayPatch(dst, base, offset, val)
	return dst
}

// overlayPatch copies the part of val that lands inside the chunk at base
// over dst, dst being that chunk's bytes.
func overlayPatch(dst []byte, base, offset int, val []byte) {
	lo, hi := base, base+len(dst)
	if offset > lo {
		lo = offset
	}
	if end := offset + len(val); end < hi {
		hi = end
	}
	if lo < hi {
		copy(dst[lo-base:hi-base], val[lo-offset:hi-offset])
	}
}

// writeChunked lays down a fresh chunk directory and chunks for the logical
// patched value of newLen bytes (old, then val overlaid at offset, zeros in
// any gap; a plain SET passes old nil and offset 0). The directory is always
// arena-resident: it is index-side metadata, sized at one pointer per chunk,
// and only the chunk bytes follow the spill rule. On any failure everything
// placed so far unwinds and the store is as before.
func (s *Store) writeChunked(old []byte, offset int, val []byte, newLen int) (dirOff uint64, n uint32, err error) {
	n = chunkCount(uint64(newLen))
	dirOff, ok := s.arenaAlloc(uint64(n) * ptrSize)
	if !ok {
		return 0, 0, ErrFull
	}
	end := offset + len(val)
	for k := uint32(0); k < n; k++ {
		base := int(k) * strChunkSize
		clen := newLen - base
		if clen > strChunkSize {
			clen = strChunkSize
		}
		es := dirOff + uint64(k)*ptrSize
		// A chunk with no old bytes the patch never reaches is all zeros; it
		// stays a hole (a nil run word carrying the length) and consumes no
		// bytes, so a SETBIT at a high offset stores only the live chunk.
		if base >= len(old) && (end <= base || offset >= base+clen) {
			s.writePtr(es, 0, uint32(clen), 0)
			continue
		}
		var a []byte
		if offset <= base && end >= base+clen {
			// The chunk is wholly patch bytes: write straight from val, no
			// staging copy.
			a = val[base-offset : base-offset+clen]
		} else {
			a = s.composeChunk(base, clen, old, offset, val)
		}
		word, vcap, werr := s.writeRun(a, nil, 0)
		if werr != nil {
			for j := uint32(0); j < k; j++ {
				w, l, c := s.readPtr(dirOff + uint64(j)*ptrSize)
				if w != 0 {
					s.dropRun(w, l, c)
				}
			}
			s.arena.unlink(dirOff, uint64(n)*ptrSize)
			return 0, 0, werr
		}
		s.writePtr(es, word, uint32(clen), vcap)
	}
	return dirOff, n, nil
}

// dropChunks releases a chunked record's outside bytes: every chunk run,
// then the directory block. The chunked-band charge is credited against the
// record's value length, the same figure the charge sites added, so the
// counter balances even when an error path left a chunk longer than the
// committed length.
func (s *Store) dropChunks(addr uint64) {
	s.chunkBytes -= s.vlen(addr)
	word, n, dcap := s.readPtr(s.valueStart(addr))
	dirOff := word & runAddrMask
	for k := uint32(0); k < n; k++ {
		w, l, c := s.readPtr(dirOff + uint64(k)*ptrSize)
		if w != 0 {
			s.dropRun(w, l, c)
		}
	}
	s.arena.unlink(dirOff, uint64(dcap)*ptrSize)
}

// reassembleChunked rebuilds a chunked value during recovery from its durable
// directory (chunkDirRow's form: 12 bytes per chunk, an 8-byte run word then a
// 4-byte length) into dst, grown to total when needed. Each chunk's bytes come from
// the durable value log the word names, or are zeros for a hole word (zero), the
// mirror of readChunked's live walk over the arena directory. A word that is neither
// a hole nor log-resident never had a durable copy and fails the replay closed, the
// same fail-closed a torn frame takes. A directory whose lengths do not sum to total,
// or that outruns dst, is a corrupt frame and fails too.
func (s *Store) reassembleChunked(dir []byte, total int, dst []byte) ([]byte, error) {
	if cap(dst) < total {
		dst = make([]byte, total)
	}
	dst = dst[:total]
	pos := 0
	for off := 0; off+12 <= len(dir); off += 12 {
		w := binary.LittleEndian.Uint64(dir[off:])
		clen := int(binary.LittleEndian.Uint32(dir[off+8:]))
		if clen < 0 || pos+clen > total {
			return nil, errChunkRead
		}
		switch {
		case w == 0:
			clear(dst[pos : pos+clen])
		case w&inLogBit != 0:
			if err := s.logReadFill(w&runAddrMask, dst[pos:pos+clen]); err != nil {
				return nil, err
			}
		default:
			return nil, errChunkRead
		}
		pos += clen
	}
	if pos != total {
		return nil, errChunkRead
	}
	return dst, nil
}

// readChunked copies a chunked record's whole value into dst, chunk by chunk.
// This is the non-streaming access the wrappers and the in-store rewrites
// use; the server read path streams through a ChunkStream instead and never
// calls this for a reply.
func (s *Store) readChunked(addr uint64, dst []byte) ([]byte, bool) {
	total := int(s.vlen(addr))
	word, n, _ := s.readPtr(s.valueStart(addr))
	dirOff := word & runAddrMask
	if cap(dst) < total {
		dst = make([]byte, total)
	}
	dst = dst[:total]
	pos := 0
	for k := uint32(0); k < n; k++ {
		w, l, _ := s.readPtr(dirOff + uint64(k)*ptrSize)
		clen := int(l)
		if w == 0 {
			clear(dst[pos : pos+clen])
		} else if w&inLogBit != 0 {
			if err := s.logReadFill(w&runAddrMask, dst[pos:pos+clen]); err != nil {
				return dst[:0], false
			}
		} else {
			run := w & runAddrMask
			copy(dst[pos:pos+clen], s.arena.buf[run:run+uint64(clen)])
		}
		pos += clen
	}
	return dst, true
}

// updateChunked patches a chunked record in place, chunk-bounded: only the
// chunks the write touches (the patch range, the zero-fill gap, and any
// extension) are composed and rewritten, everything else keeps its run. An
// arena chunk with capacity takes the patch in its run; a log chunk is
// immutable and rewrites. The directory grows with headroom when the value
// takes more chunks. offset+len(val) drives newLen; the caller guarantees
// newLen >= oldLen (a full replace goes through SetString, which re-selects
// the band from scratch). A placement failure mid-walk surfaces the error
// with the chunks before it already patched: the multi-chunk write has no
// atomicity promise once the store itself cannot place bytes.
func (s *Store) updateChunked(addr uint64, offset int, val []byte, oldLen, newLen int) error {
	vs := s.valueStart(addr)
	word, oldN, dcap := s.readPtr(vs)
	dirOff := word & runAddrMask
	newN := chunkCount(uint64(newLen))
	if newN > dcap {
		grow := newN + newN/2
		if maxN := chunkCount(maxValueLen); grow > maxN {
			grow = maxN
		}
		nd, ok := s.arenaAlloc(uint64(grow) * ptrSize)
		if !ok {
			return ErrFull
		}
		copy(s.arena.buf[nd:nd+uint64(oldN)*ptrSize], s.arena.buf[dirOff:dirOff+uint64(oldN)*ptrSize])
		s.arena.unlink(dirOff, uint64(dcap)*ptrSize)
		dirOff, dcap = nd, grow
		s.writePtr(vs, dirOff, oldN, dcap)
	}
	end := offset + len(val)
	buf := s.arena.buf
	for k := uint32(0); k < newN; k++ {
		base := int(k) * strChunkSize
		clen := newLen - base
		if clen > strChunkSize {
			clen = strChunkSize
		}
		oldClen := 0
		if k < oldN {
			oldClen = oldLen - base
			if oldClen > strChunkSize {
				oldClen = strChunkSize
			}
		}
		// Untouched when the patch misses the chunk and its length holds.
		if oldClen == clen && (end <= base || offset >= base+clen) {
			continue
		}
		es := dirOff + uint64(k)*ptrSize
		if oldClen > 0 {
			cw, _, cc := s.readPtr(es)
			if cw&inLogBit == 0 && clen <= int(cc) {
				// Patch the arena run in place.
				run := cw & runAddrMask
				if oldClen < clen {
					clear(buf[run+uint64(oldClen) : run+uint64(clen)])
				}
				overlayPatch(buf[run:run+uint64(clen)], base, offset, val)
				s.writePtr(es, cw, uint32(clen), cc)
				continue
			}
			// Rewrite the chunk: existing bytes into the stage, gap zeroed,
			// patch overlaid, then a fresh run replaces the old one. A
			// partial final chunk keeps append headroom under the separated
			// growth rule, capped at the chunk width.
			dst := s.stage()[:clen]
			if cw == 0 {
				// The old chunk was a hole (all zeros, no run); the existing
				// bytes are zero, and there is no run to read or drop.
				clear(dst[:oldClen])
			} else if cw&inLogBit != 0 {
				if err := s.logReadFill(cw&runAddrMask, dst[:oldClen]); err != nil {
					return err
				}
			} else {
				run := cw & runAddrMask
				copy(dst[:oldClen], buf[run:run+uint64(oldClen)])
			}
			if oldClen < clen {
				clear(dst[oldClen:clen])
			}
			overlayPatch(dst, base, offset, val)
			capB := uint64(0)
			if clen < strChunkSize {
				capB = growSepCap(uint64(clen), uint64(cc))
				if capB > strChunkSize {
					capB = strChunkSize
				}
			}
			nw, nc, err := s.writeRun(dst, nil, capB)
			if err != nil {
				return err
			}
			ow, ol, oc := s.readPtr(es)
			if ow != 0 {
				s.dropRun(ow, ol, oc)
			}
			s.writePtr(es, nw, uint32(clen), nc)
			continue
		}
		// Fresh chunk past the old end. A gap chunk the patch never reaches is
		// all zeros and stays a hole, so extending to a high offset stores only
		// the live covering chunk.
		if end <= base || offset >= base+clen {
			s.writePtr(es, 0, uint32(clen), 0)
			continue
		}
		var a []byte
		if offset <= base && end >= base+clen {
			a = val[base-offset : base-offset+clen]
		} else {
			a = s.composeChunk(base, clen, nil, offset, val)
		}
		nw, nc, err := s.writeRun(a, nil, 0)
		if err != nil {
			return err
		}
		s.writePtr(es, nw, uint32(clen), nc)
	}
	s.writePtr(vs, dirOff, newN, dcap)
	s.setRecFlags(addr, s.recFlags(addr)|flagRawSticky)
	s.setVlen(addr, uint32(newLen))
	s.chunkBytes += uint64(newLen) - uint64(oldLen)
	return nil
}

// allocChunked lays down a fresh chunked record over the logical patched
// value, the create and band-transition twin of allocSep. The record is
// unlinked again if the chunk build fails.
func (s *Store) allocChunked(key, old []byte, offset int, val []byte, newLen int, flags byte, at, now int64) (uint64, error) {
	off, err := s.allocString(key, ptrSize, flags|flagChunked, at, now)
	if err != nil {
		return 0, err
	}
	dirOff, n, err := s.writeChunked(old, offset, val, newLen)
	if err != nil {
		s.arena.unlink(off, s.recBytes(off))
		return 0, err
	}
	s.writePtr(s.valueStart(off), dirOff, n, n)
	s.setVlen(off, uint32(newLen))
	s.chunkBytes += uint64(newLen)
	return off, nil
}

// chunkRef is one chunk's location snapshot inside a ChunkStream.
type chunkRef struct {
	word uint64
	clen uint32
}

// ChunkStream reads one chunked value chunk by chunk, the streaming reply's
// source. It snapshots the chunk locations at open and pins the store's
// arena while it lives: the arena compactor and the write path's full-arena
// backstop both refuse to free or move segments while any stream is open
// (store.openStreams), and the log is append-only with CompactLog gated to
// the same stream-free idle boundary, so the snapshot stays readable across
// later writes to the same key. The shard's stream pump calls Release when
// the stream finishes, fails, or aborts.
type ChunkStream struct {
	s     *Store
	refs  []chunkRef
	total int64
	k     int
}

// Total is the value's byte length, the bulk header the streamed reply
// carries.
func (cs *ChunkStream) Total() int64 { return cs.total }

// Release drops the stream's pin on the store's arena; the compactor may
// move or free segments again once every open stream has released.
// Idempotent, and owner-goroutine only like every other store touch.
func (cs *ChunkStream) Release() {
	if cs.s != nil {
		cs.s.openStreams--
		cs.s = nil
	}
}

// Next copies the next chunk into dst and returns its length, zero once the
// value is exhausted. dst must be at least ChunkSize bytes. It runs on the
// shard owner (the stream pump), so the store access is single-owner like
// every other read.
func (cs *ChunkStream) Next(dst []byte) (int, error) {
	if cs.k >= len(cs.refs) {
		return 0, nil
	}
	r := cs.refs[cs.k]
	clen := int(r.clen)
	if r.word == 0 {
		clear(dst[:clen])
	} else if r.word&inLogBit != 0 {
		// Through the store's read seam, so a chunk that spilled to the shared
		// .aki value region resolves there just as an arena chunk resolves in
		// the arena. cs.s is live until Release, and the stream pins the arena
		// and gates compaction, so the seam's routing is stable for the read.
		if err := cs.s.logReadFill(r.word&runAddrMask, dst[:clen]); err != nil {
			return 0, err
		}
	} else {
		run := r.word & runAddrMask
		copy(dst[:clen], cs.s.arena.buf[run:run+uint64(clen)])
	}
	cs.k++
	return clen, nil
}

// GetStream is GetString with the chunked band split out for streaming: a
// chunked value comes back as a ChunkStream (and no bytes), everything else
// as the materialized value with a nil stream. The point bands pay one flag
// check over GetString and allocate nothing; the stream is an allocation the
// giant band accepts.
func (s *Store) GetStream(key []byte, now int64, dst []byte) ([]byte, *ChunkStream, bool) {
	h := Hash(key)
	_, addr, _ := s.findResident(h, key, now)
	if addr == 0 {
		return dst[:0], nil, false
	}
	if s.recFlags(addr)&flagChunked != 0 {
		return dst[:0], s.chunkStreamAt(addr), true
	}
	v, ok := s.readValue(addr, dst)
	return v, nil, ok
}

// chunkStreamAt snapshots a chunked record's directory into a fresh
// ChunkStream, the one allocation the giant band accepts.
func (s *Store) chunkStreamAt(addr uint64) *ChunkStream {
	word, n, _ := s.readPtr(s.valueStart(addr))
	dirOff := word & runAddrMask
	cs := &ChunkStream{s: s, total: int64(s.vlen(addr)), refs: make([]chunkRef, n)}
	s.openStreams++
	for k := uint32(0); k < n; k++ {
		w, l, _ := s.readPtr(dirOff + uint64(k)*ptrSize)
		cs.refs[k] = chunkRef{word: w, clen: l}
	}
	return cs
}
