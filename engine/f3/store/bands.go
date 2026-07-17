package store

import "encoding/binary"

// The value bands (spec 2064/f3/09 section 2). A record's value area holds
// one of four shapes, selected by length at write time and re-selected from
// scratch on every full replace:
//
//	V_INT      8-byte cell                          canonical integer text
//	embedded   the bytes, vcap words of capacity    len <= strInlineMax
//	separated  16-byte run pointer                  strInlineMax < len < strChunkMin
//	chunked    16-byte pointer to a chunk directory len >= strChunkMin
//
// A separated or chunked value's bytes live in a run outside the record:
// in the arena while the resident budget allows, in the shard's value log
// once it does not. The run pointer is 16 bytes: a word whose top bit says
// log-or-arena and whose low 48 bits are the offset, then the value length
// and the run's reserved capacity.
const (
	// strInlineMax is the embedded band's cap: the settled inline-threshold
	// lab (values win inline to 512B with the knee at 1KiB). Embedded growth
	// doubles up to here and moves to the separated band past it.
	strInlineMax = 1024

	// strChunkMin is the chunked band's threshold, contract-bound to the
	// streaming reply path: at and past it a value is stored and served in
	// chunks and never materialized whole on the reply path.
	strChunkMin = 64 << 10

	// strChunkSize is the chunk width, equal to the threshold by the settled
	// giant-value lab.
	strChunkSize = 64 << 10

	// ptrSize is the separated/chunked value area: one 16-byte run pointer.
	ptrSize = 16

	// inLogBit in a run word means the offset addresses the value log, not
	// the arena.
	inLogBit = uint64(1) << 63

	// runAddrMask extracts the 48-bit offset from a run word.
	runAddrMask = (uint64(1) << addrBits) - 1
)

// maxValueLen is the proto-max-bulk-len value ceiling (512MiB), the bound the
// chunked band accepts. Keys keep the 64KiB header field width.
const maxValueLen = 512 << 20

// readPtr reads the run pointer at value-area offset vs.
func (s *Store) readPtr(vs uint64) (word uint64, vlen, vcap uint32) {
	buf := s.arena.buf
	word = binary.LittleEndian.Uint64(buf[vs:])
	vlen = binary.LittleEndian.Uint32(buf[vs+8:])
	vcap = binary.LittleEndian.Uint32(buf[vs+12:])
	return
}

// writePtr writes the run pointer at value-area offset vs.
func (s *Store) writePtr(vs uint64, word uint64, vlen, vcap uint32) {
	buf := s.arena.buf
	binary.LittleEndian.PutUint64(buf[vs:], word)
	binary.LittleEndian.PutUint32(buf[vs+8:], vlen)
	binary.LittleEndian.PutUint32(buf[vs+12:], vcap)
}

// spillNow reports whether n more resident bytes would cross the shard's
// resident budget, the doc 09 section 8 RAM-exceeded rule: past the cap a
// separated or chunked value's bytes go to the log instead of the arena. Only
// the value bytes move; the record, header, and key always stay resident.
//
// The budget is charged against the live bytes, not the fill: dead bytes are
// the compactor's debt, and gating admission on them deadlocks the resident
// set. With a fill gate, a fill parked just over the cap by dead bytes too
// thin for any segment to be a victim blocks every promotion and every fresh
// write forever, and the hot set can never form (lab 13's cap=1GiB cell hit
// exactly this: zero promotions, the miss rate frozen at the fill-order
// prefix). The fill excess over live is bounded by the compaction trigger at
// the owner boundaries (ResidentOver fires on fill), so the cap still bounds
// the touched pages, it just does not let garbage crowd out the working set.
func (s *Store) spillNow(n uint64) bool {
	return s.vlog != nil && s.residentCap > 0 && s.arena.live()+n > s.residentCap
}

// writeRun places a value run of a and then b (b may be nil), reserving capB
// bytes of capacity in the arena case; a log run is immutable so its capacity
// is exactly its length. The two-part form exists so APPEND can build
// old+add without assembling a contiguous copy first. Falls back to the log
// when the arena is full and a log exists: past the budget the store degrades
// to slower placement, it does not refuse writes it could take.
func (s *Store) writeRun(a, b []byte, capB uint64) (word uint64, vcap uint32, err error) {
	n := uint64(len(a) + len(b))
	if capB < n {
		capB = align8(n)
	}
	if !s.spillNow(capB) {
		if off, ok := s.arenaAlloc(capB); ok {
			copy(s.arena.buf[off:], a)
			copy(s.arena.buf[off+uint64(len(a)):], b)
			return off, uint32(capB), nil
		}
		if s.vlog == nil {
			return 0, 0, ErrFull
		}
	}
	if s.vlog == nil {
		return 0, 0, ErrFull
	}
	off, err := s.vlog.append(a)
	if err != nil {
		return 0, 0, err
	}
	if len(b) > 0 {
		// Contiguous with a by append's contract: single owner, no
		// interleaving appender exists.
		if _, err := s.vlog.append(b); err != nil {
			// The a bytes are already appended; they become dead space the
			// next compaction drops.
			s.logUnlink(uint64(len(a)))
			return 0, 0, err
		}
	}
	s.logRuns++
	return inLogBit | off, uint32(n), nil
}

// dropRun releases one value run: a log run's bytes become dead space the
// compactor reclaims, an arena run's bytes charge back to their segment. The
// log-run counter moves here, mirror to writeRun, so the pair balances at
// every placement and release site.
func (s *Store) dropRun(word uint64, vlen, vcap uint32) {
	if word&inLogBit != 0 {
		s.logUnlink(uint64(vlen))
		s.logRuns--
		return
	}
	s.arena.unlink(word&runAddrMask, uint64(vcap))
}

// dropValue releases whatever the record's value area points at outside the
// record itself. Embedded and int bands own no outside bytes; a separated
// record owns one run; a chunked record owns a directory and its chunks.
func (s *Store) dropValue(addr uint64) {
	f := s.recFlags(addr)
	if f&flagChunked != 0 {
		s.dropChunks(addr)
		return
	}
	if f&flagSep == 0 {
		return
	}
	word, vlen, vcap := s.readPtr(s.valueStart(addr))
	s.dropRun(word, vlen, vcap)
}

// dropRecord is the one exit for a record leaving the index: band accounting,
// outside value bytes, then the record's own arena charge.
func (s *Store) dropRecord(addr uint64) {
	s.noteDrop(s.recFlags(addr))
	s.dropValue(addr)
	s.arena.unlink(addr, s.recBytes(addr))
}

// BandStats is the per-band live-record census plus the log-resident run
// count, the evidence surface the LTM harness reads through INFO.
type BandStats struct {
	Int       uint64
	Embedded  uint64
	Separated uint64
	Chunked   uint64

	// LogRuns counts live value runs (a separated value's one run, a chunked
	// value's chunks each) whose bytes sit in the value log rather than the
	// arena.
	LogRuns uint64
}

func bandIdx(f byte) int {
	switch {
	case f&flagInt != 0:
		return 0
	case f&flagChunked != 0:
		return 3
	case f&flagSep != 0:
		return 2
	default:
		return 1
	}
}

func (s *Store) noteNew(f byte)  { s.bands[bandIdx(f)]++ }
func (s *Store) noteDrop(f byte) { s.bands[bandIdx(f)]-- }

// noteFlip re-banded a record in place (int cell to raw bytes or back).
func (s *Store) noteFlip(oldF, newF byte) {
	if bandIdx(oldF) != bandIdx(newF) {
		s.bands[bandIdx(oldF)]--
		s.bands[bandIdx(newF)]++
	}
}

// Stats reports the band census and log-run count.
func (s *Store) Stats() BandStats {
	return BandStats{
		Int:       s.bands[0],
		Embedded:  s.bands[1],
		Separated: s.bands[2],
		Chunked:   s.bands[3],
		LogRuns:   s.logRuns,
	}
}
