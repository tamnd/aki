package store

import (
	"errors"
	"runtime"
)

// ErrFull is returned by Set when no arena segment has room for a new record.
// Raising the arena bytes (or, once the cold tier lands, its capacity) is the
// response.
var ErrFull = errors.New("store: arena full")

// ErrTooBig is returned when a key exceeds the 64 KiB field width or a value
// the 512MiB proto-max-bulk-len ceiling.
var ErrTooBig = errors.New("store: key or value over the size cap")

var errEmptyKey = errors.New("store: empty key")

// Store is one shard's memory engine: the segment-split index over the
// segmented bump arena, plus the shard's value log when one is configured. It
// belongs to exactly one goroutine; nothing in it is safe for concurrent use,
// on purpose, because the single-owner contract is what deletes the whole
// coordination category from the hot path.
type Store struct {
	arena arena
	idx   index
	count int64

	// The value-log half (doc 09 section 8): nil without a log. residentCap
	// is the resident byte budget; past it a separated or chunked value's
	// bytes spill to the log.
	vlog        *vlog
	residentCap uint64

	// The cold tier (doc 06 sections 2 and 7, cold.go): nil without a cold
	// region. cold is the per-shard append log of whole-record cold frames the
	// migrator demotes out of the arena; coldRecs is the live cold-record
	// count, the census figure the arena band counts exclude (a demoted record
	// leaves its resident band and joins this count). coldBuf and frameBuf are
	// the owner-only scratch the cold read, compare, and frame paths reuse.
	cold     *vlog
	coldRecs uint64
	coldBuf  []byte
	frameBuf []byte

	// coldHand is the whole-record migrator's clock position (migrate.go), the
	// directory index its bounded pass resumes from. Separate from demoteHand:
	// the residency hand moves separated value runs to the log, this hand moves
	// whole int and embedded records out of the arena into the cold region, and
	// the two walk independently.
	coldHand uint64

	// migrating counts the records the async migrator (coldstage.go) has staged
	// into in-flight cold drains but not yet flipped or dropped. It gates the
	// findResident stale-flip interlock: at zero, a foreground write skips the
	// flagMigrating check entirely, so the no-pressure path pays one field
	// compare and nothing more. Phase 1 adds a staged record, phase 2 removes it
	// whether it flipped, dropped, or was cancelled by a racing write.
	migrating int

	// drained lists the arena segments the async migrator's phase 2 emptied
	// this boundary: a flip that unlinked the last live record of a segment
	// appends its index here (markDrained, deduped). The shard worker takes the
	// list at the boundary, stamps each with the current epoch, and retires it
	// through the F6 reclamation path (RetireSegment) rather than letting the
	// compactor free it outright, so a segment a batch in flight could still name
	// waits the bracket out. Nil and untouched until the migrator empties its
	// first segment, so the resident path never allocates it.
	drained []uint64

	// The residency machinery (resid.go). ltmOn folds the whole
	// configuration check into one load for the read path; residMode is the
	// promotion policy (labs override it); markAlways is lab 15's
	// always-store mark variant; demoteHand is the clock hand's
	// directory position; the counters are the ResidStats surface.
	ltmOn      bool
	residMode  int
	markAlways bool
	dkDen      uint64
	dkRng      uint64
	demoteHand uint64
	promotes   uint64
	demotes    uint64
	logReads   uint64

	// spillLogDirect is the cold-overwrite placement policy (resid.go's
	// spillCold): with the live charge past the demotion low-water mark, an
	// overwrite of a log-resident separated value appends straight to the log
	// instead of round-tripping through the arena and the demotion hand.
	// Shipped on; TuneSpillPlacement overrides it for the lab 17 sweep.
	spillLogDirect bool

	// Band census and log-run count, plain single-owner counters.
	bands   [4]uint64
	logRuns uint64

	// chunkBytes is the live chunked-band value bytes: charged against the
	// record's value length when a chunked record publishes or grows,
	// credited against it when the record leaves the index. Placement does
	// not matter; arena chunks and logged chunks both count, because the
	// figure answers "how many value bytes live in the giant band", not
	// "where are they".
	chunkBytes uint64

	// segDeadNum/segDeadDen is the arena compactor's per-segment dead-fraction
	// threshold, defaulted from the frozen lab constant; TuneArenaReclaim
	// overrides it for the lab sweep.
	segDeadNum uint64
	segDeadDen uint64

	// openStreams counts live ChunkStreams: each holds a snapshot of chunk
	// run addresses, so while any is open no arena segment may be freed or
	// compacted. chunkStreamAt pins, ChunkStream.Release unpins, both on the
	// owner goroutine.
	openStreams int

	// victims and seen are the compactor's reusable scratch: the victim mask
	// by arena segment and the visited mask by index segment.
	victims []bool
	seen    []bool

	// vbuf is the store's value scratch for paths that must materialize a
	// run (log-resident reads inside a rewrite); grown capacity is kept.
	// cbuf is the chunk staging buffer, one chunk wide, allocated on the
	// first chunked write so stores that never see a giant value never pay
	// for it.
	// zbuf is a chunk-wide all-zero buffer the bit-range walk yields for a
	// hole chunk; it is never written, so the read path can alias it.
	vbuf []byte
	cbuf []byte
	zbuf []byte
}

// Options configures a store beyond the arena geometry.
type Options struct {
	ArenaBytes int
	SegBytes   int

	// VlogPath enables the per-shard value log at this path (created,
	// truncating any prior file).
	VlogPath string

	// ResidentCapBytes is the resident byte budget; 0 means uncapped. Only
	// meaningful with a value log.
	ResidentCapBytes uint64

	// ColdPath enables the per-shard cold region at this path (created,
	// truncating any prior file). Without it the migrator has nowhere to
	// demote and DemoteCold is a no-op.
	ColdPath string
}

// New builds a store whose arena holds arenaBytes, tiled into segments of
// segBytes (non-positive segBytes takes the default). The index starts at one
// segment and grows by splitting, so there is no bucket-count parameter and no
// index ceiling short of the directory depth cap.
func New(arenaBytes, segBytes int) *Store {
	s := &Store{
		arena:      newArena(arenaBytes, segBytes),
		idx:        newIndex(),
		segDeadNum: arenaSegDeadNum,
		segDeadDen: arenaSegDeadDen,
	}
	if s.arena.mapped {
		// The arena backing lives outside the Go heap (arena_map_unix.go), so
		// the GC cannot release it; the finalizer does what dropping the last
		// reference to a heap slice used to. It fires exactly when the buffer
		// would have been collected, so no live store can lose its arena, and
		// Close keeps its narrow contract (the log only).
		runtime.SetFinalizer(s, func(st *Store) { arenaUnmap(st.arena.buf) })
	}
	return s
}

// Open is New plus the value-log configuration.
func Open(o Options) (*Store, error) {
	s := New(o.ArenaBytes, o.SegBytes)
	if o.VlogPath != "" {
		l, err := openVlog(o.VlogPath)
		if err != nil {
			return nil, err
		}
		s.vlog = l
		s.residentCap = o.ResidentCapBytes
		s.ltmOn = s.residentCap > 0
		s.spillLogDirect = true
		s.dkDen = residDoorkeeperDen
		s.dkRng = 0x9e3779b97f4a7c15
	}
	if o.ColdPath != "" {
		// The cold region is an append log identical in mechanism to the value
		// log (append, pread, random-advise); its framing and liveness are the
		// migrator's, defined in cold.go, not CompactLog's. A separate instance
		// so a value-log rewrite never touches a cold frame.
		c, err := openVlog(o.ColdPath)
		if err != nil {
			if s.vlog != nil {
				_ = s.vlog.close()
			}
			return nil, err
		}
		s.cold = c
		s.reserveColdNull()
	}
	return s, nil
}

// Close releases the value log and the cold region, if any. The arena is plain
// memory.
func (s *Store) Close() error {
	var err error
	if s.vlog != nil {
		err = s.vlog.close()
		s.vlog = nil
	}
	if s.cold != nil {
		if cerr := s.cold.close(); err == nil {
			err = cerr
		}
		s.cold = nil
	}
	return err
}

// Len reports the number of live keys.
func (s *Store) Len() int { return int(s.count) }

// Splits reports how many index segment splits have run, a ledger figure the
// growth tests and INFO read.
func (s *Store) Splits() uint64 { return s.idx.splits }

// ArenaBytes reports the arena's handed-out and total bytes, the resident fill
// INFO surfaces.
func (s *Store) ArenaBytes() (used, total uint64) {
	return s.arena.used(), uint64(len(s.arena.buf))
}

// Get copies the value for key into dst (reusing its capacity) and reports
// whether the key is present. Clockless form of GetString: it never reaps, so
// it is for callers with no expiry semantics.
func (s *Store) Get(key, dst []byte) ([]byte, bool) {
	return s.GetString(key, 0, dst)
}

// Set stores val under key, blind-upsert semantics, no deadline. Clockless
// form of SetString.
func (s *Store) Set(key, val []byte) error {
	return s.SetString(key, val, 0, 0, false)
}

// Delete removes key and reports whether it was present. The entry word is
// zeroed in place; the probe tolerates the hole, so nothing shifts. The record
// bytes stay valid until their segment is freed, and their charge leaves the
// segment's live counter now so a fully dead segment reads as drained.
func (s *Store) Delete(key []byte) bool {
	return s.Del(key, 0)
}

// Reset drops every key and rewinds the arena, the flush path (FLUSHALL rides
// this). Quiesced by construction: the owner calls it between commands. The
// index is rebuilt from scratch so its grown tables go back to the GC, the
// arena hands its touched pages back to the OS, and the scratch buffers are
// dropped: a flush must actually return the memory, not just zero the
// counters, or the resident footprint carries the old dataset forever. The
// value log rewinds with it; the truncate is best-effort, since a stale tail
// past the rewound cursor is unreachable either way.
func (s *Store) Reset() {
	s.idx = newIndex()
	s.arena.reset()
	s.count = 0
	s.bands = [4]uint64{}
	s.logRuns = 0
	s.chunkBytes = 0
	s.coldRecs = 0
	s.coldHand = 0
	s.migrating = 0
	s.drained = s.drained[:0]
	s.vbuf = nil
	s.cbuf = nil
	s.zbuf = nil
	s.coldBuf = nil
	s.frameBuf = nil
	s.demoteHand = 0
	s.promotes = 0
	s.demotes = 0
	s.logReads = 0
	if s.vlog != nil {
		_ = s.vlog.f.Truncate(0)
		s.vlog.tail = 0
		s.vlog.wtail = 0
		s.vlog.pending = nil
		s.vlog.werr = nil
		s.vlog.dead = 0
	}
	if s.cold != nil {
		_ = s.cold.f.Truncate(0)
		s.cold.tail = 0
		s.cold.wtail = 0
		s.cold.pending = nil
		s.cold.werr = nil
		s.cold.dead = 0
		s.reserveColdNull()
	}
}
