package store

import (
	"errors"
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

	// vbuf is the store's value scratch for paths that must materialize a
	// run (log-resident reads inside a rewrite); grown capacity is kept.
	// cbuf is the chunk staging buffer, one chunk wide, allocated on the
	// first chunked write so stores that never see a giant value never pay
	// for it.
	vbuf []byte
	cbuf []byte
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
}

// New builds a store whose arena holds arenaBytes, tiled into segments of
// segBytes (non-positive segBytes takes the default). The index starts at one
// segment and grows by splitting, so there is no bucket-count parameter and no
// index ceiling short of the directory depth cap.
func New(arenaBytes, segBytes int) *Store {
	return &Store{
		arena: newArena(arenaBytes, segBytes),
		idx:   newIndex(),
	}
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
	}
	return s, nil
}

// Close releases the value log, if any. The arena is plain memory.
func (s *Store) Close() error {
	if s.vlog == nil {
		return nil
	}
	err := s.vlog.close()
	s.vlog = nil
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
	s.vbuf = nil
	s.cbuf = nil
	if s.vlog != nil {
		_ = s.vlog.f.Truncate(0)
		s.vlog.tail = 0
		s.vlog.dead = 0
	}
}
