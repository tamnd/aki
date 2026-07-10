package store

import (
	"errors"
)

// ErrFull is returned by Set when no arena segment has room for a new record.
// Raising the arena bytes (or, once the cold tier lands, its capacity) is the
// response.
var ErrFull = errors.New("store: arena full")

// ErrTooBig is returned when a key or value exceeds the 64 KiB field width.
// Values past the embed threshold move to extents in a later slice.
var ErrTooBig = errors.New("store: key or value over 64 KiB")

var errEmptyKey = errors.New("store: empty key")

// Store is one shard's memory engine: the segment-split index over the
// segmented bump arena. It belongs to exactly one goroutine; nothing in it is
// safe for concurrent use, on purpose, because the single-owner contract is
// what deletes the whole coordination category from the hot path.
type Store struct {
	arena arena
	idx   index
	count int64
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

// Reset drops every key and rewinds the arena, the flush path. Quiesced by
// construction: the owner calls it between commands.
func (s *Store) Reset() {
	s.idx = newIndex()
	s.arena.reset()
	s.count = 0
}
