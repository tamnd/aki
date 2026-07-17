package store

import "github.com/tamnd/aki/engine/f3/akifile"

// The record-log seam: the store side of durable append, the coupled core of the
// M8 arc. The value-log flip (#1067) made a separated value's bytes durable, but
// the record itself, the key plus its value locator, expiry, and flags, was never
// written anywhere but the volatile arena, so a crash lost the index. This seam
// closes that: every string commit on an .aki-backed store also stages its record
// row into the shared file's record log and cuts it, so recovery has a durable
// row to re-derive the index entry from.
//
// It rides the same publish-after-durable edge the whole doc reduces to (doc 07
// section 8, step 6 before step 7): an address a reader can observe must point at
// bytes that survive a crash. The interim form here cuts eagerly, one stage plus
// one flush per commit, mirroring the value log's interim spillRun: the record's
// log address is absolute before the command returns and no provisional record
// address ever escapes, so the edge holds by construction. The batched form, one
// cut per group at the reactor's commit boundary through a ledger, is the same
// reactor-side perf follow-up the value log's batched spill waits on, and it is
// box-gated, not this slice's.
//
// Scope: the string type's three commit points (a fresh or replaced record, and
// the two in-place overwrite branches). It is default off, so a store with no
// .aki handle keeps the pure in-memory index unchanged and pays nothing. Recovery
// is not wired yet, so a logged row is written but never read back until PR 6
// consumes it; that is why an inline value's locator can still be a volatile arena
// offset here without consequence (a durable capture of inline bytes is recovery's
// obligation to confront, not this slice's). Delete and expiry tombstones, the
// other collection types, and reopen are the sibling slices this one clears the
// path for.

// recordRow reads the durable-relevant fields of the record at off into a row the
// record log frames: its value locator, current value length, absolute expiry,
// and key. A separated or chunked record's locator is the payload pointer word
// (durable when the run is log-resident); an inline or int record's is the arena
// value offset, a volatile locator recovery replaces with a durable capture. The
// key aliases the arena, which is safe because stage copies it into the frame the
// instant it returns.
func (s *Store) recordRow(off uint64) akifile.RecordRow {
	f := s.recFlags(off)
	vs := s.valueStart(off)
	var word uint64
	if f&(flagSep|flagChunked) != 0 {
		word, _, _ = s.readPtr(vs)
	} else {
		word = vs
	}
	var at uint64
	if f&flagHasTTL != 0 {
		at = uint64(s.expireAt(off))
	}
	return akifile.RecordRow{
		ValueWord: word,
		ValueLen:  uint32(s.vlen(off)),
		ExpireAt:  at,
		Key:       s.keyAt(off),
	}
}

// logRecord durably appends the record at off to the shared .aki record log, the
// step that makes a string commit survivable. On a store with no record log it is
// a no-op that returns nil, so the volatile-index path is byte-for-byte unchanged
// and the default configuration pays one nil check. On the .aki path it stages the
// row and cuts eagerly, so the record is durable and its log address absolute
// before the command's caller sees success; a cut failure surfaces as the
// command's error, the same way a value-log spill failure already does.
func (s *Store) logRecord(off uint64) error {
	if s.akirlog == nil {
		return nil
	}
	s.akirlog.stage(s.recordRow(off))
	_, err := s.akirlog.flush()
	return err
}

// logTombstone durably appends a clear record for key, so a replay that meets it
// removes the entry instead of resurrecting the deleted value: without it the log
// records every SET but no delete, and replay lands on a key the client dropped. A
// tombstone carries only the key and the tombstone flag, no value or expiry. On a
// store with no record log it is a no-op.
//
// The delete and expiry paths answer with a boolean, not an error, so a failed cut
// cannot return through them; it is held in rlogErr for the ack-barrier path to
// surface. The dead-byte accounting is not this call's: the caller's dropRecord
// already charged the superseded bytes.
func (s *Store) logTombstone(key []byte) {
	if s.akirlog == nil {
		return
	}
	s.akirlog.stage(akifile.RecordRow{Flags: akifile.RecFlagTombstone, Key: key})
	if _, err := s.akirlog.flush(); err != nil && s.rlogErr == nil {
		s.rlogErr = err
	}
}

// TakeRecordLogErr returns and clears the first durability fault a tombstone cut
// raised, or nil. The ack-barrier path reads it before releasing a durability-
// requiring ack, so a delete whose tombstone never reached the disk fails the
// command rather than acking a loss. Owner goroutine only.
func (s *Store) TakeRecordLogErr() error {
	err := s.rlogErr
	s.rlogErr = nil
	return err
}
