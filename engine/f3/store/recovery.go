package store

import (
	"fmt"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// The record-log recovery consumer: the read side of the M8 durable arc. The seam
// (recordseam.go) makes every string commit durable by framing its record row into
// the shared .aki record log; this walks that log back on open and reapplies each
// row to a fresh store, so the index a crash left in the volatile arena is rebuilt
// from the one durable copy that survived. It is the tail replay of doc 07 section
// 6 step 7 in its interim, checkpoint-free form: it walks from the start of the
// append space rather than from a checkpoint's log position, because the checkpoint
// producer that would let it skip the settled prefix is the next slice. Every row
// is reapplied, in append order, idempotently, so a later write always wins over
// the superseded row it replaced.
//
// Replay routes each row back through the ordinary commit path, SetString for a
// value row and Del for a tombstone, so the rebuilt index is byte-for-byte the one
// a live run would hold: the same band selection, the same dead-byte accounting,
// the same expiry layout. The one difference from a live command is that replay
// must not re-log what it reads back, which the replaying guard on logRecord and
// logTombstone enforces, so the walk does not double the log or mis-count its dead
// bytes.

// ReplayRecords rebuilds this store's index by walking the shared .aki record log
// from the start of the append space and reapplying every framed record in append
// order. An inline row's value bytes ride the frame, so replay reinserts them
// directly; a separated row's bytes live in the durable value log, so replay derefs
// its word and reads them back; a tombstone clears its key. now is the wall clock
// the reapplied writes stamp their lazy-expiry checks against, the same value a
// live command would pass. On a store with no record log it is a no-op.
//
// A chunked row, or a separated row whose word never reached the value log, is
// deferred: recovery of a multi-chunk value needs the chunk-directory walk a later
// slice adds, and an arena-resident run does not survive a restart to be read back.
// Either stops the replay with an error rather than resurrecting a partial value,
// the same fail-closed a torn frame takes.
func (s *Store) ReplayRecords(now int64) error {
	if s.akirlog == nil {
		return nil
	}
	s.replaying = true
	defer func() { s.replaying = false }()

	var vbuf []byte
	return s.akirlog.f.WalkRecords(akifile.PageSize, func(addr uint64, row akifile.RecordRow) error {
		switch {
		case row.Flags&akifile.RecFlagTombstone != 0:
			s.Del(row.Key, now)
			return nil
		case row.Flags&akifile.RecFlagChunked != 0:
			return fmt.Errorf("store: replay of chunked record at %#x is not supported yet", addr)
		case row.Flags&akifile.RecFlagInline != 0:
			return s.SetString(row.Key, row.Value, now, int64(row.ExpireAt), false)
		default:
			// Separated: the durable bytes are the log-resident run its word names.
			// An arena-resident word points at bytes a restart already dropped, so
			// it cannot be reapplied and stops the replay rather than reinserting a
			// stale or empty value.
			if row.ValueWord&inLogBit == 0 {
				return fmt.Errorf("store: replay of arena-resident record at %#x has no durable value", addr)
			}
			v, err := s.logReadInto(row.ValueWord&runAddrMask, int(row.ValueLen), vbuf)
			if err != nil {
				return err
			}
			vbuf = v
			return s.SetString(row.Key, v, now, int64(row.ExpireAt), false)
		}
	})
}
