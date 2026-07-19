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

// ReplayRecords rebuilds this store's index by walking this shard's record log
// from the start of the append space and reapplying every framed record in append
// order. The append space interleaves every shard's segments, so the walk skips
// the other shards and replays only the records this store's shard cut. An inline
// row's value bytes ride the frame, so replay reinserts them
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
	return s.replayFrom(akifile.PageSize, now)
}

// ReplayTail rebuilds the tail of this store's index by walking only the records
// this shard cut at or past fromOff, the records a checkpoint's dump did not cover.
// It is the second half of a bounded recovery: ReplayFromCheckpoint loads the
// settled prefix from a dump, then this replays the log past the point the dump was
// consistent up to, so a restart never rescans the records the checkpoint already
// folded. fromOff is the append offset captured when the checkpoint was built. On a
// store with no record log it is a no-op.
func (s *Store) ReplayTail(fromOff uint64, now int64) error {
	if s.akirlog == nil {
		return nil
	}
	return s.replayFrom(fromOff, now)
}

// replayFrom walks this shard's record log from fromOff and reapplies each framed
// record in append order, a tombstone as a clear and a value row through
// applyValueRow. It holds the replaying guard for the walk so a reapplied write
// does not re-log the row it is reading back. It is the shared body of the full-log
// ReplayRecords and the tail-only ReplayTail, which differ only in where the walk
// starts.
func (s *Store) replayFrom(fromOff uint64, now int64) error {
	s.replaying = true
	defer func() { s.replaying = false }()

	var vbuf []byte
	return s.akirlog.walkShardFrom(fromOff, func(addr uint64, row akifile.RecordRow) error {
		// Collection frames share this log but rebuild through WalkCollection, not the
		// string index: the type-side Recover replays them into the per-shard registry.
		// The string walk must skip them, else applyValueRow reads an opaque effect
		// payload as a value and fails closed on its missing band.
		if row.Flags&(akifile.RecFlagCollectionOp|akifile.RecFlagCollectionSnap) != 0 {
			return nil
		}
		if row.Flags&akifile.RecFlagTombstone != 0 {
			s.Del(row.Key, now)
			return nil
		}
		var err error
		vbuf, err = s.applyValueRow(addr, row, now, vbuf)
		return err
	})
}

// ReplayFromCheckpoint rebuilds this store's index from a full index checkpoint
// (BuildIndexCheckpoint) rather than a log walk: it derefs each entry's record
// address, decodes the frame, and reapplies the value, so a restart pays a read
// per live key instead of a scan of every record ever logged. It is the fast path
// a bounded recovery takes, and the caller replays only the tail past the
// checkpoint's log position afterward.
//
// A checkpoint carries no tombstones, the producer folded every delete out, so a
// tombstone-flagged frame under a checkpoint address is a corrupt or mismatched
// dump and stops the load. now stamps the reapplied writes' lazy-expiry checks,
// the same as the walk path. On a store with no record log it is a no-op.
func (s *Store) ReplayFromCheckpoint(payload []byte, now int64) error {
	if s.akirlog == nil {
		return nil
	}
	hdr, err := akifile.ParseCkptHeader(payload)
	if err != nil {
		return err
	}
	entries, err := akifile.CkptEntries(payload, hdr)
	if err != nil {
		return err
	}
	s.replaying = true
	defer func() { s.replaying = false }()

	var vbuf []byte
	for _, e := range entries {
		row, err := s.akirlog.readAt(e.RecordAddr)
		if err != nil {
			return err
		}
		if row.Flags&akifile.RecFlagTombstone != 0 {
			return fmt.Errorf("store: checkpoint entry at %#x points at a tombstone", e.RecordAddr)
		}
		if vbuf, err = s.applyValueRow(e.RecordAddr, row, now, vbuf); err != nil {
			return err
		}
	}
	return nil
}

// RecoverIndex rebuilds this shard's whole index at open from a file recovery, the
// store half of the open sequence akifile.Recover leaves to the caller. Recover
// picks the live root, rebuilds the file-structural roots, and hands back the SRT
// and the tail offset; this turns that into the shard's live keyspace. When the
// root names an index checkpoint for this shard it takes the bounded path, loading
// the dump with ReplayFromCheckpoint and replaying only the tail past it with
// ReplayTail, so a restart never rescans the records the checkpoint already folded.
// When no root, or no checkpoint for this shard, covers the keyspace it falls back
// to the full log walk, the honest recovery of a shard that never checkpointed or a
// file with no committed root at all.
//
// rec.TailFrom is the file-global minimum first-tail offset across every shard that
// checkpointed, so a shard whose own checkpoint sat later than the minimum replays a
// few pre-checkpoint records twice: harmless, because append order makes the last
// write win and the dump already holds that same last write. now stamps the
// reapplied writes' lazy-expiry checks. On a store with no record log it is a no-op.
func (s *Store) RecoverIndex(rec *akifile.Recovery, now int64) error {
	if s.akirlog == nil {
		return nil
	}
	// No trusted root, or no row for this shard: the whole keyspace comes from the
	// log, so walk it from the start of the append space.
	if rec == nil || rec.SRT == nil || int(s.akirlog.shard) >= len(rec.SRT.Rows) {
		return s.ReplayRecords(now)
	}
	row := rec.SRT.Rows[s.akirlog.shard]
	// A zero IndexCkptOff is a shard that never cut a checkpoint: its records are all
	// in the tail the global TailFrom may start past, so a bounded replay would miss
	// them. Walk the full log instead.
	if row.IndexCkptOff == 0 {
		return s.ReplayRecords(now)
	}
	payload, err := s.akirlog.readCheckpoint(row.IndexCkptOff)
	if err != nil {
		return err
	}
	if err := s.ReplayFromCheckpoint(payload, now); err != nil {
		return err
	}
	return s.ReplayTail(rec.TailFrom, now)
}

// applyValueRow reinserts one value record during recovery, shared by the log walk
// and the checkpoint load. An inline row's value bytes ride the frame, so it
// reinserts them directly; a separated row's bytes live in the durable value log,
// so it derefs the word and reads them back through the value seam, reusing vbuf.
// It routes through SetString so the rebuilt record matches a live commit's band
// selection, dead-byte accounting, and expiry layout. It returns vbuf, grown when
// a separated read needed more room.
//
// A chunked row, or a separated row whose word never reached the value log, is
// deferred and fails closed: a multi-chunk value needs the chunk-directory walk a
// later slice adds, and an arena-resident run did not survive the restart to be
// read back. addr names the frame in the error so a bad record is locatable.
func (s *Store) applyValueRow(addr uint64, row akifile.RecordRow, now int64, vbuf []byte) ([]byte, error) {
	switch {
	case row.Flags&akifile.RecFlagChunked != 0:
		v, err := s.reassembleChunked(row.Value, int(row.ValueLen), vbuf)
		if err != nil {
			return vbuf, fmt.Errorf("store: replay of chunked record at %#x: %w", addr, err)
		}
		vbuf = v
		return vbuf, s.SetString(row.Key, v, now, int64(row.ExpireAt), false)
	case row.Flags&akifile.RecFlagInline != 0:
		return vbuf, s.SetString(row.Key, row.Value, now, int64(row.ExpireAt), false)
	default:
		if row.ValueWord&inLogBit == 0 {
			return vbuf, fmt.Errorf("store: replay of arena-resident record at %#x has no durable value", addr)
		}
		v, err := s.logReadInto(row.ValueWord&runAddrMask, int(row.ValueLen), vbuf)
		if err != nil {
			return vbuf, err
		}
		return v, s.SetString(row.Key, v, now, int64(row.ExpireAt), false)
	}
}
