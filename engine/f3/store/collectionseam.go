package store

import "github.com/tamnd/aki/engine/f3/akifile"

// The collection effect-log seam: the store side of collection durability (spec
// 2064/f3/M8-collection-durability-plan). The record seam (recordseam.go) made a
// string commit survivable by cutting its record row into the shared .aki log; a
// collection has no such row, because a set, list, hash, zset, or stream lives as
// a native-heap struct in a per-shard registry that rebuilds from nothing on
// restart, so a crash loses every collection key in full. This seam closes that
// gap for the effect path: every mutating collection command cuts one small effect
// frame naming the collection key, the sub-key it touched, and the op it applied,
// and a reopen re-drives those frames through the type's mutation funnel to rebuild
// the collection.
//
// It rides the type-agnostic record frame the collection codec designed
// (collectionlog.go): the payload is opaque to akifile, so the record writer, the
// shard walk, and the CRC path carry a collection frame unchanged, and only the
// store-side frame-build here and the type-side apply on recovery differ. Like the
// record seam it is default off, so a store with no .aki handle pays one nil check
// and keeps the pure in-memory registry unchanged, and it cuts eagerly, one stage
// plus one flush per effect, the same interim form the record seam uses until the
// group-commit batching flip lands box-side. The snapshot half (a whole-collection
// base at the checkpoint) and compaction are the sibling slices this one clears the
// path for.

// LogCollectionOp durably appends one collection mutation to the shared .aki record
// log: the collection key rides the frame's key slot and the effect payload (kind,
// op, sub-key, sub-value) rides its value slot under RecFlagCollectionOp. On a store
// with no record log or mid-replay it is a no-op, so the volatile-registry path is
// unchanged and recovery does not re-log the frames it is reading back. It stages
// and cuts eagerly, so the effect is durable before the command returns; a cut
// failure is held in rlogErr for the ack-barrier path the way a tombstone's is,
// since the collection commands answer with a count, not an error, and cannot
// return a durability fault through their reply.
func (s *Store) LogCollectionOp(key []byte, kind akifile.CollKind, op uint8, subKey, subValue []byte) {
	if s.akirlog == nil || s.replaying {
		return
	}
	s.collScratch = akifile.AppendCollOp(s.collScratch[:0], akifile.CollOpRow{
		Kind:     kind,
		Op:       op,
		SubKey:   subKey,
		SubValue: subValue,
	})
	s.akirlog.stage(akifile.RecordRow{
		Flags:    akifile.RecFlagCollectionOp,
		ValueLen: uint32(len(s.collScratch)),
		Value:    s.collScratch,
		Key:      key,
	})
	if _, err := s.akirlog.flush(); err != nil && s.rlogErr == nil {
		s.rlogErr = err
	}
}

// LogCollectionSnap durably appends a whole-collection snapshot to the shared .aki
// record log: the collection key rides the frame's key slot and the snapshot payload
// (kind, header, element run) rides its value slot under RecFlagCollectionSnap. It is
// the base a reopen rebuilds a collection from before it replays the effect tail cut
// after it, the collection sibling of the string index checkpoint. Like
// LogCollectionOp it no-ops on a store with no record log or mid-replay, stages and
// cuts eagerly, and holds a cut failure in rlogErr for the ack-barrier path.
func (s *Store) LogCollectionSnap(key []byte, kind akifile.CollKind, header, elementRun []byte) {
	if s.akirlog == nil || s.replaying {
		return
	}
	s.collScratch = akifile.AppendCollSnap(s.collScratch[:0], akifile.CollSnapRow{
		Kind:       kind,
		Header:     header,
		ElementRun: elementRun,
	})
	s.akirlog.stage(akifile.RecordRow{
		Flags:    akifile.RecFlagCollectionSnap,
		ValueLen: uint32(len(s.collScratch)),
		Value:    s.collScratch,
		Key:      key,
	})
	if _, err := s.akirlog.flush(); err != nil && s.rlogErr == nil {
		s.rlogErr = err
	}
}

// WalkCollection replays this shard's collection frames for one kind in append
// order, routing each to onSnap or onOp by whether it is a snapshot or an effect. A
// snapshot frame resets its key and every later effect for that key applies on top,
// so a recovery that resets on onSnap and mutates on onOp rebuilds the collection
// from the last snapshot plus its effect tail, the bounded path the string index
// recovery already takes. It skips string records, tombstones, and the other types'
// collection frames. On a store with no record log it is a no-op. The key and the
// payload slices alias the segment for the callback's duration, so a callback that
// keeps them copies. A decode error on a CRC-clean but malformed payload stops the
// walk, the fail-closed cut a recovering reader wants.
func (s *Store) WalkCollection(
	kind akifile.CollKind,
	onSnap func(key []byte, row akifile.CollSnapRow) error,
	onOp func(key []byte, row akifile.CollOpRow) error,
) error {
	if s.akirlog == nil {
		return nil
	}
	return s.akirlog.walkShard(func(_ uint64, rec akifile.RecordRow) error {
		switch {
		case rec.Flags&akifile.RecFlagCollectionSnap != 0:
			snap, err := akifile.ParseCollSnap(rec.Value)
			if err != nil {
				return err
			}
			if snap.Kind != kind {
				return nil
			}
			return onSnap(rec.Key, snap)
		case rec.Flags&akifile.RecFlagCollectionOp != 0:
			op, err := akifile.ParseCollOp(rec.Value)
			if err != nil {
				return err
			}
			if op.Kind != kind {
				return nil
			}
			return onOp(rec.Key, op)
		}
		return nil
	})
}
