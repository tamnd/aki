package store

import (
	"math/bits"
	"sort"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// The index-checkpoint producer: the store side of doc 07 section 5. A checkpoint
// bounds recovery, which without one replays every record ever logged and with one
// loads a dump of the live index and replays only the tail past the checkpoint's
// log position. This builds that dump for one shard.
//
// The dump is key_hash to record_addr, and the store does not keep a per-key
// record address in memory: logRecord discards the address its flush returns,
// because holding one per key would spend the RAM the whole memory bar is fought
// to save. So the producer recovers the addresses the same way recovery does, by
// walking the shard's record log: a later record for a key supersedes an earlier
// one and a tombstone drops it, so the walk lands on exactly the live set with each
// key's newest durable frame address. The result is a full checkpoint, consistent
// up to the durable tail.
//
// This is the payload producer only. Appending the payload as an index_ckpt
// segment, assembling the SRT row, and flipping the meta slot are the commit
// slice, and consuming a checkpoint to skip the settled prefix is the recovery
// slice. The payload framed here round-trips through the akifile checkpoint codec
// and every address in it dereferences to the key it is filed under.

// BuildIndexCheckpoint frames a full index checkpoint for this shard onto dst and
// returns the grown buffer with the header it stamped. It walks the shard's record
// log, folds it to the live key_hash to record_addr set, and frames a CkptFull
// header followed by one entry per live key. The entries are ordered by key hash so
// the dump is deterministic across runs. On a store with no record log it frames an
// empty full checkpoint, the honest dump of an index with nothing durable behind
// it.
func (s *Store) BuildIndexCheckpoint(dst []byte) ([]byte, akifile.CkptHeader, error) {
	live := make(map[uint64]uint64)
	if s.akirlog != nil {
		err := s.akirlog.walkShard(func(addr uint64, row akifile.RecordRow) error {
			// Collection frames share this log but belong to WalkCollection, not the
			// string index dump: fold them out so the checkpoint never files an effect
			// frame's address under a string key hash.
			if row.Flags&(akifile.RecFlagCollectionOp|akifile.RecFlagCollectionSnap) != 0 {
				return nil
			}
			h := Hash(row.Key)
			if row.Flags&akifile.RecFlagTombstone != 0 {
				delete(live, h)
				return nil
			}
			live[h] = addr
			return nil
		})
		if err != nil {
			return dst, akifile.CkptHeader{}, err
		}
	}

	// A power-of-two bucket count at least as large as the live set lets a loader
	// pre-size its index without a rehash storm, and gives each entry a low-bits
	// slot hint the loader can trust or recompute.
	bucket := pow2ceil(uint64(len(live)))
	mask := bucket - 1

	hdr := akifile.CkptHeader{
		FullOrDelta: akifile.CkptFull,
		EntryCount:  uint64(len(live)),
		BucketCount: bucket,
	}
	if s.akirlog != nil {
		hdr.CkptLogPos = s.akirlog.globalSeq()
		hdr.SeqHigh = s.akirlog.seqHigh()
	}
	dst = akifile.AppendCkptHeader(dst, hdr)

	hashes := make([]uint64, 0, len(live))
	for h := range live {
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool { return hashes[i] < hashes[j] })
	for _, h := range hashes {
		dst = akifile.AppendCkptEntry(dst, akifile.CkptEntry{
			KeyHash:    h,
			RecordAddr: live[h],
			Slot:       uint16(h & mask),
		})
	}
	return dst, hdr, nil
}

// WriteIndexCheckpoint builds this shard's index checkpoint, appends it to the file
// as an index_ckpt segment, and returns the SRT row a file-global commit stamps for
// this shard. It is the persist half of the checkpoint commit: it lays the dump down
// in free space and hands back the row naming it, but it does not flip the meta slot.
// The flip is the coordinator's, which gathers every shard's row into one SRT and
// commits once, because the one meta slot names all shards' roots together and a
// per-shard flip would strand the others.
//
// The row names the dump the recovery fast path reads (IndexCkptOff and IndexCkptLen)
// and the tail the recovery replays after it (FirstTailSeg, the append offset the
// instant the checkpoint was taken, so every record cut later falls in the tail). It
// also carries the log position and shard sequence the dump is consistent up to and
// the live record count. On a store with no record log there is nothing durable to
// checkpoint, so it returns a zero row.
func (s *Store) WriteIndexCheckpoint() (akifile.SRTRow, error) {
	if s.akirlog == nil {
		return akifile.SRTRow{}, nil
	}
	// Capture the tail start before the append: the checkpoint covers every record
	// logged so far, and the append that follows only lays down the dump segment,
	// which a tail replay skips as a non-log kind, so records cut after this offset
	// are exactly the tail past the dump.
	tailFrom := s.akirlog.cursor()
	payload, hdr, err := s.BuildIndexCheckpoint(nil)
	if err != nil {
		return akifile.SRTRow{}, err
	}
	off, err := s.akirlog.writeCheckpoint(payload)
	if err != nil {
		return akifile.SRTRow{}, err
	}
	return akifile.SRTRow{
		IndexCkptOff: off,
		IndexCkptLen: uint64(len(payload)),
		CkptLogPos:   hdr.CkptLogPos,
		ShardSeqHigh: hdr.SeqHigh,
		FirstTailSeg: tailFrom,
		LiveRecords:  hdr.EntryCount,
	}, nil
}

// CommitCheckpoint installs a set of per-shard index checkpoints as the file's live
// root in one meta flip. rows is indexed by shard: rows[i] is the SRT row
// WriteIndexCheckpoint returned for shard i, and a shard with nothing to checkpoint
// takes a zero row. It is the coordinator step of the checkpoint commit: the one
// meta slot names every shard's roots together, so the rows gather into one SRT and
// commit once rather than a shard flipping the slot on its own and stranding the
// others.
//
// stats is the file-global accounting the new root records, the live and dead bytes
// and record count a reopen reads back to seed compaction without a rescan; the
// caller aggregates it across shards and stamps the checkpoint time, which this
// layer has no clock for. After it returns the committed root names each shard's
// index_ckpt and its tail, so a plain reopen recovers through Recover without the
// rows being handed in.
func CommitCheckpoint(f *akifile.File, rows []akifile.SRTRow, stats akifile.CheckpointStats) error {
	return f.CheckpointWithGlobals(&akifile.SRT{Rows: rows}, nil, stats, akifile.CheckpointGlobals{})
}

// pow2ceil is the smallest power of two at least n, the bucket-count rounding a
// checkpoint header carries. It is defined here rather than inlined so the loader
// side can share the same rounding when it lands.
func pow2ceil(n uint64) uint64 {
	if n <= 1 {
		return 1
	}
	return uint64(1) << bits.Len64(n-1)
}
