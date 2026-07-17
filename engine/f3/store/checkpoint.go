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

// pow2ceil is the smallest power of two at least n, the bucket-count rounding a
// checkpoint header carries. It is defined here rather than inlined so the loader
// side can share the same rounding when it lands.
func pow2ceil(n uint64) uint64 {
	if n <= 1 {
		return 1
	}
	return uint64(1) << bits.Len64(n-1)
}
