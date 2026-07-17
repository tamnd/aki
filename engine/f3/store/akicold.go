package store

import "github.com/tamnd/aki/engine/f3/akifile"

// akiCold re-homes the per-shard scratch cold log (the cold *vlog in cold.go)
// onto the shared .aki cold-chunk region, the cold-tier counterpart of akiVlog.
// The scratch cold log owns its own file, truncates on open, and hands out plain
// offsets into that file; this adapter instead appends whole cold frames through
// the akifile cold region and cuts one cold_chunk segment per demote batch into
// the single .aki, so the store keeps one durable file instead of a scratch cold
// file per shard. It borrows the same File the value-log adapter does: the two
// tiers are distinct segment kinds in one file, not two files.
//
// Unlike the value-log spill, a cold demote is already a batch: the migrator
// moves a whole quantum of records out of the arena at once, so a single
// AppendColdFrames call cuts one segment and returns every frame's absolute
// offset in the same call. There is no deferred-publish split here and no
// provisional word, the offset a tier entry keeps is known the moment the batch
// lands. The frame liveness stays Bitcask-style, the tier index names a frame or
// it is dead, and dead marks the bytes a superseding demote or a delete released.
//
// The scratch log reserved offset 0 so no live frame sat at the store's null
// sentinel; here every offset is an absolute file offset past the header pages,
// so it is never 0 and the reservation falls away.
//
// This adapter is store-side but not yet wired into the migrator: it proves the
// cold re-home's accounting and read paths in isolation before any flip of the
// demote path onto it.
type akiCold struct {
	f     *akifile.File
	shard uint16

	// seq stamps each cut cold_chunk segment, advanced once per non-empty batch.
	seq uint64

	// total is every appended cold-frame byte; dead is the subset a superseding
	// demote, a delete, or an expiry unlinked. live = total - dead is what a cold
	// region compaction keeps, the same accounting the scratch log exposed.
	total uint64
	dead  uint64
}

// newAkiCold builds a cold adapter for shard backed by f's cold-chunk region.
func newAkiCold(f *akifile.File, shard uint16) *akiCold {
	return &akiCold{f: f, shard: shard}
}

// appendBatch writes a demote quantum of whole cold frames into one cold_chunk
// segment and returns each frame's absolute file offset in order, the offset a
// tier entry keeps. An empty batch is a no-op that leaves the sequence untouched,
// so shard_seq advances only on a real cut. The frame bytes join total the moment
// they land.
func (c *akiCold) appendBatch(frames [][]byte) ([]uint64, error) {
	if len(frames) == 0 {
		return nil, nil
	}
	c.seq++
	offs, err := c.f.AppendColdFrames(c.shard, c.seq, frames)
	if err != nil {
		return nil, err
	}
	for _, fr := range frames {
		c.total += uint64(len(fr))
	}
	return offs, nil
}

// readFrame reads the whole cold frame at off, the two-pread cold read the tier
// resolver takes for a frame it names directly. dst is reused when it has the
// room.
func (c *akiCold) readFrame(off uint64, dst []byte) ([]byte, error) {
	return c.f.ReadColdFrame(off, dst)
}

// readInto reads n bytes at off, the positioned sub-read the cold header, key,
// and value reads take (cold.go): a frame's fixed header, then the key or the
// value at a known in-frame offset, without pulling the whole frame. It sizes dst
// when it is short and reuses its capacity when it fits, the parity of the scratch
// log's readInto. No checksum, like the scratch log: the segment payload CRC
// guards the frame, and these reads step inside a frame the resolver already
// located.
func (c *akiCold) readInto(off uint64, n int, dst []byte) ([]byte, error) {
	if cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]
	if err := c.f.ReadValueRangeAt(off, dst); err != nil {
		return dst[:0], err
	}
	return dst, nil
}

// unlink records n bytes a superseding demote, a delete, or an expiry reap no
// longer references, so a later cold-region compaction knows what it can
// reclaim, the way the scratch log's dead counter did.
func (c *akiCold) unlink(n uint64) { c.dead += n }

// logBytes reports the total appended cold bytes and the dead subset, the pair a
// cold-region compaction weighs to decide when a rewrite is worth it.
func (c *akiCold) logBytes() (total, dead uint64) { return c.total, c.dead }
