package store

import "unsafe"

// Memory accounting (spec 2064/f3/16 section 8.1): each shard keeps plain
// single-owner byte counters, charged at the handful of choke points where
// bytes actually change hands, and never inferred from a proxy metric. The
// arena and value-log figures below are those counters (the segment live
// charge, the bump cursors, the log tail and dead counters); the index figure
// is a walk over the directory and segment slab, O(segments) like the arena
// fill walk, run only on the introspection path. Everything here is read on
// the shard's owner goroutine (the INFO sub-command executes as shard work),
// so every load is a plain load.

// MemLedger is one shard's byte accounting snapshot.
type MemLedger struct {
	// Keys is the live key count.
	Keys uint64

	// IndexBytes is the index footprint: directory slots, segment tables,
	// and overflow slabs. Slice headers and the free-ordinal list are noise
	// and deliberately uncounted.
	IndexBytes uint64

	// ArenaLiveBytes is the bytes charged to live records, value runs, and
	// chunk directories: the allocation charge of everything still reachable
	// from the index. Dead bytes waiting for their segment to drain are not
	// in it.
	ArenaLiveBytes uint64

	// ArenaAllocBytes is the bump-cursor fill: every byte handed out of a
	// touched segment, live or dead, the resident pressure figure spillNow
	// compares against the cap.
	ArenaAllocBytes uint64

	// ArenaTotalBytes is the arena's configured backing size.
	ArenaTotalBytes uint64

	// VlogTotalBytes and VlogLiveBytes are the value log's appended bytes and
	// the still-referenced subset. Log bytes are disk, never memory: they are
	// reported so the harness can see the spill, and they stay out of
	// UsedMemory on purpose.
	VlogTotalBytes uint64
	VlogLiveBytes  uint64

	// ChunkedBytes is the live chunked-band value bytes, summed over records
	// against their value length, wherever the chunks sit (arena or log).
	ChunkedBytes uint64
}

// UsedMemory is the shard's accounted live memory: index tables plus the
// arena bytes charged to live records, runs, and chunk directories. It is an
// account, not a measurement: no RSS, no Go runtime, no allocator slack, no
// dead arena bytes awaiting segment reclaim, and no value-log bytes, which
// are disk. The honest RSS number is used_memory_rss where the platform can
// read it, and the two are expected to differ.
func (m MemLedger) UsedMemory() uint64 {
	return m.IndexBytes + m.ArenaLiveBytes
}

// Mem snapshots the shard's ledger. Owner-goroutine only, like every other
// store read.
func (s *Store) Mem() MemLedger {
	lt, ld := s.LogBytes()
	return MemLedger{
		Keys:            uint64(s.count),
		IndexBytes:      s.idx.bytes(),
		ArenaLiveBytes:  s.arena.live(),
		ArenaAllocBytes: s.arena.used(),
		ArenaTotalBytes: uint64(len(s.arena.buf)),
		VlogTotalBytes:  lt,
		VlogLiveBytes:   lt - ld,
		ChunkedBytes:    s.chunkBytes,
	}
}

// bytes reports the index's table footprint: 4 bytes per directory slot plus
// each segment's fixed table and its overflow slab.
func (ix *index) bytes() uint64 {
	n := uint64(len(ix.dir)) * 4
	segSize := uint64(unsafe.Sizeof(indexSegment{}))
	bucketSize := uint64(unsafe.Sizeof(bucket{}))
	for _, seg := range ix.segs {
		n += segSize + uint64(len(seg.overflow))*bucketSize
	}
	return n
}

// live sums the segments' live charges: allocation charges minus unlink
// credits, zero when every record ever placed has left the index.
// Introspection only, same walk cost as used.
func (a *arena) live() uint64 {
	var n int64
	for i := range a.segs {
		n += a.segs[i].live
	}
	if n < 0 {
		return 0
	}
	return uint64(n)
}
