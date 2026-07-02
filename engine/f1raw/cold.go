package f1raw

import (
	"encoding/binary"
	"os"
	"sync/atomic"
)

// This file adds the minimal cold value tier the larger-than-memory string regime
// needs (implementation checklist milestone M1). It is WiscKey-style key-value
// separation: the resident lock-free index and the record headers with their keys
// stay in the in-memory arena, but a value larger than a separation threshold is
// written to an append-only on-disk log and the in-memory record holds a 12-byte
// cold pointer in place of the value bytes. Under a RAM cap smaller than the total
// value bytes, f1raw's resident footprint stays near the index-plus-keys size, while
// Redis and Valkey hold every value in the heap and swap the overflow and fault it
// back scattered on each read. That gap is the structural effect the LTM string
// benchmark measures, and it is the whole reason to separate values here.
//
// This is deliberately the minimal cold tier M1 calls for, not the full F2 engine:
// there is no migration down (M2), no read-cache promotion up (M3), no compaction,
// and no epoch reclaimer. A separated value is immutable once written. A same-key
// update publishes a fresh record that the index swaps to and leaves the old cold
// bytes as dead space; a delete just drops the index entry. Because a separated
// record never mutates, a cold read needs no seqlock: read the immutable pointer,
// do one pread, done. Migration and compaction reclaim the dead space in the later
// milestones; M1 proves the cold read path works on the easy type first.

// ptrSize is the width of an in-arena cold pointer: an 8-byte cold-log offset then a
// 4-byte value length. A separated record's value cell holds exactly these 12 bytes,
// so its reserved capacity is align8(12) and a value's real length lives in the
// pointer, not in the record's vlen field.
const ptrSize = 12

// flagSep, set in a record's flags byte, marks its value cell as a cold pointer
// rather than inline value bytes. It is written once at initRecord and never changes
// for a record's life (a same-key update publishes a new record), so a reader loads
// it with a plain read like klen and vcap, with no seqlock.
const flagSep = 1 << 0

// coldLog is an append-only value log backed by one file. WriteAt and ReadAt map to
// pwrite and pread, which carry their own offset instead of the shared file cursor,
// so appends and reads need no lock: a reserved offset is bumped atomically and each
// syscall lands at its own offset regardless of interleaving.
type coldLog struct {
	f    *os.File
	path string // the log file's path, so compaction can rename a fresh log over it
	tail atomic.Uint64
	// dead counts the bytes in the log that are no longer referenced by any live record:
	// the cold value of every separated record a same-key overwrite or a delete has
	// unlinked. The log is append-only, so a superseded value's bytes are not freed in
	// place; they sit as dead space until a compaction milestone rewrites the live bytes
	// forward and truncates. This counter is the accounting that later compaction reads to
	// decide when the dead fraction (dead/tail) is worth a compaction pass. It is dormant
	// here: nothing acts on it yet, it only has to stay honest so the trigger it feeds is
	// measuring the real waste. Bumped with a plain atomic add at each unlink site, so it
	// costs one add on the delete and overwrite paths and nothing on the read path.
	dead atomic.Uint64
}

// openColdLog creates (truncating any prior file) and opens the cold value log at
// path. M1 is a fresh-start cold tier: it does not reopen an existing log, because
// recovery and the durable single-file format are milestone M2.
func openColdLog(path string) (*coldLog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	c := &coldLog{f: f, path: path}
	// Cold reads are random point lookups, so tell the kernel to disable readahead on
	// the log up front. Otherwise each read prefetches a full readahead window that the
	// per-read DONTNEED does not cover, and those pages accumulate under the memory cap.
	c.adviseRandom()
	return c, nil
}

// append reserves a region for val with one atomic add and pwrites it there. The
// reservation guarantees two concurrent appenders never overlap, and the pwrite
// lands at the reserved offset no matter which append's syscall runs first.
func (c *coldLog) append(val []byte) (uint64, error) {
	n := uint64(len(val))
	off := c.tail.Add(n) - n
	if _, err := c.f.WriteAt(val, int64(off)); err != nil {
		return 0, err
	}
	return off, nil
}

// readInto preads n bytes at off into dst, reusing dst's capacity when it fits. A
// written region of the log is immutable and the log is grow-only, so a pread against
// a published offset is always valid and needs no coordination with appenders or
// other readers.
func (c *coldLog) readInto(off uint64, n int, dst []byte) ([]byte, error) {
	if cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]
	if _, err := c.f.ReadAt(dst, int64(off)); err != nil {
		return dst[:0], err
	}
	// Drop the page cache for exactly the range just read so cold reads never
	// accumulate cache. WiscKey separation only keeps its promise (resident footprint
	// near index-plus-keys) if the cold values do not linger in the OS page cache: a
	// buffered pread pulls each touched value page in, and under a hard memory cap
	// smaller than the value bytes that cache races the kernel reclaimer and trips the
	// cgroup OOM killer before reclaim catches up (observed: f1srv killed with anon-rss
	// near zero, the cgroup filled by cold-read page cache alone). Advising only the
	// just-read range away after each read holds the resident cache to the handful of
	// in-flight reads regardless of read volume, so the footprint stays at
	// index-plus-keys. A real bounded read cache that keeps hot values resident is
	// milestone M3; M1 just holds the invariant.
	c.adviseDontNeed(off, n)
	return dst, nil
}

func (c *coldLog) close() error {
	if c.f == nil {
		return nil
	}
	return c.f.Close()
}

// isSep reports whether the record at off carries a cold pointer instead of an inline
// value. The flags byte is immutable for a record's life, so this is a plain read.
func (s *Store) isSep(off uint64) bool {
	return s.arena[off+offFlags]&flagSep != 0
}

// sepValLen returns the cold value length a separated record at off points at, read from
// the length field of its 12-byte cold pointer. The pointer is immutable for the record's
// life, so this is a plain read. The caller must have checked isSep(off); on an inline
// record the value cell is value bytes, not a pointer, and this would misread them.
func (s *Store) sepValLen(off uint64) int {
	vbase := off + hdrSize + align8(s.klen(off))
	return int(binary.LittleEndian.Uint32(s.arena[vbase+8:]))
}

// markSepDead accounts the cold bytes of the record at off as dead space, if that record
// is separated. It is called at every site that unlinks a record from the index (an
// overwrite's entry swap, a delete, a collection element delete, a list pop), right after
// the unlink commits, so the cold bytes the unlinked record pointed at are counted exactly
// once as reclaimable. An inline record or a store with no cold log is a no-op: there is no
// cold value to reclaim. The record bytes stay valid in the grow-only arena, so reading the
// pointer here is safe even though the index no longer points at the record.
func (s *Store) markSepDead(off uint64) {
	if s.cold == nil || !s.isSep(off) {
		return
	}
	s.cold.dead.Add(uint64(s.sepValLen(off)))
}

// ColdBytes reports the cold value log's total appended bytes and the subset of those that
// are dead (unreferenced by any live record). A store with no cold log reports zero for
// both. live = total - dead is the bytes a compaction pass would keep; dead is what it
// would reclaim. This is the accounting a later compaction milestone reads to decide when
// the dead fraction is worth a rewrite; it is exposed now so the invariant is testable and
// the introspection path can surface it.
func (s *Store) ColdBytes() (total, dead uint64) {
	if s.cold == nil {
		return 0, 0
	}
	return s.cold.tail.Load(), s.cold.dead.Load()
}

// encPtr writes a cold pointer (offset then value length) into a 12-byte cell.
func encPtr(dst []byte, coldOff uint64, vlen int) {
	binary.LittleEndian.PutUint64(dst[0:], coldOff)
	binary.LittleEndian.PutUint32(dst[8:], uint32(vlen))
}

// readSeparated resolves a separated record's value from the cold log into dst. The
// 12-byte pointer at the record's value cell is immutable, so it is read with plain
// loads; the length in the pointer is the real value length (the record's vlen is the
// pointer width, not the value width). One pread serves the value.
func (s *Store) readSeparated(off uint64, dst []byte) ([]byte, bool) {
	vbase := off + hdrSize + align8(s.klen(off))
	coldOff := binary.LittleEndian.Uint64(s.arena[vbase:])
	n := int(binary.LittleEndian.Uint32(s.arena[vbase+8:]))
	v, err := s.cold.readInto(coldOff, n, dst)
	if err != nil {
		return dst[:0], false
	}
	return v, true
}

// setSeparated stores a large value out of line: it appends the value bytes to the
// cold log and publishes a record whose value cell is the resulting 12-byte pointer,
// flagged separated. A same-key overwrite always lands here as a fresh record (the
// index entry swaps to it), so a separated record is never updated in place and stays
// immutable for its life.
func (s *Store) setSeparated(key, val []byte, h uint64) error {
	coldOff, err := s.cold.append(val)
	if err != nil {
		return err
	}
	var ptr [ptrSize]byte
	encPtr(ptr[:], coldOff, len(val))
	return s.publish(key, ptr[:], h, stringKind, flagSep)
}

// putKindSeparated is setSeparated for a collection element namespace: it appends a large
// element value to the cold log and publishes a record in the given kind whose value cell is
// the resulting 12-byte pointer, flagged separated. It is the collection twin of the string
// setSeparated, so a hash of large field values keeps its index and field names resident and
// spills only the values, the property that lets a single collection exceed memory. A same-key
// overwrite always lands here as a fresh record (the index entry swaps to it), so a separated
// element record is never updated in place and stays immutable for its life.
func (s *Store) putKindSeparated(key, val []byte, h uint64, kind byte) error {
	coldOff, err := s.cold.append(val)
	if err != nil {
		return err
	}
	var ptr [ptrSize]byte
	encPtr(ptr[:], coldOff, len(val))
	return s.publish(key, ptr[:], h, kind, flagSep)
}
