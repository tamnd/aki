package store

import (
	"errors"
	"os"
)

// The per-shard value log (spec 2064/f3/09 section 8): WiscKey-style
// key-value separation for the larger-than-memory string regime. The index,
// record headers, and keys stay resident in the arena; a value the resident
// budget cannot hold has its bytes appended to this log and the record keeps
// a 16-byte pointer in its place. One log per shard, owned by the shard's
// worker like everything else in the store, so the tail and dead counters are
// plain fields: no reservation atomics, no cross-thread appenders, because a
// second toucher does not exist.
//
// Appends are buffered (the doc 06 batched sequential pwrite, lab 17): a
// value copies into the pending buffer at the logical tail and the buffer
// hits the disk in one pwrite per vlogFlushBytes, not one per value. The
// pending bytes are readable at their logical offsets the moment append
// returns, served from the buffer until the flush lands, so a published
// pointer never dangles on the flush cadence. The buffer is bounded by the
// threshold plus one value, so the spill path holds no unbounded queue.
//
// The log is append-only and a written region is immutable: a same-key
// overwrite publishes a fresh record and leaves the old bytes as dead space
// the dead counter measures and CompactLog reclaims. M0 is a fresh-start log
// (open truncates); the durable format and recovery are later milestones.
type vlog struct {
	f    *os.File
	path string // the file's path, so compaction can rename a fresh log over it

	// tail is the logical tail, the next append offset: wtail flushed bytes
	// on disk plus the pending buffer. Offsets in [wtail, tail) read from
	// pending; below wtail they read from the file. A flush drains the whole
	// buffer, so no value ever straddles the boundary.
	tail    uint64
	wtail   uint64
	pending []byte

	// flushAt is the pending-buffer flush threshold, vlogFlushBytes unless a
	// lab tuned it (1 flushes every append, the pre-batching posture).
	flushAt int

	// werr is the first flush failure, sticky. Past it appends refuse (the
	// spill path degrades to the arena or to backpressure) and the pending
	// bytes stay in the buffer, still readable, so every pointer handed out
	// before the failure keeps resolving from memory.
	werr error

	// dead counts bytes no longer referenced by any live record: the logged
	// value of every record an overwrite, a delete, or an expiry reap
	// unlinked. Append-only means those bytes sit as waste until CompactLog
	// rewrites the live bytes forward; this counter is the accounting that
	// decides when the rewrite is worth it, so it must stay honest at every
	// unlink site.
	dead uint64
}

// vlogFlushBytes is the pending buffer's flush threshold: appends coalesce
// and hit the disk in pwrites of about this size. Swept by labs/f3/m0/17.
const vlogFlushBytes = 1 << 20

// errLogBroken is returned by append once a flush has failed: the log takes
// no new bytes, and the caller's placement policy decides what degrades.
var errLogBroken = errors.New("store: value log write failed")

// openVlog creates (truncating any prior file) and opens the value log.
func openVlog(path string) (*vlog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	l := &vlog{f: f, path: path, flushAt: vlogFlushBytes}
	// Log reads are random point lookups: disable readahead up front, or each
	// read prefetches a full readahead window the per-read DONTNEED does not
	// cover, and those pages accumulate under a memory cap.
	l.adviseRandom()
	return l, nil
}

// append reserves the logical tail for val and stages the bytes in the
// pending buffer; the buffer flushes at the threshold. Single owner: the
// reservation is a plain add. The returned offset is readable immediately.
// Consecutive appends are contiguous, which is what lets writeRun place a
// two-part run with two calls.
func (l *vlog) append(val []byte) (uint64, error) {
	if l.werr != nil {
		return 0, errLogBroken
	}
	off := l.tail
	l.pending = append(l.pending, val...)
	l.tail += uint64(len(val))
	if len(l.pending) >= l.flushAt {
		if err := l.flush(); err != nil {
			// The bytes just staged stay readable from the buffer; only
			// later appends refuse. This append's caller already has a
			// resolvable offset, so it succeeds.
			return off, nil
		}
	}
	return off, nil
}

// flush pwrites the whole pending buffer at the flushed tail. On failure the
// buffer is kept, werr goes sticky, and every offset already handed out keeps
// reading from memory: a failing disk degrades to a resident-only store, not
// to dangling pointers.
func (l *vlog) flush() error {
	if l.werr != nil {
		return l.werr
	}
	if len(l.pending) == 0 {
		return nil
	}
	if _, err := l.f.WriteAt(l.pending, int64(l.wtail)); err != nil {
		l.werr = err
		return err
	}
	l.wtail += uint64(len(l.pending))
	l.pending = l.pending[:0]
	return nil
}

// readInto reads n bytes at off into dst, reusing dst's capacity when it
// fits. A written region is immutable and the log grow-only, so a read
// against a published offset is always valid: from the pending buffer while
// the bytes await their flush, from the file after. The file range just read
// is advised away so log reads never accumulate page cache: the
// resident-footprint invariant of the LTM regime holds only if the spilled
// values do not linger in the OS cache after each read.
func (l *vlog) readInto(off uint64, n int, dst []byte) ([]byte, error) {
	if cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]
	if off >= l.wtail {
		if err := l.readPending(off, dst); err != nil {
			return dst[:0], err
		}
		return dst, nil
	}
	if _, err := l.f.ReadAt(dst, int64(off)); err != nil {
		return dst[:0], err
	}
	l.adviseDontNeed(off, n)
	return dst, nil
}

// readFill reads exactly len(b) bytes at off into b, under the same rules as
// readInto.
func (l *vlog) readFill(off uint64, b []byte) error {
	if off >= l.wtail {
		return l.readPending(off, b)
	}
	if _, err := l.f.ReadAt(b, int64(off)); err != nil {
		return err
	}
	l.adviseDontNeed(off, len(b))
	return nil
}

// readPending copies from the pending buffer. A range past the staged bytes
// was never appended, which is a corrupt-pointer read and errors like a short
// file would.
func (l *vlog) readPending(off uint64, b []byte) error {
	i := off - l.wtail
	if i+uint64(len(b)) > uint64(len(l.pending)) {
		return errLogBroken
	}
	copy(b, l.pending[i:])
	return nil
}

func (l *vlog) close() error {
	if l.f == nil {
		return nil
	}
	// Best-effort: pending bytes are non-durable scratch in M0 either way,
	// but there is no reason to drop them when the disk still takes writes.
	_ = l.flush()
	return l.f.Close()
}

// LogBytes reports the value log's total appended bytes and the dead subset.
// live = total - dead is what a compaction keeps; dead is what it reclaims.
func (s *Store) LogBytes() (total, dead uint64) {
	if s.vlog == nil {
		return 0, 0
	}
	return s.vlog.tail, s.vlog.dead
}
