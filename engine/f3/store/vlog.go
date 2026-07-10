package store

import (
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
// The log is append-only and a written region is immutable: a same-key
// overwrite publishes a fresh record and leaves the old bytes as dead space
// the dead counter measures and CompactLog reclaims. M0 is a fresh-start log
// (open truncates); the durable format and recovery are later milestones.
type vlog struct {
	f    *os.File
	path string // the file's path, so compaction can rename a fresh log over it
	tail uint64 // appended bytes, the next append offset
	// dead counts bytes no longer referenced by any live record: the logged
	// value of every record an overwrite, a delete, or an expiry reap
	// unlinked. Append-only means those bytes sit as waste until CompactLog
	// rewrites the live bytes forward; this counter is the accounting that
	// decides when the rewrite is worth it, so it must stay honest at every
	// unlink site.
	dead uint64
}

// openVlog creates (truncating any prior file) and opens the value log.
func openVlog(path string) (*vlog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	l := &vlog{f: f, path: path}
	// Log reads are random point lookups: disable readahead up front, or each
	// read prefetches a full readahead window the per-read DONTNEED does not
	// cover, and those pages accumulate under a memory cap.
	l.adviseRandom()
	return l, nil
}

// append reserves the tail for val and pwrites it there. Single owner: the
// reservation is a plain add.
func (l *vlog) append(val []byte) (uint64, error) {
	off := l.tail
	if _, err := l.f.WriteAt(val, int64(off)); err != nil {
		return 0, err
	}
	l.tail += uint64(len(val))
	return off, nil
}

// readInto preads n bytes at off into dst, reusing dst's capacity when it
// fits. A written region is immutable and the log grow-only, so a pread
// against a published offset is always valid. The range just read is advised
// away so log reads never accumulate page cache: the resident-footprint
// invariant of the LTM regime holds only if the spilled values do not linger
// in the OS cache after each read.
func (l *vlog) readInto(off uint64, n int, dst []byte) ([]byte, error) {
	if cap(dst) < n {
		dst = make([]byte, n)
	}
	dst = dst[:n]
	if _, err := l.f.ReadAt(dst, int64(off)); err != nil {
		return dst[:0], err
	}
	l.adviseDontNeed(off, n)
	return dst, nil
}

// readFill preads exactly len(b) bytes at off into b, under the same
// immutability and advise-away rules as readInto.
func (l *vlog) readFill(off uint64, b []byte) error {
	if _, err := l.f.ReadAt(b, int64(off)); err != nil {
		return err
	}
	l.adviseDontNeed(off, len(b))
	return nil
}

func (l *vlog) close() error {
	if l.f == nil {
		return nil
	}
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
