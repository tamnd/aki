package store

// The value-log read seam. Every band that resolves a run word with inLogBit
// set (str.go's separated read, view.go, and the bitmap and chunked bands in
// bit.go, bitkernel.go, bitop.go, chunks.go) reads its spilled bytes through
// these two methods rather than reaching for s.vlog directly. Today they
// forward to the scratch log unchanged, so this is a pure centralization with
// no behavior change. It exists so the value-log re-home flips one seam onto
// the .aki adapter (akiVlog) instead of rewriting a dozen scattered call
// sites: when s.akivlog is live the offset addresses the .aki value region, so
// the read must route there, and this is the single place that decision lands.

// logReadInto reads n bytes of a spilled value at off, reusing dst's capacity
// when it fits, for a caller that wants the bytes back as a slice. When the store
// opened over an .aki value region the offset addresses that region, so the read
// routes to the adapter; otherwise it is a scratch-log offset.
func (s *Store) logReadInto(off uint64, n int, dst []byte) ([]byte, error) {
	if s.akivlog != nil {
		return s.akivlog.readInto(off, n, dst)
	}
	return s.vlog.readInto(off, n, dst)
}

// logReadFill reads exactly len(b) bytes of a spilled value at off into b, the
// partial sub-range read the bitmap and chunked bands take at value_off+i.
func (s *Store) logReadFill(off uint64, b []byte) error {
	if s.akivlog != nil {
		return s.akivlog.readFill(off, b)
	}
	return s.vlog.readFill(off, b)
}

// logUnlink marks n value-log bytes dead: an overwrite, a delete, an expiry, or
// a demote no longer references them, so a later compaction of the log knows it
// can reclaim them. Every drop and re-home site that supersedes a spilled value
// funnels through here for the same reason the reads do, so the value-log
// re-home points the dead-byte accounting at akiVlog.unlink in one place rather
// than at each drop site. The tail side of the accounting stays in LogBytes.
func (s *Store) logUnlink(n uint64) {
	if s.akivlog != nil {
		s.akivlog.unlink(n)
		return
	}
	s.vlog.dead += n
}
