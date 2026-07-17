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
// when it fits, for a caller that wants the bytes back as a slice.
func (s *Store) logReadInto(off uint64, n int, dst []byte) ([]byte, error) {
	return s.vlog.readInto(off, n, dst)
}

// logReadFill reads exactly len(b) bytes of a spilled value at off into b, the
// partial sub-range read the bitmap and chunked bands take at value_off+i.
func (s *Store) logReadFill(off uint64, b []byte) error {
	return s.vlog.readFill(off, b)
}
