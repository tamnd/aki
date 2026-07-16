package akifile

// SegmentTally is the segment population a grid walk found: a count per kind, the
// total, and the durable tail the walk stopped at (spec 2064/f3/07 section 6). It
// carries no payload, only the shape of what is on disk.
type SegmentTally struct {
	ByKind      map[uint16]int
	Total       int
	DurableTail uint64
}

// ScanSegments walks the append-space segment grid from `from` and tallies every
// intact segment by kind, stopping at the durable tail (the first torn or
// never-synced segment, or the end of the walked range). It is the read-only
// backbone of the file-info and verify tools: it reports the file's segment
// population without interpreting any payload, so a torn tail costs only the
// segments past it, never a decode error.
//
// It shares the recovery tail-replay walk, so what a tool reports as the durable
// tail is exactly where recovery would resume.
func ScanSegments(dev Device, prefix *Prefix, from, size uint64) (SegmentTally, error) {
	tally := SegmentTally{ByKind: make(map[uint16]int)}
	end, err := ReplayTail(dev, prefix, from, size, func(_ uint64, h *SegHeader, _ []byte) error {
		tally.ByKind[h.Kind]++
		tally.Total++
		return nil
	})
	if err != nil {
		return SegmentTally{}, err
	}
	tally.DurableTail = end
	return tally, nil
}
