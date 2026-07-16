package akifile

// Extent is one row of the coarse extent map (spec 2064/f3/07 section 3). The
// map records only extents, never individual segments, so a tool or a fresh open
// finds the shape of the file without a scan; segments themselves are reached by
// SRT roots and per-shard chains.
type Extent struct {
	Kind     uint32
	Flags    uint32
	StartOff uint64
	Length   uint64
}

// MarshalExtents encodes the extent rows as a flat 24-byte-per-entry table, the
// payload of an extent_table segment.
func MarshalExtents(es []Extent) []byte {
	b := make([]byte, len(es)*ExtentSize)
	off := 0
	for i := range es {
		le.PutUint32(b[off:], es[i].Kind)
		le.PutUint32(b[off+4:], es[i].Flags)
		le.PutUint64(b[off+8:], es[i].StartOff)
		le.PutUint64(b[off+16:], es[i].Length)
		off += ExtentSize
	}
	return b
}

// ParseExtents decodes an extent table. A length that is not a whole number of
// entries is a torn or corrupt table, returned as ErrLength.
func ParseExtents(b []byte) ([]Extent, error) {
	if len(b)%ExtentSize != 0 {
		return nil, ErrLength
	}
	n := len(b) / ExtentSize
	es := make([]Extent, n)
	off := 0
	for i := 0; i < n; i++ {
		es[i] = Extent{
			Kind:     le.Uint32(b[off:]),
			Flags:    le.Uint32(b[off+4:]),
			StartOff: le.Uint64(b[off+8:]),
			Length:   le.Uint64(b[off+16:]),
		}
		off += ExtentSize
	}
	return es, nil
}
