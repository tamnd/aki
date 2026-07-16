package akifile

// The free-map codec (spec 2064/f3/07 sections 2 and 3). The free map is a segment
// like everything else (KindFreeMap): a 16-byte header, then a run of 24-byte
// entries, each a reclaimable run of the file. The writer owns it; compaction,
// expiry, and eviction submit reclaim requests and the writer applies them at commit
// points, allocating first-fit from the free set before it grows the physical file
// (section 2, the alloc protocol).
//
// An entry is either free (allocatable now) or pending-free: a run whose reclaim was
// submitted but is still referenced by the live root until the next meta flip, so it
// must not be handed out until the flip makes the supersession durable (section 3,
// the extent kinds free and pending-free). The FreeMapPending flag carries that
// state so a torn commit that leaves a pending run is never allocated over.
//
// The free-map root is written whole at each commit, not as a delta chain, because
// the free set is bounded and small next to the data; the extent table takes the
// same flat-snapshot shape. The payload carries no checksum of its own; the segment
// header's payload CRC covers it, so the FRM3 magic is a kind cross-check. Codec
// only: it frames into and reads out of a caller-owned payload and never touches a
// File. The writer that maintains the live set and the alloc path that reads it are
// separate slices.

// FreeMapMagic is the free_map payload sentinel.
const FreeMapMagic = "FRM3"

const (
	// FreeMapHeaderLen is the fixed free-map header size.
	FreeMapHeaderLen = 16
	// FreeExtentSize is one reclaimable run: start_off u64, length u64, flags u32,
	// and 4 reserved bytes.
	FreeExtentSize = 24
)

// FreeMapPending marks a run as pending-free: its reclaim is submitted but still
// referenced by the live root, so it is not allocatable until the next meta flip.
const FreeMapPending uint32 = 1 << 0

// FreeMapHeader counts the runs that follow. The totals are derived, not stored, so
// there is no header-versus-body invariant to tear.
type FreeMapHeader struct {
	EntryCount uint64 // reclaimable runs that follow the header
}

// FreeExtent is one reclaimable run of the file. A pending run carries
// FreeMapPending and is skipped by first-fit until the flip that frees it.
type FreeExtent struct {
	StartOff uint64 // where the run begins
	Length   uint64 // its size in bytes
	Flags    uint32 // FreeMapPending or zero
}

// AppendFreeMapHeader frames a free-map header onto dst. Entries follow with
// AppendFreeExtent, so a large free set streams out in bounded slices.
func AppendFreeMapHeader(dst []byte, h FreeMapHeader) []byte {
	var b [FreeMapHeaderLen]byte
	copy(b[0:4], FreeMapMagic)
	// b[4:8] reserved, left zero.
	le.PutUint64(b[8:16], h.EntryCount)
	return append(dst, b[:]...)
}

// AppendFreeExtent frames one 24-byte reclaimable run onto dst.
func AppendFreeExtent(dst []byte, e FreeExtent) []byte {
	var b [FreeExtentSize]byte
	le.PutUint64(b[0:8], e.StartOff)
	le.PutUint64(b[8:16], e.Length)
	le.PutUint32(b[16:20], e.Flags)
	// b[20:24] reserved, left zero.
	return append(dst, b[:]...)
}

// ParseFreeMapHeader decodes and validates a free-map header: only the magic, since
// the header carries no invariant beyond its count.
func ParseFreeMapHeader(b []byte) (FreeMapHeader, error) {
	if len(b) < FreeMapHeaderLen {
		return FreeMapHeader{}, ErrShort
	}
	if string(b[0:4]) != FreeMapMagic {
		return FreeMapHeader{}, ErrMagic
	}
	return FreeMapHeader{EntryCount: le.Uint64(b[8:16])}, nil
}

// ParseFreeExtent decodes one 24-byte reclaimable run.
func ParseFreeExtent(b []byte) (FreeExtent, error) {
	if len(b) < FreeExtentSize {
		return FreeExtent{}, ErrShort
	}
	return FreeExtent{
		StartOff: le.Uint64(b[0:8]),
		Length:   le.Uint64(b[8:16]),
		Flags:    le.Uint32(b[16:20]),
	}, nil
}

// FreeExtents decodes every run in a free-map payload after its header, the load
// path a fresh open uses to seed the allocator. It bounds entry_count against the
// payload so a corrupt count cannot over-read; a count that outruns the bytes is
// ErrLength.
func FreeExtents(payload []byte, h FreeMapHeader) ([]FreeExtent, error) {
	if uint64(len(payload)) < FreeMapHeaderLen {
		return nil, ErrShort
	}
	avail := (uint64(len(payload)) - FreeMapHeaderLen) / FreeExtentSize
	if h.EntryCount > avail {
		return nil, ErrLength
	}
	entries := make([]FreeExtent, h.EntryCount)
	off := uint64(FreeMapHeaderLen)
	for i := range entries {
		e, err := ParseFreeExtent(payload[off : off+FreeExtentSize])
		if err != nil {
			return nil, err
		}
		entries[i] = e
		off += FreeExtentSize
	}
	return entries, nil
}

// FreeMapTotals sums the free and pending-free bytes across a run set. The free
// total is the forward-progress signal F9 waits on: a blocked writer proceeds when
// the free map gains allocatable bytes (section 2).
func FreeMapTotals(entries []FreeExtent) (free, pending uint64) {
	for _, e := range entries {
		if e.Flags&FreeMapPending != 0 {
			pending += e.Length
		} else {
			free += e.Length
		}
	}
	return free, pending
}
