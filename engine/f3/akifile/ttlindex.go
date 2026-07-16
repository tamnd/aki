package akifile

// The TTL-index codec (spec 2064/f3/07 section 3, "TTL-class segments"). A ttl_index
// payload is the reclaim accelerator: per TTL class, its expiry upper bound and the
// list of segment offsets whose records all belong to that class. The writer
// maintains it as class segments seal, it is checkpointed like every other root, and
// active expiry consults it to reclaim wholly-expired classes without scanning
// segments. Recovery step 11 compares every class bound against the clock at open and
// re-queues the segments of any class the clock has passed (spec section 6, test T8).
//
// A TTL class is an opaque u32 here: the class encoding (an epoch bucket at
// exponentially widening granularity) is owned by 16-expiry-eviction-memory.md. This
// file leans on one property only, that every record in a class segment expires no
// later than the class's expiry_upper_bound_unix, so a class whose bound is at or
// below the clock is wholly expired and all its segments are reclaimable.
//
// The index is written whole at each commit like the free map and the extent table,
// not as a delta chain, because the set of live TTL classes is bounded and small next
// to the data; a class that expired is simply absent from the next full image. The
// payload carries no checksum of its own; the ttl_index segment header's payload CRC
// covers it, so the TTL3 magic is a kind cross-check, not an integrity field. Codec
// only: it frames into and reads out of a caller-owned payload and never touches a
// File. The writer that maintains the live class table and the expiry pass that acts
// on it are separate slices.

// TTLIndexMagic is the ttl_index payload sentinel.
const TTLIndexMagic = "TTL3"

const (
	// TTLIndexHeaderLen is the fixed ttl_index header size.
	TTLIndexHeaderLen = 16
	// TTLClassHeaderLen is one class's fixed header: class u32, segment_count u32,
	// and expiry_upper_bound_unix u64. Segment offsets follow it.
	TTLClassHeaderLen = 16
	// TTLSegmentSize is one segment offset in a class's list.
	TTLSegmentSize = 8
)

// TTLIndexHeader counts the classes that follow. Like the free map, the header
// carries no derived total, so there is no header-versus-body invariant to tear.
type TTLIndexHeader struct {
	ClassCount uint64 // TTL classes that follow the header
}

// TTLClass is one TTL class's reclaim entry: its opaque class id, the upper bound on
// every member record's expiry, and the offsets of the segments that hold them.
type TTLClass struct {
	Class           uint32   // the opaque TTL-class id (F11), owned by the expiry model
	ExpiryUpperUnix uint64   // no member record expires later than this
	Segments        []uint64 // offsets of the segments in this class
}

// AppendTTLIndexHeader frames a ttl_index header onto dst. Classes follow with
// AppendTTLClassHeader and AppendTTLSegment, so a large index streams out in bounded
// slices without holding every class in memory at once.
func AppendTTLIndexHeader(dst []byte, h TTLIndexHeader) []byte {
	var b [TTLIndexHeaderLen]byte
	copy(b[0:4], TTLIndexMagic)
	// b[4:8] reserved, left zero.
	le.PutUint64(b[8:16], h.ClassCount)
	return append(dst, b[:]...)
}

// AppendTTLClassHeader frames one class's 16-byte header onto dst. segmentCount
// segment offsets follow with AppendTTLSegment.
func AppendTTLClassHeader(dst []byte, class uint32, segmentCount uint32, expiryUpperUnix uint64) []byte {
	var b [TTLClassHeaderLen]byte
	le.PutUint32(b[0:4], class)
	le.PutUint32(b[4:8], segmentCount)
	le.PutUint64(b[8:16], expiryUpperUnix)
	return append(dst, b[:]...)
}

// AppendTTLSegment frames one 8-byte segment offset onto dst.
func AppendTTLSegment(dst []byte, segOff uint64) []byte {
	var b [TTLSegmentSize]byte
	le.PutUint64(b[0:8], segOff)
	return append(dst, b[:]...)
}

// ParseTTLIndexHeader decodes and validates a ttl_index header: only the magic, since
// the header carries no invariant beyond its count.
func ParseTTLIndexHeader(b []byte) (TTLIndexHeader, error) {
	if len(b) < TTLIndexHeaderLen {
		return TTLIndexHeader{}, ErrShort
	}
	if string(b[0:4]) != TTLIndexMagic {
		return TTLIndexHeader{}, ErrMagic
	}
	return TTLIndexHeader{ClassCount: le.Uint64(b[8:16])}, nil
}

// TTLClasses decodes every class in a ttl_index payload after its header, the load
// path a fresh open uses to seed the expiry accelerator. It walks the nested
// structure with a bounds check at every step so a corrupt count (classes or
// segments) cannot over-read: a count that outruns the remaining bytes is ErrLength.
func TTLClasses(payload []byte, h TTLIndexHeader) ([]TTLClass, error) {
	if uint64(len(payload)) < TTLIndexHeaderLen {
		return nil, ErrShort
	}
	// Every class costs at least its fixed header, so a count beyond the remaining
	// bytes divided by that floor is corrupt before we walk a single offset.
	rest := uint64(len(payload)) - TTLIndexHeaderLen
	if h.ClassCount > rest/TTLClassHeaderLen {
		return nil, ErrLength
	}
	classes := make([]TTLClass, h.ClassCount)
	off := uint64(TTLIndexHeaderLen)
	for i := range classes {
		if off+TTLClassHeaderLen > uint64(len(payload)) {
			return nil, ErrLength
		}
		class := le.Uint32(payload[off : off+4])
		segCount := le.Uint32(payload[off+4 : off+8])
		expiry := le.Uint64(payload[off+8 : off+16])
		off += TTLClassHeaderLen

		need := uint64(segCount) * TTLSegmentSize
		if need > uint64(len(payload))-off {
			return nil, ErrLength
		}
		segs := make([]uint64, segCount)
		for j := range segs {
			segs[j] = le.Uint64(payload[off : off+TTLSegmentSize])
			off += TTLSegmentSize
		}
		classes[i] = TTLClass{Class: class, ExpiryUpperUnix: expiry, Segments: segs}
	}
	return classes, nil
}

// ExpiredSegments collects the segment offsets of every wholly-expired class, the
// reclaim list active expiry and recovery step 11 queue for the free map: a class is
// wholly expired when its upper bound is at or below nowUnix, so all its member
// records have passed and every one of its segments is reclaimable.
func ExpiredSegments(classes []TTLClass, nowUnix uint64) []uint64 {
	var out []uint64
	for _, c := range classes {
		if c.ExpiryUpperUnix <= nowUnix {
			out = append(out, c.Segments...)
		}
	}
	return out
}
