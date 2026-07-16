package sqlo1

import "unsafe"

// The hot-tier header, doc 04 section 3: one per dirty or resident key,
// packed into a dense slice behind the hash index. Header bytes are the
// enemy, so the struct is size-asserted at 48 bytes both ways and every
// field earns its place.
//
// Doc 04's field list also named keyHash u64, but the listed fields sum
// to 52 bytes, which Go pads to 56, not the 48 the doc's cost accounting
// uses. The hash is the one derivable field: the index map already keys
// on it, and the paths that need it away from a live lookup (eviction
// dropping the index entry, ghost-ring insert) still hold the key bytes
// in the arena and can rehash. Dropping it lands the struct at exactly
// 48 with spare bytes for future fields (the queued flag below took the
// first); doc 04 is amended accordingly.
type hdr struct {
	vptr    uint64 // disk position when clean; 0 when dirty-only
	keyRef  uint32 // key arena ref
	valRef  uint32 // value arena ref
	klen    uint16
	state   uint8
	typeTag uint8
	// queued means the write-behind queue holds an entry for this slot,
	// which is what keeps the queue duplicate-free under re-dirtying
	// (the coalescing rule: five writes between drains, one queue entry).
	// It took one of the header's spare bytes.
	queued uint8
	// expireRem is the low 10 bits of expire_ms, so the header carries
	// the exact expiry: expireLo is the doc 11 wheel projection (floor,
	// so the trigger tick never fires late) and lo<<10|rem reconstructs
	// the millisecond for the confirm check and the drain. It took the
	// header's last two spare bytes.
	expireRem uint16
	lastRead  uint32 // coarse ticks, 1s resolution
	lastWrite uint32
	prevRead  uint32 // WATT-lite second timestamp
	prevWrite uint32
	expireLo  uint32 // expire_ms>>10, 0 if none
	gen       uint32 // collection root generation, else 0
}

// hdrSize is pinned in both directions: either array length goes
// negative, and the build breaks, if the struct grows or shrinks.
const hdrSize = 48

var (
	_ [hdrSize - unsafe.Sizeof(hdr{})]byte
	_ [unsafe.Sizeof(hdr{}) - hdrSize]byte
)

// Record states per the doc 04 state machine. Cold has no header at all;
// ghost headers exist only transiently while their timestamps move to
// the ghost ring.
const (
	stateDirty    uint8 = 1
	stateResident uint8 = 2
	stateGhost    uint8 = 3
)

// Type tags, doc 12 command surface order.
const (
	TagString uint8 = 1
	TagHash   uint8 = 2
	TagList   uint8 = 3
	TagSet    uint8 = 4
	TagZset   uint8 = 5
	TagStream uint8 = 6
)

// TagRoot is a flag bit ORed onto a type tag, not a tag value: the
// header's record is a collection or rope root image, and drain
// carries it across the seam as Record.Root. The low bits stay the
// type tag.
const TagRoot uint8 = 1 << 7
