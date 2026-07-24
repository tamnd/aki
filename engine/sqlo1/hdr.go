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

// tagIntShadow marks a hot string whose parsed int64 sits in the
// HotTable's shadow map (doc 05 section 2's integer fast path). Only
// the INCR family arms it, and PutGen's wholesale typeTag assignment
// is what makes the invalidation rule structural: every byte-level
// write replaces the tag and the bit dies with it, so a stale shadow
// cannot survive any writer that did not deliberately re-arm it. The
// bit never crosses the seam; drain only reads TagRoot, TagDelta,
// and TagFence.
const tagIntShadow uint8 = 1 << 6

// TagDelta is a flag bit beside TagRoot: this root post-image differs
// from its predecessor only in ways segment replay can reconstruct,
// count, min_expire, and fence meta, never the fence shape itself.
// That is rule W2 of doc 06 section 5: a store whose replay reconciles
// roots from segment frames (rule W3) may elide this image's WAL frame
// and halve the hot path's root framing; a store that cannot must
// frame it like any root. The type layer sets it on count-only root
// writes and leaves it off structural ones (upgrade, split, merge,
// paging), and drain carries it across the seam as Record.Delta.
const TagDelta uint8 = 1 << 5

// TagFence is a flag bit beside TagRoot: this record is a fence page
// of a paged collection root (doc 06 section 2.3), a plane-scoped
// data record that carries its rootgen in Gen like a segment does.
// Drain carries it across the seam as Record.Fence so a backend with
// record types can map it (Track B rtype 5) and probe its liveness
// through the same rootgen door as segments. It never combines with
// TagRoot: a fence page is not a root image.
const TagFence uint8 = 1 << 4

// The queued byte's bits. queuedBit means the write-behind queue holds
// an entry for this slot; queuedDefer means that entry must re-file at
// the queue tail when it pops, which is how rule W1 keeps a re-dirtied
// root from draining in a batch before segments a later command wrote
// under it (the root's image summarizes them, so landing it first
// would let an eviction expose an overcounting on-disk root).
// queuedVolMask holds the volatile deferral lap count, doc 11 section
// 6: how many queue laps this dirty record has sat out waiting to die
// in RAM. The drainer caps it at maxVolDefers; a fresh transition into
// dirty resets it (enqueueDirty writes the whole byte).
const (
	queuedBit     uint8 = 1 << 0
	queuedDefer   uint8 = 1 << 1
	queuedVolStep uint8 = 1 << 2
	queuedVolMask uint8 = 3 << 2
)
