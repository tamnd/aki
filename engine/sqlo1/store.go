package sqlo1

import (
	"context"
	"errors"
)

// ErrNotFound is returned by Get for a key the store does not hold.
var ErrNotFound = errors.New("sqlo1: key not found")

// Record is one stored row as the runtime sees it: the flat key, the value
// bytes, the expiry wall time, and the rootgen generation. Type structure
// (roots, segments, subkeys) is encoded above this seam by the per-type
// logic; a backend stores records, it never interprets them.
type Record struct {
	Key      []byte
	Value    []byte
	ExpireMs int64  // wall milliseconds, 0 means no expiry
	Gen      uint32 // rootgen generation, doc 03 section 7
	// Root marks a collection or rope root image: the value is a
	// per-type root payload, and a backend maps the flag to its root
	// representation (Track B rtype 2, Track A the root kv tag). A
	// root's own generation lives inside that payload, so Gen must be
	// zero when Root is set; backends reject the combination.
	Root bool
	// Delta qualifies a Root image per doc 06 rule W2: it differs from
	// the previous durable image of the same key only in ways segment
	// replay can reconstruct (count, min_expire, fence meta, never the
	// fence shape), and the whole dirty window it coalesces was such
	// writes. A backend whose recovery reconciles roots from segment
	// frames (rule W3) may elide this image's WAL frame; one that
	// cannot must frame it in full. Advisory either way: the image
	// itself is always exact. Meaningless without Root; backends
	// reject the combination.
	Delta bool
	// Fence marks a fence page of a paged collection root, doc 06
	// section 2.3: a plane-scoped record on a 16-byte subkey that
	// carries its rootgen in Gen exactly like a segment, so Gen must
	// be nonzero and Root unset when Fence is set; backends reject
	// both combinations. A backend with record types maps the flag to
	// its fence representation (Track B rtype 5) and retires the page
	// with its plane through the same generation door as segments.
	Fence bool
}

// Op is one mutation inside a DrainBatch: a record put, or a delete when
// Del is set (only Rec.Key is meaningful then).
type Op struct {
	Del bool
	Rec Record
}

// DrainBatch is the atomicity unit both backends honor (doc 02 section 1):
// Track A maps it to one SQL transaction, Track B to one WAL-covered extent
// write set. Seq is the drain high-water mark and lands atomically with the
// ops, which is what makes recovery replay exactly-once: a store must apply
// a batch whose Seq is not greater than its current high-water mark as a
// no-op and report success.
type DrainBatch struct {
	Seq int64
	Ops []Op
	// Bumps are the root-generation bumps riding this batch. A bump
	// retires every record minted under Rooth with a generation below
	// NewGen; it lands in the same durability unit as the ops and
	// replays with the same exactly-once discipline, plus one of its
	// own: the apply is monotonic per rooth, so a bump at or below the
	// recorded generation is a no-op. Generations start at 1; a zero
	// NewGen rejects the batch.
	Bumps []Bump
}

// Bump is one root-generation bump. It rides the drain batch that
// carries its root's replacement image (the tombstone or the new root
// record of a collection DEL or type overwrite), so a crash can never
// separate a retired generation from the image that retired it: both
// land, or recovery replays both.
type Bump struct {
	Rooth  uint64
	NewGen uint32
}

// Cursor is an opaque scan resume token. A nil Cursor starts a scan; a nil
// returned Cursor means the scan is complete.
type Cursor []byte

// StoreStats is the backend's own accounting, polled by INFO and by the
// budget reconciliation in the bench harness.
type StoreStats struct {
	Keys      int64 // live records
	DiskBytes int64 // bytes on disk, 0 if the backend has no file yet
	HighWater int64 // last applied DrainBatch.Seq
	// SchemeGroups counts compressed frame groups written per encoding
	// scheme since open, indexed by scheme id; nil when the backend has
	// written none. The doc 04 cascade selection histogram INFO shows.
	SchemeGroups []int64
	// Decode-cost counters for the compressed read path: frame
	// payloads decoded, uncompressed bytes those decodes produced, and
	// reads served from an already-decoded frame. All zero on backends
	// without compressed groups.
	FrameDecodes     int64
	FrameDecodeBytes int64
	FrameHits        int64
}

// Store is the seam between the shared runtime and a backend (doc 02
// section 1). Implementations: the placeholder MemStore here, sqlo1a over
// SQLite from A2, sqlo1b over the Track B format from B2.
//
// Get returns ErrNotFound for a missing key. BatchGet returns one Record
// per requested key in order, with a nil Key marking a miss; it exists so
// a backend can turn a cold-miss batch into one IO round instead of N.
// ApplyBatch must not retain any op memory after it returns: the drain
// scheduler passes slices aliasing the hot tier's arenas, which the next
// write may rewrite. Both real backends copy by construction (SQLite
// binds, Track B fills group buffers); a store that keeps bytes in RAM
// must clone them.
// Scan visits records until fn returns false or the store is exhausted,
// in an order that is stable while the store is quiescent, and returns the
// cursor to resume from.
type Store interface {
	Get(ctx context.Context, key []byte) (Record, error)
	BatchGet(ctx context.Context, keys [][]byte) ([]Record, error)
	ApplyBatch(ctx context.Context, b *DrainBatch) error
	Scan(ctx context.Context, cur Cursor, fn func(Record) bool) (Cursor, error)
	Stats() StoreStats
}
