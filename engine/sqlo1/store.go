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
