package keyspace

import "sync/atomic"

// version.go stripes the keyspace write-version counter across cache lines.
//
// Every mutation stamps its ValueHeader with a write version, and the only thing
// the keyspace ever does with a version is compare two headers of the *same* key:
// the write-behind reconciliation (wbPending), the hot-value cache, and the
// version-guarded delete all ask "is the staged version newer than this one" for
// one key, never across keys. So a version has to be strictly increasing per key,
// but it does not have to be a single global sequence.
//
// A single atomic.Uint64 gave the global sequence, which is more than the
// comparisons need and costs more than they can afford: under a write-heavy load
// on many cores every writer does a read-modify-write on one int64, so one cache
// line bounces between the cores and each writer waits its turn. At 10-core SET
// saturation that one counter cost about 18 ns/op, the largest shared-line cost
// left on the path after the used_memory counter was striped (see striped.go).
//
// versionCounter keeps the per-key monotonicity the comparisons rely on while
// dropping the global contention: it is an array of independent counters, and a
// key always draws from the cell its hash selects, the same CRC16 the keyspace
// shards by. A given key therefore always increments the same cell and sees a
// strictly increasing sequence, while two different keys usually fall on different
// cells and increment without fighting over a line. Versions from different cells
// are never ordered against each other, and nothing outside the keyspace reads a
// version, so the loss of a single global sequence is not observable.
//
// numVersionStripes is a power of two at or above any realistic core count, so
// concurrent writers on distinct keys almost always land on distinct lines.
const numVersionStripes = 64

// versionCell is one counter padded to a full 64-byte cache line so two cells
// never share a line and an increment on one core cannot invalidate another's.
type versionCell struct {
	n atomic.Uint64
	_ [56]byte
}

type versionCounter struct {
	cells [numVersionStripes]versionCell
}

// next returns the next write version for key, drawn from key's cell. The cell is
// chosen by HashSlot, the same hash ShardOf uses, so it is stable for a key across
// its whole lifetime and the returned sequence is strictly increasing per key.
func (v *versionCounter) next(key []byte) uint64 {
	return v.cells[uint(HashSlot(key))&(numVersionStripes-1)].n.Add(1)
}
