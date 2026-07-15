package sqlo1b

import (
	"fmt"
	"io"
)

// ExtentState is the lifecycle position of one extent (doc 03
// section 4.1): free, active (append tail of exactly one owner
// stream), sealed (immutable), or quarantined (freed, waiting for a
// durable superblock at or above its tag before reuse).
type ExtentState uint8

const (
	StateFree ExtentState = iota
	StateActive
	StateSealed
	StateQuarantined
)

func (s ExtentState) String() string {
	switch s {
	case StateFree:
		return "free"
	case StateActive:
		return "active"
	case StateSealed:
		return "sealed"
	case StateQuarantined:
		return "quarantined"
	}
	return fmt.Sprintf("state(%d)", uint8(s))
}

// streamKey names an owner stream: one active extent per kind and
// shard.
type streamKey struct {
	kind  uint8
	shard uint16
}

// Grid tracks extent states in memory. Extent 0 is the header extent
// and is never allocatable. The grid is not concurrency-safe; the
// owner serializes (single-writer discipline, doc 04).
type Grid struct {
	states []ExtentState
	qtag   map[uint64]uint64 // quarantined extent -> superblock seq tag
	active map[streamKey]uint64
}

// NewGrid returns a grid of extentCount extents, all free except the
// header extent.
func NewGrid(extentCount uint64) *Grid {
	g := &Grid{
		states: make([]ExtentState, extentCount),
		qtag:   make(map[uint64]uint64),
		active: make(map[streamKey]uint64),
	}
	if extentCount > 0 {
		g.states[0] = StateSealed // the header extent is never handed out
	}
	return g
}

// ExtentCount reports the grid size.
func (g *Grid) ExtentCount() uint64 { return uint64(len(g.states)) }

// State reports one extent's lifecycle position.
func (g *Grid) State(ext uint64) ExtentState { return g.states[ext] }

// FreeCount counts free extents by scanning the states. The pressure
// gauge calls it only on byte-capped stores; if the scan ever shows on
// a profile at scale, the fix is a running counter maintained at the
// state transitions.
func (g *Grid) FreeCount() uint64 {
	var n uint64
	for _, st := range g.states {
		if st == StateFree {
			n++
		}
	}
	return n
}

// Grow appends n free extents (the file grows by whole extents and
// never shrinks in v0).
func (g *Grid) Grow(n uint64) {
	g.states = append(g.states, make([]ExtentState, n)...)
}

// Allocate hands the lowest free extent to the (kind, shard) stream
// as its append tail. Free extents are reused before the file grows,
// so the caller Grows only when Allocate reports no space. A stream
// seals its current tail before allocating the next; two active
// extents on one stream is a bug, not a policy.
func (g *Grid) Allocate(kind uint8, shard uint16) (uint64, error) {
	if kind < KindVlog || kind > KindStats {
		return 0, fmt.Errorf("sqlo1b: allocate kind %d out of range", kind)
	}
	key := streamKey{kind, shard}
	if ext, ok := g.active[key]; ok {
		return 0, fmt.Errorf("sqlo1b: stream kind %d shard %d already active on extent %d", kind, shard, ext)
	}
	for ext := uint64(1); ext < uint64(len(g.states)); ext++ {
		if g.states[ext] == StateFree {
			g.states[ext] = StateActive
			g.active[key] = ext
			return ext, nil
		}
	}
	return 0, fmt.Errorf("sqlo1b: no free extent for kind %d shard %d, grow the file", kind, shard)
}

// Seal transitions the stream's active extent to sealed and releases
// the stream for its next Allocate. Only active extents seal.
func (g *Grid) Seal(kind uint8, shard uint16) (uint64, error) {
	key := streamKey{kind, shard}
	ext, ok := g.active[key]
	if !ok {
		return 0, fmt.Errorf("sqlo1b: seal with no active extent on kind %d shard %d", kind, shard)
	}
	g.states[ext] = StateSealed
	delete(g.active, key)
	return ext, nil
}

// Free quarantines a sealed extent, tagged with the current
// superblock seq. Only sealed extents free (active tails are owned
// by a stream, and freeing free space is a double free).
func (g *Grid) Free(ext, curSeq uint64) error {
	if ext == 0 {
		return fmt.Errorf("sqlo1b: the header extent cannot be freed")
	}
	if s := g.states[ext]; s != StateSealed {
		return fmt.Errorf("sqlo1b: free extent %d in state %s, want sealed", ext, s)
	}
	g.states[ext] = StateQuarantined
	g.qtag[ext] = curSeq
	return nil
}

// ReleaseQuarantine frees every quarantined extent whose tag is at or
// below the seq of the last durable superblock, and reports how many
// it released. No pointer reachable from any durable root can dangle
// into space released this way; this is retirement by root sequence.
func (g *Grid) ReleaseQuarantine(durableSeq uint64) int {
	n := 0
	for ext, tag := range g.qtag {
		if tag <= durableSeq {
			g.states[ext] = StateFree
			delete(g.qtag, ext)
			n++
		}
	}
	return n
}

// Allocmap packs the grid into the on-disk bitmap: 1 bit per extent,
// set means not-free (doc 03 section 9 records only free versus
// not-free; kind and seal state live in extent headers). Quarantined
// extents are not-free, because their space is not yet reusable.
func (g *Grid) Allocmap() []byte {
	b := make([]byte, (len(g.states)+7)/8)
	for ext, s := range g.states {
		if s != StateFree {
			b[ext/8] |= 1 << (ext % 8)
		}
	}
	return b
}

// LoadGrid rebuilds free-versus-not-free state from an allocmap. The
// bitmap does not distinguish active from sealed from quarantined;
// non-free extents load as sealed, and recovery re-derives tails and
// quarantine from extent headers and the WAL.
func LoadGrid(allocmap []byte, extentCount uint64) (*Grid, error) {
	if want := (extentCount + 7) / 8; uint64(len(allocmap)) != want {
		return nil, fmt.Errorf("sqlo1b: allocmap is %d bytes, want %d for %d extents", len(allocmap), want, extentCount)
	}
	g := NewGrid(extentCount)
	for ext := uint64(1); ext < extentCount; ext++ {
		if allocmap[ext/8]&(1<<(ext%8)) != 0 {
			g.states[ext] = StateSealed
		}
	}
	return g, nil
}

// RebuildAllocmap is the repair path: rebuild the bitmap from extent
// headers alone. It is deliberately conservative, marking any extent
// whose header verifies as not-free, because a freed extent keeps
// its stale header until reuse; the rebuild may leak space (scrub
// and compaction reclaim it) but can never mark a used extent free.
// The header extent is always not-free.
func RebuildAllocmap(r io.ReaderAt, extentSize uint32, extentCount uint64) ([]byte, error) {
	b := make([]byte, (extentCount+7)/8)
	if extentCount > 0 {
		b[0] |= 1
	}
	hdr := make([]byte, ExtentHeaderSize)
	for ext := uint64(1); ext < extentCount; ext++ {
		if _, err := r.ReadAt(hdr, int64(ext)*int64(extentSize)); err != nil {
			return nil, fmt.Errorf("sqlo1b: rebuild allocmap extent %d: %w", ext, err)
		}
		if _, err := DecodeExtentHeader(hdr); err == nil {
			b[ext/8] |= 1 << (ext % 8)
		}
	}
	return b, nil
}
