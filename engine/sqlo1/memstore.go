package sqlo1

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// MemStore is the placeholder Store: a mutex-guarded map with sorted scans.
// It exists so the runtime, the server, and the bench harness are built and
// tested against a real Store implementation before either track lands.
// Nothing about it is tuned and nothing about it persists; its one job is
// to honor the Store contract exactly, including replay idempotence.
type MemStore struct {
	mu        sync.Mutex
	recs      map[string]Record
	gens      map[uint64]uint32
	highWater int64
	mintMark  uint64
	// keyRecs counts recs entries that name addressable keys (gen 0,
	// not a fence): the StoreStats.KeyEntries feed, mirroring the
	// key-class counter the real backend keeps.
	keyRecs int64
}

// keyClassRec reports whether a seam record names an addressable key.
// Segments and fences carry their plane's generation, roots and plain
// values cross the seam with gen 0.
func keyClassRec(rec *Record) bool {
	return rec.Gen == 0 && !rec.Fence
}

var _ Minter = (*MemStore)(nil)

// NewMemStore returns an empty placeholder store.
func NewMemStore() *MemStore {
	return &MemStore{recs: make(map[string]Record), gens: make(map[uint64]uint32)}
}

func (s *MemStore) Get(ctx context.Context, key []byte) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.recs[string(key)]
	if !ok {
		return Record{}, ErrNotFound
	}
	return r, nil
}

func (s *MemStore) BatchGet(ctx context.Context, keys [][]byte) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, len(keys))
	for i, k := range keys {
		if r, ok := s.recs[string(k)]; ok {
			out[i] = r
		}
	}
	return out, nil
}

func (s *MemStore) ApplyBatch(ctx context.Context, b *DrainBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.Seq <= s.highWater {
		return nil // replayed batch, already applied
	}
	for i := range b.Ops {
		// Validate before touching anything so a bad op cannot leave the
		// batch half-applied; the real backends reject at their own plan
		// or bind step for the same reason.
		op := &b.Ops[i]
		if !op.Del && op.Rec.Root && op.Rec.Gen > 0 {
			return fmt.Errorf("sqlo1: batch %d op %d: root record %x with seam gen %d", b.Seq, i, op.Rec.Key, op.Rec.Gen)
		}
		if !op.Del && op.Rec.Delta && !op.Rec.Root {
			return fmt.Errorf("sqlo1: batch %d op %d: delta flag on non-root record %x", b.Seq, i, op.Rec.Key)
		}
		if !op.Del && op.Rec.Fence && op.Rec.Root {
			return fmt.Errorf("sqlo1: batch %d op %d: fence flag on root record %x", b.Seq, i, op.Rec.Key)
		}
		if !op.Del && op.Rec.Fence && op.Rec.Gen == 0 {
			return fmt.Errorf("sqlo1: batch %d op %d: fence record %x without a generation", b.Seq, i, op.Rec.Key)
		}
	}
	for i := range b.Bumps {
		if b.Bumps[i].NewGen == 0 {
			return fmt.Errorf("sqlo1: batch %d bump %d: rooth %#x to generation 0", b.Seq, i, b.Bumps[i].Rooth)
		}
	}
	for _, op := range b.Ops {
		if op.Del {
			if old, ok := s.recs[string(op.Rec.Key)]; ok && keyClassRec(&old) {
				s.keyRecs--
			}
			delete(s.recs, string(op.Rec.Key))
			continue
		}
		// The batch memory belongs to the caller (it may alias arena
		// bytes the next write rewrites), so a store that keeps records
		// in RAM clones what it keeps.
		rec := op.Rec
		rec.Key = append([]byte(nil), op.Rec.Key...)
		rec.Value = append([]byte(nil), op.Rec.Value...)
		if old, ok := s.recs[string(rec.Key)]; ok && keyClassRec(&old) {
			s.keyRecs--
		}
		if keyClassRec(&rec) {
			s.keyRecs++
		}
		s.recs[string(rec.Key)] = rec
	}
	for _, bp := range b.Bumps {
		if bp.NewGen > s.gens[bp.Rooth] {
			s.gens[bp.Rooth] = bp.NewGen
		}
	}
	s.highWater = b.Seq
	return nil
}

// RootLive mirrors the Track B liveness probe so tests above the seam
// can observe bumps through the placeholder: a record minted under
// rooth is live unless a durable bump went past its generation.
func (s *MemStore) RootLive(rooth uint64, rootgen uint32) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return rootgen >= s.gens[rooth], nil
}

// Scan walks records in key order. The cursor is the last visited key; the
// resumed scan starts strictly after it, so a record is never visited twice
// on a quiescent store.
func (s *MemStore) Scan(ctx context.Context, cur Cursor, fn func(Record) bool) (Cursor, error) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.recs))
	after := string(cur)
	for k := range s.recs {
		if cur == nil || k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	recs := make([]Record, len(keys))
	for i, k := range keys {
		recs[i] = s.recs[k]
	}
	s.mu.Unlock()

	for i, r := range recs {
		if !fn(r) {
			return Cursor(keys[i]), nil
		}
	}
	return nil, nil
}

// MintLease implements the Minter capability at MemStore's durability
// level, which is none: the mark is as volatile as every record it
// holds, so "durable before return" is honored trivially.
func (s *MemStore) MintLease(ctx context.Context, n uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mark, err := LeaseEnd(s.mintMark, n)
	if err != nil {
		return 0, err
	}
	start := s.mintMark
	s.mintMark = mark
	return start, nil
}

func (s *MemStore) Stats() StoreStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StoreStats{Keys: int64(len(s.recs)), KeyEntries: s.keyRecs, HighWater: s.highWater}
}
