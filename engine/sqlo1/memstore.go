package sqlo1

import (
	"context"
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
	highWater int64
}

// NewMemStore returns an empty placeholder store.
func NewMemStore() *MemStore {
	return &MemStore{recs: make(map[string]Record)}
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
	for _, op := range b.Ops {
		if op.Del {
			delete(s.recs, string(op.Rec.Key))
			continue
		}
		// The batch memory belongs to the caller (it may alias arena
		// bytes the next write rewrites), so a store that keeps records
		// in RAM clones what it keeps.
		rec := op.Rec
		rec.Key = append([]byte(nil), op.Rec.Key...)
		rec.Value = append([]byte(nil), op.Rec.Value...)
		s.recs[string(rec.Key)] = rec
	}
	s.highWater = b.Seq
	return nil
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

func (s *MemStore) Stats() StoreStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StoreStats{Keys: int64(len(s.recs)), HighWater: s.highWater}
}
