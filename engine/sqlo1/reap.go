package sqlo1

// The sampling reaper, doc 11 section 3.3: a background pass over the
// cold index that finds expired user records and turns them into
// ordinary tombstones. It exists to bound DBSIZE staleness and index
// bloat on workloads where compaction alone lags (huge cold volatile
// sets with no write traffic to book garbage), and it is pure
// optimization: switching it off changes no visible semantics, so it
// ships off by default until the gate box confirms the lab verdict.
//
// The shape comes from labs/sqlo1/t7/01_reaper. A pass runs under the
// store lock, so its duration is exactly the stall a queued command
// can see, and pass cost varies 20x with keyspace composition because
// the per-entry probe dominates; a fixed chunk budget therefore
// cannot hold a duty target, and the pass is TIME-boxed instead. Hits
// become dirty tombstones in the hot tier and ride the next drain
// cycle, because a dedicated tombstone ApplyBatch is fsync-bound at
// ~4ms regardless of batch size.

import (
	"context"
	"time"
)

const (
	// reapBox bounds one pass's time under the store lock. At the
	// reapTick cadence this holds the reaper's duty near the doc 11
	// 1 percent target by construction, whatever the keyspace mix,
	// and bounds the foreground stall at the box plus one chain
	// overshoot.
	reapBox = 100 * time.Microsecond
	// reapBatch caps one pass's candidates. The tombstones join the
	// hot tier's dirty queue, so the cap is a RAM bound on a pass's
	// bookings, not a batch the reaper flushes itself.
	reapBatch = 256
	// reapTick is the cadence Serve drives passes at.
	reapTick = 10 * time.Millisecond
)

// ReapCandidate is one expired user record a ReapScan pass collected:
// the key to tombstone and, for a root, the payload the type layer
// needs to retire the plane. Both slices are copies and survive
// subsequent store calls.
type ReapCandidate struct {
	Key   []byte
	Value []byte
	Root  bool
}

// ExpiryReaper is the optional Store capability behind the sampling
// reaper. One call is one time-boxed pass over the cold index from
// the store's expiry cursor, collecting at most maxKeys expired user
// records; segment, fence, and meta records are never candidates
// because their planes die by rootgen when the root's tombstone
// lands. A store without the capability simply has no reaper.
type ExpiryReaper interface {
	ReapScan(box time.Duration, maxKeys int) ([]ReapCandidate, error)
}

// ReapStep runs one reaper pass and reports how many keys it reaped.
// It lives on the string layer because a reaped root retires its
// plane through the same shared-prefix decode as Del, whatever the
// root's type. A key the hot tier holds in any state is skipped: the
// resident copy is authoritative and the index record is a stale
// version, so touching it would delete live data. The tombstone is
// best-effort in the same way promotion is; a key the tier had no
// room for comes back on a later lap.
func (s *Str) ReapStep(ctx context.Context) (int, error) {
	rp, ok := s.t.st.(ExpiryReaper)
	if !ok {
		return 0, nil
	}
	cands, err := rp.ReapScan(reapBox, reapBatch)
	if err != nil {
		return 0, err
	}
	n := 0
	for i := range cands {
		c := &cands[i]
		if s.t.ht.has(c.Key) {
			s.t.stats.ReapSkips++
			continue
		}
		// Decode the plane before the tombstone books: a root whose
		// payload will not decode must surface loudly, not land a
		// tombstone whose missing genbump strands its segments.
		var m strMeta
		if c.Root {
			if m, err = s.rootMeta(c.Value, 0); err != nil {
				return n, err
			}
		}
		ok, err := s.t.reapTombstone(ctx, c.Key)
		if err != nil {
			return n, err
		}
		if !ok {
			s.t.stats.ReapSkips++
			continue
		}
		if m.rope || (m.otherType && !m.planeless) {
			s.retire(c.Key, m.root)
		}
		n++
	}
	s.t.stats.Reaped += int64(n)
	if n > 0 {
		if _, err := s.t.lad.step(ctx); err != nil {
			return n, err
		}
	}
	return n, nil
}

// reapTombstone files the dirty tombstone for one reaped cold key.
// Unlike Del's tail it never applies vacate pressure: the reaper is
// an optimization and a refused slot just means the key waits for a
// later lap.
func (t *Tiered) reapTombstone(ctx context.Context, key []byte) (bool, error) {
	if t.ht.delCold(key) {
		return true, nil
	}
	if err := t.makeRoomFor(ctx, len(key)); err != nil {
		return false, err
	}
	return t.ht.delCold(key), nil
}
