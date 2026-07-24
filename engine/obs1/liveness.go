// Chain-observed failure detection (spec 2064/obs1 doc 02 section 4.2).
// There is no ping mesh: a node is suspect when its newest heartbeat-or-
// commit on the chain is older than TTL plus skew. Every node follows the
// chain anyway, so detection is a byproduct of reading, and independent
// observers converge on the same verdicts at the same chain positions.
// The wall stamps are this observer's own clock at apply time, advisory
// like everything DeadlineMS-shaped (C-I7): the chain orders events, the
// clock only ages them.
package obs1

import (
	"cmp"
	"fmt"
	"slices"
	"sync"
	"time"
)

// LivenessRow is one node's newest observed activity: the chain position
// of its last batch and when this observer applied it.
type LivenessRow struct {
	Pos ChainPos
	At  time.Time
}

// Liveness wraps a ChainApplier and stamps every writer's activity as
// batches fold through it, the composition Checkpointer set the pattern
// for. One Liveness per followed domain; the appender serializes applies,
// the mutex only guards readers against the apply path.
type Liveness struct {
	inner   ChainApplier
	horizon time.Duration
	now     func() time.Time

	mu   sync.Mutex
	rows map[uint64]LivenessRow
}

// NewLiveness wraps inner. The horizon is TTL plus skew, the doc 02
// section 4.2 staleness bound.
func NewLiveness(inner ChainApplier, ttl, skew time.Duration, now func() time.Time) (*Liveness, error) {
	if inner == nil || now == nil {
		return nil, fmt.Errorf("obs1: liveness needs an inner applier and a clock")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("obs1: liveness needs a positive ttl")
	}
	return &Liveness{
		inner:   inner,
		horizon: ttl + skew,
		now:     now,
		rows:    make(map[uint64]LivenessRow),
	}, nil
}

// ApplyChain folds the batch through the inner applier first, then
// stamps the writer's row. A member(leave) drops its node's row: a node
// that left is gone, not suspect. The leave-then-stamp order means a
// writer leaving for itself ends the batch with no row, which is the
// point of leaving.
func (l *Liveness) ApplyChain(pos ChainPos, h Header, batch ChainBatch) error {
	if err := l.inner.ApplyChain(pos, h, batch); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.now()
	l.rows[h.Writer] = LivenessRow{Pos: pos, At: t}
	for _, r := range batch.Records {
		switch rec := r.(type) {
		case MemberRecord:
			switch rec.Op {
			case MemberJoin:
				l.rows[rec.Node] = LivenessRow{Pos: pos, At: t}
			case MemberLeave:
				delete(l.rows, rec.Node)
			}
		}
	}
	return nil
}

// Primed stamps every member the checkpoint carries at pos, now: this
// observer first saw those nodes alive at boot, the same convention the
// checkpointer uses for lease stamps.
func (l *Liveness) Primed(members []Member, pos ChainPos) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.now()
	for _, m := range members {
		l.rows[m.Node] = LivenessRow{Pos: pos, At: t}
	}
}

// LastSeen reports a node's newest observed activity.
func (l *Liveness) LastSeen(node uint64) (LivenessRow, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	row, ok := l.rows[node]
	return row, ok
}

// Suspect reports whether a node's newest activity is older than the
// horizon at the given instant. A node this observer never saw act is
// suspect: with no activity there is nothing to trust.
func (l *Liveness) Suspect(node uint64, at time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	row, ok := l.rows[node]
	return !ok || at.Sub(row.At) > l.horizon
}

// Suspects filters the given member table to the nodes suspect at the
// given instant, ascending by node id. Callers pass the fold's Members()
// so a departed node, gone from the table, is never on the list.
func (l *Liveness) Suspects(members []Member, at time.Time) []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []uint64
	for _, m := range members {
		row, ok := l.rows[m.Node]
		if !ok || at.Sub(row.At) > l.horizon {
			out = append(out, m.Node)
		}
	}
	slices.SortFunc(out, func(a, b uint64) int { return cmp.Compare(a, b) })
	return out
}
