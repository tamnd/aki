// The multi-group lease manager (spec 2064/obs1 doc 02 sections 3.1, 3.2,
// 3.5): the holder-side loop that acquires free groups, renews everything
// it holds through heartbeats, and reconciles its belief against the fold
// after every append. It is the heartbeat loop the LeaseGate deferred to
// O3a: without it an idle gated group runs down its TTL and suspends.
//
// Safety never lives here. The fold fences epochs and every folder agrees
// (doc 02 section 3.3); the manager only keeps the node's belief and the
// serving gate in step with what the chain already decided. Acquisition
// covers freeness case (a), a released or never-granted group; case (b),
// takeover of a stale holder, needs the full-TTL observation discipline
// and lands with the crash-takeover slice.
package obs1

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// LeaseManagerConfig wires a manager. Appender, Fold, and Gate are the
// node's own: the fold must be the applier fed by the appender (directly
// or through wrappers), so an Append's catch-up lands in it before Append
// returns.
type LeaseManagerConfig struct {
	Self     uint64
	Appender *ChainAppender
	Fold     *LeaseFold
	Gate     *LeaseGate
	Interval time.Duration    // heartbeat cadence, zero takes DefaultHeartbeatEvery
	Now      func() time.Time // nil takes time.Now
}

// LeaseManager runs on the appender owner's goroutine like everything
// around the appender; the mutex only lets other goroutines read Held and
// HeartbeatDue.
type LeaseManager struct {
	self     uint64
	ap       *ChainAppender
	fold     *LeaseFold
	gate     *LeaseGate
	interval time.Duration
	now      func() time.Time

	mu         sync.Mutex
	held       map[uint16]uint32 // group -> epoch we believe we hold
	lastAppend time.Time
}

// NewLeaseManager builds a manager over the node's chain machinery.
func NewLeaseManager(cfg LeaseManagerConfig) (*LeaseManager, error) {
	if cfg.Self == 0 {
		return nil, fmt.Errorf("obs1: lease manager needs a nonzero node id")
	}
	if cfg.Appender == nil || cfg.Fold == nil || cfg.Gate == nil {
		return nil, fmt.Errorf("obs1: lease manager needs an appender, a fold, and a gate")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultHeartbeatEvery
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &LeaseManager{
		self: cfg.Self, ap: cfg.Appender, fold: cfg.Fold, gate: cfg.Gate,
		interval: cfg.Interval, now: cfg.Now,
		held: make(map[uint16]uint32),
	}, nil
}

// Acquire appends a grant for a free group, naming ourselves at the
// fold's next epoch, and reports whether we won. The append's catch-up
// applies everything ahead of us first, so a rival grant that landed
// earlier moves the epoch and our record folds rejected; the fold's
// verdict after the append is the truth either way. A group the fold
// already shows as ours (boot found our lease alive) is adopted without
// an append. The belief clock starts before the append lands, the
// conservative end of the doc 02 section 3.5 rule.
func (m *LeaseManager) Acquire(ctx context.Context, group uint16) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.held[group]; ok {
		return true, nil
	}
	t := m.now()
	if node, epoch, ok := m.fold.Holder(group); ok {
		if node != m.self {
			return false, nil
		}
		m.held[group] = epoch
		m.gate.Regrant(group, t)
		return true, nil
	}
	epoch := m.fold.NextEpoch(group)
	rec := GrantRecord{Group: group, Node: m.self, Epoch: epoch}
	if _, err := m.ap.Append(ctx, []ChainRecord{rec}); err != nil {
		return false, err
	}
	m.lastAppend = t
	m.reconcileLocked(t)
	if node, e, ok := m.fold.Holder(group); ok && node == m.self && e == epoch {
		m.held[group] = e
		m.gate.Regrant(group, t)
		return true, nil
	}
	return false, nil
}

// Release voluntarily frees a group: the gate stops serving it first,
// then the release record lands under our epoch. If the append fails the
// group stays ours on the chain but suspended at the gate, the safe side;
// the caller retries. The final flush that precedes a release in the
// doc 02 section 4.3 sequence is the graceful-handoff slice's business.
func (m *LeaseManager) Release(ctx context.Context, group uint16) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	epoch, ok := m.held[group]
	if !ok {
		return nil
	}
	m.gate.Release(group)
	t := m.now()
	if _, err := m.ap.Append(ctx, []ChainRecord{ReleaseRecord{Group: group, Epoch: epoch}}); err != nil {
		return err
	}
	delete(m.held, group)
	m.lastAppend = t
	m.reconcileLocked(t)
	return nil
}

// Heartbeat appends one heartbeat record, which renews every lease the
// fold says we hold (doc 02 section 3.1: renewal is implicit and carries
// the owned-group list by omission), then extends the gate's belief for
// the groups that survived reconciliation. A group granted away while we
// were quiet demotes here rather than renewing, which is the section 3.5
// retry-landed-too-late half.
func (m *LeaseManager) Heartbeat(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.now()
	if _, err := m.ap.Append(ctx, []ChainRecord{HeartbeatRecord{}}); err != nil {
		return err
	}
	m.lastAppend = t
	m.reconcileLocked(t)
	for group := range m.held {
		m.gate.Renewed(group, t)
	}
	return nil
}

// HeartbeatDue reports whether the cadence calls for a heartbeat at now:
// one per interval, suppressed while any append landed inside it, since
// every append by us renews everything we hold. NoteAppend feeds the
// suppression from the commit path.
func (m *LeaseManager) HeartbeatDue(now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastAppend.IsZero() || now.Sub(m.lastAppend) >= m.interval
}

// NoteAppend records that an append of ours landed at t (a data commit
// through the committer, a checkpoint record), suppressing the next
// heartbeat: a commit is a heartbeat.
func (m *LeaseManager) NoteAppend(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.After(m.lastAppend) {
		m.lastAppend = t
	}
}

// Reconcile compares the belief against the fold and settles every
// difference through the gate; the owner calls it after any Follow, the
// manager's own appends run it internally. Demotion redirects to the new
// holder's RESP endpoint from the member table.
func (m *LeaseManager) Reconcile() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconcileLocked(m.now())
}

func (m *LeaseManager) reconcileLocked(t time.Time) {
	for group, epoch := range m.held {
		node, cur, ok := m.fold.Holder(group)
		switch {
		case ok && node == m.self && cur == epoch:
			// Still ours.
		case ok && node == m.self:
			// Re-granted to us at a newer epoch behind our back; adopt it.
			m.held[group] = cur
			m.gate.Regrant(group, t)
		case ok:
			m.gate.Demote(group, m.endpoint(node))
			delete(m.held, group)
		default:
			// Released on the chain without us asking: treat as a plain
			// drop, writes suspend until someone grants it again.
			m.gate.Release(group)
			delete(m.held, group)
		}
	}
}

// endpoint is the RESP endpoint the member table carries for a node, ""
// when the node has no row yet; the doc 07 redirect tolerates an empty
// host.
func (m *LeaseManager) endpoint(node uint64) string {
	for _, mem := range m.fold.Members() {
		if mem.Node == node {
			return mem.Resp
		}
	}
	return ""
}

// Held lists the groups the manager believes it holds, ascending.
func (m *LeaseManager) Held() []uint16 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]uint16, 0, len(m.held))
	for group := range m.held {
		out = append(out, group)
	}
	slices.Sort(out)
	return out
}
