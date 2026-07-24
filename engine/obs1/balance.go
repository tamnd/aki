// The balancer (spec 2064/obs1 doc 02 section 4.6): rendezvous hashing
// over (group, live members) with capacity weights decides each group's
// preferred node, and each node's tick sheds at most one group per tick
// that prefers elsewhere and takes at most one free group per tick it
// prefers. Placement is advisory and safety never depends on it (doc 02
// section 3.2): any grant that follows the freeness rule is valid, the
// policy only prevents thrash, which is also why one-per-tick is enough.
package obs1

import (
	"context"
	"fmt"
	"math"
	"time"
)

// DefaultBalanceEvery is the doc 02 section 4.6 tick.
const DefaultBalanceEvery = 10 * time.Second

// mix64 is splitmix64's finalizer, the stateless hash rendezvous scoring
// runs on; every node computes identical scores from identical inputs.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// rendezvousScore is the weighted rendezvous score of node for group,
// the logarithmic method: draw a uniform (0,1) from the hash and score
// -weight/ln(u), so a node with twice the weight expects twice the
// groups. Higher wins.
func rendezvousScore(group uint16, node uint64, weight uint16) float64 {
	h := mix64(node ^ mix64(uint64(group)+1))
	// 53 bits into (0,1), never exactly 0 or 1.
	u := (float64(h>>11) + 0.5) / (1 << 53)
	return -float64(weight) / math.Log(u)
}

// PreferredNode is the group's rendezvous winner among the given members.
// A member with weight zero is out of the running, which is also the
// operator's drain knob. False when no member can take the group. Ties
// break toward the lower node id, though a float tie takes two equal
// scores from distinct 64-bit hashes first.
func PreferredNode(group uint16, members []Member) (uint64, bool) {
	var best uint64
	bestScore := math.Inf(-1)
	ok := false
	for _, m := range members {
		if m.Weight == 0 {
			continue
		}
		s := rendezvousScore(group, m.Node, m.Weight)
		if !ok || s > bestScore || (s == bestScore && m.Node < best) {
			best, bestScore, ok = m.Node, s, true
		}
	}
	return best, ok
}

// BalancePlan is one tick's worth of movement: at most one group to shed
// and one to take.
type BalancePlan struct {
	Shed    uint16
	HasShed bool
	Take    uint16
	HasTake bool
}

// PlanBalance computes self's move for one tick over groups [0, nGroups):
// the lowest-numbered held group whose preferred node is someone else
// sheds, and the lowest-numbered free group that prefers self is taken.
// Members should already be the live view (the caller filters suspects
// through the Liveness verdicts); an empty member list plans nothing.
func PlanBalance(self uint64, nGroups int, members []Member, fold *LeaseFold) BalancePlan {
	var plan BalancePlan
	for g := 0; g < nGroups; g++ {
		group := uint16(g)
		pref, ok := PreferredNode(group, members)
		if !ok {
			continue
		}
		node, _, held := fold.Holder(group)
		switch {
		case held && node == self && pref != self && !plan.HasShed:
			plan.Shed, plan.HasShed = group, true
		case !held && pref == self && !plan.HasTake:
			plan.Take, plan.HasTake = group, true
		}
		if plan.HasShed && plan.HasTake {
			break
		}
	}
	return plan
}

// Balancer drives the tick for one node: every interval, jittered per
// node so a fleet's ticks spread out, it plans and executes one move.
// Takes go through the lease manager's Acquire; sheds go through the
// Shed hook, which the graceful-handoff machinery owns (doc 02 section
// 4.6 sheds via handoff, never by dropping the group on the floor), so
// a nil hook means this node keeps what it holds.
type Balancer struct {
	mgr     *LeaseManager
	fold    *LeaseFold
	self    uint64
	nGroups int
	every   time.Duration
	jitter  time.Duration
	now     func() time.Time

	// Alive reports whether a node may receive placement, the Liveness
	// verdict; nil means every member is live.
	Alive func(node uint64) bool
	// Shed hands a group to the handoff machinery.
	Shed func(ctx context.Context, group uint16) error

	last time.Time
}

// NewBalancer builds a balancer for the manager's node. Zero every takes
// the doc 02 default; the jitter is a deterministic per-node fraction of
// the interval, up to a quarter of it.
func NewBalancer(mgr *LeaseManager, nGroups int, every time.Duration, now func() time.Time) (*Balancer, error) {
	if mgr == nil {
		return nil, fmt.Errorf("obs1: balancer needs a lease manager")
	}
	if nGroups <= 0 {
		return nil, fmt.Errorf("obs1: balancer needs a positive group count")
	}
	if every <= 0 {
		every = DefaultBalanceEvery
	}
	if now == nil {
		now = time.Now
	}
	quarter := uint64(every / 4)
	return &Balancer{
		mgr: mgr, fold: mgr.fold, self: mgr.self, nGroups: nGroups,
		every: every, jitter: time.Duration(mix64(mgr.self) % (quarter + 1)),
		now: now,
	}, nil
}

// Due reports whether the tick has come around at now.
func (b *Balancer) Due(now time.Time) bool {
	if b.last.IsZero() {
		return true
	}
	return now.Sub(b.last) >= b.every+b.jitter
}

// liveMembers filters the fold's member table through the Alive verdict.
func (b *Balancer) liveMembers() []Member {
	members := b.fold.Members()
	if b.Alive == nil {
		return members
	}
	live := members[:0]
	for _, m := range members {
		if m.Node == b.self || b.Alive(m.Node) {
			live = append(live, m)
		}
	}
	return live
}

// Tick plans and executes one balance step at now: at most one take and
// one shed. Losing the take race to another node is not an error, the
// fold's verdict already settled it.
func (b *Balancer) Tick(ctx context.Context) error {
	b.last = b.now()
	plan := PlanBalance(b.self, b.nGroups, b.liveMembers(), b.fold)
	if plan.HasTake {
		if _, err := b.mgr.Acquire(ctx, plan.Take); err != nil {
			return err
		}
	}
	if plan.HasShed && b.Shed != nil {
		if err := b.Shed(ctx, plan.Shed); err != nil {
			return err
		}
	}
	return nil
}
