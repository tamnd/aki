// Crash takeover (spec 2064/obs1 doc 02 sections 3.2 case (b) and 4.4):
// a group whose holder went silent is free once its last heartbeat-or-
// commit on the chain is older than TTL plus skew AND the taker has
// observed that staleness for a full TTL of its own local time. The
// second half is the wait-out-the-TTL rule and it holds even for a taker
// that just booted with no history, because the watch starts from the
// taker's first stale observation, never from chain arithmetic.
//
// The judge is writer discipline, not safety: the fold cannot verify
// freeness without a clock (doc 02 section 3.3) and does not need to,
// because an early grant still moves the epoch deterministically and the
// zombie's later commits fold dead everywhere. The judge only keeps an
// honest taker from opening the zombie ack window wider than section 3.4
// promises.
package obs1

import (
	"sync"
	"time"
)

// takeWatch is one group's staleness observation: who the stale holder
// was, at which epoch and renewal position, and when this taker first saw
// the staleness. Any movement in those chain facts means the holder acted
// or the lease changed hands, and the watch restarts.
type takeWatch struct {
	node  uint64
	epoch uint32
	renew ChainPos
	since time.Time
}

// TakeoverJudge tracks per-group staleness observations against one
// node's fold and liveness view. One judge per node; Eligible is called
// from the takeover poll with the caller's clock, the same explicit-at
// convention Liveness.Suspect uses.
type TakeoverJudge struct {
	fold *LeaseFold
	live *Liveness
	ttl  time.Duration

	mu    sync.Mutex
	watch map[uint16]takeWatch
}

// NewTakeoverJudge builds a judge over the node's fold and liveness view.
// Zero ttl takes the doc 02 default; the ttl here is the taker-side full
// observation window, the same knob the leases run on.
func NewTakeoverJudge(fold *LeaseFold, live *Liveness, ttl time.Duration) *TakeoverJudge {
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	return &TakeoverJudge{
		fold: fold, live: live, ttl: ttl,
		watch: make(map[uint16]takeWatch),
	}
}

// Eligible reports whether the group may be taken over at the given
// instant. A free or released group answers false: that is freeness case
// (a) and belongs to plain Acquire. A held group answers true only when
// its holder is suspect (stale by TTL plus skew as this node observed the
// chain) and this judge has watched that same holder, epoch, and renewal
// position stay stale for a full TTL. Holder activity, a foreign grant,
// or a release all restart or drop the watch.
func (j *TakeoverJudge) Eligible(group uint16, at time.Time) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	node, epoch, ok := j.fold.Holder(group)
	if !ok {
		delete(j.watch, group)
		return false
	}
	if !j.live.Suspect(node, at) {
		delete(j.watch, group)
		return false
	}
	renew, _ := j.fold.LastRenewal(group)
	w, seen := j.watch[group]
	if !seen || w.node != node || w.epoch != epoch || w.renew != renew {
		j.watch[group] = takeWatch{node: node, epoch: epoch, renew: renew, since: at}
		return false
	}
	return at.Sub(w.since) >= j.ttl
}

// PlanTakeover lists the groups self should take over at the given
// instant: held by a takeover-eligible holder and preferring self by
// rendezvous among the given members. Callers pass the surviving member
// view (the fold's table with suspects filtered out), the same filter the
// balancer applies, so a crashed node never wins its own groups back and
// recovery load spreads per doc 02 section 4.4: a full-node takeover is
// G/N parallel single-group takeovers with no ordering between them,
// which is why this returns every eligible group rather than one.
func PlanTakeover(self uint64, nGroups int, members []Member, judge *TakeoverJudge, at time.Time) []uint16 {
	var out []uint16
	for g := 0; g < nGroups; g++ {
		group := uint16(g)
		if !judge.Eligible(group, at) {
			continue
		}
		if pref, ok := PreferredNode(group, members); ok && pref == self {
			out = append(out, group)
		}
	}
	return out
}
