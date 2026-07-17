package obs1_test

import (
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// The gate is the serving-side lease view.
var _ shard.LeaseView = (*obs1.LeaseGate)(nil)

// TestLeaseGuardHorizon pins the guard's fast-path summary: the horizon is
// the minimum tracked deadline minus the skew bound, it follows drops, and a
// guard tracking nothing has none.
func TestLeaseGuardHorizon(t *testing.T) {
	g := obs1.NewLeaseGuard(time.Hour, time.Minute)
	if _, ok := g.Horizon(); ok {
		t.Fatal("empty guard reports a horizon")
	}
	base := time.Unix(1000, 0)
	g.Renewed(1, base)
	g.Renewed(2, base.Add(30*time.Minute))
	h, ok := g.Horizon()
	if want := base.Add(time.Hour - time.Minute); !ok || !h.Equal(want) {
		t.Fatalf("horizon = %v %v, want %v: the earliest deadline minus the skew", h, ok, want)
	}
	g.Drop(1)
	h, ok = g.Horizon()
	if want := base.Add(30*time.Minute + time.Hour - time.Minute); !ok || !h.Equal(want) {
		t.Fatalf("horizon after drop = %v %v, want %v", h, ok, want)
	}
	g.Drop(2)
	if _, ok := g.Horizon(); ok {
		t.Fatal("drained guard still reports a horizon")
	}
}

// TestLeaseGateView drives the view contract without a pipeline: gated by
// definition while tracking nothing, open after a renewal, suspended again
// once the believed deadline passes, and demotion outranking any later
// renewal until an explicit regrant.
func TestLeaseGateView(t *testing.T) {
	gate := obs1.NewLeaseGate(time.Hour, time.Minute)
	now := time.Now()
	nowMs := now.UnixMilli()

	if !gate.Gated(nowMs) || !gate.AnySuspended(nowMs) || !gate.Suspended(3, nowMs) {
		t.Fatal("a gate tracking nothing must be gated and suspended by definition")
	}
	gate.Renewed(3, now)
	if gate.Gated(nowMs) || gate.AnySuspended(nowMs) || gate.Suspended(3, nowMs) {
		t.Fatal("a freshly renewed gate is still gated")
	}
	if gate.Renewals() != 1 {
		t.Fatalf("renewals = %d, want 1", gate.Renewals())
	}
	// A second group renewed in the past drags the whole-gate horizon back
	// while the fresh group stays serveable: Gated goes true, the per-group
	// check splits them.
	gate.Renewed(4, now.Add(-2*time.Hour))
	if !gate.Gated(nowMs) || !gate.Suspended(4, nowMs) || gate.Suspended(3, nowMs) {
		t.Fatal("a stale group must gate the fast path and suspend alone")
	}
	// Demotion outranks renewal: the endpoint stays until a regrant, even
	// with a raced late renewal landing after the drop.
	gate.Demote(4, "10.0.0.9:7000")
	if ep, ok := gate.Demoted(4); !ok || ep != "10.0.0.9:7000" {
		t.Fatalf("demoted = %q %v, want the taker's endpoint", ep, ok)
	}
	gate.Renewed(4, now)
	if _, ok := gate.Demoted(4); !ok {
		t.Fatal("a late renewal cleared a demotion; only a regrant may")
	}
	if !gate.Gated(nowMs) {
		t.Fatal("a gate with a demotion must stay gated")
	}
	gate.Regrant(4, now)
	if _, ok := gate.Demoted(4); ok {
		t.Fatal("regrant left the demotion in place")
	}
	if gate.Gated(nowMs) {
		t.Fatal("gate still gated after the regrant renewed both groups")
	}
	// Release forgets the group: suspended again, no redirect.
	gate.Release(3)
	if !gate.Suspended(3, nowMs) {
		t.Fatal("a released group must suspend")
	}
	if _, ok := gate.Demoted(3); ok {
		t.Fatal("a released group must not redirect")
	}
}

// TestLeaseGateRenewsOnCommit proves the doc 02 section 3.5 progress rule
// through the real pipeline: a gate whose group ran down its TTL is
// suspended, and the next flush's chain append, heard through the
// committer's OnAppended hook, renews exactly the groups the batch carried
// and un-suspends it. The renewal is the append, not a timer.
func TestLeaseGateRenewsOnCommit(t *testing.T) {
	const node = uint64(0xD7)
	store := sim.New(sim.Config{})
	rig := newLogRig(t, store, node)
	rig.grant(t, node, 1, 0, 1)
	gate := obs1.NewLeaseGate(time.Hour, time.Minute)
	// The committer fires OnAppended before OnCommitted, so this channel is
	// the happens-after edge the assertions wait on; the watermark wake
	// alone is not, because it rides the fold inside Append itself.
	committed := make(chan struct{}, 1)
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{Gate: gate, OnCommitted: func(uint64, obs1.ChainPos) {
		select {
		case committed <- struct{}{}:
		default:
		}
	}})
	defer func() {
		if err := wl.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	wl.SetGroup(0, 1, 1)
	wl.SetGroup(1, 1, 1)

	// Both groups renewed long ago: the TTL ran down, the gate suspends
	// them, which is the state a parked write waits in.
	past := time.Now().Add(-2 * time.Hour)
	gate.Renewed(0, past)
	gate.Renewed(1, past)
	nowMs := time.Now().UnixMilli()
	if !gate.Suspended(0, nowMs) || !gate.Suspended(1, nowMs) {
		t.Fatal("groups renewed two hours ago must be suspended")
	}
	before := gate.Renewals()

	// One frame on group 0, flushed and committed: the append carries a
	// group 0 section only, so only group 0 renews.
	if _, _, err := wl.StrSet([]byte("alpha"), []byte("v"), 0, false); err != nil {
		t.Fatal(err)
	}
	wl.Barrier()
	select {
	case <-committed:
	case <-time.After(10 * time.Second):
		t.Fatal("no commit within 10s")
	}
	if gate.Renewals() <= before {
		t.Fatal("the commit did not move the renewal count")
	}
	nowMs = time.Now().UnixMilli()
	if gate.Suspended(0, nowMs) {
		t.Fatal("group 0 still suspended after its own append committed")
	}
	if !gate.Suspended(1, nowMs) {
		t.Fatal("group 1 renewed without a section in the batch")
	}
}
