// Gate D3 (spec 2064/obs1 doc 10): crash takeover clocked from kill to
// taker serving, across the fault schedule set, on the fleetsim
// harness. The mechanics half of the doc 02 prediction was scored
// wall-clock by PRED-OBS1-O3A-TAKEOVER; this lab scores the policy
// half, kill to full survivor coverage in simulated fleet time, under
// every degraded mode the harness reaches, with the invariants of the
// recovery verified on the healed bucket after each schedule.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/fleetsim"
	"github.com/tamnd/aki/engine/obs1/sim"
)

const (
	nGroups = 12
	step    = 100 * time.Millisecond
	victim  = uint64(3)
)

type schedule struct {
	name string
	seed uint64
	// fault arms at the kill; nil is the clean baseline.
	fault func() sim.FaultFn
	// heal clears the fault this long after the kill; zero keeps it
	// armed until recovery is observed.
	heal   time.Duration
	lo, hi time.Duration
	// settleHi bounds the post-recovery rebalance back to the
	// live-members rendezvous; maxEpoch bounds a victim group's epoch
	// after it, 3 meaning seized once and rebalanced once.
	settleHi time.Duration
	maxEpoch uint32
}

// ambiguous fails every nth mutation after it landed, the doc 02
// section 2.4 shape; reads stay clean so recheck-ours can see its own
// writes.
func ambiguous(nth int) sim.FaultFn {
	count := 0
	return func(op sim.Op, key string) *sim.Fault {
		if op == sim.OpGet {
			return nil
		}
		count++
		if count%nth != 0 {
			return nil
		}
		return &sim.Fault{Err: fmt.Errorf("gated3: ambiguous put on %s", key), Applied: true}
	}
}

func schedules() []schedule {
	return []schedule{
		{name: "clean", seed: 101,
			lo: 6500 * time.Millisecond, hi: 8 * time.Second,
			settleHi: 2 * time.Second, maxEpoch: 2},
		{name: "storm", seed: 102, fault: func() sim.FaultFn { return fleetsim.Storm(3) },
			lo: 6500 * time.Millisecond, hi: 9 * time.Second,
			settleHi: 12 * time.Second, maxEpoch: 3},
		{name: "read-outage", seed: 103, fault: fleetsim.ReadOutage, heal: 2 * time.Second,
			lo: 6500 * time.Millisecond, hi: 9 * time.Second,
			settleHi: 2 * time.Second, maxEpoch: 2},
		{name: "ambiguous-put", seed: 104, fault: func() sim.FaultFn { return ambiguous(5) },
			lo: 6500 * time.Millisecond, hi: 9 * time.Second,
			settleHi: 2 * time.Second, maxEpoch: 2},
		{name: "write-outage", seed: 105, fault: fleetsim.WriteOutage, heal: 9 * time.Second,
			lo: 9 * time.Second, hi: 10500 * time.Millisecond,
			settleHi: 12 * time.Second, maxEpoch: 3},
	}
}

type row struct {
	sc           schedule
	victimGroups int
	recovery     time.Duration
	settle       time.Duration
	errs         int
	pass         bool
}

// liveMembers is harness truth, every joined member whose stack is not
// crashed. The settle predicate must not use an observer's
// suspicion-filtered view: a degraded view prefers the observer for
// everything it can see and satisfies placement vacuously.
func liveMembers(f *fleetsim.Fleet, observer uint64) []obs1.Member {
	var out []obs1.Member
	for _, m := range f.Node(observer).Fold.Members() {
		if !f.Node(m.Node).Crashed {
			out = append(out, m)
		}
	}
	return out
}

func runSchedule(sc schedule) (row, error) {
	ctx := context.Background()
	r := row{sc: sc}
	f, err := fleetsim.New(fleetsim.Config{
		Seed: sc.seed, Nodes: 3, NGroups: nGroups, BalanceEvery: time.Second,
	})
	if err != nil {
		return r, err
	}
	for id := uint64(1); id <= 3; id++ {
		if err := f.Join(ctx, id); err != nil {
			return r, err
		}
	}
	converged := false
	for elapsed := time.Duration(0); elapsed < 20*time.Second; elapsed += step {
		f.Tick(ctx, step)
		if _, full := f.Coverage(1); full {
			converged = true
			break
		}
	}
	if !converged {
		return r, fmt.Errorf("%s: fleet never converged", sc.name)
	}
	if err := f.FollowAll(ctx); err != nil {
		return r, err
	}

	holders, _ := f.Coverage(1)
	var victimGroups []uint16
	for g, node := range holders {
		if node == victim {
			victimGroups = append(victimGroups, g)
		}
	}
	if len(victimGroups) == 0 {
		return r, fmt.Errorf("%s: the victim holds nothing", sc.name)
	}
	r.victimGroups = len(victimGroups)
	flushed := victimGroups[0]
	if err := f.FlushData(ctx, victim, flushed, 1, 3); err != nil {
		return r, err
	}
	if err := f.FollowAll(ctx); err != nil {
		return r, err
	}
	errsBefore := f.Node(1).Errs + f.Node(2).Errs

	// The kill. The schedule's fault arms with it.
	f.Crash(victim)
	if sc.fault != nil {
		f.SetFault(sc.fault())
	}
	healed := sc.fault == nil
	recovered := false
	for elapsed := time.Duration(0); elapsed < 15*time.Second; elapsed += step {
		if !healed && sc.heal > 0 && elapsed >= sc.heal {
			f.SetFault(nil)
			healed = true
		}
		f.Tick(ctx, step)
		covered := true
		for _, g := range victimGroups {
			if node, _, ok := f.Node(1).Fold.Holder(g); !ok || node == victim {
				covered = false
				break
			}
		}
		if covered {
			r.recovery = elapsed + step
			recovered = true
			break
		}
	}
	if !recovered {
		return r, fmt.Errorf("%s: survivors never recovered coverage", sc.name)
	}
	f.SetFault(nil)

	// Settle: tick until the whole placement matches the live-members
	// rendezvous. An outage or a phase-locked storm past the
	// discipline lets a survivor seize its peer's groups from a
	// degraded view, epoch-fenced and self-correcting, one balancer
	// move per tick; this phase prices that tail.
	settled := false
	for elapsed := time.Duration(0); elapsed < 20*time.Second; elapsed += step {
		members := liveMembers(f, 1)
		placed := true
		for g := 0; g < nGroups; g++ {
			node, _, ok := f.Node(1).Fold.Holder(uint16(g))
			pref, prefOK := obs1.PreferredNode(uint16(g), members)
			if !ok || !prefOK || node != pref {
				placed = false
				break
			}
		}
		if placed {
			r.settle = elapsed
			settled = true
			break
		}
		f.Tick(ctx, step)
	}
	if !settled {
		return r, fmt.Errorf("%s: placement never settled at the rendezvous", sc.name)
	}
	if err := f.FollowAll(ctx); err != nil {
		return r, err
	}
	r.errs = f.Node(1).Errs + f.Node(2).Errs - errsBefore

	// Invariants of the recovery, verified on the healed bucket.
	for _, g := range victimGroups {
		node, epoch, ok := f.Node(1).Fold.Holder(g)
		if !ok || node == victim || epoch < 2 || epoch > sc.maxEpoch {
			return r, fmt.Errorf("%s: group %d recovered on node %d at epoch %d ok=%v", sc.name, g, node, epoch, ok)
		}
	}
	taker, _, _ := f.Node(1).Fold.Holder(flushed)
	st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: f.Store, Prefix: "db/t", Group: flushed, Window: f.Node(taker).Win,
	})
	if err != nil {
		return r, err
	}
	if st.FramesApplied != 3 || st.Applied != 3 {
		return r, fmt.Errorf("%s: replay walked %+v, want 3 frames", sc.name, st)
	}
	if f.Node(1).Fold.StateSum() != f.Node(2).Fold.StateSum() {
		return r, fmt.Errorf("%s: survivor folds diverged", sc.name)
	}
	for id := uint64(1); id <= 2; id++ {
		if dead := f.Node(id).Fold.Stats.SectionsDead; dead != 0 {
			return r, fmt.Errorf("%s: node %d folded %d sections dead", sc.name, id, dead)
		}
	}
	if sc.fault == nil && r.errs != 0 {
		return r, fmt.Errorf("%s: %d duty-cycle errors on a clean bucket", sc.name, r.errs)
	}
	r.pass = r.recovery >= sc.lo && r.recovery <= sc.hi && r.settle <= sc.settleHi
	return r, nil
}

func main() {
	fmt.Println("schedule,victim_groups,recovery_ms,band_lo_ms,band_hi_ms,settle_ms,survivor_errs,verdict")
	fail := false
	for _, sc := range schedules() {
		r, err := runSchedule(sc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gated3: %v\n", err)
			os.Exit(1)
		}
		verdict := "PASS"
		if !r.pass {
			verdict, fail = "MISS", true
		}
		fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%s\n",
			r.sc.name, r.victimGroups, r.recovery.Milliseconds(),
			r.sc.lo.Milliseconds(), r.sc.hi.Milliseconds(),
			r.settle.Milliseconds(), r.errs, verdict)
	}
	if fail {
		os.Exit(1)
	}
}
