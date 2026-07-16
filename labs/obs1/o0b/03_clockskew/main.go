// Clock-skew lab (O0b, doc 02 sections 3.4, 3.5, and 5): one member's
// clock is wrong, by constant offset or by rate, and the questions are
// whether folded state stays safe (it must, section 3.3 is clock-free)
// and how the zombie ack window moves against the section 3.4 bound of
// TTL plus skew plus one heartbeat interval.
//
// Time is virtual: the loop advances a real-time cursor in fixed steps
// and each node sees clock(t) = offset + rate*t. Appends execute
// synchronously against the simulator and count as instantaneous next to
// the second-scale protocol windows, so store latency is not the
// adversary here; fence-torture already covered store-level races on
// live MinIO, and the clock adversary is orthogonal to the store.
//
// Each schedule runs a holder H and a taker T on one chain. H holds the
// group, heartbeats on its own clock, and acks client writes into RAM
// whenever its LeaseGuard says the lease is believed alive. At a set
// point H is partitioned from the chain: heartbeats stop landing but H
// keeps acking until its guard suspends, which is exactly the doc 02
// section 3.4 zombie. T watches renewals arrive on its own clock and
// grants itself the group at epoch+1 once staleness passes TTL plus
// skew (having itself observed for a full TTL). The zombie ack window
// is the real time between T's grant landing and H's last ack. After
// the run H reconnects, discovers the takeover, and its buffered commit
// goes to the chain under the old epoch, where every folder must kill
// it identically.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

const (
	ttl  = obs1.DefaultLeaseTTL
	skew = obs1.DefaultSkewBound
	hb   = obs1.DefaultHeartbeatEvery
	dt   = 10 * time.Millisecond
)

const csvHeader = "arm,offset_ms,rate,partition,acks,zombie_acks,window_ms,suspended,takeover,violations"

type result struct {
	acks       int
	zombieAcks int
	windowMS   int64
	suspended  bool
	takeover   bool
	violations []string
}

// schedule runs one arm. offset and rate shape H's clock; T's clock is
// honest. partition <= 0 means H stays healthy for the whole run.
func schedule(offset time.Duration, rate float64, partition, runFor time.Duration) result {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 5})
	const prefix = "cs"
	const group = 1

	hClock := func(t time.Duration) time.Time {
		return time.UnixMilli(1_000_000).Add(offset + time.Duration(rate*float64(t)))
	}
	tClock := func(t time.Duration) time.Time {
		return time.UnixMilli(1_000_000).Add(t)
	}

	foldH := obs1.NewLeaseFold()
	foldT := obs1.NewLeaseFold()
	aH, err := obs1.NewChainAppender(s, prefix, 0, 7, 1, obs1.ChainPos{}, foldH)
	check(err)
	aT, err := obs1.NewChainAppender(s, prefix, 0, 9, 1, obs1.ChainPos{}, foldT)
	check(err)
	guard := obs1.NewLeaseGuard(0, 0)

	// H takes the group at t=0 and the grant is its first renewal.
	_, err = aH.Append(ctx, []obs1.ChainRecord{obs1.GrantRecord{Group: group, Node: 7, Epoch: 1}})
	check(err)
	guard.Renewed(group, hClock(0))
	check(aT.Follow(ctx))

	var r result
	nextHB := hb          // next heartbeat due, on H's clock since its grant
	lastSeen := tClock(0) // when T last saw a renewal arrive, T's clock
	lastRenewPos, _ := foldT.LastRenewal(group)
	var grantAt time.Duration = -1 // real time T's takeover landed
	var lastAckAt time.Duration = -1
	demoted := false

	for t := time.Duration(0); t < runFor; t += dt {
		partitioned := partition > 0 && t >= partition
		// H: heartbeat cadence on its own clock.
		if !demoted && hClock(t).Sub(hClock(0)) >= nextHB {
			nextHB += hb
			if !partitioned {
				if _, err := aH.Append(ctx, []obs1.ChainRecord{obs1.HeartbeatRecord{}}); err == nil {
					// The append's catch-up folded the tail into H's own
					// fold; doc 02 section 3.3 says a holder that sees a
					// foreign grant demotes on the spot. Without this the
					// harness models a broken node, not a mis-clocked
					// one, and the first sweep proved it inflates the
					// window on cadence-starved arms.
					if n, _, ok := foldH.Holder(group); ok && n == 7 {
						guard.Renewed(group, hClock(t))
					} else {
						guard.Drop(group)
						demoted = true
					}
				}
			}
		}
		// H: ack a client write into RAM if the lease is believed alive.
		// A partitioned H that still acks after T's grant is the zombie.
		if !demoted && !guard.Suspended(group, hClock(t)) {
			r.acks++
			lastAckAt = t
			if grantAt >= 0 {
				r.zombieAcks++
			}
		} else if !r.suspended && !demoted {
			r.suspended = true
		}
		// T: watch arrivals on its own clock, take over past the bound.
		check(aT.Follow(ctx))
		if pos, ok := foldT.LastRenewal(group); ok && pos != lastRenewPos {
			lastRenewPos = pos
			if n, _, hold := foldT.Holder(group); hold && n == 7 {
				lastSeen = tClock(t)
			}
		}
		if grantAt < 0 && t >= ttl && tClock(t).Sub(lastSeen) > ttl+skew {
			_, err := aT.Append(ctx, []obs1.ChainRecord{obs1.GrantRecord{Group: group, Node: 9, Epoch: 2}})
			check(err)
			grantAt = t
			r.takeover = true
		}
	}

	if grantAt >= 0 && lastAckAt > grantAt {
		r.windowMS = (lastAckAt - grantAt).Milliseconds()
	}

	// H reconnects: its buffered writes go to the chain as one commit
	// under the epoch it still believes, then it follows and discovers.
	_, err = aH.Append(ctx, []obs1.ChainRecord{obs1.CommitRecord{
		WALNode: 7, WALSeq: 1, WALSize: 64,
		Sections: []obs1.CommitSection{{Group: group, Epoch: 1, StoredLen: 64, NFrames: 1, FirstSeq: 1, LastSeq: 1}},
	}})
	check(err)
	var verdicts []obs1.CommitVerdict
	foldCold := obs1.NewLeaseFold()
	foldCold.OnCommit = func(v obs1.CommitVerdict) error {
		verdicts = append(verdicts, v)
		return nil
	}
	aC, err := obs1.NewChainAppender(s, prefix, 0, 11, 1, obs1.ChainPos{}, foldCold)
	check(err)
	check(aC.Follow(ctx))
	check(aH.Follow(ctx))
	check(aT.Follow(ctx))

	// Safety: every folder agrees, and when a takeover happened the
	// zombie's late commit died at the fence on all of them.
	if foldH.StateSum() != foldCold.StateSum() || foldT.StateSum() != foldCold.StateSum() {
		r.violations = append(r.violations, "folders disagree")
	}
	last := verdicts[len(verdicts)-1]
	if r.takeover {
		if last.Live[0] {
			r.violations = append(r.violations, "zombie commit folded live after takeover")
		}
		if n, e, ok := foldCold.Holder(group); !ok || n != 9 || e != 2 {
			r.violations = append(r.violations, "taker does not hold the group")
		}
	} else if !last.Live[0] {
		r.violations = append(r.violations, "healthy holder's commit died without a takeover")
	}
	return r
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "clockskew:", err)
		os.Exit(1)
	}
}

func main() {
	header := flag.Bool("header", false, "print the csv header and exit")
	flag.Parse()
	if *header {
		fmt.Println(csvHeader)
		return
	}

	type arm struct {
		name      string
		offset    time.Duration
		rate      float64
		partition time.Duration
	}
	arms := []arm{
		// Constant offsets at honest rate, partitioned: elapsed-time
		// arithmetic should make every one identical to offset 0.
		{"offset", 0, 1.0, 6 * time.Second},
		{"offset", 250 * time.Millisecond, 1.0, 6 * time.Second},
		{"offset", -2 * time.Second, 1.0, 6 * time.Second},
		{"offset", 10 * time.Second, 1.0, 6 * time.Second},
		{"offset", -10 * time.Second, 1.0, 6 * time.Second},
		// Rate skew, partitioned: the guard suspends at (TTL-skew)/r
		// real ms after the last renewal, the taker grants at TTL+skew
		// after last arrival, so zombie acks start near r = 2500/3500.
		{"rate", 0, 1.0, 6 * time.Second},
		{"rate", 0, 0.9, 6 * time.Second},
		{"rate", 0, 0.8, 6 * time.Second},
		{"rate", 0, 0.714, 6 * time.Second},
		{"rate", 0, 0.6, 8 * time.Second},
		{"rate", 0, 0.4, 10 * time.Second},
		{"rate", 0, 0.1, 30 * time.Second},
		// Frozen clock, the VM-pause pathology: believed time never
		// advances, the guard never suspends, the window runs to the
		// end of the schedule. Safety must still hold.
		{"frozen", 0, 0.0, 30 * time.Second},
		// Healthy arms, no partition: a wrong clock alone must cause
		// no suspension and no takeover, slow or fast.
		{"healthy", 0, 1.0, 0},
		{"healthy", -5 * time.Second, 0.5, 0},
		{"healthy", 5 * time.Second, 2.0, 0},
	}

	fmt.Println(csvHeader)
	bad := 0
	for _, a := range arms {
		runFor := 20 * time.Second
		if a.partition > 0 {
			runFor = a.partition + 3*a.partition
		}
		if a.rate > 0 && a.rate < 0.5 {
			runFor = 60 * time.Second
		}
		if a.rate == 0 {
			runFor = 40 * time.Second
		}
		r := schedule(a.offset, a.rate, a.partition, runFor)
		fmt.Printf("%s,%d,%.3f,%v,%d,%d,%d,%v,%v,%d\n",
			a.name, a.offset.Milliseconds(), a.rate, a.partition > 0,
			r.acks, r.zombieAcks, r.windowMS, r.suspended, r.takeover, len(r.violations))
		for _, v := range r.violations {
			fmt.Fprintf(os.Stderr, "VIOLATION %s offset=%v rate=%.3f: %s\n", a.name, a.offset, a.rate, v)
			bad++
		}
	}
	if bad > 0 {
		os.Exit(1)
	}
}
